#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <errno.h>
#include <syslog.h>
#include <libgen.h>
#include <dirent.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <netinet/in.h>
#include <arpa/inet.h> // For htons, htonl
#include <fstream>
#include <string.h>

#include <algorithm>
#include <map>
#include <deque>
#include <vector>

#include "USBstream.h"
#include "USBstreamUtils.h"

using std::vector;
using std::string;
using std::map;

enum TriggerMode {kNone, kSingleLayer, kDoubleLayer};
enum packet_type { kOVR_DISCRIM = 0, kOVR_ADC = 1, kOVR_TRIGBOX = 2};


// Just little convenience structs for reading from the database
struct usb_sbop{
  int serial, board, offset, pmtboard_u;
  usb_sbop(const int serial_, const int board_,
           const int pmtboard_u_, const int offset_)
  {
    serial = serial_;
    board = board_;
    pmtboard_u = pmtboard_u_;
    offset = offset_;
  }
};

// Read up to this many sets of files from the DAQ before opening a new
// output file. The bigger this number is, the more memory is used,
// because no built data is written out until all files are read in.
// However, when we write out, we lose the ability to sort later data
// into place if it overlaps in time with data already written out. This
// can happen because times that the DAQ closes each USB stream's file
// are approximate.
//
// Nominally each DAQ file is 5 seconds of data, so 12 means we suffer
// this effect once per minute.
const int max_filesets_subrun = 12;

static const int maxUSB=10; // Maximum number of USB streams
static const int latency=5; // Seconds before DAQ switches files.
                            // FixME: 5 anticipated for far detector

static const int numChannels=64; // Number of channels in M64
static const int maxModules=64; // Maximum number of modules PER USB
                                // (okay if less than total number of modules)

// Map from USB serial numbers to their location in array of OVUSBStreams
// (sigh).  Filled in setup_from_config().
static map<int, int> usbserial_to_usbindex;

// Will stop if we haven't seen a new input file in ENDTIME seconds when
// we know the run is over or MAXTIME seconds regardless. For Double
// Chooz, MAXTIME was 60.
static const int MAXTIME=5;
static const int ENDTIME=1;

static const int SYNC_PULSE_CLK_COUNT_PERIOD_LOG2=29; // trigger system emits
                                                      // sync pulse at 62.5MHz

// Mutated as program runs
static int OV_EB_State = 0;
static int initial_delay = 0;

// Set in parse_options()
static int Threshold = 73; //default 1.5 PE threshold
static string OutBase; // output file
static TriggerMode EBTrigMode = kDoubleLayer; // double-layer threshold
static string InputDir; // input data directory

// Set in setup_from_config() and used throughout
static unsigned int numUSB = 0;

// Maps {USB_serial, board_number}, the input numbering convention, to
// pmtboard_u, the output numbering convention
static map<std::pair<int, int>, uint16_t> PMTUniqueMap;

static USBstream OVUSBStream[maxUSB];

// *Size* set in setup_from_config()
static bool *overflow; // Keeps track of sync overflows for all boards

// Keeps track of max clock count for sync overflows for all boards
static long int *maxcount_16ns;


// Decodes USB stream with array index *usbindex. For threading.
static void * decode(void * usbindex)
{
  OVUSBStream[*((int *)usbindex)].decodefile();
  return NULL;
}

// opens output data file
static int open_file(const char * const name)
{
  errno = 0;
  const int fd = open(name, O_WRONLY | O_CREAT, 0644);
  if(fd < 0)
    log_msg(LOG_CRIT, "Fatal Error: failed to open file %s: %s\n",
            name, strerror(errno));
  return fd;
}

static int check_disk_space(const string & dir)
{
  struct statvfs fiData;
  if((statvfs(dir.c_str(), &fiData)) < 0 ) {
    log_msg(LOG_NOTICE, "Error: Failed to stat %s\n", dir.c_str());
    return 0;
  }
  else {
    if(fiData.f_blocks) {
      const int free_space_percent =
        (int)(100*(double)fiData.f_bfree/(double)fiData.f_blocks);
      if(free_space_percent < 3) {
        log_msg(LOG_ERR, "Error: Can't write to disk: Found disk "
          ">97 percent full: statvfs called on %s\n", dir.c_str());
        return -1;
      }
    }
    else {
      log_msg(LOG_ERR, "Error: statvfs failed to read blocks in %s\n", dir.c_str());
      return -1;
    }
  }
  return 0;
}

static void check_status(const vector<string> & files)
{
  static int Ddelay = 0;
  // Performance monitor
  const int f_delay = (int)(latency*files.size()/numUSB/20);
  if(f_delay != OV_EB_State) {
    if(f_delay > OV_EB_State) {
      if(OV_EB_State <= initial_delay) { // OV EBuilder was not already behind
        log_msg(LOG_NOTICE, "Falling behind processing files\n");
      }
      else { // OV EBuilder was already behind
        Ddelay = f_delay - initial_delay;
        if(Ddelay % 3 == 0) { // Every minute of delay
          Ddelay /= 3;
          log_msg(LOG_NOTICE,
            "Process has accumulated %d min of delay since starting\n", Ddelay);
        }
      }
    }
    else if(f_delay < OV_EB_State) {
      if(OV_EB_State >= initial_delay) {
        log_msg(LOG_NOTICE, "Catching up processing files\n");
      }
      else {
        Ddelay = initial_delay - f_delay;
        if(Ddelay % 3 == 0) { // Every minute of recovery
          Ddelay /= 3;
          log_msg(LOG_NOTICE, "Process has reduced data processing delay by %d "
            "min since starting\n", Ddelay);
        }
      }
    }
    OV_EB_State = f_delay;
  }
}

// Fills myfiles with a list of files in the given directory.
//
// These files are the set that does not have a dot in their name.
// This excludes files that the DAQ is in the process of writing out,
// which end with ".wr".
//
// Also exclude files with names containing "baseline" unless
// 'allow_baseline' is true.
//
// Return true if no files are found that satisfy those rules, including if
// the directory couldn't be read.  Otherwise, returns false.
static bool GetDir(const std::string dir, std::vector<std::string> &myfiles,
                   const bool allow_baseline = false)
{
  DIR *dp;
  struct dirent *dirp;

  errno = 0;
  if((dp = opendir(dir.c_str())) == NULL) return true;

  while((dirp = readdir(dp)) != NULL){
    const std::string myfname = std::string(dirp->d_name);

    if(myfname.find(".") != std::string::npos)
      continue;

    if(!allow_baseline && (myfname.find("baseline")  != std::string::npos))
      continue;

    myfiles.push_back(myfname);
  }

  if(closedir(dp) < 0) return true;

  return myfiles.size()==0;
}

static bool TryInitRun()
{
  vector<string> files;
  if(GetDir(InputDir, files) && errno)
    log_msg(LOG_CRIT, "Error (%s) opening directory %s\n", strerror(errno),
            InputDir.c_str());

  OV_EB_State = initial_delay = (int)(latency*files.size()/numUSB/20);

  return true;
}

// Checks that we can open the input directory and that there's at least
// one file in there. Sets up performance statistics.
static void InitRun()
{
  const time_t oldtime = time(0);

  while(!TryInitRun()) {
    if((int)difftime(time(0), oldtime) > MAXTIME)
      log_msg(LOG_CRIT, "No input files found for %d seconds.\n", MAXTIME);
    else
      sleep(1);
  }
}

// If there is a file ready for each USB stream, open one for each.
// Returns true if this happens, and false otherwise.
static bool OpenNextFileSet()
{
  if(check_disk_space(InputDir) < 0) // Why are we checking the *input* directory?
    log_msg(LOG_CRIT, "Fatal error in check_disk_space(%s)\n", InputDir.c_str());

  vector<string> files;
  if(GetDir(InputDir, files)) return false;
  if(files.size() < numUSB) return false;

  sort(files.begin(), files.end());

  const string fdelim = "_"; // Files must be of form xxxxxxxxx_xx

  // Ignore files without a delimiter
  for(unsigned int j = 0; j < files.size(); j++){
    if(files[j].find(fdelim) == string::npos){
      files.erase(files.begin()+j);
      j--;
    }
  }

  check_status(files); // Performance monitor

  int missing = 0;
  for(unsigned int k = 0; k<numUSB; k++) {
    for(unsigned int j = k; j < files.size(); j++) {
      const size_t fname_it_delim = files[j].find(fdelim);

      const string fusb = files[j].substr(fname_it_delim+1);
      if(strtol(fusb.c_str(), NULL, 10) == OVUSBStream[k].GetUSB()) {
        log_msg(LOG_INFO, "Data file from USB %d found\n", OVUSBStream[k].GetUSB());
        std::swap(files[j], files[k]);
        break;
      }

      if(j == files.size()-1){ // Failed to find a file for USB k
        log_msg(LOG_INFO, "Data file from USB %d not found\n", OVUSBStream[k].GetUSB());
        missing++;
      }
    }
  }
  if(missing > 0){
    log_msg(LOG_WARNING, "Only %d of %d USB data files found\n",
            numUSB-missing, numUSB);
    return false;
  }

  vector<string>::iterator filesit = files.begin();

  int status = 0;
  string base_filename;
  for(unsigned int k=0; k<numUSB; k++) {
    const size_t fname_it_delim = filesit->find(fdelim);
    const string ftime_min = filesit->substr(0, fname_it_delim);
    const string fusb = filesit->substr(fname_it_delim+1);

    // Build input filename ( _$usb will be added by LoadFile function )
    base_filename=InputDir;
    base_filename.append("/");
    base_filename.append(ftime_min);
    if( (status = OVUSBStream[k].LoadFile(base_filename)) < 1 ) // Can't load file
      return status;

    filesit++;
  }

  return true;
}

static void BuildEvent(const vector<decoded_packet> & in_packets,
                       const vector<int> & OutIndex, const int fd)
{
  if(fd <= 0)
    log_msg(LOG_CRIT, "Fatal Error in BuildEvent(). Invalid file "
      "handle for previously opened data file!\n");

  if(in_packets.empty()){
    log_msg(LOG_WARNING, "Got empty data in BuildEvent(). Trying to continue.\n");
    return;
  }

  OVEventHeader evheader;
  evheader.time_sec = in_packets[0].timeunix;
  evheader.n_ov_data_packets = in_packets.size();

  if(!evheader.writeout(fd))
    log_msg(LOG_CRIT, "Fatal Error: Cannot write event header!\n");

  for(unsigned int packeti = 0; packeti < in_packets.size(); packeti++){
    const decoded_packet & packet = in_packets[packeti];

    const int usb = OVUSBStream[OutIndex[packeti]].GetUSB();
    if(!PMTUniqueMap.count(std::pair<int, int>(usb, packet.module)))
      log_msg(LOG_ERR, "Got unknown module number %d on USB %d\n",
              packet.module, usb);

    const int16_t module = PMTUniqueMap[std::pair<int, int>(usb, packet.module)];

    if(!packet.isadc){
      log_msg(LOG_ERR, "Got non-ADC packet. Not supported!\n");
      continue;
    }

    // Sync pulse diagnostic info: pulse expected at clock count
    // 2^(SYNC_PULSE_CLK_COUNT_PERIOD_LOG2).  Look for overflows.
    if( packet.time16ns > (1 << SYNC_PULSE_CLK_COUNT_PERIOD_LOG2) ) {
      if(!overflow[module]) {
        log_msg(LOG_WARNING, "Module %d missed sync pulse near "
          "Unix time stamp %ld\n", module, evheader.time_sec);
        overflow[module] = true;
      }
      maxcount_16ns[module] = packet.time16ns;
    }
    else if(overflow[module]) {
      log_msg(LOG_WARNING, "Module %d max clock count %ld\t",
        module, maxcount_16ns[module]);
      maxcount_16ns[module] = packet.time16ns;
      overflow[module] = false;
    }

    OVDataPacketHeader moduleheader;
    moduleheader.nHits = packet.hits.size();
    moduleheader.module = module;
    moduleheader.time16ns = packet.time16ns;

    if(!moduleheader.writeout(fd))
      log_msg(LOG_CRIT, "Fatal Error: Cannot write data packet header!\n");

    for(int m = 0; m < moduleheader.nHits; m++) {
      OVHitData hit;
      hit.channel = packet.hits[m].channel;
      hit.charge  = packet.hits[m].charge;

      if(!hit.writeout(fd))
        log_msg(LOG_CRIT, "Fatal Error: Cannot write hit!\n");
    }
  }
}

static string parse_options(int argc, char **argv)
{
  bool option_t_used = false;
  string configfile;
  if(argc <= 1) goto fail;

  char c;
  while((c = getopt(argc, argv, "c:t:T:i:o:h")) != -1) {
    switch (c) {
      case 'i': InputDir = optarg; break;
      case 'o': OutBase  = optarg; break;
      case 't': Threshold = atoi(optarg); option_t_used = true; break;
      case 'T': EBTrigMode = (TriggerMode)atoi(optarg); break;
      case 'c': configfile = optarg; break;
      case 'h':
      default:  goto fail;
    }
  }
  if(configfile == ""){
    printf("You must use the -c option\n");
    goto fail;
  }
  if(OutBase == ""){
    printf("You must use the -o option\n");
    goto fail;
  }
  if(InputDir == ""){
    printf("You must use the -i option\n");
    goto fail;
  }
  if(option_t_used && EBTrigMode == kNone){
    printf("Warning: threshold given with -t ignored with -T 0\n");
  }
  if(optind < argc){
    printf("Unknown options given\n");
    goto fail;
  }
  if(EBTrigMode < kNone || EBTrigMode > kDoubleLayer){
    printf("Invalid trigger mode %d\n", EBTrigMode);
    goto fail;
  }
  if(Threshold < 0) {
    printf("Negative thresholds not allowed.\n");
    goto fail;
  }

  for(int index = optind; index < argc; index++){
    printf("Non-option argument %s\n", argv[index]);
    goto fail;
  }

  return configfile;

  fail:
  printf(
    "Usage: %s -i <input data directory> -o <EBuilder_output_disk>\n"
    "          -c <config file>\n"
    "         [-t <offline_threshold>] [-T <offline_trigger_mode>]\n"
    "\n"
    "Mandatory arguments:\n"
    "  -i : Input data directory\n"
    "  -o : Output file\n"
    "  -c : Configuration file giving USB and PMT information\n"
    "\n"
    "Optional arguments:\n"
    "  -t : offline threshold (ADC counts) to apply\n"
    "       default: 0 (no software threshold, even negative ADC hits pass)\n"
    "  -T : offline trigger mode\n"
    "       0: No threshold\n"
    "       1: Per-channel threshold\n"
    "       2: [default] Overlapping pair: both hits over threshold, if any\n",
    argv[0]);
  exit(127);
}

static void CalculatePedestal(int baseptr[maxModules][numChannels],
                              const vector<decoded_packet> & BaselineData)
{
  double baseline[maxModules][numChannels] = {};
  int counter[maxModules][numChannels] = {};

  for(vector<decoded_packet>::const_iterator I = BaselineData.begin();
      I != BaselineData.end();
      I++) {

    const int module = I->module;
    const int type = I->isadc;

    if(type != kOVR_ADC) continue;

    if(module > maxModules)
      log_msg(LOG_CRIT, "Fatal Error: Module number requested "
        "(%d) out of range (0-%d) in calculate pedestal\n", module, maxModules);

    for(unsigned int i = 0; i < I->hits.size(); i++) {
      const int charge = I->hits[i].charge;
      const int channel = I->hits[i].channel;
      if(channel >= numChannels)
        log_msg(LOG_CRIT, "Fatal Error: Channel number requested "
          "(%d) out of range (0-%d) in calculate pedestal\n",
          channel, numChannels-1);

      // Should these be modified to better handle large numbers of baseline
      // triggers?
      baseline[module][channel] = (baseline[module][channel]*
        counter[module][channel] + charge)/(counter[module][channel]+1);
      counter[module][channel]++;
    }
  }

  for(int i = 0; i < maxModules; i++)
    for(int j = 0; j < numChannels; j++)
      baseptr[i][j] = (int)baseline[i][j];
}

static bool GetBaselines()
{
  // Check for a baseline file directory with the right right number of files.
  {
    vector<string> all_files;
    if(GetDir(InputDir, all_files, true)) {
      if(errno)
        log_msg(LOG_ERR, "Error (%s) opening binary "
          "directory %s for baselines\n", strerror(errno), InputDir.c_str());
      return false;
    }

    vector<string> baseline_files;
    for(vector<string>::iterator f = all_files.begin(); f != all_files.end(); f++)
      if(f->find("baseline") != string::npos)
        baseline_files.push_back(*f);

    if(baseline_files.size() != numUSB) {
      log_msg(LOG_ERR, "Error: Baseline file count (%lu) != "
        "numUSB (%u) in directory %s\n", (long int)baseline_files.size(), numUSB,
         InputDir.c_str());
      return false;
    }
  }

  log_msg(LOG_INFO, "Processing baselines...\n");

  // Set USB numbers for each OVUSBStream and load baseline files
  for(unsigned int i = 0; i < numUSB; i++) {
    if( OVUSBStream[i].GetUSB() == -1 ) {
      log_msg(LOG_ERR, "Error: USB number unassigned while getting baselines\n");
      return false;
    }
    if(OVUSBStream[i].LoadFile(InputDir+ "/baseline") < 1)
      return false; // Load baseline file for data streams
  }

  // Decode all files and load into memory
  for(unsigned int j = 0; j < numUSB; j++){
    log_msg(LOG_INFO, "Decoding baseline %d\n", j),
    OVUSBStream[j].decodefile();
  }

  for(unsigned int i = 0; i < numUSB; i++) {
    vector<decoded_packet> BaselineData;
    OVUSBStream[i].GetBaselineData(&BaselineData);
    int baselines[maxModules][numChannels] = { { } };
    CalculatePedestal(baselines, BaselineData);
    OVUSBStream[i].SetBaseline(baselines);
  }

  return true;
}

// Try to read in the baselines for MAXTIME seconds.  If they don't appear,
// exit with status 127.
static void LoadBaselineData()
{
  const time_t oldtime = time(0);
  while(!GetBaselines()){
    if((int)difftime(time(0), oldtime) > MAXTIME)
      log_msg(LOG_CRIT, "Baseline data not found for %d seconds.\n", MAXTIME);
    sleep(2);
  }
}

// Return a vector of {USB serial numbers, board numbers, pmtboard_u, time offsets}
// for all USBs in the given table.  Sets no globals.
static vector<usb_sbop> get_sbops(const char * configfilename)
{
  vector<usb_sbop> sbops;
  FILE * configfile = fopen(configfilename, "r");
  if(configfile == NULL) log_msg(LOG_CRIT, "Could not read config file\n");

  char * line = NULL;
  size_t len = 0;
  while(getline(&line, &len, configfile) != -1){
    if(len < 2 || line[0] == '#') continue;
    int serial, board, pmtboard, offset;
    if(4 != sscanf(line, "%d %d %d %d", &serial, &board, &pmtboard, &offset))
      log_msg(LOG_CRIT, "Invalid line in config file: %s\n", line);
    sbops.push_back(usb_sbop(serial, board, pmtboard, offset));
  }
  fclose(configfile);

  if(line) free(line);
  return sbops;
}

// Finds the list of distinct usb serial numbers.  No side effects.
static vector<int> get_distinct_usb_serials(const vector<usb_sbop> & sbops)
{
  vector<int> serials;
  for(unsigned int i = 0; i < sbops.size(); i++)
    if(serials.end() == find(serials.begin(), serials.end(), sbops[i].serial))
      serials.push_back(sbops[i].serial);
  return serials;
}


// Returns the number of boards and the highest module number
static int sbop_max_board(const vector<usb_sbop> & sbops)
{
  int maxb = 0;
  for(unsigned int i = 0; i < sbops.size(); i++)
    if(sbops[i].pmtboard_u > maxb)
      maxb = sbops[i].pmtboard_u;
  return maxb;
}

static void setup_from_config(const string & configfile)
{
  const vector<usb_sbop> sbops = get_sbops(configfile.c_str());

  const vector<int> usbserials = get_distinct_usb_serials(sbops);
  numUSB = usbserials.size();

  // Helpful for baseline substraction later on
  for(unsigned int i = 0; i < usbserials.size(); i++)
    usbserial_to_usbindex[usbserials[i]] = i;

  for(unsigned int i = 0; i < sbops.size(); i++) {
    if(sbops[i].board >= maxModules)
      log_msg(LOG_CRIT, "Error: config references module %d, but max is %d.\n",
              sbops[i].board, maxModules-1);

    // Set offsets, clumsily dealing with the indexing of OVUSBStream
    OVUSBStream[
      std::find(usbserials.begin(), usbserials.end(), sbops[i].serial)
      - usbserials.begin()
    ].SetOffset(sbops[i].board, sbops[i].offset);

    // Maps input numbering convention to output numbering convention.
    PMTUniqueMap[std::pair<int, int>(sbops[i].serial, sbops[i].board)]
      = sbops[i].pmtboard_u;
  }

  // Count the number of boards in this setup
  const int max_board   = sbop_max_board(sbops);
  overflow = new bool[max_board+1];
  maxcount_16ns = new long int[max_board+1];
  memset(overflow, 0, (max_board+1)*sizeof(bool));
  memset(maxcount_16ns, 0, (max_board+1)*sizeof(long int));

  for(unsigned int i = 0; i < numUSB; i++){
    OVUSBStream[i].SetThresh(Threshold, (int)EBTrigMode);
    OVUSBStream[i].SetUSB(usbserials[i]);
  }
}

static bool run_has_ended = false;

static void end_run_signal_handler(__attribute__((unused)) int sig)
{
  run_has_ended = true;
}

static bool write_end_block_and_close(const int data_fd)
{
  const uint32_t end = 0x53544F50; // "STOP"
  const uint32_t nend = htonl(end);
  if(sizeof nend != write(data_fd, &nend, sizeof nend)){
    log_msg(LOG_ERR, "End of run write error\n");
    return false;
  }

  if(close(data_fd) < 0){
    log_msg(LOG_ERR, "Could not close output data file\n");
    return false;
  }

  return true;
}

// This is all the code that I found around BuildEvent. It does some things. I
// have not really figured out what they are yet. Sorry sorry sorrysorrysorry.
// In some fashion it does the building of the available data and leaves the
// unbuilt data for the next try.  It returns the number of events built.
static unsigned int
  SuperBuildEvents(vector< vector<decoded_packet> > & CurrentData, const int fd)
{
  // I don't know how many of these need to be static
  static vector<decoded_packet>::iterator CurrentDataIt[maxUSB];
  static vector<decoded_packet> MinData; // Current minimum data packets
  static decoded_packet MinDataPacket; // Minimum and Last Data Packets added
  static vector<int> MinIndex; // USB indices of Minimum Data Packet
  static vector<decoded_packet> ExtraData;// carries events from last timestamp
  static vector<int> ExtraIndex;

  unsigned int EventCounter = 0;
  // index of minimum event added to USB stream
  int imin = 0;
  for(unsigned int i = 0; i < numUSB; i++) {
    // MinIndex is set to the last CurrentData that's empty,
    // or zero if none are empty.
    CurrentDataIt[i] = CurrentData[i].begin();
    if(CurrentData[i].empty()) imin = i;
  }
  MinData .assign(ExtraData .begin(), ExtraData .end());
  MinIndex.assign(ExtraIndex.begin(), ExtraIndex.end());

  // This is an elaborate test for whether all CurrentDatas are
  // non-empty up to numUSB.
  while( CurrentDataIt[imin] != CurrentData[imin].end() ) {
    // Until 1 USB stream finishes timestamp

    imin=0; // Reset minimum to first USB stream
    MinDataPacket = *(CurrentDataIt[imin]);

    for(unsigned int k = 0; k < numUSB; k++) { // Loop over USB streams, find minimum
      decoded_packet CurrentDataPacket = *(CurrentDataIt[k]);

      // Find real minimum; no clock slew
      if( LessThan(CurrentDataPacket, MinDataPacket, 0) ) {
        // If current packet less than min packet, min = current
        MinDataPacket = CurrentDataPacket;
        imin = k;
      }
    } // End of for loop: MinDataPacket has been filled appropriately

    if(MinData.size() > 0) { // Check for equal events
      if( LessThan(MinData.back(), MinDataPacket, 3) ) {
        // Ignore gaps which consist of fewer than 4 clock cycles
        ++EventCounter;
        BuildEvent(MinData, MinIndex, fd);

        MinData.clear();
        MinIndex.clear();
      }
    }
    MinData.push_back(MinDataPacket); // Add new element
    MinIndex.push_back(imin);
    CurrentDataIt[imin]++; // Increment iterator for added packet

  } // End of while loop: Events have been built for this time stamp

  // Clean up operations and store data for later
  for(unsigned int k = 0; k < numUSB; k++)
    CurrentData[k].assign(CurrentDataIt[k], CurrentData[k].end());
  ExtraData .assign(MinData .begin(), MinData .end());
  ExtraIndex.assign(MinIndex.begin(), MinIndex.end());

  return EventCounter;
}

// move files into a subdirectory called decoded/ and rename then with ".done"
static void rename_files_we_have_read()
{
  for(unsigned int j = 0; j<numUSB; j++) {
    const string origname = OVUSBStream[j].GetFileName();
    const string origname2 = OVUSBStream[j].GetFileName(); // basename insanity
    const string donedir = dirname((char *)origname.c_str()) + string("/decoded/");
    const string donename = donedir +
      string(basename((char *)OVUSBStream[j].GetFileName())) + ".done";

    errno = 0;
    if(mkdir(donedir.c_str(), 0755) == -1 && errno != EEXIST){
      log_msg(LOG_CRIT, "Could not create directory %s: %s.\n",
              donedir.c_str(), strerror(errno));
      exit(1);
    }

    errno = 0;
    if(rename(origname2.c_str(), donename.c_str())) {
      log_msg(LOG_CRIT, "Could not rename input file %s to %s: %s.\n",
              origname2.c_str(), donename.c_str(), strerror(errno));
      exit(1);
    }
  }
}

static void setup_signals()
{
  // Lots of boilerplate that just says that when we get a
  // SIGUSR1, call end_run_signal_handler().
  struct sigaction usr1_action;
  memset(&usr1_action, 0, sizeof(struct sigaction));
  sigemptyset(&usr1_action.sa_mask);
  usr1_action.sa_handler = end_run_signal_handler;

  if(-1 == sigaction(SIGUSR1, &usr1_action, NULL)){
    perror("sigaction()");
    exit(1);
  }
}

// Waits for new files and returns true if it opened some.  If the run
// ends or no files are forthcoming, return false.
static bool HandleOpenNextFileSet()
{
  const time_t oldtime = time(0);

  while(!OpenNextFileSet()){ // Try to find new files for each USB
    if((difftime(time(0), oldtime) > ENDTIME && run_has_ended)
     || difftime(time(0), oldtime) > MAXTIME) {

      if(run_has_ended)
        log_msg(LOG_INFO, "Finished processing run\n");
      else
        log_msg(LOG_ERR, "No new files for %ds, but I didn't hear that "
          "the run was over! Closing output file anyway.\n", MAXTIME);

      return false;
    }
    log_msg(LOG_INFO, "Files are not ready. Waiting...\n");
    sleep(1);
  }
  return true;
}

// Decode the latest set of open input files.  Each is decoded in a
// separate thread.  The decoded data is kept inside the USBStream
// objects for later retrieval.
static void DecodeFileSet()
{
  pthread_t threads[numUSB]; // An array of threads to decode files

  int indices[numUSB]; // arguments for pthread.
  for(unsigned int j = 0; j < numUSB; j++){ // Load all files in at once
    indices[j] = j;
    pthread_create(&threads[j], NULL, decode, &indices[j]);
  }

  for(unsigned int j = 0; j < numUSB; j++)
    pthread_join(threads[j], NULL);
}

// Reads in data from files into CurrentData until either the maximum
// number of files has been read or the conditions for stopping the run
// have been met. A "subrun" is the set of data read in this way. All
// data for a subrun is kept in memory together so that it can be sorted
// by time.
static void read_in_for_subrun(vector< vector<decoded_packet> > & CurrentData)
{
  for(int nfilesets = 0; nfilesets < max_filesets_subrun; nfilesets++){
    // Open set of files
    if(!HandleOpenNextFileSet()) return;

    // Move the data from the files into USBStream objects
    log_msg(LOG_INFO, "Decoding file set #%d for this run\n", nfilesets);
    DecodeFileSet();

    rename_files_we_have_read();

    // Move data from USBStream object into CurrentData's.
    // XXX worried about this.  It reads up to the Unix time stamp, a
    // synchronization point, except nothing seems to keep these time stamps
    // synchronized between the several USB streams.
    for(unsigned int j = 0; j < numUSB; j++)
      OVUSBStream[j].GetDecodedDataUpToNextUnixTimeStamp(CurrentData[j]);
  }
}

// Do everything after the setup steps and the baseline determinations.
// Reads data and writes out subrun files until there's no more to do.
static void MainBuild()
{
  // for current timestamp to process
  vector< vector<decoded_packet> > CurrentData(maxUSB);

  for(unsigned int subrun = 0; !run_has_ended; subrun++){
    read_in_for_subrun(CurrentData);

    const unsigned int BUFSIZE = 1024;
    char outfile[BUFSIZE];
    snprintf(outfile, BUFSIZE, "%s_%05u", OutBase.c_str(), subrun);
    const int fd = open_file(outfile);

    const unsigned int EventCounter = SuperBuildEvents(CurrentData, fd);
    write_end_block_and_close(fd);

    log_msg(LOG_INFO, "Number of built events: %d\nProcessed time stamp: %d\n",
            EventCounter, OVUSBStream[0].GetTOLUTC());
  }
}

int main(int argc, char **argv)
{
  const string configfile = parse_options(argc, argv);
  setup_signals(); // so we will know when each run has ended
  start_log(); // establish syslog connection
  setup_from_config(configfile);
  LoadBaselineData();
  InitRun();

  MainBuild();

  return 0;
}
