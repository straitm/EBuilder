#include "USBstream-TypeDef.h"
#include "USBstream.h"
#include "USBstreamUtils.h"

#include <stdio.h>
#include <stdarg.h>
#include <pthread.h>
#include <errno.h>
#include <syslog.h>
#include <libgen.h>
#include <dirent.h>
#include <algorithm>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <map>

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

struct some_run_info{
  int ebsubrun;
  bool has_ebsubrun, has_stoptime;
};

// Consts
static const int maxUSB=10; // Maximum number of USB streams
static const int latency=5; // Seconds before DAQ switches files.
                            // FixME: 5 anticipated for far detector
static const int timestampsperoutput = 5; // XXX what?
static const int numChannels=64; // Number of channels in M64
static const int maxModules=64; // Maximum number of modules PER USB
                                // (okay if less than total number of modules)

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

// Set in setup_from_config() from database information and used throughout
static unsigned int numUSB = 0;
static map<int, int> Datamap; // XXX Maps numerical ordering of
                              // USBs to all numerical ordering of all USBs
static map<int, int*> PMTOffsets; // Map to hold offsets for each PMT board
static map<int, uint16_t> PMTUniqueMap; // Maps 1000*USB_serial + board_number
                                        // to pmtboard_u in MySQL table
static USBstream OVUSBStream[maxUSB];

// *Size* set in setup_from_config()
static bool *overflow; // Keeps track of sync overflows for all boards
static long int *maxcount_16ns_lo; // Keeps track of max clock count
static long int *maxcount_16ns_hi; // for sync overflows for all boards


static void *decode(void *ptr) // This defines a thread to decode files
{
  long int usb = (long int) ptr; // XXX munging a void* into an int!
  while(!OVUSBStream[(int)usb].decode()) usleep(100);
  return NULL;
}

// opens output data file
static int open_file(const char * const name)
{
  errno = 0;
  const int fd = open(name, O_WRONLY | O_CREAT, 0644);
  if(fd < 0){
    log_msg(LOG_CRIT, "Fatal Error: failed to open file %s: %s\n",
            name, strerror(errno));
    exit(1);
  }
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
//
// Also exclude files with names containing "baseline" or "processed" unless
// 'allow_bl_and_pc' is true.
//
// Return true if no files are found that satisfy those rules, including if
// the directory couldn't be read.  Otherwise, returns false.
static bool GetDir(const std::string dir, std::vector<std::string> &myfiles,
                   const bool allow_bl_and_pc = false)
{
  DIR *dp;
  struct dirent *dirp;

  errno = 0;
  if((dp = opendir(dir.c_str())) == NULL) return true;

  while((dirp = readdir(dp)) != NULL){
    const std::string myfname = std::string(dirp->d_name);

    if(myfname.find(".") != std::string::npos)
      continue;

    if(!allow_bl_and_pc && (myfname.find("baseline")  != std::string::npos ||
                            myfname.find("processed") != std::string::npos) )
      continue;

    myfiles.push_back(myfname);
  }

  if(closedir(dp) < 0) return true;

  return myfiles.size()==0;
}

static bool TryInitRun()
{
  vector<string> files;
  if(GetDir(InputDir, files)) {
    if(errno) {
      log_msg(LOG_CRIT, "Error (%s) opening directory %s\n", strerror(errno),
              InputDir.c_str());
      exit(1);
    }
    return false;
  }

  OV_EB_State = initial_delay = (int)(latency*files.size()/numUSB/20);

  return true;
}

// Checks that we can open the input directory and that there's at least
// one file in there. Sets up performance statistics.
static void InitRun()
{
  const time_t oldtime = time(0);

  while(!TryInitRun()) {
    if((int)difftime(time(0), oldtime) > MAXTIME) {
      log_msg(LOG_CRIT, "Error: No input files found in last %d seconds.\n",
        MAXTIME);
      exit(127);
    }
    else sleep(1);
  }
}

// If there is a file ready for each USB stream, open one for each.
// Returns true if this happens, and false otherwise.
static bool OpenNextFileSet()
{
  if(check_disk_space(InputDir) < 0) { // Why are we checking the *input* directory?
    log_msg(LOG_CRIT, "Fatal error in check_disk_space(%s)\n", InputDir.c_str());
    return false;
  }

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

static void BuildEvent(const DataVector & in_packets,
                       const vector<int32_t> & OutIndex, const int fd)
{
  if(fd <= 0) {
    log_msg(LOG_CRIT, "Fatal Error in BuildEvent(). Invalid file "
      "handle for previously opened data file!\n");
    exit(1);
  }

  if(in_packets.empty()){
    log_msg(LOG_WARNING, "Got empty data in BuildEvent(). Trying to continue.\n");
    return;
  }

  const std::vector<int32_t> & firstpacket = in_packets[0];
  const int32_t time_s = (firstpacket.at(1) << 24) + (firstpacket.at(2) << 16) +
                         (firstpacket.at(3) <<  8) +  firstpacket.at(4);
  OVEventHeader evheader;
  evheader.time_sec = time_s;
  evheader.n_ov_data_packets = in_packets.size();

  if(!evheader.writeout(fd)){
    log_msg(LOG_CRIT, "Fatal Error: Cannot write event header!\n");
    exit(1);
  }

  for(unsigned int packeti = 0; packeti < in_packets.size(); packeti++){
    const std::vector<int32_t> & packet = in_packets[packeti];

    if(packet.size() < 7) {
      log_msg(LOG_CRIT, "Fatal Error in BuildEvent(): packet of size %u < 7\n",
         (unsigned int) packet.size());
      exit(1);
    }

    const int module_local = (packet.at(0) >> 8) & 0x7f;
    // EBuilder temporary internal mapping is decoded back to pmtbaord_u from
    // MySQL table
    const int usb = OVUSBStream[OutIndex[packeti]].GetUSB();
    if(!PMTUniqueMap.count(usb*1000+module_local)){
      log_msg(LOG_ERR, "Got unknown module number %d on USB %d\n",
              module_local, usb);
    }
    const int16_t module = PMTUniqueMap[usb*1000+module_local];
    const int8_t type = packet.at(0) >> 15;
    const int32_t time_16ns_hi = packet.at(5);
    const int32_t time_16ns_lo = packet.at(6);
    const uint32_t time_16ns= (time_16ns_hi << 16)+time_16ns_lo;

    if(type != kOVR_ADC) {
      log_msg(LOG_CRIT, "Got non-ADC packet, type %d. Not supported!\n", type);
      exit(1);
    }

    // Sync Pulse Diagnostic Info: Sync pulse expected at clk count =
    // 2^(SYNC_PULSE_CLK_COUNT_PERIOD_LOG2).  Look for overflows in 62.5 MHz
    // clock count bit corresponding to SYNC_PULSE_CLK_COUNT_PERIOD_LOG2
    if( (time_16ns_hi >> (SYNC_PULSE_CLK_COUNT_PERIOD_LOG2 - 16)) ) {
      if(!overflow[module]) {
        log_msg(LOG_WARNING, "Module %d missed sync pulse near "
          "time stamp %ld\n", module, time_s);
        overflow[module] = true;
      }
      maxcount_16ns_lo[module] = time_16ns_lo;
      maxcount_16ns_hi[module] = time_16ns_hi;
    }
    else {
      if(overflow[module]) {
        log_msg(LOG_WARNING, "Module %d max clock count hi: %ld\tlo: %ld\n",
          module, maxcount_16ns_hi[module], maxcount_16ns_lo[module]);
        maxcount_16ns_lo[module] = time_16ns_lo;
        maxcount_16ns_hi[module] = time_16ns_hi;
        overflow[module] = false;
      }
    }

    OVDataPacketHeader moduleheader;
    // XXX Magic here.  What is 7?  Why divide by 2?  Also it's a little
    // scary that length overflows at 128. Had we better check for that?
    moduleheader.nHits = (packet.size()-7)/2;
    moduleheader.module = module;
    moduleheader.time16ns = time_16ns;

    if(!moduleheader.writeout(fd)){
      log_msg(LOG_CRIT, "Fatal Error: Cannot write data packet header!\n");
      exit(1);
    }

    for(int m = 0; m < moduleheader.nHits; m++) {
      OVHitData hit;
      hit.channel = packet.at(8+2*m);
      hit.charge = packet.at(7+2*m);

      if(!hit.writeout(fd)){
        log_msg(LOG_CRIT, "Fatal Error: Cannot write hit!\n");
        exit(1);
      }
    }
  }
}

static void parse_options(int argc, char **argv)
{
  bool option_t_used = false;
  if(argc <= 1) goto fail;

  char c;
  while((c = getopt(argc, argv, "t:T:i:o:h")) != -1) {
    switch (c) {
      case 'i': InputDir = optarg; break;
      case 'o': OutBase  = optarg; break;
      case 't': Threshold = atoi(optarg); option_t_used = true; break;
      case 'T': EBTrigMode = (TriggerMode)atoi(optarg); break;
      case 'h':
      default:  goto fail;
    }
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

  return;

  fail:
  printf("Usage: %s -i <input data directory> -o <EBuilder_output_disk>\n"
         "      [-t <offline_threshold>] [-T <offline_trigger_mode>]\n"
         "-i : Input data directory - mandatory argument\n"
         "-o : Output file - mandatory argument\n"
         "-t : offline threshold (ADC counts) to apply\n"
         "     [default: 0 (no software threshold)]\n"
         "-T : offline trigger mode\n"
         "     0: No threshold\n"
         "     1: Per-channel threshold\n"
         "     2: [default] Overlapping pair with both channels over threshold\n",
         argv[0]);
  exit(127);
}

static void CalculatePedestal(int baseptr[maxModules][numChannels],
                              const DataVector & BaselineData)
{
  double baseline[maxModules][numChannels] = {};
  int counter[maxModules][numChannels] = {};

  for(DataVector::const_iterator BaselineDataIt = BaselineData.begin();
      BaselineDataIt != BaselineData.end();
      BaselineDataIt++) {

    // Data Packets should have 7 + 2*num_hits elements
    if(BaselineDataIt->size() < 8 && BaselineDataIt->size() % 2 != 1)
      log_msg(LOG_ERR, "Fatal Error: Baseline data packet found with no data\n");

    const int module = (BaselineDataIt->at(0) >> 8) & 0x7f;
    const int type = BaselineDataIt->at(0) >> 15;

    if(type != kOVR_ADC) continue;

    if(module > maxModules){
      log_msg(LOG_CRIT, "Fatal Error: Module number requested "
        "(%d) out of range (0-%d) in calculate pedestal\n", module, maxModules);
      exit(1);
    }

    for(int i = 7; i+1 < (int)BaselineDataIt->size(); i += 2) {
      const int charge = BaselineDataIt->at(i);
      const int channel = BaselineDataIt->at(i+1); // Channels run 0-63
      if(channel >= numChannels){
        log_msg(LOG_CRIT, "Fatal Error: Channel number requested "
          "(%d) out of range (0-%d) in calculate pedestal\n",
          channel, numChannels-1);
        exit(1);
      }
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

  printf("Processing baselines...\n");

  // Set USB numbers for each OVUSBStream and load baseline files
  for(unsigned int i = 0; i < numUSB; i++) {
    // Error: all usbs should have been assigned from MySQL
    if( OVUSBStream[Datamap[i]].GetUSB() == -1 ) {
      log_msg(LOG_ERR, "Error: USB number unassigned while getting baselines\n");
      return false;
    }
    if(OVUSBStream[Datamap[i]].LoadFile(InputDir+ "/baseline") < 1)
      return false; // Load baseline file for data streams
  }

  // Decode all files and load into memory
  for(unsigned int j = 0; j < numUSB; j++){
    log_msg(LOG_INFO, "Decoding baseline %d\n", Datamap[j]),
    decode((void*) Datamap[j]);
  }

  int baselines[maxModules][numChannels] = { { } };

  for(unsigned int i = 0; i < numUSB; i++) {
    DataVector BaselineData;
    OVUSBStream[Datamap[i]].GetBaselineData(&BaselineData);
    CalculatePedestal(baselines, BaselineData);
    OVUSBStream[Datamap[i]].SetBaseline(baselines);
  }

  return true;
}

// Try to read in the baselines for MAXTIME seconds.  If they don't appear,
// exit with status 127.
static void LoadBaselineData()
{
  const time_t oldtime = time(0);
  while(!GetBaselines()){
    if((int)difftime(time(0), oldtime) > MAXTIME) {
      log_msg(LOG_CRIT, "Error: Baseline data not found in last %d seconds.\n",
        MAXTIME);
      exit(127);
    }
    sleep(2);
  }
}

static void die_with_log(const char * const format, ...)
{
  va_list ap;
  va_start(ap, format);
  log_msg(LOG_CRIT, format, ap);
  exit(127);
}


// Return a list of distict USB serial numbers for the given config table.
// Sets no globals.
static vector<int> get_distinct_usb_serials()
{
  vector<int> serials;
  // TODO: read from a config file.
  serials.push_back(21);
  serials.push_back(24);
  serials.push_back(25);
  serials.push_back(29);
  serials.push_back(6);
  return serials;
}

// Return a vector of {USB serial numbers, board numbers, pmtboard_u, time offsets}
// for all USBs in the given table.  Sets no globals.
static vector<usb_sbop> get_sbops()
{
  vector<usb_sbop> sbops;
  // from sample data. TODO: Read from a config file.
  sbops.push_back(usb_sbop(21,36,200,0));
  sbops.push_back(usb_sbop(21,37,201,0));
  sbops.push_back(usb_sbop(21,38,202,0));
  sbops.push_back(usb_sbop(21,39,203,0));
  sbops.push_back(usb_sbop(21,40,204,0));
  sbops.push_back(usb_sbop(21,41,205,0));
  sbops.push_back(usb_sbop(21,42,206,0));
  sbops.push_back(usb_sbop(21,43,207,0));
  sbops.push_back(usb_sbop(24,18,208,0));
  sbops.push_back(usb_sbop(24,19,209,0));
  sbops.push_back(usb_sbop(24,20,210,0));
  sbops.push_back(usb_sbop(24,21,211,0));
  sbops.push_back(usb_sbop(24,22,212,0));
  sbops.push_back(usb_sbop(24,23,213,0));
  sbops.push_back(usb_sbop(24,24,214,0));
  sbops.push_back(usb_sbop(24,25,215,0));
  sbops.push_back(usb_sbop(24,26,216,0));
  sbops.push_back(usb_sbop(24,27,217,0));
  sbops.push_back(usb_sbop(25,32,218,0));
  sbops.push_back(usb_sbop(25,33,219,0));
  sbops.push_back(usb_sbop(25,34,220,0));
  sbops.push_back(usb_sbop(25,35,221,0));
  sbops.push_back(usb_sbop(29,0,222,0));
  sbops.push_back(usb_sbop(29,1,223,0));
  sbops.push_back(usb_sbop(29,2,224,0));
  sbops.push_back(usb_sbop(29,3,225,0));
  sbops.push_back(usb_sbop(29,4,226,0));
  sbops.push_back(usb_sbop(29,5,227,0));
  sbops.push_back(usb_sbop(29,6,228,0));
  sbops.push_back(usb_sbop(29,7,229,0));
  sbops.push_back(usb_sbop(29,8,230,0));
  sbops.push_back(usb_sbop(29,9,231,0));
  sbops.push_back(usb_sbop(6,10,232,0));
  sbops.push_back(usb_sbop(6,11,233,0));
  sbops.push_back(usb_sbop(6,12,234,0));
  sbops.push_back(usb_sbop(6,13,235,0));
  sbops.push_back(usb_sbop(6,14,236,0));
  sbops.push_back(usb_sbop(6,15,237,0));
  sbops.push_back(usb_sbop(6,16,238,0));
  sbops.push_back(usb_sbop(6,17,239,0));
  return sbops;
}

// Returns the number of boards and the highest module number
static std::pair<int, int> board_count()
{
  return std::pair<int, int>(40, 240); // TODO: read from config file
}

// Gets the necessary run info from the run summary table, does some checking,
// and returns the values that will be used to set globals.  No globals set here.
static some_run_info get_some_run_info()
{
  some_run_info info;
  memset(&info, 0, sizeof(some_run_info));
  info.has_ebsubrun = false;
  info.has_stoptime = true;
  return info;
}

static void setup_from_config()
{
  const vector<int> usbserials = get_distinct_usb_serials();
  numUSB = usbserials.size();

  map<int, int> usbmap; // Maps USB number to numerical ordering of all USBs
  for(unsigned int i = 0; i < usbserials.size(); i++) {
    usbmap[usbserials[i]] = i;
    OVUSBStream[i].SetUSB(usbserials[i]);
  }

  // Load the time offsets for these boards
  const vector<usb_sbop> sbops = get_sbops();

  // Create map of UBS_serial to array of pmt board offsets
  for(unsigned int i = 0; i < sbops.size(); i++) {
    if(!PMTOffsets.count(sbops[i].serial))
      PMTOffsets[sbops[i].serial] = new int[maxModules];
    if(sbops[i].board < maxModules)
      PMTOffsets[sbops[i].serial][sbops[i].board] = sbops[i].offset;
  }

  for(unsigned int i = 0; i < usbserials.size(); i++)
    Datamap[i] = usbmap[usbserials[i]];

  // Count the number of boards in this setup
  const int totalboards = board_count().first;
  const int max_board   = board_count().second;
  overflow = new bool[max_board+1];
  maxcount_16ns_hi = new long int[max_board+1];
  maxcount_16ns_lo = new long int[max_board+1];
  memset(overflow, 0, (max_board+1)*sizeof(bool));
  memset(maxcount_16ns_hi, 0, (max_board+1)*sizeof(long int));
  memset(maxcount_16ns_lo, 0, (max_board+1)*sizeof(long int));

  if((int)sbops.size() != totalboards)
    die_with_log("Found duplicate pmtboard_u entries\n");

  // This is a temporary internal mapping used only by the EBuilder
  for(unsigned int i = 0; i < sbops.size(); i++)
    PMTUniqueMap[1000*sbops[i].serial+sbops[i].board] = sbops[i].pmtboard_u;

  //////////////////////////////////////////////////////////////////////
  // Get run summary information
  const some_run_info runinfo = get_some_run_info();

  for(unsigned int i = 0; i < numUSB; i++)
    OVUSBStream[Datamap[i]].SetThresh(Threshold, (int)EBTrigMode);
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
static unsigned int SuperBuildEvents(DataVector * CurrentDataVector,
                                     const int fd)
{
  // I don't know how many of these need to be static
  static DataVector::iterator CurrentDataVectorIt[maxUSB];
  static DataVector MinDataVector; // DataVector of current minimum data packets
  static vector<int32_t> MinDataPacket; // Minimum and Last Data Packets added
  static vector<int32_t> MinIndexVector; // Vector of USB index of Minimum Data Packet
  static DataVector ExtraDataVector; // DataVector carries over events from last time stamp
  static vector<int32_t> ExtraIndexVector;

  unsigned int EventCounter = 0;
  // index of minimum event added to USB stream
  int MinIndex=0;
  for(unsigned int i=0; i < numUSB; i++) {
    // MinIndex is set to the last CurrentDataVector that's empty,
    // or zero if none are empty.
    CurrentDataVectorIt[i]=CurrentDataVector[i].begin();
    if(CurrentDataVector[i].empty()) MinIndex = i;
  }
  MinDataVector.assign(ExtraDataVector.begin(), ExtraDataVector.end());
  MinIndexVector.assign(ExtraIndexVector.begin(), ExtraIndexVector.end());

  // This is an elaborate test for whether all CurrentDataVectors are
  // non-empty up to numUSB.
  while( CurrentDataVectorIt[MinIndex]!=CurrentDataVector[MinIndex].end() ) {
    // Until 1 USB stream finishes timestamp

    MinIndex=0; // Reset minimum to first USB stream
    MinDataPacket = *(CurrentDataVectorIt[MinIndex]);

    for(unsigned int k=0; k<numUSB; k++) { // Loop over USB streams, find minimum

      vector<int32_t> CurrentDataPacket = *(CurrentDataVectorIt[k]);

      // Find real minimum; no clock slew
      if( LessThan(CurrentDataPacket, MinDataPacket, 0) ) {
        // If current packet less than min packet, min = current
        MinDataPacket = CurrentDataPacket;
        MinIndex = k;
      }

    } // End of for loop: MinDataPacket has been filled appropriately

    if(MinDataVector.size() > 0) { // Check for equal events
      if( LessThan(MinDataVector.back(), MinDataPacket, 3) ) {
        // Ignore gaps which have consist of fewer than 4 clock cycles

        ++EventCounter;
        BuildEvent(MinDataVector, MinIndexVector, fd);

        MinDataVector.clear();
        MinIndexVector.clear();
      }
    }
    MinDataVector.push_back(MinDataPacket); // Add new element
    MinIndexVector.push_back(MinIndex);
    CurrentDataVectorIt[MinIndex]++; // Increment iterator for added packet

  } // End of while loop: Events have been built for this time stamp

  // Clean up operations and store data for later
  for(unsigned int k=0; k<numUSB; k++)
    CurrentDataVector[k].assign(CurrentDataVectorIt[k], CurrentDataVector[k].end());
  ExtraDataVector.assign(MinDataVector.begin(), MinDataVector.end());
  ExtraIndexVector.assign(MinIndexVector.begin(), MinIndexVector.end());

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

int main(int argc, char **argv)
{
  parse_options(argc, argv);
  setup_signals(); // so we will know when each run has ended
  start_log(); // establish syslog connection
  setup_from_config();
  LoadBaselineData();
  InitRun();

  DataVector CurrentDataVector[maxUSB]; // for current timestamp to process
  int fd = 0; // output file descriptor

  int SubRunCounter = 0;

  // This is the main Event Builder loop
  while(true) {

    // Open output data file
    if(SubRunCounter % timestampsperoutput == 0) {
      if(fd) write_end_block_and_close(fd);
      const unsigned int BUFSIZE = 1024;
      char outfile[BUFSIZE];
      snprintf(outfile, BUFSIZE, "%s_%05d",
        OutBase.c_str(), SubRunCounter/timestampsperoutput);
      fd = open_file(outfile);
    }

    // XXX this is a loop over numUSB, but within it, all USB streams
    // are read on every iteration.  What's going on?
    for(unsigned int i=0; i < numUSB; i++) {
      printf("Doing big loop for USB index %d\n", i);

      // Until we have a new time stamp on USB i, keep trying to load up and
      // decode a whole new set of files
      while(!OVUSBStream[i].GetNextTimeStamp(&(CurrentDataVector[i]))) {
        const time_t oldtime = time(0);

        while(!OpenNextFileSet()){ // Try to find new files for each USB

          if(((int)difftime(time(0), oldtime) > ENDTIME && run_has_ended)
           || (int)difftime(time(0), oldtime) > MAXTIME) {

            if(run_has_ended)
              log_msg(LOG_INFO, "Event Builder has finished processing run\n");
            else
              log_msg(LOG_ERR, "No new files for %ds, but I didn't hear that "
                "the run was over! Closing run anyway.\n", MAXTIME);

            run_has_ended = false; // reset for next run
            goto out; // break 3
          }
          log_msg(LOG_INFO, "Files are not ready. Waiting...\n");
          sleep(1);
        }

        pthread_t threads[numUSB]; // An array of threads to decode files

        for(unsigned int j = 0; j < numUSB; j++) // Load all files in at once
          pthread_create(&threads[j], NULL, decode, (void*) j);

        for(unsigned int j = 0; j < numUSB; j++)
          pthread_join(threads[j], NULL);

        rename_files_we_have_read();
      }
    }
    out:

    // Advance all of these to the end.  Experimentally, it seems necessary.
    for(unsigned int i = 0; i < numUSB; i++)
      OVUSBStream[i].GetNextTimeStamp(&(CurrentDataVector[i]));

    // We don't actually write out any data until we've failed to find
    // new files for a while or it's been declared that the run is over?
    // That seems like it might be problematic.
    //
    // Not sure why we advance the subrun counter if and only if this
    // has happened, either.

    const unsigned int EventCounter = SuperBuildEvents(CurrentDataVector, fd);
    ++SubRunCounter;

    if((SubRunCounter % timestampsperoutput == 0) && fd) {
      if(close(fd) < 0) {
        log_msg(LOG_CRIT, "Fatal Error: Could not close output data file!\n");
        return 127;
      }
    }

    log_msg(LOG_INFO, "Number of built events: %d\nProcessed time stamp: %d\n",
            EventCounter, OVUSBStream[0].GetTOLUTC());
    write_end_block_and_close(fd);
    break; // XXX get out for testing
  }

  log_msg(LOG_WARNING, "Normally this program should not terminate like this...\n");

  return 0;
}
