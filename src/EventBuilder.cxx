#include "USBstream.h"
#include "USBstream-TypeDef.h"
#include "USBstreamUtils.h"

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <vector>
#include "TThread.h"
#include <sys/types.h>

#include <string.h>
#include <time.h>
#include <algorithm>
#include <sys/statvfs.h>
#include <mysql++.h>

using namespace std;

const int maxUSB=10; // Maximum number of USB streams
const int latency=5; // Number of seconds before DAQ switches files. FixME: 5 anticipated for far detector
const int timestampsperoutput = 5;
const int numChannels=64; // Number of channels in M64
const int maxModules=64; // Maximum number of modules PER USB (okay if less than total number of modules)
const int MAXTIME=60; // Number of seconds before time out looking for baselines and binary data
const int ENDTIME=5; // Number of seconds before time out at end of run
const int SYNCPULSECUT=6; // Max time interval in seconds between sync pulses before throwing error
const int SYNC_PULSE_CLK_COUNT_PERIOD_LOG2=29; // DC trigger system emits sync pulse at 62.5 MHz clock count = 2^29

USBstream OVUSBStream[maxUSB]; // An array of USBstream objects
TThread *th[maxUSB]; // An array of threads to decode files
TThread *tp; // Joiner thread for decoding
bool *ovf; // Keeps track of sync overflows for all boards
long int *maxcount_16ns_lo; // Keeps track of max clock count for sync overflows for all boards
long int *maxcount_16ns_hi; // Keeps track of max clock count for sync overflows for all boards
bool finished=false; // Flag for joiner thread
bool sem_inc_success; // Flag for dynamic semaphore adjustment

vector<std::string> files; // vector to hold file names
map<int,int> USBmap; // Maps USB number to numerical ordering of all USBs
map<int,int> Datamap; // Maps numerical ordering of non-Fan-in USBs to all numerical ordering of all USBs
map<int,int> PMTUniqueMap; // Maps 1000*USB_serial + board_number to pmtboard_u in MySQL table
std::map<int,int*> myoffsets; // Map to hold offsets for each PMT board

//std::string DataFolder = "/snap/data1/OVDAQ/DATA"; // Path to data hard-coded
std::string DataFolder; // = "OVDAQ/DATA"; // Path to data hard-coded
std::string OutputDir;
std::string OutputFolder; // = "/data1/OVDAQ/DATA/"; // Default output data path hard-coded
std::string ReprocessFolder; // = "/data1/OVDAQ/REP/"; // Default reprocess folder path hard-coded
std::string sname = "/var/tmp/OV_EBuilder.txt"; // Default ftok semaphore file name
std::string RunNumber = "";
std::string OVRunType = "P";
std::string OVDAQHost = "dcfovdaq";
long int lastunixtimestamp[maxModules] = {0};
long int EBcomment = 0;
int SubRunCounter = 0;
int Disk=2; // default location of OV DAQ data: /data2
int OutDisk=1; // default output location of OV Ebuilder: /data1
int Threshold = 73; //default 1.5 PE threshold
int Res1 = 0;
int Res2 = 0;
bool Repeat = false;
int DOGSifier = 1;
//int OFFLINE = 0;
int initial_delay = 0;
int OV_EB_State = 0;
int Ddelay = 0;
int numUSB = 0;
int numFanUSB = 0;
int totalboards = 0;
int totalPMTboards = 0;
int totalFanboards = 0;
char server[BUFSIZE] = {0};
char username[BUFSIZE] = {0};
char password[BUFSIZE] = {0};
char database[BUFSIZE] = {0};

enum RunMode {kNormal, kRecovery, kReprocess};
RunMode EBRunMode = kNormal;
enum TriggerMode {kNone, kSingleLayer, kDoubleLayer};
TriggerMode EBTrigMode = kDoubleLayer; // double-layer threshold

void *handle(void *ptr); // This defines a thread to decode files
void *joiner(void *ptr); // This thread joins the above threads
int LoadAll(std::string dir); // Loads files
bool LoadRun(std::string &mydatadir,std::string outdir); // Loads files
void BuildEvent(DataVector *OutDataVector, std::vector<int> *OutIndexVector, int mydataFile); // Builds events
int open_file(std::string name);
int check_options(int argc, char **argv);
void start_gaibu();
int check_disk_space(std::string dir);
void check_status();
bool GetBaselines();
void CalculatePedestal(DataVector* BaselineData, int **baseptr);
bool WriteBaselineTable(int **baseptr, int usb);
bool read_summary_table();
bool write_summary_table(long int lastutc, int subrunnumber);
bool read_stop_time();
bool write_endofrun_block(std::string file, int fd);
bool write_ebsummary();
bool write_ebretval(int val);

int main(int argc, char **argv)
{
  if(argc <= 1) {
    printf("Usage: ./EventBuilder -r <run_number> [opt args]\n");
    printf("For help: /EventBuilder -h\n");
    write_ebretval(-1);
    return -1;
  }

  int r = check_options(argc, argv);
  if(r < 0) { write_ebretval(-1); return -1; }

  DataVector ExtraDataVector; // DataVector carries over events from last time stamp
  std::vector<int> ExtraIndexVector;
  DataVector CurrentDataVector[maxUSB]; // Array of DataVectors for current timestamp to process
  DataVectorIt CurrentDataVectorIt[maxUSB]; // Array of iterators for current DataVectors
  DataVector MinDataVector; // DataVector of current minimum data packets
  DataPacket MinDataPacket; // Minimum and Last Data Packets added
  std::vector<int> MinIndexVector; // Vector of USB index of Minimum Data Packet
  int MinIndex; // index of minimum event added to USB stream
  int sem_id = 0; // output file DOGSifier synchronization semaphore
  int dataFile = 0; // output file descriptor
  std::string fname; // output file name
  std::string iname; // input file name
  time_t timeout;
  std::string tempfilename;
  int EventCounter = 0;
  bool first_time = true;
  int status = 0;

  start_gaibu(); // establish gaibu connections

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Load OV run_summary table
  // This should handle reprocessing eventually
  if(read_summary_table() == false) {
    sprintf(gaibu_debug_msg,"Fatal Error while loading OV_runsummary table.");
    gaibu_msg(MEXCEPTION,gaibu_debug_msg,RunNumber);
    printf("Fatal Error while loading OV_runsummary table.\n");
    write_ebretval(-1);
    return -1;
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Load baseline data
  timeout = time(0);
  while(GetBaselines()==false) { // Get baselines
    if((int)difftime(time(0),timeout) > MAXTIME) {
      sprintf(gaibu_debug_msg,"Error: Baseline data not found in the last %d seconds.",MAXTIME);
      gaibu_msg(MEXCEPTION,gaibu_debug_msg,RunNumber);
      printf("Error: Baseline data not found in the last %d seconds.\n",MAXTIME);
      //timeout = time(0);
      write_ebretval(-1);
      return -1;
    }
    /*
      if(OFFLINE) {
      sprintf(gaibu_debug_msg,"Could not load baseline data in offline mode.");
      gaibu_msg(MERROR,gaibu_debug_msg);
      printf("Could not load baseline data in offline mode.");
      return -1;
    }
    */
    else sleep(2); // FixME: optimize?
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Set Thresholds
  for(int i = 0; i<numUSB-numFanUSB; i++) {
    OVUSBStream[Datamap[i]].SetThresh(Threshold,(int)EBTrigMode); // Set threshold only for data streams
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Locate existing binary data and create output folder
  OutputFolder = OutputFolder + "Run_" + RunNumber;
  timeout = time(0);

  while(LoadRun(iname, OutputFolder) == false) {
    if((int)difftime(time(0),timeout) > MAXTIME) {
      sprintf(gaibu_debug_msg,"Error: Binary data not found in the last %d seconds.",MAXTIME);
      gaibu_msg(MEXCEPTION,gaibu_debug_msg,RunNumber);
      printf("Error: Binary data not found in the last %d seconds.\n",MAXTIME);
      //timeout = time(0);
      write_ebretval(-1);
      return -1;
    }
    /*
    if(OFFLINE) {
      sprintf(gaibu_debug_msg,"Could not load run in offline mode. Exiting.");
      gaibu_msg(MERROR,gaibu_debug_msg);
      printf("Could not load run in offline mode. Exiting.");
      return -1;
    }
    */
    else sleep(1);
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Create DOGSifier semaphores
  if(DOGSifier) {
    //sem_id = create_semaphore('S',sname); // Create semaphore
    sem_id = create_semaphore('S' + atoi(RunNumber.c_str())); // Create semaphore
    //init_semaphore(sem_id,0); // Do not initialize semaphore to 0
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // This is the main Event Builder loop
  while(true) {

    for(int i=0; i < numUSB; i++) {

      DataVector *DataVectorPtr = &(CurrentDataVector[i]);

      while( OVUSBStream[i].GetNextTimeStamp(DataVectorPtr) == false) {
        timeout = time(0);

        while( (status = LoadAll(iname)) < 1 ) { // Try to find new files for each USB
          if(status == -1) {
            /*
            sprintf(gaibu_debug_msg,"Fatal error in data. Exiting.");
            gaibu_msg(MERROR,gaibu_debug_msg);
            printf("Fatal error in data. Exiting.");
            */
            write_ebretval(-1);
            return -1;
          }
          cout << "Files are not ready...\n";
          //cout << "Time diff: " << (int)difftime(time(0),timeout) << endl;
          if((int)difftime(time(0),timeout) > ENDTIME) {
            if(read_stop_time() == true || (int)difftime(time(0),timeout) > MAXTIME) {
              while(write_endofrun_block(fname, dataFile) == false) { sleep(1); }
              // Increment DOGSifier semaphore
              bool repeat_sem = true; int repeat_sem_cnt = 0;
              while(repeat_sem) {
                repeat_sem = false;
                repeat_sem_cnt++;
                if(DOGSifier) {
                  try{ increase_semaphore(sem_id); }
                  catch(sem_inc_except & e) {
                    if(repeat_sem_cnt <= 10) { repeat_sem = true; sleep(1); }
                    else {
                      DOGSifier = 0;
                      //cout<<e.what()<<endl;
                      sprintf(gaibu_debug_msg,"Could not increase semaphore--working in offline mode");
                      gaibu_msg(MWARNING,gaibu_debug_msg);
                      printf("Could not increase semaphore--working in offline mode\n");
                    }
                  }
                  if(DOGSifier) {
                    cout << "Semaphore value: " << get_semaphore_value(sem_id) << endl;
                  }
                }
              }
              //while(write_ebsummary() == false) { sleep(1); } // timeout?
              if((int)difftime(time(0),timeout) > MAXTIME) {
                sprintf(gaibu_debug_msg,"No data found for %d seconds!  Closing run %s without finding stop time on MySQL",MAXTIME, RunNumber.c_str());
                gaibu_msg(MERROR,gaibu_debug_msg,RunNumber);
                printf("No data found for %d seconds!  Closing run %s without finding stop time on MySQL\n",MAXTIME, RunNumber.c_str());
              } else {
                sprintf(gaibu_debug_msg,"OV Event Builder has finished processing run %s",RunNumber.c_str());
                //gaibu_msg(MNOTICE,gaibu_debug_msg,RunNumber);
                printf("OV Event Builder has finished processing run %s\n",RunNumber.c_str());
              }
              write_ebretval(1);
              return 0;
            }
          }

      sleep(1); // FixMe: optimize?
    }

    for(int j=0; j<numUSB; j++) { // Load all files in at once
      printf("Starting Thread %d\n",j);
      th[j] = new TThread(Form("th%d",j), handle, (void*) j);
      th[j]->Run();
    }

    tp = new TThread("tp", joiner, (void*) numUSB);
    tp->Run();

    while(!finished) { sleep(1); } // To be optimized
    finished = false;

    tp->Join();

    for(int k=0; k<numUSB; k++) {
      if(th[k]) delete th[k];
    }
    if(tp) delete tp;

    cout << "Joined all threads!\n";

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Rename file names
    for(int i = 0; i<numUSB; i++) {
      tempfilename = OVUSBStream[i].GetFileName();
      size_t mypos = tempfilename.find("binary");
      if(mypos != tempfilename.npos) {
        tempfilename.replace(mypos,6,"decoded");
      }
      tempfilename += ".done";

      while(rename(OVUSBStream[i].GetFileName(),tempfilename.c_str())) {
        sprintf(gaibu_debug_msg,"Could not rename binary data file.");
        gaibu_msg(MERROR,gaibu_debug_msg);
        printf(gaibu_debug_msg,"Could not rename binary data file.\n");
        sleep(1);
      }
    }

      } // End of while(OVUSBStream[i].GetNextTimeStamp==false) loop
    } // End of for loop: data has been loaded into CurrentDataVec properly

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Open output data file
    // This should handle re-processing eventually
    //fname = OutputFolder + "/" + OVUSBStream[0].GetTOLUTC();
    if(EBRunMode == kRecovery) {
      if(OVUSBStream[0].GetTOLUTC() <= (unsigned long int)EBcomment) {
        printf("Time stamp to process: %ld\n",OVUSBStream[0].GetTOLUTC());
        printf("Recovery mode waiting to exceed time stamp %ld\n",EBcomment);
        dataFile = open_file("/dev/null");
      }
      else {
        printf("Run has been recovered!\n");
        EBRunMode = kNormal;
      }
    }
    if(EBRunMode != kRecovery && SubRunCounter % timestampsperoutput == 0) {
      fname = OutputFolder + "/DCRunF" + RunNumber;
      char subrun[BUFSIZE];
      sprintf(subrun,"%s%.5dOVDAQ", OVRunType.c_str(), (SubRunCounter/timestampsperoutput));
      fname.append(subrun);
      dataFile = open_file(fname);
      // Increment DOGSifier semaphore here
      bool repeat_sem2 = true; int repeat_sem_cnt2 = 0;
      while(repeat_sem2) {
        repeat_sem2 = false;
        repeat_sem_cnt2++;
        if(DOGSifier && !first_time) {
          try{ increase_semaphore(sem_id); }
          catch(sem_inc_except & e) {
            if(repeat_sem_cnt2 <= 10) { repeat_sem2 = true; sleep(1); }
            else {
              DOGSifier = 0;
              //cout<<e.what()<<endl;
              sprintf(gaibu_debug_msg,"Could not increase semaphore--working in offline mode");
              gaibu_msg(MWARNING,gaibu_debug_msg);
              printf("Could not increase semaphore--working in offline mode\n");
            }
          }
          if(DOGSifier) {
            cout << "Semaphore value: " << get_semaphore_value(sem_id) << endl;
          }
        }
        first_time = false;
      }
      /*
      if(DOGSifier && !first_time) {
        try{ increase_semaphore(sem_id); }
        catch(sem_inc_except & e) {
          //cout<<e.what()<<endl;
          sprintf(gaibu_debug_msg,"Could not increase semaphore--working in offline mode");
          gaibu_msg(MEXCEPTION,gaibu_debug_msg,RunNumber);
          printf("Could not increase semaphore--working in offline mode\n");
          DOGSifier = 0;
        }
        if(DOGSifier) {
          cout << "Semaphore value: " << get_semaphore_value(sem_id) << endl;
        }
      }
      first_time = false;
      */
      // Increment DOGSifier semaphore here
    }

    MinIndex=0;
    for(int i=0; i < numUSB; i++) {
      CurrentDataVectorIt[i]=CurrentDataVector[i].begin();
      if(CurrentDataVectorIt[i]==CurrentDataVector[i].end()) { MinIndex = i; }
    }
    MinDataVector.assign(ExtraDataVector.begin(),ExtraDataVector.end());
    MinIndexVector.assign(ExtraIndexVector.begin(),ExtraIndexVector.end());

    while( CurrentDataVectorIt[MinIndex]!=CurrentDataVector[MinIndex].end() ) {
      // Until 1 USB stream finishes timestamp

      MinIndex=0; // Reset minimum to first USB stream
      MinDataPacket = *(CurrentDataVectorIt[MinIndex]);

      for(int k=0; k<numUSB; k++) { // Loop over USB streams, find minimum

        DataPacket CurrentDataPacket = *(CurrentDataVectorIt[k]);

        if( LessThan(CurrentDataPacket,MinDataPacket,0) ) { // Find real minimum; no clock slew
          // If current packet less than min packet, min = current
          MinDataPacket = CurrentDataPacket;
          MinIndex = k;
        }

      } // End of for loop: MinDataPacket has been filled appropriately

      if(MinDataVector.size() > 0) { // Check for equal events
        if( LessThan(MinDataVector.back(), MinDataPacket,3) ) { // Ignore gaps which have consist of fewer than 4 clock cycles

          ++EventCounter;
          BuildEvent(&MinDataVector, &MinIndexVector, dataFile);

          //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
          // Increase semaphore ONCE PER EVENT (not used for now)
          /*
          if(DOGSifier) {
            try{ increase_semaphore(sem_id); }
            catch(sem_inc_except & e) {
              //cout<<e.what()<<endl;
              sprintf(gaibu_debug_msg,"Could not increase semaphore--working in offline mode");
              gaibu_msg(MERROR,gaibu_debug_msg);
              printf("Could not increase semaphore--working in offline mode\n");
              DOGSifier = 0;
            }
            if(DOGSifier) {
              cout << "Semaphore value: " << get_semaphore_value(sem_id) << endl;
            }
          }
          */

          MinDataVector.clear();
          MinIndexVector.clear();
        }
      }
      MinDataVector.push_back(MinDataPacket); // Add new element
      MinIndexVector.push_back(MinIndex);
      CurrentDataVectorIt[MinIndex]++; // Increment iterator for added packet

    } // End of while loop: Events have been built for this time stamp

    // Clean up operations and store data for later
    for(int k=0; k<numUSB; k++) {
      CurrentDataVector[k].assign(CurrentDataVectorIt[k],CurrentDataVector[k].end());
    }
    ExtraDataVector.assign(MinDataVector.begin(),MinDataVector.end());
    ExtraIndexVector.assign(MinIndexVector.begin(),MinIndexVector.end());

    if(EBRunMode == kRecovery) {
      int cl = close(dataFile);
      if(cl<0) {
        sprintf(gaibu_debug_msg,"Fatal Error: Could not close output data file in recovery mode!");
        gaibu_msg(MERROR,gaibu_debug_msg,RunNumber);
        printf("Fatal Error: Could not close output data file in recovery mode!\n");
        write_ebretval(-1);
        return -1;
      }
    }
    else {
      ++SubRunCounter;
      if((SubRunCounter % timestampsperoutput == 0) && dataFile) {
        int cl = close(dataFile);
        if(cl<0) {
          sprintf(gaibu_debug_msg,"Fatal Error: Could not close output data file!");
          gaibu_msg(MEXCEPTION,gaibu_debug_msg,RunNumber);
          printf("Fatal Error: Could not close output data file!\n");
          write_ebretval(-1);
          return -1;
        }
        else {
          //cout << "Finished file for subrun: " << (SubRunCounter/timestampsperoutput) << endl;
          while(write_summary_table(OVUSBStream[0].GetTOLUTC(),(SubRunCounter/timestampsperoutput))==false) {
            sprintf(gaibu_debug_msg,"Error writing to OV_runsummary table.");
            gaibu_msg(MNOTICE,gaibu_debug_msg);
            printf("Error writing to OV_runsummary table.\n");
            sleep(1);
          }
        }
        //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // Increase semaphore ONCE PER FILE
        //if(DOGSifier) {
        //  try{ increase_semaphore(sem_id); }
        //  catch(sem_inc_except & e) {
        //    //cout<<e.what()<<endl;
        //    sprintf(gaibu_debug_msg,"Could not increase semaphore--working in offline mode");
        //    gaibu_msg(MERROR,gaibu_debug_msg);
        //    printf("Could not increase semaphore--working in offline mode\n");
        //    DOGSifier = 0;
        //  }
        //  if(DOGSifier) {
        //    cout << "Semaphore value: " << get_semaphore_value(sem_id) << endl;
        //  }
        //}
      }
    }

    cout << "Number of Merged Muon Events: " << EventCounter << endl;
    cout << "Processed Time Stamp: " << OVUSBStream[0].GetTOLUTC() << endl;
    EventCounter = 0;
  } // End of while(true)loop

  cout << "In normal operation this program should not terminate like it is now...\n";

  write_ebretval(1);
  return 0;

} // End of main program



void *handle(void *ptr) // This defines a thread to decode files
{
  long int usb = (long int) ptr;
  while(!OVUSBStream[(int)usb].decode()) { usleep(100); }
  return 0;
}

void *joiner(void *ptr) // This thread joins the above threads
{
  long int nusb = (long int) ptr;
  if((int)nusb < numUSB) {
    for(int n=0; n<(int)nusb; n++) { th[Datamap[n]]->Join(); }
  }
  else { for(int n=0; n<numUSB; n++) { th[n]->Join(); } }
  finished = true;
  return 0;
}

// opens output data file
int open_file(std::string name)
{
  int temp_dataFile = open(name.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
  //cout << "File Name: "<< name <<endl;
  if ( temp_dataFile >= 0 ) {
    //cout << "file"<< name <<" open " <<endl;
  }
  else {
    sprintf(gaibu_debug_msg,"Fatal Error: failed to open file %s",name.c_str());
    gaibu_msg(MEXCEPTION,gaibu_debug_msg,RunNumber);
    printf("Fatal Error: failed to open file %s\n",name.c_str());
    write_ebretval(-1);
    exit(1);
  }
  return temp_dataFile;
}

int check_disk_space(std::string dir)
{
  struct statvfs fiData;
  int free_space_percent;
  if((statvfs(dir.c_str(),&fiData)) < 0 ) {
    //cout << "Failed to stat " << dir << endl;
    sprintf(gaibu_debug_msg,"Error: Failed to stat %s",dir.c_str());
    gaibu_msg(MNOTICE, gaibu_debug_msg);
    printf("Error: Failed to stat %s",dir.c_str());
    return 0;
  }
  else {
    if(fiData.f_blocks) {
      free_space_percent = (int)(100*(double)fiData.f_bfree/(double)fiData.f_blocks);
      //cout << "Free space: " << free_space_percent << "%\n";
      if(free_space_percent < 3) {
        sprintf(gaibu_debug_msg,"Error: Can't write to disk: Found disk >97 percent full: statvfs called on %s",dir.c_str());
        gaibu_msg(MERROR, gaibu_debug_msg);
        return -1;
      }
    }
    else {
      sprintf(gaibu_debug_msg,"Error: statvfs failed to read blocks in %s",dir.c_str());
      gaibu_msg(MERROR, gaibu_debug_msg);
      return -1;
    }
  }
  return 0;
}

// FixME: To be optimized
void check_status()
{
  // Performancs monitor
  cout << "Found " << files.size() << " files." << endl;
  int f_delay = (int)(latency*files.size()/numUSB/20);
  if(f_delay != OV_EB_State) {
    if(f_delay > OV_EB_State) {
      if(OV_EB_State <= initial_delay) { // OV EBuilder was not already behind
        sprintf(gaibu_debug_msg,"Falling behind processing files");
        gaibu_msg(MNOTICE, gaibu_debug_msg);
      }
      else { // OV EBuilder was already behind
        Ddelay = f_delay - initial_delay;
        if(Ddelay % 3 == 0) { // Every minute of delay
          Ddelay = Ddelay/3;
          sprintf(gaibu_debug_msg,"Process has accumulated %d min of delay since starting",Ddelay);
          gaibu_msg(MNOTICE, gaibu_debug_msg);
        }
      }
    }
    else if(f_delay < OV_EB_State) {
      if(OV_EB_State >= initial_delay) {
        sprintf(gaibu_debug_msg,"Catching up processing files");
        gaibu_msg(MNOTICE, gaibu_debug_msg);
      }
      else {
        Ddelay = initial_delay - f_delay;
        if(Ddelay % 3 == 0) { // Every minute of recovery
          Ddelay = Ddelay/3;
          sprintf(gaibu_debug_msg,"Process has reduced data processing delay by %d min since starting",Ddelay);
          gaibu_msg(MNOTICE, gaibu_debug_msg);
        }
      }
    }
    OV_EB_State = f_delay;
  }
}

// Just check that it exists
bool LoadRun(std::string &datadir, std::string outdatadir)
{
  datadir = DataFolder + "/Run_" + RunNumber + "/binary";

  files.clear();
  if(GetDir(datadir, files)) {
    if(errno) {
      sprintf(gaibu_debug_msg,"Error(%d) opening directory %s",errno,datadir.c_str());
      gaibu_msg(MEXCEPTION, gaibu_debug_msg);
      write_ebretval(-1);
      exit(1);
      //printf("Error(%d) opening directory %s\n",errno,datadir.c_str());
    }
    return false;
  }
  else {
    initial_delay = (int)(latency*files.size()/numUSB/20);
    OV_EB_State = initial_delay;
  }


  sort(files.begin(),files.end());
  //fname_begin = files.begin();

  std::string TempProcessedOutput = OutputFolder + "/processed";
  umask(0);
  if(mkdir(OutputFolder.c_str(), 0777)) { //O_CREAT??
    if(EBRunMode != kRecovery) {
    //if(EBRunMode == kNormal) {
      sprintf(gaibu_debug_msg,"Error creating output file %s",OutputFolder.c_str());
      gaibu_msg(MEXCEPTION, gaibu_debug_msg);
      printf("Error creating output file %s\n",OutputFolder.c_str());
      write_ebretval(-1);
      exit(1);
    }
  }
  else if(mkdir(TempProcessedOutput.c_str(), 0777)) { //O_CREAT??
    if(EBRunMode != kRecovery) {
      sprintf(gaibu_debug_msg,"Error creating output file %s",TempProcessedOutput.c_str());
      gaibu_msg(MEXCEPTION, gaibu_debug_msg);
      printf("Error creating output file %s\n",TempProcessedOutput.c_str());
      write_ebretval(-1);
      exit(1);
    }
  }
  else { // FixMe: To optimize (logic)
    if(EBRunMode == kRecovery) {
      sprintf(gaibu_debug_msg,"Output directories did not already exist in recovery mode.");
      gaibu_msg(MEXCEPTION, gaibu_debug_msg);
      printf("Error. Output directories did not already exist in recovery mode.");
      write_ebretval(-1);
      exit(1);
    }
  }

  return true;
}

// This function checks to see if files are ready to be processed
int LoadAll(std::string dir)
{

  int r = check_disk_space(dir);
  if(r < 0) {
    sprintf(gaibu_debug_msg,"Fatal error in check_disk_space(%s)",dir.c_str());
    gaibu_msg(MEXCEPTION, gaibu_debug_msg, RunNumber);
    printf("Fatal error in check_disk_space(%s)\n",dir.c_str());
    return r;
  }

  if(EBRunMode != kReprocess) {
    if((int)files.size() <= 3*numUSB) { // FixME: Is 2*numUSB sufficient to guaruntee a match?
      files.clear();
      if(GetDir(dir,files))
        return 0;
      //sort(files.begin(),files.end());
    }
  }
  if((int)files.size() < numUSB)
    return 0;

  sort(files.begin(),files.end()); // FixME: Is it safe to avoid this every time?

  vector<std::string>::iterator fname_begin=files.begin();

  check_status(); // Performance monitor

  std::string fdelim = "_"; // Assume files are of form xxxxxxxxx_xx
  size_t fname_it_delim;
  std::string ftime_min;
  if(fname_begin->find(fdelim) == fname_begin->npos) {
    sprintf(gaibu_debug_msg,"Fatal Error: Cannot find '_' in input file name");
    gaibu_msg(MEXCEPTION, gaibu_debug_msg,RunNumber);
    printf("Fatal Error: Cannot find '_' in input file name\n");
    return -1;
  }

  fname_it_delim = fname_begin->find(fdelim);
  //fname_begin = fname_curr; //files.begin();
  std::string temp3;
  std::string fusb;
  char *pEnd;
  for(int k = 0; k<numUSB; k++) {
    for(int j = k; j<(int)files.size(); j++) {
      fusb = (files[j]).substr(fname_it_delim+1,(files[j]).npos);
      if(strtol(fusb.c_str(),&pEnd,10) == OVUSBStream[k].GetUSB()) {
        temp3 = files[j];
        files[j] = files[k];
        files[k] = temp3;
        break;
      }
      if(j==(int)(files.size()-1)) { // Failed to find a file for USB k
        sprintf(gaibu_debug_msg,"USB %d data file not found",OVUSBStream[k].GetUSB());
        gaibu_msg(MWARNING, gaibu_debug_msg,RunNumber);
        printf("USB %d data file not found\n",OVUSBStream[k].GetUSB());
        files.clear();
        return 0;
      }
    }
  }

  fname_begin = files.begin();

  int status = 0;
  std::string base_filename;
  for(int k=0; k<numUSB; k++) {
    fname_it_delim = fname_begin->find(fdelim);
    ftime_min = fname_begin->substr(0,fname_it_delim);
    fusb = fname_begin->substr(fname_it_delim+1,fname_begin->npos);
    if(OVUSBStream[k].GetUSB() == -1) { // Error: All usbs should have been assigned by MySQL
        sprintf(gaibu_debug_msg,"Fatal Error: USB number unassigned");
        gaibu_msg(MEXCEPTION, gaibu_debug_msg, RunNumber);
        printf("Fatal Error: USB number unassigned\n");
        return -1;
      //OVUSBStream[k].SetUSB(strtol(fusb.c_str(),&pEnd,10)); // Set it
    }
    else { // Check to see that USB numbers are aligned
      if(OVUSBStream[k].GetUSB() != strtol(fusb.c_str(),&pEnd,10)) {
        sprintf(gaibu_debug_msg,"Fatal Error: USB number misalignment");
        gaibu_msg(MEXCEPTION, gaibu_debug_msg, RunNumber);
        printf("Fatal Error: USB number misalignment\n");
        return -1;
      }
    }
    // Build input filename ( _$usb will be added by LoadFile function )
    base_filename=dir;
    base_filename.append("/");
    base_filename.append(ftime_min);
    if( (status = OVUSBStream[k].LoadFile(base_filename)) < 1 ) { // Can't load file
      return status;
    }
    fname_begin++; // Increment file name iterator
  }

  files.assign(fname_begin,files.end());
  //fname_curr = fname_begin; // Update position of iterator
  return 1;
}

void BuildEvent(DataVector *OutDataVector, std::vector<int> *OutIndexVector, int mydataFile)
{

  int k, nbs, length, module, type, nwords, module_local, usb;
  char channel;
  short int charge;
  long int time_s_hi = 0;
  long int time_s_lo = 0;
  long int time_16ns_hi = 0;
  long int time_16ns_lo = 0;
  DataVectorIt CurrentOutDataVectorIt = OutDataVector->begin();
  std::vector<int>::iterator CurrentOutIndexVectorIt = OutIndexVector->begin();
  OVEventHeader *CurrEventHeader = new OVEventHeader;
  OVDataPacketHeader *CurrDataPacketHeader = new OVDataPacketHeader;
  OVHitData *CurrHit = new OVHitData;
  int cnt=0;

  if(mydataFile <= 0) {
    sprintf(gaibu_debug_msg,"Fatal Error in Build Event. Invalid file handle for previously opened data file!\n");
    gaibu_msg(MEXCEPTION, gaibu_debug_msg,RunNumber);
    printf("Fatal Error in Build Event. Invalid file handle for previously opened data file!\n");
    write_ebretval(-1);
    exit(1);
  }

  while( CurrentOutDataVectorIt != OutDataVector->end() ) {

    if(CurrentOutDataVectorIt->size() < 7) {
      sprintf(gaibu_debug_msg,"Fatal Error in Build Event: Vector of data found with too few words..");
      gaibu_msg(MEXCEPTION, gaibu_debug_msg,RunNumber);
      printf("Fatal Error in Build Event: Vector of data found with too few words..");
      write_ebretval(-1);
      exit(1);
    }
    else {
      //std::cout << "Size: " << CurrentOutDataVectorIt->size() << std::endl;
    }

    if(CurrentOutDataVectorIt == OutDataVector->begin()) { // First packet in built event
      time_s_hi = (CurrentOutDataVectorIt->at(1) << 8) + CurrentOutDataVectorIt->at(2);
      time_s_lo = (CurrentOutDataVectorIt->at(3) << 8) + CurrentOutDataVectorIt->at(4);
      CurrEventHeader->SetTimeSec(time_s_hi*65536 + time_s_lo);
      CurrEventHeader->SetNOVDataPackets(OutDataVector->size());

      nbs = write(mydataFile, CurrEventHeader, sizeof(OVEventHeader)); // fOVEvent is a pointer to object
      if (nbs<0){
        sprintf(gaibu_debug_msg,"Fatal Error: Cannot write event header to disk!");
        gaibu_msg(MEXCEPTION, gaibu_debug_msg, RunNumber);
        printf("Fatal Error: Cannot write event header to disk!\n");
        write_ebretval(-1);
        exit(1);
      } // To be optimized

    }

    nwords = (CurrentOutDataVectorIt->at(0) & 0xff) - 1;
    module_local = (CurrentOutDataVectorIt->at(0) >> 8) & 0x7f;
    // EBuilder temporary internal mapping is decoded back to pmtbaord_u from MySQL table
    usb = OVUSBStream[*CurrentOutIndexVectorIt].GetUSB();
    module = PMTUniqueMap[usb*1000+module_local];
    type = CurrentOutDataVectorIt->at(0) >> 15;
    time_16ns_hi = CurrentOutDataVectorIt->at(5);
    time_16ns_lo = CurrentOutDataVectorIt->at(6);

    // Sync Pulse Diagnostic Info: Sync pulse expected at clk count = 2^(SYNC_PULSE_CLK_COUNT_PERIOD_LOG2)
    // Look for overflows in 62.5 MHz clock count bit corresponding to SYNC_PULSE_CLK_COUNT_PERIOD_LOG2
    if( (time_16ns_hi >> (SYNC_PULSE_CLK_COUNT_PERIOD_LOG2 - 16)) ) {
      if(!ovf[module]) {
        sprintf(gaibu_debug_msg,"Module %d missed sync pulse near time stamp %ld",module,(time_s_hi*65536+time_s_lo));
        gaibu_msg(MWARNING, gaibu_debug_msg);
        printf(gaibu_debug_msg,"Module %d missed sync pulse near time stamp %ld \n",module,(time_s_hi*65536+time_s_lo));
        ovf[module] = true;
        maxcount_16ns_lo[module] = time_16ns_lo;
        maxcount_16ns_hi[module] = time_16ns_hi;
      }
      else {
        maxcount_16ns_lo[module] = time_16ns_lo;
        maxcount_16ns_hi[module] = time_16ns_hi;
      }
    }
    else {
      if(ovf[module] == true) {
        sprintf(gaibu_debug_msg,"Module %d max clock count hi: %ld\tlo: %ld",module,maxcount_16ns_hi[module], maxcount_16ns_lo[module]);
        gaibu_msg(MWARNING, gaibu_debug_msg);
        printf(gaibu_debug_msg,"Module %d max clock count hi: %ld\tlo: %ld \n",module,maxcount_16ns_hi[module],maxcount_16ns_lo[module]);
        maxcount_16ns_lo[module] = time_16ns_lo;
        maxcount_16ns_hi[module] = time_16ns_hi;
        ovf[module] = false;
      }
    }

    // For latch packets, compute the number of hits
    k = 0;
    if(type == 0) {
      for(int w = 0; w < nwords - 3 ; w++) { // fan-in packets have length=6
        int temp = CurrentOutDataVectorIt->at(7+w) + 0;
        for(int n = 0; n < 16; n++) {
          if(temp & 1) {
            k++;
          }
          temp >>=1;
        }
      }
      length = k;
      if(nwords == 5) { // trigger box packet
        type = 2; // Set trigger box packet type

        /* Sync Pulse Diagnostic Info: Throw error if sync pulse does not come at expected clock count */
        if(length == 32) { // Firmware only allows this for special sync pulse packets in trigger box
          long int time_16ns_sync = time_16ns_hi*65536+time_16ns_lo;
          long int expected_time_16ns_sync = (1 << SYNC_PULSE_CLK_COUNT_PERIOD_LOG2) - 1;
          long int expected_time_16ns_sync_offset = *(myoffsets[usb]+module_local);
          //Camillo modification
          if(expected_time_16ns_sync - expected_time_16ns_sync_offset != time_16ns_sync) {
            sprintf(gaibu_debug_msg,"Trigger box module %d received sync pulse at clock count %ld instead of expected clock count (%ld).",module,time_16ns_sync,expected_time_16ns_sync);
            gaibu_msg(MERROR, gaibu_debug_msg);
            printf("Trigger box module %d received sync pulse at clock count %ld instead of expected clock count (%ld).\n",module,time_16ns_sync,expected_time_16ns_sync);
          }
        }

      }
    }
    else {
      length = (CurrentOutDataVectorIt->size()-7)/2;
      //length = (length-3)/2;
    }

    CurrDataPacketHeader->SetNHits((char)length);
    CurrDataPacketHeader->SetModule((short int)module);
    CurrDataPacketHeader->SetType((char)type);
    //CurrHit->SetTimeSec(time_s_hi*65536 + time_s_lo);
    CurrDataPacketHeader->SetTime16ns(time_16ns_hi*65536 + time_16ns_lo);

    nbs = write(mydataFile, CurrDataPacketHeader, sizeof(OVDataPacketHeader)); // fOVEvent is a pointer to object
    if (nbs<0){
      sprintf(gaibu_debug_msg,"Fatal Error: Cannot write data packet header to disk!");
      gaibu_msg(MEXCEPTION, gaibu_debug_msg,RunNumber);
      printf("Fatal Error: Cannot write data packet header to disk!\n");
      write_ebretval(-1);
      exit(1);
    } // To be optimize

    if(type == 1) { // PMTBOARD ADC Packet

      for(int m = 0; m <= length - 1; m++) { //Loop over all hits in the packet

        channel = (char)CurrentOutDataVectorIt->at(8+2*m);
        charge = (short int)CurrentOutDataVectorIt->at(7+2*m);
        CurrHit->SetHit( OVSignal(channel, charge) );

        nbs = write(mydataFile, CurrHit, sizeof(OVHitData));
        if (nbs<0){
          sprintf(gaibu_debug_msg,"Fatal Error: Cannot write ov hit to disk!");
          gaibu_msg(MEXCEPTION, gaibu_debug_msg, RunNumber);
          printf("Fatal Error: Cannot write ov hit to disk!\n");
          write_ebretval(-1);
          exit(1);
        } // To be optimized

        cnt++;
      }
    }
    else { // PMTBOARD LATCH PACKET OR TRIGGER BOX PACKET

      for(int w = 0; w < nwords-3 ; w++) {
        int temp = CurrentOutDataVectorIt->at(7+w) + 0;
        for(int n = 0; n < 16; n++) {
          if(temp & 1) {

            //CurrHit->SetHit( OVSignal((char)(w*16 + n),(short int) 1) );
            CurrHit->SetHit( OVSignal((char)(16*(nwords-4-w) + n),(short int) 1) );

            nbs = write(mydataFile, CurrHit, sizeof(OVHitData));
            if (nbs<0){
              sprintf(gaibu_debug_msg,"Fatal Error: Cannot write ov hit to disk!");
              gaibu_msg(MEXCEPTION, gaibu_debug_msg, RunNumber);
              printf("Fatal Error: Cannot write ov hit to disk!\n");
              write_ebretval(-1);
              exit(1);
            } // To be optimized

            cnt++;
          }
          temp >>=1;
        }
      }

    }
    CurrentOutDataVectorIt++;
    CurrentOutIndexVectorIt++;

  } // For all events in data packet

  delete CurrEventHeader;
  delete CurrDataPacketHeader;
  delete CurrHit;
}

int check_options(int argc, char **argv)
{
  int index;
  char c;
  bool show_help=0;
  //char in_dir[BUFSIZE]; //data folder
  //char out_dir[BUFSIZE]; //default data folder
  char sem_name[BUFSIZE]; //default ftok semaphore file name
  char run_number[BUFSIZE]; // run number
  char mydaqhost[BUFSIZE];
  int datadisk = 0;
  int myoutdisk = 0;
  int mythresh = 0;
  int mytrigger = 0;
  char myruntype[BUFSIZE];
  int use_dogsifier = 0;
  //int offline_mode = 0;


  while ((c = getopt (argc, argv, "d:r:D:s:t:T:R:H:e:h:")) != -1) {
    switch (c) {
      //case 'F': strcpy(in_dir,optarg); DataFolder = in_dir;  break;
    case 'd': datadisk=atoi(optarg); Disk = datadisk; break;
      //case 'o': strcpy(out_dir,optarg); OutputFolder = out_dir;  break;
    case 'r': strcpy(run_number,optarg); RunNumber = run_number; break;
    case 'D': use_dogsifier=atoi(optarg); DOGSifier = use_dogsifier;  break;
    case 's': strcpy(sem_name,optarg); sname = sem_name;  break;
      //case 'O': offline_mode=atoi(optarg); OFFLINE = offline_mode;  break;
    case 't': mythresh=atoi(optarg); Threshold = mythresh;  break;
    case 'T': mytrigger=atoi(optarg); EBTrigMode = (TriggerMode)mytrigger;  break;
    case 'R': strcpy(myruntype,optarg); OVRunType = myruntype;  break;
    case 'H': strcpy(mydaqhost,optarg); OVDAQHost = mydaqhost; break;
    case 'e': myoutdisk=atoi(optarg); OutDisk = myoutdisk; break;
    case 'h':
    default:  show_help = true; break;
    }
  }
  if(optind < argc) show_help = true;
  if(EBTrigMode < kNone || EBTrigMode > kDoubleLayer) show_help = true;
  if(Threshold < 0) {
    sprintf(gaibu_debug_msg,"Negative thresholds not allowed. Reseting threshold to 0");
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Negative thresholds not allowed. Reseting threshold to 0");
    Threshold = 0;
  }

  for (index = optind; index < argc; index++) {
    printf ("Non-option argument %s\n", argv[index]);
  }

  if(show_help) {
    printf("Usage: %s -r <run_number> [-d <data_disk>]\n",argv[0]);
    printf("\t\t[-D <use_DOGSifier>] [-s <semaphore_filename>] [-O <offline_mode>]\n");
    printf("\t\t[-t <offline_threshold>] [-T <offline_trigger_mode>] [-R <run_type>]\n");
    printf("\t\t[-H <OV_DAQ_data_mount_point>] [-e <EBuilder_output_disk>]\n");
    //printf("-F : data folder of OV DAQ data to be processed    [default: OVDAQ/DATA]\n");
    printf("-d : disk number of OV DAQ data to be processed    [default: 2]\n");
    //printf("-o : directory of OV EBuilder output data          [default: /data1/OVDAQ/DATA]\n");
    printf("-r : specify expected run number for incoming data [default: Run_yyyymmdd_hh_mm (most recent)]\n");
    printf("-D : whether to run in online mode with DOGSifier  [default: 0 (don't use DOGSifier)]\n");
    printf("-s : file name to generate DOGSifier semaphore     [default: /var/tmp/OV_EBuilder.txt]\n");
    printf("-O : whether to run in offline mode (static dir)   [default: 0 (check for new files)]\n");
    printf("-t : offline threshold (in ADC counts) to apply    [default: 0 (no software threshold)]\n");
    printf("-T : offline trigger mode (0: NONE, 1: OR, 2: AND) [default: 0 (No trigger pattern between layers)]\n");
    printf("-R : OV run type (P: physics, C: calib, D: debug)  [default: P (physics run)]\n");
    printf("-H : OV DAQ mount path on EBuilder machine         [default: ovfovdaq]\n");
    printf("-e : disk number of OV EBuilder output binary      [default: 1]\n");
    return -1;
  }

  return 1;
}

bool GetBaselines()
{
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Search baseline directory for files and sort them lexigraphically
  std::string in_dir = DataFolder + "/Run_" + RunNumber + "/binary";
  //std::string in_dir = DataFolder + "/Run_" + RunNumber + "/baseline";
  vector<std::string> in_files_tmp;
  vector<std::string>::iterator in_files_tmp_it;
  if(GetDir(in_dir, in_files_tmp, 0, 1)) { // Get baselines too
    if(errno) {
      sprintf(gaibu_debug_msg,"Fatal Error(%d) opening binary directory %s for baselines",errno,in_dir.c_str());
      gaibu_msg(MERROR, gaibu_debug_msg);
      printf("Fatal Error(%d) opening binary directory %s for baselines\n",errno,in_dir.c_str());
    }
    return false;
  }
  else {
    printf("Processing baselines...\n"); // for run: %s\n",in_dir.c_str());
  }

  // Preparing for baseline shift
  vector<std::string> in_files;
  for(in_files_tmp_it = in_files_tmp.begin(); in_files_tmp_it != in_files_tmp.end(); in_files_tmp_it++) {
    if(in_files_tmp_it->find("baseline") != in_files_tmp_it->npos) {
      in_files.push_back(*in_files_tmp_it);
    }
  }

  sort(in_files.begin(),in_files.end());

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Sanity check on number of baseline files
  if((int)in_files.size() != numUSB-numFanUSB) {
    sprintf(gaibu_debug_msg,"Fatal Error: Baseline file count (%lu) != numUSB (%d) in directory %s", (long int)in_files.size(), numUSB, in_dir.c_str());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("Fatal Error: Baseline file count (%lu) != numUSB (%d) in directory %s\n", (long int)in_files.size(), numUSB, in_dir.c_str());
    return false;
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Set USB numbers for each OVUSBStream and load baseline files
  vector<std::string>::iterator in_files_it=in_files.begin();
  std::string fusb;
  long int iusb;
  char *pEnd;
  for(int i = 0; i<numUSB-numFanUSB; i++) {
    fusb = in_files_it->substr(in_files_it->find("_")+1,in_files_it->npos);
    iusb = strtol(fusb.c_str(),&pEnd,10); // if the usb is in the list of non-fan-in usbs
    if( OVUSBStream[Datamap[i]].GetUSB() == -1 ) { // Error: all usbs should have been assigned from MySQL
        sprintf(gaibu_debug_msg,"Fatal Error: Found USB number unassigned while getting baselines");
        gaibu_msg(MERROR, gaibu_debug_msg);
        printf("Fatal Error: Found USB number unassigned while getting baselines\n");
        return false;
    }
    if(OVUSBStream[Datamap[i]].LoadFile(in_dir+ "/baseline") < 1) { return false; } // Load baseline file for data streams
    in_files_it++;
  }
  //return true;
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Decode all files at once and load into memory
  for(int j=0; j<numUSB-numFanUSB; j++) { // Load all files in at once
    printf("Starting Thread %d\n",Datamap[j]);
    th[Datamap[j]] = new TThread(Form("th%d",Datamap[j]), handle, (void*) Datamap[j]);
    th[Datamap[j]]->Run();
  }

  tp = new TThread("tp", joiner, (void*) (numUSB-numFanUSB));
  tp->Run();

  while(!finished) { sleep(1); } // To be optimized
  finished = false;

  tp->Join();

  for(int k=0; k<numUSB-numFanUSB; k++) { delete th[Datamap[k]]; }
  delete tp;

  cout << "Joined all threads!\n";

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Build baseline tables
  int **baselines = new int*[maxModules];
  for(int i = 0; i<maxModules; i++) {
    baselines[i] = new int[numChannels];
    for(int j = 0; j<numChannels; j++) {
      *(*(baselines+i)+j) = 0;
    }
  }

  DataVector *BaselineData = new DataVector; // = new DataVector;
  int usb = 0;
  for(int i = 0; i<numUSB-numFanUSB; i++) {

    if(OVUSBStream[Datamap[i]].GetBaselineData(BaselineData) == false) { return false; }
    else {
      CalculatePedestal(BaselineData,baselines);
      OVUSBStream[Datamap[i]].SetBaseline(baselines); // Should I check for success here?
      usb = OVUSBStream[Datamap[i]].GetUSB(); // Should I check for success here?
      if(WriteBaselineTable(baselines,usb)==false) {
        sprintf(gaibu_debug_msg,"Fatal Error writing baseline table to MySQL database");
        gaibu_msg(MERROR, gaibu_debug_msg);
        printf("Fatal Error writing baseline table to MySQL database\n");
        return false;
      }
      BaselineData->clear();
    }
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Delete pointers
  for(int i = 0; i<maxModules; i++) {
    delete [] baselines[i];
  }
  delete [] baselines;
  delete BaselineData;

  return true;

}

void CalculatePedestal(DataVector* BaselineData, int **baseptr)
{
  //int numChannels = 64;
  double baseline[maxModules][numChannels] = {};
  int counter[maxModules][numChannels] = {};
  int channel, charge, module, type;
  channel = charge = module = 0;

  DataVectorIt BaselineDataIt;

  for(BaselineDataIt = BaselineData->begin(); BaselineDataIt != BaselineData->end(); BaselineDataIt++) {

    // Data Packets should have 7 + 2*num_hits elements
    if(BaselineDataIt->size() < 8 && BaselineDataIt->size() % 2 != 1) {
      sprintf(gaibu_debug_msg,"Fatal Error: Baseline data packet found with no data");
      gaibu_msg(MERROR, gaibu_debug_msg);
      printf("Fatal Error: Baseline data packet found with no data\n");
    }

    module = (BaselineDataIt->at(0) >> 8) & 0x7f;
    type = BaselineDataIt->at(0) >> 15;

    if(type) {
      if(module > maxModules) {
        sprintf(gaibu_debug_msg,"Fatal Error: Module number requested (%d) out of range in calculate pedestal",module);
        gaibu_msg(MERROR, gaibu_debug_msg);
        printf("Fatal Error: Module number requested (%d) out of range in calculate pedestal\n",module);
      }

      for(int i = 7; i+1 < (int)BaselineDataIt->size(); i=i+2) {
        charge = BaselineDataIt->at(i);
        channel = BaselineDataIt->at(i+1); // Channels run 0-63
        if(channel >= numChannels) {
          sprintf(gaibu_debug_msg,"Fatal Error: Channel number requested (%d) out of range in calculate pedestal",channel);
          gaibu_msg(MERROR, gaibu_debug_msg);
          printf("Fatal Error: Channel number requested (%d) out of range in calculate pedestal\n",channel);
        }
        // Should these be modified to better handle large numbers of baseline triggers?
        baseline[module][channel] = (baseline[module][channel]*counter[module][channel] + charge)/(counter[module][channel]+1);
        counter[module][channel] = counter[module][channel] + 1;
      }
    }
  }

  for(int i = 0; i < maxModules; i++) {
    for( int j = 0; j < numChannels; j++) {
      *(*(baseptr+i) + j) = (int)baseline[i][j];
    }
  }
}

bool WriteBaselineTable(int **baseptr, int usb)
{
  mysqlpp::Connection myconn(false); // false to not throw exceptions on errors
  mysqlpp::StoreQueryResult res;
  //mysqlpp::Query query;

  //Open connection
  if(myconn.connect(database, server, username, password) == false) {
    sprintf(gaibu_debug_msg,"Cannot connect to MySQL database %s at %s",database,server);
    gaibu_msg(MNOTICE, gaibu_debug_msg);
    printf("Cannot connect to MySQL database %s at %s\n",database,server);
    return false;
  }

  char insert_base[BUFSIZE] = "INSERT INTO OV_pedestal";

  char query_string[BUFSIZE];

  time_t rawtime;
  struct tm * timeinfo;

  time ( &rawtime );
  timeinfo = localtime ( &rawtime );

  char mydate[BUFSIZE];
  char mytime[BUFSIZE];
  char mybuff[BUFSIZE];
  sprintf(mydate,"%.4d-%.2d-%.2d",1900+timeinfo->tm_year,(1+timeinfo->tm_mon),timeinfo->tm_mday);
  sprintf(mytime,"%.2d:%.2d:%.2d",timeinfo->tm_hour,timeinfo->tm_min,timeinfo->tm_sec);
  std::string baseline_values = "";

  for(int i = 0; i < maxModules; i++) {
    baseline_values = "";
    for( int j = 0; j < numChannels; j++) {
      if( *(*(baseptr+i) + j) > 0) {

        if(j==0) {
          sprintf(mybuff,",%d",PMTUniqueMap[1000*usb+i]); // sprintf(mybuff,",%d",i);
          baseline_values.append(mybuff); // Insert board_address
        }

        sprintf(mybuff,",%d",*(*(baseptr+i) + j)); // Insert baseline value
        baseline_values.append(mybuff);

        if(j==numChannels-1) { // last baseline value has been inserted

          sprintf(query_string,"%s VALUES ('%s','%s','%s',''%s);",insert_base,
                  RunNumber.c_str(),mydate,mytime,baseline_values.c_str());

          mysqlpp::Query query = myconn.query(query_string);
          if(query.execute()==false) {
            sprintf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query.error());
            gaibu_msg(MNOTICE, gaibu_debug_msg);
            printf("MySQL query (%s) error: %s\n",query_string,query.error());
            myconn.disconnect();
            return false;
          }

        }
      }
    }
  }
  //Close the connection to the database
  myconn.disconnect();
  return true;
}

bool read_summary_table()
{
  mysqlpp::Connection myconn(false); // false to not throw exceptions on errors
  mysqlpp::StoreQueryResult res;
  //mysqlpp::Query query;

  char DCDatabase_path[BUFSIZE];
  sprintf(DCDatabase_path,"%s/config/DCDatabase.config",getenv("DCONLINE_PATH"));

  sprintf(server,"%s",config_string(DCDatabase_path,"DCDB_SERVER_HOST"));
  //printf("DB server: %s\n",server);
  sprintf(username,"%s",config_string(DCDatabase_path,"DCDB_OV_USER"));
  //printf("DB username: %s\n",username);
  sprintf(password,"%s",config_string(DCDatabase_path,"DCDB_OV_PASSWORD"));
  //printf("DB password: %s\n",password);
  sprintf(database,"%s",config_string(DCDatabase_path,"DCDB_OV_DBNAME"));
  //printf("DB database name: %s\n",database);

  //Open connection
  if(myconn.connect(database, server, username, password) == false) {
    sprintf(gaibu_debug_msg,"Cannot connect to MySQL database %s at %s",database,server);
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Cannot connect to MySQL database %s at %s\n",database,server);
    return false;
  }

  //////////////////////////////////////////////////////////////////////
  // Get mysql config table name
  char insert_base[BUFSIZE] = "SELECT Run_number,config_table FROM OV_runsummary WHERE Run_number = ";
  char query_string[BUFSIZE];
  sprintf(query_string,"%s'%s' ORDER BY start_time DESC;",insert_base,RunNumber.c_str());
  mysqlpp::Query query2 = myconn.query(query_string);
  res = query2.store();
  if(res.num_rows() < 1) {
    sprintf(gaibu_debug_msg,"Found no matching entry for run %s in OV_runsummary",RunNumber.c_str());
    //printf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query2.error());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("Found no matching entry for run %s in OV_runsummary\n",RunNumber.c_str());
    //printf("MySQL query (%s) error: %s\n",query_string,query2.error());
    myconn.disconnect();
    return false;
  }
  else if(res.num_rows() > 1) {
    sprintf(gaibu_debug_msg,"Found more than one entry for run %s in OV_runsummary. Using most recent entry.",RunNumber.c_str());
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Found more than one entry for run %s in OV_runsummary. Using most recent entry\n",RunNumber.c_str());
  }
  else {
    sprintf(gaibu_debug_msg,"Found MySQL run summary entry for run: %s",RunNumber.c_str());
    //gaibu_msg(MNOTICE, gaibu_debug_msg);
    printf("Found MySQL run summary entry for run: %s\n",RunNumber.c_str());
  }
  char config_table[BUFSIZE];
  strcpy(config_table,res[0][1].c_str());

  //////////////////////////////////////////////////////////////////////
  // Count number of distinct USBs
  strcpy(insert_base,"SELECT DISTINCT USB_serial FROM ");
  sprintf(query_string,"%s%s ORDER BY USB_serial;",insert_base,config_table);
  mysqlpp::Query query3 = myconn.query(query_string);
  res=query3.store();
  if(res.num_rows() < 1) {
    printf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query3.error());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("MySQL query (%s) error: %s\n",query_string,query3.error());
    myconn.disconnect();
    return false;
  }
  else {
    sprintf(gaibu_debug_msg,"Found %ld distinct USBs in table %s",(long int)res.num_rows(),config_table);
    //gaibu_msg(MNOTICE, gaibu_debug_msg);
    printf("Found %ld distinct USBs in table %s\n",(long int)res.num_rows(),config_table);
  }
  numUSB = (long int)res.num_rows();
  for(int i = 0; i<numUSB; i++) {
    USBmap[atoi(res[i][0])]=i;
    OVUSBStream[i].SetUSB(atoi(res[i][0]));
  }

  //////////////////////////////////////////////////////////////////////
  // Count number of non fan-in USBs
  strcpy(insert_base,"SELECT DISTINCT USB_serial FROM ");
  sprintf(query_string,"%s%s WHERE HV != -999 ORDER BY USB_serial;",insert_base,config_table);
  mysqlpp::Query query4 = myconn.query(query_string);
  res=query4.store();
  if(res.num_rows() < 1) {
    printf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query4.error());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("MySQL query (%s) error: %s\n",query_string,query4.error());
    myconn.disconnect();
    return false;
  }
  else {
    sprintf(gaibu_debug_msg,"Found %ld distinct non-Fan-in USBs in table %s",(long int)res.num_rows(),config_table);
    //gaibu_msg(MNOTICE, gaibu_debug_msg);
    printf("Found %ld distinct non-Fan-in USBs in table %s\n",(long int)res.num_rows(),config_table);
  }
  numFanUSB = numUSB - res.num_rows();
  for(int i = 0; i<numUSB-numFanUSB; i++) {
    Datamap[i] = USBmap[atoi(res[i][0])];
  }
  // Identify Fan-in USBs -- FixME is this needed?
  map<int,int>::iterator USBmapIt,DatamapIt;
  for(USBmapIt = USBmap.begin(); USBmapIt!=USBmap.end(); USBmapIt++) {
    bool PMTUSBFound = false;
    for(DatamapIt = Datamap.begin(); DatamapIt!=Datamap.end(); DatamapIt++) {
      if(USBmapIt->second == DatamapIt->second) { PMTUSBFound = true; }
    }
    if(!PMTUSBFound) {
      OVUSBStream[USBmapIt->second].SetIsFanUSB();
      sprintf(gaibu_debug_msg,"USB %d identified as Fan-in USB",USBmapIt->first);
      //gaibu_msg(MNOTICE, gaibu_debug_msg);
      printf("USB %d identified as Fan-in USB\n",USBmapIt->first);

    }
  }

  //////////////////////////////////////////////////////////////////////
  // Load the offsets for these boards
  strcpy(insert_base,"SELECT USB_serial, board_number, offset FROM ");
  sprintf(query_string,"%s%s;",insert_base,config_table);
  mysqlpp::Query query45 = myconn.query(query_string);
  res=query45.store();
  if(res.num_rows() < 1) {
    printf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query45.error());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("MySQL query (%s) error: %s\n",query_string,query45.error());
    myconn.disconnect();
    return false;
  }
  else {
    sprintf(gaibu_debug_msg,"Found time offsets for online table %s",config_table);
    //gaibu_msg(MNOTICE, gaibu_debug_msg);
    printf("Found time offsets for online table %s\n",config_table);
  }
  // Create map of UBS_serial to array of pmt board offsets
  //std::map<int,int*> myoffsets;
  for(int i = 0; i<(int)res.num_rows(); i++) {
    //printf("USB_serial: %d\tpmtboard: %d\toffset: %d\n",atoi(res[i][0]),atoi(res[i][1]),atoi(res[i][2]));
    if(!myoffsets.count(atoi(res[i][0]))) {
      myoffsets[atoi(res[i][0])] = new int[maxModules];
    }
    if(atoi(res[i][1]) < maxModules) {
      *(myoffsets[atoi(res[i][0])]+atoi(res[i][1])) = atoi(res[i][2]);
    }
  }
  std::map<int,int*>::iterator myoffsetsIt; // USB to array of PMT offsets
  for(myoffsetsIt = myoffsets.begin(); myoffsetsIt != myoffsets.end(); myoffsetsIt++) {
    OVUSBStream[USBmap[myoffsetsIt->first]].SetOffset(myoffsetsIt->second);
  }

  //////////////////////////////////////////////////////////////////////
  // Count the number of boards in this setup
  strcpy(insert_base,"SELECT DISTINCT pmtboard_u FROM ");
  sprintf(query_string,"%s%s;",insert_base,config_table);
  mysqlpp::Query query5 = myconn.query(query_string);
  res=query5.store();
  if(res.num_rows() < 1) {
    printf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query5.error());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("MySQL query (%s) error: %s\n",query_string,query5.error());
    myconn.disconnect();
    return false;
  }
  else {
    sprintf(gaibu_debug_msg,"Found %ld distinct PMT boards in table %s",(long int)res.num_rows(),config_table);
    //gaibu_msg(MNOTICE, gaibu_debug_msg);
    printf("Found %ld distinct PMT boards in table %s\n",(long int)res.num_rows(),config_table);
  }
  totalboards = res.num_rows();

  // New function establishes overflow array
  int max = 0; int pmtnum;
  for(int i = 0; i<(int)res.num_rows(); i++) {
    pmtnum = atoi(res[i][0]); // res[i][0] - (100)*(int)(res[i][0]/100);
    if(pmtnum > max) { max = pmtnum; }
  }
  sprintf(gaibu_debug_msg,"Found max PMT board number %d in table %s",max,config_table);
  //gaibu_msg(MNOTICE, gaibu_debug_msg);
  printf("Found max PMT board number %d in table %s\n",max,config_table);

  ovf = new bool[max+1];
  maxcount_16ns_hi = new long int[max+1];
  maxcount_16ns_lo = new long int[max+1];
  for(int i = 0; i<max+1; i++) {
    ovf[i] = false;
    maxcount_16ns_hi[i] = 0;
    maxcount_16ns_lo[i] = 0;
  }

  //////////////////////////////////////////////////////////////////////
  // Count the number of PMT and Fan-in boards in this setup
  strcpy(insert_base,"SELECT HV, USB_serial, board_number, pmtboard_u FROM ");
  sprintf(query_string,"%s%s;",insert_base,config_table);
  mysqlpp::Query query6 = myconn.query(query_string);
  res=query6.store();
  if(res.num_rows() < 1) {
    printf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query3.error());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("MySQL query (%s) error: %s\n",query_string,query3.error());
    myconn.disconnect();
    return false;
  }
  else {
    sprintf(gaibu_debug_msg,"Found %ld distinct PMT boards in table %s",(long int)res.num_rows(),config_table);
    //gaibu_msg(MNOTICE, gaibu_debug_msg);
    printf("Found %ld distinct PMT boards in table %s\n",(long int)res.num_rows(),config_table);
  }
  if((int)res.num_rows() != totalboards) { // config table has duplicate entries
    sprintf(gaibu_debug_msg,"Found duplicate pmtboard_u entries in table %s",config_table);
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Found duplicate pmtboard_u entries in table %s\n",config_table);
    myconn.disconnect();
    return false;
  }
  for(int i = 0; i<(int)res.num_rows(); i++) {
    if(atoi(res[i][0])!=-999) { ++totalPMTboards; }
    else { ++totalFanboards; }
    // This is a temporary internal mapping used only by the EBuilder
    PMTUniqueMap[1000*atoi(res[i][1])+atoi(res[i][2])] = atoi(res[i][3]);
  }

  //////////////////////////////////////////////////////////////////////
  // Get run summary information
  strcpy(insert_base,"SELECT Run_number,Run_Type,SW_Threshold,SW_TriggerMode,use_DOGSifier,daq_disk,EBcomment,EBsubrunnumber,stop_time FROM OV_runsummary where Run_number = ");
  sprintf(query_string,"%s'%s' ORDER BY start_time DESC;",insert_base,RunNumber.c_str());
  mysqlpp::Query query = myconn.query(query_string);
  res = query.store();
  if(res.num_rows() < 1) {
    sprintf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query.error());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("MySQL query (%s) error: %s\n",query_string,query.error());
    myconn.disconnect();
    return false;
  }
  else if(res.num_rows() > 1) { // Check that OVRunType is the same?
    sprintf(gaibu_debug_msg,"Found more than one entry for run %s in OV_runsummary. Using most recent entry",RunNumber.c_str());
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Found more than one entry for run %s in OV_runsummary. Using most recent entry\n",RunNumber.c_str());
  }
  else {
    sprintf(gaibu_debug_msg,"Found MySQL run summary entry for run: %s",RunNumber.c_str());
    //gaibu_msg(MNOTICE, gaibu_debug_msg);
    printf("Found MySQL run summary entry for run: %s\n",RunNumber.c_str());
  }

  //for(int i = 0; i<(int)(res[0]).size(); i++) {
  //  printf("res[0][%d]: %s\n",i,res[0][i].c_str());
  //}

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Set the Data Folder and Ouput Dir
  //if(res[0][1].c_str() != OVRunType) {
  if(OVRunType.compare(res[0][1].c_str()) != 0) {
    sprintf(gaibu_debug_msg,"MySQL Run Type: %s does not match command line Run Type: %s",
            res[0][1].c_str(),OVRunType.c_str());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("MySQL Run Type: %s does not match command line Run Type: %s\n",
            res[0][1].c_str(),OVRunType.c_str());
    myconn.disconnect();
    return false;
  }
  if(DOGSifier != atoi(res[0][4])) {
    sprintf(gaibu_debug_msg,"DOGSifier mode requested (%d) will override MySQL setting (%d)",DOGSifier,atoi(res[0][4]));
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("DOGSifier mode requested (%d) will override MySQL setting (%d)",DOGSifier,atoi(res[0][4]));
  }

  char tmpoutdir[BUFSIZE];
  if(OVRunType.compare("P") == 0) {
    DataFolder = "OVDAQ/DATA";
    sprintf(tmpoutdir,"/data%d/OVDAQ/",OutDisk);
  } else if(OVRunType.compare("C") == 0) {
    DataFolder = "OVDAQ/DATA"; // OVCalib/DATA
    sprintf(tmpoutdir,"/data%d/OVDAQ/",OutDisk);
    //sprintf(tmpoutdir,"/data%d/OVCalib",OutDisk);
  } else { // DEBUG mode
    DataFolder = "OVDAQ/DATA"; //OVDebug/DATA
    sprintf(tmpoutdir,"/data%d/OVDAQ/",OutDisk);
    //sprintf(tmpoutdir,"/data%d/OVDebug",OutDisk);
  }
  OutputDir = tmpoutdir;
  OutputFolder = OutputDir + "DATA/";

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Locate OV binary data
  if(atoi(res[0][5]) == 1 || atoi(res[0][5]) == 2) { // Data Disk
    Disk = atoi(res[0][5]);
    char inpath[BUFSIZE]; // Assign output folder based on disk number
    sprintf(inpath,"/%s/data%d/%s",OVDAQHost.c_str(),Disk,DataFolder.c_str());
    DataFolder = inpath;
    //printf("DataFolder: %s\n",DataFolder.c_str());
  }
  else {
    sprintf(gaibu_debug_msg,"MySQL query error: could not retrieve data disk for Run: %s",RunNumber.c_str());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("MySQL query error: could not retrieve data disk for Run: %s\n",RunNumber.c_str());
    myconn.disconnect();
    return false;
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Determine run mode
  vector<string> initial_files;
  string binary_dir = DataFolder + "/Run_" + RunNumber + "/binary/";
  string decoded_dir = DataFolder + "/Run_" + RunNumber + "/decoded/";
  char *pEnd;
  if(!res[0][6].is_null()) { // EBcomment filled each successful write attempt
    if(GetDir(binary_dir, initial_files, 0, 0)) { // False if non-baseline files are found
      if(errno) {
        sprintf(gaibu_debug_msg,"Error(%d) opening directory %s",errno,binary_dir.c_str());
        gaibu_msg(MERROR, gaibu_debug_msg);
        printf("Error(%d) opening directory %s\n",errno,binary_dir.c_str());
        myconn.disconnect();
        return false;
      }
      if(!res[0][8].is_null()) { // stop_time has been filled and so was a successful run
        EBRunMode = kReprocess;
        //DOGSifier = 0;
      }
      else { // stop_time has not been filled. DAQ could be waiting in STARTED_S
        EBRunMode = kRecovery;
        EBcomment = strtol(res[0][6].c_str(),&pEnd,10);
      }
    }
    else {
      EBRunMode = kRecovery;
      EBcomment = strtol(res[0][6].c_str(),&pEnd,10);
    }
  }
  sprintf(gaibu_debug_msg,"OV EBuilder Run Mode: %d",EBRunMode);
  //gaibu_msg(MNOTICE, gaibu_debug_msg);
  printf("OV EBuilder Run Mode: %d\n",EBRunMode);

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Sanity check for each run mode
  if(EBRunMode == kReprocess) { // Check if original run already used these parameters. No reprocess.
    if(atoi(res[0][2]) == Threshold && (TriggerMode)atoi(res[0][3]) == EBTrigMode) {
      sprintf(gaibu_debug_msg,"MySQL running parameters match reprocessing parameters. Threshold: %04d\tTrigger type: %01d",
              Threshold, (int)EBTrigMode);
      gaibu_msg(MERROR, gaibu_debug_msg);
      printf("MySQL running parameters match Reprocessing parameters. Threshold: %04d\tTrigger type: %01d\n",
             Threshold, (int)EBTrigMode);
      myconn.disconnect();
      return false;
    }
  }
  else if(EBRunMode == kRecovery) { // Check consistency of run parameters
    if(res[0][1].c_str() != OVRunType || atoi(res[0][2]) != Threshold || atoi(res[0][3]) != (int)EBTrigMode) {
      sprintf(gaibu_debug_msg,"MySQL parameters do not match recovery parameters. RunType: %s\tThreshold: %04d\tTrigger type: %01d",
              OVRunType.c_str(),Threshold, (int)EBTrigMode);
      gaibu_msg(MERROR, gaibu_debug_msg);
      printf("MySQL parameters do not match recovery parameters. RunType: %s\tThreshold: %04d\tTrigger type: %01d\n",
             OVRunType.c_str(),Threshold, (int)EBTrigMode);
      myconn.disconnect();
      return false;
    }
    if(!res[0][7].is_null()) { SubRunCounter = timestampsperoutput*atoi(res[0][7]); }

  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Get EBuilder parameters from MySQL database
  if(EBRunMode != kReprocess) {
    if(EBTrigMode < kNone || EBTrigMode > kDoubleLayer) { // Sanity check
      sprintf(gaibu_debug_msg,"Invalid EBTrigMode: %01d",EBTrigMode);
      gaibu_msg(MERROR, gaibu_debug_msg);
      printf("Invalid EBTrigMode: %04d\n",EBTrigMode);
      myconn.disconnect();
      return false;
    }
    if(EBTrigMode != (TriggerMode)atoi(res[0][3])) {
      sprintf(gaibu_debug_msg,"Trigger Mode requested (%d) will override MySQL setting (%d)",(int)EBTrigMode,atoi(res[0][3]));
      gaibu_msg(MWARNING, gaibu_debug_msg);
      printf("Trigger Mode requested (%d) will override MySQL setting (%d)",(int)EBTrigMode,atoi(res[0][3]));
    }
    if(Threshold != atoi(res[0][2])) {
      sprintf(gaibu_debug_msg,"Threshold requested (%d) will override MySQL setting (%d)",Threshold,atoi(res[0][2]));
      gaibu_msg(MWARNING, gaibu_debug_msg);
      printf("Threshold requested (%d) will override MySQL setting (%d)",Threshold,atoi(res[0][2]));
    }

    printf("Threshold: %d \t EBTrigMode: %d\n",Threshold,EBTrigMode);

  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Connect to EBuilder MySQL table
  if(EBRunMode == kReprocess) {

    umask(0); // For file permissions
    char tempfolder[BUFSIZE];

    sprintf(query_string,"SELECT Path FROM OV_ebuilder WHERE Run_number = '%s';",RunNumber.c_str());
    //mysqlpp::Query query = myconn.query(query_string);
    mysqlpp::Query query2 = myconn.query(query_string);
    mysqlpp::StoreQueryResult res2 = query2.store();

    if(res2.num_rows() == 0) { // Can't find run in OV_ebuilder
      sprintf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query2.error());
      gaibu_msg(MERROR, gaibu_debug_msg);
      printf("MySQL query (%s) error: %s\n",query_string,query2.error());

      sprintf(gaibu_debug_msg,"EBuilder did not finish processing run %s. Run recovery mode first.",RunNumber.c_str());
      gaibu_msg(MERROR, gaibu_debug_msg);
      printf("EBuilder did not finish processing run %s. Run recovery mode first.\n",RunNumber.c_str());

      myconn.disconnect();
      return false;
    }
    else if(res2.num_rows() == 1) { // Run has never been reprocessed

      sprintf(tempfolder,"%sREP/Run_%s",OutputDir.c_str(),RunNumber.c_str());
      OutputFolder = tempfolder;
      if(mkdir(OutputFolder.c_str(), 0777)) { //O_CREAT??
        if(errno != EEXIST) {
          sprintf(gaibu_debug_msg,"Error (%d) creating output folder %s",errno,OutputFolder.c_str());
          gaibu_msg(MERROR, gaibu_debug_msg);
          printf("Error (%d) creating output folder %s\n",errno,OutputFolder.c_str());
          myconn.disconnect();
          return false;
        }
        else {
          sprintf(gaibu_debug_msg,"Output folder %s already exists.",OutputFolder.c_str());
          gaibu_msg(MWARNING, gaibu_debug_msg);
          printf("Output folder %s already exists.\n",OutputFolder.c_str());
        }
      }
    }

    char insert_base2[BUFSIZE] = "SELECT Path,ENTRY FROM OV_ebuilder WHERE ";
    string Path = "";

    sprintf(query_string,"%s Run_number = '%s' and SW_Threshold = '%04d' and SW_TriggerMode = '%01d' and Res1 = '%02d' and Res2 = '%02d' ORDER BY ENTRY;",
            insert_base2,RunNumber.c_str(),Threshold,(int)EBTrigMode, Res1, Res2);
    printf("query: %s\n",query_string);
    //mysqlpp::Query query = myconn.query(query_string);
    mysqlpp::Query query3 = myconn.query(query_string);
    mysqlpp::StoreQueryResult res3 = query3.store();

    if(res3.num_rows() > 0) {  // Run has already been reprocessed with same configuration
      Repeat = true;
      Path = res3[0][0].c_str();
      sprintf(gaibu_debug_msg,"Same reprocessing configuration found for run %s at %s. Deleting...",
              RunNumber.c_str(),Path.c_str());
      gaibu_msg(MWARNING, gaibu_debug_msg);
      printf("Same reprocessing configuration found for run %s at %s. Deleting...\n",RunNumber.c_str(),Path.c_str());

      //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      // Clean up old directory
      vector<string> old_files;
      string tempfile;
      string tempdir = Path + "Run_" + RunNumber + "/processed";
      if(GetDir(tempdir, old_files, 1)) {
        if(errno) {
          sprintf(gaibu_debug_msg,"Error(%d) opening directory %s",errno,tempdir.c_str());
          gaibu_msg(MERROR, gaibu_debug_msg);
          printf("Error(%d) opening directory %s\n",errno,tempdir.c_str());
          myconn.disconnect();
          return false;
        }
      }
      for(int m = 0; m<(int)old_files.size(); m++) {
        tempfile = tempdir + "/" + old_files[m];
        if(remove(tempfile.c_str())) {
          sprintf(gaibu_debug_msg,"Error deleting file %s",tempfile.c_str());
          gaibu_msg(MERROR, gaibu_debug_msg);
          printf("Error deleting file %s\n",tempfile.c_str());
          myconn.disconnect();
          return false;
        }
      }
      if(rmdir(tempdir.c_str())) {
        sprintf(gaibu_debug_msg,"Error deleting folder %s",tempdir.c_str());
        gaibu_msg(MERROR, gaibu_debug_msg);
        printf("Error deleting folder %s\n",tempdir.c_str());
        myconn.disconnect();
        return false;
      }
      old_files.clear();

      tempdir = Path + "Run_" + RunNumber;
      if(GetDir(tempdir, old_files, 1)) {
        if(errno) {
          sprintf(gaibu_debug_msg,"Error(%d) opening directory %s",errno,tempdir.c_str());
          gaibu_msg(MERROR, gaibu_debug_msg);
          //printf("Error(%d) opening directory %s\n",errno,tempdir.c_str());
          myconn.disconnect();
          return false;
        }
      }
      for(int m = 0; m<(int)old_files.size(); m++) {
        tempfile = tempdir + "/" + old_files[m];
        if(remove(tempfile.c_str())) {
          sprintf(gaibu_debug_msg,"Error deleting file %s",tempfile.c_str());
          gaibu_msg(MERROR, gaibu_debug_msg);
          printf("Error deleting file %s\n",tempfile.c_str());
          myconn.disconnect();
          return false;
        }
      }
      if(rmdir(tempdir.c_str())) {
        sprintf(gaibu_debug_msg,"Error deleting folder %s",tempdir.c_str());
        gaibu_msg(MERROR, gaibu_debug_msg);
        printf("Error deleting folder %s\n",tempdir.c_str());
        myconn.disconnect();
        return false;
      }
      //OutputFolder = Path;
      //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    } // if(res3.num_rows() > 0) {

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Create folder based on parameters
    sprintf(tempfolder,"%sREP/Run_%s/T%dADC%04dP1%02dP2%02d/",
            OutputDir.c_str(),RunNumber.c_str(),(int)EBTrigMode,Threshold,Res1,Res2);
    OutputFolder = tempfolder;
    if(mkdir(OutputFolder.c_str(), 0777)) { //O_CREAT??
      if(errno != EEXIST) {
        sprintf(gaibu_debug_msg,"Error (%d) creating output folder %s",errno,OutputFolder.c_str());
        gaibu_msg(MERROR, gaibu_debug_msg);
        printf("Error (%d) creating output folder %s\n",errno,OutputFolder.c_str());
        myconn.disconnect();
        return false;
      }
      else {
        sprintf(gaibu_debug_msg,"Output folder %s already exists.",OutputFolder.c_str());
        gaibu_msg(MWARNING, gaibu_debug_msg);
        printf("Output folder %s already exists.\n",OutputFolder.c_str());
      }

    }
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  } //  if(EBRunMode == kReprocess)
  if(!Repeat) {
    if(write_ebsummary() == false) {return false; }
  }

  if(EBRunMode != kNormal) {

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Move some decoded OV binary files for processing
    int rn,rn_error;
    //int tot_file_count,max_count
    string tempfname;
    size_t temppos;

    initial_files.clear();
    if(GetDir(decoded_dir, initial_files, 1)) {
      if(errno) {
        sprintf(gaibu_debug_msg,"Error(%d) opening directory %s",errno,decoded_dir.c_str());
        gaibu_msg(MERROR, gaibu_debug_msg);
        //printf("Error(%d) opening directory %s\n",errno,decoded_dir.c_str());
        myconn.disconnect();
        return false;
      }
      sprintf(gaibu_debug_msg,"No decoded files found in directory %s",decoded_dir.c_str());
      gaibu_msg(MERROR, gaibu_debug_msg);
      printf("No decoded files found in directory %s\n",decoded_dir.c_str());
      myconn.disconnect();
      return false;
    }
    sort(initial_files.begin(),initial_files.end());

    //for(int i = 0; i<initial_files.size(); i++) {
    //  cout << "File " << i << ": " << initial_files[i] << endl;
    //}

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Determine files to rename
    vector<std::string>::iterator fname_begin=initial_files.begin();
    std::string fdelim = "_"; // Assume files are of form xxxxxxxxx_xx.done
    //std::string ftime_min;
    if(fname_begin->find(fdelim) == fname_begin->npos) {
      sprintf(gaibu_debug_msg,"Error: Cannot find '_' in file name");
      gaibu_msg(MWARNING, gaibu_debug_msg);
      printf("Error: Cannot find '_' in file name\n");
      myconn.disconnect();
      return false;
    }
    size_t fname_it_delim = fname_begin->find(fdelim);

    vector<string> myfiles[maxUSB];
    map<int,int> mymap;
    std::string fusb;
    char *pEnd;
    int iusb, mapindex;
    mapindex = 0;
    for(int k = 0; k<(int)initial_files.size(); k++) {
      fusb = (initial_files[k]).substr(fname_it_delim+1,2);
      iusb = (int)strtol(fusb.c_str(),&pEnd,10);
      if(!mymap.count(iusb)) { mymap[iusb] = mapindex++; }
      (myfiles[mymap[iusb]]).push_back(initial_files[k]);
    }

    vector<string> files_to_rename;// = initial_files;
    int avgsize = initial_files.size()/numUSB;
    if(EBRunMode == kRecovery) {
      for(int j = 0; j<numUSB; j++) {
        int mysize = myfiles[j].size();
        if(mysize > 0)
          files_to_rename.push_back(decoded_dir + myfiles[j].at(mysize - 1));
        if(mysize > 1)
          files_to_rename.push_back(decoded_dir + myfiles[j].at(mysize - 2));
        if(mysize > 2 && mysize > avgsize) {
          files_to_rename.push_back(decoded_dir + myfiles[j].at(mysize - 3));
        }
      }
    }
    else { // Rename all files if EBRunMode == kReprocess
      for(int j = 0; j<(int)initial_files.size(); j++) {
        files_to_rename.push_back(decoded_dir + initial_files[j]);
      }
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Rename files
    for(int i = 0; i<(int)files_to_rename.size(); i++) {
      rn = 0;
      rn_error = 0;
      //tempfname = initial_files[tot_file_count - i];
      tempfname = files_to_rename[i];
      temppos = tempfname.find("decoded");
      if(temppos != tempfname.npos) {
        tempfname.replace(temppos,7,"binary");
      }
      else { rn_error = 1; }
      temppos = tempfname.find(".done");
      if(temppos != tempfname.npos) {
        tempfname.replace(temppos,5,"");
      }
      else { rn_error = 1; }
      if(!rn_error) {
        while(rename(files_to_rename[i].c_str(),tempfname.c_str())) {
            sprintf(gaibu_debug_msg,"Could not rename decoded data file.");
            gaibu_msg(MERROR,gaibu_debug_msg);
            printf(gaibu_debug_msg,"Could not rename decoded data file.\n");
            sleep(1);
          }
      }
      else {
        sprintf(gaibu_debug_msg,"Unexpected decoded data file name: %s",tempfname.c_str());
        gaibu_msg(MERROR,gaibu_debug_msg);
        printf("Unexpected decoded data file name: %s\n",tempfname.c_str());
        myconn.disconnect();
        return false;
      }
      /*
      if(rn!=0 || rn_error!=0) {
        sprintf(gaibu_debug_msg,"Could not rename decoded data file to %s",tempfname.c_str());
        gaibu_msg(MERROR,gaibu_debug_msg);
        printf("Could not rename decoded data file to %s\n",tempfname.c_str());
        myconn.disconnect();
        return false;
      }
      */
    }
  }
  myconn.disconnect();
  return true;
}

bool write_summary_table(long int lasttime, int subrun)
{
  mysqlpp::Connection myconn(false); // false to not throw exceptions on errors
  mysqlpp::StoreQueryResult res;

  //Open connection
  if(myconn.connect(database, server, username, password) == false) {
    sprintf(gaibu_debug_msg,"Cannot connect to MySQL database %s at %s",database,server);
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Cannot connect to MySQL database %s at %s\n",database,server);
    return false;
  }

  char insert_base[BUFSIZE] = "UPDATE OV_runsummary SET EBcomment = ";
  char query_string[BUFSIZE];

  sprintf(query_string,"%s'%ld', EBsubrunnumber = '%d' WHERE Run_number = '%s';",insert_base,lasttime,subrun,RunNumber.c_str());
  mysqlpp::Query query = myconn.query(query_string);
  if(query.execute()==false) {
    sprintf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query.error());
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("MySQL query (%s) error: %s\n",query_string,query.error());
    myconn.disconnect();
    return false;
  }

  myconn.disconnect();
  return true;
}

bool read_stop_time()
{
  mysqlpp::Connection myconn(false); // false to not throw exceptions on errors
  mysqlpp::StoreQueryResult res;

  //Open connection
  if(myconn.connect(database, server, username, password) == false) {
    sprintf(gaibu_debug_msg,"Cannot connect to MySQL database %s at %s",database,server);
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Cannot connect to MySQL database %s at %s\n",database,server);
    return false;
  }

  char insert_base[BUFSIZE] = "SELECT stop_time FROM OV_runsummary WHERE Run_number = ";
  char query_string[BUFSIZE];

  sprintf(query_string,"%s'%s' ORDER BY start_time DESC;",insert_base,RunNumber.c_str());
  //printf("%s'%s' ORDER BY start_time DESC\n;",insert_base,RunNumber.c_str());
  mysqlpp::Query query = myconn.query(query_string);
  res = query.store();
  if(res.num_rows() < 1) {
    sprintf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query.error());
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("MySQL query (%s) error: %s\n",query_string,query.error());
    myconn.disconnect();
    return false;
  }
  /*
  else if(res.num_rows() > 1) {
    sprintf(gaibu_debug_msg,"Found more than one entry for run %s in OV_runsummary. Using most recent entry.",RunNumber.c_str());
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Found more than one entry for run %s in OV_runsummary. Using most recent entry\n",RunNumber.c_str());
  }
  */
  std::cout << "res[0][0]: " << res[0][0].c_str() << std::endl;
  if(res[0][0].is_null()) {
    myconn.disconnect();
    return false;
  }

  sprintf(gaibu_debug_msg,"Found MySQL stop time for run: %s",RunNumber.c_str());
  //gaibu_msg(MNOTICE, gaibu_debug_msg);
  printf("Found MySQL stop time for run: %s\n",RunNumber.c_str());
  myconn.disconnect();

  return true;
}

bool write_endofrun_block(std::string myfname, int data_fd)
{
  std::cout << "Trying to write end of run block" << std::endl;
  if(EBRunMode == kRecovery || SubRunCounter % timestampsperoutput == 0) {
    data_fd = open_file(myfname);
    if(data_fd <= 0) {
      sprintf(gaibu_debug_msg,"Cannot open file %s to write end-of-run block for run %s",myfname.c_str(),RunNumber.c_str());
      gaibu_msg(MERROR, gaibu_debug_msg);
      sprintf(gaibu_debug_msg,"Cannot open file %s to write end-of-run block for run %s\n",myfname.c_str(),RunNumber.c_str());
      return false;
    }
  }

  OVEventHeader *CurrEventHeader = new OVEventHeader;
  CurrEventHeader->SetTimeSec(0);
  CurrEventHeader->SetNOVDataPackets(-99);

  int nbs = write(data_fd, CurrEventHeader, sizeof(OVEventHeader)); // fOVEvent is a pointer to object
  if (nbs<0){
    sprintf(gaibu_debug_msg,"End of run write error");
    gaibu_msg(MERROR, gaibu_debug_msg);
    printf("End of run write error\n");
    return false;
  } // To be optimized

  int cl = close(data_fd);
  if(cl<0) {
    sprintf(gaibu_debug_msg,"Could not close output data file");
    gaibu_msg(MERROR,gaibu_debug_msg);
    printf("Error! Could not close output data file\n");
  }

  return true;
}

bool write_ebsummary()
{
  mysqlpp::Connection myconn(false); // false to not throw exceptions on errors
  mysqlpp::StoreQueryResult res;

  //Open connection
  if(myconn.connect(database, server, username, password) == false) {
    sprintf(gaibu_debug_msg,"Cannot connect to MySQL database %s at %s",database,server);
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Cannot connect to MySQL database %s at %s\n",database,server);
    return false;
  }

  char insert_base[BUFSIZE] = "SELECT Path FROM OV_ebuilder WHERE ";
  char query_string[BUFSIZE];
  //string Path = "";

  sprintf(query_string,"%s Run_number = '%s', SW_Threshold = '%04d', SW_TriggerMode = '%01d', Res1 = '%02d', Res2 = '%02d';",
          insert_base,RunNumber.c_str(),Threshold,(int)EBTrigMode,Res1,Res2);
  mysqlpp::Query query = myconn.query(query_string);
  res = query.store();

  if(res.num_rows() == 0) {  // Run has never been reprocessed with this configuration

    char insert_base2[BUFSIZE] = "INSERT INTO OV_ebuilder";
    sprintf(query_string,"%s (%s,%s,%s,%s,%s,%s) VALUES ('%s','%s','%04d','%01d','%02d','%02d');",
            insert_base2,"Run_number","Path","SW_Threshold","SW_TriggerMode","Res1","Res2",
            RunNumber.c_str(),OutputFolder.c_str(),Threshold,(int)EBTrigMode,Res1,Res2);

    mysqlpp::Query query2 = myconn.query(query_string);
    if(query2.execute()==false) {
      sprintf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query2.error());
      gaibu_msg(MWARNING, gaibu_debug_msg);
      printf("MySQL query (%s) error: %s\n",query_string,query2.error());
      myconn.disconnect();
      return false;
    }

  }

  myconn.disconnect();
  return true;
}

bool write_ebretval(int val)
{
  mysqlpp::Connection myconn(false); // false to not throw exceptions on errors
  mysqlpp::StoreQueryResult res;

  //Open connection
  if(myconn.connect(database, server, username, password) == false) {
    sprintf(gaibu_debug_msg,"Cannot connect to MySQL database %s at %s",database,server);
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Cannot connect to MySQL database %s at %s\n",database,server);
    return false;
  }

  char insert_base[BUFSIZE] = "SELECT Run_number FROM OV_runsummary WHERE ";
  char query_string[BUFSIZE];
  //string Path = "";

  sprintf(query_string,"%s Run_number = '%s';",
          insert_base,RunNumber.c_str());
  mysqlpp::Query query = myconn.query(query_string);
  res = query.store();

  if(res.num_rows() == 1) {  // Run has never been reprocessed with this configuration

    char insert_base2[BUFSIZE] = "Update OV_runsummary set EBretval = ";
    sprintf(query_string,"%s '%d' where Run_number = '%s';",
            insert_base2,val,RunNumber.c_str());

    mysqlpp::Query query2 = myconn.query(query_string);
    if(query2.execute()==false) {
      sprintf(gaibu_debug_msg,"MySQL query (%s) error: %s",query_string,query2.error());
      gaibu_msg(MWARNING, gaibu_debug_msg);
      printf("MySQL query (%s) error: %s\n",query_string,query2.error());
      myconn.disconnect();
      return false;
    }

  } else {
    sprintf(gaibu_debug_msg,"Did not find unique OV_runsummary entry for run %s in eb_writeretval",RunNumber.c_str());
    gaibu_msg(MWARNING, gaibu_debug_msg);
    printf("Did not find unique OV_runsummary entry for run %s in eb_writeretval\n",RunNumber.c_str());
    myconn.disconnect();
    return false;
  }

  myconn.disconnect();
  return true;
}
