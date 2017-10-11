#include <stdlib.h>
#include "USBstream.h"
#include <syslog.h>

USBstream::USBstream()
{
  mythresh=0;
  myusb=-1;
  mytolutc=0;
  words = 0;
  got_hi = false;
  restart = false;
  first_packet = false;
  time_hi_1 = 0;
  time_hi_2 = 0;
  time_lo_1 = 0;
  time_lo_2 = 0;
  word_index = 0;
  word_count[0] = 0;
  word_count[1] = 0;
  word_count[2] = 0;
  word_count[3] = 0;
  bytesleft=0;
  fsize=0;
  myvec.reserve(65535); // XXX really one less than 0x10000? I mean, reserve()
                        // usually doesn't matter anyway...
  myit=myvec.begin();
  IsOpen = false;
  IsFanUSB = false;
  BothLayerThresh = false;
  UseThresh = false;
  for(int i = 0; i < 32; i++) { // Map of adjacent channels
    adj1[i] = i+32;
    if(i==0) adj2[i] = adj1[i];
    else if(i % 8 == 0) adj2[i] = adj1[i]-1;
    else if(i % 8 < 4) adj2[i] = adj1[i]+3;
    else adj2[i] = adj1[i]-4;
  }
}

USBstream::~USBstream()
{
}

void USBstream::Reset()
{
  words = 0;
  got_hi = false;
  word_index = 0;
  word_count[0] =  0;
  word_count[1] = 0;
  word_count[2] = 0;
  word_count[3] = 0;
  bytesleft=0;
  fsize=0;
}

void USBstream::SetOffset(int *off)
{
  for(int i = 0; i < 64; i++) {
    if(*(off+i) > 0) offset[i] = *(off+i);
    else             offset[i] = 0;
  }
}

void USBstream::SetThresh(int thresh, int threshtype)
{
  UseThresh = (bool)threshtype;
  BothLayerThresh=(bool)(threshtype-1);
  if(thresh)
    mythresh=thresh;
  else
    mythresh = -20; // Put SW threshold well below HW threshold (including spread)
}

void USBstream::SetBaseline(int **baseptr)
{
  for(int i = 0; i < 64; i++) {
    for(int j = 0; j < 64; j++) {
      if(*(*(baseptr+i)+j) > 0) baseline[i][j] = *(*(baseptr+i)+j);
      else                      baseline[i][j] = 0;
    }
  }
}

void USBstream::GetBaselineData(DataVector *vec)
{
  vec->clear();
  for(myit = myvec.begin(); myit != myvec.end(); myit++)
    if(myit->size() > 7)
      vec->push_back(*myit);

  mytolutc = 0; // Reset mytolutc for data
  time_hi_1 = time_hi_2 = 0;
  time_lo_1 = time_lo_2 = 0;
}

bool USBstream::GetNextTimeStamp(DataVector *vec)
{
  static long int mucounter = 0;
  static long int spycounter = 0;
  static float avglength = 0;
  unsigned long int tmp = 0;
  while(1) {
    if(myit==myvec.end())
      return false;

    vec->push_back(*myit);

    // Begin Diagnostics
    ++mucounter;
    if( ( ((*myit)[0] >> 8) & 0x7f) % 10 == 2 ) {
      if( ( ((*myit)[0] >> 8) & 0x7f) % 20 != 12  || (myusb != 18 && myusb != 19) ) {
        avglength = (avglength*(float)spycounter
                    + (float)((*myit)[0] & 0xff))/(float)(spycounter+1);
        ++spycounter;
      }
    }
    // End Diagnostics

    if((*myit).size()>4) { // Look for next time stamp and break
      tmp = (*myit)[1] << 8;
      if(tmp + (*myit)[2] > ((mytolutc >> 16) & 0xffff))
        break;
      else if(tmp + (*myit)[2] == ((mytolutc >> 16) & 0xffff)) {
        tmp = (*myit)[3] << 8;
        if(tmp + (*myit)[4] > (mytolutc & 0xffff))
          break;
      }
    }
    myit++;
  }
  tmp = ((*myit)[1] << 8) + (*myit)[2];
  mytolutc = (tmp << 16) + ((*myit)[3] << 8) + (*myit)[4];
  myit++;

  log_msg(LOG_INFO, "Number of muon events found in usb stream %d: %ld (%ld) (%f)\n",
         myusb,mucounter,spycounter,avglength);
  mucounter = 0;
  spycounter = 0;
  avglength = 0;

  return true;
}

int USBstream::LoadFile(std::string nextfile)
{
  sprintf(myfilename,"%s_%d",nextfile.c_str(),GetUSB());

  struct stat myfileinfo;
  if(!IsOpen) {
    myFile = new std::fstream(myfilename, std::fstream::in | std::fstream::binary);
    if(myFile->is_open()) {
      if(stat(myfilename, &myfileinfo) == 0 && //get file size
         myfileinfo.st_size) {
        IsOpen = true;
        return 1;
      }
      myFile->close();
      delete myFile; // Clean up
      log_msg(LOG_ERR,"USB %d has died. Exiting.\n",myusb);
      return -1;
    }
    else {
      delete myFile;
    }
  }
  else{
    return 1;
  }

  log_msg(LOG_INFO, "Waiting for files...\n");
  return 0;
}

bool USBstream::decode()
{
  // Yes, it appears that this really is supposed to be one less than 0x10000
  // as it fills it in 0xffff size increments below.
  char filedata[0xffff];

  if(!myFile->is_open()) {
    log_msg(LOG_CRIT, "File not open! Exiting.\n");
    exit(1);
    return false;
  }

  if(myit == myvec.end())
    myvec.clear();
  else if(myit < myvec.end())
    myvec.assign(myit,myvec.end());

  Reset();

  if(stat(myfilename, &fileinfo) == 0) //get file size
    bytesleft = fileinfo.st_size;

  unsigned int long word = 0; // holds 24-bit word being built, must be unsigned int
  char exp = 0; // expecting this type next
  int counter=0;

  while(true){ //loop over buffer packets
    counter++;
    if(bytesleft < 0xffff){
      fsize = bytesleft;
    }
    else {
      fsize = 0xffff;
      bytesleft -= 0xffff;
    }

    myFile->read(filedata, fsize);//read data of appropriate size

    for(int bytedex = 0; bytedex < fsize; bytedex++){ //loop over members in buffer
      char payload = filedata[bytedex] & 63;
      char pretype = (filedata[bytedex] >> 6) & 3;
      if(pretype == 0){ //not handling type very well.
        exp = 1;
        word = payload;
      }
      else {
        if(pretype == exp) {
          word = (word << 6) | payload;
          if(++exp == 4) {
            exp = 0;

            if(got_word(word)) { //24-bit word stored, process it
              restart = false;
              myit=myvec.end();
              myFile->seekg(std::ios::beg);
              data.clear();
              return false;
            }
          }
        }
        else {
          log_msg(LOG_ERR,"Found corrupted data in file %s\n",myfilename);
          exp = 0;
        }
      }
    }
    if(fsize==bytesleft) //exit the loop after last read
      break;
  }
  //extra = true;  // Uncommenting these 2 lines assumes OVDAQ only
                   // writes complete packets to disk at start/end of files
  //flush_extra(); // For now this is not true

  while(myFile->is_open()) {
    myFile->close();
    usleep(100);
  }
  delete myFile;
  IsOpen = false;

  myit=myvec.begin();
  return true;
}

bool USBstream::got_word(unsigned long int d)
{
  words++;
  char type = (d >> 22) & 3;
  if(type == 1) { // command word, not data
    flush_extra();
  }
  else if(type == 3) {
    if(check_debug(d))
      return restart;

    data.push_back((unsigned int)(d & 0xffff));//segfault this line
    check_data();
  }
  return false;
}

void USBstream::check_data()
{
  while(1) {
    bool got_packet = false;
    const unsigned int got = data.size();

    if(got == 0)
      break;

    if(data[0] == 0xffff) { // check header
      if(got < 2)
        break;

      const unsigned int len = data[1] & 0xff;
      if(len > 1) {
        if(got < len + 1)
          break;
        unsigned int par = 0;
        const int module = (data[1] >> 8) & 0x7f;
        const int type = data[1] >> 15; // check to see if trigger packets
        std::vector<int> *packet=new std::vector<int>;
        bool hitarray1[64] = {0};
        bool hitarray2[64] = {0};

        for(unsigned int m = 1; m <= len; m++) {
          par ^= data[m];
          if(m < len) {
            if(m < 4) {
              // Handle trigger box packets
              // XXX Drop this whole concept for ProtoDUNE-SP CRT?
              if(m == 3 && IsFanUSB) {
                if(module >= 0 && module <= 63) {
                  int low = (data[m] & 0xffff) - offset[module];
                  if(low < 0) {
                    int high = packet->back() - 1;
                    if(high < 0) // sync packets come every 2^29 clock counts
                      high += (1 << 13);
                    packet->pop_back(); packet->push_back(high);
                    low += (1 << 16);
                    packet->push_back(low);
                  }
                  else {
                    packet->push_back(low);
                  }
                }
                else {
                  packet->push_back(data[m]);
                }
              }
              else {
                packet->push_back(data[m]);
              }
            }
            else {
              if(type &&
                 m%2 == 0 && // ADC charge
                 !(data[m+1] > 63 || data[m+1] < 0 || module < 0 || module > 63)) {
                data[m] -= baseline[module][data[m+1]];
                packet->push_back(data[m]);
                packet->push_back(data[m+1]);
                hitarray1[data[m+1]] = true;
                if(data[m]>mythresh)
                  hitarray2[data[m+1]] = true;
              }
              else {
                packet->push_back(data[m]);
              }
            }
          }
          if(m==1) {
            packet->push_back(time_hi_1);
            packet->push_back(time_hi_2);
            packet->push_back(time_lo_1);
            packet->push_back(time_lo_2);
          }
        }

        if(!par) { // check parity
          got_packet = true;
          flush_extra();
          first_packet = false;

          // This block builds muon events
          bool MuonEvent = false;
          if(UseThresh && type) {
            if(BothLayerThresh) {
              for(int i = 0; i<32; i++) {
                if(hitarray2[i] &&
                   ( hitarray2[adj1[i]] || hitarray2[adj2[i]] )){
                  MuonEvent = true;
                  break;
                }
              }
            }
            else {
              for(int i = 0; i<32; i++) {
                if(hitarray1[i] &&
                   ( hitarray2[adj1[i]] || hitarray2[adj2[i]] )){
                  MuonEvent = true;
                  break;
                }
                if(hitarray2[i] &&
                   ( hitarray1[adj1[i]] || hitarray1[adj2[i]] )){
                  MuonEvent = true;
                  break;
                }
              }
            }
          }
          else {
            MuonEvent = true;
          }

          if(packet->size() > 7 && // guaruntees at least 1 hit (size > 9 for 2 hits)
             MuonEvent) { // Mu-like double found for this event
            if( myvec.size() > 0 ) { //&& LessThan(*packet,myvec.back()) ) \{
              DataVectorIt InsertionSortIt = myvec.end();
              bool found = false;
              while(--InsertionSortIt >= myvec.begin()) {
                if(!LessThan(*packet,*InsertionSortIt)) {
                  // Due to edge strip trigger logic in the trigger box firmware,
                  // we find duplicate trigger box packets of the form:
                  // p, 15366, 8086, 6128, 0, 1100 0000 0000 0000
                  // p, 15366, 8086, 6131, 0, 1000 0000 0000 0000
                  // These packets come from the same coincidence and one
                  // packet is missing a hit because its partner was actually
                  // also satisfied the mu-like double criteria.
                  // We search for these duplicate trigger box packets and
                  // make an OR of the hit data.
                  if(IsFanUSB &&
                     // packets should be separated by no more than 3 clock cycles
                     // XXX Is this desirable for ProtoDUNE-SP CRT?
                     !LessThan(*InsertionSortIt,*packet,3) &&
                     // packets should come from the same module
                     ((packet->at(0) >> 8) & 0x7f) ==
                     ((InsertionSortIt->at(0) >> 8) & 0x7f)) {
                    // Trigger box packets all have a fixed size
                    (*InsertionSortIt)[7] |= (*packet)[7];
                    (*InsertionSortIt)[8] |= (*packet)[8];
                    found = true;
                    break;
                  }
                  myvec.insert(InsertionSortIt+1,*packet);
                  found = true;
                  break;
                }
              }
              if(!found) // Reached beginning of myvec
                myvec.insert(myvec.begin(),*packet);
            }
            else {
              myvec.push_back(*packet);
            }
          }
          //delete first few elements of data
          data.erase(data.begin(),data.begin()+len+1); //(no longer)
        }
        else {
          log_msg(LOG_ERR,"Found packet parity mismatch in USB stream %d",myusb);
        }
        delete packet;
      }
    }
    if(!got_packet){
      extra = true;
      data.pop_front();
    }
  }
}

/*
  # c1 dac pmt
  # c2 dac
  # c5 XXXX  -skipped
  # c6 data_hi    - number of received data words
  # c6 data_lo
  # c6 lost_hi     - number of lost data words
  # c6 lost_lo
  # c8 timestamp_hi
  # c9 timestamp_lo
*/

bool USBstream::check_debug(unsigned long int d)
{
  const unsigned int a = (d >> 16) & 0xff;
  d = d & 0xffff;

  if(a == 0xc8) {
    time_hi_1 = (d >> 8) & 0xff;
    time_hi_2 = d & 0xff;
    got_hi = true;
    return true;
  }
  else if(a == 0xc9) {
    if(got_hi) {
      time_lo_1 = (d >> 8) & 0xff;
      time_lo_2 = d & 0xff;
      got_hi = false;
      flush_extra();
      if(!mytolutc) { // Check to see if first time stamp found and if so, rewind file
        mytolutc = (time_hi_1 << 8) + time_hi_2;
        mytolutc = (mytolutc << 16) + (time_lo_1 << 8) + time_lo_2;
        restart = true;
        first_packet = true;
      }
    }
    return true;
  }
  else if(a == 0xc5) {
    word_index = 0;
    return true;
  }
  else if(a == 0xc6) {
    word_count[word_index++] = d;
    if(word_index == 4) {
      const long int t = (word_count[0] << 16) + word_count[1];
      const long int v = (word_count[2] << 16) + word_count[3];
      long int diff = t - v - words;
      if(diff < 0) {
        diff += (1 << 31);
        diff += (1 << 31); // XXX what?  Is this taking advantage of overflow?
                           // That's probably not a good idea.
      }
      flush_extra();
    }
    return true;
  }
  else if(a == 0xc1) {
    flush_extra();
    return true;
  }
  else if(a == 0xc2) {
    flush_extra();
    return true;
  }

  return false;
}

void USBstream::flush_extra()
{
  if(extra) {
    extra = false;

    // Ignore incomplete packets at the beginning of the run
    if(!first_packet && mytolutc)
      log_msg(LOG_NOTICE, "Found x packet in file %s\n",myfilename);
  }
}
