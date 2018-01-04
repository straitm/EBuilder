#include "USBstream-TypeDef.h"
#include "USBstream.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <stdint.h>

USBstream::USBstream()
{
  mythresh=0;
  myusb=-1;
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
  myvec.reserve(0xffff);
  myit=myvec.begin();
  IsOpen = false;
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


void USBstream::SetThresh(int thresh, int threshtype)
{
  //threshtype: 0=NONE, 1=OR, 2=AND
  UseThresh = (bool)threshtype;
  BothLayerThresh=(bool)(threshtype-1);
  if(thresh)
    mythresh=thresh;
  else
    mythresh = -20; // Put SW threshold well below HW threshold (including spread)

}

void USBstream::SetBaseline(
  const int baseptr[64 /* maxModules */][64 /* numChannels*/])
{
  for(int i = 0; i < 64; i++)
    for(int j = 0; j < 64; j++)
      baseline[i][j] = std::max(0, baseptr[i][j]);
}

void USBstream::GetBaselineData(DataVector *vec)
{
  vec->clear();
  for(myit = myvec.begin(); myit != myvec.end(); myit++) {
    if(myit->size() > 7) {
      vec->push_back(*myit);
    }
  }
  time_hi_1 = time_hi_2 = 0;
  time_lo_1 = time_lo_2 = 0;
}

// Empirically, returns true if it finds a time stamp past the location of myit
// when called.  Otherwise, returns false and sets myit to myvec.end(). Also
// fills vec with whatever it iterates past.
bool USBstream::GetNextTimeStamp(DataVector *vec)
{
  while(1) {
    if(myit==myvec.end())
      return false;

    vec->push_back(*myit);

    if((*myit).size()>4) { // Look for next time stamp and break
      uint64_t tmp = (*myit)[1] << 8;
      if(tmp + (*myit)[2] > ((mytolutc >> 16) & 0xffff)) {
        break;
      }
      else if(tmp + (*myit)[2] == ((mytolutc >> 16) & 0xffff)) {
        tmp = (*myit)[3] << 8;
        if(tmp + (*myit)[4] > (mytolutc & 0xffff)) {
          break;
        }
      }
    }
    myit++;
  }
  const uint64_t tmp = ((*myit)[1] << 8) + (*myit)[2];
  mytolutc = (tmp << 16) + ((*myit)[3] << 8) + (*myit)[4];
  myit++;

  return true;
}

int USBstream::LoadFile(std::string nextfile)
{
  std::ostringstream smyfilename;
  smyfilename << nextfile << "_" << GetUSB();
  myfilename = smyfilename.str();

  struct stat myfileinfo;
  if(!IsOpen) {
    myFile = new std::fstream(myfilename.c_str(),
                              std::fstream::in | std::fstream::binary);
    if(myFile == NULL || myFile->is_open()) {
      if(stat(myfilename.c_str(), &myfileinfo) == 0 &&
         myfileinfo.st_size) {
        IsOpen = true;
        return 1;
      }
      myFile->close();
      delete myFile;
      printf("USB %d has died. Exiting.\n",myusb);
      return -1;
    }
    else {
      fprintf(stderr, "Could not open %s\n", myfilename.c_str());
      delete myFile;
      return -1;
    }
  }
  else return 1;

  std::cout << "Waiting for files...\n";
  return 0;
}

bool USBstream::decode()
{
  char filedata[0xffff];//data buffer

  if(!myFile->is_open()) {
    std::cerr << "File not open! Exiting.\n";
    exit(1);
    return false;
  }

  if(myit==myvec.end()) {
    myvec.clear();
  }
  else if(myit<myvec.end()) {
    myvec.assign(myit,myvec.end());
  }

  Reset();

  struct stat fileinfo;
  if(stat(myfilename.c_str(), &fileinfo) == 0) //get file size
    bytesleft = fileinfo.st_size;

  uint64_t word = 0; // holds 24-bit word being built, must be unsigned
  char exp = 0;        // expecting this type next
  int counter=0;

  while(true)//loop over buffer packets
    {
      counter++;
      if(bytesleft < 0xffff)
        fsize = bytesleft;

      else
        {
          fsize = 0xffff;
          bytesleft -= 0xffff;
        }

      myFile->read(filedata, fsize);//read data of appropriate size

      for(int bytedex = 0; bytedex < fsize; bytedex++)//loop over members in buffer
        {
          char payload = filedata[bytedex] & 63;
          char pretype = (filedata[bytedex] >> 6) & 3;
          if(pretype == 0) //not handling type very well.
            {
              exp = 1;
              word = payload;
            }
          else
            {
              if(pretype == exp)
                {
                  word = (word << 6) | payload; //bitwise OR
                  if(++exp == 4)
                    {
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
              else
                {
                  printf("Found corrupted data in file %s: expected %d, got %d\n",
                         myfilename.c_str(), exp, pretype);
                  exp = 0;
                }
            }
        }
      if(fsize==bytesleft) //exit the loop after last read
        break;
    }
  //extra.push_back(data); // Uncommenting these lines assumes OVDAQ only
                           // writes complete packets to disk at start/end of files
  //flush_extra();         // For now this is not true

  if(myFile->is_open()) myFile->close();
  delete myFile;
  IsOpen = false;

  myit=myvec.begin();
  return true;
}

bool USBstream::got_word(uint64_t d)
{
  words++;
  char type = (d >> 22) & 3;
  if(type == 1)
    { // command word, not data
      flush_extra();
    }
  else if(type == 3)
    {
      if(check_debug(d))
        return restart;

      data.push_back(d & 0xffff);
      check_data();
    }
  return 0;
}

void USBstream::check_data()
{
  while(1)
  {
    bool got_packet = false;
    size_t got = data.size();

    if(!got)
      break;
    if(data[0] == 0xffff)
    {            // check header
      if(!(got >= 2))
        break;
      unsigned int len = data[1] & 0xff;
      if(len > 1)
      {
        if(!(got >= len + 1))
          break;
        unsigned int par = 0;
        int8_t module = (data[1] >> 8) & 0x7f;
        int8_t type = data[1] >> 15; // check to see if trigger packets
        std::vector<int32_t> packet;
        bool hitarray1[64] = {0};
        bool hitarray2[64] = {0};

        for(unsigned int m = 1; m <= len; m++)
        {
          par ^= data[m];
          if(m<len) {
            if(m<4) {
              packet.push_back(data[m]);
            }
            else {
              if(type) {
                if(m%2==0) { // ADC charge
                  if(data[m+1]>63 || data[m+1]<0 || module<0 || module>63) {
                  }
                  else {
                    data[m] -= baseline[module][data[m+1]];
                    packet.push_back(data[m]);
                    packet.push_back(data[m+1]);
                    hitarray1[data[m+1]] = true;
                    if(data[m]>mythresh) {
                      hitarray2[data[m+1]] = true;
                    }
                  }
                }
              }
              else { packet.push_back(data[m]); }
            }
          }
          if(m==1) {
            packet.push_back(time_hi_1);
            packet.push_back(time_hi_2);
            packet.push_back(time_lo_1);
            packet.push_back(time_lo_2);
          }
        }

        if(!par)
        { // check parity
          got_packet = true;
          flush_extra();
          if(first_packet) { first_packet = false; }

          // This block builds muon events
          bool MuonEvent = false;
          if(UseThresh && type) {
            if(BothLayerThresh) {
              for(int i = 0; i<32; i++) {
                if( hitarray2[i]) {
                  if( hitarray2[adj1[i]] || hitarray2[adj2[i]] ){
                    MuonEvent = true;
                    break;
                  }
                }
              }
            }
            else {
              for(int i = 0; i<32; i++) {
                if(hitarray1[i]) {
                  if( hitarray2[adj1[i]] || hitarray2[adj2[i]] ){
                    MuonEvent = true;
                    break;
                  }
                }
                if(hitarray2[i]) {
                  if( hitarray1[adj1[i]] || hitarray1[adj2[i]] ){
                    MuonEvent = true;
                    break;
                  }
                }
              }
            }
          }
          else {
            MuonEvent = true;
          }
          if(packet.size() > 7) { // guarantees at least 1 hit (size > 9 for 2 hits)
            if(MuonEvent) { // Mu-like double found for this event
              if( myvec.size() > 0 ) {
                DataVector::iterator InsertionSortIt = myvec.end();
                bool found = false;
                while(--InsertionSortIt >= myvec.begin()) {
                  if(!LessThan(packet,*InsertionSortIt, 0)) {
                    // Due to edge strip trigger logic in the trigger box firmware,
                    // we find duplicate trigger box packets of the form:
                    // p, 15366, 8086, 6128, 0, 1100 0000 0000 0000
                    // p, 15366, 8086, 6131, 0, 1000 0000 0000 0000
                    // These packets come from the same coincidence and one
                    // packet is missing a hit because its partner was actually
                    // also satisfied the mu-like double criteria.
                    // We search for these duplicate trigger box packets and
                    // make an OR of the hit data
                    myvec.insert(InsertionSortIt+1,packet);
                    found = true;
                    break;
                  }
                }

                // Reached beginning of myvec
                if(!found) { myvec.insert(myvec.begin(),packet); }
              }
              else { myvec.push_back(packet); }
            }
          }
          //delete first few elements of data
          data.erase(data.begin(),data.begin()+len+1); //(no longer)
        }
        else {
          printf("Found packet parity mismatch in USB stream %d\n",myusb);
        }
      }
    }
    if(!got_packet)
    {
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

bool USBstream::check_debug(uint64_t d)
{
  uint32_t a = (d >> 16) & 0xff;
  d = d & 0xffff;

  if(a == 0xc8)
    {
      time_hi_1 = (d >> 8) & 0xff;
      time_hi_2 = d & 0xff;
      got_hi = true;
      return 1;
    }
  else if(a == 0xc9)
    {
      if(got_hi)
        {
          time_lo_1 = (d >> 8) & 0xff;
          time_lo_2 = d & 0xff;
          got_hi = false;
          flush_extra();

          // Check to see if first time stamp found and if so, rewind file
          if(!mytolutc) {
            mytolutc = (time_hi_1 << 8) + time_hi_2;
            mytolutc = (mytolutc << 16) + (time_lo_1 << 8) + time_lo_2;
            restart = true;
            first_packet = true;
          }
        }
      return 1;
    }
  else if(a == 0xc5)
    {
      word_index = 0;
      return 1;
    }
  else if(a == 0xc6)
    {
      word_count[word_index++] = d;
      if(word_index == 4)
        {
          int64_t t = (word_count[0] << 16) + word_count[1];
          int64_t v = (word_count[2] << 16) + word_count[3];
          int64_t diff = t - v - words;
          if(diff < 0) { diff += (1 << 31); diff += (1 << 31); }
          flush_extra();
        }
      return 1;
    }
  else if(a == 0xc1)
    {
      flush_extra();
      return 1;
    }
  else if(a == 0xc2)
    {
      flush_extra();
      return 1;
    }
  else
    return 0;
}

void USBstream::flush_extra()
{
  if(extra) {
    extra = false;

    // Ignore incomplete packets at the beginning of the run
    if(!first_packet && mytolutc) {
      printf("Found extra packet (?) in file %s\n",myfilename.c_str());
    }
  }
}
