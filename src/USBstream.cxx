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

void USBstream::SetOffset(const int module, const int off)
{
  if(module < 0 || module >= 64){
     fprintf(stderr, "Ignoring attempt to set offset on module %d\n", module);
     return;
  }

  offset[module] = off;
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

// Empirically, returns true if it finds a *Unix* time stamp past the location
// of myit when called.  Otherwise, returns false and sets myit to myvec.end().
// Also fills vec with whatever it iterates past.
bool USBstream::GetNextTimeStamp(DataVector & vec)
{
  for( ; myit != myvec.end(); myit++) {
    vec.push_back(*myit);

    if(myit->size() <= 4) continue;

    const uint64_t new_time = ((uint64_t)(*myit)[1] << 24)
                            + ((uint64_t)(*myit)[2] << 16)
                            + ((uint64_t)(*myit)[3] <<  8)
                            + ((uint64_t)(*myit)[4]      );

    if(new_time > mytolutc) break;
  }

  if(myit == myvec.end()) return false;

  mytolutc = ((uint64_t)(*myit)[1] << 24)
           + ((uint64_t)(*myit)[2] << 16)
           + ((uint64_t)(*myit)[3] <<  8)
           + ((uint64_t)(*myit)[4]      );

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

/* This would be better named "process_word()" */
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

/* This function is called "check_data", but it is clearly not just
 * checking.  It is decoding. */
void USBstream::check_data()
{

  // Try to decode the data in 'data'. Stop trying if 'data' is empty, or
  // if it starts out right with 0xffff but has nothing else, or if it is
  // shorter than the length it claims to have.  But otherwise, drop the
  // first word and try to decode again.
  while(1)
  {
    bool got_packet = false;

    if(data.empty()) break;

    // First word of all packets other than unix timestamp packets is 0xffff
    if(data[0] == 0xffff) {
      if(data.size() < 2) break;

      unsigned int len = data[1] & 0xff;
      if(len > 1)
      {
        if(data.size() < len + 1) break;

        unsigned int parity = 0;
        int8_t module = (data[1] >> 8) & 0x7f;
        int8_t type = data[1] >> 15; // check to see if trigger packets
        std::vector<int32_t> packet;
        bool hitarray1[64] = {0};
        bool hitarray2[64] = {0};

        for(unsigned int m = 1; m <= len; m++)
        {
          parity ^= data[m];
          if(m<len) {
            if(m<4) {
              if(m==3) {
                if(module >= 0 && module <= 63) {
                  int low = (data[m] & 0xffff) - offset[module];
                  if(low < 0) {
                    int high = packet.back() - 1;
                    if(high < 0) { high += (1 << 13); } // sync packets come every 2^29 clock counts
                    packet.pop_back(); packet.push_back(high);
                    low += (1 << 16);
                    packet.push_back(low);
                  }
                  else { packet.push_back(low); }
                }
                else { packet.push_back(data[m]); }
              }
              else{
                packet.push_back(data[m]);
              }
            }
            else {
              if(type) {
                if(m%2==0) { // ADC charge
                  if(!(data[m+1]>63 || data[m+1]<0 || module<0 || module>63)) {
                    data[m] -= baseline[module][data[m+1]];
                    packet.push_back(data[m]);
                    packet.push_back(data[m+1]);
                    hitarray1[data[m+1]] = true;
                    if(data[m]>mythresh)
                      hitarray2[data[m+1]] = true;
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

        if(!parity)
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
                  if(!LessThan(packet, *InsertionSortIt, 0)) {
                    myvec.insert(InsertionSortIt+1, packet);
                    found = true;
                    break;
                  }
                }

                // Reached beginning of myvec
                if(!found)
                  myvec.insert(myvec.begin(), packet);
              }
              else {
                myvec.push_back(packet);
              }
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
  This function is named "check_debug", but I don't think it is just
  checking or debugging.  It seems to be decoding and setting variables
  which are then used later.  Here's the old top-of-function comment:

  # c1 dac pmt
  # c2 dac
  # c5 XXXX  -skipped
  # c6 data_hi    - number of received data words
  # c6 data_lo
  # c6 lost_hi     - number of lost data words
  # c6 lost_lo
  # c8 timestamp_hi
  # c9 timestamp_lo

  These refer to the control bytes of DAQ packets.
*/
bool USBstream::check_debug(uint64_t d)
{
  uint32_t a = (d >> 16) & 0xff;
  d = d & 0xffff;

  // Unix-timestamp-high-bits packet - see appendix B.2 of Matt
  // Toups' thesis.
  if(a == 0xc8)
    {
      time_hi_1 = (d >> 8) & 0xff;
      time_hi_2 = d & 0xff;
      got_hi = true;
      return 1;
    }

  // Unix-timestamp-low-bits packet - B.2 of Toups thesis
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

  // XXX undocumented packet type - only clue is "skipped" in the comment
  // at the top of this function.
  else if(a == 0xc5)
    {
      word_index = 0;
      return 1;
    }

  // Not explained in Toups thesis.  Top of function says
  //    c6 data_hi    - number of received data words
  //    c6 data_lo
  //    c6 lost_hi     - number of lost data words
  //    c6 lost_lo
  // which could be more helpful.
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

  // Not explained in Toups thesis.  Top of function says "dac pmt"
  // which isn't much to go on.
  else if(a == 0xc1)
    {
      flush_extra();
      return 1;
    }

  // Not explained in Toups thesis.  Top of function says "dac"...
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
