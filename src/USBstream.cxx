#include <syslog.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h> // For htons, htonl
#include <sys/types.h>
#include <sys/stat.h>

#include <fstream>
#include <sstream>
#include <vector>
#include <deque>

#include "USBstream.h"
#include "USBstreamUtils.h"

USBstream::USBstream()
{
  mythresh=0;
  myusb=-1;
  got_unix_time_hi = false;
  unix_time_hi = 0;
  unix_time_lo = 0;
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

void USBstream::SetOffset(const int module, const int off)
{
  if(module < 0 || module >= 64){
     log_msg(LOG_WARNING, "Ignoring attempt to set offset on module %d\n", module);
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

void USBstream::GetBaselineData(std::vector<decoded_packet> *vec)
{
  if(!vec->empty())
    log_msg(LOG_CRIT, "Expected vec to be empty for GetBaselineData()\n");

  for(std::vector<decoded_packet>::iterator i = sortedpackets.begin();
      i != sortedpackets.end(); i++)
    if(!i->hits.empty())
      vec->push_back(*i);

  // Done with baselines. Clear this to be ready for the main data.
  sortedpackets.clear();

  unix_time_hi = unix_time_lo = 0;
}

// Appends all decoded data to 'vec' up to the next change of Unix time stamp
// or the end of the decoded data, whichever is sooner.  In the former case,
// returns true, otherwise false.
bool USBstream::GetDecodedDataUpToNextUnixTimeStamp(
  std::vector<decoded_packet> & vec)
{
  if(sortedpacketsptr == sortedpackets.end()){
    log_msg(LOG_NOTICE, "No decoded data to send (Unix time stamp %lu) "
      "for USB %d\n", mytolutc, myusb);
    return false;
  }

  for( ; sortedpacketsptr != sortedpackets.end(); sortedpacketsptr++) {
    vec.push_back(*sortedpacketsptr);

    if(sortedpacketsptr->hits.empty()) continue;

    const uint32_t new_time = sortedpacketsptr->timeunix;

    if(new_time > mytolutc) break;
  }

  if(sortedpacketsptr == sortedpackets.end()){
    log_msg(LOG_NOTICE, "Sent decoded data up to end (Unix time stamp "
      "%lu) for USB %d\n", mytolutc, myusb);
    return false;
  }

  mytolutc = sortedpacketsptr->timeunix;

  log_msg(LOG_NOTICE, "Sent decoded data up to Unix time stamp %lu for "
    "USB %d\n", mytolutc, myusb);

  sortedpacketsptr++; // Point at the next packet after the Unix timestamp

  return true;
}

int USBstream::LoadFile(const std::string & nextfile)
{
  std::ostringstream smyfilename;
  smyfilename << nextfile << "_" << GetUSB();
  myfilename = smyfilename.str();

  struct stat myfileinfo;
  if(myFile == NULL || !myFile->is_open()) {
    myFile = new std::fstream(myfilename.c_str(),
                              std::fstream::in | std::fstream::binary);
    if(myFile == NULL || myFile->is_open()) {
      if(stat(myfilename.c_str(), &myfileinfo) == 0 &&
         myfileinfo.st_size)
        return 1;
      myFile->close();
      delete myFile;
      log_msg(LOG_ERR, "USB %d has died. Exiting.\n", myusb);
      return -1;
    }
    else {
      log_msg(LOG_ERR, "Could not open %s\n", myfilename.c_str());
      delete myFile;
      return -1;
    }
  }
  else return 1;

  log_msg(LOG_NOTICE, "Waiting for files...\n");
  return 0;
}

void USBstream::decodefile()
{
  top: // we return here if triggered by restart leading from finding
       // the first Unix timestamp packet, which means we have to go
       // back and assign the time to each hit that came before that packet.

  const unsigned int BUFSIZE = 0x10000;

  char filedata[BUFSIZE];//data buffer

  if(!myFile->is_open()) log_msg(LOG_CRIT, "File not open! Exiting.\n");

  // Throw out what has already been passed on up
  if(sortedpacketsptr <= sortedpackets.end())
    sortedpackets.assign(sortedpacketsptr, sortedpackets.end());

  got_unix_time_hi = false;

  struct stat fileinfo;
  if(stat(myfilename.c_str(), &fileinfo) == -1)
    log_msg(LOG_CRIT, "File %s stopped being readable!\n", myfilename.c_str());

  unsigned int bytesleft = fileinfo.st_size;
  unsigned int bytestoread = 0;

  uint32_t word = 0; // holds 24-bit word being built, must be unsigned
  char expcounter = 0; // expecting this counter next

  do{
    bytestoread = std::min(BUFSIZE, bytesleft);
    bytesleft -= bytestoread;

    myFile->read(filedata, bytestoread);

    /*
      Undocumented input file format is revealed by inspection to be
      constructed like this:

      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |0 0|     A     |0 1|      B    |1 0|     C     |1 1|     D     |
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

     Where the bits of A, B, C, and D concatenated make the 24-bit words
     described in Matt Toups' thesis.
   */

    for(unsigned int bytedex = 0; bytedex < bytestoread; bytedex++){
      const char counter = (filedata[bytedex] >> 6) & 3;
      const char payload = filedata[bytedex] & 0x3f;
      if(counter == 0){
        expcounter = 1;
        word = payload;
      }
      else if(counter == expcounter){
        word = (word << 6) | payload;
        if(++expcounter == 4){
          expcounter = 0;

          if(raw24bit_to_raw16bit(word)) { //24-bit word stored, process it
            sortedpackets.clear();
            raw16bitdata.clear();
            myFile->seekg(std::ios::beg);
            goto top;
          }
        }
      }
      else{
        log_msg(LOG_WARNING, "Found corrupted data in file %s: "
          "expected %d, got %d\n", myfilename.c_str(), expcounter, counter);
        expcounter = 0;
      }
    }
  }while(bytestoread != bytesleft);

  if(myFile->is_open()) myFile->close();
  delete myFile;
  myFile = NULL;

  sortedpacketsptr = sortedpackets.begin();
}

/* This would be better named "process_word()". Returns true if we need
 * to rewind to the beginning of the file. */
bool USBstream::raw24bit_to_raw16bit(uint32_t in24bitword)
{
  // Old comment here said "command word, not data" for the case that
  // the first two bits were 01b. Apparently there are 24 bit words
  // undocumented in Matt Toups' thesis that start with values other
  // than 11b, but we just ignore them.
  if(((in24bitword >> 22) & 3) == 3) {
    if(handle_unix_time_words(in24bitword)) return true;

    raw16bitdata.push_back(in24bitword & 0xffff);
    raw16bit_to_packets();
  }
  return false;
}

// Return true if the hits in this module packet satisfy the cuts
bool USBstream::ThresholdCut(const bool * const allhits,
                             const bool * const threshits)
{
  for(int i = 0; i < 32; i++) {
    // If this strip and an overlapping strip are over threshold
    if(BothLayerThresh &&
       threshits[i] && (threshits[adj1[i]] || threshits[adj2[i]]))
      return true;

    // If this strip is hit and an overlapping strip is over threshold
    // or an overlapping strip is over threshold and this channel is hit
    if(!BothLayerThresh &&
       ((allhits  [i] && (threshits[adj1[i]] || threshits[adj2[i]])) ||
        (threshits[i] && (allhits  [adj1[i]] || allhits  [adj2[i]]))))
      return true;
  }
  return false;
}

/* This function was called "check_data", but it is clearly not just
 * checking.  It is decoding. */
void USBstream::raw16bit_to_packets()
{
  // ADC packet word indices.  As per Toups thesis:
  //
  // 0xffff                         | Header word
  // 1, mod#[7 bits], wdcnt[8 bits] | Data type, module #, word count
  // time                           | High 16 bits of 62.5 MHz clock counter
  // time                           | Low  16 bits of 62.5 MHz clock counter
  // adc                            | ADC ...
  // channel                        | ... and channel number, repeated N times
  // parity                         | Parity
  enum ADC_WINX { ADC_WIDX_HEAD   = 0,
                  ADC_WIDX_MODLEN = 1,
                  ADC_WIDX_CLKHI  = 2,
                  ADC_WIDX_CLKLO  = 3,
                  ADC_WIDX_HIT    = 4 };

  // Try to decode the data in 'data'. Stop trying if 'data' is empty, or
  // if it starts out right with 0xffff but has nothing else, or if it is
  // shorter than the length it claims to have.  But otherwise, drop the
  // first word and try to decode again.
  while(1) {
    bool got_packet = false;

    if(raw16bitdata.empty()) break;

    // First word of all packets other than unix timestamp packets is 0xffff
    if(raw16bitdata[0] == 0xffff) {
      if(raw16bitdata.size() < 2) break;

      unsigned int len = raw16bitdata[ADC_WIDX_MODLEN] & 0xff;
      if(len == 0) continue;

      // we don't have all the data in this packet yet
      if(raw16bitdata.size() < len + 1) break;

      got_packet = true;

      unsigned int parity = 0;
      decoded_packet packet;
      packet.timeunix = ((uint32_t)unix_time_hi << 16) + unix_time_lo;
      packet.module = (raw16bitdata[ADC_WIDX_MODLEN] >> 8) & 0x7f;
      if(packet.module > 63)
        log_msg(LOG_ERR, "Invalid module number %u\n", packet.module);
      packet.isadc = raw16bitdata[ADC_WIDX_MODLEN] >> 15;
      bool allhits  [64] = {0}; // which channels were hit
      bool threshits[64] = {0}; // which channels were hit over threshold

      for(unsigned int wordi = ADC_WIDX_MODLEN; wordi < len; wordi++){
        parity ^= raw16bitdata[wordi];

        if(wordi == ADC_WIDX_CLKHI) {
          packet.time16ns |= (raw16bitdata[wordi] << 16);
        }
        else if(wordi == ADC_WIDX_CLKLO) {
          packet.time16ns |= raw16bitdata[wordi];
          packet.time16ns -= offset[packet.module];
        }
        else if(packet.isadc) { // we are in the words that give the hit info
          // hits start on even numbered words
          if(wordi%2 == 0 && raw16bitdata[wordi+1] < 64 && packet.module < 64) {
            decoded_hit hit;
            hit.channel = raw16bitdata[wordi+1];
            hit.charge  = raw16bitdata[wordi] - baseline[packet.module][hit.channel];
            packet.hits.push_back(hit);

            allhits[hit.channel] = true;
            if(hit.charge > mythresh) threshits[hit.channel] = true;
          }
        }
      }

      if(parity != raw16bitdata[len])
        log_msg(LOG_WARNING, "Parity error in USB stream %d\n", myusb);

      if(!UseThresh || !packet.isadc || ThresholdCut(allhits, threshits)){
        // Slot this packet into place in time order, searching from the end
        std::vector<decoded_packet>::iterator i = sortedpackets.end();
        while(i != sortedpackets.begin() && LessThan(packet, *(i-1), 0))
          i--;
        sortedpackets.insert(i, packet);
      }

      //delete the data that we've decoded into 'packet'
      raw16bitdata.erase(raw16bitdata.begin(), raw16bitdata.begin()+len+1);
    }

    if(!got_packet) raw16bitdata.pop_front();
  }
}

/*
  If the input 24 bit word is part of a Unix timestamp packet, as revealed
  by its control code (bits 3-8), set the Unix time on this USB stream, which
  will be attached to hits from now on.  If we didn't know the time before,
  signal that we need to rewind to the beginning of the file so these time
  can be set.  Returns true in this case, false otherwise.

  This function was named "check_debug". Here's the old top-of-function comment:

  # c1 dac pmt
  # c2 dac
  # c5 ....  -skipped
  # c6 data_hi    - number of received data words
  # c6 data_lo
  # c6 lost_hi     - number of lost data words
  # c6 lost_lo
  # c8 timestamp_hi
  # c9 timestamp_lo

  These refer to the control bytes of DAQ packets.
*/
bool USBstream::handle_unix_time_words(const uint32_t wordin)
{
  const uint8_t control = (wordin >> 16) & 0xff;
  const uint16_t payload = wordin & 0xffff;

  // Two cases were here in the old code, one for control == 0xc1 and
  // 0xc2. These are undocumented Toups thesis. Top of function says
  // "dac pmt" and "dac", respectively, which isn't much to go on. For
  // each, we end up doing nothing.
  //
  // For 0xc5, "skipped" and c6, "number of received data words", old
  // code did some arithmetic and then discarded the result. Let's not.

  // Unix-timestamp-high-bits packet - see appendix B.2 of Matt
  // Toups' thesis.
  if(control == 0xc8) {
    unix_time_hi = payload;
    got_unix_time_hi = true;
  }

  // Unix-timestamp-low-bits packet - B.2 of Toups thesis
  else if(control == 0xc9) {
    if(got_unix_time_hi) {
      unix_time_lo = payload;
      got_unix_time_hi = false;

      // So if we've been reading hits, but don't know what the Unix time
      // stamp is yet, now that we've found the Unix time stamp, set it
      // and start reading the file from the beginning, this time setting
      // the Unix time on each packet.
      if(!mytolutc) {
        mytolutc = ((uint32_t)unix_time_hi << 16) + unix_time_lo;
        return true; // rewind file and get times set this time
      }
    }
  }

  return false;
}
