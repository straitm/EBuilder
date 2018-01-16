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

#include "USBstream-TypeDef.h"
#include "USBstream.h"
#include "USBstreamUtils.h"

USBstream::USBstream()
{
  mythresh=0;
  myusb=-1;
  words = 0;
  got_hi = false;
  restart = false;
  first_packet = false;
  unix_time_hi = 0;
  unix_time_lo = 0;
  word_index = 0;
  word_count[0] = 0;
  word_count[1] = 0;
  word_count[2] = 0;
  word_count[3] = 0;
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

void USBstream::GetBaselineData(DataVector *vec)
{
  if(!vec->empty())
    log_msg(LOG_CRIT, "Expected vec to be empty for GetBaselineData()\n");

  for(DataVector::iterator i = sortedpackets.begin();
      i != sortedpackets.end(); i++)
    if(i->size() > /* >= ? */ MIN_ADC_PACKET_SIZE)
      vec->push_back(*i);

  // Done with baselines. Clear this to be ready for the main data.
  sortedpackets.clear();

  unix_time_hi = unix_time_lo = 0;
}

// Appends all decoded data to 'vec' up to the next change of Unix time stamp
// or the end of the decoded data, whichever is sooner.  In the former case,
// returns true, otherwise false.
bool USBstream::GetDecodedDataUpToNextUnixTimeStamp(DataVector & vec)
{
  if(sortedpacketsptr == sortedpackets.end()){
    log_msg(LOG_NOTICE, "No decoded data to send (Unix time stamp %lu) "
      "for USB %d\n", mytolutc, myusb);
    return false;
  }

  for( ; sortedpacketsptr != sortedpackets.end(); sortedpacketsptr++) {
    vec.push_back(*sortedpacketsptr);

    if(sortedpacketsptr->size() <= 2) continue;

    const uint32_t new_time = ((uint32_t)(*sortedpacketsptr)[1] << 16)
                            + ((uint32_t)(*sortedpacketsptr)[2]      );

    if(new_time > mytolutc) break;
  }

  if(sortedpacketsptr == sortedpackets.end()){
    log_msg(LOG_NOTICE, "Sent decoded data up to end (Unix time stamp "
      "%lu) for USB %d\n", mytolutc, myusb);
    return false;
  }

  mytolutc = ((uint32_t)(*sortedpacketsptr)[1] << 16)
           + ((uint32_t)(*sortedpacketsptr)[2]      );

  log_msg(LOG_NOTICE, "Sent decoded data up to Unix time stamp %lu for "
    "USB %d\n", mytolutc, myusb);

  sortedpacketsptr++; // Point at the next packet after the Unix timestamp

  return true;
}

int USBstream::LoadFile(std::string nextfile)
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

void USBstream::decode()
{
  top: // we return here if triggered by restart leading from finding
       // something-something-something about a Unix timestamp packet
       // on the line marked beltshortcrimefight.

  const unsigned int BUFSIZE = 0x10000;

  char filedata[BUFSIZE];//data buffer

  if(!myFile->is_open()) log_msg(LOG_CRIT, "File not open! Exiting.\n");

  // Throw out what has already been passed on up
  if(sortedpacketsptr <= sortedpackets.end())
    sortedpackets.assign(sortedpacketsptr, sortedpackets.end());

  Reset();

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

          if(got_word(word)) { //24-bit word stored, process it
            sortedpackets.clear();
            data.clear();
            myFile->seekg(std::ios::beg);
            restart = false;
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

/* This would be better named "process_word()" */
bool USBstream::got_word(uint32_t d)
{
  words++;
  char type = (d >> 22) & 3;
  if(type == 1) {
    // Old comment here said "command word, not data". Apparently there
    // are 24 bit words undocumented in Matt Toups' thesis that start
    // with 01b instead of 11b, but we just ignore them.
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

    bool got_packet = false;

    if(data.empty()) break;

    // First word of all packets other than unix timestamp packets is 0xffff
    if(data[0] == 0xffff) {
      if(data.size() < 2) break;

      unsigned int len = data[ADC_WIDX_MODLEN] & 0xff;
      if(len == 0) continue;
      if(data.size() < len + 1) break;

      unsigned int parity = 0;
      const uint8_t module = (data[ADC_WIDX_MODLEN] >> 8) & 0x7f;
      const bool isadcpacket = data[ADC_WIDX_MODLEN] >> 15;
      std::vector<uint16_t> packet;
      bool hitarray1[64] = {0};
      bool hitarray2[64] = {0};

      for(unsigned int wordi = ADC_WIDX_MODLEN; wordi < len; wordi++){
        parity ^= data[wordi];

        if(wordi == ADC_WIDX_CLKLO) {
          if(module <= 63) {
            const int low = (int)data[wordi] - offset[module];
            if(low < 0) {
              const int high = (int)packet.back() - 1;

              // sync packets come every 2^29 clock counts
              const int highcorr = high < 0? high + (1 << 13): high;

              packet.pop_back();
              packet.push_back(highcorr);
              packet.push_back(low + (1 << 16));
            }
            else {
              packet.push_back(low);
            }
          }
          else {
            log_msg(LOG_ERR, "Invalid module number %u\n", module);
            packet.push_back(data[wordi]);
          }
        }
        else if(wordi < ADC_WIDX_HIT) {
          packet.push_back(data[wordi]);
        }
        else { // we are in the words that give the hit info
          if(isadcpacket) {
            if(wordi%2 == 0 && // ADC charge
               data[wordi+1] < 64 && module < 64) {
              // Baseline subtraction can make ADC negative.  Use signed
              // value here, but do not modify the value in packet. This
              // is done when writing out.
              const int adc = data[wordi] - baseline[module][data[wordi+1]];
              if(adc > mythresh) hitarray2[data[wordi+1]] = true;

              packet.push_back(data[wordi]);
              packet.push_back(data[wordi+1]);
              hitarray1[data[wordi+1]] = true;
            }
          }
          else { packet.push_back(data[wordi]); }
        }

        if(wordi == ADC_WIDX_MODLEN) {
          packet.push_back(unix_time_hi);
          packet.push_back(unix_time_lo);
        }
      }

      if(parity != data[len])
        log_msg(LOG_WARNING, "Parity error in USB stream %d\n", myusb);

      got_packet = true;
      if(first_packet) first_packet = false;

      // This block builds muon events
      bool MuonEvent = false;
      if(UseThresh && isadcpacket) {
        if(BothLayerThresh) {
          for(int i = 0; i<32; i++) {
            if(hitarray2[i] &&
               (hitarray2[adj1[i]] || hitarray2[adj2[i]])){
              MuonEvent = true;
              break;
            }
          }
        }
        else {
          for(int i = 0; i<32; i++) {
            if(hitarray1[i] &&
               (hitarray2[adj1[i]] || hitarray2[adj2[i]])){
              MuonEvent = true;
              break;
            }
            if(hitarray2[i] &&
               (hitarray1[adj1[i]] || hitarray1[adj2[i]])){
              MuonEvent = true;
              break;
            }
          }
        }
      }
      else {
        MuonEvent = true;
      }

      if(packet.size() > MIN_ADC_PACKET_SIZE) {
        if(MuonEvent) { // Mu-like double found for this event
          if(!sortedpackets.empty()) {
            DataVector::iterator InsertionSortIt = sortedpackets.end();
            bool found = false;
            while(--InsertionSortIt >= sortedpackets.begin()) {
              if(!LessThan(packet, *InsertionSortIt, 0)) {
                sortedpackets.insert(InsertionSortIt+1, packet);
                found = true;
                break;
              }
            }

            // Reached beginning of sortedpackets
            if(!found) sortedpackets.insert(sortedpackets.begin(), packet);
          }
          else {
            sortedpackets.push_back(packet);
          }
        }
      }

      //delete the data that we've decoded into 'packet'
      data.erase(data.begin(), data.begin()+len+1);
    }

    if(!got_packet) data.pop_front();
  }
}

/*
  This function is named "check_debug", but I don't think it is just
  checking or debugging.  It seems to be decoding and setting variables
  which are then used later.  Here's the old top-of-function comment:

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
bool USBstream::check_debug(uint32_t wordin)
{
  uint8_t control = (wordin >> 16) & 0xff;
  uint16_t payload = wordin & 0xffff;

  // Unix-timestamp-high-bits packet - see appendix B.2 of Matt
  // Toups' thesis.
  if(control == 0xc8) {
    unix_time_hi = payload;
    got_hi = true;
    return 1;
  }

  // Unix-timestamp-low-bits packet - B.2 of Toups thesis
  else if(control == 0xc9) {
    if(got_hi) {
      unix_time_lo = payload;
      got_hi = false;

      // Check to see if first time stamp found and if so, rewind file
      // (This cryptic line tagged: beltshortcrimefight)
      if(!mytolutc) {
        mytolutc = ((uint32_t)unix_time_hi << 16) + unix_time_lo;
        restart = true;
        first_packet = true;
      }
    }
    return 1;
  }

  // XXX undocumented packet type - only clue is "skipped" in the comment
  // at the top of this function.
  else if(control == 0xc5) {
    word_index = 0;
    return 1;
  }

  // Not explained in Toups thesis.  Top of function says
  //    c6 data_hi    - number of received data words
  //    c6 data_lo
  //    c6 lost_hi     - number of lost data words
  //    c6 lost_lo
  // which could be more helpful.
  else if(control == 0xc6) {
    word_count[word_index++] = payload;
    if(word_index == 4) {
      int32_t t = (word_count[0] << 16) + word_count[1];
      int32_t v = (word_count[2] << 16) + word_count[3];
      int32_t diff = t - v - words;
      if(diff < 0) { diff += (1 << 31); diff += (1 << 31); }
    }
    return 1;
  }

  // Not explained in Toups thesis.  Top of function says "dac pmt"
  // which isn't much to go on.
  else if(control == 0xc1) {
    return 1;
  }

  // Not explained in Toups thesis.  Top of function says "dac"...
  else if(control == 0xc2) {
    return 1;
  }
  else{
    return 0;
  }
}
