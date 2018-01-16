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
  myvec.reserve(0x10000);
  myit=myvec.begin();
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

  for(myit = myvec.begin(); myit != myvec.end(); myit++)
    if(myit->size() > /* >= ? */ MIN_ADC_PACKET_SIZE)
      vec->push_back(*myit);

  unix_time_hi = unix_time_lo = 0;
}

// Empirically, returns true if it finds a *Unix* time stamp past the location
// of myit when called.  Otherwise, returns false and sets myit to myvec.end().
// Also fills vec with whatever it iterates past in myvec.
bool USBstream::GetDecodedDataUpToNextUnixTimeStamp(DataVector & vec)
{
  if(myit == myvec.end()){
    log_msg(LOG_NOTICE, "No decoded data to send (Unix time stamp %lu) "
      "for USB %d\n", mytolutc, myusb);
    return false;
  }

  for( ; myit != myvec.end(); myit++) {
    vec.push_back(*myit);

    if(myit->size() <= 4) continue;

    const uint64_t new_time = ((uint64_t)(*myit)[1] << 16)
                            + ((uint64_t)(*myit)[2]      );

    if(new_time > mytolutc) break;
  }

  if(myit == myvec.end()){
    log_msg(LOG_NOTICE, "Sent decoded data up to end (Unix time stamp "
      "%lu) for USB %d\n", mytolutc, myusb);
    return false;
  }

  mytolutc = ((uint64_t)(*myit)[1] << 16)
           + ((uint64_t)(*myit)[2]      );

  log_msg(LOG_NOTICE, "Sent decoded data up to Unix time stamp %lu for "
    "USB %d\n", mytolutc, myusb);

  myit++;

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

  if     (myit == myvec.end()) myvec.clear();
  else if(myit  < myvec.end()) myvec.assign(myit, myvec.end());

  Reset();

  struct stat fileinfo;
  if(stat(myfilename.c_str(), &fileinfo) == -1)
    log_msg(LOG_CRIT, "File %s stopped being readable!\n", myfilename.c_str());

  unsigned int bytesleft = fileinfo.st_size;
  unsigned int bytestoread = 0;

  uint64_t word = 0; // holds 24-bit word being built, must be unsigned
  char exp = 0;        // expecting this type next

  do{
    bytestoread = std::min(BUFSIZE, bytesleft);
    bytesleft -= bytestoread;

    myFile->read(filedata, bytestoread);

    for(unsigned int bytedex = 0; bytedex < bytestoread; bytedex++){
      const char payload = filedata[bytedex] & 0x3f;
      const char pretype = (filedata[bytedex] >> 6) & 3;
      if(pretype == 0){ //not handling type very well.
        exp = 1;
        word = payload;
      }
      else if(pretype == exp){
        word = (word << 6) | payload;
        if(++exp == 4){
          exp = 0;

          if(got_word(word)) { //24-bit word stored, process it
            restart = false;
            myit = myvec.end();
            myFile->seekg(std::ios::beg);
            data.clear();
            goto top;
          }
        }
      }
      else{
        log_msg(LOG_WARNING, "Found corrupted data in file %s: "
          "expected %d, got %d\n", myfilename.c_str(), exp, pretype);
        exp = 0;
      }
    }
  }while(bytestoread != bytesleft);

  //extra.push_back(data); // Uncommenting these lines assumes OVDAQ only
                           // writes complete packets to disk at start/end of files
  //flush_extra();         // For now this is not true

  if(myFile->is_open()) myFile->close();
  delete myFile;
  myFile = NULL;

  myit = myvec.begin();
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

      unsigned int len = data[1] & 0xff;
      if(len > 1) {
        if(data.size() < len + 1) break;

        unsigned int parity = 0;
        uint8_t module = (data[1] >> 8) & 0x7f;
        uint8_t type = data[1] >> 15; // check to see if trigger packets
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
            if(type) {
              if(wordi%2 == 0) { // ADC charge
                if(data[wordi+1] < 64 && module < 64) {
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

        if(packet.size() > MIN_ADC_PACKET_SIZE) {
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
  # c5 ....  -skipped
  # c6 data_hi    - number of received data words
  # c6 data_lo
  # c6 lost_hi     - number of lost data words
  # c6 lost_lo
  # c8 timestamp_hi
  # c9 timestamp_lo

  These refer to the control bytes of DAQ packets.
*/
bool USBstream::check_debug(uint64_t wordin)
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
      flush_extra();

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
  else if(control == 0xc1) {
    flush_extra();
    return 1;
  }

  // Not explained in Toups thesis.  Top of function says "dac"...
  else if(control == 0xc2) {
    flush_extra();
    return 1;
  }
  else{
    return 0;
  }
}

void USBstream::flush_extra()
{
  if(!extra) return;

  extra = false;

  // Ignore incomplete packets at the beginning of the run
  if(!first_packet && mytolutc)
    log_msg(LOG_WARNING, "Extra packet (?) in file %s\n", myfilename.c_str());
}
