#ifndef __USBstream_h__
#define __USBstream_h__

#include <USBstreamUtils.h>

#include <fstream>
#include <deque>
#include <sys/stat.h>
#include <string.h>
#include <stdint.h>

class USBstream {

public:

  USBstream();

  void SetUSB(int usb) { myusb=usb; }
  void SetThresh(int thresh, int threshtype);
  void SetIsFanUSB() { IsFanUSB = true; }
  void SetTOLUTC(uint64_t tolutc) { mytolutc=tolutc; }
  void SetOffset(int *off);
  void SetBaseline(const int base[64 /* maxModules */][64 /* numChannels */]);

  void Reset();

  int GetUSB() const { return myusb; }
  bool GetIsFanUSB() { return IsFanUSB; }
  int GetNPMT() const { return mynpmt; }
  const char* GetFileName() { return myfilename.c_str(); }
  uint64_t GetTOLUTC() const { return mytolutc; }

  bool GetNextTimeStamp(DataVector *vec);
  void GetBaselineData(DataVector *vec);
  int LoadFile(const std::string nextfile);
  bool decode();

private:

  int mythresh;
  int myusb;
  int mynpmt;
  int baseline[64 /* maxModules */][64 /* numChannels */];
  int offset[64];
  int adj1[64];
  int adj2[64];
  uint64_t mytolutc;
  std::string myfilename;
  std::fstream *myFile;
  bool IsOpen;
  bool IsFanUSB;
  bool BothLayerThresh;
  bool UseThresh;
  DataVector myvec;
  DataVector::iterator myit;

  // These functions are for the decoding
  bool got_word(uint64_t d);
  void check_data();
  bool check_debug(uint64_t d);
  void flush_extra();

  // These variables are for the decoding
  int64_t words;
  bool got_hi;
  bool restart;
  bool first_packet;
  int32_t time_hi_1;
  int32_t time_hi_2;
  int32_t time_lo_1;
  int32_t time_lo_2;
  int timestamps_read;

  // XXX this is signed, but gets things cast to unsigned pushed into it.
  // Probably they get cast back and forth and happen to be right in the end...
  std::deque<int32_t> data;      // 11

  bool extra;                     // leftovers
  unsigned int word_index;
  unsigned int word_count[4];
  int bytesleft;
  int fsize;
};

struct OVHitData {

  OVHitData()
  { // See comments in OVEventHeader
    memset(this, 0, sizeof(*this));
    // "HIT DATA"
    compatibility = 0x4154414420544948;
  }

  void SetHit(const int8_t channel_, const int16_t charge_)
  {
    channel = channel_;
    charge = charge_;
  }

  uint64_t compatibility;
  int8_t channel;
  int16_t charge;
};

struct OVEventHeader {

  OVEventHeader()
  { // See comments in OVEventHeader
    memset(this, 0, sizeof(*this));
    // "EVENT HD"
    compatibility = 0x444820544E455645;
  }

  void SetNOVDataPackets(const int8_t npackets) { n_ov_data_packets = npackets; }
  void SetTimeSec(const uint32_t time_s) {  time_sec = time_s; }

  int8_t GetNOVDataPackets() const { return n_ov_data_packets; }
  uint32_t GetTimeSec() const { return time_sec; }

  // When I found this class, it had a virtual destructor and was being
  // write()ten and read() to a file in its entirety.  This means that the
  // vtable pointer went to the file and then trashed the vtable pointer in the
  // read()ing program.  Not good, except that because they were compiled with
  // the same version of gcc, maybe it was the same value?  Or maybe it simply
  // was never dereferenced because no virtual functions were ever called?  In
  // any case, it primarily is causing me grief because it writes uninteresting
  // and unpredictable values into the output file, which makes it hard to
  // check whether the program is producing output that matches my reference
  // output file from Camillo.  To keep the layout the same, for now I am
  // writing out the same length of meaningful text.  I am *not* going to be reading
  // these files back in with the Double Chooz "Dogsifier", so there's no
  // reason to retain this compatibility after initial testing.
  uint64_t compatibility;
  int8_t n_ov_data_packets;
  uint64_t time_sec;
};

struct OVDataPacketHeader {

  OVDataPacketHeader()
  { // See comments in OVEventHeader
    memset(this, 0, sizeof(*this));
    // "PACKETHD"
    compatibility = 0x444854454B434150;
  }

  void SetNHits(int8_t nh) { fNHits = nh; }
  void SetModule(int16_t mod) { fModule = mod; }
  void SetType(int8_t type) { fDataType = type; }
  void SetTime16ns(int64_t time_16ns) { fTime16ns = time_16ns; }

  int8_t GetNHits() const { return fNHits; }
  int16_t GetModule() const { return fModule; }
  int8_t GetType() const { return fDataType; }
  int64_t GetTime16ns() const { return fTime16ns; }

private:

  uint64_t compatibility;

  int8_t fNHits;
  int8_t fDataType;
  int16_t fModule;
  int64_t fTime16ns;
};

#endif
