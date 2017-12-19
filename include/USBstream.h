#ifndef __USBstream_h__
#define __USBstream_h__

#include <USBstreamUtils.h>

#include <fstream>
#include <deque>
#include <arpa/inet.h> // For htons, htonl
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
  bool writeout(const int fd)
  {
    const uint8_t magic = 'H';
    if(1 != write(fd, &magic, 1)) return false;

    if(1 != write(fd, &channel, 1)) return false;

    const uint16_t ncharge = htons(charge);
    if(sizeof ncharge != write(fd, &ncharge, sizeof ncharge)) return false;

    return true;
  }

  uint16_t magic;
  uint8_t channel;
  int16_t charge;
};

struct OVEventHeader {
  bool writeout(const int fd)
  {
    const uint16_t magic = htons(0x4556); // "EV"
    if(sizeof magic != write(fd, &magic, sizeof magic)) return false;

    const uint16_t nnov = htons(n_ov_data_packets);
    if(sizeof nnov != write(fd, &nnov, sizeof nnov)) return false;

    const uint32_t ntime_sec = htonl(time_sec);
    if(sizeof ntime_sec != write(fd, &ntime_sec, sizeof ntime_sec)) return false;

    return true;
  }

  uint16_t n_ov_data_packets;
  uint32_t time_sec;
};

struct OVDataPacketHeader {
  bool writeout(const int fd)
  {
    const uint16_t magic = htons(0x444D); // "MD"
    if(sizeof(magic) != write(fd, &magic, sizeof magic)) return false;

    if(1 != write(fd, &nHits, 1)) return false;

    const uint16_t nmodule = htons(module);
    if(sizeof(nmodule) != write(fd, &nmodule, sizeof nmodule)) return false;

    const uint32_t ntime16ns = htonl(time16ns);
    if(sizeof(ntime16ns) != write(fd, &ntime16ns, sizeof ntime16ns)) return false;

    return true;
  }

  uint8_t nHits;
  uint16_t module;
  uint32_t time16ns; // Was int64_t, but I think is always {0..0xffffffff}
};

#endif
