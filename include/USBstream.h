#ifndef __USBstream_h__
#define __USBstream_h__

#include <USBstreamUtils.h>

#include <fstream>
#include <deque>
#include <sys/stat.h>

class USBstream {

public:

  USBstream();
  virtual ~USBstream();

  void SetUSB(int usb) { myusb=usb; }
  void SetThresh(int thresh, int threshtype);
  void SetIsFanUSB() { IsFanUSB = true; }
  void SetTOLUTC(unsigned long int tolutc) { mytolutc=tolutc; }
  void SetOffset(int *off);
  void SetBaseline(int **base);

  void Reset();

  int GetUSB() const { return myusb; }
  bool GetIsFanUSB() { return IsFanUSB; }
  int GetNPMT() const { return mynpmt; }
  char* GetFileName() { return myfilename; }
  unsigned long int GetTOLUTC() const { return mytolutc; }

  bool GetNextTimeStamp(DataVector *vec);
  void GetBaselineData(DataVector *vec);
  int LoadFile(std::string nextfile);
  bool decode();

private:

  int mythresh;
  int myusb;
  int mynpmt;
  int baseline[64][64]; // Data structure?
  int offset[64];
  int adj1[64];
  int adj2[64];
  unsigned long int mytolutc;
  char myfilename[100]; // XXX looks dangerous
  std::fstream *myFile;
  bool IsOpen;
  bool IsFanUSB;
  bool BothLayerThresh;
  bool UseThresh;
  DataVector myvec;
  DataVectorIt myit;

  // These functions are for the decoding
  bool got_word(unsigned long int d);
  void check_data();
  bool check_debug(unsigned long int d);
  void flush_extra();

  // These variables are for the decoding
  long int words;
  bool got_hi;
  bool restart;
  bool first_packet;
  int time_hi_1;
  int time_hi_2;
  int time_lo_1;
  int time_lo_2;
  int timestamps_read;
  std::deque<int> data;           // 11
  bool extra = false;          // leftovers
  unsigned int word_index;
  unsigned int word_count[4];
  int bytesleft;
  int fsize;
};

class OVHitData {

public:

  OVHitData() {}
  virtual ~OVHitData() {}

  void SetHit(OVSignal sig) { fHit = sig; }

  OVSignal GetHit() const { return fHit; }

  char GetChannel() const { return fHit.first; }
  short int GetCharge() const { return fHit.second; }

private:

  OVSignal fHit;
};

class OVEventHeader {

public:

  OVEventHeader() {}
  virtual ~OVEventHeader() {}

  void SetNOVDataPackets(char npackets) { fNOVDataPackets = npackets; }
  void SetTimeSec(long int time_s) { fTimeSec = time_s; }

  char GetNOVDataPackets() const { return fNOVDataPackets; }
  long int GetTimeSec() const { return fTimeSec; }

protected:

  char fNOVDataPackets;
  long int fTimeSec;

};

class OVDataPacketHeader {

public:

  OVDataPacketHeader() {}
  virtual ~OVDataPacketHeader(){}

  void SetNHits(char nh) { fNHits = nh; }
  void SetModule(short int mod) { fModule = mod; }
  void SetType(char type) { fDataType = type; }
  void SetTime16ns(long int time_16ns) { fTime16ns = time_16ns; }

  char GetNHits() const { return fNHits; }
  short int GetModule() const { return fModule; }
  char GetType() const { return fDataType; }
  long int GetTime16ns() const { return fTime16ns; }

private:

  char fNHits;
  char fDataType;
  short int fModule;
  long int fTime16ns;
};

#endif
