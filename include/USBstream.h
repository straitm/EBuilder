class USBstream {

public:

  USBstream();

  void SetUSB(int usb) { myusb=usb; }
  void SetThresh(int thresh, int threshtype);

  // Set per-module timing offset on this USB stream.  As per Camillo:
  //
  // This is a feature that is included in the firmware of the pmt
  // board, in Double Chooz was used minimally only in the far detector
  // to take into account the difference between cable length between
  // the lower and upper outer veto. Now for the CRT I do not know if we
  // will have all cables related to clock and sync of the same length
  // or if we are going to have different cable length between the
  // front  and the back of the CRT.
  void SetOffset(const int module, const int off);

  void SetBaseline(const int base[64 /* maxModules */][64 /* numChannels */]);

  void Reset();

  int GetUSB() const { return myusb; }
  const char* GetFileName() { return myfilename.c_str(); }
  uint64_t GetTOLUTC() const { return mytolutc; }

  bool GetDecodedDataUpToNextUnixTimeStamp(DataVector & vec);
  void GetBaselineData(DataVector *vec);
  int LoadFile(const std::string nextfile);
  bool decode();

private:

  int mythresh;
  int myusb;
  int baseline[64 /* maxModules */][64 /* numChannels */];
  int offset[64 /* maxModules */];
  int adj1[64];
  int adj2[64];
  uint64_t mytolutc;
  std::string myfilename;
  std::fstream *myFile;
  bool IsOpen;
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
    const uint8_t magic = 0x4D; // "M"
    if(1 != write(fd, &magic, 1)) return false;

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
