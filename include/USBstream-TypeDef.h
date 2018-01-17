// A hit after decoding.  Happens to be the same as a OVHitData, but
// semantically this is the in-memory format.
struct decoded_hit {
  uint8_t channel;
  int16_t charge;
};

// A module packet after decoding.
struct decoded_packet {
  decoded_packet()
  {
    isadc = 0;
    module = 0;
    timeunix = 0;
    time16ns = 0;
  }

  bool isadc; // ADC hits (true) or something else (false)
  uint16_t module;
  uint32_t timeunix;
  uint32_t time16ns;
  std::vector<decoded_hit> hits;
};
