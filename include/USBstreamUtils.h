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

// Send message to screen and syslog. If the message is at level
// LOG_CRIT or worse, exit with status 1. (LOG_CRIT is the most severe
// level that should be used since more severe levels, by convention,
// indicate system-wide problems.)
void log_msg(const int priority, const char * const format, ...);

void start_log();

bool LessThan(const decoded_packet & lhs,
              const decoded_packet & rhs, const int ClockSlew);
