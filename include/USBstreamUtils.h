// Send message to screen and syslog. If the message is at level
// LOG_CRIT or worse, exit with status 1. (LOG_CRIT is the most severe
// level that should be used since more severe levels, by convention,
// indicate system-wide problems.)
void log_msg(const int priority, const char * const format, ...);

void start_log();

bool LessThan(const std::vector<int32_t> & lhs,
              const std::vector<int32_t> & rhs, int ClockSlew);
