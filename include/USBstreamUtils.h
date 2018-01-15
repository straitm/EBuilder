#ifndef __USBStreamUtils__
#define __USBStreamUtils__

#include <stdint.h>
#include <string>
#include <vector>

// Send message to screen and syslog
void log_msg(int priority, const char * const format, ...);

bool LessThan(const std::vector<int32_t> & lhs,
              const std::vector<int32_t> & rhs, int ClockSlew);

void start_log();

#endif
