#ifndef __USBStreamUtils__
#define __USBStreamUtils__

#include <stdint.h>
#include <string>
#include <vector>

// get the configuration from the DCSpaceIP.config file --
// read IP configuration from file
char* config_string(const char* path, const char* key);

// Send message to screen and syslog
void log_msg(int priority, const char * const format, ...);

bool LessThan(const std::vector<int32_t> & lhs,
              const std::vector<int32_t> & rhs, int ClockSlew);

void start_log();

#endif
