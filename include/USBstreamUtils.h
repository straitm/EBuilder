#ifndef __USBStreamUtils__
#define __USBStreamUtils__

#include "USBstream-TypeDef.h"
#include <string>

// get the configuration from the DCSpaceIP.config file --
// read IP configuration from file
char* config_string(const char* path, const char* key);

// Send message to screen and syslog
void log_msg(int priority, const char * const format, ...);

bool LessThan(const std::vector<int> & lhs,
              const std::vector<int> & rhs, int ClockSlew);

void start_log();

int GetDir(std::string dir, std::vector<std::string> &myfiles,
           int opt = 0, int opt2 = 0);

#endif
