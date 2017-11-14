#ifndef __USBStreamUtils__
#define __USBStreamUtils__

#include <string>
#include <vector>

// get the configuration from the DCSpaceIP.config file --
// read IP configuration from file
char* config_string(const char* path, const char* key);

// Send message to screen and syslog
void log_msg(int priority, const char * const format, ...);

bool LessThan(const std::vector<int> & lhs,
              const std::vector<int> & rhs, int ClockSlew);

void start_log();

// Fills myfiles with a list of files in the given directory.
//
// These files are the set that does not have a dot in their name or, if 'allowdots',
// are at least three characters long (I guess to exclude the . and ..
// directories?).
//
// Also exclude files with names containing "baseline" or "processed" unless
// 'allow_bl_and_pc' is true.
//
// Return true if no files are found that satisfy those rules, including if
// the directory couldn't be read.  Otherwise, returns false.
bool GetDir(const std::string dir, std::vector<std::string> &myfiles,
            const bool allowdots = false, const bool allow_bl_and_pc = false);

#endif
