#ifndef __USBStreamUtils__
#define __USBStreamUtils__

#include "USBstream-TypeDef.h"
#include <string>

#define BUFSIZE   1024
#define MNAME            -1
#define MINSTRUCTION      1
#define MNOTICE           2
#define MWARNING          3
#define MERROR            4
#define MEXCEPTION        5

//get the configuration from the DCSpaceIP.config file --
// read IP configuration from file

char* config_string(const char* path, const char* key);

// Send gaibu message and write to the syslog
void gaibu_msg(int priority, char *gaibu_buf, std::string myRunNumber="");

bool LessThan(DataPacket lhs, DataPacket rhs, int ClockSlew=0);

void start_gaibu();

int GetDir(std::string dir, std::vector<std::string> &myfiles,
           int opt = 0, int opt2 = 0);

#endif
