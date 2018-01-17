#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <syslog.h>

#include <vector>

#include "USBstream-TypeDef.h"

void log_msg(const int priority, const char * const format, ...)
{
  va_list ap;
  va_start(ap, format);
  vprintf(format, ap);

  // Always send the message to syslog, regardless of level. Syslog
  // policy set by the machine administrator determines which priority
  // levels actually get written to the log.  See
  // https://www.gnu.org/software/libc/manual/html_node/Syslog.html
  va_start(ap, format);
  vsyslog(LOG_MAKEPRI(LOG_DAEMON, priority), format, ap);

  // On Linux, more severe levels are lower numbers, but nothing I've
  // read suggests that this is standardized, so check individually.
  if(priority == LOG_CRIT || priority == LOG_ALERT || priority == LOG_EMERG)
    exit(1);
}

void start_log()
{
  openlog("OV EBuilder", LOG_NDELAY, LOG_USER);
  log_msg(LOG_NOTICE, "OV Event Builder Started\n");
}

/* Returns true if the packet 'lhs' is earlier in time than 'rhs' */
bool LessThan(const decoded_packet & lhs,
              const decoded_packet & rhs, const int ClockSlew)
{
  const int64_t dt_unix_hi = (int64_t)(lhs.timeunix >> 16) - (rhs.timeunix >> 16),
                dt_unix_lo = (int64_t)(rhs.timeunix & 0xffff) - (rhs.timeunix & 0xffff);
  const int64_t dt_16ns_high = (int64_t)(lhs.time16ns >> 16) - (rhs.time16ns >> 16);
  const int64_t dt_16ns_low  = (int64_t)(lhs.time16ns & 0xffff) - (rhs.time16ns & 0xffff);

  if(dt_unix_hi != 0) return dt_unix_hi < 0; // Very different timestamps
  if(labs(dt_unix_lo) > 1) return dt_unix_lo < 0; // Timestamps are not adjacent

  // Was sync pulse 2sec (sqrd)
  if(labs(dt_16ns_high) > 2000) return dt_16ns_high > 0;

  if(dt_16ns_high != 0) return dt_16ns_high < 0; // Order hi 16 bits of clock counter

  return dt_16ns_low < -ClockSlew; // Order lo 16 bits of clock counter
}
