#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <syslog.h>

#include <vector>

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
  const int64_t dt_unix = (int64_t)lhs.timeunix - rhs.timeunix;

  if(labs(dt_unix) > 1) return dt_unix < 0; // Timestamps are not adjacent

  const int64_t dt_16ns = (int64_t)lhs.time16ns - rhs.time16ns;

  // Found comment "Was sync pulse 2sec (sqrd)"
  // I do not understand what that means.  Is this supposed to be 0x2000 such
  // that this indicates a clock overflow after missing a sync pulse?  That
  // produces different results.
  if(labs(dt_16ns) > 2000 * (1 << 16)) return dt_16ns > 0;

  return dt_16ns < -ClockSlew;
}
