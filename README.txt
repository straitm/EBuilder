M. Toups
Created: 9/10/09
Updated: 11/24/09

Compilation: ROOT must be properly installed on your system.
The Event Builder has been tested with root522 compiled with SL5 and gcc 4.4.0

The OV event builder assumes the existence of the following paths or files:
  1. OV DAQ raw data parent folder:         "/snap/data1/OVDAQ/DATA"
  2. OV EBuilder output data folder:        "/data/OVDAQ/DATA/"
  3. Dummy file for semaphore creation:     "/var/tmp/OV_EBuilder.txt"

The OV event builder assumes the individual run data folders found in 1, when
sorted as strings, will be in chronological order. This is fulfilled with the
current OV naming convention: Run_${yr}${mon}${day}_${24hr}_${min}

The OV event builder assumes OV raw data files have the following name convention:
  a. ${unix_time_stamp}_${usb_no}.wr        As they are being written by OV DAQ
  b. ${unix_time_stamp}_${usb_no}           After being closed by OV DAQ
  c. ${unix_time_stamp}_${usb_no}.done      After being processed by OV EBuilder

By default the OV EBuilder tries to process the oldest 5 files it finds with naming
convention b after checking that these belong to distinct USB streams. If it cannot
find 5 files that fit this criterion, it looks for the newest run in the folder 1.

To compile the code type "make" (typing "make clean" will reset).
An executable called EventBuilder will be created. It can be run in 2 modes
indicated by a flag passed as an argument to the program:
   ./EventBuilder <use DOGSifier>

To run in offline mode, use:
   ./EventBuilder 0

In this mode, the semaphore is still created to communicate with the DOGSifier, but
it is never incremented.

To run in online (DOGSifier) mode, use:
   ./EventBuilder 1


This mode should be used in conjunction with the DOGSifier OVreader, a first version
of which has been written and is working on OV data generated at Nevis. We are
currently testing the data rates that the DOGSifier can handle and for now we
have to artificially slow down the OV event builder to not break the DOGSifier.
In the event the OV event builder outputs a data rate too high for the DOGSifier,
it switches to offline mode. In this case, it must be restarted to re-enter online
mode.

Known issues:
  1. Does not delete or move binary files after processing them
  2. Must be slowed down artificially to work with the DOGSifier
  3. May be a small memory leak preventing it from running for days continuously
