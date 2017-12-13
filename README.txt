Originally by M. Toups for Double Chooz, created 9/10/09, updated 11/24/09.
Now by M. Strait for ProtoDUNE-SP, adaption began 2017.

Combines data streams from individual modules' triggers into a single data
stream with "events".

The event builder assumes the existence of the following paths or files:
  1. DAQ raw data parent folder:         "/data1/OVDAQ/DATA"
  2. EBuilder output data folder:        "/data/OVDAQ/DATA/"

The event builder assumes the individual run data folders found in 1, when
sorted as strings, will be in chronological order. This is fulfilled with the
current naming convention: Run_${yr}${mon}${day}_${24hr}_${min}

The event builder assumes raw data files have the following name convention:
  a. ${unix_time_stamp}_${usb_no}.wr        As they are being written by DAQ
  b. ${unix_time_stamp}_${usb_no}           After being closed by DAQ
  c. ${unix_time_stamp}_${usb_no}.done      After being processed by EBuilder

By default the EBuilder tries to process the oldest 5 files it finds with naming
convention b after checking that these belong to distinct USB streams. If it cannot
find 5 files that fit this criterion, it looks for the newest run in the folder 1.

Compile with "make".
