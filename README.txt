Originally by M. Toups for Double Chooz, created 9/10/09, updated 11/24/09.
Now by M. Strait for ProtoDUNE-SP, adaption began 2017.

======================= Purpose and overview of behavior =======================

Combines data streams from individual modules' triggers into a single data
stream with "events", which are sequences of hits close together in time.
Optionally applies a software threshold per-channel.  Optionally also requires
hits to be parts of overlapping pairs within a module.

The event builder reads data from a directory specified by the user.  The
data will be read in lexicographic order, and must be named:

  ${unix_time_stamp}_${usb_number}

for instance:

  1506152664_23

Once the EBuilder is finished reading a file, it moves it into a subdirectory
called "decoded/" and renames it with the extension ".done".

================================== Compiling ===================================

Say "make".  There are no special dependencies.

============================== Output file format ==============================

The output file consists of a series of events followed by an end-of-run
marker.  Events consist of one or more module packets.  Module packets consist
of one or more hits from a single module.

All data is stored in big endian (a.k.a. network) format.

The format of an event, where each tick represents one bit, is:

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Magic number = 0x4556 = "EV" |    Number of module packets   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                        Unix Time stamp                        |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |               .         module packets        .               |
   |               .               .               .               |

  Number of module packets: Unsigned 16 bit integer.

    The number of module packets can be greater than the number of modules with
    hits if an event is created out of a long string of hits in several modules.

  Time stamp: Unsigned 32 bit integer.

    Equal to the Unix time stamp. The Unix time stamp itself is signed.
    However, as we do not expect to take any data prior to 1 Jan 1970, this is
    irrelevant.  You may use the first bit to allow data taking beyond 19 Jan
    2038, 3:14:08 UTC (until 2106, when that too will overflow).

    Note that Unix time stamps REPEAT in the case of a positive leap second
    (common!) and SKIP in the case of a negative leap second (none so far, but
    totally possible).  Really.  They are not a very good time stamp, so we
    need to keep an eye on potential problems related to this.

The format of an end-of-run marker is:

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |              Magic number = 0x53544F50 = "STOP"               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+


The format of a module packet is:

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Magic number | Count of hits |         Module number         |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                   62.5 MHz counter time stamp                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |               .              hits             .               |
   |               .               .               .               |

  Magic number: 0x4D = "M"

  Count of hits: Unsigned 8 bit integer.

  Module number: Unsigned 16 bit integer.

  Time stamp: Unsigned 32 bit integer

    Number of 16ns ticks since last sync pulse.  Shouldn't usually be more than
    2^29 - 1.

The format of a hit is:

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Magic number |    Channel    |             Charge            |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  Magic number: 0x48 = "H"

  Channel number: Unsigned 8 bit integer

    Actually only uses 5 bits (0-63).

  Charge: *Signed* 16 bit integer

    Stored in two's complement.  Represents a baseline-subtracted 12-bit ADC
    value.  So we could store this in 13 bits, but there's no great motivation
    to pack the data so closely, especially because the minimum size of a hit
    is 18 bits, which I would tend to pad out to 32 anyway.

============================ Configuration file format =========================
4 singly-spaced columns of the form:

USB_Serial PMT_Serial pmtboard_u pipedelay

The first 4 columns come from the MYSQL database that you can see with:

mysql -u CRT_DAQ -p
use crt;
SELECT USB_Serial, board_number, pmtboard_u, pipedelay FROM crt_downstream;
quit;

Where crt_downstream can be replaced by any of the tables that configure the
"manual" CRT perl DAQ scripts.  See test.config for an example.
