CXX           = g++
LD            = g++
RM=rm -rf
CP=cp -f

SRCDIR  = ./src
BINDIR  = ./bin
TMPDIR  = ./tmp
INCDIR =  $(DCONLINE_PATH)/DCOV/EBuilder/include
INC =  -I$(DCONLINE_PATH)/DCOV/EBuilder/include
INC += -I$(DCONLINE_PATH)/DCDOGSifier
DATADIR = /data

ROOTCFLAGS   := $(shell root-config --cflags)
ROOTLDFLAGS  := $(shell root-config --ldflags)
ROOTLIBS     := $(shell root-config --libs)

CXXFLAGS        = -O2 -Wall -fPIC $(INC) -I/usr/include/mysql -I/usr/include/mysql++
LDFLAGS       = -O2 $(INC)
SOFLAGS       = -shared

CXXFLAGS       += $(ROOTCFLAGS)
LDFLAGS      += $(ROOTLDFLAGS)
LIBS          = $(ROOTLIBS)
LIBS	     += -L/usr/lib/mysql -lmysqlclient -lmysqlpp
MAIN=EventBuilder.cxx
TARGET=$(MAIN:%.cxx=$(BINDIR)/%)

all: dir $(TARGET)
#------------------------------------------------------------------------------

USBSTREAMO        = $(TMPDIR)/USBstream.o
USBSTREAMSO       = $(TMPDIR)/libUSBstream.so
USBSTREAMLIB      = $(shell pwd)/$(USBSTREAMSO)

EVENTBUILDERO    = $(TMPDIR)/EventBuilder.o
EVENTBUILDERS    = $(SRCDIR)/EventBuilder.cxx

OBJS          = $(USBSTREAMO) $(EVENTBUILDERO)

#------------------------------------------------------------------------------

.SUFFIXES: .cxx .o .so

all:            dir $(TARGET)

$(USBSTREAMSO):     $(USBSTREAMO)
		$(LD) $(SOFLAGS) $(LDFLAGS) $^ -o  $@
		@echo "$@ done"

$(TARGET):	$(USBSTREAMSO) $(EVENTBUILDERO)
		$(LD) $(LDFLAGS) $(EVENTBUILDERO) $(USBSTREAMLIB) $(LIBS) -o $@
		@echo "$@ done"

clean:
		@rm -rf $(BINDIR) $(TMPDIR) core
		@rm -f $(SRCDIR)/*Dict*

$(TMPDIR)/%.o: $(SRCDIR)/%.cxx $(INCDIR)/USBstream.h $(INCDIR)/USBstream-TypeDef.h $(INCDIR)/USBstreamUtils.h
	$(CXX)  $(CXXFLAGS) -c $< -o $@

dir:
	@ echo '<< Creating OV EBuilder directory structure >>'
	@ if [ ! -d $(BINDIR) ];then mkdir $(BINDIR) ;fi
	@ if [ ! -d $(TMPDIR) ];then mkdir $(TMPDIR) ;fi
	@ if [ ! -f /var/tmp/OV_EBuilder.txt ];then touch /var/tmp/OV_EBuilder.txt ;fi
	@ echo '<< Creating OV EBuilder directory structure succeeded >>'
