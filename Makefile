CXX= g++
LD = g++

SRCDIR  = ./src
BINDIR  = ./bin
TMPDIR  = ./tmp
INCDIR =  ./include
INC =  -I./include

ROOTCFLAGS   := $(shell root-config --cflags)
ROOTLDFLAGS  := $(shell root-config --ldflags)
ROOTLIBS     := $(shell root-config --libs)

PREFIX=/home/strait

CXXFLAGS      = -O2 -Wunused -Wall -Wextra -fPIC $(INC) \
                -I/usr/include/mysql -I$(PREFIX)/include/mysql++
LDFLAGS       = $(INC) # does nothing?
SOFLAGS       = -shared

CXXFLAGS     += $(ROOTCFLAGS)
LDFLAGS      += $(ROOTLDFLAGS)
LIBS          = $(ROOTLIBS)
LIBS         += -L/usr/lib64/mysql -L$(PREFIX)/lib -lmysqlclient -lmysqlpp
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

all: dir $(TARGET)

$(USBSTREAMSO):     $(USBSTREAMO)
	$(LD) $(SOFLAGS) $(LDFLAGS) $^ -o  $@
	@echo "$@ done"

$(TARGET): $(USBSTREAMSO) $(EVENTBUILDERO)
	$(LD) $(LDFLAGS) $(EVENTBUILDERO) $(USBSTREAMLIB) $(LIBS) -o $@
	@echo "$@ done"

clean:
	@rm -rf $(BINDIR) $(TMPDIR) core $(SRCDIR)/*Dict*

$(TMPDIR)/%.o: $(SRCDIR)/%.cxx \
               $(INCDIR)/USBstream.h \
               $(INCDIR)/USBstream-TypeDef.h \
               $(INCDIR)/USBstreamUtils.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

dir:
	@mkdir -p $(BINDIR) $(TMPDIR)
