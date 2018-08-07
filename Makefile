CXX= g++
LD = g++

SRCDIR  = ./src
BINDIR  = ./bin
TMPDIR  = ./tmp
INCDIR =  ./include
INC =  -I./include

PREFIX=/home/strait

CXXFLAGS      = -O2 -Wunused -Wall -Wextra -Wshadow $(INC)
LDFLAGS       = -pthread
SOFLAGS       = -shared

LIBS         += -L$(PREFIX)/lib
MAIN=EventBuilder.cxx
TARGET=$(MAIN:%.cxx=$(BINDIR)/%)

all: dir $(TARGET)
#------------------------------------------------------------------------------

USBSTREAMO       = $(TMPDIR)/USBstream.o
USBSTREAMUTILSO  = $(TMPDIR)/USBstreamUtils.o
EVENTBUILDERO    = $(TMPDIR)/EventBuilder.o

OBJS          = $(USBSTREAMO) $(USBSTREAMUTILSO) $(EVENTBUILDERO)

#------------------------------------------------------------------------------

.SUFFIXES: .cxx .o .so

all: dir $(TARGET)

$(TARGET): $(OBJS)
	$(LD) $(LDFLAGS) $(OBJS) $(LIBS) -o $@
	@echo "$@ done"

clean:
	@rm -rf $(BINDIR) $(TMPDIR) core $(SRCDIR)/*Dict*

$(TMPDIR)/%.o: $(SRCDIR)/%.cxx \
               $(INCDIR)/USBstream.h \
               $(INCDIR)/USBstreamUtils.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

dir:
	@mkdir -p $(BINDIR) $(TMPDIR)
