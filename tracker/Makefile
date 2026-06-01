# Makefile for the tracker server.
#
# Shared modules (SHA1, sockets, utilities) live in ../common and are compiled
# in directly, so the tracker/ directory together with ../common is fully
# self-contained. `make` produces the ./tracker binary.

CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread -I../common
LDFLAGS  := -pthread

COMMON_SRC := ../common/sha1.cpp ../common/net.cpp ../common/util.cpp
SRC        := tracker.cpp $(COMMON_SRC)
OBJ        := $(SRC:.cpp=.o)

TARGET := tracker

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET) *.o ../common/*.o

.PHONY: all clean
