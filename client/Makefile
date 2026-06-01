# Makefile for the client application.
#
# Shared modules (SHA1, sockets, utilities) live in ../common and are compiled
# in directly. `make` produces the ./client binary.

CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread -I../common
LDFLAGS  := -pthread

COMMON_SRC := ../common/sha1.cpp ../common/net.cpp ../common/util.cpp
SRC        := client.cpp tracker_conn.cpp peer_server.cpp download.cpp $(COMMON_SRC)

TARGET := client

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET) *.o ../common/*.o

.PHONY: all clean
