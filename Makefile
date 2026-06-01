# Top-level convenience Makefile: builds both components.
#
#   make          -> build tracker/tracker and client/client
#   make clean    -> remove all build artifacts

.PHONY: all tracker client clean

all: tracker client

tracker:
	$(MAKE) -C tracker

client:
	$(MAKE) -C client

clean:
	$(MAKE) -C tracker clean
	$(MAKE) -C client clean
