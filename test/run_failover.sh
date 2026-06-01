#!/bin/bash
# Failover test: both clients start on tracker 0. We KILL tracker 0 mid-session;
# the downloader must transparently fail over to tracker 1 (which has the synced
# state) and still complete the download from the seeder.
set -u
cd "$(dirname "$0")"
TRK=../tracker/tracker
CLI=../client/client
rm -f f_t0.log f_t1.log f_seeder.log f_downloader.log fgot_big.bin

tail -f /dev/null | "$TRK" tracker_info.txt 0 >f_t0.log 2>&1 &
T0=$!
tail -f /dev/null | "$TRK" tracker_info.txt 1 >f_t1.log 2>&1 &
T1=$!
sleep 1

# seeder alice on tracker 0
{
  echo "create_user alice secret"
  echo "login alice secret"
  echo "create_group grp"
  echo "upload_file grp big.bin"
  sleep 6
  echo "accept_request grp bob"
  sleep 20
  echo "quit"
} | "$CLI" 127.0.0.1:6000 tracker_info.txt >f_seeder.log 2>&1 &
S=$!

# downloader bob on tracker 0 -> will fail over to tracker 1 after the kill
{
  sleep 3
  echo "create_user bob pass"
  echo "login bob pass"
  echo "join_group grp"
  sleep 8        # by now tracker 0 has been killed
  echo "download_file grp big.bin fgot_big.bin"
  sleep 8
  echo "show_downloads"
  sleep 2
  echo "quit"
} | "$CLI" 127.0.0.1:6000 tracker_info.txt >f_downloader.log 2>&1 &
D=$!

# Kill tracker 0 at t=7s (after alice accepted bob, before bob downloads)
sleep 7
echo ">>> killing tracker 0 now"
kill -9 $T0 2>/dev/null

wait $S $D
kill $T1 2>/dev/null
pkill -f "tail -f /dev/null" 2>/dev/null
sleep 0.3

echo "=================== downloader.log ==================="
cat f_downloader.log
echo "=================== integrity ==================="
if cmp -s big.bin fgot_big.bin; then echo "OK big.bin downloaded after failover"; else echo "FAIL"; fi