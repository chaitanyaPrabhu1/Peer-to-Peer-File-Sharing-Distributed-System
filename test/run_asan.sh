#!/bin/bash
# Run the basic scenario under ASan/UBSan. Trackers are killed (-9) so only the
# cleanly-exiting clients produce leak reports -- exactly what we want to check.
set -u
cd "$(dirname "$0")"
TRK=./tracker_asan
CLI=./client_asan
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=0
rm -f a_*.log agot_*.bin

tail -f /dev/null | "$TRK" tracker_info.txt 0 >a_t0.log 2>&1 &
T0=$!
tail -f /dev/null | "$TRK" tracker_info.txt 1 >a_t1.log 2>&1 &
T1=$!
sleep 2

{
  echo "create_user alice s"; echo "login alice s"
  echo "create_group grp"
  echo "upload_file grp big.bin"; echo "upload_file grp small.bin"
  sleep 6
  echo "accept_request grp bob"
  sleep 14
  echo "stop_share grp small.bin"
  echo "logout"
  echo "quit"
} | "$CLI" 127.0.0.1:6000 tracker_info.txt >a_alice.log 2>&1 &
A=$!

{
  sleep 3
  echo "create_user bob b"; echo "login bob b"
  echo "join_group grp"
  sleep 6
  echo "download_file grp big.bin agot_big.bin"
  echo "download_file grp small.bin agot_small.bin"
  sleep 8
  echo "show_downloads"
  echo "logout"; echo "quit"
} | "$CLI" 127.0.0.1:6001 tracker_info.txt >a_bob.log 2>&1 &
B=$!

wait $A $B
kill -9 $T0 $T1 2>/dev/null
pkill -f "tail -f /dev/null" 2>/dev/null
sleep 0.3

echo "=== integrity ==="
cmp -s big.bin agot_big.bin && echo "OK big" || echo "FAIL big"
cmp -s small.bin agot_small.bin && echo "OK small" || echo "FAIL small"
echo "=== sanitizer findings in client logs (empty == clean) ==="
grep -nE "ERROR: (AddressSanitizer|LeakSanitizer)|runtime error|definitely lost|indirectly lost|SUMMARY: (AddressSanitizer|UndefinedBehaviorSanitizer)" a_alice.log a_bob.log || echo "NONE FOUND -- clean"