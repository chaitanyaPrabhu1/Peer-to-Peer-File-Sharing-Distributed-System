#!/bin/bash
# Leak-check the tracker by driving it to a clean "quit" under ASan.
set -u
cd "$(dirname "$0")"
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=0

{ sleep 10; echo quit; } | ./tracker_asan tracker_info.txt 0 >tk_t0.log 2>&1 &
T0=$!
tail -f /dev/null | ../tracker/tracker tracker_info.txt 1 >tk_t1.log 2>&1 &
T1=$!
sleep 2
{
  echo "create_user u1 p"; echo "login u1 p"
  echo "create_group g"; echo "upload_file g small.bin"
  echo "join_group g"; echo "list_groups"; echo "list_files g"
  echo "list_requests g"; echo "stop_share g small.bin"; echo "leave_group g"
  sleep 3; echo "quit"
} | ../client/client 127.0.0.1:6000 tracker_info.txt >tk_cli.log 2>&1
wait $T0
kill -9 $T1 2>/dev/null; pkill -f "tail -f /dev/null" 2>/dev/null
echo "=== sanitizer findings in tracker (empty == clean) ==="
grep -nE "ERROR: (AddressSanitizer|LeakSanitizer)|runtime error|definitely lost|indirectly lost" tk_t0.log || echo "NONE -- clean"
echo "=== tracker tail ==="; tail -4 tk_t0.log