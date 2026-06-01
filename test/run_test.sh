#!/bin/bash
# End-to-end test: 2 trackers, 2 clients on DIFFERENT trackers (exercises sync),
# multi-piece + tiny + empty file downloads with integrity verification.
set -u
cd "$(dirname "$0")"
TRK=../tracker/tracker
CLI=../client/client

rm -f t0.log t1.log seeder.log downloader.log got_*.bin

# --- start two trackers (tail keeps their stdin open so they don't exit) ---
tail -f /dev/null | "$TRK" tracker_info.txt 0 >t0.log 2>&1 &
T0=$!
tail -f /dev/null | "$TRK" tracker_info.txt 1 >t1.log 2>&1 &
T1=$!
sleep 1

# --- seeder: alice on tracker 0 ---
{
  echo "create_user alice secret"
  echo "login alice secret"
  echo "create_group grp"
  echo "upload_file grp big.bin"
  echo "upload_file grp small.bin"
  echo "upload_file grp empty.bin"
  sleep 6        # wait for bob to request to join
  echo "list_requests grp"
  echo "accept_request grp bob"
  sleep 20       # stay online to seed
  echo "quit"
} | "$CLI" 127.0.0.1:6000 tracker_info.txt >seeder.log 2>&1 &
S=$!

# --- downloader: bob on tracker 1 (different tracker!) ---
{
  sleep 3
  echo "create_user bob pass"
  echo "login bob pass"
  echo "join_group grp"
  sleep 6        # wait for alice to accept
  echo "list_files grp"
  echo "download_file grp big.bin got_big.bin"
  echo "download_file grp small.bin got_small.bin"
  echo "download_file grp empty.bin got_empty.bin"
  sleep 8
  echo "show_downloads"
  sleep 2
  echo "quit"
} | "$CLI" 127.0.0.1:6001 tracker_info.txt >downloader.log 2>&1 &
D=$!

wait $S $D
kill $T0 $T1 2>/dev/null
pkill -f "tail -f /dev/null" 2>/dev/null
sleep 0.3

echo "=================== seeder.log ==================="
cat seeder.log
echo "=================== downloader.log ==================="
cat downloader.log
echo "=================== integrity check ==================="
ok=1
for f in big small empty; do
  if [ -f got_${f}.bin ] && cmp -s ${f}.bin got_${f}.bin; then
    echo "OK   $f matches"
  else
    echo "FAIL $f mismatch/missing"; ok=0
  fi
done
echo "=================== tracker logs ==================="
echo "--- t0 ---"; cat t0.log
echo "--- t1 ---"; cat t1.log
[ $ok -eq 1 ] && echo "ALL FILES VERIFIED" || echo "SOME FILES FAILED"
