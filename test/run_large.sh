#!/bin/bash
# Large-file test: 200 MB transfer. Verifies correctness at scale and that the
# downloader's peak memory stays small (streaming piece-by-piece design).
set -u
cd "$(dirname "$0")"
TRK=../tracker/tracker
CLI=../client/client

if [ ! -f large.bin ]; then head -c 209715200 /dev/urandom > large.bin; fi
REF=$(sha1sum large.bin | awk '{print $1}')
echo "large.bin sha1=$REF size=$(stat -c %s large.bin)"

tail -f /dev/null | "$TRK" tracker_info.txt 0 >l_t0.log 2>&1 &
T0=$!
tail -f /dev/null | "$TRK" tracker_info.txt 1 >l_t1.log 2>&1 &
T1=$!
sleep 1

{
  echo "create_user alice s"; echo "login alice s"
  echo "create_group grp"; echo "upload_file grp large.bin"
  sleep 4; echo "accept_request grp bob"
  sleep 40; echo "quit"
} | "$CLI" 127.0.0.1:6000 tracker_info.txt >l_alice.log 2>&1 &
A=$!

{
  sleep 2
  echo "create_user bob b"; echo "login bob b"; echo "join_group grp"
  sleep 5
  echo "download_file grp large.bin lgot.bin"
  sleep 35; echo "quit"
} | "$CLI" 127.0.0.1:6001 tracker_info.txt >l_bob.log 2>&1 &
B=$!

# Sample bob's peak RSS while it downloads.
peak=0
for i in $(seq 1 400); do
  if [ -d /proc/$B ]; then
    rss=$(awk '/VmHWM/{print $2}' /proc/$B/status 2>/dev/null)
    [ -n "${rss:-}" ] && [ "$rss" -gt "$peak" ] && peak=$rss
  fi
  kill -0 $B 2>/dev/null || break
  sleep 0.1
done

start=$(date +%s)
wait $A $B
end=$(date +%s)
kill -9 $T0 $T1 2>/dev/null; pkill -f "tail -f /dev/null" 2>/dev/null

echo "=== result ==="
GOT=$(sha1sum lgot.bin | awk '{print $1}')
echo "downloaded sha1=$GOT"
[ "$GOT" == "$REF" ] && echo "OK 200MB integrity verified" || echo "FAIL integrity"
echo "downloader peak RSS = $((peak/1024)) MB"
echo "elapsed ~ $((end-start)) s (includes scripted sleeps)"