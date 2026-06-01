#!/bin/bash
# Multi-peer test: alice uploads big.bin; carol downloads it fully (becoming a
# second seeder); bob then downloads with TWO seeders available, so pieces are
# pulled from both peers in parallel. P2P_DEBUG shows the per-peer breakdown.
set -u
cd "$(dirname "$0")"
TRK=../tracker/tracker
CLI=../client/client
export P2P_DEBUG=1
rm -f m_*.log mcarol_big.bin mbob_big.bin

tail -f /dev/null | "$TRK" tracker_info.txt 0 >m_t0.log 2>&1 &
T0=$!
tail -f /dev/null | "$TRK" tracker_info.txt 1 >m_t1.log 2>&1 &
T1=$!
sleep 1

# alice (tracker 0): owner + original seeder
{
  echo "create_user alice s"; echo "login alice s"
  echo "create_group grp"; echo "upload_file grp big.bin"
  sleep 4
  echo "accept_request grp carol"
  echo "accept_request grp bob"
  sleep 22
  echo "quit"
} | "$CLI" 127.0.0.1:6000 tracker_info.txt >m_alice.log 2>&1 &
A=$!

# carol (tracker 1): joins, downloads fully -> becomes 2nd seeder
{
  sleep 2
  echo "create_user carol c"; echo "login carol c"
  echo "join_group grp"
  sleep 5
  echo "download_file grp big.bin mcarol_big.bin"
  sleep 18      # finish, then keep seeding
  echo "quit"
} | "$CLI" 127.0.0.1:6001 tracker_info.txt >m_carol.log 2>&1 &
C=$!

# bob (tracker 0): joins, downloads with TWO seeders available
{
  sleep 2
  echo "create_user bob b"; echo "login bob b"
  echo "join_group grp"
  sleep 12     # wait until carol is also a seeder
  echo "list_files grp"
  echo "download_file grp big.bin mbob_big.bin"
  sleep 8
  echo "quit"
} | "$CLI" 127.0.0.1:6000 tracker_info.txt >m_bob.log 2>&1 &
B=$!

wait $A $C $B
kill $T0 $T1 2>/dev/null
pkill -f "tail -f /dev/null" 2>/dev/null
sleep 0.3

echo "=================== carol.log ==================="; cat m_carol.log
echo "=================== bob.log ==================="; cat m_bob.log
echo "=================== integrity ==================="
cmp -s big.bin mcarol_big.bin && echo "OK carol copy" || echo "FAIL carol"
cmp -s big.bin mbob_big.bin   && echo "OK bob copy"   || echo "FAIL bob"