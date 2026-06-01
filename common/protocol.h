#ifndef P2P_PROTOCOL_H
#define P2P_PROTOCOL_H

#include <cstdint>

// ===========================================================================
//  Shared protocol constants and a description of every wire format used.
//
//  Every control message is one length-prefixed frame (see net.h). Frames are
//  human-readable, space-separated text unless noted as "raw bytes".
// ===========================================================================

namespace proto {

// Each file is cut into fixed-size pieces; the final piece may be shorter.
constexpr uint64_t PIECE_SIZE = 512u * 1024u; // 512 KiB

// ----- Tracker <-> Client -------------------------------------------------
// The client sends one request frame; the tracker replies with one response
// frame. A response always begins with "OK" or "ERR". For list-style replies
// the payload carries additional '\n'-separated lines after the status line.
// Apart from CREATE_USER/LOGIN, every command operates on the connection's
// authenticated session UID, so the UID is NOT repeated in the request.
//
// Requests (verb + args):
//   CREATE_USER  <uid> <password>
//   LOGIN        <uid> <password> <peer_port>
//   LOGOUT
//   CREATE_GROUP <gid>
//   JOIN_GROUP   <gid>
//   LEAVE_GROUP  <gid>
//   LIST_GROUPS
//   LIST_REQ     <gid>
//   ACCEPT_REQ   <gid> <target_uid>
//   UPLOAD       <gid> <filename> <size> <filehash> <npieces> <h0> <h1> ...
//   LIST_FILES   <gid>
//   GET_META     <gid> <filename>
//   ADD_SEEDER   <gid> <filename>
//   STOP_SHARE   <gid> <filename>
//
// GET_META success payload layout:
//   OK <size> <filehash> <npieces>
//   <h0>
//   <h1>
//   ...
//   PEERS <count>
//   <uid> <ip> <port>
//   ...

// ----- Tracker <-> Tracker (synchronisation) ------------------------------
// On connecting, a tracker sends "HELLO <tracker_no>", then a full state
// snapshot bracketed by SNAP_BEGIN / SNAP_END frames, then a live stream of
// the idempotent mutation ops below (one op per frame):
//   ADD_USER   <uid> <password>
//   ONLINE     <uid> <ip> <port>
//   OFFLINE    <uid>
//   NEW_GROUP  <gid> <owner>
//   JOIN_REQ   <gid> <uid>
//   ADD_MEMBER <gid> <uid>
//   DEL_MEMBER <gid> <uid>
//   NEW_FILE   <gid> <filename> <size> <filehash> <npieces> <h0> <h1> ...
//   SEED_ADD   <gid> <filename> <uid>
//   SEED_DEL   <gid> <filename> <uid>
// All ops are applied with union/idempotent semantics so snapshots and live
// ops can overlap harmlessly and trackers converge.

// ----- Client <-> Client (piece transfer) ---------------------------------
// Requests:
//   BITFIELD <gid> <filename>
//       reply frame: "OK <npieces> <bitstring>"  (bitstring is '0'/'1' chars)
//                or  "ERR <reason>"
//   GET <gid> <filename> <piece_index>
//       reply frame 1: "OK <length>" or "ERR <reason>"
//       reply frame 2 (only on OK): <length> raw piece bytes

} // namespace proto

#endif // P2P_PROTOCOL_H
