# Peer-to-Peer Distributed File Sharing System

A BitTorrent-style group file-sharing system built from scratch in C++ using
only POSIX system calls. Files are split into pieces, distributed across
peers, and downloaded in parallel from multiple sources with end-to-end SHA-1
integrity verification. A pair of synchronised, fault-tolerant trackers
coordinate metadata and peer discovery; the system keeps working as long as at
least one tracker is alive.

> AOS Assignment 3 — Monsoon 2025, IIIT Hyderabad.

---

## 1. Directory layout

```
.
├── common/        Shared modules (compiled into both binaries)
│   ├── sha1.{h,cpp}      Streaming SHA-1 (FIPS 180-1), our own implementation
│   ├── net.{h,cpp}       Blocking sockets + length-prefixed message framing
│   ├── util.{h,cpp}      String / parsing helpers
│   └── protocol.h        Constants + full description of every wire format
├── tracker/
│   ├── tracker.cpp       Tracker server: state, client protocol, sync
│   └── Makefile
├── client/
│   ├── client.cpp        Main, command loop, upload
│   ├── tracker_conn.{h,cpp}  Resilient tracker channel with fail-over
│   ├── peer_server.{h,cpp}   Seeder: serves pieces & bitfields to peers
│   ├── download.{h,cpp}      Multi-peer parallel download engine
│   ├── client_state.h        Shared-file registry, downloads, piece math
│   └── Makefile
├── test/          Self-contained end-to-end test scripts (see §9)
├── Makefile       Convenience: builds tracker and client
└── README.md
```

The `common/` modules are compiled directly into each binary, so `tracker/`
(with `common/`) and `client/` (with `common/`) are each self-contained.

---

## 2. Build

Requires a C++17 compiler (g++/clang++) and a POSIX system (Linux). No external
libraries.

```bash
make                 # builds tracker/tracker and client/client
# or build individually:
make -C tracker
make -C client
make clean           # remove all binaries/objects
```

---

## 3. Run

### tracker_info.txt

A plain text file listing **both** trackers, one `IP PORT` per line. The line
index is the tracker number:

```
127.0.0.1 5000      # tracker 0
127.0.0.1 5001      # tracker 1
```

### Start the trackers (one terminal each)

```bash
./tracker/tracker tracker_info.txt 0      # this process is tracker 0
./tracker/tracker tracker_info.txt 1      # this process is tracker 1
```

Type `quit` in a tracker console to shut it down cleanly. The two trackers find
each other automatically and synchronise; either may be started first, stopped,
and restarted.

### Start clients (one terminal each)

```bash
./client/client 127.0.0.1:5000 tracker_info.txt   # prefers tracker 0
./client/client 127.0.0.1:5001 tracker_info.txt   # prefers tracker 1
```

The `IP:PORT` argument is the tracker the client contacts first; the full list
in `tracker_info.txt` is used for transparent fail-over if that tracker dies.
The client binds its own ephemeral peer-listening port automatically and
reports it to the tracker at login.

---

## 4. Commands

User / session / group:

| Command | Description |
|---|---|
| `create_user <user_id> <password>` | Register a new account |
| `login <user_id> <password>` | Authenticate and start a session |
| `logout` | End session and stop sharing |
| `create_group <group_id>` | Create a group (you become owner) |
| `join_group <group_id>` | Request to join a group |
| `leave_group <group_id>` | Leave a group (ownership transfers if owner) |
| `list_groups` | List all groups |
| `list_requests <group_id>` | List pending join requests (owner only) |
| `accept_request <group_id> <user_id>` | Accept a join request (owner only) |

Files:

| Command | Description |
|---|---|
| `upload_file <group_id> <file_path>` | Share a file with a group |
| `list_files <group_id>` | List files shared in a group |
| `download_file <group_id> <file_name> <dest_path>` | Download from peers |
| `show_downloads` | Show download progress |
| `stop_share <group_id> <file_name>` | Stop sharing a file |
| `help`, `quit` | Help / exit (`quit` logs out first) |

`show_downloads` prints:

```
[C] [group_id] filename        completed download
[D] [group_id] filename  k/n (p%)   in progress
[F] [group_id] filename  (reason)   failed
```

---

## 5. Architectural overview

```
        sync (snapshot + live ops, bidirectional, auto-reconnect)
   Tracker 0  <───────────────────────────────────────────>  Tracker 1
      ▲                                                          ▲
      │ control (login, groups, metadata, peer discovery)        │
      │                                                          │
   Client A  ───────────── piece transfer (TCP) ───────────  Client B
   (seeder + downloader)                              (seeder + downloader)
```

* **Trackers** hold all metadata (users, groups, files, piece hashes, seeder
  lists, who is online and where). They never store file data. Two trackers
  replicate each other so the system tolerates the loss of one.
* **Clients** are simultaneously **seeders** (a `PeerServer` thread serves
  pieces of everything they share) and **downloaders** (the `DownloadManager`
  pulls pieces from many peers in parallel). A client even seeds the pieces of
  a file it is *still downloading*.
* **Three channels**, all over TCP with a common length-prefixed framing:
  tracker↔client (control), client↔client (piece transfer), tracker↔tracker
  (synchronisation).

Key algorithms (full detail in `REPORT.md`):

* **Piece management** — fixed 512 KiB pieces, last piece short; per-piece and
  whole-file SHA-1. Pieces are written at their final offset with `pwrite`, so
  memory use is bounded regardless of file size (verified: 200 MB downloads in
  ~7 MB RSS).
* **Multi-peer download** — open several connections across all seeders, fetch
  each peer's bitfield, hand out pieces **rarest-first**, verify each piece's
  SHA-1 on arrival, and re-queue corrupt/failed pieces to a different peer.
* **Tracker synchronisation** — on (re)connect a tracker pushes a full state
  snapshot, then streams idempotent live mutation ops. All ops use union
  semantics so snapshots and live ops overlap harmlessly and the trackers
  always converge; a tracker that was offline recovers automatically.
* **Fail-over** — if a client's tracker dies, the next request transparently
  reconnects to the other tracker, re-authenticates, and re-announces shared
  files. The user's session survives.

---

## 6. Data structures and rationale

**Tracker** (`tracker.cpp`)
* `map<uid,password>` users, `map<uid,{ip,port}>` online — O(log n) lookups,
  ordered iteration for deterministic snapshots.
* `map<gid, Group>` where `Group{owner, set<members>, set<pending>,
  map<filename, FileMeta>}`. Sets give idempotent insert/erase (matches the
  union-style sync ops) and dedupe membership/requests for free.
* `FileMeta{size, filehash, vector<piece_hashes>, set<seeders>}`.
* One `std::mutex` guards all state; an op queue (`deque<string>`) guarded by
  the *same* mutex makes "apply mutation" and "enqueue its sync op" atomic.

**Client**
* `SharedFile{fd, size, npieces, piece_hashes, vector<char> have, mutex}` — one
  structure serves both fully-uploaded files and in-progress downloads; the
  `have` bitfield grows as pieces arrive (enables partial seeding). `fd` is
  closed in the destructor (RAII), so a peer reading a piece can never have the
  descriptor pulled out from under it.
* `Registry` — thread-safe `map<(group,filename), shared_ptr<SharedFile>>`.
* `DCtx` (download) — per-piece state array (pending/inflight/done), per-piece
  retry counts, and a rarest-first ordering, all under one mutex.

---

## 7. Network protocol

All control messages are a single **length-prefixed frame**:

```
[ 4-byte big-endian length ][ payload bytes ]
```

This makes partial reads/writes invisible to callers (`recv_all`/`send_all`
loop until the whole frame is in/out) and lets a raw binary 512 KiB piece be
shipped as a payload without escaping. Frames are human-readable text except
for raw piece bytes. Responses start with `OK` or `ERR <reason>`.

The complete grammar for all three channels (tracker↔client, tracker↔tracker
sync ops, client↔client `BITFIELD`/`GET`) is documented in
[`common/protocol.h`](common/protocol.h).

---

## 8. Assumptions

* User IDs, passwords, group IDs and file names contain no whitespace (they are
  space-separated tokens in the text protocol).
* Passwords are stored and compared in plaintext (no crypto requirement in the
  spec). Treat this as a teaching system, not a secure one.
* All hosts are reachable on the IPs the tracker observes at connect time
  (works for `localhost` and normal LANs; not designed for NAT traversal).
* Shared-file state lives in client memory: a client must stay running to seed,
  and re-uploads after a restart. Tracker state is in memory and is recovered
  on the peer tracker by synchronisation, not persisted to disk.
* IPv4.

---

## 9. Testing

The `test/` directory contains scripted, self-verifying scenarios. Each starts
its own trackers and clients and checks downloaded files with `cmp`/`sha1sum`.

```bash
cd test
./run_test.sh        # cross-tracker sync; multi-piece + tiny + empty files
./run_failover.sh    # kill a tracker mid-session; client fails over & finishes
./run_multipeer.sh   # 3 clients; download served from 2 seeders in parallel
./run_large.sh       # 200 MB transfer; verifies integrity + low peak RSS
./run_asan.sh        # full scenario under AddressSanitizer/UBSan/LeakSanitizer
./run_asan_tracker.sh# tracker leak-check with a clean shutdown
```

Validated behaviours: file sizes from 0 bytes to 200 MB; text/binary content;
concurrent multi-file downloads; downloads served from multiple peers; tracker
fail-over; graceful peer/tracker disconnects; and **no leaks or memory errors**
under ASan/UBSan/LeakSanitizer for both binaries.

To build sanitizer binaries manually:

```bash
g++ -std=c++17 -g -fsanitize=address,undefined -pthread -Icommon \
    client/*.cpp common/*.cpp -o client_asan
```

---

## 10. Implemented features & limitations

**Implemented**: all required commands; two-tracker synchronisation with
snapshot + live-op recovery; transparent client fail-over with session
restoration; multi-peer parallel downloads with rarest-first selection;
partial (in-progress) seeding; per-piece and whole-file SHA-1 with automatic
re-fetch of corrupt pieces; concurrent multi-file downloads; bounded memory for
files up to 1 GB; graceful handling of peer/tracker disconnects; clean,
leak-free shutdown.

**Limitations**: tracker state is not persisted to disk (recovered via the peer
tracker, not across a full two-tracker shutdown); whitespace is not allowed in
identifiers/filenames; plaintext passwords; IPv4 only; the stall timeout aborts
a download if a uniquely-held piece becomes permanently unavailable.

---

## 11. License

Released under the [MIT License](LICENSE).
