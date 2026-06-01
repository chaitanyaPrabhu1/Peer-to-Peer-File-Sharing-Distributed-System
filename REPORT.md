# Technical Report — Peer-to-Peer Distributed File Sharing System

AOS Assignment 3, Monsoon 2025 — IIIT Hyderabad

This report explains the implementation approach, the design of the
synchronisation and piece-selection algorithms, the rationale behind the
protocol, and the engineering challenges encountered along the way.

---

## 1. Implementation approach

The system is decomposed into a small set of focused modules with a shared core
(`common/`) reused by both binaries:

* **`common/sha1`** — a streaming SHA-1 implementation (FIPS 180-1). Streaming
  is essential: a 1 GiB file is hashed 512 KiB at a time and never resident in
  memory. Verified against the standard test vectors (`abc`, the empty string,
  the "quick brown fox", and 10⁶ × `a`).
* **`common/net`** — blocking sockets with a uniform length-prefixed framing
  layer (`send_msg`/`recv_msg`) over `send_all`/`recv_all` loops that absorb
  partial transfers. `SIGPIPE` is ignored so a write to a vanished peer returns
  an error rather than killing the process.
* **Tracker** — a single mutex-guarded in-memory state plus a synchronisation
  subsystem. Connections are classified on their first frame (`HELLO` ⇒ peer
  tracker, otherwise a client) and handled on dedicated threads.
* **Client** — four cooperating parts: a resilient tracker channel
  (`TrackerConn`), a seeder (`PeerServer`), a parallel download engine
  (`DownloadManager`), and a command loop (`client.cpp`). A thread-safe
  `Registry` of `SharedFile`s is the single source of truth for what the client
  can serve.

A guiding principle throughout was **bounded resource usage**: pieces are read
and written with `pread`/`pwrite` at explicit offsets, so no operation needs
the whole file in memory and the same code path handles a 100 byte file and a
1 GB file. Measured peak RSS for a 200 MB download was ~7 MB.

---

## 2. Synchronisation algorithm

The two trackers must converge on identical state and tolerate each other being
offline, with automatic recovery on reconnect. The design is an
**idempotent, op-based replication** scheme.

**Links.** Each tracker keeps one *outbound* link (a connector thread that
dials the peer and retries forever) and accepts one *inbound* link. Its own
client-driven mutations flow out on the outbound link; mutations it receives
arrive on the inbound link. Two unidirectional streams together give
bidirectional replication.

**Live ops.** Every state-changing client request, while holding the state
mutex, also appends a compact mutation op (e.g. `ADD_MEMBER g u`,
`NEW_FILE g f size hash n h0 h1…`, `SEED_ADD g f u`) to an outbound queue. The
connector drains the queue and ships each op as a frame. Crucially, applying a
mutation and enqueueing its op happen **under the same lock**, so the two can
never be reordered relative to a snapshot.

**Snapshots and recovery.** When the outbound link is (re)established the
connector takes a full snapshot of the current state — serialised as the same
op vocabulary — and clears the pending queue, atomically under the state lock.
It sends `SNAP_BEGIN`, the snapshot ops, `SNAP_END`, then resumes live
streaming. A tracker that was down (or just started) is thus brought fully up
to date the moment its peer reconnects; nothing needs to be persisted.

**Why idempotent union semantics.** Every op is designed so that applying it
twice, or applying a snapshot that overlaps queued live ops, is harmless:
`users[uid]=pw`, `members.insert(u)`, `seeders.insert(u)`, "create group only
if absent", etc. This removes the need for sequence numbers, acknowledgements,
or exactly-once delivery — the hard parts of replication — while still
guaranteeing convergence. The snapshot/queue-clear races are eliminated by the
single-lock invariant above: any mutation is either captured in the snapshot or
present in the post-snapshot live stream, never both-lost and never
double-counted in a way that changes the result.

**No echo loops.** Ops received on the inbound link are applied but **not**
re-enqueued, so a mutation crosses the wire exactly once and cannot ping-pong.

**Disconnect handling.** A dropped client connection marks that user offline and
emits `OFFLINE`, so peers vanish from discovery on both trackers. If a whole
tracker dies, its peer keeps the last-known online set; a downloader that picks
a now-dead peer simply fails to connect and the download engine routes around
it — stale entries are self-correcting in practice.

---

## 3. Piece-selection strategy

Downloads use **rarest-first** selection with parallel multi-peer fetching:

1. **Discovery.** Ask the tracker for metadata + the set of online seeders.
   Open up to `CONN_PER_PEER` (4) TCP connections spread across all seeders
   (capped at 16 total), and fetch each peer's `BITFIELD`. Multiple
   connections per peer extract parallelism even when there is a single seeder.
2. **Rarity ordering.** Count, for each piece, how many connected peers
   advertise it, and sort piece indices by ascending count. Rare pieces are
   fetched first so they propagate into the swarm early and no single peer
   becomes a late bottleneck.
3. **Work assignment.** Each connection is a worker. A worker locks the shared
   state, scans the rarest-first order for the first *pending* piece that *its*
   peer has, marks it in-flight, and releases the lock before doing network
   I/O. This naturally load-balances: faster peers/connections claim more
   pieces, and two peers never redundantly download the same piece.
4. **Verify-on-arrival.** Each received piece is checked against its expected
   SHA-1 *before* being written. A piece that is corrupt, wrong-length, or comes
   back as an error is returned to the pending pool and retried — typically from
   a different worker/peer. A per-piece attempt cap and an overall stall timeout
   prevent infinite retry if a piece becomes permanently unavailable.
5. **Completion.** Pieces are `pwrite`-n at their final offset, so the file is
   assembled in place. When all pieces are present the whole-file SHA-1 is
   recomputed by streaming the file back from disk, and the client confirms
   itself as a full seeder.

Because the in-progress file is registered with a growing bitfield, the
downloader **seeds the pieces it already has**, so a third client can pull from
both the original seeder and an in-progress downloader — true swarm behaviour.
(Diagnostics under `P2P_DEBUG=1` print the per-peer piece breakdown; a test
showed a download split 4/6 + 2/6 across two seeders.)

---

## 4. Protocol design rationale

A single **length-prefixed framing** (`[4-byte length][payload]`) underlies all
three channels. The motivations:

* **Partial-transfer safety for free.** TCP is a byte stream; the framing layer
  turns it into discrete messages, and the `*_all` loops handle short
  reads/writes, so no higher-level code worries about fragmentation.
* **Uniform binary + text.** The same frame carries a human-readable command
  *or* a raw 512 KiB piece. Piece transfer uses a two-frame reply (`OK <len>`
  then the raw bytes), which cleanly separates status from payload and avoids
  any in-band escaping of binary data.
* **Extensibility.** Text, token-based messages with an `OK`/`ERR <reason>`
  convention are trivial to extend and to debug with a packet trace, while a
  4-byte length cap (`MAX_MSG`) guards against a corrupt/hostile length prefix.
* **Idempotent sync vocabulary.** Reusing the same op grammar for both live
  updates and snapshots meant one apply-path (`apply_sync_op`) serves recovery
  and steady-state replication alike.

Connection classification by first frame (`HELLO`) lets a single listening port
serve both clients and the peer tracker without extra configuration.

---

## 5. Concurrency model

* **Tracker:** one thread per connection (client or sync) plus connector
  threads; a single coarse mutex protects all shared state. With the assignment
  scale (≤ 3 clients, 2 trackers) a single lock is simpler and demonstrably
  correct, and critical sections never span blocking network I/O (state is
  copied out, then sent unlocked).
* **Client:** the command loop, the peer-server accept loop + one thread per
  inbound peer connection, and the download workers all run concurrently. Shared
  structures (`Registry`, `SharedFile::have`, `DCtx`) are mutex-protected;
  `pread`/`pwrite` are inherently thread-safe (they carry their own offset), so
  many workers read/write one file descriptor without races.
* **RAII everywhere:** `shared_ptr<SharedFile>` keeps a file alive while any
  peer thread is still reading it, and the descriptor is closed in the
  destructor — eliminating use-after-close races and descriptor leaks.

---

## 6. Challenges and solutions

* **Snapshot/live-stream consistency.** The subtle bug-prone part of
  replication is the boundary between the initial snapshot and the live op
  stream. Solved by making "mutate + enqueue" and "snapshot + clear-queue"
  atomic under one mutex, plus idempotent ops, so overlap is provably harmless.
* **Avoiding echo storms.** Early designs risked ops bouncing between trackers;
  resolved by the rule that received ops are applied but never re-forwarded.
* **Bounded memory for huge files.** Holding pieces or whole files in memory
  would blow up at 1 GB. Solved with offset-based `pread`/`pwrite` and streaming
  hashes; confirmed with a 200 MB run at ~7 MB RSS.
* **Parallelism from a single seeder.** One TCP stream serialises pieces;
  opening several connections per peer recovers parallelism and keeps the link
  saturated.
* **Tracker fail-over without losing the session.** Made the tracker channel
  lazily reconnect on the next request, replay `LOGIN`, and re-announce shared
  files, so a tracker crash is invisible to the user. Verified by killing a
  tracker mid-session and observing the download complete from the survivor.
* **Robustness against bad/slow peers.** Corrupt pieces are caught by per-piece
  SHA-1 and re-fetched; dead connections drop their worker and other workers
  cover the remaining pieces; a stall timeout and per-piece attempt cap
  guarantee termination instead of a hang.
* **Clean teardown.** Ignoring `SIGPIPE`, closing every descriptor (RAII), and a
  cooperative `running` flag give a leak-free shutdown — confirmed clean under
  AddressSanitizer/UBSan/LeakSanitizer for both binaries.

---

## 7. References

* FIPS PUB 180-1, *Secure Hash Standard* — SHA-1 algorithm and test vectors.
* W. R. Stevens, *UNIX Network Programming, Vol. 1* — TCP sockets, partial
  read/write handling.
* B. Cohen, *Incentives Build Robustness in BitTorrent* (2003) — piece-based
  transfer, rarest-first selection, and swarm seeding (conceptual inspiration).
* Linux man pages: `pread(2)`, `pwrite(2)`, `send(2)`, `recv(2)`,
  `getaddrinfo(3)`, `pthread(7)`.

All code (SHA-1, networking, protocol, synchronisation, download engine) was
written from scratch for this assignment; no external file-sharing, torrent,
hashing, or database libraries were used.
