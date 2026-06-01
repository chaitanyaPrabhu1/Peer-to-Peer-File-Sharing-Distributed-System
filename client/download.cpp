#include "download.h"
#include "net.h"
#include "util.h"
#include "sha1.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <mutex>
#include <thread>
#include <unistd.h>

namespace {

constexpr int CONN_PER_PEER  = 4;    // parallel streams per seeder
constexpr int MAX_WORKERS    = 16;   // overall cap on download threads
constexpr int MAX_ATTEMPTS   = 8;    // per-piece retries before giving up
constexpr auto STALL_TIMEOUT = std::chrono::seconds(30);

// A discovered seeder and the pieces it advertised.
struct Peer {
    std::string uid, ip;
    int port = 0;
    std::vector<char> avail;   // avail[i] != 0 => peer claims to have piece i
};

// One outcome of trying to fetch a piece from a peer.
enum class Fetch { OK, CORRUPT, PEER_ERR, CONN_DEAD };

// Shared, mutex-protected progress for all workers on one download.
struct DCtx {
    std::vector<char> state;          // 0 pending, 1 inflight, 2 done
    std::vector<int>  attempts;
    std::vector<int>  order;          // piece indices, rarest first
    std::vector<int>  got_from;       // pieces fetched from each peer (diag)
    int remaining = 0;
    std::atomic<bool> abort{false};
    std::mutex m;
    std::chrono::steady_clock::time_point last_progress;
};

// Fetch one piece over an already-open peer connection and verify its hash.
Fetch fetch_piece(int fd, const std::string &group, const std::string &fname,
                  int idx, uint64_t expect_len, const std::string &expect_hash,
                  std::string &out) {
    std::string req = "GET " + group + " " + fname + " " + std::to_string(idx);
    if (!net::send_msg(fd, req)) return Fetch::CONN_DEAD;

    std::string hdr;
    if (!net::recv_msg(fd, hdr)) return Fetch::CONN_DEAD;
    std::vector<std::string> ht = util::split_ws(hdr);
    if (ht.empty() || ht[0] != "OK") return Fetch::PEER_ERR;

    std::string data;
    if (!net::recv_msg(fd, data)) return Fetch::CONN_DEAD;
    if (data.size() != expect_len) return Fetch::CORRUPT;
    if (SHA1::hash(data.data(), data.size()) != expect_hash) return Fetch::CORRUPT;

    out = std::move(data);
    return Fetch::OK;
}

// Verify the assembled file's whole-file SHA-1 by streaming it back from disk
// (never loads more than one piece into memory -> safe for 1 GiB files).
bool verify_whole_file(int fd, uint64_t size, const std::string &expect) {
    SHA1 ctx;
    std::string buf;
    buf.resize(proto::PIECE_SIZE);
    uint64_t off = 0;
    while (off < size) {
        uint64_t want = std::min<uint64_t>(proto::PIECE_SIZE, size - off);
        ssize_t n = ::pread(fd, &buf[0], want, (off_t)off);
        if (n != (ssize_t)want) return false;
        ctx.update(buf.data(), want);
        off += want;
    }
    return ctx.final() == expect;
}

} // namespace

bool DownloadManager::start(const std::string &group, const std::string &fname,
                            const std::string &dest, std::string &err) {
    std::string resp = tc_->request("GET_META " + group + " " + fname);
    if (resp.compare(0, 2, "OK") != 0) {
        err = resp.size() > 4 ? resp.substr(4) : resp;
        return false;
    }

    // Parse: line0 = "OK size filehash npieces", then npieces hash lines,
    // then "PEERS count", then the peer lines.
    std::vector<std::string> lines = util::split(resp, '\n');
    std::vector<std::string> head = util::split_ws(lines[0]);
    if (head.size() < 4) { err = "malformed metadata"; return false; }

    uint64_t size = 0;
    long long npieces = 0;
    util::parse_u64(head[1], size);
    std::string filehash = head[2];
    util::parse_int(head[3], npieces);

    auto sf = std::make_shared<SharedFile>();
    sf->group = group;
    sf->filename = fname;
    sf->local_path = dest;
    sf->size = size;
    sf->npieces = (int)npieces;
    sf->filehash = filehash;
    sf->have.assign(npieces, 0);
    for (int i = 0; i < npieces; ++i) {
        if (1 + i >= (int)lines.size()) { err = "missing piece hashes"; return false; }
        sf->piece_hashes.push_back(util::trim(lines[1 + i]));
    }

    // The remainder (PEERS ...) is handed to run() verbatim.
    std::string peers_blob;
    for (size_t i = 1 + (size_t)npieces; i < lines.size(); ++i) {
        peers_blob += lines[i];
        peers_blob += '\n';
    }

    // Pre-allocate the destination file up front so pieces can be written at
    // their final offsets in any order.
    int fd = ::open(dest.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { err = "cannot create destination file"; return false; }
    if (size > 0 && ::ftruncate(fd, (off_t)size) != 0) {
        ::close(fd);
        err = "cannot allocate destination file";
        return false;
    }
    sf->fd = fd;

    reg_->add(sf);
    tc_->request("ADD_SEEDER " + group + " " + fname); // announce (best effort)

    auto dl = dls_->create(group, fname, dest, (int)npieces);

    std::thread(&DownloadManager::run, this, sf, dl, peers_blob).detach();
    return true;
}

void DownloadManager::run(std::shared_ptr<SharedFile> sf,
                          std::shared_ptr<Download> dl,
                          std::string peers_blob) {
    const std::string &group = sf->group;
    const std::string &fname = sf->filename;
    const int npieces = sf->npieces;

    auto fail = [&](const std::string &why) {
        dl->error = why;
        dl->status = 2;
        reg_->remove(group, fname);                 // stop seeding a failed file
        tc_->request("STOP_SHARE " + group + " " + fname);
        std::fprintf(stdout, "[!] download of %s failed: %s\n",
                     fname.c_str(), why.c_str());
        std::fflush(stdout);
    };

    // Empty file: nothing to fetch, just verify the (empty) hash.
    if (npieces == 0) {
        if (verify_whole_file(sf->fd, 0, sf->filehash)) {
            dl->completed = 0;
            dl->status = 1;
            std::fprintf(stdout, "[C] [%s] %s\n", group.c_str(), fname.c_str());
            std::fflush(stdout);
        } else {
            fail("empty-file hash mismatch");
        }
        return;
    }

    // ---- discover peers and open worker connections -----------------------
    std::vector<Peer> peers;
    std::vector<std::pair<int, int>> conns;   // (peer index, socket fd)

    for (auto &pl : util::split(peers_blob, '\n')) {
        std::vector<std::string> pt = util::split_ws(pl);
        if (pt.empty() || pt[0] == "PEERS") continue;
        if (pt.size() < 3) continue;
        long long port = 0;
        util::parse_int(pt[2], port);

        int fd = net::connect_to(pt[1], (int)port);
        if (fd < 0) continue;

        // Fetch this peer's bitfield over the connection we just opened.
        std::string r;
        if (!net::send_msg(fd, "BITFIELD " + group + " " + fname) ||
            !net::recv_msg(fd, r)) { ::close(fd); continue; }
        std::vector<std::string> rt = util::split_ws(r);
        if (rt.size() < 3 || rt[0] != "OK") { ::close(fd); continue; }

        Peer pe;
        pe.uid = pt[0]; pe.ip = pt[1]; pe.port = (int)port;
        pe.avail.assign(npieces, 0);
        const std::string &bits = rt[2];
        for (int i = 0; i < npieces && i < (int)bits.size(); ++i)
            pe.avail[i] = (bits[i] == '1');
        peers.push_back(std::move(pe));
        int pidx = (int)peers.size() - 1;

        conns.push_back({pidx, fd});            // reuse this socket as a worker
        for (int k = 1; k < CONN_PER_PEER && (int)conns.size() < MAX_WORKERS; ++k) {
            int fd2 = net::connect_to(pt[1], (int)port);
            if (fd2 >= 0) conns.push_back({pidx, fd2});
        }
        if ((int)conns.size() >= MAX_WORKERS) break;
    }

    if (conns.empty()) {
        for (auto &c : conns) ::close(c.second);
        fail("no seeders available");
        return;
    }

    // ---- build rarest-first order -----------------------------------------
    std::vector<int> count(npieces, 0);
    for (auto &p : peers)
        for (int i = 0; i < npieces; ++i) count[i] += p.avail[i] ? 1 : 0;

    DCtx ctx;
    ctx.state.assign(npieces, 0);
    ctx.attempts.assign(npieces, 0);
    ctx.remaining = npieces;
    ctx.got_from.assign(peers.size(), 0);
    ctx.order.resize(npieces);
    for (int i = 0; i < npieces; ++i) ctx.order[i] = i;
    std::stable_sort(ctx.order.begin(), ctx.order.end(),
                     [&](int a, int b) { return count[a] < count[b]; });
    ctx.last_progress = std::chrono::steady_clock::now();

    // ---- worker threads ----------------------------------------------------
    auto worker = [&](int conn_fd, int peer_index) {
        const Peer &peer = peers[peer_index];
        while (!ctx.abort.load()) {
            int chosen = -1;
            {
                std::lock_guard<std::mutex> lk(ctx.m);
                if (ctx.remaining == 0) break;
                for (int idx : ctx.order) {
                    if (ctx.state[idx] == 0 && peer.avail[idx]) {
                        chosen = idx;
                        ctx.state[idx] = 1;          // mark inflight
                        break;
                    }
                }
                if (chosen == -1) {
                    // Nothing this peer can serve right now. Bail out if the
                    // whole download has stalled (a needed piece is lost).
                    auto idle = std::chrono::steady_clock::now() - ctx.last_progress;
                    if (idle > STALL_TIMEOUT) {
                        ctx.abort = true;
                        break;
                    }
                }
            }
            if (chosen == -1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }

            std::string data;
            Fetch res = fetch_piece(conn_fd, group, fname, chosen,
                                    sf->piece_len(chosen),
                                    sf->piece_hashes[chosen], data);
            if (res == Fetch::OK) {
                ::pwrite(sf->fd, data.data(), data.size(),
                         (off_t)sf->piece_off(chosen));
                sf->mark_have(chosen);
                std::lock_guard<std::mutex> lk(ctx.m);
                if (ctx.state[chosen] != 2) {
                    ctx.state[chosen] = 2;
                    ctx.remaining--;
                    ctx.got_from[peer_index]++;
                    dl->completed++;
                    ctx.last_progress = std::chrono::steady_clock::now();
                }
            } else {
                std::lock_guard<std::mutex> lk(ctx.m);
                ctx.state[chosen] = 0;               // back to the pool
                if (++ctx.attempts[chosen] > MAX_ATTEMPTS) ctx.abort = true;
                if (res == Fetch::CONN_DEAD) break;  // this connection is gone
            }
        }
        ::close(conn_fd);
    };

    std::vector<std::thread> threads;
    for (auto &c : conns) threads.emplace_back(worker, c.second, c.first);
    for (auto &t : threads) t.join();

    // ---- finalise ----------------------------------------------------------
    if (ctx.remaining != 0) {
        fail(ctx.abort ? "could not obtain all pieces (peers unavailable)"
                       : "incomplete download");
        return;
    }
    if (!verify_whole_file(sf->fd, sf->size, sf->filehash)) {
        fail("whole-file hash mismatch");
        return;
    }

    dl->status = 1;
    // Confirm we are a (now complete) seeder and report completion.
    tc_->request("ADD_SEEDER " + group + " " + fname);
    std::fprintf(stdout, "[C] [%s] %s\n", group.c_str(), fname.c_str());
    std::fflush(stdout);

    // Optional diagnostics: how many pieces came from each peer.
    if (std::getenv("P2P_DEBUG")) {
        std::lock_guard<std::mutex> lk(ctx.m);
        for (size_t i = 0; i < peers.size(); ++i)
            std::fprintf(stderr, "[dbg] %s: %d/%d pieces from peer %s (%s:%d)\n",
                         fname.c_str(), ctx.got_from[i], npieces,
                         peers[i].uid.c_str(), peers[i].ip.c_str(), peers[i].port);
    }
}
