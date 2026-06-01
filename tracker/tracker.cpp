// ===========================================================================
//  tracker.cpp  --  Peer-to-Peer Distributed File Sharing System (AOS A3)
//
//  A tracker server maintains synchronised metadata about users, groups and
//  shared files, and answers peer-discovery queries. Exactly two trackers run
//  together; each pushes its own client-driven mutations to the other and
//  applies what it receives, so the pair converges and the system survives the
//  loss of either tracker.
//
//  Usage:  ./tracker tracker_info.txt <tracker_no>
//          (tracker_no is the 0-based line in tracker_info.txt for *this*
//           tracker; the file lists "ip port" per tracker, one per line.)
//
//  Console:  type "quit" to shut the tracker down cleanly.
// ===========================================================================

#include "net.h"
#include "util.h"
#include "protocol.h"

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <sys/socket.h>
#include <vector>

// ---------------------------------------------------------------------------
//  Tracker state
// ---------------------------------------------------------------------------
namespace {

struct PeerAddr {
    std::string ip;
    int port = 0;
};

struct FileMeta {
    std::string filename;
    uint64_t size = 0;
    std::string filehash;            // SHA1 of the whole file
    int npieces = 0;
    std::vector<std::string> piece_hashes;
    std::set<std::string> seeders;   // uids that hold/share this file
};

struct Group {
    std::string owner;
    std::set<std::string> members;            // includes the owner
    std::set<std::string> pending;            // outstanding join requests
    std::map<std::string, FileMeta> files;    // filename -> metadata
};

struct State {
    std::map<std::string, std::string> users;   // uid -> password
    std::map<std::string, PeerAddr>    online;  // uid -> address (logged in)
    std::map<std::string, Group>       groups;  // gid -> group
    std::mutex mtx;                             // guards everything above
};

State g_state;

// Outgoing synchronisation queue (guarded by g_state.mtx so that "apply a
// mutation" and "enqueue its op" are atomic with respect to snapshotting).
std::deque<std::string> g_pending;
std::condition_variable g_ops_cv;
constexpr size_t MAX_PENDING = 200000;

std::atomic<bool> g_running{true};
int g_listen_fd = -1;
int g_my_no = 0;

// Append a mutation op to the outbound sync stream. Caller MUST hold
// g_state.mtx. Bounded so a long-offline peer cannot exhaust memory; the
// snapshot sent on reconnect supersedes anything we drop here.
void record_op(const std::string &op) {
    g_pending.push_back(op);
    if (g_pending.size() > MAX_PENDING) g_pending.pop_front();
    g_ops_cv.notify_all();
}

// ---------------------------------------------------------------------------
//  Applying a synchronisation op received from the peer tracker. Every op is
//  idempotent / union-style so snapshots and live ops may overlap freely.
//  These never call record_op() -> no echo loop.
//  Caller MUST hold g_state.mtx.
// ---------------------------------------------------------------------------
void apply_sync_op(const std::vector<std::string> &t) {
    if (t.empty()) return;
    const std::string &op = t[0];

    if (op == "ADD_USER" && t.size() >= 3) {
        g_state.users[t[1]] = t[2];
    } else if (op == "ONLINE" && t.size() >= 4) {
        long long p = 0;
        util::parse_int(t[3], p);
        g_state.online[t[1]] = PeerAddr{t[2], (int)p};
    } else if (op == "OFFLINE" && t.size() >= 2) {
        g_state.online.erase(t[1]);
    } else if (op == "NEW_GROUP" && t.size() >= 3) {
        auto &g = g_state.groups[t[1]];
        if (g.owner.empty()) g.owner = t[2];
        g.members.insert(t[2]);
    } else if (op == "SET_OWNER" && t.size() >= 3) {
        auto &g = g_state.groups[t[1]];
        g.owner = t[2];
        g.members.insert(t[2]);
    } else if (op == "JOIN_REQ" && t.size() >= 3) {
        auto it = g_state.groups.find(t[1]);
        if (it != g_state.groups.end() && !it->second.members.count(t[2]))
            it->second.pending.insert(t[2]);
    } else if (op == "ADD_MEMBER" && t.size() >= 3) {
        auto &g = g_state.groups[t[1]];
        g.members.insert(t[2]);
        g.pending.erase(t[2]);
    } else if (op == "DEL_MEMBER" && t.size() >= 3) {
        auto it = g_state.groups.find(t[1]);
        if (it != g_state.groups.end()) it->second.members.erase(t[2]);
    } else if (op == "NEW_FILE" && t.size() >= 6) {
        long long npieces = 0;
        util::parse_int(t[5], npieces);
        auto &g = g_state.groups[t[1]];
        if (!g.files.count(t[2])) {
            FileMeta fm;
            fm.filename = t[2];
            util::parse_u64(t[3], fm.size);
            fm.filehash = t[4];
            fm.npieces = (int)npieces;
            for (int i = 0; i < npieces && (size_t)(6 + i) < t.size(); ++i)
                fm.piece_hashes.push_back(t[6 + i]);
            g.files[t[2]] = std::move(fm);
        }
    } else if (op == "SEED_ADD" && t.size() >= 4) {
        auto git = g_state.groups.find(t[1]);
        if (git != g_state.groups.end()) {
            auto fit = git->second.files.find(t[2]);
            if (fit != git->second.files.end()) fit->second.seeders.insert(t[3]);
        }
    } else if (op == "SEED_DEL" && t.size() >= 4) {
        auto git = g_state.groups.find(t[1]);
        if (git != g_state.groups.end()) {
            auto fit = git->second.files.find(t[2]);
            if (fit != git->second.files.end()) fit->second.seeders.erase(t[3]);
        }
    }
    // Unknown / malformed ops are ignored on purpose (forward compatibility).
}

// Build a full snapshot of the current state as a list of sync ops. Caller
// MUST hold g_state.mtx. ADD_USER/ONLINE are emitted before group data so the
// receiver always has the referenced users when it applies later ops.
std::vector<std::string> build_snapshot_ops() {
    std::vector<std::string> ops;
    for (auto &u : g_state.users)
        ops.push_back("ADD_USER " + u.first + " " + u.second);
    for (auto &o : g_state.online)
        ops.push_back("ONLINE " + o.first + " " + o.second.ip + " " +
                      std::to_string(o.second.port));
    for (auto &gp : g_state.groups) {
        const std::string &gid = gp.first;
        const Group &g = gp.second;
        ops.push_back("NEW_GROUP " + gid + " " + g.owner);
        ops.push_back("SET_OWNER " + gid + " " + g.owner);
        for (auto &m : g.members) ops.push_back("ADD_MEMBER " + gid + " " + m);
        for (auto &p : g.pending) ops.push_back("JOIN_REQ " + gid + " " + p);
        for (auto &fp : g.files) {
            const FileMeta &fm = fp.second;
            std::string line = "NEW_FILE " + gid + " " + fm.filename + " " +
                               std::to_string(fm.size) + " " + fm.filehash +
                               " " + std::to_string(fm.npieces);
            for (auto &h : fm.piece_hashes) line += " " + h;
            ops.push_back(line);
            for (auto &s : fm.seeders)
                ops.push_back("SEED_ADD " + gid + " " + fm.filename + " " + s);
        }
    }
    return ops;
}

// ---------------------------------------------------------------------------
//  Client request handling
// ---------------------------------------------------------------------------

// Per-connection session context.
struct Session {
    std::string uid;        // empty until logged in
    std::string peer_ip;    // address we saw the client connect from
};

std::string err(const std::string &msg) { return "ERR " + msg; }

// Returns true if `uid` is a member of `gid` (owner counts as member).
bool is_member(const Group &g, const std::string &uid) {
    return g.members.count(uid) > 0;
}

std::string handle_request(Session &sess, const std::string &line) {
    std::vector<std::string> t = util::split_ws(line);
    if (t.empty()) return err("empty request");
    const std::string &cmd = t[0];

    std::lock_guard<std::mutex> lk(g_state.mtx);

    // ---- account / session ------------------------------------------------
    if (cmd == "CREATE_USER") {
        if (t.size() != 3) return err("usage: create_user <uid> <password>");
        if (g_state.users.count(t[1])) return err("user already exists");
        g_state.users[t[1]] = t[2];
        record_op("ADD_USER " + t[1] + " " + t[2]);
        return "OK user created";
    }
    if (cmd == "LOGIN") {
        if (t.size() != 4) return err("usage: login <uid> <password> <peer_port>");
        auto it = g_state.users.find(t[1]);
        if (it == g_state.users.end() || it->second != t[2])
            return err("invalid credentials");
        long long port = 0;
        if (!util::parse_int(t[3], port) || port <= 0 || port > 65535)
            return err("bad peer port");
        g_state.online[t[1]] = PeerAddr{sess.peer_ip, (int)port};
        sess.uid = t[1];
        record_op("ONLINE " + t[1] + " " + sess.peer_ip + " " + std::to_string(port));
        return "OK login successful";
    }

    // Everything below requires an authenticated session.
    if (sess.uid.empty()) return err("not logged in");

    if (cmd == "LOGOUT") {
        g_state.online.erase(sess.uid);
        record_op("OFFLINE " + sess.uid);
        std::string who = sess.uid;
        sess.uid.clear();
        return "OK logged out " + who;
    }

    // ---- groups -----------------------------------------------------------
    if (cmd == "CREATE_GROUP") {
        if (t.size() != 2) return err("usage: create_group <gid>");
        if (g_state.groups.count(t[1])) return err("group already exists");
        Group g;
        g.owner = sess.uid;
        g.members.insert(sess.uid);
        g_state.groups[t[1]] = std::move(g);
        record_op("NEW_GROUP " + t[1] + " " + sess.uid);
        return "OK group created, you are the owner";
    }
    if (cmd == "JOIN_GROUP") {
        if (t.size() != 2) return err("usage: join_group <gid>");
        auto it = g_state.groups.find(t[1]);
        if (it == g_state.groups.end()) return err("no such group");
        if (it->second.members.count(sess.uid)) return err("already a member");
        if (it->second.pending.count(sess.uid)) return err("request already pending");
        it->second.pending.insert(sess.uid);
        record_op("JOIN_REQ " + t[1] + " " + sess.uid);
        return "OK join request sent to owner";
    }
    if (cmd == "LEAVE_GROUP") {
        if (t.size() != 2) return err("usage: leave_group <gid>");
        auto it = g_state.groups.find(t[1]);
        if (it == g_state.groups.end()) return err("no such group");
        Group &g = it->second;
        if (!g.members.count(sess.uid)) return err("you are not a member");
        g.members.erase(sess.uid);
        // Drop this user from every file's seeder set in the group.
        for (auto &fp : g.files) {
            if (fp.second.seeders.erase(sess.uid))
                record_op("SEED_DEL " + t[1] + " " + fp.first + " " + sess.uid);
        }
        record_op("DEL_MEMBER " + t[1] + " " + sess.uid);
        if (g.owner == sess.uid) {
            if (!g.members.empty()) {
                g.owner = *g.members.begin();   // transfer ownership
                record_op("SET_OWNER " + t[1] + " " + g.owner);
                return "OK left group; ownership transferred to " + g.owner;
            }
            // No members left: keep the (now empty) group and its files so a
            // late-joining tracker does not resurrect stale membership.
        }
        return "OK left group";
    }
    if (cmd == "LIST_GROUPS") {
        std::string out = "OK groups";
        if (g_state.groups.empty()) return "OK no groups exist";
        for (auto &gp : g_state.groups) {
            out += "\n" + gp.first + " owner=" + gp.second.owner +
                   " members=" + std::to_string(gp.second.members.size());
        }
        return out;
    }
    if (cmd == "LIST_REQ") {
        if (t.size() != 2) return err("usage: list_requests <gid>");
        auto it = g_state.groups.find(t[1]);
        if (it == g_state.groups.end()) return err("no such group");
        if (it->second.owner != sess.uid) return err("only the owner may view requests");
        if (it->second.pending.empty()) return "OK no pending requests";
        std::string out = "OK pending requests for " + t[1];
        for (auto &p : it->second.pending) out += "\n" + p;
        return out;
    }
    if (cmd == "ACCEPT_REQ") {
        if (t.size() != 3) return err("usage: accept_request <gid> <uid>");
        auto it = g_state.groups.find(t[1]);
        if (it == g_state.groups.end()) return err("no such group");
        if (it->second.owner != sess.uid) return err("only the owner may accept requests");
        if (!it->second.pending.count(t[2])) return err("no such pending request");
        it->second.pending.erase(t[2]);
        it->second.members.insert(t[2]);
        record_op("ADD_MEMBER " + t[1] + " " + t[2]);
        return "OK " + t[2] + " added to " + t[1];
    }

    // ---- files ------------------------------------------------------------
    if (cmd == "UPLOAD") {
        // UPLOAD <gid> <filename> <size> <filehash> <npieces> <h0> <h1> ...
        if (t.size() < 6) return err("malformed upload");
        auto it = g_state.groups.find(t[1]);
        if (it == g_state.groups.end()) return err("no such group");
        if (!is_member(it->second, sess.uid)) return err("not a member of this group");
        const std::string &fname = t[2];
        uint64_t size = 0;
        long long npieces = 0;
        if (!util::parse_u64(t[3], size) || !util::parse_int(t[5], npieces))
            return err("malformed upload");
        if ((size_t)t.size() < 6 + (size_t)npieces) return err("missing piece hashes");

        Group &g = it->second;
        auto fit = g.files.find(fname);
        if (fit != g.files.end()) {
            if (fit->second.filehash != t[4])
                return err("a different file with that name already exists in the group");
            // Same content: just register this user as an additional seeder.
            fit->second.seeders.insert(sess.uid);
            record_op("SEED_ADD " + t[1] + " " + fname + " " + sess.uid);
            return "OK already shared; you are now also a seeder";
        }
        FileMeta fm;
        fm.filename = fname;
        fm.size = size;
        fm.filehash = t[4];
        fm.npieces = (int)npieces;
        for (int i = 0; i < npieces; ++i) fm.piece_hashes.push_back(t[6 + i]);
        fm.seeders.insert(sess.uid);
        g.files[fname] = fm;

        std::string op = "NEW_FILE " + t[1] + " " + fname + " " +
                         std::to_string(size) + " " + t[4] + " " +
                         std::to_string(npieces);
        for (int i = 0; i < npieces; ++i) op += " " + fm.piece_hashes[i];
        record_op(op);
        record_op("SEED_ADD " + t[1] + " " + fname + " " + sess.uid);
        return "OK file shared with group";
    }
    if (cmd == "LIST_FILES") {
        if (t.size() != 2) return err("usage: list_files <gid>");
        auto it = g_state.groups.find(t[1]);
        if (it == g_state.groups.end()) return err("no such group");
        if (!is_member(it->second, sess.uid)) return err("not a member of this group");
        if (it->second.files.empty()) return "OK no files shared in this group";
        std::string out = "OK files in " + t[1];
        for (auto &fp : it->second.files) {
            int online_seeders = 0;
            for (auto &s : fp.second.seeders)
                if (g_state.online.count(s)) ++online_seeders;
            out += "\n" + fp.first + " size=" + std::to_string(fp.second.size) +
                   " seeders=" + std::to_string(online_seeders);
        }
        return out;
    }
    if (cmd == "GET_META") {
        if (t.size() != 3) return err("usage: download_file <gid> <filename> <dest>");
        auto it = g_state.groups.find(t[1]);
        if (it == g_state.groups.end()) return err("no such group");
        if (!is_member(it->second, sess.uid)) return err("not a member of this group");
        auto fit = it->second.files.find(t[2]);
        if (fit == it->second.files.end()) return err("no such file in group");
        const FileMeta &fm = fit->second;

        std::ostringstream os;
        os << "OK " << fm.size << " " << fm.filehash << " " << fm.npieces;
        for (auto &h : fm.piece_hashes) os << "\n" << h;
        // Collect online seeders other than the requester.
        std::vector<std::string> peers;
        for (auto &s : fm.seeders) {
            if (s == sess.uid) continue;
            auto on = g_state.online.find(s);
            if (on != g_state.online.end())
                peers.push_back(s + " " + on->second.ip + " " +
                                std::to_string(on->second.port));
        }
        os << "\nPEERS " << peers.size();
        for (auto &p : peers) os << "\n" << p;
        return os.str();
    }
    if (cmd == "ADD_SEEDER") {
        if (t.size() != 3) return err("malformed add_seeder");
        auto it = g_state.groups.find(t[1]);
        if (it == g_state.groups.end()) return err("no such group");
        auto fit = it->second.files.find(t[2]);
        if (fit == it->second.files.end()) return err("no such file");
        fit->second.seeders.insert(sess.uid);
        record_op("SEED_ADD " + t[1] + " " + t[2] + " " + sess.uid);
        return "OK registered as seeder";
    }
    if (cmd == "STOP_SHARE") {
        if (t.size() != 3) return err("usage: stop_share <gid> <filename>");
        auto it = g_state.groups.find(t[1]);
        if (it == g_state.groups.end()) return err("no such group");
        auto fit = it->second.files.find(t[2]);
        if (fit == it->second.files.end()) return err("no such file");
        fit->second.seeders.erase(sess.uid);
        record_op("SEED_DEL " + t[1] + " " + t[2] + " " + sess.uid);
        return "OK stopped sharing " + t[2];
    }

    return err("unknown command");
}

// ---------------------------------------------------------------------------
//  Connection threads
// ---------------------------------------------------------------------------

// Handle an inbound synchronisation link (the peer tracker pushing its ops).
void sync_inbound_thread(int fd) {
    std::cout << "[sync] peer tracker connected (inbound)\n" << std::flush;
    std::string frame;
    while (g_running && net::recv_msg(fd, frame)) {
        if (frame == "SNAP_BEGIN" || frame == "SNAP_END") continue;
        std::vector<std::string> t = util::split_ws(frame);
        std::lock_guard<std::mutex> lk(g_state.mtx);
        apply_sync_op(t);
    }
    std::cout << "[sync] inbound link closed\n" << std::flush;
    ::close(fd);
}

// Maintain the outbound synchronisation link to the peer tracker: connect,
// push a full snapshot, then stream live ops; reconnect forever on failure.
void sync_connector_thread(std::string peer_ip, int peer_port) {
    while (g_running) {
        int fd = net::connect_to(peer_ip, peer_port);
        if (fd < 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        std::cout << "[sync] connected to peer tracker (outbound)\n" << std::flush;

        bool ok = net::send_msg(fd, "HELLO " + std::to_string(g_my_no));

        // Snapshot + queue-clear must be atomic so no mutation is lost or
        // double-counted across the snapshot/live boundary.
        std::vector<std::string> snapshot;
        {
            std::lock_guard<std::mutex> lk(g_state.mtx);
            snapshot = build_snapshot_ops();
            g_pending.clear();
        }
        ok = ok && net::send_msg(fd, "SNAP_BEGIN");
        for (auto &op : snapshot) {
            if (!ok) break;
            ok = net::send_msg(fd, op);
        }
        ok = ok && net::send_msg(fd, "SNAP_END");

        // Live streaming loop.
        while (ok && g_running) {
            std::vector<std::string> batch;
            {
                std::unique_lock<std::mutex> lk(g_state.mtx);
                g_ops_cv.wait_for(lk, std::chrono::seconds(1), [] {
                    return !g_pending.empty() || !g_running;
                });
                while (!g_pending.empty()) {
                    batch.push_back(std::move(g_pending.front()));
                    g_pending.pop_front();
                }
            }
            for (auto &op : batch) {
                if (!net::send_msg(fd, op)) { ok = false; break; }
            }
        }
        ::close(fd);
        if (g_running) {
            std::cout << "[sync] outbound link lost; will retry\n" << std::flush;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

// Accept loop: classify each connection as a peer-tracker sync link or a
// client by peeking at its first frame.
void accept_thread() {
    while (g_running) {
        std::string peer_ip;
        int fd = net::accept_conn(g_listen_fd, peer_ip);
        if (fd < 0) {
            if (!g_running) break;
            continue;
        }
        std::string first;
        if (!net::recv_msg(fd, first)) { ::close(fd); continue; }

        std::vector<std::string> t = util::split_ws(first);
        if (!t.empty() && t[0] == "HELLO") {
            std::thread(sync_inbound_thread, fd).detach();
        } else {
            // First frame is already a client request: serve it, then loop.
            std::thread([fd, peer_ip, first]() {
                Session sess;
                sess.peer_ip = peer_ip;
                std::string resp = handle_request(sess, first);
                if (net::send_msg(fd, resp)) {
                    std::string req;
                    while (g_running && net::recv_msg(fd, req)) {
                        std::string r = handle_request(sess, req);
                        if (!net::send_msg(fd, r)) break;
                    }
                }
                if (!sess.uid.empty()) {
                    std::lock_guard<std::mutex> lk(g_state.mtx);
                    g_state.online.erase(sess.uid);
                    record_op("OFFLINE " + sess.uid);
                }
                ::close(fd);
            }).detach();
        }
    }
}

} // namespace

// ---------------------------------------------------------------------------
//  main
// ---------------------------------------------------------------------------
int main(int argc, char **argv) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s tracker_info.txt <tracker_no>\n", argv[0]);
        return 1;
    }
    net::ignore_sigpipe();

    long long my_no = 0;
    if (!util::parse_int(argv[2], my_no) || my_no < 0) {
        std::fprintf(stderr, "tracker_no must be a non-negative integer\n");
        return 1;
    }
    g_my_no = (int)my_no;

    // Parse tracker_info.txt: one "ip port" per tracker, line index == number.
    std::ifstream tf(argv[1]);
    if (!tf) {
        std::fprintf(stderr, "cannot open %s\n", argv[1]);
        return 1;
    }
    std::vector<PeerAddr> trackers;
    std::string line;
    while (std::getline(tf, line)) {
        auto parts = util::split_ws(line);
        if (parts.size() >= 2) {
            long long p = 0;
            util::parse_int(parts[1], p);
            trackers.push_back(PeerAddr{parts[0], (int)p});
        }
    }
    if ((size_t)g_my_no >= trackers.size()) {
        std::fprintf(stderr, "tracker_no %d out of range (file has %zu trackers)\n",
                     g_my_no, trackers.size());
        return 1;
    }

    PeerAddr me = trackers[g_my_no];
    g_listen_fd = net::create_listener(me.port);
    if (g_listen_fd < 0) {
        std::fprintf(stderr, "failed to bind on port %d\n", me.port);
        return 1;
    }
    std::cout << "Tracker " << g_my_no << " listening on " << me.ip << ":"
              << me.port << "\n" << std::flush;

    std::thread accepter(accept_thread);

    // Start the outbound sync link to every *other* tracker (two-tracker
    // system -> exactly one peer, but the loop generalises cleanly).
    std::vector<std::thread> connectors;
    for (size_t i = 0; i < trackers.size(); ++i) {
        if ((int)i == g_my_no) continue;
        connectors.emplace_back(sync_connector_thread, trackers[i].ip, trackers[i].port);
    }

    // Console: wait for "quit".
    std::string cmd;
    while (std::getline(std::cin, cmd)) {
        cmd = util::trim(cmd);
        if (cmd == "quit") break;
        if (!cmd.empty())
            std::cout << "Unknown console command. Type 'quit' to shut down.\n"
                      << std::flush;
    }

    std::cout << "Shutting down tracker...\n" << std::flush;
    g_running = false;
    g_ops_cv.notify_all();
    ::shutdown(g_listen_fd, SHUT_RDWR);
    ::close(g_listen_fd);

    // Detached worker threads end with the process; nudge connectors to exit.
    accepter.detach();
    for (auto &c : connectors) c.detach();
    return 0;
}
