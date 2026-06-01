// ===========================================================================
//  client.cpp  --  Peer-to-Peer Distributed File Sharing System (AOS A3)
//
//  A client authenticates with a tracker, joins/creates groups, shares files
//  and downloads files in parallel from multiple peers. It is simultaneously a
//  seeder (serving pieces of everything it shares) and a downloader.
//
//  Usage:  ./client <IP>:<PORT> tracker_info.txt
//          (<IP>:<PORT> is the tracker to contact first; tracker_info.txt
//           lists all trackers so the client can fail over.)
// ===========================================================================

#include "client_state.h"
#include "tracker_conn.h"
#include "peer_server.h"
#include "download.h"
#include "net.h"
#include "util.h"
#include "sha1.h"

#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {

// Compute the whole-file SHA-1 and the per-piece SHA-1s by streaming the file
// from disk one piece at a time (constant memory, safe for 1 GiB files).
bool compute_hashes(int fd, uint64_t size, int npieces, std::string &filehash,
                    std::vector<std::string> &piece_hashes) {
    SHA1 whole;
    std::string buf;
    buf.resize(npieces > 0 ? proto::PIECE_SIZE : 1);
    for (int i = 0; i < npieces; ++i) {
        uint64_t off = (uint64_t)i * proto::PIECE_SIZE;
        uint64_t len = std::min<uint64_t>(proto::PIECE_SIZE, size - off);
        ssize_t n = ::pread(fd, &buf[0], len, (off_t)off);
        if (n != (ssize_t)len) return false;
        whole.update(buf.data(), len);
        piece_hashes.push_back(SHA1::hash(buf.data(), len));
    }
    filehash = whole.final();   // SHA1("") for an empty file
    return true;
}

void print_response(const std::string &resp) {
    if (resp.compare(0, 2, "OK") == 0) {
        std::string body = resp.size() > 3 ? resp.substr(3) : "";
        if (body.empty()) std::cout << "OK\n";
        else std::cout << body << "\n";
    } else if (resp.compare(0, 4, "ERR ") == 0) {
        std::cout << "Error: " << resp.substr(4) << "\n";
    } else {
        std::cout << resp << "\n";
    }
    std::cout.flush();
}

void print_help() {
    std::cout <<
        "Commands:\n"
        "  create_user <uid> <password>\n"
        "  login <uid> <password>\n"
        "  logout\n"
        "  create_group <gid>\n"
        "  join_group <gid>\n"
        "  leave_group <gid>\n"
        "  list_groups\n"
        "  list_requests <gid>\n"
        "  accept_request <gid> <uid>\n"
        "  upload_file <gid> <file_path>\n"
        "  list_files <gid>\n"
        "  download_file <gid> <file_name> <destination_path>\n"
        "  show_downloads\n"
        "  stop_share <gid> <file_name>\n"
        "  help | quit\n";
    std::cout.flush();
}

} // namespace

int main(int argc, char **argv) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <IP>:<PORT> tracker_info.txt\n", argv[0]);
        return 1;
    }
    net::ignore_sigpipe();

    // Parse the preferred tracker "IP:PORT".
    std::vector<std::string> hp = util::split(argv[1], ':');
    if (hp.size() != 2) {
        std::fprintf(stderr, "tracker address must be IP:PORT\n");
        return 1;
    }
    long long pref_port = 0;
    if (!util::parse_int(hp[1], pref_port)) {
        std::fprintf(stderr, "bad tracker port\n");
        return 1;
    }

    // Load the full tracker list (for fail-over).
    std::vector<TrackerInfo> trackers;
    std::ifstream tf(argv[2]);
    if (!tf) {
        std::fprintf(stderr, "cannot open %s\n", argv[2]);
        return 1;
    }
    std::string line;
    while (std::getline(tf, line)) {
        auto parts = util::split_ws(line);
        if (parts.size() >= 2) {
            long long p = 0;
            util::parse_int(parts[1], p);
            trackers.push_back(TrackerInfo{parts[0], (int)p});
        }
    }
    // Determine the preferred index; if not present in the file, prepend it.
    int preferred = -1;
    for (int i = 0; i < (int)trackers.size(); ++i)
        if (trackers[i].ip == hp[0] && trackers[i].port == (int)pref_port)
            preferred = i;
    if (preferred < 0) {
        trackers.insert(trackers.begin(), TrackerInfo{hp[0], (int)pref_port});
        preferred = 0;
    }

    // Bring up the local state, peer-seeder server, tracker link and downloads.
    Registry registry;
    PeerServer peer_server(&registry);
    if (!peer_server.start()) {
        std::fprintf(stderr, "failed to start peer server\n");
        return 1;
    }
    int peer_port = peer_server.port();

    TrackerConn tracker(trackers, preferred, &registry);
    Downloads downloads;
    DownloadManager dlmgr(&tracker, &registry, &downloads);

    std::cout << "Client started. Peer server listening on port " << peer_port
              << ". Type 'help' for commands.\n";
    std::cout.flush();

    bool tty = isatty(STDIN_FILENO);
    std::string input;
    while (true) {
        if (tty) { std::cout << "p2p> "; std::cout.flush(); }
        if (!std::getline(std::cin, input)) break;       // EOF
        std::vector<std::string> t = util::split_ws(input);
        if (t.empty()) continue;
        const std::string &cmd = t[0];

        if (cmd == "quit" || cmd == "exit") {
            if (tracker.logged_in()) tracker.request("LOGOUT");
            break;
        }
        if (cmd == "help") { print_help(); continue; }
        if (cmd == "show_downloads") {
            auto all = downloads.snapshot();
            if (all.empty()) std::cout << "No downloads.\n";
            for (auto &d : all) {
                int st = d->status.load();
                if (st == 1) {
                    std::cout << "[C] [" << d->group << "] " << d->filename << "\n";
                } else if (st == 2) {
                    std::cout << "[F] [" << d->group << "] " << d->filename
                              << "  (" << d->error << ")\n";
                } else {
                    int done = d->completed.load();
                    int pct = d->npieces ? (done * 100 / d->npieces) : 100;
                    std::cout << "[D] [" << d->group << "] " << d->filename
                              << "  " << done << "/" << d->npieces
                              << " (" << pct << "%)\n";
                }
            }
            std::cout.flush();
            continue;
        }

        // --- session ---
        if (cmd == "create_user" && t.size() == 3) {
            print_response(tracker.request("CREATE_USER " + t[1] + " " + t[2]));
            continue;
        }
        if (cmd == "login" && t.size() == 3) {
            std::string resp = tracker.request("LOGIN " + t[1] + " " + t[2] +
                                               " " + std::to_string(peer_port));
            if (resp.compare(0, 2, "OK") == 0)
                tracker.set_session(t[1], t[2], peer_port);
            print_response(resp);
            continue;
        }
        if (cmd == "logout") {
            print_response(tracker.request("LOGOUT"));
            tracker.clear_session();
            registry.clear();          // stop sharing all files
            continue;
        }

        // Everything below needs an authenticated session.
        if (!tracker.logged_in() &&
            cmd != "create_user" && cmd != "login") {
            std::cout << "Please login first.\n";
            continue;
        }

        // --- groups ---
        if (cmd == "create_group" && t.size() == 2) {
            print_response(tracker.request("CREATE_GROUP " + t[1]));
        } else if (cmd == "join_group" && t.size() == 2) {
            print_response(tracker.request("JOIN_GROUP " + t[1]));
        } else if (cmd == "leave_group" && t.size() == 2) {
            print_response(tracker.request("LEAVE_GROUP " + t[1]));
        } else if (cmd == "list_groups") {
            print_response(tracker.request("LIST_GROUPS"));
        } else if (cmd == "list_requests" && t.size() == 2) {
            print_response(tracker.request("LIST_REQ " + t[1]));
        } else if (cmd == "accept_request" && t.size() == 3) {
            print_response(tracker.request("ACCEPT_REQ " + t[1] + " " + t[2]));
        }
        // --- files ---
        else if (cmd == "upload_file" && t.size() == 3) {
            const std::string &gid = t[1];
            const std::string &path = t[2];
            int fd = ::open(path.c_str(), O_RDONLY);
            if (fd < 0) { std::cout << "Error: cannot open file\n"; continue; }
            struct stat st;
            if (::fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
                std::cout << "Error: not a regular file\n";
                ::close(fd);
                continue;
            }
            uint64_t size = (uint64_t)st.st_size;
            int npieces = (int)((size + proto::PIECE_SIZE - 1) / proto::PIECE_SIZE);
            std::string filehash;
            std::vector<std::string> phs;
            if (!compute_hashes(fd, size, npieces, filehash, phs)) {
                std::cout << "Error: failed to read file\n";
                ::close(fd);
                continue;
            }
            std::string fname = util::basename(path);
            std::string req = "UPLOAD " + gid + " " + fname + " " +
                              std::to_string(size) + " " + filehash + " " +
                              std::to_string(npieces);
            for (auto &h : phs) req += " " + h;

            std::string resp = tracker.request(req);
            if (resp.compare(0, 2, "OK") == 0) {
                auto sf = std::make_shared<SharedFile>();
                sf->group = gid; sf->filename = fname; sf->local_path = path;
                sf->size = size; sf->npieces = npieces;
                sf->filehash = filehash; sf->piece_hashes = phs;
                sf->have.assign(npieces, 1);     // we have every piece
                sf->fd = fd;                      // SharedFile now owns the fd
                registry.add(sf);
            } else {
                ::close(fd);
            }
            print_response(resp);
        } else if (cmd == "list_files" && t.size() == 2) {
            print_response(tracker.request("LIST_FILES " + t[1]));
        } else if (cmd == "download_file" && t.size() == 4) {
            std::string err;
            if (dlmgr.start(t[1], t[2], t[3], err))
                std::cout << "Download started: [" << t[1] << "] " << t[2] << "\n";
            else
                std::cout << "Error: " << err << "\n";
        } else if (cmd == "stop_share" && t.size() == 3) {
            registry.remove(t[1], t[2]);
            print_response(tracker.request("STOP_SHARE " + t[1] + " " + t[2]));
        } else {
            std::cout << "Unknown or malformed command. Type 'help'.\n";
        }
        std::cout.flush();
    }

    peer_server.stop();
    return 0;
}
