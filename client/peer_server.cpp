#include "peer_server.h"
#include "net.h"
#include "util.h"

#include <sys/socket.h>
#include <unistd.h>
#include <cstdio>

bool PeerServer::start() {
    listen_fd_ = net::create_listener(0);   // ephemeral port
    if (listen_fd_ < 0) return false;
    port_ = net::local_port(listen_fd_);
    if (port_ < 0) { ::close(listen_fd_); listen_fd_ = -1; return false; }
    running_ = true;
    accepter_ = std::thread(&PeerServer::accept_loop, this);
    return true;
}

void PeerServer::stop() {
    if (!running_.exchange(false)) return;
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (accepter_.joinable()) accepter_.join();
}

void PeerServer::accept_loop() {
    while (running_) {
        std::string ip;
        int fd = net::accept_conn(listen_fd_, ip);
        if (fd < 0) {
            if (!running_) break;
            continue;
        }
        std::thread(&PeerServer::conn_handler, this, fd).detach();
    }
}

// One peer may request many pieces over a single connection, so we loop.
void PeerServer::conn_handler(int fd) {
    std::string req;
    while (running_ && net::recv_msg(fd, req)) {
        std::vector<std::string> t = util::split_ws(req);
        if (t.empty()) continue;

        if (t[0] == "BITFIELD" && t.size() >= 3) {
            auto sf = reg_->get(t[1], t[2]);
            if (!sf) { net::send_msg(fd, "ERR no such file"); continue; }
            net::send_msg(fd, "OK " + std::to_string(sf->npieces) + " " +
                                  sf->bitstring());
        } else if (t[0] == "GET" && t.size() >= 4) {
            long long idx = -1;
            util::parse_int(t[3], idx);
            auto sf = reg_->get(t[1], t[2]);
            if (!sf || idx < 0 || idx >= sf->npieces || !sf->has_piece((int)idx)) {
                net::send_msg(fd, "ERR notavail");
                continue;
            }
            uint64_t len = sf->piece_len((int)idx);
            std::string buf;
            buf.resize(len);
            ssize_t n = ::pread(sf->fd, &buf[0], len, (off_t)sf->piece_off((int)idx));
            if (n != (ssize_t)len) {
                net::send_msg(fd, "ERR read failed");
                continue;
            }
            // Frame 1: status/length. Frame 2: the raw piece bytes.
            if (!net::send_msg(fd, "OK " + std::to_string(len))) break;
            if (!net::send_msg(fd, buf)) break;
        } else {
            net::send_msg(fd, "ERR bad request");
        }
    }
    ::close(fd);
}
