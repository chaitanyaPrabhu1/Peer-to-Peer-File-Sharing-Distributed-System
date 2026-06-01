#include "tracker_conn.h"
#include "net.h"

#include <cstdio>
#include <unistd.h>

TrackerConn::TrackerConn(std::vector<TrackerInfo> trackers, int preferred, Registry *reg)
    : trackers_(std::move(trackers)), preferred_(preferred), reg_(reg) {}

TrackerConn::~TrackerConn() {
    if (fd_ >= 0) ::close(fd_);
}

void TrackerConn::set_session(const std::string &uid, const std::string &pass, int peer_port) {
    std::lock_guard<std::mutex> lk(m_);
    logged_in_ = true;
    uid_ = uid;
    pass_ = pass;
    peer_port_ = peer_port;
}

void TrackerConn::clear_session() {
    std::lock_guard<std::mutex> lk(m_);
    logged_in_ = false;
    uid_.clear();
    pass_.clear();
}

std::string TrackerConn::uid() {
    std::lock_guard<std::mutex> lk(m_);
    return uid_;
}

bool TrackerConn::logged_in() {
    std::lock_guard<std::mutex> lk(m_);
    return logged_in_;
}

bool TrackerConn::ensure_connected_locked() {
    if (fd_ >= 0) return true;

    // Try the user's preferred tracker first, then the others in order.
    std::vector<int> order;
    if (preferred_ >= 0 && preferred_ < (int)trackers_.size())
        order.push_back(preferred_);
    for (int i = 0; i < (int)trackers_.size(); ++i)
        if (i != preferred_) order.push_back(i);

    for (int idx : order) {
        int fd = net::connect_to(trackers_[idx].ip, trackers_[idx].port);
        if (fd < 0) continue;
        fd_ = fd;

        if (logged_in_) {
            // Re-authenticate on the freshly chosen tracker.
            std::string resp;
            std::string login = "LOGIN " + uid_ + " " + pass_ + " " +
                                std::to_string(peer_port_);
            if (!net::send_msg(fd_, login) || !net::recv_msg(fd_, resp) ||
                resp.compare(0, 2, "OK") != 0) {
                ::close(fd_);
                fd_ = -1;
                continue;
            }
            // Re-announce everything we are sharing so peer discovery on the
            // new tracker knows about us immediately.
            for (auto &k : reg_->keys()) {
                std::string r;
                std::string ann = "ADD_SEEDER " + k.first + " " + k.second;
                if (net::send_msg(fd_, ann)) net::recv_msg(fd_, r);
            }
            std::fprintf(stderr,
                         "[tracker] failed over to %s:%d and restored session\n",
                         trackers_[idx].ip.c_str(), trackers_[idx].port);
        }
        return true;
    }
    return false;
}

std::string TrackerConn::request(const std::string &req) {
    std::lock_guard<std::mutex> lk(m_);
    for (int attempt = 0; attempt < 3; ++attempt) {
        if (!ensure_connected_locked()) return "ERR no tracker reachable";
        std::string resp;
        if (net::send_msg(fd_, req) && net::recv_msg(fd_, resp)) return resp;
        // The link broke mid-request; drop it and let the next attempt fail
        // over to another tracker.
        ::close(fd_);
        fd_ = -1;
    }
    return "ERR tracker communication failed";
}
