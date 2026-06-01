#ifndef P2P_PEER_SERVER_H
#define P2P_PEER_SERVER_H

#include "client_state.h"
#include <atomic>
#include <thread>

// Listens for connections from other clients and serves piece data and piece
// bitfields out of the local Registry. Because a file being downloaded is
// also registered (with a growing bitfield), this seeds partial content too.
class PeerServer {
public:
    explicit PeerServer(Registry *reg) : reg_(reg) {}
    ~PeerServer() { stop(); }

    // Bind an ephemeral port and start accepting. Returns false on failure.
    bool start();
    void stop();
    int  port() const { return port_; }

private:
    void accept_loop();
    void conn_handler(int fd);

    Registry         *reg_;
    int               listen_fd_ = -1;
    int               port_ = 0;
    std::atomic<bool> running_{false};
    std::thread       accepter_;
};

#endif // P2P_PEER_SERVER_H
