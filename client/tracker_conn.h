#ifndef P2P_TRACKER_CONN_H
#define P2P_TRACKER_CONN_H

#include "client_state.h"

#include <mutex>
#include <string>
#include <vector>

struct TrackerInfo {
    std::string ip;
    int port = 0;
};

// A resilient control channel to the tracker pair. All requests are
// serialised. If the active tracker dies, the next request transparently
// fails over to the other tracker, re-authenticates the session, and
// re-announces every file this client is sharing -- so the user's session
// survives a tracker crash (the system keeps working while one tracker lives).
class TrackerConn {
public:
    TrackerConn(std::vector<TrackerInfo> trackers, int preferred, Registry *reg);
    ~TrackerConn();

    // Send one request, return the tracker's response (or an "ERR ..." string
    // if no tracker could be reached). Thread-safe.
    std::string request(const std::string &req);

    // Remember credentials so reconnects can restore the session.
    void set_session(const std::string &uid, const std::string &pass, int peer_port);
    void clear_session();

    std::string uid();
    bool logged_in();

private:
    bool ensure_connected_locked();   // m_ held by caller

    std::vector<TrackerInfo> trackers_;
    int preferred_;
    int fd_ = -1;
    std::mutex m_;

    bool        logged_in_ = false;
    std::string uid_;
    std::string pass_;
    int         peer_port_ = 0;
    Registry   *reg_;
};

#endif // P2P_TRACKER_CONN_H
