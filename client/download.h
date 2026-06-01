#ifndef P2P_DOWNLOAD_H
#define P2P_DOWNLOAD_H

#include "client_state.h"
#include "tracker_conn.h"
#include <string>

// Orchestrates a single multi-peer download in the background.
//
//  * asks the tracker for file metadata + the current set of online seeders;
//  * opens several connections across all peers (parallelism even from one
//    seeder) and fetches each peer's piece bitfield;
//  * hands out pieces rarest-first to a pool of workers;
//  * verifies every piece against its SHA-1 before writing it at the correct
//    offset, re-queuing corrupt/failed pieces to a different peer;
//  * registers the partially-downloaded file so the client seeds pieces it has
//    already obtained, and verifies the whole-file SHA-1 on completion.
class DownloadManager {
public:
    DownloadManager(TrackerConn *tc, Registry *reg, Downloads *dls)
        : tc_(tc), reg_(reg), dls_(dls) {}

    // Validates metadata and pre-allocates the destination, then runs the
    // transfer on a detached background thread. Returns false (with `err`) if
    // the download could not even be started.
    bool start(const std::string &group, const std::string &fname,
               const std::string &dest, std::string &err);

private:
    void run(std::shared_ptr<SharedFile> sf, std::shared_ptr<Download> dl,
             std::string peers_blob);

    TrackerConn *tc_;
    Registry    *reg_;
    Downloads   *dls_;
};

#endif // P2P_DOWNLOAD_H
