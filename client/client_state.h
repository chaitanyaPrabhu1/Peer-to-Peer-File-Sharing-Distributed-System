#ifndef P2P_CLIENT_STATE_H
#define P2P_CLIENT_STATE_H

#include "protocol.h"

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unistd.h>

// ---------------------------------------------------------------------------
//  A file this client can serve to peers. The same structure represents both
//  a fully-uploaded file (every piece present) and a file currently being
//  downloaded (pieces filled in as they arrive -> the client seeds partial
//  content while it downloads, BitTorrent-style).
// ---------------------------------------------------------------------------
struct SharedFile {
    std::string group;
    std::string filename;
    std::string local_path;            // backing file on disk
    uint64_t    size = 0;
    int         npieces = 0;
    std::string filehash;
    std::vector<std::string> piece_hashes;

    int fd = -1;                       // open descriptor for pread/pwrite
    std::vector<char> have;            // have[i] != 0  => piece i present & verified
    std::mutex m;                      // guards `have`

    ~SharedFile() { if (fd >= 0) ::close(fd); }

    // Byte length of piece i (the final piece may be short).
    uint64_t piece_len(int i) const {
        uint64_t off = (uint64_t)i * proto::PIECE_SIZE;
        uint64_t rem = size - off;
        return rem < proto::PIECE_SIZE ? rem : proto::PIECE_SIZE;
    }
    uint64_t piece_off(int i) const { return (uint64_t)i * proto::PIECE_SIZE; }

    bool has_piece(int i) {
        std::lock_guard<std::mutex> lk(m);
        return i >= 0 && i < (int)have.size() && have[i];
    }
    void mark_have(int i) {
        std::lock_guard<std::mutex> lk(m);
        if (i >= 0 && i < (int)have.size()) have[i] = 1;
    }
    // '0'/'1' string describing which pieces we currently hold.
    std::string bitstring() {
        std::lock_guard<std::mutex> lk(m);
        std::string s(npieces, '0');
        for (int i = 0; i < npieces && i < (int)have.size(); ++i)
            if (have[i]) s[i] = '1';
        return s;
    }
};

// Thread-safe registry of the files this client is sharing, keyed by
// (group, filename).
class Registry {
public:
    using Key = std::pair<std::string, std::string>;

    void add(const std::shared_ptr<SharedFile> &sf) {
        std::lock_guard<std::mutex> lk(m_);
        files_[{sf->group, sf->filename}] = sf;
    }
    std::shared_ptr<SharedFile> get(const std::string &g, const std::string &f) {
        std::lock_guard<std::mutex> lk(m_);
        auto it = files_.find({g, f});
        return it == files_.end() ? nullptr : it->second;
    }
    void remove(const std::string &g, const std::string &f) {
        std::lock_guard<std::mutex> lk(m_);
        files_.erase({g, f});       // fd closed when last shared_ptr drops
    }
    std::vector<Key> keys() {
        std::lock_guard<std::mutex> lk(m_);
        std::vector<Key> out;
        for (auto &kv : files_) out.push_back(kv.first);
        return out;
    }
    void clear() {
        std::lock_guard<std::mutex> lk(m_);
        files_.clear();
    }

private:
    std::mutex m_;
    std::map<Key, std::shared_ptr<SharedFile>> files_;
};

// Progress record for show_downloads.
struct Download {
    std::string group, filename, dest;
    int npieces = 0;
    std::atomic<int> completed{0};
    std::atomic<int> status{0};        // 0 = active, 1 = done, 2 = failed
    std::string error;
};

class Downloads {
public:
    std::shared_ptr<Download> create(const std::string &g, const std::string &f,
                                     const std::string &dest, int npieces) {
        auto d = std::make_shared<Download>();
        d->group = g; d->filename = f; d->dest = dest; d->npieces = npieces;
        std::lock_guard<std::mutex> lk(m_);
        list_.push_back(d);
        return d;
    }
    std::vector<std::shared_ptr<Download>> snapshot() {
        std::lock_guard<std::mutex> lk(m_);
        return list_;
    }
private:
    std::mutex m_;
    std::vector<std::shared_ptr<Download>> list_;
};

#endif // P2P_CLIENT_STATE_H
