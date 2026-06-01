#ifndef P2P_NET_H
#define P2P_NET_H

#include <string>
#include <cstdint>

// Thin, blocking-socket helper layer over the raw POSIX sockets API.
//
// All control traffic (tracker<->client, tracker<->tracker, and the
// client<->client request/response handshake) uses a simple length-prefixed
// framing so that partial reads/writes are handled transparently:
//
//     [ 4-byte big-endian payload length ][ payload bytes ]
//
// The same framing is reused to ship a raw 512 KiB piece as the payload, so
// binary piece data never needs escaping.
namespace net {

// Largest single framed message we will accept (32 MiB). Comfortably above a
// 512 KiB piece plus headroom for large metadata blobs; protects against a
// corrupt/hostile length prefix.
constexpr uint32_t MAX_MSG = 32u * 1024u * 1024u;

// Block SIGPIPE process-wide so a write to a peer that vanished returns an
// error instead of killing us. Call once at startup.
void ignore_sigpipe();

// Write exactly `len` bytes, retrying on short writes / EINTR.
bool send_all(int fd, const void *buf, size_t len);

// Read exactly `len` bytes, retrying on short reads / EINTR. Returns false on
// EOF or error.
bool recv_all(int fd, void *buf, size_t len);

// Send `payload` as one length-prefixed frame.
bool send_msg(int fd, const std::string &payload);

// Receive one length-prefixed frame into `out`. Returns false on EOF/error.
bool recv_msg(int fd, std::string &out);

// Connect to host:port. Returns a connected fd or -1.
int connect_to(const std::string &host, int port);

// Create a listening TCP socket bound to `port` on all interfaces.
// Returns the listening fd or -1.
int create_listener(int port, int backlog = 64);

// Accept one connection; fills `peer_ip` with the dotted-quad of the peer.
// Returns the accepted fd or -1.
int accept_conn(int listen_fd, std::string &peer_ip);

// Return the local TCP port a socket is bound to (useful after binding to
// port 0 for an ephemeral peer-listening port). Returns -1 on error.
int local_port(int fd);

} // namespace net

#endif // P2P_NET_H
