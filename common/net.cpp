#include "net.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <signal.h>
#include <cstring>
#include <cerrno>

namespace net {

void ignore_sigpipe() {
    signal(SIGPIPE, SIG_IGN);
}

bool send_all(int fd, const void *buf, size_t len) {
    const char *p = static_cast<const char *>(buf);
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, p + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        sent += (size_t)n;
    }
    return true;
}

bool recv_all(int fd, void *buf, size_t len) {
    char *p = static_cast<char *>(buf);
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::recv(fd, p + got, len - got, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false; // peer closed
        got += (size_t)n;
    }
    return true;
}

bool send_msg(int fd, const std::string &payload) {
    if (payload.size() > MAX_MSG) return false;
    uint32_t len = (uint32_t)payload.size();
    uint8_t hdr[4] = {
        uint8_t((len >> 24) & 0xFF),
        uint8_t((len >> 16) & 0xFF),
        uint8_t((len >> 8) & 0xFF),
        uint8_t(len & 0xFF)};
    if (!send_all(fd, hdr, 4)) return false;
    if (len == 0) return true;
    return send_all(fd, payload.data(), payload.size());
}

bool recv_msg(int fd, std::string &out) {
    uint8_t hdr[4];
    if (!recv_all(fd, hdr, 4)) return false;
    uint32_t len = (uint32_t(hdr[0]) << 24) | (uint32_t(hdr[1]) << 16) |
                   (uint32_t(hdr[2]) << 8) | uint32_t(hdr[3]);
    if (len > MAX_MSG) return false;
    out.resize(len);
    if (len == 0) return true;
    return recv_all(fd, &out[0], len);
}

int connect_to(const std::string &host, int port) {
    struct addrinfo hints, *res = nullptr;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    std::string port_str = std::to_string(port);
    if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0)
        return -1;

    int fd = -1;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd >= 0) {
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    return fd;
}

int create_listener(int port, int backlog) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (::bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    if (::listen(fd, backlog) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

int accept_conn(int listen_fd, std::string &peer_ip) {
    struct sockaddr_in addr;
    socklen_t alen = sizeof(addr);
    int fd = ::accept(listen_fd, (struct sockaddr *)&addr, &alen);
    if (fd < 0) return -1;
    char buf[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
    peer_ip = buf;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

int local_port(int fd) {
    struct sockaddr_in addr;
    socklen_t alen = sizeof(addr);
    if (getsockname(fd, (struct sockaddr *)&addr, &alen) < 0) return -1;
    return ntohs(addr.sin_port);
}

} // namespace net
