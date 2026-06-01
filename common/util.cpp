#include "util.h"
#include <cctype>
#include <cstdlib>
#include <cerrno>

namespace util {

std::vector<std::string> split_ws(const std::string &s) {
    std::vector<std::string> out;
    size_t i = 0, n = s.size();
    while (i < n) {
        while (i < n && std::isspace((unsigned char)s[i])) ++i;
        if (i >= n) break;
        size_t start = i;
        while (i < n && !std::isspace((unsigned char)s[i])) ++i;
        out.push_back(s.substr(start, i - start));
    }
    return out;
}

std::vector<std::string> split(const std::string &s, char delim, int max_parts) {
    std::vector<std::string> out;
    size_t start = 0;
    while (true) {
        if (max_parts > 0 && (int)out.size() == max_parts - 1) {
            out.push_back(s.substr(start));
            break;
        }
        size_t pos = s.find(delim, start);
        if (pos == std::string::npos) {
            out.push_back(s.substr(start));
            break;
        }
        out.push_back(s.substr(start, pos - start));
        start = pos + 1;
    }
    return out;
}

std::string trim(const std::string &s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace((unsigned char)s[a])) ++a;
    while (b > a && std::isspace((unsigned char)s[b - 1])) --b;
    return s.substr(a, b - a);
}

std::string join(const std::vector<std::string> &parts, char sep) {
    std::string out;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i) out += sep;
        out += parts[i];
    }
    return out;
}

bool parse_int(const std::string &s, long long &out) {
    if (s.empty()) return false;
    errno = 0;
    char *end = nullptr;
    long long v = strtoll(s.c_str(), &end, 10);
    if (errno != 0 || end == s.c_str() || *end != '\0') return false;
    out = v;
    return true;
}

bool parse_u64(const std::string &s, uint64_t &out) {
    if (s.empty()) return false;
    errno = 0;
    char *end = nullptr;
    unsigned long long v = strtoull(s.c_str(), &end, 10);
    if (errno != 0 || end == s.c_str() || *end != '\0') return false;
    out = v;
    return true;
}

std::string basename(const std::string &path) {
    size_t pos = path.find_last_of('/');
    if (pos == std::string::npos) return path;
    return path.substr(pos + 1);
}

} // namespace util
