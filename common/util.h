#ifndef P2P_UTIL_H
#define P2P_UTIL_H

#include <string>
#include <vector>
#include <cstdint>

namespace util {

// Split `s` into whitespace-separated tokens (collapsing runs of spaces).
std::vector<std::string> split_ws(const std::string &s);

// Split `s` on `delim` into at most `max_parts` parts (the last part keeps
// any remaining delimiters). max_parts <= 0 means unlimited.
std::vector<std::string> split(const std::string &s, char delim, int max_parts = 0);

// Trim leading/trailing whitespace (space, tab, CR, LF).
std::string trim(const std::string &s);

// Join tokens with a single space.
std::string join(const std::vector<std::string> &parts, char sep = ' ');

// Numeric parsing helpers that never throw.
bool parse_int(const std::string &s, long long &out);
bool parse_u64(const std::string &s, uint64_t &out);

// Extract the base file name from a path (portion after the last '/').
std::string basename(const std::string &path);

} // namespace util

#endif // P2P_UTIL_H
