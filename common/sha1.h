#ifndef P2P_SHA1_H
#define P2P_SHA1_H

#include <cstdint>
#include <cstddef>
#include <string>

// A small, self-contained SHA-1 implementation (FIPS 180-1).
//
// We implement SHA-1 ourselves because the assignment forbids high-level /
// external libraries.  The API is intentionally streaming so that a 1 GiB
// file can be hashed without ever holding more than one piece in memory.
class SHA1 {
public:
    SHA1();

    // Reset the context so the object can be reused for another hash.
    void reset();

    // Feed an arbitrary chunk of bytes into the running digest.
    void update(const void *data, size_t len);

    // Finalise and return the 40-char lowercase hex digest. After calling
    // final() the object is reset and ready for a new hash.
    std::string final();

    // Convenience one-shot helper for an in-memory buffer.
    static std::string hash(const void *data, size_t len);

private:
    void process_block(const uint8_t *block);

    uint32_t h_[5];          // running hash state
    uint64_t total_bits_;    // total length of the message in bits
    uint8_t  buffer_[64];    // partial 512-bit block
    size_t   buffer_len_;    // bytes currently buffered
};

#endif // P2P_SHA1_H
