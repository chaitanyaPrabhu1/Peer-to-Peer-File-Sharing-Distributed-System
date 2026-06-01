#include "sha1.h"
#include <cstring>
#include <cstdio>

static inline uint32_t rotl32(uint32_t x, int n) {
    return (x << n) | (x >> (32 - n));
}

SHA1::SHA1() { reset(); }

void SHA1::reset() {
    h_[0] = 0x67452301u;
    h_[1] = 0xEFCDAB89u;
    h_[2] = 0x98BADCFEu;
    h_[3] = 0x10325476u;
    h_[4] = 0xC3D2E1F0u;
    total_bits_ = 0;
    buffer_len_ = 0;
}

void SHA1::process_block(const uint8_t *block) {
    uint32_t w[80];
    // Load the 16 big-endian words of this 512-bit block.
    for (int i = 0; i < 16; ++i) {
        w[i] = (uint32_t(block[i * 4]) << 24) |
               (uint32_t(block[i * 4 + 1]) << 16) |
               (uint32_t(block[i * 4 + 2]) << 8) |
               (uint32_t(block[i * 4 + 3]));
    }
    // Message schedule expansion.
    for (int i = 16; i < 80; ++i) {
        w[i] = rotl32(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }

    uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3], e = h_[4];

    for (int i = 0; i < 80; ++i) {
        uint32_t f, k;
        if (i < 20)      { f = (b & c) | ((~b) & d);          k = 0x5A827999u; }
        else if (i < 40) { f = b ^ c ^ d;                     k = 0x6ED9EBA1u; }
        else if (i < 60) { f = (b & c) | (b & d) | (c & d);   k = 0x8F1BBCDCu; }
        else             { f = b ^ c ^ d;                     k = 0xCA62C1D6u; }

        uint32_t tmp = rotl32(a, 5) + f + e + k + w[i];
        e = d;
        d = c;
        c = rotl32(b, 30);
        b = a;
        a = tmp;
    }

    h_[0] += a;
    h_[1] += b;
    h_[2] += c;
    h_[3] += d;
    h_[4] += e;
}

void SHA1::update(const void *data, size_t len) {
    const uint8_t *p = static_cast<const uint8_t *>(data);
    total_bits_ += uint64_t(len) * 8;

    // If we have a partial buffer, top it up first.
    if (buffer_len_ > 0) {
        size_t need = 64 - buffer_len_;
        size_t take = (len < need) ? len : need;
        memcpy(buffer_ + buffer_len_, p, take);
        buffer_len_ += take;
        p += take;
        len -= take;
        if (buffer_len_ == 64) {
            process_block(buffer_);
            buffer_len_ = 0;
        }
    }

    // Process as many full 64-byte blocks as we can directly.
    while (len >= 64) {
        process_block(p);
        p += 64;
        len -= 64;
    }

    // Stash the remainder.
    if (len > 0) {
        memcpy(buffer_ + buffer_len_, p, len);
        buffer_len_ += len;
    }
}

std::string SHA1::final() {
    // Append the 0x80 padding byte then zeros until 56 mod 64, then the
    // 64-bit big-endian length.
    uint64_t bits = total_bits_;
    uint8_t pad = 0x80;
    update(&pad, 1);

    uint8_t zero = 0x00;
    while (buffer_len_ != 56) {
        update(&zero, 1);
    }

    uint8_t len_bytes[8];
    for (int i = 0; i < 8; ++i) {
        len_bytes[i] = uint8_t((bits >> (56 - i * 8)) & 0xFF);
    }
    // update() would re-add to total_bits_, so write the length manually.
    memcpy(buffer_ + buffer_len_, len_bytes, 8);
    buffer_len_ += 8;
    process_block(buffer_);
    buffer_len_ = 0;

    char out[41];
    for (int i = 0; i < 5; ++i) {
        snprintf(out + i * 8, 9, "%08x", h_[i]);
    }
    std::string result(out, 40);
    reset();
    return result;
}

std::string SHA1::hash(const void *data, size_t len) {
    SHA1 ctx;
    ctx.update(data, len);
    return ctx.final();
}
