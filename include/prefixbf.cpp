#include <vector>
#include <memory>
#include <cassert>
#include <inttypes.h>
#include <cstring>
#include <cmath>
#include <random>
#include <map>
#include <functional>

#include "prefixbf.hpp"
#include "config.hpp"

namespace proteus {

bool PrefixBF::get(uint64_t i) const {
    return (data_[div8(i)] >> (7 - mod8(i))) & 1;
}

void PrefixBF::set(uint64_t i, bool v) {
    if (get(i) != v) {
        data_[div8(i)] ^= (1 << (7 - mod8(i)));
    }
}

bool PrefixBF::Query(const uint64_t key, bool shift) {
    bool out = true;
    uint64_t k = shift ? key >> (64 - prefix_len_) : key;
    for (size_t i = 0; i < seeds32_.size() && out; ++i) {
        out &= get(hash(k, seeds32_[i]));
    }
    return out;
}

/*
    To execute a range query, we shift the query bounds to the
    specified prefix length and do point queries for all the
    values in the shifted query range.
*/
bool PrefixBF::Query(const uint64_t from, const uint64_t to) {
    uint64_t iterator = from >> (64 - prefix_len_);
    uint64_t upper_bound = (to - 1) >> (64 - prefix_len_);
    while (iterator <= upper_bound) {
        if (Query(iterator, false)) {
            return true;
        }
        iterator++;
    }
    return false;
}

bool PrefixBF::Query(const std::string& key) {
    bool out = true;
    uint32_t prefix_byte_len = div8(prefix_len_ + 7);

    std::string padded_key = key;
    if (key.length() < prefix_byte_len) {
        padded_key.resize(prefix_byte_len, '\0');
    }

    if (mod8(prefix_len_) == 0) {
        for (size_t i = 0; i < seeds128_.size() /* # of hash functions */ && out; ++i) {
            out &= get(clhash(seeds128_[i], padded_key.data(), prefix_byte_len) % nmod_);
        }
    } else {
        std::string edited_key = editKey(padded_key, prefix_len_, true);
        for (size_t i = 0; i < seeds128_.size() /* # of hash functions */ && out; ++i) {
            out &= get(clhash(seeds128_[i], edited_key.data(), prefix_byte_len) % nmod_);
        }
    }

    return out;
}

/* 
    For string range queries, we choose to make both `from` and `to` inclusive in order to
    avoid generating (to - 1). When integrated in RocksDB, this may incur an additional prefix
    query in some (rare) situations. We also pre-calculate the number of prefix queries to 
    avoid multiple expensive string comparisons. Similar to the integer range query, we create
    a query iterator and increment it successively.
*/
bool PrefixBF::Query(const std::string& from, 
                     const std::string& to) {

    uint32_t prefix_byte_len = div8(prefix_len_ + 7);
    uint32_t shift_bits = mod8(8 - mod8(prefix_len_));

    // Pad `to` with zeroes because a shorter string is 
    // lexicographically smaller than longer strings
    // that share the same prefix
    std::string padded_from = editKey(from, prefix_len_, true);
    std::string padded_to = editKey(to, prefix_len_, true);

    // count_prefixes returns 0 if the result is bigger than a uint64_t - treat as guaranteed false positive
    uint64_t total_queries = count_prefixes(padded_from, padded_to, prefix_len_);
    if (total_queries == 0) {
        return true;
    }

    bool carry = false;
    uint32_t idx = 0;
    uint8_t shifted_last_char = 0;
    std::string& iterator = padded_from;

    for (uint64_t i = 0; i < total_queries; i++) {
        if (Query(iterator)) {
            return true;
        }

        idx = prefix_byte_len - 1;

        // prefix length may fall within a byte so we shift the byte accordingly
        shifted_last_char = static_cast<uint8_t>(iterator[idx]) >> shift_bits;
        if (shifted_last_char == ((MAX_UINT8) >> shift_bits)) {
            carry = true;
            iterator[idx] = 0;
        } else {
            carry = false;
            iterator[idx] = (shifted_last_char + 1) << shift_bits;
        }

        // increment prior bytes if there is a carry
        while (carry && idx > 0) {
            idx -= 1;
            if (static_cast<uint8_t>(iterator[idx]) == MAX_UINT8) {
                iterator[idx] = 0;
            } else {
                iterator[idx] = static_cast<uint8_t>(iterator[idx]) + 1;
                carry = false;
            }
        }
    }
    
    return false;
}

std::pair<uint8_t*, uint64_t> PrefixBF::serialize() const {
    // Size (bytes) of the serialized Prefix BF.
    uint64_t serlen = sizeof(uint32_t) /* prefix_len_ */
                      + sizeof(uint64_t) /* nmod_ */ 
                      + sizeof(size_t) /* size of seeds32_ */ + seeds32_.size() * sizeof(uint32_t) /* actual seeds32_ vector */
                      + sizeof(size_t) /* size of seeds64_ */+ seeds64_.size() * sizeof(std::pair<uint64_t, uint64_t>) /* actual seeds64_ vector */
                      + div8(nmod_);

    uint8_t *out = new uint8_t[serlen];
    uint8_t *pos = out;

    memcpy(pos, &prefix_len_, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    
    memcpy(pos, &nmod_, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    // seeds array for integer prefixbf
    size_t seeds_sz = seeds32_.size();
    memcpy(pos, &seeds_sz, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(pos, seeds32_.data(), seeds32_.size() * sizeof(uint32_t));
    pos += seeds32_.size() * sizeof(uint32_t);
    
    // seeds array for string prefixbf
    size_t seeds64_sz = seeds64_.size();
    memcpy(pos, &seeds64_sz, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(pos, seeds64_.data(), seeds64_.size() * sizeof(std::pair<uint64_t, uint64_t>));
    pos += seeds64_.size() * sizeof(std::pair<uint64_t, uint64_t>);
    
    memcpy(pos, data_, div8(nmod_));
    return {out, serlen};
}

std::pair<PrefixBF*, uint64_t> PrefixBF::deserialize(uint8_t* ser) {
    uint8_t* pos = ser;

    uint32_t prefix_len;    
    memcpy(&prefix_len, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    uint64_t nmod;
    memcpy(&nmod, pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    size_t seeds32_sz;
    memcpy(&seeds32_sz, pos, sizeof(size_t));
    pos += sizeof(size_t);

    uint32_t* pos32 = reinterpret_cast<uint32_t*>(pos);
    std::vector<uint32_t> seeds32(pos32, pos32 + seeds32_sz);
    pos += seeds32_sz * sizeof(uint32_t);
    
    size_t seeds64_sz;
    memcpy(&seeds64_sz, pos, sizeof(size_t));
    pos += sizeof(size_t);

    std::pair<uint64_t, uint64_t>* pos64 = reinterpret_cast<std::pair<uint64_t, uint64_t>*>(pos);
    std::vector<std::pair<uint64_t, uint64_t>> seeds64(pos64, pos64 + seeds64_sz);
    pos += seeds64_sz * sizeof(std::pair<uint64_t, uint64_t>);

    size_t len = pos - ser + div8(nmod);
    return {new PrefixBF(prefix_len, pos, seeds32, seeds64, nmod), len};
}

} // namespace hybrid
