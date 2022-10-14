#pragma once
#ifndef PROTEUS_PREFIXBF_HPP
#define PROTEUS_PREFIXBF_HPP

#include <vector>
#include <memory>
#include <cassert>
#include <inttypes.h>
#include <cstring>
#include <cmath>
#include <random>
#include <map>

#include "MurmurHash3.hpp"
#include "clhash.hpp"
#include "config.hpp"

namespace proteus {

const uint32_t MAX_PBF_HASH_FUNCS = 32;

class PrefixBF {
    private:
        uint32_t prefix_len_;
        uint8_t* data_;
        // Number of hash functions is given by the size of the seed vector 
        // (seeds32_ for uint64 keys, and seeds128_ for string keys)
        std::vector<uint32_t> seeds32_;
        std::vector<std::pair<uint64_t, uint64_t>> seeds64_;
        std::vector<void*> seeds128_;
        uint64_t nmod_;

    public:
        /*
            Hash key prefixes of specified length into the Bloom filter
            Based on the number of unique key prefixes hashed, the optimal
            number of hash functions is calculated and the appropriate number
            of seeds is generated. We set a maximum of 32 hash functions to bound
            filter latency when the number of filter elements is small. This can
            happen if a shorter prefix length is chosen.
        */
        PrefixBF(uint32_t prefix_len, uint64_t nbits, const std::vector<uint64_t>& keys) : 
            prefix_len_(prefix_len), nmod_(std::min(static_cast<uint64_t>(UINT32_MAX), (nbits+7)/8*8)) {  
            // Filter uses 32-bit MurmurHash function so the number of filter bits
            // cannot be more than UINT32_MAX. Such situations are unlikely to happen 
            // in RocksDB as it would entail an extremely high BPK or number of keys.

            assert(nbits > 0);

            // Create zeroed out bit array - parentheses value initializes array to 0
            data_ = new uint8_t[div8(nmod_)]();
            assert(data_);

            std::vector<size_t> uniq_idxs;
            uniq_idxs.emplace_back(0);
            uint64_t prev_key = keys[0] >> (64 - prefix_len_);
            uint64_t current_key = 0;

            for (size_t i = 0; i < keys.size(); i++) {
                current_key = keys[i] >> (64 - prefix_len_);
                if (current_key != prev_key) {
                    uniq_idxs.emplace_back(i);
                    prev_key = current_key;
                }
            }

            uint32_t nhf = static_cast<uint32_t>(round(M_LN2 * nmod_ / uniq_idxs.size()));
            nhf = (nhf == 0 ? 1 : nhf);
            nhf = std::min(MAX_PBF_HASH_FUNCS, nhf);
            
            seeds32_.resize(nhf);
            std::mt19937 gen(1337);
            for (uint32_t i = 0; i < nhf; ++i) {
                seeds32_[i] = gen();
            }

            uint64_t edited_key = 0ULL;
            for (auto const & idx : uniq_idxs) {
                edited_key = keys[idx] >> (64 - prefix_len_);
                for (uint32_t i = 0; i < nhf; ++i) {
                    set(hash(edited_key, seeds32_[i]), 1);
                }
            }
        }

        PrefixBF(uint32_t prefix_len, uint64_t nbits, const std::vector<std::string>& keys) : 
            prefix_len_(prefix_len), nmod_((nbits+7)/8*8) {  

            assert(nbits > 0);

            // Create zeroed out bit array - parentheses value initializes array to 0
            data_ = new uint8_t[(nmod_ / 8)]();
            assert(data_);

            std::vector<size_t> uniq_prefixes;
            uniq_prefixes.emplace_back(0);
            std::string prev_key = keys[0];
            std::string current_key = std::string();

            for (size_t i = 0; i < keys.size(); i++) {
                current_key = keys[i];
                if (compare(current_key, prev_key, prefix_len_) != 0) {
                    uniq_prefixes.emplace_back(i);
                    prev_key = current_key;
                }
            }

            uint32_t nhf = static_cast<uint32_t>(round(M_LN2 * nmod_ / uniq_prefixes.size()));
            nhf = (nhf == 0 ? 1 : nhf);
            nhf = std::min(MAX_PBF_HASH_FUNCS, nhf);
            
            seeds64_.resize(nhf);
            seeds128_.resize(nhf);

            std::mt19937_64 gen(1337);
            for (uint32_t i = 0; i < nhf; ++i) {
                seeds64_[i] = std::make_pair(gen(), gen());
                seeds128_[i] = get_random_key_for_clhash(seeds64_[i].first, seeds64_[i].second);
            }

            uint32_t prefix_byte_len = div8(prefix_len_ + 7);
            if (mod8(prefix_len_) == 0) {
                for (auto const & idx : uniq_prefixes) {
                    for (uint32_t i = 0; i < nhf; ++i) {
                        set(clhash(seeds128_[i], keys[idx].data(), prefix_byte_len) % nmod_, 1);
                    }
                }
            } else {
                std::string edited_key;
                for (auto const & idx : uniq_prefixes) {
                    edited_key = editKey(keys[idx], prefix_len_, true);
                    for (uint32_t i = 0; i < nhf; ++i) {
                        set(clhash(seeds128_[i], edited_key.data(), prefix_byte_len) % nmod_, 1);
                    }
                }
            }
        }

        PrefixBF(uint32_t prefix_len, uint8_t* data, std::vector<uint32_t> seeds32, std::vector<std::pair<uint64_t, uint64_t>> seeds64, uint64_t nmod) : 
            prefix_len_(prefix_len), seeds32_(seeds32), seeds64_(seeds64), nmod_(nmod) {

            // The string hash function (CLHash) generates 128-bit seeds from a pair of 64-bit integers.
            seeds128_.resize(seeds64_.size());
            for (size_t i = 0; i < seeds64_.size(); ++i) {
                seeds128_[i] = get_random_key_for_clhash(seeds64_[i].first, seeds64_[i].second);
            }   

            // Copy over bit array
            data_ = new uint8_t[(nmod_ / 8)];
            assert(data_);
            memcpy(reinterpret_cast<void*>(data_), data, (nmod_/8));
        }

        ~PrefixBF() {
            delete[] data_;
        }

        uint32_t getPrefixLen() const {
            return prefix_len_;
        }

        bool Query(const uint64_t key, bool shift = true);
        bool Query(const uint64_t from, const uint64_t to);

        bool Query(const std::string& key);
        bool Query(const std::string& from, const std::string& to);

        std::pair<uint8_t*, uint64_t> serialize() const;
        static std::pair<PrefixBF*, uint64_t> deserialize(uint8_t* ser);
        
        bool get(uint64_t i) const;
        void set(uint64_t i, bool v);
        
        uint64_t hash(const uint64_t edited_key, const uint32_t& seed) {
            uint32_t h;
            MurmurHash3_x86_32(&edited_key, 8, seed, &h);
            return static_cast<uint64_t>(h) % nmod_;
        }
};

} // namespace proteus
#endif
