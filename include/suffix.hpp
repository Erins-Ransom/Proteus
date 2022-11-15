#pragma once
#ifndef PROTEUS_SUFFIX_HPP
#define PROTEUS_SUFFIX_HPP

#include <assert.h>
#include <cstring>
#include <vector>

#include "bitvector.hpp"
#include "config.hpp"

namespace proteus {

// Max suffix_len_ = 64 bits
// For kReal suffixes, if the stored key is not long enough to provide
// suffix_len_ suffix bits, its suffix field is cleared (i.e., all 0's)
// to indicate that there is no suffix info associated with the key.
class BitvectorSuffix : public Bitvector {
private:
    level_t start_level_;
    std::vector<position_t> num_suffixes_per_level_;
public:
    BitvectorSuffix() {}

    BitvectorSuffix(const std::vector<std::vector<word_t> >& bitvector_per_level,
                    const std::vector<position_t>& num_bits_per_level,
                    const std::vector<position_t> num_suffixes_per_level,
                    const level_t start_level,
                    level_t end_level /* non-inclusive */)
        : Bitvector(bitvector_per_level, num_bits_per_level, start_level, end_level),
          start_level_(start_level), num_suffixes_per_level_(num_suffixes_per_level) {}

    static word_t constructSuffix(const std::string& key,
                                  const level_t level, 
                                  const level_t len) {
        
        // In rare cases, some Proteus configurations may require
        // keys to be extended using suffixes longer than 64 bits.
        // e.g. deep trie, highly clustered keys with singular outliers
        assert(len <= 64);

        // the length of the queried key should be >= the length of the stored key
        // since we extend the queried key to the trie depth.
        assert(!(key.length() < level || ((key.length() - level) * 8) < len));
            
        word_t suffix = 0;
        level_t num_complete_bytes = len / 8;
        if (num_complete_bytes > 0) {
            suffix += (word_t)(label_t)key[level];
            for (position_t i = 1; i < num_complete_bytes; i++) {
                suffix <<= 8;
                suffix += (word_t)(uint8_t)key[level + i];
            }
        }
        level_t offset = len % 8;
        if (offset > 0) {
            suffix <<= offset;
            word_t remaining_bits = 0;
            remaining_bits = (word_t)(uint8_t)key[level + num_complete_bytes];
            remaining_bits >>= (8 - offset);
            suffix += remaining_bits;
        }
        return suffix;
    }

    // Determines the start position of the desired suffix within the suffix bitvector
    position_t calcBitPos(const position_t idx,
                          const level_t level,
                          const uint32_t trie_depth) const {
                              
        position_t bit_pos = 0;
        level_t num_suffixes = 0; 

        for (level_t l = start_level_; l < (level - 1); l++) {
            bit_pos += num_suffixes_per_level_[l] * getSuffixLen(l + 1, trie_depth);
            num_suffixes += num_suffixes_per_level_[l];
        }
        bit_pos += (idx - num_suffixes) * getSuffixLen((level - 1) + 1, trie_depth);

        return bit_pos;
    }

    level_t getSuffixLen(const level_t level, 
                         const uint32_t trie_depth) const {
        // Returns the suffix length for a node terminating at the supplied level (which is 0-indexed).
        // A node terminating at level 1 has 1 key byte, level 2 has 2 key bytes, etc.
        return (level * 8 < trie_depth) ? trie_depth - (level * 8) : 0;                
    }
    
    position_t serializedSize() const {
        position_t size = sizeof(num_bits_) + bitsSize() +
                          sizeof(start_level_) + sizeof(position_t) + sizeof(position_t) * num_suffixes_per_level_.size();
        sizeAlign(size);
        return size;
    }

    position_t size() const {
	    return (sizeof(BitvectorSuffix) + bitsSize());
    }

    word_t read(const position_t bit_pos, const level_t level, const uint32_t trie_depth) const;
    bool checkEquality(const position_t idx, const std::string& key, const level_t level, const uint32_t trie_depth) const;

    // Compare stored suffix to querying suffix.
    int compare(const position_t idx, const std::string& key, const level_t level, const uint32_t trie_depth) const;

    void serialize(char*& dst) const {
        memcpy(dst, &start_level_, sizeof(start_level_));
        dst += sizeof(start_level_);
        position_t nlevels = static_cast<position_t>(num_suffixes_per_level_.size());
        memcpy(dst, &nlevels, sizeof(nlevels));
        dst += sizeof(nlevels);
        memcpy(dst, num_suffixes_per_level_.data(), sizeof(position_t) * nlevels);
        dst += sizeof(position_t) * nlevels;
        memcpy(dst, &num_bits_, sizeof(num_bits_));
        dst += sizeof(num_bits_);
        memcpy(dst, bits_, bitsSize());
        dst += bitsSize();
	    align(dst);
    }

    static BitvectorSuffix* deSerialize(char*& src) {
        BitvectorSuffix* sv = new BitvectorSuffix();
        level_t start_level = 0;
        memcpy(&start_level, src, sizeof(start_level));
        sv->start_level_ = start_level;
        src += sizeof(start_level);
        
        position_t nlevels = 0;
        memcpy(&nlevels, src, sizeof(nlevels));
        src += sizeof(nlevels);
        sv->num_suffixes_per_level_.resize(nlevels);
        memcpy(sv->num_suffixes_per_level_.data(), src, nlevels * sizeof(position_t));
        src += sizeof(position_t) * nlevels;
       
        memcpy(&(sv->num_bits_), src, sizeof(sv->num_bits_));
        src += sizeof(sv->num_bits_);

        sv->bits_ = new word_t[sv->numWords()];
        memcpy(sv->bits_, src, sv->bitsSize());
        src += sv->bitsSize();
        
        align(src);
        return sv;
    }

    void destroy() {
        if (bits_)
            delete[] bits_;
    }
};

word_t BitvectorSuffix::read(const position_t bit_pos,
                             const level_t level,
                             const uint32_t trie_depth) const {

    level_t suffix_len = getSuffixLen(level, trie_depth);
    if (bit_pos >= num_bits_)
	    return 0;

    position_t word_id = bit_pos / kWordSize;
    position_t offset = bit_pos & (kWordSize - 1);
    word_t ret_word = (bits_[word_id] << offset) >> (kWordSize - suffix_len);
    if (offset + suffix_len > kWordSize)
	    ret_word += (bits_[word_id+1] >> (kWordSize - offset - suffix_len));
    return ret_word;
}

bool BitvectorSuffix::checkEquality(const position_t idx,
				                    const std::string& key, 
                                    const level_t level,
                                    const uint32_t trie_depth) const {

    position_t bit_pos = calcBitPos(idx, level, trie_depth);                                    
    level_t suffix_len = getSuffixLen(level, trie_depth);
    
    /*
        Since we store fixed length (padded) prefixes in the trie, an invalid suffix means
        that we have reached the trie depth and all key bytes have matched up to this point.
        Hence, we return true so the point query may continue in the prefix Bloom filter.
    */
    if (bit_pos >= num_bits_) { return true; }

    word_t stored_suffix = read(bit_pos, level, trie_depth);

    // the length of the queried key should be >= the length of the stored key
    // since we extend the queried key to the trie depth.
    assert(!(key.length() < level || ((key.length() - level) * 8) < suffix_len));
        
    word_t querying_suffix = constructSuffix(key, level, suffix_len);
    return (stored_suffix == querying_suffix);
}

int BitvectorSuffix::compare(const position_t idx,
			                 const std::string& key, 
                             const level_t level,
                             const uint32_t trie_depth) const {

    position_t bit_pos = calcBitPos(idx, level, trie_depth);                             
    level_t suffix_len = getSuffixLen(level, trie_depth);

    if (bit_pos >= num_bits_) { return kCouldBePositive; }

    word_t stored_suffix = read(bit_pos, level, trie_depth);
    word_t querying_suffix = constructSuffix(key, level, suffix_len);

    if (stored_suffix == querying_suffix) {
        return kCouldBePositive;
    } else if (stored_suffix < querying_suffix) {
        return -1;
    } else {
        return 1;
    }
}

} // namespace proteus

#endif