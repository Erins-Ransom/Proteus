#include <cstdint>
#include <cstring>
#include <cassert>
#include <tuple>
#include <climits>
#include <algorithm>

#include "config.hpp"

namespace proteus {

/******************************** 
███████╗██╗   ██╗██████╗ ███████╗
██╔════╝██║   ██║██╔══██╗██╔════╝
███████╗██║   ██║██████╔╝█████╗  
╚════██║██║   ██║██╔══██╗██╔══╝  
███████║╚██████╔╝██║  ██║██║     
╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚═╝   
********************************/

void align(char*& ptr) {
    ptr = (char*)(((uint64_t)ptr + 7) & ~((uint64_t)7));
}

void sizeAlign(position_t& size) {
    size = (size + 7) & ~((position_t)7);
}

void sizeAlign(uint64_t& size) {
    size = (size + 7) & ~((uint64_t)7);
}

std::string uint64ToString(const uint64_t word) {
    uint64_t endian_swapped_word = __builtin_bswap64(word);
    return std::string(reinterpret_cast<const char*>(&endian_swapped_word), 8);
}

uint64_t stringToUint64(const std::string& str_word) {
    uint64_t int_word = 0;
    memcpy(reinterpret_cast<char*>(&int_word), str_word.data(), 8);
    return __builtin_bswap64(int_word);
}

/**********************************************************
██████╗ ██████╗  ██████╗ ████████╗███████╗██╗   ██╗███████╗
██╔══██╗██╔══██╗██╔═══██╗╚══██╔══╝██╔════╝██║   ██║██╔════╝
██████╔╝██████╔╝██║   ██║   ██║   █████╗  ██║   ██║███████╗
██╔═══╝ ██╔══██╗██║   ██║   ██║   ██╔══╝  ██║   ██║╚════██║
██║     ██║  ██║╚██████╔╝   ██║   ███████╗╚██████╔╝███████║
╚═╝     ╚═╝  ╚═╝ ╚═════╝    ╚═╝   ╚══════╝ ╚═════╝ ╚══════╝
**********************************************************/

std::string stringify(const uint64_t word) {
    return uint64ToString(word);
}

std::string stringify(const std::string& word) {
    return word;
}

uint64_t integerify(const uint64_t word) {
    return word;
}

uint64_t integerify(const std::string& word) {
    return stringToUint64(word);
}


std::string uint8ToString(const uint8_t word) {
    return std::string(reinterpret_cast<const char*>(&word), 1);
}

// Returns the key truncated or extended to the specified prefix length; pads with either 1s or 0s
std::string editKey(const std::string& key, const uint32_t prefix_bit_len, bool zero) {
    const uint32_t bit_remainder = mod8(prefix_bit_len);
    const uint32_t prefix_byte_len = div8(prefix_bit_len + 7);
    std::string edited_key;
    if (zero) {
        edited_key.resize(prefix_byte_len, '\0');
    } else {
        edited_key.resize(prefix_byte_len, UCHAR_MAX);
    }

    std::copy_n(key.begin(), std::min(prefix_byte_len, static_cast<uint32_t>(key.length())), edited_key.begin());
    if (prefix_byte_len <= key.length() && bit_remainder != 0) {
        edited_key[prefix_byte_len - 1] = zero ? edited_key[prefix_byte_len - 1] & bitCutoffMasks[bit_remainder]
                                               : edited_key[prefix_byte_len - 1] | invertedBitCutoffMasks[bit_remainder];                                                         
    }

    return edited_key;
}

uint64_t editKey(const uint64_t key, const uint32_t prefix_bit_len, bool zero) {
    uint64_t zeroed = (key >> (64 - prefix_bit_len)) << (64 - prefix_bit_len);
    return zero ? zeroed : zeroed | ((__UINT64_C(1) << (64 - prefix_bit_len)) - 1);
}

std::string editAndStringify(const uint64_t key, const uint32_t prefix_length, bool zero) {
    return uint64ToString(editKey(key, prefix_length, zero));
}

std::string editAndStringify(const std::string& key, const uint32_t prefix_length, bool zero) {
    return editKey(key, prefix_length, zero);
}

int compare(const std::string& a, const std::string& b, const uint32_t prefix_bit_len) {
    std::string shorter = a.length() < b.length() ? a : b;
    std::string longer = a.length() > b.length() ? a : b;
    bool isSameLength = a.length() == b.length();
    bool aIsLonger = a.length() > b.length();
    const uint32_t prefix_byte_len = div8(prefix_bit_len + 7);

    if (shorter.length() >= prefix_byte_len) {
        int r = memcmp(a.data(), b.data(), prefix_byte_len - 1);
        if (r == 0) {
            size_t bit_remainder = mod8(prefix_bit_len);
            uint8_t u1 = (bit_remainder == 0) ?  static_cast<uint8_t>(a[prefix_byte_len - 1])
                                              : (static_cast<uint8_t>(a[prefix_byte_len - 1]) & bitCutoffMasks[bit_remainder]);
            uint8_t u2 = (bit_remainder == 0) ?  static_cast<uint8_t>(b[prefix_byte_len - 1])
                                              : (static_cast<uint8_t>(b[prefix_byte_len - 1]) & bitCutoffMasks[bit_remainder]);
            
            if (u1 < u2) {
                r = -1;
            } else if (u1 > u2) {
                r = +1;
            }
        }

        return r;

    } else {
        int r = memcmp(a.data(), b.data(), shorter.length());
        if (r == 0 && !isSameLength) {
            if (longer[shorter.length()] == '\0' &&
                !memcmp(longer.data(), 
                        longer.data() + 1,
                        std::min(longer.length() - shorter.length() - 1,  
                                 prefix_byte_len - shorter.length() - 1))) {
                
                r = 0;
            } else if (aIsLonger) {
                r = +1;
            } else {
                r = -1;
            }
        }
    
        return r;
    }
}

int longestCommonPrefix(const uint64_t a, const uint64_t b, const size_t max_klen) {
    (void) max_klen;
    uint64_t xored = a ^ b;
    return xored == 0 ? 64 : __builtin_clzll(xored);
}

static uint8_t clzlut[256] = {
    8,7,6,6,5,5,5,5,
    4,4,4,4,4,4,4,4,
    3,3,3,3,3,3,3,3,
    3,3,3,3,3,3,3,3,
    2,2,2,2,2,2,2,2,
    2,2,2,2,2,2,2,2,
    2,2,2,2,2,2,2,2,
    2,2,2,2,2,2,2,2,
    1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0
};

int longestCommonPrefix(const std::string& a, const std::string& b, const size_t max_klen) {
    std::string shorter = a.length() < b.length() ? a : b;
    std::string longer = a.length() > b.length() ? a : b;

    int lcp = 0;
    for (size_t i = 0; i < shorter.length(); i++) {
        if (a[i] != b[i]) {
            uint8_t xored = ((uint8_t) a[i]) ^ ((uint8_t) b[i]);
            return (lcp * 8) + clzlut[xored];
        }
        lcp++;
    }
    for (size_t i = shorter.length(); i < longer.length(); i++) {
        if (longer[i] != '\0') {
            uint8_t xored = ((uint8_t) a[i]) ^ ((uint8_t) b[i]);
            return (lcp * 8) + clzlut[xored];
        }
        lcp++;
    }
    
    /* 
        We can only get here if: 
            1) the keys are the same OR
            2) the additional bytes of the longer key are null bytes (since we pad with null bytes)
        
        Therefore, we implicitly pad the LCP returned to the max key length.
    */
    return max_klen;
}

/*
    The number of prefixes can be easily calculated by bitshifting the 
    query bounds to the appropriate bit length and taking the difference.
    Note that for uint64 queries, `to` is exclusive.
*/
uint64_t count_prefixes(const uint64_t from, const uint64_t to, const uint32_t prefix_bit_len) {
    return ((to - 1) >> (64 - prefix_bit_len)) - (from >> (64 - prefix_bit_len)) + 1;
}

/*
    - Counts the number of prefixes of specified length overlapping the query range.
    - Note that `from` and `to` are inclusive for string queries. 
    - We calculate the difference between the truncated prefixes of the query bounds 
      by iterating through the bytes and accumulating the difference in byte values. 
    - If the prefix length is not byte-aligned, the last byte is shifted accordingly.
    - We increment by 1 to account for the inclusivity of `to`.

    Returns 0 if the number of query prefixes overflows a uint64_t
*/

#define TOTAL_MUL(shift)    if (total > (UINT64_MAX >> (shift))) { \
                                return 0; \
                            } \
                            total <<= (shift);

#define TOTAL_ADD_DIFF(a, b)    if (total > (UINT64_MAX - (b))) { \
                                    return 0; \
                                } \
                                total = (total + (b)) - (a);

uint64_t count_prefixes(const std::string& a, const std::string& b, const uint32_t prefix_bit_len) {
    uint64_t total = 0;
    std::string shorter = a.length() < b.length() ? a : b;
    std::string longer = a.length() > b.length() ? a : b;
    bool aIsLonger = a.length() > b.length();
    const size_t prefix_byte_len = static_cast<size_t>(div8(prefix_bit_len + 7));

    if (shorter.length() >= prefix_byte_len) {
        if (mod8(prefix_bit_len) == 0) {
            for (uint32_t i = 0; i < prefix_byte_len; i++) {
                TOTAL_MUL(8)
                TOTAL_ADD_DIFF(static_cast<uint8_t>(a[i]), static_cast<uint8_t>(b[i]))
            }
            total++;
        } else {
            for (uint32_t i = 0; i < (prefix_byte_len - 1); i++) {
                TOTAL_MUL(8)
                TOTAL_ADD_DIFF(static_cast<uint8_t>(a[i]), static_cast<uint8_t>(b[i]))
            }

            // WARNING: Bit shifting should be done on CASTED uint8_t bytes
            // The max value of the last byte depends on the shifting
            uint32_t shift_bits = mod8(8 - mod8(prefix_bit_len));
            TOTAL_MUL(8 - shift_bits)
            TOTAL_ADD_DIFF(static_cast<uint8_t>(a[prefix_byte_len - 1]) >> shift_bits, 
                           static_cast<uint8_t>(b[prefix_byte_len - 1]) >> shift_bits)

            total++;
        }
    } else {
        for (uint32_t i = 0; i < shorter.length(); i++) {
            TOTAL_MUL(8)
            TOTAL_ADD_DIFF(static_cast<uint8_t>(a[i]), static_cast<uint8_t>(b[i]))
        }
        
        if (aIsLonger) {
            for (uint32_t i = shorter.length(); i < std::min(prefix_byte_len - 1, longer.length()); i++) {
                TOTAL_MUL(8)
                TOTAL_ADD_DIFF(static_cast<uint8_t>(a[i]), static_cast<uint8_t>(0))
            }
        } else {
            for (uint32_t i = shorter.length(); i < std::min(prefix_byte_len - 1, longer.length()); i++) {
                TOTAL_MUL(8)
                TOTAL_ADD_DIFF(static_cast<uint8_t>(0), static_cast<uint8_t>(b[i]))
            }
        }

        if (prefix_byte_len > longer.length()) {
            TOTAL_MUL(prefix_bit_len - longer.length() * 8)
        } else {
            uint32_t shift_bits = mod8(8 - mod8(prefix_bit_len));
            TOTAL_MUL(8 - shift_bits)
            TOTAL_ADD_DIFF(static_cast<uint8_t>(a[prefix_byte_len - 1]) >> shift_bits, 
                           static_cast<uint8_t>(b[prefix_byte_len - 1]) >> shift_bits)
        }
    }

    return total;
}

}