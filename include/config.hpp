#pragma once
#ifndef PROTEUS_CONFIG_HPP
#define PROTEUS_CONFIG_HPP

#include <cstdint>
#include <string>

namespace proteus {

/******************************** 
███████╗██╗   ██╗██████╗ ███████╗
██╔════╝██║   ██║██╔══██╗██╔════╝
███████╗██║   ██║██████╔╝█████╗  
╚════██║██║   ██║██╔══██╗██╔══╝  
███████║╚██████╔╝██║  ██║██║     
╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚═╝   
********************************/

using level_t = uint32_t;
using position_t = uint32_t;
static const position_t kMaxPos = UINT32_MAX;

using label_t = uint8_t;
static const position_t kFanout = 256;

using word_t = uint64_t;
static const unsigned kWordSize = 64;
static const word_t kMsbMask = 0x8000000000000000;
static const word_t kOneMask = 0xFFFFFFFFFFFFFFFF;

static const bool kIncludeDense = true;

static const int kHashShift = 7;

static const int kCouldBePositive = 2018; // used in suffix comparison


void align(char*& ptr);
void sizeAlign(position_t& size);
void sizeAlign(uint64_t& size);
std::string uint64ToString(const uint64_t word);
uint64_t stringToUint64(const std::string& str_word);

/**********************************************************
██████╗ ██████╗  ██████╗ ████████╗███████╗██╗   ██╗███████╗
██╔══██╗██╔══██╗██╔═══██╗╚══██╔══╝██╔════╝██║   ██║██╔════╝
██████╔╝██████╔╝██║   ██║   ██║   █████╗  ██║   ██║███████╗
██╔═══╝ ██╔══██╗██║   ██║   ██║   ██╔══╝  ██║   ██║╚════██║
██║     ██║  ██║╚██████╔╝   ██║   ███████╗╚██████╔╝███████║
╚═╝     ╚═╝  ╚═╝ ╚═════╝    ╚═╝   ╚══════╝ ╚═════╝ ╚══════╝
**********************************************************/

static const uint8_t MAX_UINT8 = UINT8_MAX;
static const uint8_t bitCutoffMasks[8] = {0b0, 0b10000000, 0b11000000, 0b11100000, 0b11110000, 0b11111000, 0b11111100, 0b11111110};
static const uint8_t invertedBitCutoffMasks[8] = {0b0, 0b01111111, 0b00111111, 0b00011111, 0b00001111, 0b00000111, 0b00000011, 0b00000001};

/*
    ╔╦╗┬┌┐┌┌─┐┬─┐  ╔═╗┌─┐┌┬┐┬┌┬┐┬┌─┐┌─┐┌┬┐┬┌─┐┌┐┌ .
    ║║║│││││ │├┬┘  ║ ║├─┘ │ │││││┌─┘├─┤ │ ││ ││││ .
    ╩ ╩┴┘└┘└─┘┴└─  ╚═╝┴   ┴ ┴┴ ┴┴└─┘┴ ┴ ┴ ┴└─┘┘└┘

    - Modulo 8 => AND 7
    - Divide by 8 => Right Shift 3
*/

inline uint32_t mod8(uint32_t a) {
    return a & 7U;
}

inline uint64_t mod8(uint64_t a) {
    return a & __UINT64_C(7);
}

inline uint32_t div8(uint32_t a) {
    return a >> 3;
}

inline uint64_t div8(uint64_t a) {
    return a >> 3;
}

std::string stringify(const uint64_t word);
std::string stringify(const std::string& word);

uint64_t integerify(const uint64_t word);
uint64_t integerify(const std::string& word);

std::string uint8ToString(const uint8_t word);

int compare(const std::string& a, const std::string& b, const uint32_t prefix_bit_len);

uint64_t editKey(const uint64_t key, const uint32_t prefix_bit_len, bool zero);
std::string editKey(const std::string& key, const uint32_t prefix_bit_len, bool zero);

std::string editAndStringify(const uint64_t key, const uint32_t prefix_length, bool zero);
std::string editAndStringify(const std::string& key, const uint32_t prefix_length, bool zero);

int longestCommonPrefix(const uint64_t a, const uint64_t b, const size_t max_klen);
int longestCommonPrefix(const std::string& a, const std::string& b, const size_t max_klen);

uint64_t count_prefixes(const uint64_t from, const uint64_t to, const uint32_t prefix_bit_len);
uint64_t count_prefixes(const std::string& from, const std::string& to, const uint32_t prefix_bit_len);

} // namespace proteus

#endif