#include <unistd.h>
#include <stdint.h>
#include <assert.h> 
#include <cstring>
#include <vector>
#include <set>
#include <fstream>
#include <set>
#include <random>
#include <algorithm>

#include "util.hpp"

namespace proteus {

// For RocksDB
uint64_t sliceToUint64(const char* data) {
    uint64_t out = 0ULL;
    std::memcpy(&out, data, 8);
    return __builtin_bswap64(out);
}


std::string util_uint64ToString(const uint64_t& word) {
    uint64_t endian_swapped_word = __builtin_bswap64(word);
    return std::string(reinterpret_cast<const char*>(&endian_swapped_word), 8);
}

void intLoadKeys(std::string keyFilePath,
                 std::vector<uint64_t>& keys,
                 std::vector<std::string>& skeys,
                 std::set<uint64_t>& keyset) {
    
    std::ifstream keyFile;
    uint64_t key;

    keyFile.open(keyFilePath);
    while (keyFile >> key) {
        keyset.insert(key);
        keys.push_back(key);        
    }
    keyFile.close();

    std::sort(keys.begin(), keys.end());
    for (const auto & k : keys) {
        skeys.push_back(util_uint64ToString(k));
    }
}


void intLoadQueries(std::string lQueryFilePath, 
                    std::string uQueryFilePath,
                    std::vector<std::pair<uint64_t, uint64_t>>& range_queries,
                    std::vector<std::pair<std::string, std::string>>& squeries) {
    
    std::ifstream lQueryFile, uQueryFile;    
    uint64_t lq, uq;

    lQueryFile.open(lQueryFilePath);
    uQueryFile.open(uQueryFilePath);
    while((lQueryFile >> lq) && (uQueryFile >> uq)) {
        assert(lq <= uq);
        range_queries.push_back(std::make_pair(lq, uq));
    }
    lQueryFile.close();
    uQueryFile.close();

    std::sort(range_queries.begin(), range_queries.end());
    for (const auto & q : range_queries) {
        squeries.push_back(std::make_pair(util_uint64ToString(q.first), util_uint64ToString(q.second)));
    }
}


size_t strLoadKeys(std::string keyFilePath,
                   std::vector<std::string>& skeys,
                   std::set<std::string>& keyset) {
    
    std::ifstream keyFile(keyFilePath, std::ifstream::binary);
    uint32_t sz;
    keyFile.read(reinterpret_cast<char*>(&sz), sizeof(uint32_t));

    char* k_arr = new char[sz];
    while (keyFile.peek() != EOF) {
        keyFile.read(k_arr, sz);
        keyset.insert(std::string(k_arr, sz));
        skeys.push_back(std::string(k_arr, sz)); 
    }

    keyFile.close();

    std::sort(skeys.begin(), skeys.end());
    delete[] k_arr;

    return static_cast<size_t>(sz);
}

void strLoadQueries(std::string lQueryFilePath, 
                    std::string rQueryFilePath,
                    std::vector<std::pair<std::string, std::string>>& squeries) {
    
    std::ifstream lQueryFile(lQueryFilePath, std::ifstream::binary);
    std::ifstream rQueryFile(rQueryFilePath, std::ifstream::binary);
    
    uint32_t lsz, rsz;
    lQueryFile.read(reinterpret_cast<char*>(&lsz), sizeof(uint32_t));
    rQueryFile.read(reinterpret_cast<char*>(&rsz), sizeof(uint32_t));
    assert(lsz == rsz);

    std::string lq, rq;
    char* lq_arr = new char[lsz];
    char* rq_arr = new char[rsz];

    while (lQueryFile.peek() != EOF && rQueryFile.peek() != EOF) {
        lQueryFile.read(lq_arr, lsz);
        rQueryFile.read(rq_arr, rsz);
        lq = std::string(lq_arr, lsz);
        rq = std::string(rq_arr, rsz);
        assert(lq <= rq);
        squeries.push_back(std::make_pair(lq, rq));
    }

    lQueryFile.close();
    rQueryFile.close();

    std::sort(squeries.begin(), squeries.end());
    delete[] lq_arr;
    delete[] rq_arr;
}

}