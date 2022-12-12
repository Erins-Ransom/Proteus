#include <unistd.h>
#include <random>
#include <iostream>
#include <fstream>
#include <experimental/filesystem> // C++14 compatible

#include "rocksdb/db.h"

static const size_t VAL_SZ = 512;

inline bool isPointQuery(const uint64_t& a, const uint64_t& b) {
    return b == (a + 1);
}

inline bool isPointQuery(const std::string& a, const std::string& b) {
    return b == a;
}

std::pair<std::vector<std::vector<std::string>>, std::vector<std::vector<rocksdb::Slice>>> intLoadKeysValues();
std::pair<std::vector<std::vector<std::string>>, std::vector<std::vector<rocksdb::Slice>>> strLoadKeysValues();
std::vector<std::vector<std::pair<std::string, std::string>>> intLoadQueries();
std::vector<std::vector<std::pair<std::string, std::string>>> strLoadQueries();
std::vector<std::vector<bool>> loadTrace();

const std::vector<std::pair<uint64_t, uint64_t>> intSampleInitialQueries(const std::vector<std::pair<std::string, std::string>>& queries, 
                                                                         size_t sample_cache_size);
const std::vector<std::pair<std::string, std::string>> strSampleInitialQueries(const std::vector<std::pair<std::string, std::string>>& queries, 
                                                                               size_t sample_cache_size);

void printCompactionAndDBStats(rocksdb::DB* db);
void printLSM(rocksdb::DB* db);
void flushMemTable(rocksdb::DB* db);
void waitForBGCompactions(rocksdb::DB* db);
void printFPR(rocksdb::Options* options, std::ofstream& stream);
void printStats(rocksdb::DB* db,
                rocksdb::Options* options,
                std::ofstream& stream);
