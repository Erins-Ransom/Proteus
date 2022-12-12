#ifndef PROTEUS_UTIL_HPP
#define PROTEUS_UTIL_HPP
#pragma once

#include <stdint.h>
#include <set>
#include <vector>
#include <mutex>
#include <random>

namespace proteus {

template <class T>
class FIFOSampleQueryCache {
    private:
        std::vector<std::pair<T, T>> sample_queries_;
        std::mutex mut_;
        size_t pos_; // position in the array for the next insert
        size_t sample_freq_;
        size_t counter_;

    public:
        explicit FIFOSampleQueryCache(std::vector<std::pair<T, T>> initial_sample, size_t sample_frequency) :
                                      pos_(0), sample_freq_(sample_frequency), counter_(0) {
            sample_queries_ = initial_sample;
        }

        std::vector<std::pair<T, T>> getSampleQueries() {
            std::lock_guard<std::mutex> guard(mut_);
            return sample_queries_;
        }
        
        void add(const std::pair<std::string, std::string> q);
};

uint64_t sliceToUint64(const char* data);

void intLoadKeys(std::string keyFilePath,
                 std::vector<uint64_t>& keys,
                 std::vector<std::string>& skeys,
                 std::set<uint64_t>& keyset);

void intLoadQueries(std::string lQueryFilePath, 
                    std::string uQueryFilePath,
                    std::vector<std::pair<uint64_t, uint64_t>>& range_queries,
                    std::vector<std::pair<std::string, std::string>>& squeries);

size_t strLoadKeys(std::string keyFilePath,
                   std::vector<std::string>& skeys,
                   std::set<std::string>& keyset);

void strLoadQueries(std::string lQueryFilePath, 
                    std::string rQueryFilePath,
                    std::vector<std::pair<std::string, std::string>>& squeries);

template<typename T>
std::vector<std::pair<T, T>> sampleQueries(const std::vector<std::pair<T, T>>& queries, double sample_proportion) {
    std::vector<std::pair<T, T>> sample_queries;
    std::default_random_engine generator;
    std::bernoulli_distribution distribution(sample_proportion);
    for (auto const & q : queries) {
        if (distribution(generator)) {
            sample_queries.push_back(q);
        }
    }
    printf("Percent Sample Queries: %lf\n", (sample_queries.size() * 1.0 / queries.size()) * 100.0);
    return sample_queries;
}

}

#endif