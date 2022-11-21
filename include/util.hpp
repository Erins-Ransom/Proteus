#ifndef PROTEUS_UTIL_HPP
#define PROTEUS_UTIL_HPP
#pragma once

#include <stdint.h>
#include <set>
#include <vector>
#include <mutex>

namespace proteus {

template <class T>
class FIFOSampleQueryCache {
    private:
        std::vector<std::pair<T, T>> sample_queries_;
        std::mutex mut_;
        size_t pos_; // position in the array for the next insert
        size_t sample_rate_;
        size_t counter_;

    public:
        explicit FIFOSampleQueryCache(std::vector<std::pair<T, T>> initial_sample, size_t sample_rate) :
                                      pos_(0), sample_rate_(sample_rate), counter_(0) {
            sample_queries_ = initial_sample;
        }
        
        void add(const std::pair<T, T> sq) {
            // Add every {sample_rate_}-th query
            std::lock_guard<std::mutex> guard(mut_);
            counter_ = (counter_ == (sample_rate_ - 1)) ? 0 : (counter_ + 1);
            if (counter_ == 0) {
                sample_queries_[pos_] = sq;
                pos_ = (pos_ == (sample_queries_.size() - 1)) ? 0 : (pos_ + 1);
            }
        }

        std::vector<std::pair<T, T>> getSampleQueries() {
            std::lock_guard<std::mutex> guard(mut_);
            return sample_queries_;
        }
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
                   std::set<std::string>& keyset,
                   const size_t nkeys);

void strLoadQueries(std::string lQueryFilePath, 
                    std::string rQueryFilePath,
                    std::vector<std::pair<std::string, std::string>>& squeries,
                    const size_t nqueries);

template<typename T>
std::vector<std::pair<T, T>> sampleQueries(const std::vector<std::pair<T, T>>& queries, double sample_rate) {
    std::vector<std::pair<T, T>> sample_queries;
    std::default_random_engine generator;
    std::bernoulli_distribution distribution(sample_rate);
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