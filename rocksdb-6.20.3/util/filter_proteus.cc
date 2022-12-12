#include <iostream>
#include <algorithm>

#include "rocksdb/slice.h"
#include "rocksdb/filter_policy.h"

#include "../include/proteus.hpp"
#include "../include/modeling.hpp"
#include "../include/util.hpp"

namespace rocksdb {

class IntProteusFilterBitsBuilder : public FilterBitsBuilder {
    private:
        std::vector<uint64_t> keys_;
        std::vector<std::pair<uint64_t, uint64_t>> sample_queries_;
        double bpk_;

    public:
        IntProteusFilterBitsBuilder(proteus::FIFOSampleQueryCache<uint64_t>* sqc,
                                    double bpk) :
                                    bpk_(bpk) {
            
            // Get latest queries from sample query cache
            sample_queries_ = sqc->getSampleQueries();
            std::sort(sample_queries_.begin(), sample_queries_.end());
        }

        ~IntProteusFilterBitsBuilder() {
            keys_.clear();
            sample_queries_.clear();
        }

        void AddKey(const Slice& key) {
            keys_.push_back(proteus::sliceToUint64(key.data()));
        }

        Slice Finish(std::unique_ptr<const char[]>* buf) {
            std::tuple<uint64_t, uint64_t, uint64_t> parameters = proteus::modeling(keys_, 
                                                                                    sample_queries_, 
                                                                                    bpk_,
                                                                                    64);

            proteus::Proteus* proteus = new proteus::Proteus(keys_,
                                                             std::get<0>(parameters), // Trie Depth
                                                             std::get<1>(parameters), // Sparse Dense Cutoff
                                                             std::get<2>(parameters), // Bloom Filter Prefix Length
                                                             bpk_);

            std::pair<uint8_t*, size_t> ser = proteus->serialize();
            buf->reset((const char*)ser.first);
            Slice out((const char*)ser.first, ser.second);

            printf("Filter BPK: %0.9f \t Num Keys: %zu\n", ((double) ser.second * 8) / keys_.size(), keys_.size());
            
            delete proteus;

            return out;
        }
};

class StrProteusFilterBitsBuilder : public FilterBitsBuilder {
    private:
        std::vector<std::string> keys_;
        std::vector<std::pair<std::string, std::string>> sample_queries_;
        double bpk_;
        size_t max_keylen_;

    public:
        StrProteusFilterBitsBuilder(proteus::FIFOSampleQueryCache<std::string>* sqc,
                                    double bpk) :
                                    bpk_(bpk) {
            
            // Get latest queries from sample query cache
            sample_queries_ = sqc->getSampleQueries();
            std::sort(sample_queries_.begin(), sample_queries_.end());
            max_keylen_ = 0;
        }

        ~StrProteusFilterBitsBuilder() {
            keys_.clear();
            sample_queries_.clear();
        }

        void AddKey(const Slice& key) override {
            max_keylen_ = std::max(max_keylen_, key.size());
            keys_.push_back(key.ToString());
        }

        Slice Finish(std::unique_ptr<const char[]>* buf) override {
            std::tuple<uint64_t, uint64_t, uint64_t> parameters = proteus::modeling(keys_, 
                                                                                    sample_queries_, 
                                                                                    bpk_,
                                                                                    max_keylen_ * 8);

            proteus::Proteus* proteus = new proteus::Proteus(keys_,
                                                             std::get<0>(parameters), // Trie Depth
                                                             std::get<1>(parameters), // Sparse Dense Cutoff
                                                             std::get<2>(parameters), // Bloom Filter Prefix Length
                                                             bpk_);

            std::pair<uint8_t*, size_t> ser = proteus->serialize();
            buf->reset((const char*)ser.first);
            Slice out((const char*)ser.first, ser.second);

            printf("Filter BPK: %0.9f \t Num Keys: %zu\n", ((double) ser.second * 8) / keys_.size(), keys_.size());
            
            delete proteus;

            return out;
        }
};

class ProteusFilterBitsReader : public FilterBitsReader {
    protected:
        proteus::Proteus* filter_;

    public:
        ProteusFilterBitsReader(const Slice& contents) {
            filter_ = proteus::Proteus::deSerialize((char*) contents.data());  
        }

        ~ProteusFilterBitsReader(){
            delete filter_;
        }
        
        using FilterBitsReader::MayMatch;
        void MayMatch(int num_keys, Slice **keys, bool *may_match) override {
            (void)num_keys;
            (void)keys;
            (void)may_match;
            return;
        }
};

class IntProteusFilterBitsReader : public ProteusFilterBitsReader {
    using ProteusFilterBitsReader::ProteusFilterBitsReader;
    public:
        bool RangeQuery(const Slice& left, const Slice& right) override {
            return filter_->Query(proteus::sliceToUint64(left.data()), 
                                  proteus::sliceToUint64(right.data()));
        }
        
        using ProteusFilterBitsReader::MayMatch;
        bool MayMatch(const Slice& entry) override {
            return filter_->Query(proteus::sliceToUint64(entry.data()));
        }
};

class StrProteusFilterBitsReader : public ProteusFilterBitsReader {
    using ProteusFilterBitsReader::ProteusFilterBitsReader;
    public:
        bool RangeQuery(const Slice& left, const Slice& right) override {
            return filter_->Query(left.ToString(), right.ToString());
        }

        using ProteusFilterBitsReader::MayMatch;
        bool MayMatch(const Slice& entry) override {
            return filter_->Query(entry.ToString());
        }
};

class ProteusFilterPolicy : public FilterPolicy {
    public:
        const char* Name() const {
            return "Proteus";
        }

        void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
            (void)keys;
            (void)n;
            (void)dst;
            assert(false);
        }

        bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
            (void)key;
            (void)filter;

            assert(false);
            return true;
        }
};

class IntProteusFilterPolicy : public ProteusFilterPolicy {
    private:
        proteus::FIFOSampleQueryCache<uint64_t>* sqc_;
        double bpk_;

    public:
        explicit IntProteusFilterPolicy(proteus::FIFOSampleQueryCache<uint64_t>* sqc,
                                        double bpk) :
                                        sqc_(sqc),
                                        bpk_(bpk) { }
        ~IntProteusFilterPolicy() { }

        IntProteusFilterBitsBuilder* GetFilterBitsBuilder() const override {
            return new IntProteusFilterBitsBuilder(sqc_, bpk_);
        }

        IntProteusFilterBitsReader* GetFilterBitsReader(const Slice& contents) const override {
            return new IntProteusFilterBitsReader(contents);
        }
};

class StrProteusFilterPolicy : public ProteusFilterPolicy {
    private:
        proteus::FIFOSampleQueryCache<std::string>* sqc_;
        double bpk_;

    public:
        explicit StrProteusFilterPolicy(proteus::FIFOSampleQueryCache<std::string>* sqc,
                                        double bpk) :
                                        sqc_(sqc),
                                        bpk_(bpk) { }
        ~StrProteusFilterPolicy() { }

        StrProteusFilterBitsBuilder* GetFilterBitsBuilder() const override {
            return new StrProteusFilterBitsBuilder(sqc_, bpk_);
        }

        StrProteusFilterBitsReader* GetFilterBitsReader(const Slice& contents) const override {
            return new StrProteusFilterBitsReader(contents);
        }
};

const FilterPolicy* NewProteusFilterPolicy(proteus::FIFOSampleQueryCache<uint64_t>* sqc,
                                           double bpk) {
    return new IntProteusFilterPolicy(sqc, bpk);
}

const FilterPolicy* NewProteusFilterPolicy(proteus::FIFOSampleQueryCache<std::string>* sqc,
                                           double bpk) {
    return new StrProteusFilterPolicy(sqc, bpk);
}

} // namespace rocksdb