#include <iostream>
#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/filter_policy.h"
#include "../SuRF/include/surf.hpp"

namespace rocksdb {

class SuRFFilterBitsBuilder : public FilterBitsBuilder {
    private:
        uint32_t hash_len_;
        uint32_t real_len_;
        std::vector<std::string> keys_;

    public:
        explicit SuRFFilterBitsBuilder(uint32_t hash_len, uint32_t real_len)
        : hash_len_(hash_len),
          real_len_(real_len) {}

        ~SuRFFilterBitsBuilder() {
            keys_.clear();
        }

        void AddKey(const Slice& key) override {
            keys_.push_back(std::string(key.data(), key.size()));
        }
        
        Slice Finish(std::unique_ptr<const char[]>* buf) override {
            surf::SuRF* filter = 0; 
            if (hash_len_ == 0 && real_len_ == 0) {
                filter = new surf::SuRF(keys_, true, 64, surf::kNone, hash_len_, real_len_);
            } else if (real_len_ == 0) {
                filter = new surf::SuRF(keys_, true, 64, surf::kHash, hash_len_, real_len_);
            } else if (hash_len_ == 0) {
                filter = new surf::SuRF(keys_, true, 64, surf::kReal, hash_len_, real_len_);
            } else {
                filter = new surf::SuRF(keys_, true, 64, surf::kMixed, hash_len_, real_len_);
            }

            uint64_t size = filter->serializedSize();
            char* data = filter->serialize();
            buf->reset(data);
            Slice out(data, size);

            printf("Filter BPK: %0.9f \t Num Keys: %zu\n", ((double) size * 8) / keys_.size(), keys_.size());

            delete filter;
            return out;
        }
};

class SuRFFilterBitsReader : public FilterBitsReader {
    private:
        surf::SuRF* filter_;

    public:
        explicit SuRFFilterBitsReader(const Slice& contents) {
            filter_ = surf::SuRF::deSerialize(const_cast<char*>(contents.data()));
        }

        ~SuRFFilterBitsReader() {
            delete filter_;
        }

        bool MayMatch(const Slice& entry) override {
            return filter_->lookupKey(std::string(entry.data(), entry.size()));
        }

        void MayMatch(int num_keys, Slice **keys, bool *may_match) override {
            (void)num_keys;
            (void)keys;
            (void)may_match;
            return;
        }

        bool RangeQuery(const Slice& left, const Slice& right) override {
            return filter_->lookupRange(std::string(left.data(), left.size()), true, std::string(right.data(), right.size()), false);
        }
};

class SuRFFilterPolicy : public FilterPolicy {
    private:
        uint32_t hash_len_;
        uint32_t real_len_;

    public:
        explicit SuRFFilterPolicy(uint32_t hash_len, uint32_t real_len)
        : hash_len_(hash_len),
          real_len_(real_len) { }

        ~SuRFFilterPolicy() { }

        const char* Name() const override {
            return "SuRF";
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

        FilterBitsBuilder* GetFilterBitsBuilder() const override {
            return new SuRFFilterBitsBuilder(hash_len_, real_len_);
        }

        FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override {
            return new SuRFFilterBitsReader(contents);
        }
};

const FilterPolicy* NewSuRFFilterPolicy(uint32_t hash_len,
										uint32_t real_len) {
    return new SuRFFilterPolicy(hash_len, real_len);
}

} // namespace rocksdb