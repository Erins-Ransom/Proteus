#pragma once
#ifndef PROTEUS_HPP
#define PROTEUS_HPP

#include <string>
#include <vector>

#include "config.hpp"
#include "louds_dense.hpp"
#include "louds_sparse.hpp"
#include "surf_builder.hpp"

#include "prefixbf.hpp"

namespace proteus {

class Proteus {
public:
    class Iter {
    public:
	Iter() {};
	Iter(const Proteus* filter) {
	    dense_iter_ = filter->validLoudsDense() ? LoudsDense::Iter(filter->louds_dense_) :  LoudsDense::Iter();
	    sparse_iter_ = filter->validLoudsSparse() ? LoudsSparse::Iter(filter->louds_sparse_) :  LoudsSparse::Iter();
	    could_be_fp_ = false;
	}

    bool prefixFilterTrue(bool valid_dense, bool valid_sparse) const;
	void clear(bool valid_dense, bool valid_sparse);
	bool isValid(bool valid_dense, bool valid_sparse) const;
	bool getFpFlag() const;

    // PROTEUS
    template<typename T>
	int compare(const T& key, bool valid_dense, bool valid_sparse, PrefixBF* prefix_filter) const;
	
    std::string getKey() const;

    private:
	void passToSparse();
	bool incrementDenseIter();

    private:
	// true implies that dense_iter_ is valid
	LoudsDense::Iter dense_iter_;
	LoudsSparse::Iter sparse_iter_;
	bool could_be_fp_;

	friend class Proteus;
    };

public:
    Proteus() {};

    //------------------------------------------------------------------
    // Input keys must be SORTED
    //------------------------------------------------------------------

    template<typename T>
    Proteus(const std::vector<T>& keys, const size_t trie_depth, 
            const size_t sparse_dense_cutoff, const size_t prefix_length, const double bpk) : 
            prefix_filter(nullptr), trie_depth_(trie_depth), sparse_dense_cutoff_(sparse_dense_cutoff) {
        
        if (std::is_same<T, uint64_t>::value) {
            assert(trie_depth <= 64); 
        }
        
        assert(sparse_dense_cutoff * 8 < trie_depth + 8);

        size_t total_bits = static_cast<size_t>(round(bpk * keys.size()));
        if (trie_depth > 0) {
            builder_ = new SuRFBuilder(sparse_dense_cutoff, trie_depth);
            builder_->build(keys);
            louds_dense_ = validLoudsDense() ? new LoudsDense(builder_) : nullptr;
            louds_sparse_ = validLoudsSparse() ? new LoudsSparse(builder_) : nullptr;
            iter_ = Proteus::Iter(this);
            delete builder_;
            
            // Initialize prefix filter if there are sufficient bits
            size_t bits_used = (trieSerializedSize() + sizeof(uint32_t) + sizeof(char)) * 8;
            if (bits_used < total_bits && (prefix_length > 0 && trie_depth < 64)) {
                prefix_filter = new PrefixBF(prefix_length, total_bits - bits_used, keys);
            }
        } else if (prefix_length > 0) {
            // No trie; Prefix filter gets all the bits
            prefix_filter = new PrefixBF(prefix_length, total_bits, keys);
        }
    }

    ~Proteus() {
        if (trie_depth_ > 0) {
            destroy();
        }

        if (prefix_filter != nullptr) {
            delete prefix_filter;
        }
    }

    // PROTEUS
    template<typename T>
    bool Query(const T& key) const;
    template<typename T>
    bool Query(const T& left_key, const T& right_key);

    uint64_t trieSerializedSize() const;
    uint64_t getMemoryUsage() const;
    level_t getHeight() const;
    level_t getSparseStartLevel() const;

    std::pair<uint8_t*, size_t> serialize() const {
        uint64_t metadata_size = sizeof(uint32_t) * 2;
        sizeAlign(metadata_size);

        uint64_t trie_size = 0;
        if (trie_depth_ > 0) {
            trie_size = trieSerializedSize();
        }

        char has_prefix_filter = '\0';
        std::pair<uint8_t*, size_t> pbf_ser = {nullptr, 0};
        if (prefix_filter != nullptr) {
            has_prefix_filter = '1';
            pbf_ser = prefix_filter->serialize();
        }

        char* data = new char[metadata_size + trie_size + sizeof(char) + pbf_ser.second];
        char* cur_data = data;

        memcpy(cur_data, &trie_depth_, sizeof(uint32_t));
        cur_data += sizeof(uint32_t);
        memcpy(cur_data, &sparse_dense_cutoff_, sizeof(uint32_t));
        cur_data += sizeof(uint32_t);
        align(cur_data);

        if (trie_depth_ > 0) {
            if (validLoudsDense()) {
                louds_dense_->serialize(cur_data);
            }
            if (validLoudsSparse()) {
                louds_sparse_->serialize(cur_data);
            }
        }

        assert(cur_data - data == static_cast<int64_t>(metadata_size + trie_size));
        
        memcpy(cur_data, &has_prefix_filter, sizeof(char));
        cur_data += sizeof(char);

        if (prefix_filter != nullptr) {
            memcpy(cur_data, pbf_ser.first, pbf_ser.second);
            delete[] pbf_ser.first;
            cur_data += pbf_ser.second;
            assert(cur_data - data == static_cast<int64_t>(metadata_size + trie_size + sizeof(char) + pbf_ser.second));
        }
        
        return {reinterpret_cast<uint8_t*>(data), metadata_size + trie_size + sizeof(char) + pbf_ser.second};
    }

    static Proteus* deSerialize(char* src) {
        Proteus* proteus = new Proteus();

        memcpy(&proteus->trie_depth_, src, sizeof(uint32_t));
        src += sizeof(uint32_t);
        memcpy(&proteus->sparse_dense_cutoff_, src, sizeof(uint32_t));
        src += sizeof(uint32_t);
        align(src);

        if (proteus->trie_depth_ > 0) {
            if (proteus->validLoudsDense()) {
                proteus->louds_dense_ = LoudsDense::deSerialize(src, proteus->trie_depth_);
            }
            if (proteus->validLoudsSparse()) {
                proteus->louds_sparse_ = LoudsSparse::deSerialize(src, proteus->trie_depth_);
            }
            proteus->iter_ = Proteus::Iter(proteus);
        }

        char has_prefix_filter;
        memcpy(&has_prefix_filter, src, sizeof(char));
        src += sizeof(char);
        
        if (has_prefix_filter == '\0') {
            proteus->prefix_filter = nullptr;
        } else {
            auto deser = PrefixBF::deserialize(reinterpret_cast<uint8_t*>(src));
            proteus->prefix_filter = deser.first;
            src += deser.second;
        }

        return proteus;
    }

    void destroy() {
        if (validLoudsDense()) {
            louds_dense_->destroy();
            delete louds_dense_;
        }
	    if (validLoudsSparse()) {
            louds_sparse_->destroy();
            delete louds_sparse_;
        }
    }

    inline bool validLoudsDense() const {
        return sparse_dense_cutoff_ > 0;
    }

    inline bool validLoudsSparse() const {
        return sparse_dense_cutoff_ < ((trie_depth_ + 7) / 8);
    }

private:
    LoudsDense* louds_dense_;
    LoudsSparse* louds_sparse_;
    SuRFBuilder* builder_;
    Proteus::Iter iter_;
    PrefixBF* prefix_filter;
    uint32_t trie_depth_;
    uint32_t sparse_dense_cutoff_;
};

template<typename T>
bool Proteus::Query(const T& key) const {
    if (trie_depth_ == 0) {
        return prefix_filter->Query(key);
    }

    position_t connect_node_num = 0;
    if (validLoudsDense() && !louds_dense_->lookupKey(key, prefix_filter, connect_node_num)) {
        return false;
    } else if (connect_node_num != 0) {
        assert(validLoudsSparse());
        return louds_sparse_->lookupKey(key, prefix_filter, connect_node_num);
    }

    return true;
}

/*
    Assume that the left and right query bounds are INCLUSIVE and EXCLUSIVE respectively, similar to RocksDB Iterator API.
    Note that the prefix Bloom filter (PBF) string range query implementation will assume that the right query bound is inclusive,
    and this may incur an additional prefix query in some (rare) situations. 

    The general principle for connecting the FST and PBF query code is that the PBF should only be queried if the
    queried key fully matches all bytes stored in the trie.

    Considering prefixes of length equal to the trie depth, if there is a trie branch such that LQ < trie_branch < RQ, 
    then the query must be positive. Therefore, the only query prefixes that can match the trie will be those of the 
    left and right bounds and so an empty query can match at most two adjacent branches of the trie.

    The query logic starts by iterating to the first trie prefix that is greater than or equal to the left bound (moveToKeyGreaterThan).
    If said trie prefix and left bound share the same common prefix up to trie depth, the matching prefixes are then queried in the Bloom filter (moveToKeyGreaterThan). 
    If the Bloom filter returns true, we immediately report a positive query. 
    If the Bloom filter returns false, we move the iterator to the next trie branch. 
    Lastly, we compare the trie iterator to the right bound, which may incur further queries in the Bloom filter if the right bound
    matches the iterator (iter_.compare).
*/
template<typename T>
bool Proteus::Query(const T& left_key, const T& right_key) {
    if (trie_depth_ == 0) {
        return prefix_filter->Query(left_key, right_key);
    }

    iter_.clear(validLoudsDense(), validLoudsSparse());
    
    if (validLoudsDense()) {
        louds_dense_->moveToKeyGreaterThan(left_key, right_key, iter_.dense_iter_, prefix_filter);
        if (!iter_.dense_iter_.isValid()) return false;
        if (!iter_.dense_iter_.isComplete()) {
            if (!iter_.dense_iter_.isSearchComplete() && validLoudsSparse()) {
                iter_.passToSparse();
                louds_sparse_->moveToKeyGreaterThan(left_key, right_key, iter_.sparse_iter_, prefix_filter);
                if (!iter_.sparse_iter_.isValid() && validLoudsDense()) {
                    iter_.incrementDenseIter();
                }
            } else if (!iter_.dense_iter_.isMoveLeftComplete() && validLoudsSparse()) {
                iter_.passToSparse();
                iter_.sparse_iter_.moveToLeftMostKey();
            }
        }
    } else if (validLoudsSparse()) {
        louds_sparse_->moveToKeyGreaterThan(left_key, right_key, iter_.sparse_iter_, prefix_filter);
    }
    
    if (!iter_.isValid(validLoudsDense(), validLoudsSparse())) {
        return false;
    }

    // Return true if Prefix Bloom Filter returned true
    if (iter_.prefixFilterTrue(validLoudsDense(), validLoudsSparse())) {
        return true;
    }

    // Return true if prefix filter query returns true or 
    // if there is a key prefix between the two query bounds
    T rk = prefix_filter != nullptr ? editKey(right_key, prefix_filter->getPrefixLen(), true) : right_key;
    int compare = iter_.compare(rk, validLoudsDense(), validLoudsSparse(), prefix_filter);
    if (std::is_same<T, uint64_t>::value) {
        // If Proteus is a full trie, we know we are comparing full keys for integers and hence
        // we shouldn't return a positive if the right query bound is matched in the trie. 
        return (compare == kCouldBePositive && trie_depth_ != 64) || (compare < 0);
    } else {
        return (compare == kCouldBePositive) || (compare < 0);
    }
}

uint64_t Proteus::trieSerializedSize() const {
    if (trie_depth_ == 0) {
        return 0;
    } else {
        uint64_t size = 0;
        size += validLoudsDense() ? louds_dense_->serializedSize() : 0;
        size += validLoudsSparse() ? louds_sparse_->serializedSize() : 0;
        return size;
    }
}

// DOES NOT account for prefix filters
uint64_t Proteus::getMemoryUsage() const {
    uint64_t size = sizeof(Proteus);
    size += validLoudsDense() ? louds_dense_->getMemoryUsage() : 0;
    size += validLoudsSparse() ? louds_sparse_->getMemoryUsage() : 0;
    return size;
}

level_t Proteus::getHeight() const {
    return louds_sparse_->getHeight();
}

level_t Proteus::getSparseStartLevel() const {
    return louds_sparse_->getStartLevel();
}

//============================================================================

// PROTEUS
bool Proteus::Iter::prefixFilterTrue(bool valid_dense, bool valid_sparse) const {
    return (valid_dense && dense_iter_.prefixFilterTrue()) || (valid_sparse && sparse_iter_.isDone()); 
}

void Proteus::Iter::clear(bool valid_dense, bool valid_sparse) {
    if (valid_dense) {
        dense_iter_.clear();
    }
    if (valid_sparse) {
        sparse_iter_.clear();
    }
}

bool Proteus::Iter::getFpFlag() const {
    return could_be_fp_;
}

bool Proteus::Iter::isValid(bool valid_dense, bool valid_sparse) const {
    if (valid_dense && valid_sparse) {
        return dense_iter_.isValid() && (dense_iter_.isComplete() || sparse_iter_.isValid());
    } else if (valid_dense) {
        return dense_iter_.isValid();
    } else if (valid_sparse) {
        return sparse_iter_.isValid();
    } else {
        assert(false);
        return false;
    }
}

template<typename T>
int Proteus::Iter::compare(const T& key, bool valid_dense, bool valid_sparse, PrefixBF* prefix_filter) const {
    if (valid_dense) {
        int dense_compare = dense_iter_.compare(key, prefix_filter);
        if (dense_iter_.isComplete() || dense_compare != 0) { return dense_compare; }
        if (valid_sparse) {
            return sparse_iter_.compare(key, prefix_filter, dense_iter_.getKey());
        } else {
            return kCouldBePositive; // return true overall if there is no valid sparse
        }   
    } else if (valid_sparse) {
        return sparse_iter_.compare(key, prefix_filter, std::string());
    } else {
        assert(false);
        return 0;
    }
}

void Proteus::Iter::passToSparse() {
    sparse_iter_.setStartNodeNum(dense_iter_.getSendOutNodeNum());
}

bool Proteus::Iter::incrementDenseIter() {
    // This function is only called when the trie has both LOUDS Dense and LOUDS Sparse
    if (!dense_iter_.isValid())
	    return false;

    dense_iter_++;
    if (!dense_iter_.isValid())
	    return false;
    if (dense_iter_.isMoveLeftComplete())
	    return true;

    passToSparse();
    sparse_iter_.moveToLeftMostKey();
    return true;
}

} // namespace proteus

#endif