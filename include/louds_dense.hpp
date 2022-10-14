#pragma once
#ifndef PROTEUS_LOUDS_DENSE_HPP
#define PROTEUS_LOUDS_DENSE_HPP

#include <string>
#include <climits>

#include "config.hpp"
#include "rank.hpp"
#include "suffix.hpp"
#include "surf_builder.hpp"

namespace proteus {

class LoudsDense {
public:
    class Iter {
    public:
	Iter() : is_valid_(false),
			 is_search_complete_(false),
			 is_move_left_complete_(false),
			 is_move_right_complete_(false),
			 prefix_filter_true_(false),
			 trie_(nullptr),
			 send_out_node_num_(0), 
			 key_len_(0) {};
			 
	Iter(LoudsDense* trie) : is_valid_(false), 
							 is_search_complete_(false),
							 is_move_left_complete_(false),
							 is_move_right_complete_(false),
							 prefix_filter_true_(false),
							 trie_(trie),
							 send_out_node_num_(0), 
							 key_len_(0) {
	    for (level_t level = 0; level < trie_->getHeight(); level++) {
			key_.push_back(0);
			pos_in_trie_.push_back(0);
	    }
	}

	void clear();
	bool isValid() const { return is_valid_; };
	bool isSearchComplete() const { return is_search_complete_; };
	bool isMoveLeftComplete() const { return is_move_left_complete_; };
	bool isMoveRightComplete() const { return is_move_right_complete_; };
	bool isComplete() const {
	    return (is_search_complete_ &&
		    (is_move_left_complete_ && is_move_right_complete_));
	}

	// PROTEUS
	bool prefixFilterTrue() const { return prefix_filter_true_; };
	template<typename T>
	int compare(const T& key, PrefixBF* prefix_filter) const;

	std::string getKey() const;
	position_t getSendOutNodeNum() const { return send_out_node_num_; };

	void setToFirstLabelInRoot();
	void setToLastLabelInRoot();
	void moveToLeftMostKey();
	void moveToRightMostKey();
	void operator ++(int);
	void operator --(int);

    private:
	inline void append(position_t pos);
	inline void set(level_t level, position_t pos);
	inline void setSendOutNodeNum(position_t node_num) { send_out_node_num_ = node_num; };
	inline void setFlags(const bool is_valid, 
						 const bool is_search_complete,
			     		 const bool is_move_left_complete,
			     		 const bool is_move_right_complete, 
				 		 bool prefix_filter_true = false);

    private:
	// True means the iter either points to a valid key
	// or to a prefix with length trie_->getHeight()
	bool is_valid_;
	// If false, call moveToKeyGreaterThan in LoudsSparse to complete
	bool is_search_complete_;
	// If false, call moveToLeftMostKey in LoudsSparse to complete
	bool is_move_left_complete_;
	// If false, call moveToRightMostKey in LoudsSparse to complete
	bool is_move_right_complete_;

	// PROTEUS
	// If true, immediately return true overall
	bool prefix_filter_true_;

	LoudsDense* trie_;
	position_t send_out_node_num_;
	level_t key_len_; // Does NOT include suffix

	std::vector<label_t> key_;
	std::vector<position_t> pos_in_trie_;

	friend class LoudsDense;
	friend class Proteus;	// Allow LoudsSparse to get access to key prefix in LoudsDense Iter
    };

public:
    LoudsDense() {};
    LoudsDense(const SuRFBuilder* builder);

    ~LoudsDense() {}

    // Returns whether key exists in the trie so far
    // out_node_num == 0 means search terminates in louds-dense.
	template<typename T>
    bool lookupKey(const T& key, PrefixBF* prefix_filter, position_t& out_node_num) const;
    
	// return value indicates potential false positive
	template<typename T>
    bool moveToKeyGreaterThan(const T& lq,
							  const T& rq, 
							  LoudsDense::Iter& iter,
							  PrefixBF* prefix_filter) const;

    uint64_t getHeight() const { return height_; };
	uint32_t getTrieDepth() const { return trie_depth_; };
    uint64_t serializedSize() const;
    uint64_t getMemoryUsage() const;

    void serialize(char*& dst) const {
		// Trie depth is already serialized in Proteus parent class
		memcpy(dst, &height_, sizeof(height_));
		dst += sizeof(height_);
		align(dst);
		label_bitmaps_->serialize(dst);
		child_indicator_bitmaps_->serialize(dst);
		suffixes_->serialize(dst);
		align(dst);
    }

    static LoudsDense* deSerialize(char*& src, uint32_t trie_depth) {
		LoudsDense* louds_dense = new LoudsDense();
		louds_dense->trie_depth_ = trie_depth;
		memcpy(&(louds_dense->height_), src, sizeof(louds_dense->height_));
		src += sizeof(louds_dense->height_);
		align(src);
		louds_dense->label_bitmaps_ = BitvectorRank::deSerialize(src);
		louds_dense->child_indicator_bitmaps_ = BitvectorRank::deSerialize(src);
		louds_dense->suffixes_ = BitvectorSuffix::deSerialize(src);
		align(src);
		return louds_dense;
    }

    void destroy() {
		label_bitmaps_->destroy();
		child_indicator_bitmaps_->destroy();
		suffixes_->destroy();
		
		// PROTEUS
		delete label_bitmaps_;
		delete child_indicator_bitmaps_;
		delete suffixes_;
    }

private:
    position_t getChildNodeNum(const position_t pos) const;
    position_t getSuffixPos(const position_t pos) const;
    position_t getNextPos(const position_t pos) const;
    position_t getPrevPos(const position_t pos, bool* is_out_of_bound) const;

    bool compareSuffixGreaterThan(const position_t pos, 
								  const level_t level, 
								  const uint64_t lq,
								  const uint64_t rq,
								  std::string& edited_lq,
								  LoudsDense::Iter& iter,
								  PrefixBF* prefix_filter) const;
	bool compareSuffixGreaterThan(const position_t pos, 
								  const level_t level, 
								  const std::string& lq,
								  const std::string& rq,
								  std::string& edited_lq,
								  LoudsDense::Iter& iter,
								  PrefixBF* prefix_filter) const;

private:
    static const position_t kNodeFanout = 256;
    static const position_t kRankBasicBlockSize  = 512;

    level_t height_;
	uint32_t trie_depth_;

    BitvectorRank* label_bitmaps_;
    BitvectorRank* child_indicator_bitmaps_;
    BitvectorSuffix* suffixes_;

	/*
		PROTEUS
		prefix_key_indicator_bits is unnecessary as keys 
		will be padded out to the same length (the trie depth).
	*/
};


LoudsDense::LoudsDense(const SuRFBuilder* builder) {
    height_ = builder->getSparseDenseCutoff();
	trie_depth_ = builder->getTrieDepth();

    std::vector<position_t> num_bits_per_level;
    for (level_t level = 0; level < height_; level++) {
		num_bits_per_level.push_back(builder->getBitmapLabels()[level].size() * kWordSize);
	}

    label_bitmaps_ = new BitvectorRank(kRankBasicBlockSize, 
									   builder->getBitmapLabels(),
				       				   num_bits_per_level, 
									   0, 
									   height_);

    child_indicator_bitmaps_ = new BitvectorRank(kRankBasicBlockSize,
												 builder->getBitmapChildIndicatorBits(),
												 num_bits_per_level, 
												 0, 
												 height_);

	std::vector<position_t> num_suffix_bits_per_level;
	std::vector<position_t> num_suffixes_per_level;
	for (level_t level = 0; level < height_; level++) {
		num_suffix_bits_per_level.push_back(builder->getSuffixCounts()[level] * (builder->getSuffixLen(level + 1)));
		num_suffixes_per_level.push_back(builder->getSuffixCounts()[level]);
	}
		
	suffixes_ = new BitvectorSuffix(builder->getSuffixes(),
									num_suffix_bits_per_level,
									num_suffixes_per_level,
									0, 
									height_);
}

template<typename T>
bool LoudsDense::lookupKey(const T& key, PrefixBF* prefix_filter, position_t& out_node_num) const {
	position_t node_num = 0;
    position_t pos = 0;
	std::string edited_key = editAndStringify(key, trie_depth_, true);

    for (level_t level = 0; level < height_; level++) {
		pos = (node_num * kNodeFanout);
		pos += static_cast<label_t>(edited_key[level]);

		// if key byte does not exist
		if (!label_bitmaps_->readBit(pos)) {
			return false;
		}

		// if trie branch terminates
		if (!child_indicator_bitmaps_->readBit(pos)) { 
			return (suffixes_->checkEquality(getSuffixPos(pos), edited_key, level + 1, trie_depth_)) 
			        && (prefix_filter == nullptr || prefix_filter->Query(key));
		} 
		
		node_num = getChildNodeNum(pos);
    }

    // search will continue in LoudsSparse
    out_node_num = node_num;

    return true;
}

template<typename T>
bool LoudsDense::moveToKeyGreaterThan(const T& lq,
									  const T& rq, 
									  LoudsDense::Iter& iter,
									  PrefixBF* prefix_filter) const {
	
	position_t node_num = 0;
    position_t pos = 0;
	std::string edited_lq = editAndStringify(lq, trie_depth_, true);
	
    for (level_t level = 0; level < height_; level++) {
		pos = node_num * kNodeFanout;
		pos += static_cast<label_t>(edited_lq[level]);
		iter.append(pos);

		// if no exact match
		if (!label_bitmaps_->readBit(pos)) {
			iter++;
			return false;
		}

		// if trie branch terminates
		if (!child_indicator_bitmaps_->readBit(pos)) {
			return compareSuffixGreaterThan(pos, level + 1, lq, rq, edited_lq, iter, prefix_filter);
		}
		
		node_num = getChildNodeNum(pos);
    }

    // search will continue in LoudsSparse
    iter.setSendOutNodeNum(node_num);

    // valid, search INCOMPLETE, moveLeft complete, moveRight complete
    iter.setFlags(true, false, true, true);

    return true;
}

// PROTEUS - align the metadata bits
uint64_t LoudsDense::serializedSize() const {
    uint64_t size = sizeof(height_);
	sizeAlign(size);
	size += (label_bitmaps_->serializedSize() +
			 child_indicator_bitmaps_->serializedSize() +
			 suffixes_->serializedSize());
    sizeAlign(size);
    return size;
}

uint64_t LoudsDense::getMemoryUsage() const {
    return (sizeof(LoudsDense)
			+ label_bitmaps_->size()
			+ child_indicator_bitmaps_->size()
			+ suffixes_->size());
}

position_t LoudsDense::getChildNodeNum(const position_t pos) const {
    return child_indicator_bitmaps_->rank(pos);
}

position_t LoudsDense::getSuffixPos(const position_t pos) const {
    return (label_bitmaps_->rank(pos)
			- child_indicator_bitmaps_->rank(pos)
			- 1);
}

position_t LoudsDense::getNextPos(const position_t pos) const {
    return pos + label_bitmaps_->distanceToNextSetBit(pos);
}

position_t LoudsDense::getPrevPos(const position_t pos, bool* is_out_of_bound) const {
    position_t distance = label_bitmaps_->distanceToPrevSetBit(pos);
    if (pos <= distance) {
		*is_out_of_bound = true;
		return 0;
    }
    *is_out_of_bound = false;
    return (pos - distance);
}

bool LoudsDense::compareSuffixGreaterThan(const position_t pos, 
										  const level_t level, 
										  const uint64_t lq,
										  const uint64_t rq,
										  std::string& edited_lq,
					  					  LoudsDense::Iter& iter,
										  PrefixBF* prefix_filter) const {
	
    int compare = suffixes_->compare(getSuffixPos(pos), 
									 edited_lq,
									 level,
									 trie_depth_);

    if (compare != kCouldBePositive) {
		if (compare < 0) {
			// Left query bound is bigger than the current key
			// prefix so we advance to the next key in the trie 
			iter++;
			return false;
		} else {
			// Left query bound is <= current key prefix so we return 
			// to lookupRange to compare against the right query bound
			iter.setFlags(true, true, true, true);
    		return true;
		}	
    }

	// No Prefix Filter
	if (prefix_filter == nullptr) {
		iter.setFlags(true, true, true, true);
		return true;
	}

	uint64_t trie_max = editKey(lq, trie_depth_, false);
	uint64_t right_query = std::min(rq, trie_max + 1);

	// At this point, lq shares a common prefix with the trie.
	// Therefore, lq >= trie_min.
	if (prefix_filter->Query(lq, right_query)) {
		// set prefix_filter_true_ to true
		iter.setFlags(true, true, true, true, true);
    	return true;
	} else {
		iter++;
		return false;
	}
}

bool LoudsDense::compareSuffixGreaterThan(const position_t pos, 
										  const level_t level, 
										  const std::string& lq,
										  const std::string& rq,
										  std::string& edited_lq,
					  					  LoudsDense::Iter& iter,
										  PrefixBF* prefix_filter) const {
	
    int compare = suffixes_->compare(getSuffixPos(pos), 
									 edited_lq, 
									 level,
									 trie_depth_);

    if (compare != kCouldBePositive) {
		if (compare < 0) {
			iter++;
			return false;
		} else {
			iter.setFlags(true, true, true, true);
    		return true;
		}	
    }

	if (prefix_filter == nullptr) {
		iter.setFlags(true, true, true, true);
		return true;
	}

	uint32_t tdepth_byte_aligned = div8(trie_depth_ + 7);
	uint32_t bflen_byte_aligned = div8(prefix_filter->getPrefixLen() + 7);

	// Pad trie bytes with 1s up to the byte-aligned BF prefix length	
	edited_lq.resize(bflen_byte_aligned, UCHAR_MAX);

	// Pad with 1s from the trie depth to the byte-aligned trie depth if necessary
	uint32_t trie_bit_remainder = mod8(trie_depth_);
	if (trie_bit_remainder != 0) {
		edited_lq[tdepth_byte_aligned - 1] |= invertedBitCutoffMasks[trie_bit_remainder];
	}

	// Pad with 0s from the BF prefix length to the byte-aligned BF prefix length if necessary
	uint32_t bf_bit_remainder = mod8(prefix_filter->getPrefixLen());
	if (bf_bit_remainder != 0) {
		edited_lq[bflen_byte_aligned - 1] &= bitCutoffMasks[bf_bit_remainder];
	}

	std::string right_query = rq.compare(edited_lq) < 0 ? rq : edited_lq;

	if (prefix_filter->Query(lq, right_query)) {
		iter.setFlags(true, true, true, true, true);
    	return true;
	} else {
		iter++;
		return false;
	}
}

//============================================================================

void LoudsDense::Iter::clear() {
    is_valid_ = false;
    key_len_ = 0;
	prefix_filter_true_ = false;
}

template<typename T>
int LoudsDense::Iter::compare(const T& key, PrefixBF* prefix_filter) const {
	std::string skey = stringify(key);
    std::string iter_key = getKey();
    int compare = iter_key.compare(skey.substr(0, iter_key.length()));
    if (compare != 0) return compare;
    if (isComplete()) {
		position_t suffix_pos = trie_->getSuffixPos(pos_in_trie_[key_len_ - 1]);
		int suffix_compare = trie_->suffixes_->compare(suffix_pos, skey, key_len_, trie_->getTrieDepth());
		if (suffix_compare != kCouldBePositive || prefix_filter == nullptr) {
			return suffix_compare;
		}
		
		iter_key.resize(skey.length(), '\0');

		bool res;
		if (std::is_same<T, std::string>::value) {
			res = prefix_filter->Query(iter_key, key);
		} else if (std::is_same<T, uint64_t>::value) {
			res = prefix_filter->Query(stringToUint64(iter_key), integerify(key));
		} else {
			assert(false);
		}

		return res ? kCouldBePositive : 1;
    }

    return compare;
}

std::string LoudsDense::Iter::getKey() const {
    if (!is_valid_)
		return std::string();
    level_t len = key_len_;
    return std::string((const char*)key_.data(), (size_t)len);
}

void LoudsDense::Iter::append(position_t pos) {
    assert(key_len_ < key_.size());
    key_[key_len_] = (label_t)(pos % kNodeFanout);
    pos_in_trie_[key_len_] = pos;
    key_len_++;
}

void LoudsDense::Iter::set(level_t level, position_t pos) {
    assert(level < key_.size());
    key_[level] = (label_t)(pos % kNodeFanout);
    pos_in_trie_[level] = pos;
}

void LoudsDense::Iter::setFlags(const bool is_valid,
							    const bool is_search_complete,
							    const bool is_move_left_complete,
							    const bool is_move_right_complete,
							    bool prefix_filter_true) {
    is_valid_ = is_valid;
    is_search_complete_ = is_search_complete;
    is_move_left_complete_ = is_move_left_complete;
    is_move_right_complete_ = is_move_right_complete;
	prefix_filter_true_ = prefix_filter_true;
}

void LoudsDense::Iter::setToFirstLabelInRoot() {
    if (trie_->label_bitmaps_->readBit(0)) {
		pos_in_trie_[0] = 0;
		key_[0] = (label_t)0;
    } else {
		pos_in_trie_[0] = trie_->getNextPos(0);
		key_[0] = (label_t)pos_in_trie_[0];
    }
    key_len_++;
}

void LoudsDense::Iter::setToLastLabelInRoot() {
    bool is_out_of_bound;
    pos_in_trie_[0] = trie_->getPrevPos(kNodeFanout, &is_out_of_bound);
    key_[0] = (label_t)pos_in_trie_[0];
    key_len_++;
}

void LoudsDense::Iter::moveToLeftMostKey() {
    assert(key_len_ > 0);
    level_t level = key_len_ - 1;
    position_t pos = pos_in_trie_[level];
    if (!trie_->child_indicator_bitmaps_->readBit(pos))
		// valid, search complete, moveLeft complete, moveRight complete
		return setFlags(true, true, true, true);

    while (level < trie_->getHeight() - 1) {
		position_t node_num = trie_->getChildNodeNum(pos);
		pos = trie_->getNextPos(node_num * kNodeFanout - 1);
		append(pos);

		// if trie branch terminates
		if (!trie_->child_indicator_bitmaps_->readBit(pos))
			// valid, search complete, moveLeft complete, moveRight complete
			return setFlags(true, true, true, true);

		level++;
    }
    send_out_node_num_ = trie_->getChildNodeNum(pos);
    // valid, search complete, moveLeft INCOMPLETE, moveRight complete
    setFlags(true, true, false, true);
}

void LoudsDense::Iter::moveToRightMostKey() {
    assert(key_len_ > 0);
    level_t level = key_len_ - 1;
    position_t pos = pos_in_trie_[level];
    if (!trie_->child_indicator_bitmaps_->readBit(pos))
		// valid, search complete, moveLeft complete, moveRight complete
		return setFlags(true, true, true, true);

    while (level < trie_->getHeight() - 1) {
		position_t node_num = trie_->getChildNodeNum(pos);
		bool is_out_of_bound;
		pos = trie_->getPrevPos((node_num + 1) * kNodeFanout, &is_out_of_bound);
		if (is_out_of_bound) {
			is_valid_ = false;
			return;
		}
		append(pos);

		// if trie branch terminates
		if (!trie_->child_indicator_bitmaps_->readBit(pos))
			// valid, search complete, moveLeft complete, moveRight complete
			return setFlags(true, true, true, true);

		level++;
    }
    send_out_node_num_ = trie_->getChildNodeNum(pos);
    // valid, search complete, moveleft complete, moveRight INCOMPLETE
    setFlags(true, true, true, false);
}

void LoudsDense::Iter::operator ++(int) {
    assert(key_len_ > 0);

    position_t pos = pos_in_trie_[key_len_ - 1];
    position_t next_pos = trie_->getNextPos(pos);
    // if crossing node boundary
    while ((next_pos / kNodeFanout) > (pos / kNodeFanout)) {
		key_len_--;
		if (key_len_ == 0) {
			is_valid_ = false;
			return;
		}
		pos = pos_in_trie_[key_len_ - 1];
		next_pos = trie_->getNextPos(pos);
    }
    set(key_len_ - 1, next_pos);
    return moveToLeftMostKey();
}

void LoudsDense::Iter::operator --(int) {
    assert(key_len_ > 0);
    position_t pos = pos_in_trie_[key_len_ - 1];
    bool is_out_of_bound;
    position_t prev_pos = trie_->getPrevPos(pos, &is_out_of_bound);
    if (is_out_of_bound) {
		is_valid_ = false;
		return;
    }

    // if crossing node boundary
    while ((prev_pos / kNodeFanout) < (pos / kNodeFanout)) {
		key_len_--;
		if (key_len_ == 0) {
			is_valid_ = false;
			return;
		}
		pos = pos_in_trie_[key_len_ - 1];
		prev_pos = trie_->getPrevPos(pos, &is_out_of_bound);
		if (is_out_of_bound) {
			is_valid_ = false;
			return;
		}
    }
    set(key_len_ - 1, prev_pos);
    return moveToRightMostKey();
}

} //namespace proteus

#endif