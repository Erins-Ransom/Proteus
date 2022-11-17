#pragma once
#ifndef PROTEUS_LOUDS_SPARSE_HPP
#define PROTEUS_LOUDS_SPARSE_HPP

#include <string>
#include <climits>

#include "config.hpp"
#include "label_vector.hpp"
#include "rank.hpp"
#include "select.hpp"
#include "suffix.hpp"
#include "surf_builder.hpp"

namespace proteus {

class LoudsSparse {
public:
    class Iter {
    public:
	Iter() : is_valid_(false),
			 is_done_(false), 
			 trie_(nullptr), 
			 start_level_(0),
			 start_node_num_(0),
			 key_len_(0) {};

	Iter(LoudsSparse* trie) : is_valid_(false), 
							  is_done_(false), 
							  trie_(trie), 
							  start_node_num_(0),
				  			  key_len_(0) {

	    start_level_ = trie_->getStartLevel();
	    for (level_t level = start_level_; level < trie_->getHeight(); level++) {
			key_.push_back(0);
			pos_in_trie_.push_back(0);
	    }
	}

	void clear();
	bool isValid() const { return is_valid_; };
	bool isDone() const { return is_done_; };

	// PROTEUS
	template<typename T>
	int compare(const T& key, PrefixBF* prefix_filter, const std::string& dense_prefix) const;

	std::string getKey() const;

	position_t getStartNodeNum() const { return start_node_num_; };
	void setStartNodeNum(position_t node_num) { start_node_num_ = node_num; };

	void setToFirstLabelInRoot();
	void setToLastLabelInRoot();
	void moveToLeftMostKey();
	void moveToRightMostKey();
	void operator ++(int);
	void operator --(int);

    private:
	void append(const position_t pos);
	void append(const label_t label, const position_t pos);
	void set(const level_t level, const position_t pos);

    private:
	bool is_valid_; // True means the iter currently points to a valid key

	// PROTEUS
	bool is_done_; // True means range query is done and is true overall

	LoudsSparse* trie_;
	level_t start_level_;
	position_t start_node_num_; // Passed in by the dense iterator; default = 0
	level_t key_len_; // Start counting from start_level_; does NOT include suffix

	std::vector<label_t> key_;
	std::vector<position_t> pos_in_trie_;

	friend class LoudsSparse;
    };

public:
    LoudsSparse() {};
    LoudsSparse(const SuRFBuilder* builder);

    ~LoudsSparse() {}

    // point query: trie walk starts at node "in_node_num" instead of root
    // in_node_num is provided by louds-dense's lookupKey function
	template<typename T>
    bool lookupKey(const T& key, PrefixBF* prefix_filter, const position_t in_node_num) const;
    
	// return value indicates potential false positive
	template<typename T>
    bool moveToKeyGreaterThan(const T& lq,
							  const T& rq, 
							  LoudsSparse::Iter& iter,
							  PrefixBF* prefix_filter) const;

    level_t getHeight() const { return height_; };
    level_t getStartLevel() const { return start_level_; };
	uint32_t getTrieDepth() const { return trie_depth_; };
    uint64_t serializedSize() const;
    uint64_t getMemoryUsage() const;

    void serialize(char*& dst) const {
		// Trie depth is already serialized in HyPR parent class
		memcpy(dst, &height_, sizeof(height_));
		dst += sizeof(height_);
		memcpy(dst, &start_level_, sizeof(start_level_));
		dst += sizeof(start_level_);
		memcpy(dst, &node_count_dense_, sizeof(node_count_dense_));
		dst += sizeof(node_count_dense_);
		memcpy(dst, &child_count_dense_, sizeof(child_count_dense_));
		dst += sizeof(child_count_dense_);
		align(dst);
		labels_->serialize(dst);
		child_indicator_bits_->serialize(dst);
		louds_bits_->serialize(dst);
		suffixes_->serialize(dst);
		align(dst);
    }

    static LoudsSparse* deSerialize(char*& src, uint32_t trie_depth) {
		LoudsSparse* louds_sparse = new LoudsSparse();
		louds_sparse->trie_depth_ = trie_depth;
		memcpy(&(louds_sparse->height_), src, sizeof(louds_sparse->height_));
		src += sizeof(louds_sparse->height_);
		memcpy(&(louds_sparse->start_level_), src, sizeof(louds_sparse->start_level_));
		src += sizeof(louds_sparse->start_level_);
		memcpy(&(louds_sparse->node_count_dense_), src, sizeof(louds_sparse->node_count_dense_));
		src += sizeof(louds_sparse->node_count_dense_);
		memcpy(&(louds_sparse->child_count_dense_), src, sizeof(louds_sparse->child_count_dense_));
		src += sizeof(louds_sparse->child_count_dense_);
		align(src);
		louds_sparse->labels_ = LabelVector::deSerialize(src);
		louds_sparse->child_indicator_bits_ = BitvectorRank::deSerialize(src);
		louds_sparse->louds_bits_ = BitvectorSelect::deSerialize(src);
		louds_sparse->suffixes_ = BitvectorSuffix::deSerialize(src);
		align(src);
		return louds_sparse;
    }

    void destroy() {
		labels_->destroy();
		child_indicator_bits_->destroy();
		louds_bits_->destroy();
		suffixes_->destroy();

		// PROTEUS
		delete labels_;
		delete child_indicator_bits_;
		delete louds_bits_;
		delete suffixes_;
    }

private:
    position_t getChildNodeNum(const position_t pos) const;
    position_t getFirstLabelPos(const position_t node_num) const;
    position_t getLastLabelPos(const position_t node_num) const;
    position_t getSuffixPos(const position_t pos) const;
    position_t nodeSize(const position_t pos) const;

    void moveToLeftInNextSubtrie(position_t pos, const position_t node_size,
				 				 const label_t label, LoudsSparse::Iter& iter) const;

    // return value indicates potential false positive
    bool compareSuffixGreaterThan(const position_t pos, 
								  const level_t level, 
							      const uint64_t lq,
								  const uint64_t rq,
								  std::string& edited_lq,
								  LoudsSparse::Iter& iter,
								  PrefixBF* prefix_filter) const;
    bool compareSuffixGreaterThan(const position_t pos, 
								  const level_t level, 
							      const std::string& lq,
								  const std::string& rq,
								  std::string& edited_lq,
								  LoudsSparse::Iter& iter,
								  PrefixBF* prefix_filter) const;

private:
    static const position_t kRankBasicBlockSize = 512;
    static const position_t kSelectSampleInterval = 64;

    level_t height_; // trie height
    level_t start_level_; // louds-sparse encoding starts at this level
    // number of nodes in louds-dense encoding
    position_t node_count_dense_;
    // number of children(1's in child indicator bitmap) in louds-dense encoding
    position_t child_count_dense_;

	uint32_t trie_depth_;

    LabelVector* labels_;
    BitvectorRank* child_indicator_bits_;
    BitvectorSelect* louds_bits_;
    BitvectorSuffix* suffixes_;
};


LoudsSparse::LoudsSparse(const SuRFBuilder* builder) {
    height_ = builder->getLabels().size();
    start_level_ = builder->getSparseDenseCutoff();
	trie_depth_ = builder->getTrieDepth();

    node_count_dense_ = 0;
    for (level_t level = 0; level < start_level_; level++) {
		node_count_dense_ += builder->getNodeCounts()[level];
	}

	// PROTEUS
    if (start_level_ == 0 || start_level_ == height_) {
		child_count_dense_ = 0;
	} else {
		child_count_dense_ = node_count_dense_ + builder->getNodeCounts()[start_level_] - 1;
	}

    labels_ = new LabelVector(builder->getLabels(), start_level_, height_);

    std::vector<position_t> num_items_per_level;
    for (level_t level = 0; level < height_; level++) {
		num_items_per_level.push_back(builder->getLabels()[level].size());
	}
		
    child_indicator_bits_ = new BitvectorRank(kRankBasicBlockSize, 
											  builder->getChildIndicatorBits(),
					     			          num_items_per_level,
											  start_level_,
											  height_);

    louds_bits_ = new BitvectorSelect(kSelectSampleInterval, 
								      builder->getLoudsBits(),
				      				  num_items_per_level, 
									  start_level_, 
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
									start_level_, 
									height_);
}

template<typename T>
bool LoudsSparse::lookupKey(const T& key, PrefixBF* prefix_filter, const position_t in_node_num) const {
    std::string truncated = editAndStringify(key, trie_depth_, true);
	
	position_t node_num = in_node_num;
    position_t pos = getFirstLabelPos(node_num);
    level_t level = 0;
    for (level = start_level_; level < truncated.length(); level++) {
		if (!labels_->search(static_cast<label_t>(truncated[level]), pos, nodeSize(pos))) {
			return false;
		}

		// if trie branch terminates
		if (!child_indicator_bits_->readBit(pos)) {
			return (suffixes_->checkEquality(getSuffixPos(pos), truncated, level + 1, trie_depth_)) 
			        && (prefix_filter == nullptr || prefix_filter->Query(key));
		}

		// move to child
		node_num = getChildNodeNum(pos);
		pos = getFirstLabelPos(node_num);
    }
		
	return false;
}

template<typename T>
bool LoudsSparse::moveToKeyGreaterThan(const T& lq,
									   const T& rq, 
									   LoudsSparse::Iter& iter,
									   PrefixBF* prefix_filter) const {

	position_t node_num = iter.getStartNodeNum();
    position_t pos = getFirstLabelPos(node_num);
	std::string edited_lq = editAndStringify(lq, trie_depth_, true);

    level_t level;
    for (level = start_level_; level < edited_lq.length(); level++) {
		position_t node_size = nodeSize(pos);

		// if no exact match
		if (!labels_->search(static_cast<label_t>(edited_lq[level]), pos, node_size)) {
			moveToLeftInNextSubtrie(pos, node_size, edited_lq[level], iter);
			return false;
		}

		iter.append(edited_lq[level], pos);

		// if trie branch terminates
		if (!child_indicator_bits_->readBit(pos))
			return compareSuffixGreaterThan(pos, level + 1, lq, rq, edited_lq, iter, prefix_filter);

		// move to child
		node_num = getChildNodeNum(pos);
		pos = getFirstLabelPos(node_num);
    }

    if (edited_lq.length() <= level) {
		iter.moveToLeftMostKey();
		return false;
    }

    iter.is_valid_ = true;
    return true;
}

// PROTEUS - align the metadata bits
uint64_t LoudsSparse::serializedSize() const {
    uint64_t size = sizeof(height_) +
					sizeof(start_level_) +
					sizeof(node_count_dense_) +
					sizeof(child_count_dense_);
	sizeAlign(size);
	size += (labels_->serializedSize() +
			 child_indicator_bits_->serializedSize() +
		     louds_bits_->serializedSize() +
			 suffixes_->serializedSize());
    sizeAlign(size);
	return size;
}

uint64_t LoudsSparse::getMemoryUsage() const {
    return (sizeof(this)
			+ labels_->size()
			+ child_indicator_bits_->size()
			+ louds_bits_->size()
			+ suffixes_->size());
}

position_t LoudsSparse::getChildNodeNum(const position_t pos) const {
    return (child_indicator_bits_->rank(pos) + child_count_dense_);
}

position_t LoudsSparse::getFirstLabelPos(const position_t node_num) const {
    return louds_bits_->select(node_num + 1 - node_count_dense_);
}

position_t LoudsSparse::getLastLabelPos(const position_t node_num) const {
    position_t next_rank = node_num + 2 - node_count_dense_;
    if (next_rank > louds_bits_->numOnes())
		return (louds_bits_->numBits() - 1);
    return (louds_bits_->select(next_rank) - 1);
}

position_t LoudsSparse::getSuffixPos(const position_t pos) const {
    return (pos - child_indicator_bits_->rank(pos));
}

position_t LoudsSparse::nodeSize(const position_t pos) const {
    assert(louds_bits_->readBit(pos));
    return louds_bits_->distanceToNextSetBit(pos);
}

void LoudsSparse::moveToLeftInNextSubtrie(position_t pos, const position_t node_size,
					  	  			      const label_t label, LoudsSparse::Iter& iter) const {
    // if no label is greater than key[level] in this node
    if (!labels_->searchGreaterThan(label, pos, node_size)) {
		iter.append(pos + node_size - 1);
		return iter++;
    } else {
		iter.append(pos);
		return iter.moveToLeftMostKey();
    }
}

bool LoudsSparse::compareSuffixGreaterThan(const position_t pos, 
										   const level_t level, 
										   const uint64_t lq,
										   const uint64_t rq,
										   std::string& edited_lq,
					  					   LoudsSparse::Iter& iter,
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
			iter.is_valid_ = true;
    		return true;
		}	
    }

	// No Prefix Filter
	if (prefix_filter == nullptr) {
		iter.is_valid_ = true;
		return true;
	}

	uint64_t trie_max = editKey(lq, trie_depth_, false);
	uint64_t right_query = std::min(rq, trie_max + 1);

	if (prefix_filter->Query(lq, right_query)) {
		// Return true overall for range query
		iter.is_done_ = true;
		iter.is_valid_ = true;
    	return true;
	} else {
		iter++;
		return false;
	}
}

bool LoudsSparse::compareSuffixGreaterThan(const position_t pos, 
										   const level_t level, 
										   const std::string& lq,
										   const std::string& rq,
										   std::string& edited_lq,
					  					   LoudsSparse::Iter& iter,
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
			iter.is_valid_ = true;
    		return true;
		}	
    } 

	// No Prefix Filter
	if (prefix_filter == nullptr) {
		iter.is_valid_ = true;
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
		iter.is_done_ = true;
		iter.is_valid_ = true;
    	return true;
	} else {
		iter++;
		return false;
	}
}

//============================================================================

void LoudsSparse::Iter::clear() {
	is_done_ = false;
    is_valid_ = false;
    key_len_ = 0;
}

template<typename T>
int LoudsSparse::Iter::compare(const T& key, PrefixBF* prefix_filter, const std::string& dense_prefix) const {
    std::string str_key = stringify(key);
	std::string iter_key = getKey();
    std::string key_sparse = str_key.substr(start_level_);
    std::string key_sparse_same_length = key_sparse.substr(0, iter_key.length());
    int compare = iter_key.compare(key_sparse_same_length);
    if (compare != 0) return compare;
    position_t suffix_pos = trie_->getSuffixPos(pos_in_trie_[key_len_ - 1]);
	int suffix_compare = trie_->suffixes_->compare(suffix_pos, str_key, start_level_ + iter_key.length(), trie_->getTrieDepth());
	if (suffix_compare != kCouldBePositive || prefix_filter == nullptr) {
		return suffix_compare;
	}

	std::string left_query = dense_prefix + iter_key;
	left_query.resize(str_key.length(), '\0');

	bool res;
	if (std::is_same<T, std::string>::value) {
		res = prefix_filter->Query(left_query, str_key);
	} else if (std::is_same<T, uint64_t>::value) {
		res = prefix_filter->Query(stringToUint64(left_query), integerify(key));
	} else {
		assert(false);
	}

	return res ? kCouldBePositive : 1;
}

std::string LoudsSparse::Iter::getKey() const {
    if (!is_valid_)
		return std::string();
    level_t len = key_len_;
    return std::string((const char*)key_.data(), (size_t)len);
}

void LoudsSparse::Iter::append(const position_t pos) {
    assert(key_len_ < key_.size());
    key_[key_len_] = trie_->labels_->read(pos);
    pos_in_trie_[key_len_] = pos;
    key_len_++;
}

void LoudsSparse::Iter::append(const label_t label, const position_t pos) {
    assert(key_len_ < key_.size());
    key_[key_len_] = label;
    pos_in_trie_[key_len_] = pos;
    key_len_++;
}

void LoudsSparse::Iter::set(const level_t level, const position_t pos) {
    assert(level < key_.size());
    key_[level] = trie_->labels_->read(pos);
    pos_in_trie_[level] = pos;
}

void LoudsSparse::Iter::setToFirstLabelInRoot() {
    assert(start_level_ == 0);
    pos_in_trie_[0] = 0;
    key_[0] = trie_->labels_->read(0);
}

void LoudsSparse::Iter::setToLastLabelInRoot() {
    assert(start_level_ == 0);
    pos_in_trie_[0] = trie_->getLastLabelPos(0);
    key_[0] = trie_->labels_->read(pos_in_trie_[0]);
}

void LoudsSparse::Iter::moveToLeftMostKey() {
    if (key_len_ == 0) {
		position_t pos = trie_->getFirstLabelPos(start_node_num_);
		label_t label = trie_->labels_->read(pos);
		append(label, pos);
    }
    level_t level = key_len_ - 1;
    position_t pos = pos_in_trie_[level];
    label_t label = trie_->labels_->read(pos);

    if (!trie_->child_indicator_bits_->readBit(pos)) {
		is_valid_ = true;
		return;
    }

    while (level < trie_->getHeight()) {
		position_t node_num = trie_->getChildNodeNum(pos);
		pos = trie_->getFirstLabelPos(node_num);
		label = trie_->labels_->read(pos);
		// if trie branch terminates
		if (!trie_->child_indicator_bits_->readBit(pos)) {
			append(label, pos);
			is_valid_ = true;
			return;
		}
		append(label, pos);
		level++;
    }
    assert(false); // shouldn't reach here
}

void LoudsSparse::Iter::moveToRightMostKey() {
    if (key_len_ == 0) {
		position_t pos = trie_->getFirstLabelPos(start_node_num_);
		pos = trie_->getLastLabelPos(start_node_num_);
		label_t label = trie_->labels_->read(pos);
		append(label, pos);
    }

    level_t level = key_len_ - 1;
    position_t pos = pos_in_trie_[level];
    label_t label = trie_->labels_->read(pos);

    if (!trie_->child_indicator_bits_->readBit(pos)) {
		is_valid_ = true;
		return;
    }

    while (level < trie_->getHeight()) {
		position_t node_num = trie_->getChildNodeNum(pos);
		pos = trie_->getLastLabelPos(node_num);
		label = trie_->labels_->read(pos);
		// if trie branch terminates
		if (!trie_->child_indicator_bits_->readBit(pos)) {
			append(label, pos);
			is_valid_ = true;
			return;
		}
		append(label, pos);
		level++;
    }
    assert(false); // shouldn't reach here
}

void LoudsSparse::Iter::operator ++(int) {
    assert(key_len_ > 0);
    position_t pos = pos_in_trie_[key_len_ - 1];
    pos++;
    while (pos >= trie_->louds_bits_->numBits() || trie_->louds_bits_->readBit(pos)) {
		key_len_--;
		if (key_len_ == 0) {
			is_valid_ = false;
			return;
		}
		pos = pos_in_trie_[key_len_ - 1];
		pos++;
    }
    set(key_len_ - 1, pos);
    return moveToLeftMostKey();
}

void LoudsSparse::Iter::operator --(int) {
    assert(key_len_ > 0);
    position_t pos = pos_in_trie_[key_len_ - 1];
    if (pos == 0) {
		is_valid_ = false;
		return;
    }
    while (trie_->louds_bits_->readBit(pos)) {
		key_len_--;
		if (key_len_ == 0) {
			is_valid_ = false;
			return;
		}
		pos = pos_in_trie_[key_len_ - 1];
    }
    pos--;
    set(key_len_ - 1, pos);
    return moveToRightMostKey();
}

} // namespace proteus

#endif