#pragma once
#ifndef PROTEUS_SURF_BUILDER_HPP
#define PROTEUS_SURF_BUILDER_HPP

#include <assert.h>
#include <math.h>
#include <string>
#include <vector>

#include "config.hpp"
#include "suffix.hpp"

#include "prefixbf.hpp"

namespace proteus {

class SuRFBuilder {
public: 
    /* 
        Proteus only uses Real suffixes to create a uniform depth trie.
        Instead of using a sparse-dense ratio like SuRF, Proteus' modeling
        is able to determine the optimal sparse-dense cutoff for the given
        dataset and specified trie depth.
    */ 
    explicit SuRFBuilder(size_t sparse_dense_cutoff, size_t trie_depth)
	: sparse_dense_cutoff_(sparse_dense_cutoff), trie_depth_(trie_depth) {};

    ~SuRFBuilder() {};

    // Fills in the LOUDS-dense and sparse vectors (members of this class)
    // through a single scan of the sorted key list.
    // After build, the member vectors are used in SuRF constructor.
    // REQUIRED: provided key list must be sorted.
    template<typename T>
    void build(const std::vector<T>& keys);

    static bool readBit(const std::vector<word_t>& bits, const position_t pos) {
        assert(pos < (bits.size() * kWordSize));
        position_t word_id = pos / kWordSize;
        position_t offset = pos % kWordSize;
        return (bits[word_id] & (kMsbMask >> offset));
    }

    static void setBit(std::vector<word_t>& bits, const position_t pos) {
        assert(pos < (bits.size() * kWordSize));
        position_t word_id = pos / kWordSize;
        position_t offset = pos % kWordSize;
        bits[word_id] |= (kMsbMask >> offset);
    }

    level_t getTreeHeight() const {
	    return labels_.size();
    }

    // const accessors
    const std::vector<std::vector<word_t> >& getBitmapLabels() const {
	    return bitmap_labels_;
    }
    const std::vector<std::vector<word_t> >& getBitmapChildIndicatorBits() const {
	    return bitmap_child_indicator_bits_;
    }
    const std::vector<std::vector<label_t> >& getLabels() const {
	    return labels_;
    }
    const std::vector<std::vector<word_t> >& getChildIndicatorBits() const {
	    return child_indicator_bits_;
    }
    const std::vector<std::vector<word_t> >& getLoudsBits() const {
	    return louds_bits_;
    }
    const std::vector<std::vector<word_t> >& getSuffixes() const {
	    return suffixes_;
    }
    const std::vector<position_t>& getSuffixCounts() const {
	    return suffix_counts_;
    }
    const std::vector<position_t>& getNodeCounts() const {
	    return node_counts_;
    }

    // PROTEUS
    level_t getSuffixLen(level_t level) const {
        // Returns the suffix length for a node terminating at the supplied level (which is 0-indexed).
        // A node that terminates at level 1 has 1 key byte, etc.
        return (level * 8 < trie_depth_) ? trie_depth_ - (level * 8) : 0; 
    }
    level_t getTrieDepth() const {
	    return trie_depth_;
    }
    level_t getSparseDenseCutoff() const {
	    return sparse_dense_cutoff_;
    }
    bool isSameEditedKey(const uint64_t a, const uint64_t b) {
	    return (a >> (64 - trie_depth_)) == (b >> (64 - trie_depth_));
    }
    bool isSameEditedKey(const std::string& a, const std::string& b) {
        return compare(a, b, trie_depth_) == 0;
    }

private:
    static bool isSameKey(const std::string& a, const std::string& b) {
	    return a.compare(b) == 0;
    }

    // Fill in the LOUDS-Sparse vectors through a single scan
    // of the sorted key list.
    template<typename T>
    void buildSparse(const std::vector<T>& keys);

    // Walks down the current partially-filled trie by comparing key to
    // its previous key in the list until their prefixes do not match.
    // The previous key is stored as the last items in the per-level 
    // label vector.
    // For each matching prefix byte(label), it sets the corresponding
    // child indicator bit to 1 for that label.
    level_t skipCommonPrefix(const std::string& key);

    // Starting at the start_level of the trie, the function inserts 
    // key bytes to the trie vectors until the first byte/label where 
    // key and next_key do not match.
    // This function is called after skipCommonPrefix. Therefore, it
    // guarantees that the stored prefix of key is unique in the trie.
    level_t insertKeyBytesToTrieUntilUnique(const std::string& key, const std::string& next_key, const level_t start_level);

    // PROTEUS - Fills in suffix bytes for key up to trie_depth
    inline void insertSuffix(const std::string& key, const level_t level);

    inline bool isCharCommonPrefix(const label_t c, const level_t level) const;
    inline bool isLevelEmpty(const level_t level) const;
    inline void moveToNextItemSlot(const level_t level);
    void insertKeyByte(const char c, const level_t level, const bool is_start_of_node);
    inline void storeSuffix(const level_t level, const word_t suffix);
    
    // Fill in the LOUDS-Dense vectors based on the built sparse vectors.
    void buildDense();

    void initDenseVectors(const level_t level);
    void setLabelAndChildIndicatorBitmap(const level_t level, const position_t node_num, const position_t pos);

    position_t getNumItems(const level_t level) const;
    void addLevel();
    bool isStartOfNode(const level_t level, const position_t pos) const;

private:
    // trie level < sparse_dense_cutoff_: LOUDS-Dense
    // trie level >= sparse_dense_cutoff_: LOUDS-Sparse
    size_t sparse_dense_cutoff_;
    size_t trie_depth_;

    // LOUDS-Sparse bit/byte vectors
    std::vector<std::vector<label_t> > labels_;
    std::vector<std::vector<word_t> > child_indicator_bits_;
    std::vector<std::vector<word_t> > louds_bits_;

    // LOUDS-Dense bit vectors
    std::vector<std::vector<word_t> > bitmap_labels_;
    std::vector<std::vector<word_t> > bitmap_child_indicator_bits_;

    std::vector<std::vector<word_t> > suffixes_;
    std::vector<position_t> suffix_counts_;

    // auxiliary per level bookkeeping vectors
    std::vector<position_t> node_counts_;
};

template<typename T>
void SuRFBuilder::build(const std::vector<T>& keys) {
    assert(keys.size() > 0);
    buildSparse(keys);
    if (sparse_dense_cutoff_ > 0) {
        buildDense();
    }
}

/*
    The trie construction process for Proteus is the same as SuRF except the
    keys are first truncated / padded appropriately to the specified trie depth.
*/
template<typename T>
void SuRFBuilder::buildSparse(const std::vector<T>& keys) {
    std::string next_edited_key = editAndStringify(keys[0], trie_depth_, true);
    for (size_t i = 0; i < keys.size(); i++) {
        std::string edited_key = next_edited_key;
        level_t level = skipCommonPrefix(edited_key);
        while ((i + 1 < keys.size()) && isSameEditedKey(keys[i], keys[i+1])) {
            i++;
        }
        if (i < keys.size() - 1) {
            next_edited_key = editAndStringify(keys[i+1], trie_depth_, true);
            level = insertKeyBytesToTrieUntilUnique(edited_key, next_edited_key, level);
        } else { // for last key, there is no successor key in the list
            level = insertKeyBytesToTrieUntilUnique(edited_key, std::string(), level);
        }

        // No need to insert suffix if we are at the cutoff / last level
        if (level * 8 < trie_depth_) {
            insertSuffix(edited_key, level);
        }
    }
}

level_t SuRFBuilder::skipCommonPrefix(const std::string& key) {
    level_t level = 0;
    while (level < key.length() && isCharCommonPrefix((label_t)key[level], level)) {
        setBit(child_indicator_bits_[level], getNumItems(level) - 1);
        level++;
    }
    return level;
}

// Potential Optimization - Avoid cost of padding keys in buildSparse by directly inserting null bytes here
level_t SuRFBuilder::insertKeyBytesToTrieUntilUnique(const std::string& key, const std::string& next_key, const level_t start_level) {
    assert(start_level < key.length());
    level_t level = start_level;
    bool is_start_of_node = false;

    // If it is the start of level, the louds bit needs to be set.
    if (isLevelEmpty(level)) {
        is_start_of_node = true;
    }
	
    // After skipping the common prefix, the first following byte
    // shoud be in an the node as the previous key.
    insertKeyByte(key[level], level, is_start_of_node);
    level++;

    if (level > next_key.length() || !isSameKey(key.substr(0, level), next_key.substr(0, level))) {
        return level;
    }
	
    // All the following bytes inserted must be the start of a new node.
    is_start_of_node = true;
    while (level < key.length() && level < next_key.length() && key[level] == next_key[level]) {
	    insertKeyByte(key[level], level, is_start_of_node);
	    level++;
    }

    // PROTEUS
    // The next byte inserted makes key unique in the trie.
    // Since keys are padded to a fixed length (the trie depth),
    // there must be a unique key byte to insert.
    insertKeyByte(key[level], level, is_start_of_node);
    level++;

    return level;
}

/*
    In SuRF, every leaf node stores a suffix of the same length across the trie. 
    In Proteus, suffixes are used to extend leaf nodes up to the trie depth,
    i.e. leaf nodes at the trie depth don't store anything.
*/
inline void SuRFBuilder::insertSuffix(const std::string& key, const level_t level) {
    if (level >= getTreeHeight()) {
        addLevel();
    }
    assert(level - 1 < suffixes_.size());
    word_t suffix_word = BitvectorSuffix::constructSuffix(key, level, getSuffixLen(level));
    storeSuffix(level, suffix_word); 
}

inline bool SuRFBuilder::isCharCommonPrefix(const label_t c, const level_t level) const {
    return (level < getTreeHeight()) && (c == labels_[level].back());
}

inline bool SuRFBuilder::isLevelEmpty(const level_t level) const {
    return (level >= getTreeHeight()) || (labels_[level].size() == 0);
}

inline void SuRFBuilder::moveToNextItemSlot(const level_t level) {
    assert(level < getTreeHeight());
    position_t num_items = getNumItems(level);
    if (num_items % kWordSize == 0) {
        child_indicator_bits_[level].push_back(0);
        louds_bits_[level].push_back(0);
    }
}

void SuRFBuilder::insertKeyByte(const char c, const level_t level, const bool is_start_of_node) {
    // level should be at most equal to tree height
    if (level >= getTreeHeight()) {
        addLevel();
    }

    assert(level < getTreeHeight());

    // sets parent node's child indicator
    if (level > 0) {
        setBit(child_indicator_bits_[level-1], getNumItems(level-1) - 1);
    }

    labels_[level].push_back(c);
    if (is_start_of_node) {
        setBit(louds_bits_[level], getNumItems(level) - 1);
        node_counts_[level]++;
    }

    moveToNextItemSlot(level);
}


inline void SuRFBuilder::storeSuffix(const level_t level, const word_t suffix) {
    level_t suffix_len = getSuffixLen(level);
    position_t pos = suffix_counts_[level-1] * suffix_len;
    assert(pos <= (suffixes_[level-1].size() * kWordSize));
    if (pos == (suffixes_[level-1].size() * kWordSize)) {
        suffixes_[level-1].push_back(0);
    }
	    
    position_t word_id = pos / kWordSize;
    position_t offset = pos % kWordSize;
    position_t word_remaining_len = kWordSize - offset;

    if (suffix_len <= word_remaining_len) {
        word_t shifted_suffix = suffix << (word_remaining_len - suffix_len);
        suffixes_[level-1][word_id] += shifted_suffix;
    } else {
        word_t suffix_left_part = suffix >> (suffix_len - word_remaining_len);
        suffixes_[level-1][word_id] += suffix_left_part;
        suffixes_[level-1].push_back(0);
        word_id++;
        word_t suffix_right_part = suffix << (kWordSize - (suffix_len - word_remaining_len));
        suffixes_[level-1][word_id] += suffix_right_part;
    }
    suffix_counts_[level-1]++;
}

void SuRFBuilder::buildDense() {
    for (level_t level = 0; level < sparse_dense_cutoff_; level++) {
        initDenseVectors(level);
        if (getNumItems(level) == 0) {
            continue;
        }

        position_t node_num = 0;
        setLabelAndChildIndicatorBitmap(level, node_num, 0);
            
        for (position_t pos = 1; pos < getNumItems(level); pos++) {
            if (isStartOfNode(level, pos)) {
                node_num++;
            }
            setLabelAndChildIndicatorBitmap(level, node_num, pos);
        }
    }
}

void SuRFBuilder::initDenseVectors(const level_t level) {
    bitmap_labels_.push_back(std::vector<word_t>());
    bitmap_child_indicator_bits_.push_back(std::vector<word_t>());

    for (position_t nc = 0; nc < node_counts_[level]; nc++) {
        for (int i = 0; i < (int)kFanout; i += kWordSize) {
            bitmap_labels_[level].push_back(0);
            bitmap_child_indicator_bits_[level].push_back(0);
        }
    }
}

void SuRFBuilder::setLabelAndChildIndicatorBitmap(const level_t level, 
						                          const position_t node_num, 
                                                  const position_t pos) {
    label_t label = labels_[level][pos];
    setBit(bitmap_labels_[level], node_num * kFanout + label);
    if (readBit(child_indicator_bits_[level], pos))
	    setBit(bitmap_child_indicator_bits_[level], node_num * kFanout + label);
}

void SuRFBuilder::addLevel() {
    labels_.push_back(std::vector<label_t>());
    child_indicator_bits_.push_back(std::vector<word_t>());
    louds_bits_.push_back(std::vector<word_t>());
    suffixes_.push_back(std::vector<word_t>());
    suffix_counts_.push_back(0);

    node_counts_.push_back(0);

    child_indicator_bits_[getTreeHeight() - 1].push_back(0);
    louds_bits_[getTreeHeight() - 1].push_back(0);
}

position_t SuRFBuilder::getNumItems(const level_t level) const {
    return labels_[level].size();
}

bool SuRFBuilder::isStartOfNode(const level_t level, const position_t pos) const {
    return readBit(louds_bits_[level], pos);
}

} // namespace proteus

#endif