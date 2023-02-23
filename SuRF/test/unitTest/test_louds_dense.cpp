#include "gtest/gtest.h"

#include <assert.h>

#include <fstream>
#include <string>
#include <vector>

#include "config.hpp"
#include "louds_dense.hpp"
#include "surf_builder.hpp"

namespace surf {

namespace densetest {

static const std::string kFilePath = "../../../test/words.txt";
static const int kTestSize = 234369;
//static const uint64_t kIntTestStart = 10;
static const int kIntTestBound = 1000001;
static const uint64_t kIntTestSkip = 10;
static const bool kIncludeDense = true;
static const uint32_t kSparseDenseRatio = 0;
static const int kNumSuffixType = 4;
static const SuffixType kSuffixTypeList[kNumSuffixType] = {kNone, kHash, kReal, kMixed};
static const int kNumSuffixLen = 6;
static const level_t kSuffixLenList[kNumSuffixLen] = {1, 3, 7, 8, 13, 26};
static std::vector<std::string> words;

class DenseUnitTest : public ::testing::Test {
public:
    virtual void SetUp () {
	truncateWordSuffixes();
	fillinInts();
	data_ = nullptr;
    }
    virtual void TearDown () {
	if (data_)
	    delete[] data_;
    }

    void newBuilder(SuffixType suffix_type, level_t suffix_len);
    void truncateWordSuffixes();
    void fillinInts();
    void testSerialize();
    void testLookupWord();

    SuRFBuilder* builder_;
    LoudsDense* louds_dense_;
    std::vector<std::string> words_trunc_;
    std::vector<std::string> ints_;
    char* data_;
};

static int getCommonPrefixLen(const std::string &a, const std::string &b) {
    int len = 0;
    while ((len < (int)a.length()) && (len < (int)b.length()) && (a[len] == b[len]))
	len++;
    return len;
}

static int getMax(int a, int b) {
    if (a < b)
	return b;
    return a;
}

void DenseUnitTest::newBuilder(SuffixType suffix_type, level_t suffix_len) {
    if (suffix_type == kNone)
	builder_ = new SuRFBuilder(kIncludeDense, kSparseDenseRatio, kNone, 0, 0);
    else if (suffix_type == kHash)
	builder_ = new SuRFBuilder(kIncludeDense, kSparseDenseRatio, kHash, suffix_len, 0);
    else if (suffix_type == kReal)
	builder_ = new SuRFBuilder(kIncludeDense, kSparseDenseRatio, kReal, 0, suffix_len);
    else if (suffix_type == kMixed)
	builder_ = new SuRFBuilder(kIncludeDense, kSparseDenseRatio, kMixed, suffix_len, suffix_len);
    else
	builder_ = new SuRFBuilder(kIncludeDense, kSparseDenseRatio, kNone, 0, 0);
}

void DenseUnitTest::truncateWordSuffixes() {
    assert(words.size() > 1);

    int commonPrefixLen = 0;
    for (unsigned i = 0; i < words.size(); i++) {
	if (i == 0) {
	    commonPrefixLen = getCommonPrefixLen(words[i], words[i+1]);
	} else if (i == words.size() - 1) {
	    commonPrefixLen = getCommonPrefixLen(words[i-1], words[i]);
	} else {
	    commonPrefixLen = getMax(getCommonPrefixLen(words[i-1], words[i]),
				     getCommonPrefixLen(words[i], words[i+1]));
	}

	if (commonPrefixLen < (int)words[i].length()) {
	    words_trunc_.push_back(words[i].substr(0, commonPrefixLen + 1));
	} else {
	    words_trunc_.push_back(words[i]);
	    words_trunc_[i] += (char)kTerminator;
	}
    }
}

void DenseUnitTest::fillinInts() {
    for (uint64_t i = 0; i < kIntTestBound; i += kIntTestSkip) {
	ints_.push_back(uint64ToString(i));
    }
}

void DenseUnitTest::testSerialize() {
    uint64_t size = louds_dense_->serializedSize();
    data_ = new char[size];
    LoudsDense* ori_louds_dense = louds_dense_;
    char* data = data_;
    ori_louds_dense->serialize(data);
    data = data_;
    louds_dense_ = LoudsDense::deSerialize(data);

    ASSERT_EQ(ori_louds_dense->getHeight(), louds_dense_->getHeight());

    ori_louds_dense->destroy();
    delete ori_louds_dense;
}

void DenseUnitTest::testLookupWord() {
    position_t out_node_num = 0;
    for (unsigned i = 0; i < words.size(); i++) {
	bool key_exist = louds_dense_->lookupKey(words[i], out_node_num);
	ASSERT_TRUE(key_exist);
    }

    for (unsigned i = 0; i < words.size(); i++) {
	for (unsigned j = 0; j < words_trunc_[i].size() && j < words[i].size(); j++) {
	    std::string key = words[i];
	    key[j] = 'A';
	    bool key_exist = louds_dense_->lookupKey(key, out_node_num);
	    ASSERT_FALSE(key_exist);
	}
    }
}

TEST_F (DenseUnitTest, lookupWordTest) {
    for (int t = 0; t < kNumSuffixType; t++) {
	for (int k = 0; k < kNumSuffixLen; k++) {
	    newBuilder(kSuffixTypeList[t], kSuffixLenList[k]);
	    builder_->build(words);
	    louds_dense_ = new LoudsDense(builder_);
	    testLookupWord();
	    delete builder_;
	    louds_dense_->destroy();
	    delete louds_dense_;
	}
    }
}

TEST_F (DenseUnitTest, serializeTest) {
    for (int t = 0; t < kNumSuffixType; t++) {
	for (int k = 0; k < kNumSuffixLen; k++) {
	    newBuilder(kSuffixTypeList[t], kSuffixLenList[k]);
	    builder_->build(words);
	    louds_dense_ = new LoudsDense(builder_);
	    testSerialize();
	    testLookupWord();
	    delete builder_;
	}
    }
}

TEST_F (DenseUnitTest, lookupIntTest) {
    newBuilder(kReal, 8);
    builder_->build(ints_);
    louds_dense_ = new LoudsDense(builder_);
    position_t out_node_num = 0;

    for (uint64_t i = 0; i < kIntTestBound; i += kIntTestSkip) {
	bool key_exist = louds_dense_->lookupKey(uint64ToString(i), out_node_num);
	if (i % kIntTestSkip == 0) {
	    ASSERT_TRUE(key_exist);
	    ASSERT_EQ(0, out_node_num);
	} else {
	    ASSERT_FALSE(key_exist);
	    ASSERT_EQ(0, out_node_num);
	}
    }
    delete builder_;
    louds_dense_->destroy();
    delete louds_dense_;
}

TEST_F (DenseUnitTest, moveToKeyGreaterThanWordTest) {
    for (int t = 0; t < kNumSuffixType; t++) {
	for (int k = 0; k < kNumSuffixLen; k++) {
	    newBuilder(kSuffixTypeList[t], kSuffixLenList[k]);
	    builder_->build(words);
	    louds_dense_ = new LoudsDense(builder_);

	    bool inclusive = true;
	    for (int i = 0; i < 2; i++) {
		if (i == 1)
		    inclusive = false;
		for (unsigned j = 0; j < words.size() - 1; j++) {
		    LoudsDense::Iter iter(louds_dense_);
		    bool could_be_fp = louds_dense_->moveToKeyGreaterThan(words[j], inclusive, iter);

		    ASSERT_TRUE(iter.isValid());
		    ASSERT_TRUE(iter.isComplete());
		    std::string iter_key = iter.getKey();
		    std::string word_prefix_fp = words[j].substr(0, iter_key.length());
		    std::string word_prefix_true = words[j+1].substr(0, iter_key.length());
		    bool is_prefix = false;
		    if (could_be_fp)
			is_prefix = (word_prefix_fp.compare(iter_key) == 0);
		    else
			is_prefix = (word_prefix_true.compare(iter_key) == 0);
		    ASSERT_TRUE(is_prefix);
		}

		LoudsDense::Iter iter(louds_dense_);
		bool could_be_fp = louds_dense_->moveToKeyGreaterThan(words[words.size() - 1], inclusive, iter);
		if (could_be_fp) {
		    std::string iter_key = iter.getKey();
		    std::string word_prefix_fp = words[words.size() - 1].substr(0, iter_key.length());
		    bool is_prefix = (word_prefix_fp.compare(iter_key) == 0);
		    ASSERT_TRUE(is_prefix);
		} else {
		    ASSERT_FALSE(iter.isValid());
		}
	    }

	    delete builder_;
	    louds_dense_->destroy();
	    delete louds_dense_;
	}
    }
}

TEST_F (DenseUnitTest, moveToKeyGreaterThanIntTest) {
    newBuilder(kReal, 8);
    builder_->build(ints_);
    louds_dense_ = new LoudsDense(builder_);

    bool inclusive = true;
    for (int i = 0; i < 2; i++) {
	if (i == 1)
	    inclusive = false;
	for (uint64_t j = 0; j < kIntTestBound; j++) {
	    LoudsDense::Iter iter(louds_dense_);
	    bool could_be_fp = louds_dense_->moveToKeyGreaterThan(uint64ToString(j), inclusive, iter);

	    ASSERT_TRUE(iter.isValid());
	    ASSERT_TRUE(iter.isComplete());
	    std::string iter_key = iter.getKey();
	    std::string int_key_fp = uint64ToString(j - (j % kIntTestSkip));
	    std::string int_key_true = uint64ToString(j - (j % kIntTestSkip) + kIntTestSkip);
	    std::string int_prefix_fp = int_key_fp.substr(0, iter_key.length());
	    std::string int_prefix_true = int_key_true.substr(0, iter_key.length());
	    bool is_prefix = false;
	    if (could_be_fp)
		is_prefix = (int_prefix_fp.compare(iter_key) == 0);
	    else
		is_prefix = (int_prefix_true.compare(iter_key) == 0);
	    ASSERT_TRUE(is_prefix);
	}

	LoudsDense::Iter iter(louds_dense_);
	bool could_be_fp = louds_dense_->moveToKeyGreaterThan(uint64ToString(kIntTestBound - 1), inclusive, iter);
	if (could_be_fp) {
	    std::string iter_key = iter.getKey();
	    std::string int_key_fp = uint64ToString(kIntTestBound - 1);
	    std::string int_prefix_fp = int_key_fp.substr(0, iter_key.length());
	    bool is_prefix = (int_prefix_fp.compare(iter_key) == 0);
	    ASSERT_TRUE(is_prefix);
	} else {
	    ASSERT_FALSE(iter.isValid());
	}
    }

    delete builder_;
    louds_dense_->destroy();
    delete louds_dense_;
}

TEST_F (DenseUnitTest, IteratorIncrementWordTest) {
    newBuilder(kReal, 8);
    builder_->build(words);
    louds_dense_ = new LoudsDense(builder_);
    bool inclusive = true;
    LoudsDense::Iter iter(louds_dense_);
    louds_dense_->moveToKeyGreaterThan(words[0], inclusive, iter);    
    for (unsigned i = 1; i < words.size(); i++) {
	iter++;
	ASSERT_TRUE(iter.isValid());
	ASSERT_TRUE(iter.isComplete());
	std::string iter_key = iter.getKey();
	std::string word_prefix = words[i].substr(0, iter_key.length());
	bool is_prefix = (word_prefix.compare(iter_key) == 0);
	ASSERT_TRUE(is_prefix);
    }
    iter++;
    ASSERT_FALSE(iter.isValid());
    delete builder_;
    louds_dense_->destroy();
    delete louds_dense_;
}

TEST_F (DenseUnitTest, IteratorIncrementIntTest) {
    newBuilder(kReal, 8);
    builder_->build(ints_);
    louds_dense_ = new LoudsDense(builder_);
    bool inclusive = true;
    LoudsDense::Iter iter(louds_dense_);
    louds_dense_->moveToKeyGreaterThan(uint64ToString(0), inclusive, iter);
    for (uint64_t i = kIntTestSkip; i < kIntTestBound; i += kIntTestSkip) {
	iter++;
	ASSERT_TRUE(iter.isValid());
	ASSERT_TRUE(iter.isComplete());
	std::string iter_key = iter.getKey();
	std::string int_prefix = uint64ToString(i).substr(0, iter_key.length());
	bool is_prefix = (int_prefix.compare(iter_key) == 0);
	ASSERT_TRUE(is_prefix);
    }
    iter++;
    ASSERT_FALSE(iter.isValid());
    delete builder_;
    louds_dense_->destroy();
    delete louds_dense_;
}

TEST_F (DenseUnitTest, IteratorDecrementWordTest) {
    newBuilder(kReal, 8);
    builder_->build(words);
    louds_dense_ = new LoudsDense(builder_);
    bool inclusive = true;
    LoudsDense::Iter iter(louds_dense_);
    louds_dense_->moveToKeyGreaterThan(words[words.size() - 1], inclusive, iter);    
    for (int i = words.size() - 2; i >= 0; i--) {
	iter--;
	ASSERT_TRUE(iter.isValid());
	ASSERT_TRUE(iter.isComplete());
	std::string iter_key = iter.getKey();
	std::string word_prefix = words[i].substr(0, iter_key.length());
	bool is_prefix = (word_prefix.compare(iter_key) == 0);
	ASSERT_TRUE(is_prefix);
    }
    iter--;
    ASSERT_FALSE(iter.isValid());
    delete builder_;
    louds_dense_->destroy();
    delete louds_dense_;
}

TEST_F (DenseUnitTest, IteratorDecrementIntTest) {
    newBuilder(kReal, 8);
    builder_->build(ints_);
    louds_dense_ = new LoudsDense(builder_);
    bool inclusive = true;
    LoudsDense::Iter iter(louds_dense_);
    louds_dense_->moveToKeyGreaterThan(uint64ToString(kIntTestBound - kIntTestSkip), inclusive, iter);
    for (uint64_t i = kIntTestBound - 1 - kIntTestSkip; i > 0; i -= kIntTestSkip) {
	iter--;
	ASSERT_TRUE(iter.isValid());
	ASSERT_TRUE(iter.isComplete());
	std::string iter_key = iter.getKey();
	std::string int_prefix = uint64ToString(i).substr(0, iter_key.length());
	bool is_prefix = (int_prefix.compare(iter_key) == 0);
	ASSERT_TRUE(is_prefix);
    }
    iter--;
    iter--;
    ASSERT_FALSE(iter.isValid());
    delete builder_;
    louds_dense_->destroy();
    delete louds_dense_;
}

void loadWordList() {
    std::ifstream infile(kFilePath);
    std::string key;
    int count = 0;
    while (infile.good() && count < kTestSize) {
	infile >> key;
	words.push_back(key);
	count++;
    }
}

} // namespace densetest

} // namespace surf

int main (int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    surf::densetest::loadWordList();
    return RUN_ALL_TESTS();
}
