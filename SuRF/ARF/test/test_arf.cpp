#include "gtest/gtest.h"

#include <assert.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <random>
#include <vector>

#include "ARF.h"
#include "Database.h"
#include "Query.h"

using namespace arf;

static const int kInputSize = 1000000;
static const int kTrainingSize = 1000000;
static const int kTestSize = 1000000;
static const int kARFSize = 14000000;
static const uint64_t kDomain = (ULLONG_MAX / 2 - 1);
static const uint64_t kGap = (kDomain / kInputSize / 2);

double getNow() {
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec + tv.tv_usec / 1000000.0;
}

class ARFUnitTest : public ::testing::Test {
public:
    void loadKeys();
    void loadQueries();
    void training();

    std::vector<uint64_t> keys_;
    std::vector<Query::Query_t> training_queries_;
    std::vector<Query::Query_t> point_test_queries_;
    std::vector<Query::Query_t> range_test_queries_;
    Database* db_;
    ARF* arf_;
};

void ARFUnitTest::loadKeys() {
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, kDomain);
    for (int i = 0; i < kInputSize; i++) {
	uint64_t num = dist(e);
	keys_.push_back(num);
    }
    std::sort(keys_.begin(), keys_.end());
}

void ARFUnitTest::loadQueries() {
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, kDomain - kGap);
    for (int i = 0; i < kTrainingSize; i++) {
        uint64_t n1 = dist(e);
        uint64_t n2 = n1 + kGap;
        Query::Query_t q;
        q.left = n1;
        q.right = n2;
        training_queries_.push_back(q);
    }

    for (int i = 0; i < kTestSize; i++) {
        uint64_t n = dist(e);
        Query::Query_t q;
        q.left = n;
        q.right = n;
        point_test_queries_.push_back(q);
    }

    for (int i = 0; i < kTestSize; i++) {
        uint64_t n1 = dist(e);
        uint64_t n2 = n1 + kGap;
        Query::Query_t q;
        q.left = n1;
        q.right = n2;
        range_test_queries_.push_back(q);
    }
}

void ARFUnitTest::training() {
    for (int i = 0; i < (int)training_queries_.size(); i++) {
        //if (i % 1000 == 0)
        //std::cout << "i = " << i << std::endl;
        Query::Query_t q = training_queries_[i];
        bool qR = db_->rangeQuery(q.left, q.right);
        arf_->handle_query(q.left, q.right, qR, true);
    }
    arf_->reset_training_phase();
    arf_->truncate(kARFSize);
    arf_->end_training_phase();
    arf_->print_size();
}

TEST_F (ARFUnitTest, EmptyTest) {
    ASSERT_TRUE(true);
}

TEST_F (ARFUnitTest, QueryTest) {
    loadKeys();
    loadQueries();
    db_ = new Database(keys_);
    arf_ = new ARF(0, kDomain, db_);

    double start_time = getNow();
    arf_->perfect(db_);
    double end_time = getNow();
    std::cout << "Build perfect time: " << (end_time - start_time) << std::endl;

    loadQueries();

    start_time = getNow();
    training();
    end_time = getNow();
    std::cout << "Training time: " << (end_time - start_time) << std::endl;

    for (int i = 0; i < (int)keys_.size(); i++) {
        bool res = arf_->handle_query(keys_[i], keys_[i], true, false);
        ASSERT_TRUE(res);
    }

    int fps = 0;
    int tns = 0;
    start_time = getNow();
    for (int i = 0; i < (int)point_test_queries_.size(); i++) {
        Query::Query_t q = point_test_queries_[i];
        bool dR = db_->rangeQuery(q.left, q.right);
        bool res = arf_->handle_query(q.left, q.right, dR, false);
        if (!dR) {
            tns++;
            if (res)
                fps++;
        }
    }
    end_time = getNow();
    std::cout << "Point Query----------------------" << std::endl;
    std::cout << "Point Query time: " << (end_time - start_time) << std::endl;
    std::cout << "fps = " << fps << std::endl;
    std::cout << "fpr = " << ((fps + 0.0) / tns) << std::endl;


    for (int i = 1; i < (int)keys_.size() - 1; i++) {
        uint64_t left = (keys_[i - 1] + keys_[i]) / 2;
        uint64_t right = (keys_[i] + keys_[i + 1]) / 2;
        bool res = arf_->handle_query(left, right, true, false);
        ASSERT_TRUE(res);
    }

    fps = 0;
    tns = 0;
    start_time = getNow();
    for (int i = 0; i < (int)range_test_queries_.size(); i++) {
        Query::Query_t q = range_test_queries_[i];
        bool dR = db_->rangeQuery(q.left, q.right);
        bool res = arf_->handle_query(q.left, q.right, dR, false);
        if (!dR) {
            tns++;
            if (res)
                fps++;
        }
    }
    end_time = getNow();
    std::cout << "Range Query----------------------" << std::endl;
    std::cout << "Range Query time: " << (end_time - start_time) << std::endl;
    std::cout << "fps = " << fps << std::endl;
    std::cout << "fpr = " << ((fps + 0.0) / tns) << std::endl;
}

int main (int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
