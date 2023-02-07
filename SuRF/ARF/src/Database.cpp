// Andrew
// #include "Database.h"
#include "../include/Database.h"

using namespace arf;

Database::Database() { }

Database::Database(std::vector<uint64> input_keys) {
    for (int i = 0; i < (int)input_keys.size(); i++) {
        keys_.push_back(input_keys[i]);
        tree_[input_keys[i]] = true;
    }
}

int Database::size() {
    return (int)keys_.size();
}

bool Database::rangeQuery(uint64 left, uint64 right) {
    std::map<uint64, bool>::iterator iter;
    iter = tree_.lower_bound(left);
    if (iter == tree_.end())
        return false;
    uint64 key = iter->first;
    return (key <= right);
}

std::vector<Query::Query_t> Database::determineEmptyRanges(Query::Query_t r) {
    std::vector<Query::Query_t> ret;
    int size = (int)keys_.size();
    if (keys_[0] != 0) {
        Query::Query_t q0;
        q0.left = 0;
        q0.right = keys_[0] - 1;
        ret.push_back(q0);
    }
    for (int i = 0; i < size - 1; i++) {
        Query::Query_t q;
        q.left = keys_[i] + 1;
        q.right = keys_[i + 1] - 1;
        ret.push_back(q);
    }
    if (keys_[size - 1] != ULLONG_MAX) {
        Query::Query_t qe;
        qe.left = keys_[size - 1] + 1;
        qe.right = ULLONG_MAX;
        ret.push_back(qe);
    }
    return ret;
}
