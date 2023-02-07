#ifndef DATABASE_H
#define DATABASE_H

#include <climits>
#include <map>
#include <vector>
#include "Query.h"
#include "Util.h"

namespace arf {

class Database {
 public:
    Database();
    Database(std::vector<uint64> input_keys);
    int size();
    bool rangeQuery(uint64 left, uint64 right);
    std::vector<Query::Query_t> determineEmptyRanges(Query::Query_t r);

 private:
    std::vector<uint64> keys_;
    std::map<uint64, bool> tree_;
};

} // namespace arf

#endif // Database_H
