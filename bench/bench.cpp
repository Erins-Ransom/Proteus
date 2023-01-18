#include <cassert>
#include <cmath>
#include <cstring>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <random>

#include "../include/proteus.hpp"
#include "../include/modeling.hpp"
#include "../include/util.hpp"
#include "../SuRF/include/surf.hpp"

static size_t surf_hlen = 0, surf_rlen = 0;
static double sample_rate = 0.0;
static bool is_int_bench;
static size_t keylen;
static double bpk;
static bool use_Proteus;
static bool use_SuRF;

inline bool isPointQuery(const uint64_t& a, const uint64_t& b) {
    return b == (a + 1);
}

inline bool isPointQuery(const std::string& a, const std::string& b) {
    return b == a;
}

template<typename T>
void runExperiment(std::vector<T>& keys,
                   std::set<T>& keyset,
                   std::vector<std::pair<T, T>>& queries,
                   std::vector<std::pair<T, T>>& sample_queries,
                   std::vector<std::string>& skeys,
                   std::vector<std::pair<std::string, std::string>>& squeries) {
    
    clock_t begin, end;
    begin = end = clock();
    surf::SuRF* surf = 0;
    proteus::Proteus* proteus = 0;

    if (use_Proteus) {
        begin = clock();
        std::tuple<size_t, size_t, size_t> parameters = proteus::modeling(keys, sample_queries, bpk, keylen);
        end = clock();
        printf("Modeling Time/ms: %lu\n", 1000*(end-begin)/CLOCKS_PER_SEC);
        printf("Trie Depth: %zu; Sparse-Dense Cutoff (bytes): %zu; BF Prefix Length: %zu\n", 
               std::get<0>(parameters), std::get<1>(parameters), std::get<2>(parameters));

        begin = clock();
        proteus = new proteus::Proteus(keys, 
                                       std::get<0>(parameters), // Trie Depth
                                       std::get<1>(parameters), // Sparse-Dense Cutoff
                                       std::get<2>(parameters), // Bloom Filter Prefix Length
                                       bpk); // BPK
        end = clock();

    } else if (use_SuRF) {
        begin = clock();
        if (surf_hlen == 0 && surf_rlen == 0) {
            surf = new surf::SuRF(skeys, true, 64, surf::kNone, surf_hlen, surf_rlen);
        } else if (surf_rlen == 0) {
            surf = new surf::SuRF(skeys, true, 64, surf::kHash, surf_hlen, surf_rlen);
        } else if (surf_hlen == 0) {
            surf = new surf::SuRF(skeys, true, 64, surf::kReal, surf_hlen, surf_rlen);
        } else {
            surf = new surf::SuRF(skeys, true, 64, surf::kMixed, surf_hlen, surf_rlen);
        }
        end = clock();
    } 

    printf("\tus/Insert:\t%lf\n", (double)1000000*(end-begin)/CLOCKS_PER_SEC/keys.size());

    // Test serialization
    if (use_Proteus) {
        auto ser = proteus->serialize();
        delete proteus;
        uint8_t* ser_tofree = ser.first;
        proteus = proteus::Proteus::deSerialize((char*) ser.first);
        delete[] ser_tofree;
    }

    begin = clock();
    size_t empty = 0, fp = 0, fn = 0;
    for (size_t i = 0; i < queries.size(); i++) {
        bool full = false;
        bool filterAns = true;
        
        if (use_Proteus) {
            if (isPointQuery(queries[i].first, queries[i].second)) {
                filterAns = proteus->Query(queries[i].first);
            } else {
                filterAns = proteus->Query(queries[i].first, queries[i].second);
            }
        } else if (use_SuRF) {
            if (isPointQuery(queries[i].first, queries[i].second)) {
                filterAns = surf->lookupKey(squeries[i].first);
            } else {
                filterAns = is_int_bench ? surf->lookupRange(squeries[i].first, true, squeries[i].second, false)
                                         : surf->lookupRange(squeries[i].first, true, squeries[i].second, true);
            }
        }

        auto it = keyset.lower_bound(queries[i].first);
        full = it != keyset.end() &&
               ((is_int_bench && (*it) < queries[i].second)||
                (!is_int_bench && (*it) <= queries[i].second));

        if (!full) {
            ++empty;
            if (filterAns) {
                ++fp;
            }
        }
        if (full && !filterAns) {
            printf("False negative!\n");
            fn++;
        }
    }
    end = clock();
    printf("\ttotal us/Query:\t%lf\n", (double)1000000*(end-begin)/CLOCKS_PER_SEC/queries.size());


    bool res;
    size_t sum;
    begin = clock();
    if (use_Proteus) {
        for (size_t i = 0; i < queries.size(); i++) {
            if (isPointQuery(queries[i].first, queries[i].second)) {
                res = proteus->Query(queries[i].first);
            } else {
                res = proteus->Query(queries[i].first, queries[i].second);
            }
            sum += 1 ? res : 0;
        }
    } else if (use_SuRF) {
        for (size_t i = 0; i < squeries.size(); i++) {
            if (isPointQuery(queries[i].first, queries[i].second)) {
                res = surf->lookupKey(squeries[i].first);
            } else {
                res = is_int_bench ? surf->lookupRange(squeries[i].first, true, squeries[i].second, false)
                                   : surf->lookupRange(squeries[i].first, true, squeries[i].second, true);
            }
            sum += 1 ? res : 0;
        }
    }
    end = clock();
    printf("\tus/Query:\t%lf\n", (double)1000000*(end-begin)/CLOCKS_PER_SEC/queries.size());

    printf("\tFPR:\t%lf\n", (double)fp/empty);
    printf("empty: %zu, fn: %lu, fp: %lu\n", empty, fn, fp);

    if (use_Proteus) {
        auto ser = proteus->serialize();
        printf("\tBPK:\t%lf\n", (double)ser.second*8/keys.size());
        delete[] ser.first;
    } else if (use_SuRF) {
        printf("\tBPK:\t%lf\n", (double)surf->serializedSize()*8/keys.size());
    } 
    
    if (use_Proteus) {
        delete proteus;
    } else if (use_SuRF) {
        delete surf;
    }
}


int main(int argc, char **argv) {
    assert(argc == 5);

    is_int_bench = strtoull(argv[1], nullptr, 10) == 1;
    use_Proteus = (strcmp(argv[2], "Proteus") == 0);
    use_SuRF = (strcmp(argv[2], "SuRF") == 0);

    if (use_Proteus) {
        bpk = strtod(argv[3], nullptr);
        sample_rate = strtod(argv[4], nullptr);
    } else if (use_SuRF) {
        surf_hlen = strtoull(argv[3], nullptr, 10);
        surf_rlen = strtoull(argv[4], nullptr, 10);
    }

    std::string dataPath = "./my_data/";
    std::string keyFilePath = dataPath + "data0.txt";
    std::string lQueryFilePath = dataPath + "txn0.txt";
    std::string uQueryFilePath = dataPath + "upper_bound0.txt";

    if (is_int_bench) {
        keylen = 64;

        std::vector<uint64_t> keys;
        std::vector<std::string> skeys;
        std::set<uint64_t> keyset;
        std::vector<std::pair<uint64_t, uint64_t>> queries;
        std::vector<std::pair<std::string, std::string>> squeries;
        std::vector<std::pair<uint64_t, uint64_t>> sample_queries;

        proteus::intLoadKeys(keyFilePath, keys, skeys, keyset);
        proteus::intLoadQueries(lQueryFilePath, uQueryFilePath, queries, squeries);
        
        if (use_Proteus) {
            sample_queries = proteus::sampleQueries(queries, sample_rate);
        }

        runExperiment(keys, keyset, queries, sample_queries, skeys, squeries);

    } else {
        std::vector<std::string> keys;
        std::set<std::string> keyset;
        std::vector<std::pair<std::string, std::string>> queries;
        std::vector<std::pair<std::string, std::string>> sample_queries;

        keylen = proteus::strLoadKeys(keyFilePath, keys, keyset) * 8;
        proteus::strLoadQueries(lQueryFilePath, uQueryFilePath, queries);
        
        if (use_Proteus) {
            sample_queries = proteus::sampleQueries(queries, sample_rate);
        }

        runExperiment(keys, keyset, queries, sample_queries, keys, queries);
    }
}