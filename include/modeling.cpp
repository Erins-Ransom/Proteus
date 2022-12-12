#include <vector>
#include <memory>
#include <cassert>
#include <inttypes.h>
#include <cstring>
#include <cmath>
#include <type_traits>
#include <chrono>
#include <iostream>
#include <tuple>
#include <array>

#include "modeling.hpp"
#include "prefixbf.hpp"
#include "config.hpp"

// Print execution times of modeling code snippets
// #define MODEL_TIMING

// Print the results of the modeling
// #define MODEL_PRINT

// Print expected FPRs for all configurations
// #define PRINT_EFPRS

#ifdef MODEL_TIMING  
#define INIT_MODEL_TIMER auto start = std::chrono::high_resolution_clock::now();  
#define START_MODEL_TIMER start = std::chrono::high_resolution_clock::now();  
#define STOP_MODEL_TIMER(name)  std::cout << "RUNTIME of " << name << ": " << \
    std::chrono::duration_cast<std::chrono::microseconds>( \
            std::chrono::high_resolution_clock::now()-start \
    ).count() << " us " << std::endl; 
#else  
#define INIT_MODEL_TIMER  
#define START_MODEL_TIMER  
#define STOP_MODEL_TIMER(name)  
#endif

#ifdef PRINT_EFPRS
#define PRINT_EFPR(i,j,fpr) printf("Trie Depth: %zu;\t Prefix Length: %zu;\t FPR: %0.9Lf\n", i,j,fpr);
#else
#define PRINT_EFPR(i,j,fpr)
#endif

// SAMPLING ASSUMES INPUT OF SORTED AND UNIQUE KEYS.
// SAMPLING ASSUMES INPUT OF SORTED SAMPLE QUERIES (according to left query bound)

// Potential Optimization: 
// Some of the nested arrays have unused space - can look into creating a flat array that represents the sort-of triangular matrix

namespace proteus {

/*
    countUniqueKeyPrefixes counts the unique key prefixes of the given key set for every prefix length.
    The supplied key set is assumed to be in sorted order. 
    The LCP of each consecutive key pair is the prefix length at which a key is unique, so by definition, 
    the key will also be unique at all prefix lengths greater than the LCP.
    Note that the LCP function implicitly pads string keys up to the maximum key length.
    A rolling sum is used to accumulate the values in the array.
    The resulting key_prefixes[i - 1] is the number of unique key prefixes of length i.
    We support LCPs up to the maximum key length in case there are duplicate keys.
*/
template<typename T>
std::vector<size_t> countUniqueKeyPrefixes(std::vector<size_t>& key_prefixes,
                                           const std::vector<T>& keys,
                                           const size_t max_klen) {

    // Increment first counter to account for first key
    // since we won't compare the first key in the loop below 
    key_prefixes[0] = 1;

    int lcp = 0;
    for (size_t i = 1; i < keys.size(); i++) {
        lcp = longestCommonPrefix(keys[i], keys[i-1], max_klen);
        key_prefixes[lcp]++;
    }

    assert(key_prefixes[0] == 1 || key_prefixes[0] == 2);

    for (size_t i = 1; i < key_prefixes.size() - 1; i++) {
        key_prefixes[i] += key_prefixes[i-1];
    }

    return key_prefixes;
}

size_t calcTrieMem(size_t sparse_dense_cutoff, 
                   uint32_t trie_bit_depth,
                   const std::vector<size_t>& dense_mem, 
                   const std::vector<size_t>& sparse_mem) {

    /*
        Calculates the memory cost of the trie given a trie depth (in bits)
        and a LOUDS Sparse / Dense cutoff (in bytes). Each level of 
        the trie (FST) encodes a different byte for all keys, i.e. the
        node fanout is 256.

        `dense_mem` and `sparse_mem` are flattened arrays that store the memory
        cost of each byte level depending on the trie depth. For trie depths that
        are not byte aligned, the number of nodes in the last byte level is smaller 
        due to the restricted fanout. For example, the cost of a LOUDS-Sparse level
        for the 4th byte with a trie depth of 30 is sparse_mem[30]. The memory cost
        of byte-aligned levels are stored in the byte aligned indexes of the arrays.
        To calculate the full cost of the trie, we sum up the cost of each LOUDS-Sparse
        and LOUDS-Dense level according to `sparse_dense_cutoff`
    */
    
    size_t dmem = 0;
    size_t smem = 0;
    
    // Adding the memory cost of dense / sparse byte levels ABOVE the last byte level
    // Hence, we use the corresponding byte-aligned indexes.
    for (size_t i = 0; i < sparse_dense_cutoff; i++) {
        dmem += dense_mem[i];
    }
    for (size_t i = sparse_dense_cutoff; i < div8(trie_bit_depth - 1); i++) {
        smem += sparse_mem[(i + 1) * 8];
    }

    /*
        Adding the memory cost of the last byte level so we index with the trie_bit_depth.
        If the last byte level is LOUDS-Dense, the memory cost is already accounted for 
        by the dense nodes at the previous level. See SuRF paper for more details!

        EXAMPLE
        ------------------------------
        Trie Depth: 14 (2 byte levels)
        Sparse-Dense Cutoff = 0 (All Sparse)
        Sparse-Dense Cutoff = 1 (1 Dense, 1 Sparse)
        Sparse-Dense Cutoff = 2 (All Dense)

        div8(trie_bit_depth - 1) + 1 = number of trie byte levels
        If condition below is true, then the last trie level must be LOUDS-Sparse.
    */
    if (sparse_dense_cutoff < div8(trie_bit_depth - 1) + 1) {
        smem += sparse_mem[trie_bit_depth];
    }

    dmem += (dmem / 512 + 1) * sizeof(uint32_t);                    // 2 Rank LUTs
    
    size_t lutsmem = ((smem / 10) / 512 + 1) * sizeof(uint32_t);    // 1 Rank LUT
    lutsmem += ((smem / 10) / 64 + 1) * sizeof(uint32_t);           // 1 Select LUT (overestimate, using num_bits_ vs num_ones_)

    return dmem + smem + lutsmem;
}

/*
    Doesn't account for (small number of) alignment bits added for serialization (underestimate).
    Assumes that all key bytes are encoded with LOUDS-DS nodes but they may actually be 
    encoded more efficiently with suffix bits if the key has a short common prefix (overestimate).
*/
size_t calcMemDist(std::vector<long double>& bf_mem,
                   std::vector<size_t>& sd_cutoffs,
                   const std::vector<size_t>& key_prefixes,
                   double bits_per_key) {
    
    // In Bits
    uint32_t DENSE_NODE_SIZE = 256 * 2;   // No D-IsPrefixKey
    uint32_t SPARSE_NODE_SIZE = 8 + 2;
    uint32_t TRIE_DEPTHS = bf_mem.size();

    // Stores the size of each trie byte level if it is encoded as LOUDS-Dense
    // Note that the size of a LOUDS-Dense node is independent of the number of children (i.e. it is constant)
    std::vector<size_t> dense_mem(div8(TRIE_DEPTHS + 7), 0);

    // Stores the size of every possible LOUDS-Sparse trie level for different trie prefix lengths
    // The size of a LOUDS-Sparse node is dependent on the number of children
    std::vector<size_t> sparse_mem(TRIE_DEPTHS, 0);

    dense_mem[0] = 1 * DENSE_NODE_SIZE;
    for (size_t i = 1; i < dense_mem.size(); i++) {
        // Get key prefixes for previous byte. For the 2nd byte level, 
        // we want the number of unique prefixes of length 1 byte / 8 bits
        // which is given by key_prefixes[7] 
        dense_mem[i] = key_prefixes[i * 8 - 1] * DENSE_NODE_SIZE;
    }

    for (size_t i = 1; i < sparse_mem.size(); i++) {
        sparse_mem[i] = key_prefixes[i - 1] * SPARSE_NODE_SIZE;
    }

    std::vector<size_t> trie_mem(TRIE_DEPTHS, SIZE_MAX);
    trie_mem[0] = 0;
    for (size_t trie_bit_depth = 1; 
                trie_bit_depth < TRIE_DEPTHS; 
                trie_bit_depth++) {
        // The Sparse-Dense Cutoff ranges from 0 to n (the number of trie levels)
        // where 0 represents ALL LOUDS-Sparse and n represents ALL LOUDS-Dense.
        for (size_t sparse_dense_cutoff = 0; 
                    sparse_dense_cutoff <= div8(trie_bit_depth - 1) + 1; 
                    sparse_dense_cutoff++) {
            size_t mem = calcTrieMem(sparse_dense_cutoff, trie_bit_depth, dense_mem, sparse_mem);
            if (mem < trie_mem[trie_bit_depth]) {
                trie_mem[trie_bit_depth] = mem;
                sd_cutoffs[trie_bit_depth] = sparse_dense_cutoff;
            }
        }

        // Account for trie metadata bits
        // Does not account for alignment bits when serializing
        
        // Proteus Metadata: 2 * sizeof(uint32_t)
        // Louds-Dense Metadata: sizeof(uint32_t)
        // Louds-Sparse Metadata: 4 * sizeof(uint32_t)
        // BitVectorRank Metadata (2 LD 1 LS): 2 * sizeof(uint32_t) each
        // BitVectorSelect Metadata (1 LS): 3 * sizeof(uint32_t)
        // LabelVector Metadata (1 LS): 1 * sizeof(uint32_t)
        // BitVectorSuffix Metadata (1 LD 1 LS): 6 * sizeof(uint32_t) + trie_levels * sizeof(uint32_t) total
        trie_mem[trie_bit_depth] += (23 + (div8(trie_bit_depth - 1) + 1)) * sizeof(uint32_t) * 8;
    }

    size_t max_trie_depth = TRIE_DEPTHS - 1; // TRIE_DEPTHS includes the "no trie" option, i.e. depth 0
    size_t total_bits = bits_per_key * key_prefixes[TRIE_DEPTHS - 2]; // ignore last element in key_prefixes so -2 to get correct index
    for (size_t i = 0; i < TRIE_DEPTHS; i++) {
        if (trie_mem[i] <= total_bits) {
            bf_mem[i] = total_bits - trie_mem[i];
        } else {
            max_trie_depth = i;
            break;
        }
    }

    return max_trie_depth;
}

std::pair<int, int> KQlcp(const std::vector<uint64_t>& keys,
                          const size_t max_klen,
                          std::vector<uint64_t>::const_iterator& kstart,
                          uint64_t qleft, 
                          uint64_t qright) {
    /*                                       
        THIS FUNCTION RETURNS THE BIT LENGTH OF THE LONGEST COMMON PREFIX
        BETWEEN THE QUERY & THE KEY SET.
        
        Legend:
        QLeft - Left Query Bound
        QRight - Right Query Bound
        KLeft - Largest key that is < QLeft
        KRight - Smallest key that is >= QRight
        KMin - Smallest Key
        KMax - Largest Key
        KStart - Smallest Key that is >= QLeft 
    */

    // QLeft <= KStart
    kstart = lower_bound(kstart, keys.cend(), qleft);

    // Negative query should yield: QLeft < QRight <= KStart
    // Assume that QLeft is inclusive and QRight is exclusive
    if (kstart != keys.cend() && *kstart < qright) {
        // Positive Query
        // QLeft <= KStart < QRight
        return std::make_pair(-1, -1);
    }

    // Derive KLeft by taking the key before KStart
    // KLeft < QLeft < QRight <= KStart = KRight
    auto kleft = (kstart != keys.cbegin()) ? kstart - 1 : kstart;
    auto kright = kstart;

    if (qleft >= *kleft && kright != keys.cend() && qright <= *kright) {
        // KLeft QLeft QRight KRight
        return std::make_pair(longestCommonPrefix(qleft, *kleft, max_klen), 
                              longestCommonPrefix(qright - 1, *kright, max_klen));
    } else if (qleft < *kleft && qright <= *kleft) {
        // QLeft QRight KMin
        return std::make_pair(-1, longestCommonPrefix(qright - 1, *kleft, max_klen));
    } else if (kright == keys.cend() && keys.back() < qleft) {
        // KMax QLeft QRight
        return std::make_pair(longestCommonPrefix(qleft, keys.back(), max_klen), -1);
    } else {
        assert(false);
    }

    // Should not reach here!
    return std::make_pair(-1, -1);
}

/*
    In the string version of KQlcp, we don't need to do qright -1 as we define
    the right query bound to be inclusive for string range queries. Note that
    it is exclusive for integer range queries.
*/
std::pair<int, int> KQlcp(const std::vector<std::string>& keys,
                          const size_t max_klen,
                          std::vector<std::string>::const_iterator& kstart,
                          std::string qleft, 
                          std::string qright) {

    kstart = lower_bound(kstart, keys.cend(), qleft);
    if (kstart != keys.cend() && *kstart < qright) {
        return std::make_pair(-1, -1);
    }

    auto kleft = (kstart != keys.cbegin()) ? kstart - 1 : kstart;
    auto kright = kstart;

    if (qleft >= *kleft && kright != keys.cend() && qright <= *kright) {
        return std::make_pair(longestCommonPrefix(qleft, *kleft, max_klen), 
                              longestCommonPrefix(qright, *kright, max_klen));
    } else if (qleft < *kleft && qright <= *kleft) {
        return std::make_pair(-1, longestCommonPrefix(qright, *kleft, max_klen));
    } else if (kright == keys.cend() && keys.back() < qleft) {
        return std::make_pair(longestCommonPrefix(qleft, keys.back(), max_klen), -1);
    } else {
        assert(false);
    }

    return std::make_pair(-1, -1);
}


template<typename T>
std::tuple<size_t, size_t, size_t, long double, size_t, size_t> find_best_conf(const std::vector<size_t>& trconfs,
                                                                                const std::vector<size_t>& bfconfs,
                                                                                const std::vector<T>& keys, 
                                                                                const std::vector<std::pair<T, T>>& sample_queries,
                                                                                const std::vector<size_t>& key_prefixes,
                                                                                const std::vector<long double>& bf_mem,
                                                                                const size_t max_klen) {
    
    std::vector<size_t> resolved_in_trie(trconfs.size(), 0);
    std::vector<std::vector<bin_array>> conf_counters(trconfs.size(), std::vector<bin_array>(bfconfs.size(), bin_array()));
    std::vector<size_t> pq_cache(bfconfs.size(), 0);

    size_t empty_queries = 0;
    typename std::vector<T>::const_iterator kstart = keys.cbegin();
    
    for (auto const & q : sample_queries) {
        std::pair<int, int> lcps = KQlcp(keys, max_klen, kstart, q.first, q.second);
        if (lcps.first < 0 && lcps.second < 0) {
            continue;
        } else {
            empty_queries++;
        }
        
        // Smallest prefix length where all the query prefixes and key prefixes are distinct
        size_t min_resolvable_len = std::max(lcps.first, lcps.second) + 1;

        // Longest common prefix of the query
        int Qlcp = longestCommonPrefix(q.first, q.second, max_klen);

        // Last trie configuration where the query is not (fully) resolvable in the trie
        auto last_trit = trconfs.back() < min_resolvable_len 
                         ? trconfs.end() 
                         : lower_bound(trconfs.begin(), trconfs.end(), min_resolvable_len);

        // Pre-calculate and cache the number of query prefixes for each bloom filter configuration
        // assuming the entire query is executed in the bloom filter.
        if (trconfs[0] == 0 || trconfs.front() <= static_cast<size_t>(Qlcp)) {
            size_t min_prefix_len = trconfs[0] == 0 ? min_resolvable_len : std::max(trconfs.front() + 1, min_resolvable_len);
            for (auto bfit = lower_bound(bfconfs.begin(), bfconfs.end(), min_prefix_len); 
                      bfit != bfconfs.end(); 
                      ++bfit) {

                pq_cache[bfit - bfconfs.begin()] = count_prefixes(q.first, q.second, *bfit);
            }
        }

        // Trie Depth = 0
        if (trconfs[0] == 0) {
            // Count Bloom filter prefix queries for Proteus configurations with no trie and only a prefix Bloom Filter
            for (auto bfit = lower_bound(bfconfs.begin(), bfconfs.end(), min_resolvable_len); 
                      bfit != bfconfs.end(); 
                      ++bfit) {

                size_t bfconf_idx = bfit - bfconfs.begin();
                size_t prefix_queries = pq_cache[bfconf_idx];

                // count_prefixes returns 0 if the result is bigger than a uint64_t - treat as guaranteed false positive
                if (prefix_queries != 0) {
                    conf_counters[0][bfconf_idx][__builtin_clzll(prefix_queries)].first += prefix_queries;
                    conf_counters[0][bfconf_idx][__builtin_clzll(prefix_queries)].second++;
                }            
            }
        }

        /*
            Iterate over Proteus configurations where the query is not (fully) resolvable in the trie
            but is resolvable in the Bloom filter. For each such configuration, we count the required 
            Bloom filter prefix queries which correspond to the portions of the query range that have a
            matching prefix in the trie. 
        */
        for (auto trit = trconfs[0] == 0 ? std::next(trconfs.begin()) : trconfs.begin();
                  trit != last_trit;
                  ++trit) {
                
            size_t trconf_idx = trit - trconfs.begin();

            for (auto bfit = lower_bound(bfconfs.begin(), bfconfs.end(), std::max(*trit + 1, min_resolvable_len));
                      bfit != bfconfs.end(); 
                      ++bfit) {
                
                size_t bfconf_idx = bfit - bfconfs.begin();

                /*
                    Empty queries, by definition, overlap with at most 2 prefixes in the trie.
                    Here, we consider cases where the query overlaps with 1 or 2 trie prefixes.
                    For these configurations, the query must match at least 1 trie prefix as the
                    trie depth is below the minimum resolvable length.
                */
                
                if (*trit > static_cast<size_t>(Qlcp)) {
                    /*
                        At the trie depth of this configuration, the query spans multiple prefixes.
                        Since the query is empty, only the extreme prefixes can overlap with the
                        trie. Either or both may overlap the trie.

                        -------------------------------------------
                        EXAMPLES
                        -------------------------------------------
                        Trie Depth: 3, BF Prefix Length: 5
                        Assume 0xACB and 0xCBD are the two closest prefixes in the trie to the given queries.
                        
                        * represents the Bloom Filter Prefix Queries

                        Query: [0xACB|AB..., 0xCBD|CC...] (overlaps with 2 trie prefixes)
                        Bloom Filter Prefix Queries: [0xACB|AB, 0xACB|FF], [0xCBD|00, 0xCBD|CC]

                        NOTE: All queries in the range [0xACC|00, 0xCBC|FF] do not need to be queried 
                            in the BF as they have already been ruled out by the trie.  

                                Trie Prefix 1: 0xACB                                     Trie Prefix 2: 0xCBD
                        
                        [0xACB00          0xACBAB  0xACBFF] <-- NO KEY PREFIXES --> [0xCBD00      0xCBDCC  0xCBDFF]
                                          *****************                         *********************


                        Query: [0xACB|AB..., 0xAD3|2A...] (overlaps with 1 trie prefix)
                        Bloom Filter Prefix Queries: [0xACB|AB, 0xACB|FF]

                        [0xACB00          0xACBAB  0xACBFF]
                                          ***************** 

                
                        Query: [0xCBA|AB..., 0xCBD|CC...] (overlaps with 1 trie prefix)
                        Bloom Filter Prefix Queries: [0xCBD|00, 0xCBD|CC]

                                                                                    [0xCBD00      0xCBDCC  0xCBDFF]
                                                                                    *********************
                        
                        0xACBFF -> max_left_prefix
                        0xCBD00 -> min_right_prefix
                    */

                    size_t bf_prefix_queries = 0;
                    // Left
                    if (*trit < static_cast<size_t>(lcps.first + 1)) {
                        T max_left_prefix = editKey(q.second, *trit, false);

                        // count_prefixes returns 0 if the result is bigger than a uint64_t - treat as guaranteed false positive
                        size_t p = count_prefixes(q.first, max_left_prefix, *bfit);
                        if (p == 0) {
                            continue;
                        } else {
                            bf_prefix_queries += p;
                        }
                    }

                    // Right
                    if (*trit < static_cast<size_t>(lcps.second + 1)) {
                        T min_right_prefix = editKey(q.second, *trit, true);

                        // count_prefixes returns 0 if the result is bigger than a uint64_t - treat as guaranteed false positive
                        size_t p = count_prefixes(min_right_prefix, q.second, *bfit);
                        if (p == 0) {
                            continue;
                        } else {
                            bf_prefix_queries += p;
                        }
                    }

                    conf_counters[trconf_idx][bfconf_idx][__builtin_clzll(bf_prefix_queries)].first += bf_prefix_queries;
                    conf_counters[trconf_idx][bfconf_idx][__builtin_clzll(bf_prefix_queries)].second++;
                } else {
                    /*
                        Entire query is contained within a single trie prefix at this trie depth 
                        and so the trie is of no benefit. Thus, the entire query is resolved in 
                        the Bloom filter, similar to configurations with no trie.

                        -------------------------------------------
                        EXAMPLE
                        -------------------------------------------
                        Trie Depth: 3, BF Prefix Length: 5
                        Query: [0xACB|AB..., 0xACB|CC...]

                        Trie Prefix: 0xACB
                        
                        * represents the BF Prefix Queries
                    
                        [0xACB00        0xACBAB      0xACBCC    0xACBFF] 
                                        ********************
                    */
                    size_t prefix_queries = pq_cache[bfconf_idx];
                    if (prefix_queries != 0) {
                        conf_counters[trconf_idx][bfconf_idx][__builtin_clzll(prefix_queries)].first += prefix_queries;
                        conf_counters[trconf_idx][bfconf_idx][__builtin_clzll(prefix_queries)].second++;
                    }
                }
            }
        }

        /*
            Marking the configurations for which this query is fully resolvable in trie
        */
        for (auto trit = lower_bound(trconfs.begin(), trconfs.end(), min_resolvable_len);
                  trit != trconfs.end();
                  ++trit) {
            resolved_in_trie[trit - trconfs.begin()] += 1;
        }
    }

    if (empty_queries == 0) {
        return std::make_tuple(0, 0, 0, 0, 0, 0);
    }

    size_t best_trie_depth = 0;
    size_t best_bflen = 0;
    long double best_efpr = 1.0L;
    size_t best_trconf = 0;
    int32_t best_bfconf = 0;

    for (auto trit = trconfs.begin(); trit != trconfs.end(); ++trit) {
        size_t i = *trit;
        size_t trconf_idx = trit - trconfs.begin();

        // Modeling FPR with no Prefix Bloom Filter (only trie)
        long double trie_efpr = ((empty_queries - resolved_in_trie[trconf_idx]) * 1.0L) / (empty_queries * 1.0L);
        if (trie_efpr <= best_efpr) {
            best_trie_depth = i; /* trie depth = length (in bits) of the prefixes in trie.*/
            best_bflen = 0;
            best_efpr = trie_efpr;
            best_trconf = trconf_idx;
            best_bfconf = -1;
            if ((empty_queries - resolved_in_trie[trconf_idx]) == 0UL) {
                // If the trie is awesome, then this is a
                // heuristic to still include a PBF for robustness:
                best_bflen = (i + max_klen) / 2;
                PRINT_EFPR(i, best_bflen, best_efpr)
                continue;
            }
        }

        PRINT_EFPR(i, 0UL, trie_efpr)

        // Bloom Prefix Length is always at least one more than the Trie Depth
        for (auto bfit = lower_bound(bfconfs.begin(), bfconfs.end(), i + 1); 
                  bfit != bfconfs.end(); 
                  ++bfit) {
            
            size_t j = *bfit;
            size_t bfconf_idx = bfit - bfconfs.begin();

            // Determine the Bloom filter modeling parameters
            size_t n = key_prefixes[j - 1];
            size_t nhf = static_cast<size_t>(round(M_LN2 * bf_mem[i] / n));
            nhf = (nhf == 0 ? 1 : nhf);
            nhf = std::min(static_cast<size_t>(MAX_PBF_HASH_FUNCS), nhf);
            long double prefix_query_fpr = pow((1.0L - exp(-((nhf * n * 1.0L) / bf_mem[i]))), nhf);
            
            // Sum up false positives probabilities for queries resolved in the Bloom Filter
            long double cumulative_fpp = 0.0L;
            size_t resolved_in_bf = 0;
            for (const auto & bin : conf_counters[trconf_idx][bfconf_idx]) {
                if (bin.second > 0) {
                    resolved_in_bf += bin.second /* sample query count */;
                    cumulative_fpp += bin.second * (1.0L - pow((1.0L - prefix_query_fpr), ((bin.first * 1.0L) / bin.second) /* average number of BF prefix queries */));
                }
            }

            // Add false positive probability for guaranteed false positives
            cumulative_fpp += 1.0L * (empty_queries - resolved_in_bf - resolved_in_trie[trconf_idx]);          

            long double efpr = cumulative_fpp / (empty_queries * 1.0L);

            PRINT_EFPR(i, j, efpr)

            if (efpr <= best_efpr) {
                best_trie_depth = i;
                best_bflen = j;
                best_efpr = efpr;
                best_trconf = trconf_idx;
                best_bfconf = bfconf_idx;
            }
        }
    }

    return std::make_tuple(empty_queries, best_trie_depth, best_bflen, best_efpr, best_trconf, best_bfconf);
}

template<typename T>
std::tuple<size_t, size_t, size_t> modeling(const std::vector<T>& keys, 
                                            const std::vector<std::pair<T, T>>& sample_queries, 
                                            const double bits_per_key,
                                            const size_t max_klen, 
                                            std::vector<size_t>* sparse_dense_cutoffs) {
    INIT_MODEL_TIMER

    assert(max_klen > 0);

    // Number of unique key prefixes for every prefix length
    std::vector<size_t> key_prefixes(max_klen + 1, 0);

    // Bloom filter memory available for every trie depth
    std::vector<long double> bf_mem(max_klen + 1, 0.0L);

    // Best sparse-dense cutoff for every trie depth
    std::vector<size_t> sd_cutoffs(max_klen + 1, 0);

    START_MODEL_TIMER
    countUniqueKeyPrefixes(key_prefixes, keys, max_klen);
    STOP_MODEL_TIMER("Count Unique Key Prefixes")
    
    START_MODEL_TIMER
    size_t max_trie_depth = calcMemDist(bf_mem, sd_cutoffs, key_prefixes, bits_per_key);
    STOP_MODEL_TIMER("Calculate Memory Distribution")

    #ifdef PRINT_EFPRS
        if (sparse_dense_cutoffs) {
            *sparse_dense_cutoffs = sd_cutoffs;
        }
    #else
        (void) sparse_dense_cutoffs;
    #endif

    // If there is enough memory for a full trie, just use it
    if (max_trie_depth == max_klen) {
        printf("Proteus Used Full Trie.\n");
        return std::make_tuple(max_klen, sd_cutoffs[max_klen], 0);
    }

    /*
        For each sample query, we extract the LCP of the query with the keyset and 
        calculate the number of Bloom filter prefix queries for each possible Proteus configuration
        for sample queries that are not guaranteed false positives due to matching prefixes.

        For each Proteus configuration, we partition the non-guaranteed false positive sample queries 
        according to the number of Bloom filter prefix queries that they require to be resolved.

        Some sample queries can be resolved in the trie and thus incur no Bloom filter prefix queries.
        We keep an array of counters for such queries for every valid trie depth (`resolved_in_trie`).

        For sample queries that cannot be resolved in the trie, we keep an array of 64 bins for every
        possible Proteus configuration with a Bloom filter (`conf_counters`). We assign each sample query to a bin based
        on the number of BF prefix queries it requires. The bin at index i takes sample queries where the
        number of BF prefix queries is in the range [2^(64-i - 1) - 1, 2^(64-i) - 1].
        
        In each bin, we keep:

            - The number of sample queries that land in the bin
            - A cumulative sum of the number of BF prefix queries

        We use both counters to determine the average number of BF prefix queries within each bin range.
        The FP probability of a given sample query asymptotically approaches 1 as the number of BF prefix queries 
        increases; therefore, despite the exponentially increasing widths of the bin range, sample queries 
        in the same bin will still have similar expected false positive probabilities.  

        The remaining sample queries are guaranteed false-positives due to matching prefixes.

        For string keys, we set a user-defined maximum (default 128) on the number of Bloom filter prefix lengths that are modeled.
        This is because the number of Proteus  configurations increases quadratically with the max key length 
        and thus this bounds the modeling time for string keys. All possible trie depths are modeled as this 
        is already bounded by the memory cost of the trie. 
    */

    /*
        Iterate over all chosen Proteus configurations within the memory budget and 
        calculate the expected FPR of each configuration by taking the average 
        false postive probabilities of the sample queries.
    */
    START_MODEL_TIMER
    
    std::vector<size_t> bfconfs;
    std::vector<size_t> trconfs;    

    if (std::is_same<T, std::string>::value) {
        trconfs.reserve(64);
        bfconfs.reserve(64);
        size_t trstep = 1 + ((max_trie_depth + 1 - 1) / 64);
        size_t bfstep = 1 + ((max_klen - 1) / 64);
        for (size_t i = 0; i <= max_trie_depth + 1; i += trstep) {
            trconfs.push_back(i);
        }
        for (size_t i = 1; i <= max_klen; i += bfstep) {
            bfconfs.push_back(i);
        }
    } else if (std::is_same<T, uint64_t>::value) {
        trconfs.resize(max_trie_depth + 1);
        bfconfs.resize(64);
        std::iota(trconfs.begin(), trconfs.end(), 0);
        std::iota(bfconfs.begin(), bfconfs.end(), 1);
    } else {
        assert(false);
    }

    std::tuple<size_t, size_t, size_t, long double, size_t, size_t> best_conf = find_best_conf(trconfs,
                                                                                               bfconfs, 
                                                                                               keys, 
                                                                                               sample_queries,
                                                                                               key_prefixes,
                                                                                               bf_mem, 
                                                                                               max_klen);

    STOP_MODEL_TIMER("Find Best Configuration 1")

    #ifdef MODEL_PRINT
    printf("Query Sample Percent Empty Queries: %lf%%\n", ((std::get<0>(best_conf) * 1.0) / sample_queries.size()) * 100.0);
    #endif

    // Default configuration for no valid sample queries 
    // is no trie and prefix filter with prefix length 
    // that is half the maximum key length.
    if (std::get<0>(best_conf) == 0) {
        printf("Proteus Used Default Configuration.\n");
        return std::make_tuple(0, 0, max_klen / 2);
    }

    START_MODEL_TIMER

    if ((std::is_same<T, std::string>::value && max_klen > 64) && std::get<5>(best_conf) > 0) {
        size_t trstart = std::get<4>(best_conf) == 0 ? trconfs[0] : trconfs[std::get<4>(best_conf) - 1] + 1;
        size_t trend = std::get<4>(best_conf) == (trconfs.size() - 1) ? max_trie_depth + 1 : trconfs[std::get<4>(best_conf) + 1] - 1;
        size_t bfstart = std::get<5>(best_conf) == 0 ? bfconfs[0] : bfconfs[std::get<5>(best_conf) - 1] + 1;
        size_t bfend = std::get<5>(best_conf) == (bfconfs.size() - 1) ? max_klen : bfconfs[std::get<5>(best_conf) + 1] - 1;
        
        size_t ntrconfs = 0;
        size_t nbfconfs = 0;

        if (trend > trstart) {
            size_t trstep = 1 + (((trend - trstart) - 1) / 64);
            for (size_t i = trstart; i < trend; i += trstep) {
                trconfs[ntrconfs] = i;
                ntrconfs++;
            }
            #ifdef MODEL_PRINT
            printf("TRStart: %zu, TREnd: %zu, TRStep: %zu\n", trstart, trend, trstep);
            #endif
        } else {
            trconfs[ntrconfs] = trstart;
            ntrconfs++;
            #ifdef MODEL_PRINT
            printf("TRConf: %zu\n", trstart);
            #endif
        }

        if (bfend > bfstart) {
            size_t bfstep = 1 + (((bfend - bfstart) - 1) / 64);
            for (size_t i = bfstart; i < bfend; i += bfstep) {
                bfconfs[nbfconfs] = i;
                nbfconfs++;
            }
            #ifdef MODEL_PRINT
            printf("BFStart: %zu, BFEnd: %zu, BFStep: %zu\n", bfstart, bfend, bfstep);
            #endif
        } else {
            bfconfs[nbfconfs] = bfstart;
            nbfconfs++;
            #ifdef MODEL_PRINT
            printf("BFConf: %zu\n", bfstart);
            #endif
        }

        trconfs.resize(ntrconfs);
        bfconfs.resize(nbfconfs);

        if (ntrconfs > 1 || nbfconfs > 1) {
            
            std::tuple<size_t, size_t, size_t, long double, size_t, size_t> best_conf2 = find_best_conf(trconfs,
                                                                                                        bfconfs, 
                                                                                                        keys, 
                                                                                                        sample_queries,
                                                                                                        key_prefixes,
                                                                                                        bf_mem, 
                                                                                                        max_klen);
            if (std::get<3>(best_conf2) < std::get<3>(best_conf)) {
                best_conf = best_conf2;
            }
        }
    }

    STOP_MODEL_TIMER("Find Best Configuration 2")

    #ifdef MODEL_PRINT
    printf("Best eFPR of %0.9Lf at Trie Depth %zu with Sparse-Dense Cutoff %zu and BF Prefix Len %zu\n", 
           std::get<3>(best_conf), std::get<1>(best_conf), sd_cutoffs[std::get<1>(best_conf)], std::get<2>(best_conf));
    printf("%% Memory Allocated to Prefix Filter: %0.9Lf\n", (bf_mem[std::get<1>(best_conf)] / (bits_per_key * keys.size())) * 100);
    #endif

    return std::make_tuple(std::get<1>(best_conf), sd_cutoffs[std::get<1>(best_conf)], std::get<2>(best_conf));

}

template std::tuple<size_t, size_t, size_t> modeling(const std::vector<uint64_t>& keys, 
                                                     const std::vector<std::pair<uint64_t, uint64_t>>& sample_queries, 
                                                     const double bits_per_key,
                                                     const size_t max_klen, 
                                                     std::vector<size_t>* sparse_dense_cutoffs = nullptr);

template std::tuple<size_t, size_t, size_t> modeling(const std::vector<std::string>& keys, 
                                                     const std::vector<std::pair<std::string, std::string>>& sample_queries, 
                                                     const double bits_per_key,
                                                     const size_t max_klen, 
                                                     std::vector<size_t>* sparse_dense_cutoffs = nullptr);

} // namespace proteus