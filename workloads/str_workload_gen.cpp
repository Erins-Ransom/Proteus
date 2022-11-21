#include <assert.h> 
#include <functional>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iterator>
#include <unordered_map>
#include <random>
#include <cstdlib>
#include <sys/stat.h>
#include <chrono>
#include <algorithm>
#include <experimental/filesystem> // C++14 compatible

static size_t DOMAIN_OFFSET = 0;

enum kdist_type {
    kuniform, knormal, kdomain
};

enum qdist_type {
    quniform, qcorrelated, qsplit, qdomain
};

static std::vector<std::string> kdist_names = {"kuniform", "knormal", "kdomain"};
static std::vector<std::string> qdist_names = {"quniform", "qcorrelated", "qsplit", "qdomain"};
static std::unordered_map<std::string, kdist_type> str_to_kdist = {{"kuniform", kuniform}, {"knormal", knormal}, {"kdomain", kdomain}};
static std::unordered_map<std::string, qdist_type> str_to_qdist = {{"quniform", quniform}, {"qcorrelated", qcorrelated}, {"qsplit", qsplit}, {"qdomain", qdomain}};

std::string lex_arithmetic(const std::string& s, const long long delta) {
    std::string ss = s;
    size_t idx = s.length();
    unsigned long long carry = llabs(delta);
    if (delta >= 0) {
        unsigned long long res = 0;
        while ((idx > 0) && (carry > 0)) {
            idx--;
            res = ((uint8_t) s[idx]) + carry;
            ss[idx] = (uint8_t) (res % (((unsigned long long) UINT8_MAX) + 1));
            carry = res / (((unsigned long long) UINT8_MAX) + 1);
        }
    } else {
        while ((idx > 0) && (carry > 0)) {
            idx--;
            uint8_t carry_byte = (uint8_t) (carry % (((unsigned long long) UINT8_MAX) + 1));
            if (((uint8_t) s[idx]) >= carry_byte) {
                ss[idx] = ((uint8_t) s[idx]) - carry_byte;
                carry /= (((unsigned long long) UINT8_MAX) + 1);
            } else {
                ss[idx] = UINT8_MAX - (carry_byte - ((uint8_t) s[idx]));
                carry /= (((unsigned long long) UINT8_MAX) + 1);
                carry += 1;
            }
        }
    }
    
    // return empty string if there is overflow/underflow
    if (carry > 0) {
        return std::string();
    }

    return ss;
}

std::vector<std::string> generateKeysNormal(unsigned long long klen, unsigned long long nkeys) {
    std::vector<std::string> keys;
    keys.reserve(nkeys);

    long double standard_deviation = std::pow(2.0L, 64) * 0.01L;
    std::normal_distribution<long double> norm_dist = std::normal_distribution<long double>(0, standard_deviation);
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine generator(seed);

    std::string mean_key;
    size_t kbytelen = klen / 8;
    mean_key.resize(kbytelen);
    mean_key[0] = (uint8_t) 128;
    for (size_t i = 1; i < kbytelen; i++) {
        mean_key[i] = '\0';
    }

    while (keys.size() < nkeys) {
        long long offset = (long long) norm_dist(generator);
        std::string key = lex_arithmetic(mean_key, offset);
        if (key.length() > 0) {
            keys.push_back(key);
        }
    }

    return keys;
}

std::vector<std::string> generateKeysUniform(unsigned long long klen, unsigned long long nkeys) {
    std::vector<std::string> keys;
    keys.reserve(nkeys);

    std::random_device rd;  // Will be used to obtain a seed for the random number engine
    std::mt19937_64 gen(rd()); // Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<uint8_t> uni_dist(0, UINT8_MAX);

    std::string key;
    size_t kbytelen = klen / 8;
    key.resize(kbytelen);
    while (keys.size() < nkeys) {
        for (size_t i = 0; i < kbytelen; i++) {
            key[i] = uni_dist(gen);
        }
        keys.push_back(key);
    }

    return keys;
}

std::vector<std::string> getDomainKeys(unsigned long long nkeys, const std::string filename) {  
    assert(std::experimental::filesystem::exists(filename));
    std::ifstream in(filename);
    if (!in.is_open()) {
        std::cerr << "unable to open " << filename << std::endl;
        exit(EXIT_FAILURE);
    }

    in.seekg(DOMAIN_OFFSET, in.beg);

    std::vector<std::string> keys;
    keys.resize(nkeys);

    std::string line;
    for (size_t i=0; i<nkeys && in.peek() != EOF; i++) {
        getline(in, line);
        keys[i] = line;
    }

    // Update global file index
    DOMAIN_OFFSET = in.tellg();
    in.close();

    return keys;
}

std::vector<std::string> generateKeys(unsigned long long klen, unsigned long long nkeys, kdist_type kdist, const std::string domains_path) {
    if (kdist == kuniform) {
        return generateKeysUniform(klen, nkeys);
    } else if (kdist == knormal) {
        return generateKeysNormal(klen, nkeys);
    } else if (kdist == kdomain) {
        return getDomainKeys(nkeys, domains_path);
    } else {
        assert(false);
    }
}

std::vector<std::pair<std::string, std::string>> generateRangeQueries(unsigned long long klen, unsigned long long nqueries, 
                                                                      unsigned long long min_range, unsigned long long max_range, 
                                                                      double pqratio, const std::vector<std::string>& keys, 
                                                                      qdist_type qdist, unsigned long long correlation_degree, 
                                                                      double pnratio, const std::string domains_path, size_t max_klen) {
    std::vector<std::pair<std::string, std::string>> queries;
    std::vector<std::string> range_lefts;
    std::vector<std::string> range_lefts1;
    std::vector<std::string> range_lefts2;

    assert(qdist == quniform || qdist == qcorrelated || qdist == qsplit || qdist == qdomain);
    if (qdist == qcorrelated) {
        range_lefts = keys;
    } else if (qdist == quniform) {
        range_lefts = generateKeysUniform(klen, nqueries);
    } else if (qdist == qsplit) {
        range_lefts1 = keys;
        range_lefts2 = generateKeysUniform(klen, nqueries / 2);
    } else if (qdist == qdomain) {
        range_lefts = getDomainKeys(nqueries, domains_path);
    }

    uint64_t range_size;
    std::string left, right;
    uint64_t index = range_lefts.size() - 1;
    srand(time(NULL));

    if (qdist == qsplit) {
        uint64_t index1 = range_lefts1.size() - 1;
        uint64_t index2 = range_lefts2.size() - 1;

        std::random_device rd1;
        std::mt19937 gen1(rd1());
        std::uniform_int_distribution<uint64_t> uni_dist1(0, index1);

        std::random_device rd2;
        std::mt19937 gen2(rd2());
        std::uniform_int_distribution<uint64_t> uni_dist2(0, index2);

        double pqratio_corr = pqratio <= 0.5 ? pqratio * 2.0 : 1.0;
        double pqratio_unif = pqratio <= 0.5 ? 0.0 : (pqratio - 0.5) * 2.0;
        
        // Correlated Queries
        std::random_device rd_corr;
        std::mt19937 gen_corr(rd_corr());
        std::uniform_int_distribution<uint64_t> corr_dist(1, correlation_degree);

        for (uint64_t i = 0; i < nqueries / 2; i++) {
            // Generate range query size
            double randdbl = uni_dist1(gen1) * 1.0 / (range_lefts1.size() - 1);
            if (randdbl < pqratio_corr) {
                range_size = 1;
            } else if (min_range == max_range) {
                range_size = 2;
            } else {   
                range_size = (rand() % (max_range - min_range)) + std::max(2ULL, min_range);
            }
            
            // Generate left query bound
            randdbl = uni_dist1(gen1) * 1.0 / (range_lefts1.size() - 1);
            if (randdbl < pnratio) {
                left = range_size > 1 ? lex_arithmetic(keys[uni_dist1(gen1)], -1) : keys[uni_dist1(gen1)];
            } else {
                left = lex_arithmetic(range_lefts1[uni_dist1(gen1)], corr_dist(gen_corr));
            }

            right = lex_arithmetic(left, range_size - 1);
            if ((left.length() != 0) && (right.length() != 0)) {
                assert(left <= right);
                queries.push_back(std::pair<std::string, std::string>(left, right));
            }
        }

        // Uniform Queries
        for (uint64_t i = 0; i < nqueries / 2; i++) {
            // Generate range query size
            double randdbl = uni_dist2(gen2) * 1.0 / (range_lefts2.size() - 1);
            if (randdbl < pqratio_unif) {
                range_size = 1;
            } else if (min_range == max_range) {
                range_size = 2;
            } else {
                range_size = (rand() % (max_range - min_range)) + std::max(2ULL, min_range);
            }

            // Generate left query bound
            randdbl = uni_dist2(gen2) * 1.0 / (range_lefts2.size() - 1);
            if (randdbl < pnratio) {
                left = range_size > 1 ? lex_arithmetic(keys[uni_dist2(gen2)], -1) : keys[uni_dist2(gen2)];
            } else {
                left = range_lefts2[uni_dist2(gen2)];
            }
        
            right = lex_arithmetic(left, range_size - 1);
            if ((left.length() != 0) && (right.length() != 0)) {
                assert(left <= right);
                queries.push_back(std::pair<std::string, std::string>(left, right));
            } else {
                std::cout << left.length() << ";"<< right.length() << ";"<< range_size << std::endl;
            }
        }
    } else {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<uint64_t> index_dist(0, index);

        std::random_device rd_corr;
        std::mt19937 gen_corr(rd_corr());
        std::uniform_int_distribution<uint64_t> corr_dist(1, correlation_degree);

        for (uint64_t i = 0; i < nqueries; i++) {
            // Generate range query size
            double randdbl = index_dist(gen) * 1.0 / index;
            if (randdbl < pqratio) {
                range_size = 1;
            } else if (min_range == max_range) {
                range_size = 2;
            } else {
                range_size = (rand() % (max_range - min_range)) + std::max(2ULL, min_range);
            }

            randdbl = index_dist(gen) * 1.0 / index;
            if (randdbl < pnratio) {
                left = range_size > 1 ? lex_arithmetic(keys[index_dist(gen)], -1) : keys[index_dist(gen)];
            } else {
                if (qdist == qcorrelated) {
                    left = lex_arithmetic(range_lefts[index_dist(gen)], corr_dist(gen_corr));
                } else if (qdist == qdomain) {
                    // Resize domain query bounds to maximum length in key set
                    std::string resized_left = range_lefts[index_dist(gen)];
                    resized_left.resize(max_klen, '\0');
                    left = resized_left;
                } else {
                    left = range_lefts[index_dist(gen)];
                }
            }
            
            // minus 1 because for strings we include the right query bound
            std::string right = lex_arithmetic(left, range_size - 1);
            if ((left.length() != 0) && (right.length() != 0)) {
                assert(left <= right);
                queries.push_back(std::pair<std::string, std::string>(left, right));
            } else {
                std::cout << left.length() << ";"<< right.length() << ";"<< range_size << std::endl;
            }
        }
    }

    if (queries.size() != nqueries) {
        std::cout << "WARNING: The number of queries generated is less than the specified amount." << std::endl;
        std::cout << "nqueries=" << nqueries << " != queries.size()=" << queries.size() << std::endl;
        std::cout << "This is because some queries generated either overflowed or underflowed." << std::endl;
    }

    return queries;
}

void writeValuesToFile(const std::vector<std::string>& v, const uint32_t max_klen, const std::string& f) {
    std::stringstream ss;
    ss << "my_data/" << f << ".txt";
    std::ofstream output_file(ss.str(), std::ofstream::binary);

    output_file.write(reinterpret_cast<const char*>(&max_klen), sizeof(uint32_t));
    for (const auto & s : v) {
        output_file.write(s.data(), max_klen);
    }
    output_file.close();
}

void writePairsToFile(const std::vector<std::pair<std::string, std::string>>& v, const uint32_t max_klen, const std::string& f1, const std::string& f2) {
    std::stringstream ss1;
    std::stringstream ss2;
    ss1 << "my_data/" << f1 << ".txt";
    ss2 << "my_data/" << f2 << ".txt";
    std::ofstream output_file1(ss1.str(), std::ofstream::binary);
    std::ofstream output_file2(ss2.str(), std::ofstream::binary);

    output_file1.write(reinterpret_cast<const char*>(&max_klen), sizeof(uint32_t));
    output_file2.write(reinterpret_cast<const char*>(&max_klen), sizeof(uint32_t));
    for (const auto & p : v) {
        output_file1.write(p.first.data(), max_klen);
        output_file2.write(p.second.data(), max_klen);
    }
    output_file1.close();
    output_file2.close();
}

int main(int argc, char *argv[]) {
    assert(argc == 12);

    const std::string DOMAINS_PATH = argv[1];
    unsigned long long nkeys = strtoull(argv[2], NULL, 0);
    unsigned long long klen = strtoull(argv[3], NULL, 0);
    unsigned long long nqueries = strtoull(argv[4], NULL, 0);
    unsigned long long min_range = strtoull(argv[5], NULL, 0);
    unsigned long long max_range = strtoull(argv[6], NULL, 0);
    assert(str_to_kdist.count(argv[7]) != 0);
    kdist_type kdist = str_to_kdist[argv[7]];
    assert(str_to_qdist.count(argv[8]) != 0);
    qdist_type qdist = str_to_qdist[argv[8]];
    double pqratio = atof(argv[9]);
    double pnratio = atof(argv[10]);
    unsigned long long correlation_degree = strtoull(argv[11], NULL, 0);
    
    // Safety Checks
    assert(std::experimental::filesystem::exists(DOMAINS_PATH));
    assert(klen % 8 == 0);
    assert(min_range >= 2);
    assert(min_range <= max_range);
    assert(pqratio >= 0 && pqratio <= 1);
    assert(pnratio >= 0 && pnratio <= 1);
    assert(correlation_degree >= 1);
     
    // Print out to double check
    printf("DOMAINS_PATH: %s; KNum: %llu; KLen: %llu; QNum: %llu; MinR: %llu; MaxR: %llu\n\t"
           "KDist: %s; QDist: %s; PQR: %f; PNR: %f; CorrD: %llu\n",
            DOMAINS_PATH.c_str(), nkeys, klen, nqueries, min_range, max_range,
            kdist_names[kdist].c_str(), qdist_names[qdist].c_str(), pqratio, pnratio, correlation_degree);

    // Create data directory
    std::string DIR = "my_data";
    mkdir(DIR.c_str(), 0777);
    
    std::vector<std::string> keys = generateKeys(klen, nkeys, kdist, DOMAINS_PATH);

    // Max Key Length in bytes
    size_t max_klen = klen / 8;
    if (kdist == kdomain) {
        auto max = std::max_element(keys.begin(), keys.end(), [](const auto& s1, const auto& s2){
            return s1.size() < s2.size();
        });
        max_klen = (*max).size();
    }

    std::vector<std::pair<std::string, std::string>> queries = generateRangeQueries(
        klen, nqueries, min_range, max_range, pqratio, keys, qdist, correlation_degree, pnratio, DOMAINS_PATH, max_klen
    );

    std::string kfilename = "data0";
    std::string qfilename1 = "txn0";
    std::string qfilename2 = "upper_bound0";

    writeValuesToFile(keys, max_klen, kfilename);
    writePairsToFile(queries, max_klen, qfilename1, qfilename2);

    return 0;
}
