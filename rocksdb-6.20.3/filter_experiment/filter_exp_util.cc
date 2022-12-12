#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice_transform.h"

#include "filter_exp_util.h"


std::string uint64ToString(const uint64_t word) {
    uint64_t endian_swapped_word = __builtin_bswap64(word);
    return std::string(reinterpret_cast<const char*>(&endian_swapped_word), 8);
}

uint64_t stringToUint64(const std::string& str_word) {
    uint64_t int_word = 0;
    memcpy(reinterpret_cast<char*>(&int_word), str_word.data(), 8);
    return __builtin_bswap64(int_word);
}

// assume compression ratio = 0.5
void setValueBuffer(char* value_buf, int size,
		            std::mt19937_64 &e,
		            std::uniform_int_distribution<unsigned long long>& dist) {
    memset(value_buf, 0, size);
    int pos = size / 2;
    while (pos < size) {
        uint64_t num = dist(e);
        char* num_bytes = reinterpret_cast<char*>(&num);
        memcpy(value_buf + pos, num_bytes, 8);
        pos += 8;
    }
}

/*
 * Generate values for multiple workloads except for the initial read workload
 */
template<typename T>
std::vector<std::vector<rocksdb::Slice>> generateValues(const std::vector<std::vector<T>>& keys) {
    char value_buf[VAL_SZ];
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);
    std::vector<std::vector<rocksdb::Slice>> vals(keys.size(), std::vector<rocksdb::Slice>());

    for (size_t i = 0; i < keys.size(); i++) {
        for (size_t j = 0; j < keys[i].size(); j++) {
            setValueBuffer(value_buf, VAL_SZ, e, dist);
            vals[i].push_back(rocksdb::Slice(value_buf, VAL_SZ));
        }
    }

    return vals;
} 

std::pair<std::vector<std::vector<std::string>>, std::vector<std::vector<rocksdb::Slice>>> intLoadKeysValues() {
    std::ifstream keyFile;
    std::string dataPath = "./my_data/";
    std::vector<std::vector<std::string>> keys;
    uint64_t key;
    size_t idx = 0;

    // Iterate over all key files
    while (std::experimental::filesystem::exists(dataPath + "data" + std::to_string(idx) + ".txt")) {
        keys.push_back(std::vector<std::string>());
        keyFile.open(dataPath + "data" + std::to_string(idx) + ".txt");
        while (keyFile >> key) {
            keys.back().push_back(uint64ToString(key));
        }
        
        keyFile.close();
        idx++;
    }

    return std::make_pair(keys, generateValues(keys)); 
}

std::pair<std::vector<std::vector<std::string>>, std::vector<std::vector<rocksdb::Slice>>> strLoadKeysValues() {
    std::ifstream keyFile;
    std::string dataPath = "./my_data/";
    std::vector<std::vector<std::string>> keys;
    uint32_t sz;
    size_t idx = 0;

    char value_buf[VAL_SZ];
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);
    std::vector<std::vector<rocksdb::Slice>> vals;

    // Iterate over all key files
    while (std::experimental::filesystem::exists(dataPath + "data" + std::to_string(idx) + ".txt")) {
        keys.push_back(std::vector<std::string>());
        keyFile.open(dataPath + "data" + std::to_string(idx) + ".txt", std::ifstream::binary);
        keyFile.read(reinterpret_cast<char*>(&sz), sizeof(uint32_t));

        char* k_arr = new char[sz];
        while (keyFile.peek() != EOF) {
            keyFile.read(k_arr, sz);
            keys.back().push_back(std::string(k_arr, sz)); 
        }

        delete[] k_arr;
        keyFile.close();
        idx++;
    }
    
    return std::make_pair(keys, generateValues(keys)); ;
}

std::vector<std::vector<std::pair<std::string, std::string>>> intLoadQueries() {
    std::ifstream lQueryFile, uQueryFile;
    std::string dataPath = "./my_data/";
    std::vector<std::vector<std::pair<std::string, std::string>>> queries;
    uint64_t lq, uq;
    size_t idx = 0;

    // Iterate over all query files
    while (std::experimental::filesystem::exists(dataPath + "txn" + std::to_string(idx) + ".txt") &&
           std::experimental::filesystem::exists(dataPath + "upper_bound" + std::to_string(idx) + ".txt")) {
        queries.push_back(std::vector<std::pair<std::string, std::string>>());
        lQueryFile.open(dataPath + "txn" + std::to_string(idx) + ".txt");
        uQueryFile.open(dataPath + "upper_bound" + std::to_string(idx) + ".txt");
        while ((lQueryFile >> lq) && (uQueryFile >> uq)) {
            assert(lq <= uq);
            queries.back().push_back(std::make_pair(uint64ToString(lq), uint64ToString(uq)));
        }
        
        lQueryFile.close();
        uQueryFile.close();
        idx++;
    }

    return queries;
}

std::vector<std::vector<std::pair<std::string, std::string>>> strLoadQueries() {
    std::ifstream lQueryFile, rQueryFile;
    std::string dataPath = "./my_data/";
    std::vector<std::vector<std::pair<std::string, std::string>>> queries;
    std::string lq, rq;
    uint32_t lsz, rsz;
    size_t idx = 0;

    // Iterate over all query files
    while (std::experimental::filesystem::exists(dataPath + "txn" + std::to_string(idx) + ".txt") &&
           std::experimental::filesystem::exists(dataPath + "upper_bound" + std::to_string(idx) + ".txt")) {
        queries.push_back(std::vector<std::pair<std::string, std::string>>());
        lQueryFile.open(dataPath + "txn" + std::to_string(idx) + ".txt", std::ifstream::binary);
        rQueryFile.open(dataPath + "upper_bound" + std::to_string(idx) + ".txt", std::ifstream::binary);
        
        lQueryFile.read(reinterpret_cast<char*>(&lsz), sizeof(uint32_t));
        rQueryFile.read(reinterpret_cast<char*>(&rsz), sizeof(uint32_t));
        assert(lsz == rsz);

        char* lq_arr = new char[lsz];
        char* rq_arr = new char[rsz];
        while (lQueryFile.peek() != EOF && rQueryFile.peek() != EOF) {
            lQueryFile.read(lq_arr, lsz);
            rQueryFile.read(rq_arr, rsz);
            lq = std::string(lq_arr, lsz);
            rq = std::string(rq_arr, rsz);
            assert(lq <= rq);
            queries.back().push_back(std::make_pair(lq, rq));
        }
        
        delete[] lq_arr;
        delete[] rq_arr;
        lQueryFile.close();
        rQueryFile.close();
        idx++;
    }

    return queries;
}

std::vector<std::vector<bool>> loadTrace() {
    std::ifstream traceFile;
    std::string dataPath = "./my_data/";

    std::vector<std::vector<bool>> trace;
    trace.push_back(std::vector<bool>());

    bool rw;
    size_t idx = 1;

    // Iterate over all trace files
    while (std::experimental::filesystem::exists(dataPath + "trace" + std::to_string(idx) + ".txt")) {
        trace.push_back(std::vector<bool>());
        traceFile.open(dataPath + "trace" + std::to_string(idx) + ".txt");
        while (traceFile >> rw) {
            trace.back().push_back(rw);
        }
        
        traceFile.close();
        idx++;
    }
    
    return trace;
}

const std::vector<std::pair<uint64_t, uint64_t>> intSampleInitialQueries(const std::vector<std::pair<std::string, std::string>>& queries, 
                                                                         size_t sample_cache_size) {
    std::vector<std::pair<uint64_t, uint64_t>> sample_queries;
    size_t interval_len = queries.size() / sample_cache_size;
    for (size_t i = 0; i < queries.size(); i += interval_len) {
        sample_queries.push_back(std::make_pair(stringToUint64(queries[i].first), stringToUint64(queries[i].second)));
    }
    return sample_queries;
}

const std::vector<std::pair<std::string, std::string>> strSampleInitialQueries(const std::vector<std::pair<std::string, std::string>>& queries, 
                                                                               size_t sample_cache_size) {
    std::vector<std::pair<std::string, std::string>> sample_queries;
    size_t interval_len = queries.size() / sample_cache_size;
    for (size_t i = 0; i < queries.size(); i += interval_len) {
        sample_queries.push_back(queries[i]);
    }
    return sample_queries;
}

void printCompactionAndDBStats(rocksdb::DB* db) {
    std::string stats;
    db->GetProperty("rocksdb.stats", &stats);
    printf("%s", stats.c_str());
}

void printLSM(rocksdb::DB* db) {
    std::cout << "Print LSM" << std::endl;
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);

    std::cout << "Total Size (bytes): " << cf_meta.size << std::endl;
    std::cout << "Total File Count: " << cf_meta.file_count << std::endl;

    int largest_used_level = -1;
    for (auto level : cf_meta.levels) {
        if (level.files.size() > 0) {
            largest_used_level = level.level;
        }
    }

    std::cout << "Largest Level: " << largest_used_level << std::endl;
    for (auto level : cf_meta.levels) {
        long level_size = 0;
        for (auto file : level.files) {
            level_size += file.size;
        }
        std::cout << "level " << level.level << ".  Size " << level_size << " bytes" << std::endl;
        std::cout << std::endl;
        for (auto file : level.files) {
            std::cout << " \t " << file.size << " bytes \t " << file.name << std::endl;
        }
        if (level.level == largest_used_level) {
            break;
        }
    }

    std::cout << std::endl;
}

void flushMemTable(rocksdb::DB* db) {
    rocksdb::FlushOptions flush_opt;
    flush_opt.wait = true;
    rocksdb::Status s = db->Flush(flush_opt);
    assert(s.ok());
}

void waitForBGCompactions(rocksdb::DB* db) {
    bool double_checked = false;
    uint64_t prop;
    while (true) {
        // Check stats every 10s  
        sleep(10);
        
        if (!(db->GetIntProperty("rocksdb.num-running-flushes", &prop))) continue;
        if (prop > 0) continue;
        if (!(db->GetIntProperty("rocksdb.num-running-compactions", &prop))) continue;
        if (prop > 0) continue;
        if (!(db->GetIntProperty("rocksdb.mem-table-flush-pending", &prop))) continue;
        if (prop == 1) continue;
        if (!(db->GetIntProperty("rocksdb.compaction-pending", &prop))) continue;
        if (prop == 1) continue;

        if (double_checked) {
            break;
        } else {
            double_checked = true;
            continue;
        }
    }

    // Print out initial LSM state
    printLSM(db);
}

void printFPR(rocksdb::Options* options, std::ofstream& stream) {
    uint32_t hits = options->statistics->getTickerCount(rocksdb::RANGE_FILTER_HIT),
             misses = options->statistics->getTickerCount(rocksdb::RANGE_FILTER_MISS),
             uses = options->statistics->getTickerCount(rocksdb::RANGE_FILTER_USE);
    printf("Uses: %u, Misses: %u, Hits: %u\n", uses, misses, hits);
    printf("Overall False Positive Rate: %Lf\n", (long double) misses / (uses-hits));
    stream << (long double) misses / (uses - hits) << ",";
}


void printStats(rocksdb::DB* db,
                rocksdb::Options* options,
                std::ofstream& stream) {

    sleep(10);

    // STOP ROCKS PROFILE
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);

    std::cout << "RocksDB Perf Context : " << std::endl;

    std::cout << rocksdb::get_perf_context()->ToString() << std::endl;

    std::cout << "RocksDB Iostats Context : " << std::endl;

    std::cout << rocksdb::get_iostats_context()->ToString() << std::endl;
    // END ROCKS PROFILE

    // Print Full RocksDB stats
    std::cout << "RocksDB Statistics : " << std::endl;
    std::cout << options->statistics->ToString() << std::endl;

    std::cout << "----------------------------------------" << std::endl;

    printLSM(db);

    std::string tr_mem;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &tr_mem);
    std::cout << "RocksDB Estimated Table Readers Memory (index, filters) : " << tr_mem << std::endl;

    printFPR(options, stream);
}
