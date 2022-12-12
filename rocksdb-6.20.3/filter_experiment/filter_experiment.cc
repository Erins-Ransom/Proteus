#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice_transform.h"

#include <random>
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>

#include "filter_exp_util.h"

#define INIT_EXP_TIMER auto start = std::chrono::high_resolution_clock::now();  
#define START_EXP_TIMER start = std::chrono::high_resolution_clock::now();  
#define STOP_EXP_TIMER(name)  std::cout << "RUNTIME of " << name << ": " << \
    std::chrono::duration_cast<std::chrono::microseconds>( \
            std::chrono::high_resolution_clock::now()-start \
    ).count() << " us " << std::endl;
#define SAVE_EXP_TIMER rescsv << std::chrono::duration_cast<std::chrono::microseconds>( \
                                 std::chrono::high_resolution_clock::now()-start).count() << ",";
#define SAVE_TO_RESCSV(val) rescsv << val << ",";


static size_t surf_hlen = 0, surf_rlen = 0;
static size_t sample_cache_size = 0;
static size_t sample_freq = 0;
static bool is_int_bench;
static double bpk;
static bool use_Proteus;
static bool use_SuRF;

std::ofstream rescsv;


template<typename T>
void init(rocksdb::DB** db,
          rocksdb::Options* options, 
          rocksdb::BlockBasedTableOptions* table_options,
          proteus::FIFOSampleQueryCache<T>* sqc) {

    // Create the corresponding filter policies
    if (use_Proteus) {
        table_options->filter_policy.reset(
            rocksdb::NewProteusFilterPolicy(sqc, bpk)
        );
    } else if (use_SuRF) {
        table_options->filter_policy.reset(
            rocksdb::NewSuRFFilterPolicy(surf_hlen,
                                         surf_rlen)
        );
    } else {
        assert(false);
    }

    std::cout << "Using " << table_options->filter_policy->Name() << "\n";
	
    options->create_if_missing = true;
    options->statistics = rocksdb::CreateDBStatistics();

    // Default scaled up by 4
    options->write_buffer_size = 4 * 64 * 1048576;          // Size of memtable = Size of SST file (256 MB) 
    options->max_bytes_for_level_base = 4 * 256 * 1048576;  // 4 SST files at L1
    options->target_file_size_base = 4 * 64 * 1048576;      // Each SST file is 256 MB

    // Force L0 to be empty for consistent LSM tree shape
    options->level0_file_num_compaction_trigger = 1;

    table_options->pin_l0_filter_and_index_blocks_in_cache = true;
    table_options->cache_index_and_filter_blocks = true;

    table_options->block_cache = rocksdb::NewLRUCache(1024 * 1024 * 1024); // 1 GB Block Cache

    // higher read-ahead generally recommended for disks, 
    // for flash/ssd generally 0 is ok, as can unnecessarily 
    // cause extra read-amp on smaller compactions
    options->compaction_readahead_size = 0;

    table_options->partition_filters = false;

    // no mmap for reads nor writes
    options->allow_mmap_reads = false;
    options->allow_mmap_writes = false;

    // direct I/O usage points
    options->use_direct_reads = true;
    options->use_direct_io_for_flush_and_compaction = true;

    // Enable compression -> more keys per SST file = more valid sample queries per filter, bigger filters
    // Don't compress first few levels and use better but slower compression at deeper levels
    options->num_levels = 4;
    options->compression_per_level.resize(options->num_levels);
    for (int i = 0; i < options->num_levels; ++i) {
        if (i < 2) {
            options->compression_per_level[i] = rocksdb::CompressionType::kNoCompression;
        } else if (i == 2) {
            options->compression_per_level[i] = rocksdb::CompressionType::kLZ4Compression;
        } else {
            options->compression_per_level[i] = rocksdb::CompressionType::kZSTD;
        }
    }

    /* 
        // By default, RocksDB uses only one background thread for flush and
        // compaction. Calling this function will set it up such that total of
        // `total_threads` is used. Good value for `total_threads` is the number of
        // cores. You almost definitely want to call this function if your system is
        // bottlenecked by RocksDB.
    */
    options->IncreaseParallelism(6);

    // Default setting - pre-load indexes and filters
    options->max_open_files = -1;
    
    options->table_factory.reset(rocksdb::NewBlockBasedTableFactory(*table_options));

    // Open database
    const std::string db_path = "./db/";
    rocksdb::Status status = rocksdb::DB::Open(*options, db_path, db);
    assert(status.ok());
}


void loadInitialKeysIntoDB(rocksdb::DB* db, const std::vector<std::string>& keys, const std::vector<rocksdb::Slice>& vals) {
    rocksdb::WriteOptions write_options = rocksdb::WriteOptions();
    rocksdb::Status s;

    // Use RocksDB Put to get "normal" LSM tree shape (all levels populated somewhat)
    for (size_t i = 0; i < keys.size(); i++) {
        s = db->Put(write_options, rocksdb::Slice(keys[i]), vals[i]);
        if (!s.ok()) {
            std::cout << s.ToString().c_str() << "\n";
            assert(false);
        }
    }
}


void warmCache(rocksdb::DB* db,
               const std::vector<std::string>& keys, 
               size_t sample_gap) {

    rocksdb::ReadOptions read_options = rocksdb::ReadOptions();
    rocksdb::Status s;
    std::string value_found;
    uint64_t eight_byte_value;

    // Get inserted keys at regular intervals
    for (size_t i = 0; i < keys.size(); i += sample_gap) {
        s = db->Get(read_options, rocksdb::Slice(keys[i]), &value_found);
        if (s.ok()) {
            assert(value_found.size() >= sizeof(eight_byte_value));
            eight_byte_value = *reinterpret_cast<const uint64_t*>(value_found.data());
            (void)eight_byte_value;
        }
    }
}


template<typename T>
void runQuery(rocksdb::DB* db, 
              proteus::FIFOSampleQueryCache<T>* sqc,
              const std::pair<std::string, std::string>& q) {
    
    if (sqc != nullptr) {
        sqc->add(q);
    }

    std::string found_key;
    std::string found_value;
    rocksdb::Slice lower_key(q.first);
    rocksdb::Slice upper_key(q.second);

    // Do a Get if it is a point query
    // Else do a Seek for the range query
    if (isPointQuery(q.first, q.second)) {
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), lower_key, &found_value);
        assert(s.IsNotFound() || s.ok());
    } else {
        rocksdb::ReadOptions read_options = rocksdb::ReadOptions();
        read_options.iterate_upper_bound = &upper_key;
        rocksdb::Iterator* it = db->NewIterator(read_options);

        for (it->Seek(lower_key); it->Valid(); it->Next()) {
            assert(it->value().size() == VAL_SZ);
            found_key = it->key().data();
            found_value = it->value().data();
            (void)found_key;
            (void)found_value;
        }

        if (!it->status().ok()) {
            std::cout << "ERROR in RocksDB Iterator: " << it->status().ToString() << std::endl;
            exit(1);
        }

        delete it;
    }
}


template<typename T>
void runInitialReadWorkload(rocksdb::DB* db,
                            proteus::FIFOSampleQueryCache<T>* sqc,
                            const std::vector<std::pair<std::string, std::string>>& queries) {

    for (size_t i = 0; i < queries.size(); ++i) {
        runQuery(db, sqc, queries[i]);
    }
}


// Interleave reads and writes according to the input workload `trace`
template<typename T>
void runReadWriteWorkloads(rocksdb::DB* db,
                           rocksdb::Options* options,
                           const std::vector<std::vector<std::string>>& keys,
                           const std::vector<std::vector<rocksdb::Slice>>& vals,
                           const std::vector<std::vector<std::pair<std::string, std::string>>>& queries,
                           const std::vector<std::vector<bool>>& trace,
                           proteus::FIFOSampleQueryCache<T>* sqc) { 
    
    rocksdb::Status s;
    rocksdb::WriteOptions writeOptions = rocksdb::WriteOptions();

    uint32_t total_hits = 0, total_misses = 0, total_uses = 0;
    size_t total_reads = 0;
    size_t prev_reads = 0;

    INIT_EXP_TIMER
    START_EXP_TIMER

    for (size_t i = 1; i < trace.size(); i++) {
        size_t nreads = 0;
        size_t nwrites = 0;
        for (const auto op : trace[i])  {
            if (op) {
                // Read
                runQuery(db, sqc, queries[i][nreads]);
                nreads++;
                total_reads++;
            } else {
                // Write
                rocksdb::Slice w_key(keys[i][nwrites]);
                s = db->Put(writeOptions, w_key, vals[i][nwrites]);          
                if (!s.ok()) {
                    std::cout << "ERROR in RocksDB Put: " << s.ToString() << std::endl;
                    exit(1);
                }
                nwrites++;
            }

            // Record FPR and Latency every 5M Range Queries
            if (total_reads > 0 && total_reads > prev_reads && (total_reads % 5000000) == 0) {
                STOP_EXP_TIMER("Mixed Read-Write, " << total_reads <<  " Total Reads Completed")
                SAVE_EXP_TIMER

                uint32_t curr_hits = options->statistics->getTickerCount(rocksdb::RANGE_FILTER_HIT) - total_hits,
                         curr_misses = options->statistics->getTickerCount(rocksdb::RANGE_FILTER_MISS) - total_misses,
                         curr_uses = options->statistics->getTickerCount(rocksdb::RANGE_FILTER_USE) - total_uses;
                                
                long double curr_fpr = (long double) curr_misses / (curr_uses - curr_hits);

                printf("Uses: %u, Misses: %u, Hits: %u\n", curr_hits, curr_misses, curr_hits);
                printf("False Positive Rate (%zu Reads Completed): %Lf\n", total_reads, curr_fpr);

                SAVE_TO_RESCSV(curr_fpr)

                total_hits += curr_hits;
                total_misses += curr_misses;
                total_uses += curr_uses;

                printCompactionAndDBStats(db);

                START_EXP_TIMER
            }

            prev_reads = total_reads;
        }
    }
}


template<typename T>
void runExperiment(std::vector<std::vector<std::string>>& keys,
                   std::vector<std::vector<rocksdb::Slice>> vals,
                   std::vector<std::vector<std::pair<std::string, std::string>>>& queries,
                   std::vector<std::vector<bool>> trace,
                   proteus::FIFOSampleQueryCache<T>* sqc) {
    
    INIT_EXP_TIMER

    // Configure and initialize database
    rocksdb::DB* db;
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    init(&db, &options, &table_options, sqc);

    START_EXP_TIMER
    loadInitialKeysIntoDB(db, keys[0], vals[0]);
    STOP_EXP_TIMER("Load Keys into DB")

    START_EXP_TIMER
    flushMemTable(db);
    STOP_EXP_TIMER("Flush MemTable")

    START_EXP_TIMER
    waitForBGCompactions(db);
    STOP_EXP_TIMER("Wait for Background Compactions")

    // We need cache warming even when running all empty queries since
    // data blocks may be loaded from disk when there are false positives.
    START_EXP_TIMER
    warmCache(db, keys[0], keys[0].size() / 1000000);
    STOP_EXP_TIMER("Cache Warming")

    printCompactionAndDBStats(db);

    // Reset performance stats
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_perf_context()->ClearPerLevelPerfContext();
    rocksdb::get_perf_context()->EnablePerLevelPerfContext();
    rocksdb::get_iostats_context()->Reset();
    
    START_EXP_TIMER
    runInitialReadWorkload(db, sqc, queries[0]);
    STOP_EXP_TIMER("Initial Read Workload")
    SAVE_EXP_TIMER
    
    // Print Initial Read Workload FPR
    printFPR(&options, rescsv);
    printCompactionAndDBStats(db);

    if (keys.size() > 1 && queries.size() > 1) {
        START_EXP_TIMER
        runReadWriteWorkloads(db, &options, keys, vals, queries, trace, sqc);
        STOP_EXP_TIMER("Overall Read Write Workloads Mixed")
        SAVE_EXP_TIMER
    }
 
    printStats(db, &options, rescsv);

    // Finish result line in result csv and close file stream
    rescsv << std::endl;
    rescsv.close();

    // Close database
    rocksdb::Status s = db->Close();
    assert(s.ok());
    delete db;        
}


int main(int argc, char** argv) {
    /****************************************
        Arguments from filter_experiment.sh
        ================================== 

        Common arguments:
            $is_int_bench
            {"Proteus, "SuRF"}
            $RES_CSV 
        
        Proteus:
            $membudg
            $samplecachesize
            $samplefreq
            
        SuRF:
            $surfhlen
            $surfrlen

    ****************************************/

    // Parse command-line arguments
    assert(argc > 5);

    is_int_bench = (strcmp(argv[1], "1") == 0);
    use_Proteus = (strcmp(argv[2], "Proteus") == 0);
    use_SuRF = (strcmp(argv[2], "SuRF") == 0);

    printf("%d\t%s\t", is_int_bench, argv[2]);

    // Open result csv to append results to file
    rescsv.open(argv[3], std::ios_base::app);

    if (use_Proteus) {
        assert(argc == 7);
        bpk = strtod(argv[4], nullptr);
        sample_cache_size = strtoull(argv[5], nullptr, 10);
        sample_freq = strtoull(argv[6], nullptr, 10);
        printf("%f\t%zu\t%zu\n", bpk, sample_cache_size, sample_freq);
    } else if (use_SuRF) {
        assert(argc == 6);
        surf_hlen = strtoull(argv[4], nullptr, 10);
        surf_rlen = strtoull(argv[5], nullptr, 10);
        printf("%zu\t%zu\n", surf_hlen, surf_rlen);
    }

    // Load Initial Keys and Queries
    if (is_int_bench) {
        // Integer keys and queries are byte reversed and converted to std::string
        auto kvps = intLoadKeysValues();
        std::vector<std::vector<std::pair<std::string, std::string>>> queries = intLoadQueries();
        std::vector<std::vector<bool>> trace = loadTrace();
        
        // Sample queries and create sample query cache for Proteus
        proteus::FIFOSampleQueryCache<uint64_t>* sqc = nullptr;
        if (use_Proteus) {
            if (sample_cache_size > (queries[0].size() / 2)) {
                printf("Sample Size: %zu,\tInitial Queries: %zu\n", sample_cache_size, queries[0].size());
                printf("Not enough queries :(\n");
                exit(1);
            }
            sqc = new proteus::FIFOSampleQueryCache<uint64_t>(intSampleInitialQueries(queries[0], sample_cache_size), sample_freq);
        }

        runExperiment(kvps.first, kvps.second, queries, trace, sqc);

        // Free Sample Query Cache
        if (use_Proteus) {
            delete sqc;    
        }

    } else {
        auto kvps = strLoadKeysValues();
        std::vector<std::vector<std::pair<std::string, std::string>>> queries = strLoadQueries();
        std::vector<std::vector<bool>> trace = loadTrace();

         // Sample queries and create sample query cache for Proteus
        proteus::FIFOSampleQueryCache<std::string>* sqc = nullptr;
        if (use_Proteus) {
            if (sample_cache_size > (queries[0].size() / 2)) {
                printf("Sample Size: %zu,\tInitial Queries: %zu\n", sample_cache_size, queries[0].size());
                printf("Not enough queries :(\n");
                exit(1);
            }
            sqc = new proteus::FIFOSampleQueryCache<std::string>(strSampleInitialQueries(queries[0], sample_cache_size), sample_freq);
        }

        runExperiment(kvps.first, kvps.second, queries, trace, sqc);

        // Free Sample Query Cache
        if (use_Proteus) {
            delete sqc;    
        }
    }
    
    return 0;
}
