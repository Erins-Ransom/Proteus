# Hybrid Prefix Range Filter (HyPR)

## RocksDB Install Dependencies
sudo apt-get install build-essential cmake libgtest.dev liblz4-dev libzstd-dev libgflags-dev
cd /usr/src/gtest
sudo cmake CMakeLists.txt
sudo make
sudo cp *.a /usr/lib

## RocksDB Build (in RocksDB folder)
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DWITH_LZ4=ON -DWITH_ZSTD=ON ..
make -j$(nproc) s_filter_experiment

## To enable ASAN and Debug Info for Optimized Code in RocksDB

mkdir build_asan
cd build_asan
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_LZ4=ON -DWITH_ZSTD=ON -DWITH_ASAN=ON ..
make -j$(nproc) s_filter_experiment

## Build Instructions (Sucessful on 2021-05-09)

make workload
cd SuRF
cmake .
make -j$(nproc)
cd ../bench
make bench

#How to run SuRF experiments
	cd SuRF/bench
	bash run_SuRF.sh

#Notes
	You will need to change the paths in my_workload.cpp and run_SuRF.sh

## Directory Layout
`include/` : contains all the files necessary to the Proteus filter.
`bench/` : contains all the files used for testing and benchmarking the standalone filter 
`rocksdb/` : contains a modified version of rocksdb to handle Proteus, SuRF, PrefixBF filters.

# Workload Generation

If you run it by `./workload`, (e.g. ./workload 1000000 40 100000 32 0.0 uniform uniform) you'll see a folder called my_data has been created.

Inside my_data,
	a) data.txt contains the keys of the data. note there are no values of the data explicitly generated here.
	b) txn.txt contains the keys indicating the start of the range. LOWER END
	c) upper_bound.txt contains the keys indicating the end of the range. HIGHER END

It is done this way to interface well with SuRF.

---

#Compiling with RocksDB
Go to "rocksdb", execute command 
make static_lib -j8 

Go to "diffident-paper/workload_gen"
make workload

Go to "rocksdb-dst-surf/examples"
make range_query_bulk_write_turf

Go to "diffident-paper/standalone"
modify the paths and emails of "run_rocksdb_experiments_w_bulkload_sel_turf.sh". Typically you can search "siqiang" or "Siqiang", and replace the relevant terms.

An example bash running is as follows (you need to modify the last a few parameters)
"bash run_rocksdb_experiments_w_bulkload_sel_turf.sh /home/siqiang/rosetta_with_turf/rocksdb-surf-dst/examples/range_query_bulk_write_turf /home/siqiang/rosetta_with_turf/diffident-paper/workload_gen/workload /home/siqiang/tmp/perform_exp /home/siqiang/tmp/exp_res /home/siqiang/tmp/bulk_data"

Make SuRF and workload