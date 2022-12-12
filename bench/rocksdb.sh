#!/bin/bash

# Reference: https://sharats.me/posts/shell-script-best-practices/

# Exit if a command fails  
set -o errexit

# Throw error when accessing an unset variable
set -o nounset

# Enable debug mode with TRACE=1 ./bench.sh
if [[ "${TRACE-0}" == "1" ]]; then
    set -o xtrace
fi

# Change to directory of this script
cd "$(dirname "$0")"


###############################################
###########   WORKLOAD PARAMETERS   ###########
###############################################

# Make sure white space in array elements isn't interpreted as a separator
IFS=""

# Refer to `bench.sh` for more details on the workload parameters

# INTEGER

IS_INT_BENCH="1"
keylen_arr=(64)
nkeys_arr=(100000000)
nqrys_arr=(10000000)
kdist_arr=("knormal" "knormal" "kuniform" "kuniform")
qdist_arr=("qsplit" "quniform" "quniform" "qcorrelated")
minrange_arr=(2 2 2 2) 
maxrange_arr=(16 4096 32 16) 
pqratio_arr=(0.0)
pnratio_arr=(0.0)
corrd_arr=(1024 1 1 1024)


# STRING 

# IS_INT_BENCH="0"
# keylen_arr=(2024)
# nkeys_arr=(20000000)
# nqrys_arr=(10000000)
# kdist_arr=("kdomain")
# qdist_arr=("qdomain")
# minrange_arr=(2) 
# maxrange_arr=(1073741824) 
# pqratio_arr=(0.0)
# pnratio_arr=(0.0)
# corrd_arr=(1)

# SHIFTING WORKLOADS
# Each array element must have the same number of space separated parameters 
# The number of space separated parameters determines the number of workloads generated.
# The i-th workload is generated according to all the i-th space separated parameters in each of the array elements.

# IS_INT_BENCH="1"
# keylen_arr=("64 64 64")
# nkeys_arr=("20000000 20000000 20000000")
# nqrys_arr=("10000000 30000000 30000000")
# kdist_arr=("kuniform kuniform kuniform" "kuniform kuniform kuniform")
# qdist_arr=("qcorrelated qcorrelated quniform" "quniform quniform qcorrelated")
# minrange_arr=("2 2 2" "2 2 2")
# maxrange_arr=("16 16 4096" "4096 4096 16")
# pqratio_arr=("0.0 0.0 0.0")
# pnratio_arr=("0.0 0.0 0.0")
# corrd_arr=("1024 1024 1" "1 1 1024")

if [[ "${IS_INT_BENCH}" == "1" ]]; then
    echo -e "Running Integer RocksDB Benchmark"
else
    echo -e "Running String RocksDB Benchmark"
fi

###############################################
############   FILTER PARAMETERS   ############
###############################################

### Proteus
membudg_arr=(8 10 12 14 16 18 20)

# Sample Cache Size:    Number of the sample queries used for modeling
# Sample Frequency:     How often a new query is sampled for the query cache 
samplecachesize=20000
samplefreq=100

### SuRF
surfhlen_arr=(0)
surfrlen_arr=(0 2 4 6 8 10)

################################################################

REPO_DIR="$(pwd)"
SOSD_DIR="$REPO_DIR/../workloads/SOSD/"
DOMAINS="$REPO_DIR/../workloads/domains/domains.txt"
DOMAINS_SHUFFLED="$REPO_DIR/../workloads/domains/shuf.txt"
INT_WORKL_BIN="$REPO_DIR/../workloads/int_workload_gen"
STR_WORKL_BIN="$REPO_DIR/../workloads/str_workload_gen"
EXP_BIN="$REPO_DIR/../rocksdb-6.20.3/build/filter_experiment/filter_experiment"

SCRATCH_DIR="$REPO_DIR/../tmp/A"
DATA_DIR="$REPO_DIR/../tmp/B"

# Create result folder - if folder exists then exit
res_folder="$REPO_DIR/$1"
if [ ! -e "$res_folder" ]; then 
    mkdir "$res_folder"
else
    echo "Result folder already exists! Please try again with a valid result folder name."
    exit -1
fi

# Create file that indexes the information of the generated experiment result files
index_file="$res_folder/index.txt"
touch $index_file

# Create result csv
res_csv="$res_folder/results.csv"
touch $res_csv

# Create directories if they don't exist
mkdir -p "$DATA_DIR"
mkdir -p "$SCRATCH_DIR"

fetch_data() {
    if [ $IS_INT_BENCH = "1" ]; then
        path="$DATA_DIR/$IS_INT_BENCH/${nkeys// /_}/${nqrys// /_}/${minrange// /_}/${maxrange// /_}/${kdist// /_}/${qdist// /_}/${pqratio// /_}/${pnratio// /_}/${corrd// /_}"
    else
        path="$DATA_DIR/$IS_INT_BENCH/${nkeys// /_}/${keylen// /_}/${nqrys// /_}/${minrange// /_}/${maxrange// /_}/${kdist// /_}/${qdist// /_}/${pqratio// /_}/${pnratio// /_}/${corrd// /_}"
    fi

    if [ ! -e "$path/my_data" ]; then
	    echo "Generating data in $path"
	    mkdir -p "$path" && cd "$path"

        if [ $IS_INT_BENCH = "1" ]; then
            $INT_WORKL_BIN "$SOSD_DIR" "$nkeys" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
        else
            # Shuffle domains on every run
            if [ $kdist = "kdomain" ]; then
                cat $DOMAINS | shuf -n $(($nkeys + $nqrys)) -o $DOMAINS_SHUFFLED
            fi
            $STR_WORKL_BIN "$DOMAINS_SHUFFLED" "$nkeys" "$keylen" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
        fi
    else
        echo "Copying data from $path"
    fi

    cp -r "$path/my_data" $expdir/my_data
}

experiment() {
    cd "$expdir"
    touch ./experiment_result
    echo -e "### FILE INFO ###" >> ./experiment_result
    echo -e "\tResult Folder:\t$res_folder" >> ./experiment_result
    echo -e "\tFile Counter:\t$filecnt\n\n" >> ./experiment_result
    
    if [[ "${IS_INT_BENCH}" == "1" ]]; then
        echo -e "### BEGIN INTEGER EXPERIMENT ###" >> ./experiment_result
    else
        echo -e "### BEGIN STRING EXPERIMENT ###" >> ./experiment_result
    fi

    echo -e "\n\n### BEGIN EXPERIMENT DESCRIPTION ###" >> ./experiment_result
    echo -e "\tFilter Name:\t$filter" >> ./experiment_result
    echo -e "\tNumber of Keys:\t$nkeys" >> ./experiment_result
    echo -e "\tKey Length:\t$keylen" >> ./experiment_result
    echo -e "\tNumber of Queries:\t$nqrys" >> ./experiment_result
    echo -e "\tKey Distribution:\t$kdist" >> ./experiment_result
    echo -e "\tQuery Distribution:\t$qdist" >> ./experiment_result
    echo -e "\tMinimum Range Size:\t$minrange" >> ./experiment_result
    echo -e "\tMaximum Range Size:\t$maxrange" >> ./experiment_result
    echo -e "\tPoint / Range Query Ratio:\t$pqratio" >> ./experiment_result
    echo -e "\tQuery Correlation Degree:\t$corrd" >> ./experiment_result
    echo -e "\tPositive / Negative Query ratio:\t$pnratio" >> ./experiment_result
    
    fetch_data

    # Print experiment parameters to index file and result CSV
    out_file="$filter-$filecnt"
    echo -e "############# $out_file #############" >> $index_file
    echo -e "IsIntBench: $IS_INT_BENCH; NKeys: $nkeys; NQueries: $nqrys; KLen: $keylen" >> $index_file
    echo -e "KDist: $kdist; QDist: $qdist" >> $index_file
    echo -e "MinRange: $minrange; MaxRange: $maxrange" >> $index_file
    echo -e "P-Q ratio: $pqratio; CorrDeg: $corrd; +/- Q ratio: $pnratio" >> $index_file
    
    config="$IS_INT_BENCH-$filter-${nkeys// /_}-${keylen// /_}-${nqrys// /_}-${minrange// /_}-${maxrange// /_}-${pqratio// /_}-${kdist// /_}-${qdist// /_}-${pnratio// /_}-${corrd// /_}"
    if [ $filter = "SuRF" ]; then
        config+="-${surfhlen}-${surfrlen}"
        echo -e "HLen: $surfhlen; RLen: $surfrlen" >> $index_file
        echo -ne "$IS_INT_BENCH,$filter,$nkeys,$nqrys,$keylen,$kdist,$qdist,$minrange,$maxrange,$pqratio,$corrd,$pnratio,$surfhlen,$surfrlen," >> $res_csv
    elif [ $filter = "Proteus" ]; then
        config+="-${membudg}"
        echo -e "BPK: $membudg" >> $index_file
        echo -ne "$IS_INT_BENCH,$filter,$nkeys,$nqrys,$keylen,$kdist,$qdist,$minrange,$maxrange,$pqratio,$corrd,$pnratio,$membudg," >> $res_csv
    fi

    echo -e "\n" >> $index_file
    echo "Running $out_file: " $config

    if [ $filter = "SuRF" ]; then
        echo -e "\tSuRF Hash Suffix Length:\t$surfhlen" >> ./experiment_result
        echo -e "\tSuRF Real Suffix Length:\t$surfrlen" >> ./experiment_result
        echo -e "### END EXPERIMENT DESCRIPTION ###\n\n" >> ./experiment_result
        $EXP_BIN "$IS_INT_BENCH" "$filter" "$res_csv" "$surfhlen" "$surfrlen" >> ./experiment_result

    elif [ $filter = "Proteus" ]; then
        echo -e "\tFilter Bits-per-Key:\t$membudg" >> ./experiment_result
        echo -e "### END EXPERIMENT DESCRIPTION ###\n\n" >> ./experiment_result
        $EXP_BIN "$IS_INT_BENCH" "$filter" "$res_csv" "$membudg" "$samplecachesize" "$samplefreq" >> ./experiment_result
    fi

    echo $out_file " - Complete "
    cp ./experiment_result "$res_folder/$out_file.txt"

    cp ./db/LOG LOG
    rm -rf ./db/
    rm -rf ./my_data/
    rm ./experiment_result
}

# Proteus Experiments
filter="Proteus"
file_cnt=1
for nkeys in "${nkeys_arr[@]}"; do
for keylen in "${keylen_arr[@]}"; do   
for nqrys in "${nqrys_arr[@]}"; do
for i in "${!kdist_arr[@]}"; do
for pqratio in "${pqratio_arr[@]}"; do
for pnratio in "${pnratio_arr[@]}"; do
for membudg in "${membudg_arr[@]}"; do

kdist="${kdist_arr[$i]}"
qdist="${qdist_arr[$i]}"
minrange="${minrange_arr[$i]}"
maxrange="${maxrange_arr[$i]}"
corrd="${corrd_arr[$i]}"

printf -v filecnt "%03d" $file_cnt
expdir=$(mktemp -d $SCRATCH_DIR/proteus_exp.XXXXXXX)
experiment
((file_cnt++))

done
done
done
done
done
done
done


# SuRF experiments
filter="SuRF"
file_cnt=1
for nkeys in "${nkeys_arr[@]}"; do
for keylen in "${keylen_arr[@]}"; do
for nqrys in "${nqrys_arr[@]}"; do
for i in "${!kdist_arr[@]}"; do
for pqratio in "${pqratio_arr[@]}"; do
for pnratio in "${pnratio_arr[@]}"; do
for surfhlen in "${surfhlen_arr[@]}"; do
for surfrlen in "${surfrlen_arr[@]}"; do

kdist="${kdist_arr[$i]}"
qdist="${qdist_arr[$i]}"
minrange="${minrange_arr[$i]}"
maxrange="${maxrange_arr[$i]}"
corrd="${corrd_arr[$i]}"

printf -v filecnt "%03d" $file_cnt
expdir=$(mktemp -d $SCRATCH_DIR/surf_exp.XXXXXXX)
experiment
((file_cnt++))

done
done
done
done
done
done
done
done
