#!/bin/bash

###############################################
###########   WORKLOAD PARAMETERS   ###########
###############################################

## NOTE: paired values
#  By default, we loop through all combinations of parameters, but paired values 
#  are considered as a single parameter.  i.e. 
#
#       kdist_arr=("kuniform" "kuniform")
#       qdist_arr=("quniform" "qcorrelated")
#
#  Corresponds to two workloads with uniform key distributions, one with 
#  uniform queries and the other with correlated queries.


## Key Length (in bits)
# For integer workloads, this value MUST be 64
# For synthetic string workloads, this value MUST be a multiple of 8
# For the string domain workload, this value is ignored.

keylen_arr=(64) # int
# keylen_arr=(1440) # str

## Number of Keys and Queries [PAIRED]
nkeys_arr=(10000000)
nqrys_arr=(1000000) 

## Key and Query Distribution [PAIRED]

# For SOSD / Domain workloads, please download the relevant datasets and follow the instructions in the README
# SOSD and Domain Key and Query Distributions must be specified together, e.g. ksosd_books + qsosd_books

# Integer Key Distributions: kuniform, knormal, ksosd_books, ksosd_fb
# Integer Query Distributions: quniform, qnormal, qcorrelated, qsplit, qsosd_books, qsosd_fb

# String Key Distributions: kuniform, knormal, kdomain
# String Query Distributions: quniform, qcorrelated, qsplit, qdomain

kdist_arr=("kuniform" "kuniform" "knormal" "knormal" "knormal")
qdist_arr=("quniform" "qcorrelated" "qsplit" "qcorrelated" "quniform")

## Min and Max Range Size [PAIRED]
# Min Range Size must be >= 2
minrange_arr=(2) 
maxrange_arr=(4096)     # int
# maxrange_arr=(1073741824)   # str

## Point / Range Query Ratio
# Must be >= 0.0 && <= 1.0
# Use 0.0 for all range queries and 1.0 for all point queries
pqratio_arr=(0.0)

## Positive / Negative Query Ratio
# Must be >= 0.0 && <= 1.0
# Use 0.0 for all negative queries and 1.0 for all positive queries
pnratio_arr=(0.0) 

## Query Correlation Degree
# The distance between the left query bound and a key is uniformly distributed between 1 and the specified value
# Must be >= 1
corrd_arr=(1024)    # int
# corrd_arr=(536870912)   # str


###############################################
############   FILTER PARAMETERS   ############
###############################################


### Proteus

## Filter Bits-per-Key
# Must be a positive real number
membudg_arr=(8 10 12 14 16 18)

## Determines the proportion of queries given to Proteus as a sample
# Must be >= 0.0 && <= 1.0
# Default sample size for in-memory benchmarks is 20K, see paper for sample size justification
samplerate_arr=(0.02)

### SuRF 

## Hash and Real Suffix Lengths
# These values must be non-negative integers and they control SuRF's memory usage.
# Hash suffixes are only useful for point queries 
surfhlen_arr=(0)
surfrlen_arr=(2)


################################################################

REPO_DIR="$(pwd)"
SOSD_DIR="$REPO_DIR/../workloads/SOSD/"
DOMAINS_DIR="$REPO_DIR/../workloads/domains/"
INT_WORKL_BIN="$REPO_DIR/../workloads/int_workload_gen"
STR_WORKL_BIN="$REPO_DIR/../workloads/str_workload_gen"
EXP_BIN="$REPO_DIR/bench"

IS_INT_BENCH="$1"
if [ $IS_INT_BENCH = "1" ]; then
    echo -e "Running Integer Benchmark"
else
    echo -e "Running String Benchmark"
fi

experiment() {
    filter=$1 && shift

    cd "$expdir"
    touch ./experiment_result
    if [ $IS_INT_BENCH = "1" ]; then
        echo -e "### BEGIN INTEGER EXPERIMENT ###" >> ./experiment_result
    else
        echo -e "### BEGIN STRING EXPERIMENT ###" >> ./experiment_result
    fi
    echo -e "### BEGIN EXPERIMENT DESCRIPTION ###" >> ./experiment_result
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
    
    if [ $IS_INT_BENCH = "1" ]; then
        echo "$filter" "$INT_WORKL_BIN" "$SOSD_DIR" "$nkeys" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
        $INT_WORKL_BIN "$SOSD_DIR" "$nkeys" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
    else
        echo "$filter" "$STR_WORKL_BIN" "$DOMAINS_DIR" "$nkeys" "$keylen" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
        $STR_WORKL_BIN "$DOMAINS_DIR" "$nkeys" "$keylen" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
    fi
    
    if [ $filter = "SuRF" ]; then
        echo -e "\tSuRF Hash Suffix Length:\t$surfhlen" >> ./experiment_result
        echo -e "\tSuRF Real Suffix Length:\t$surfrlen" >> ./experiment_result
        echo -e "### END EXPERIMENT DESCRIPTION ###\n\n" >> ./experiment_result
        $EXP_BIN "$IS_INT_BENCH" "$nkeys" "$keylen" "$nqrys" "10" "$filter" "$surfhlen" "$surfrlen" >> ./experiment_result
    
    elif [ $filter = "Proteus" ]; then
        echo -e "\tFilter Bits-per-Key:\t$membudg" >> ./experiment_result
        echo -e "\tSample Rate:\t$samplerate" >> ./experiment_result
        echo -e "### END EXPERIMENT DESCRIPTION ###\n\n" >> ./experiment_result
        $EXP_BIN "$IS_INT_BENCH" "$nkeys" "$keylen" "$nqrys" "$membudg" "$filter" "$samplerate" >> ./experiment_result
    fi

    echo -e "### END EXPERIMENT ###" >> ./experiment_result

    rm -rf ./my_data/
    cat ./experiment_result
    echo -e ""
    rm experiment_result
}

# Proteus Experiments
for nkeys in "${nkeys_arr[@]}"; do
for keylen in "${keylen_arr[@]}"; do   
for nqrys in "${nqrys_arr[@]}"; do
for i in "${!kdist_arr[@]}"; do
for j in "${!minrange_arr[@]}"; do
for pqratio in "${pqratio_arr[@]}"; do
for membudg in "${membudg_arr[@]}"; do
for corrd in "${corrd_arr[@]}"; do
for pnratio in "${pnratio_arr[@]}"; do
for samplerate in "${samplerate_arr[@]}"; do

kdist="${kdist_arr[$i]}"
qdist="${qdist_arr[$i]}"
minrange="${minrange_arr[$j]}"
maxrange="${maxrange_arr[$j]}"

expdir=$(mktemp -d /tmp/proteus_exp.XXXXXXX)
experiment "Proteus"

done
done
done
done
done
done
done
done
done
done


# SuRF experiments
for nkeys in "${nkeys_arr[@]}"; do
for keylen in "${keylen_arr[@]}"; do
for nqrys in "${nqrys_arr[@]}"; do
for i in "${!kdist_arr[@]}"; do
for j in "${!minrange_arr[@]}"; do
for pqratio in "${pqratio_arr[@]}"; do
for corrd in "${corrd_arr[@]}"; do
for pnratio in "${pnratio_arr[@]}"; do
for surfhlen in "${surfhlen_arr[@]}"; do
for surfrlen in "${surfrlen_arr[@]}"; do

kdist="${kdist_arr[$i]}"
qdist="${qdist_arr[$i]}"
minrange="${minrange_arr[$j]}"
maxrange="${maxrange_arr[$j]}"

expdir=$(mktemp -d /tmp/surf_exp.XXXXXXX)
experiment "SuRF"

done
done
done
done
done
done
done
done
done
done
done