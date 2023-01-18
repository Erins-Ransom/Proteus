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

## NOTE: LINKED PARAMETERS
#  By default, we loop through all combinations of parameters, but linked parameters 
#  are considered as a single parameter.  i.e. 
#
#       kdist_arr=("kuniform" "kuniform")
#       qdist_arr=("quniform" "qcorrelated")
#
#  Corresponds to two workloads with uniform key distributions, one with 
#  uniform queries and the other with correlated queries.


## 1. Key Length (in bits)
#######################################
# For integer workloads, this value MUST be 64
# For synthetic string workloads, this value MUST be a multiple of 8
# For the string domain workload, this value is ignored.


## 2. Number of Keys
## 3. Number of Queries
#######################################


## [LINKED]
## 4. Key Distribution
## 5. Query Distribution
#######################################
# For SOSD / Domain workloads, please download the relevant datasets and follow the instructions in the README
# SOSD and Domain Key and Query Distributions must be specified together, e.g. ksosd_books + qsosd_books
# Integer Key Distributions: kuniform, knormal, ksosd_books, ksosd_fb
# Integer Query Distributions: quniform, qnormal, qcorrelated, qsplit, qsosd_books, qsosd_fb
# String Key Distributions: kuniform, knormal, kdomain
# String Query Distributions: quniform, qcorrelated, qsplit, qdomain


## [LINKED]
## 6. Minimum Range Size
## 7. Maximum Range Size
## 8. Point / Range Query Ratio
#######################################
# Min Range Size must be >= 2
# Point / Range Query Ratio must be >= 0.0 && <= 1.0
# Use 0.0 for all range queries and 1.0 for all point queries
# For qsplit and pqratio <= 0.5, all point queries are assigned to the correlated distribution.
# For qsplit and pqratio > 0.5, half the workload comprises correlated point queries and the 
# remaining point queries are assigned to the uniform distribution.


## 9. Positive / Negative Query Ratio
#######################################
# Must be >= 0.0 && <= 1.0
# Use 0.0 for all negative queries and 1.0 for all positive queries


## 10. Query Correlation Degree
#######################################
# The distance between the left query bound and a key is uniformly distributed between 1 and the specified value
# Must be >= 1


### INTEGER

# IS_INT_BENCH="1"
# keylen_arr=(64)
# nkeys_arr=(10000000)
# nqrys_arr=(1000000)
# kdist_arr=("ksosd_fb" "ksosd_books" "kuniform" "kuniform" "knormal" "knormal" "knormal")
# qdist_arr=("qsosd_fb" "qsosd_books" "quniform" "qcorrelated" "qsplit" "qcorrelated" "quniform")
# minrange_arr=(2 2 2 2) 
# maxrange_arr=(2 32 4096 1024) 
# pqratio_arr=(1.0 0.0 0.0 0.5)
# pnratio_arr=(0.0)
# corrd_arr=(1024)

### STRING

IS_INT_BENCH="0"
keylen_arr=(1440)
nkeys_arr=(10000000)
nqrys_arr=(1000000)
kdist_arr=("kuniform" "knormal" "kuniform" "kdomain")
qdist_arr=("quniform" "qcorrelated" "qcorrelated" "qdomain")
minrange_arr=(2) 
maxrange_arr=(1073741824) 
pqratio_arr=(0.0)
pnratio_arr=(0.0)
corrd_arr=(536870912)


if [[ "${IS_INT_BENCH}" == "1" ]]; then
    echo -e "Running Integer Benchmark"
else
    echo -e "Running String Benchmark"
fi


###############################################
############   FILTER PARAMETERS   ############
###############################################


### Proteus

## Filter Bits-per-Key
#######################################
# Must be a positive real number
membudg_arr=(8 10 12 14 16 18 20)

## Sample Proportion
#######################################
# Determines the proportion of queries given to Proteus as a sample
# Must be >= 0.0 && <= 1.0
# Default sample size for in-memory benchmarks is 20K, see paper for sample size justification
sampleprop_arr=(0.02)


### SuRF

## Hash and Real Suffix Lengths
#####################################
# These values must be non-negative integers and they control SuRF's memory usage.
# Hash suffixes are only useful for point queries 
surfhlen_arr=(0)
surfrlen_arr=(0 2 4 6 8 10)


################################################################

REPO_DIR="$(pwd)"
SOSD_DIR="$REPO_DIR/../workloads/SOSD/"
DOMAINS="$REPO_DIR/../workloads/domains/domains.txt"
DOMAINS_SHUFFLED="$REPO_DIR/../workloads/domains/shuf.txt"
INT_WORKL_BIN="$REPO_DIR/../workloads/int_workload_gen"
STR_WORKL_BIN="$REPO_DIR/../workloads/str_workload_gen"
EXP_BIN="$REPO_DIR/bench"

experiment() {
    filter=$1 && shift

    cd "$expdir"
    touch ./experiment_result
    if [[ "${IS_INT_BENCH}" == "1" ]]; then
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
        # Shuffle domains on every run
        if [ $kdist = "kdomain" ]; then
            cat $DOMAINS | shuf -n $(($nkeys + $nqrys)) -o $DOMAINS_SHUFFLED
        fi
        echo "$filter" "$STR_WORKL_BIN" "$DOMAINS_SHUFFLED" "$nkeys" "$keylen" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
        $STR_WORKL_BIN "$DOMAINS_SHUFFLED" "$nkeys" "$keylen" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
    fi
    
    if [ $filter = "SuRF" ]; then
        echo -e "\tSuRF Hash Suffix Length:\t$surfhlen" >> ./experiment_result
        echo -e "\tSuRF Real Suffix Length:\t$surfrlen" >> ./experiment_result
        echo -e "### END EXPERIMENT DESCRIPTION ###\n\n" >> ./experiment_result
        $EXP_BIN "$IS_INT_BENCH" "$filter" "$surfhlen" "$surfrlen" >> ./experiment_result
    
    elif [ $filter = "Proteus" ]; then
        echo -e "\tFilter Bits-per-Key:\t$membudg" >> ./experiment_result
        echo -e "\tSample Proportion:\t$sampleprop" >> ./experiment_result
        echo -e "### END EXPERIMENT DESCRIPTION ###\n\n" >> ./experiment_result
        $EXP_BIN "$IS_INT_BENCH" "$filter" "$membudg" "$sampleprop" >> ./experiment_result
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
for membudg in "${membudg_arr[@]}"; do
for corrd in "${corrd_arr[@]}"; do
for pnratio in "${pnratio_arr[@]}"; do
for sampleprop in "${sampleprop_arr[@]}"; do

kdist="${kdist_arr[$i]}"
qdist="${qdist_arr[$i]}"
minrange="${minrange_arr[$j]}"
maxrange="${maxrange_arr[$j]}"
pqratio="${pqratio_arr[$j]}"

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


# SuRF experiments
for nkeys in "${nkeys_arr[@]}"; do
for keylen in "${keylen_arr[@]}"; do
for nqrys in "${nqrys_arr[@]}"; do
for i in "${!kdist_arr[@]}"; do
for j in "${!minrange_arr[@]}"; do
for corrd in "${corrd_arr[@]}"; do
for pnratio in "${pnratio_arr[@]}"; do
for surfhlen in "${surfhlen_arr[@]}"; do
for surfrlen in "${surfrlen_arr[@]}"; do

kdist="${kdist_arr[$i]}"
qdist="${qdist_arr[$i]}"
minrange="${minrange_arr[$j]}"
maxrange="${maxrange_arr[$j]}"
pqratio="${pqratio_arr[$j]}"

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
