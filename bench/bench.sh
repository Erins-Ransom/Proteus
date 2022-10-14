#!/bin/bash

REPO_DIR="$(pwd)"
SOSD_DIR="$REPO_DIR/../SOSD/"
INT_WORKL_BIN="$REPO_DIR/../int_workload"
STR_WORKL_BIN="$REPO_DIR/../str_workload"
EXP_BIN="$REPO_DIR/bench"

IS_INT_BENCH="$1"
if [ $IS_INT_BENCH = "1" ]; then
    echo -e "Running Integer Benchmark"
else
    echo -e "Running String Benchmark"
fi

experiment() {
    nkeys=$1 && shift
    keylen=$1 && shift
    nqrys=$1 && shift
    kdist=$1 && shift
    qdist=$1 && shift
    pqratio=$1 && shift
    membudg=$1 && shift
    filter=$1 && shift
    expdir=$1 && shift
    corrd=$1 && shift
    pnratio=$1 && shift

    cd "$expdir"
    touch ./experiment_result
    if [ $IS_INT_BENCH = "1" ]; then
        echo -e "### BEGIN INTEGER EXPERIMENT ###" >> ./experiment_result
    else
        echo -e "### BEGIN STRING EXPERIMENT ###" >> ./experiment_result
    fi
    # echo -e "$expdir" >> ./experiment_result
    echo -e "### BEGIN EXPERIMENT DESCRIPTION ###" >> ./experiment_result
    echo -e "\tFilter name:\t$filter" >> ./experiment_result
    echo -e "\tNumber of keys:\t$nkeys" >> ./experiment_result
    echo -e "\tNumber of bits in a key:\t$keylen" >> ./experiment_result
    echo -e "\tNumber of queries:\t$nqrys" >> ./experiment_result
    echo -e "\tKey distribution:\t$kdist" >> ./experiment_result
    echo -e "\tQuery distribution:\t$qdist" >> ./experiment_result
    echo -e "\tMinimum range:\t$minrange" >> ./experiment_result
    echo -e "\tMaximum range:\t$maxrange" >> ./experiment_result
    echo -e "\tPoint-Query ratio:\t$pqratio" >> ./experiment_result
    echo -e "\tFilter bits-per-key:\t$membudg" >> ./experiment_result
    echo -e "\tCorrelation degree:\t$corrd" >> ./experiment_result
    echo -e "\tPositive-Negative-Query ratio:\t$pnratio" >> ./experiment_result
    
    if [ $IS_INT_BENCH = "1" ]; then
        echo "$filter" "$INT_WORKL_BIN" "$SOSD_DIR" "$nkeys" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
        $INT_WORKL_BIN "$SOSD_DIR" "$nkeys" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
    else
        echo "$filter" "$STR_WORKL_BIN" "$nkeys" "$keylen" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
        $STR_WORKL_BIN "$nkeys" "$keylen" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio" "$corrd"
    fi
    
    if [ $filter = "SuRF" ]; then
        surfhlen=$1 && shift
        surfrlen=$1 && shift
        echo -e "\tSurf hash suffix length:\t$surfhlen" >> ./experiment_result
        echo -e "\tSurf real suffix length:\t$surfrlen" >> ./experiment_result
        echo -e "### END EXPERIMENT DESCRIPTION ###\n\n" >> ./experiment_result
        $EXP_BIN "$IS_INT_BENCH" "$nkeys" "$keylen" "$nqrys" "$membudg" "$filter" "$surfhlen" "$surfrlen" >> ./experiment_result
    
    elif [ $filter = "Proteus" ]; then
        samplerate=$1 && shift
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

# STRING

# keylen_arr=(1440) # MUST be a multiple of 8 for strings
# nkeys_arr=(10000000)
# kdist_arr=("kuniform" "kuniform" "knormal" "knormal")
# nqrys_arr=(1000000) 
# qdist_arr=("quniform" "qcorrelated" "qsplit" "qcorrelated")
# minrange_arr=(2) 
# maxrange_arr=(1073741824)
# pqratio_arr=(0.0) # Must be >= 0.0 && <= 1.0
# pnratio_arr=(0.0) # Must be >= 0.0 && <= 1.0
# corrd_arr=(536870912) # Must be >= 1

################################################################

# INTEGER

keylen_arr=(64) # Should only be 64
nkeys_arr=(10000000)
kdist_arr=("kuniform" "kuniform" "knormal" "knormal" "knormal")
nqrys_arr=(1000000) 
qdist_arr=("quniform" "qcorrelated" "qsplit" "qcorrelated" "quniform")
minrange_arr=(2) 
maxrange_arr=(4096)
pqratio_arr=(0.0) # Must be >= 0.0 && <= 1.0
pnratio_arr=(0.0) # Must be >= 0.0 && <= 1.0
corrd_arr=(1024) # Must be >= 1

################################################################

membudg_arr=(8 10 12 14 16 18)
surfhlen_arr=(0)
surfrlen_arr=(2)
samplerate_arr=(0.02)

expdir_arr=()


# Proteus Experiments
for nkeys in "${nkeys_arr[@]}"; do
for keylen in "${keylen_arr[@]}"; do   
for nqrys in "${nqrys_arr[@]}"; do
# for kdist in "${kdist_arr[@]}"; do
# for qdist in "${qdist_arr[@]}"; do
for i in "${!kdist_arr[@]}"; do
# for minrange in "${minrange_arr[@]}"; do
# for maxrange in "${maxrange_arr[@]}"; do
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

expdir=$(mktemp -d /tmp/proteus_experiment.XXXXXXX)
expdir_arr+=("$expdir")                                                
experiment $nkeys $keylen $nqrys $kdist $qdist $pqratio $membudg "Proteus" $expdir $corrd $pnratio $samplerate

# done
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
# for kdist in "${kdist_arr[@]}"; do
# for qdist in "${qdist_arr[@]}"; do
for i in "${!kdist_arr[@]}"; do
# for minrange in "${minrange_arr[@]}"; do
# for maxrange in "${maxrange_arr[@]}"; do
for j in "${!minrange_arr[@]}"; do
for pqratio in "${pqratio_arr[@]}"; do
for membudg in 10; do
for corrd in "${corrd_arr[@]}"; do
for pnratio in "${pnratio_arr[@]}"; do
for surfhlen in "${surfhlen_arr[@]}"; do
for surfrlen in "${surfrlen_arr[@]}"; do

kdist="${kdist_arr[$i]}"
qdist="${qdist_arr[$i]}"
minrange="${minrange_arr[$j]}"
maxrange="${maxrange_arr[$j]}"

expdir=$(mktemp -d /tmp/surf_experiment.XXXXXXX)
expdir_arr+=("$expdir")
experiment $nkeys $keylen $nqrys $kdist $qdist $pqratio $membudg "SuRF" "$expdir" $corrd $pnratio $surfhlen $surfrlen

# done
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