# Proteus: A Self Designing Range Filter

We introduce Proteus, a novel self-designing approximate range filter, which configures itself based on sampled data in order to optimize its false positive rate (FPR) for a given space requirement.
Proteus unifies the probabilistic and deterministic design spaces of state-of-the-art range filters to achieve robust performance across a larger variety of use cases.
At the core of Proteus lies our Contextual Prefix FPR (CPFPR) model -- a formal framework for the FPR of prefix-based filters across their design spaces.
We empirically demonstrate the accuracy of our model and Proteus' ability to optimize over both synthetic workloads and real-world datasets.
We further evaluate Proteus in RocksDB and show that it is able to improve end-to-end performance by as much as 5.3x over more brittle state-of-the-art methods such as SuRF and Rosetta.
Our experiments also indicate that the cost of modeling is not significant compared to the end-to-end performance gains and that Proteus is robust to workload shifts.

Eric R. Knorr, Baptiste Lemaire, Andrew Lim, Siqiang Luo, Huanchen Zhang, Stratos Idreos, and Michael Mitzenmacher. 2022. Proteus: A Self-Designing Range Filter. In Proceedings of the 2022 International Conference on Management of Data (SIGMOD '22). Association for Computing Machinery, New York, NY, USA, 1670–1684. https://doi.org/10.1145/3514221.3526167

[arXiv link](https://arxiv.org/abs/2207.01503)


Note: This code has been updated with several modeling optimizations since the release of the paper. Experimental results may differ but should be no worse than reported in the paper.


## Directory Layout
`include/` - contains all the files necessary to the implementation of the Proteus filter and the CPFPR model.

`bench/` - contains all the files used for in-memory standalone filter benchmarks.

`workloads/` - contains all the files used for generating synthetic workloads and downloading rreal-world datasets.

`rocksdb/` - contains a modified version of RockDB with Proteus and SuRF integrated (To be Released).

`SuRF/` - git submodule containing the SuRF repository. 

## Requirements

- `jq` (to download Internet domains dataset)
- (RocksDB) `CMake`, `gtest`, `lz4`, `gflags`, `zstandard`

```	
# For Ubuntu
sudo apt-get install jq build-essential cmake libgtest.dev liblz4-dev libzstd-dev libgflags-dev
```

## Setup

Note: use either the `--recursive` parameter when cloning the repositiory or use the `submodule update` command in order to clone the SuRF submodule.  


	git clone --recursive https://github.com/Eric-R-Knorr/Proteus.git
	cd Proteus/workloads
	./setup.sh

	
`setup.sh` - compiles string and integer workload generators and retrieves real-world datasets - `books_800M_uint64` and `fb_200M_uint64` from https://github.com/learnedsystems/SOSD, `.org` domains from https://domainsproject.org/


# Standalone In-Memory Filter Benchmarks

Configure the workload setup as detailed in `bench.sh`.

	cd Proteus/bench
	make bench
	./bench.sh &> bench.txt