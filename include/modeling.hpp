#pragma once
#ifndef PROTEUS_MODELING_HPP
#define PROTEUS_MODELING_HPP

#include <vector>
#include <memory>
#include <tuple>
#include <cassert>
#include <inttypes.h>
#include <cstring>
#include <cmath>
#include <type_traits>
#include <chrono>

#include "config.hpp"

namespace proteus {

typedef std::array<std::pair<size_t, size_t>, 64> bin_array;

template<typename T>
std::tuple<size_t, size_t, size_t> modeling(const std::vector<T>& keys, 
                                            const std::vector<std::pair<T, T>>& sample_queries, 
                                            const double bits_per_key,
                                            const size_t max_klen, 
                                            std::vector<size_t>* sparse_dense_cutoffs = nullptr);

} // namespace proteus

#endif