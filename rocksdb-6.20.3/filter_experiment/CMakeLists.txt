add_library(FilterExperimentUtil filter_exp_util.cc)

add_executable(filter_experiment filter_experiment.cc)
target_include_directories (filter_experiment PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(filter_experiment LINK_PUBLIC FilterExperimentUtil rocksdb -lstdc++fs)