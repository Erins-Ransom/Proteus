cmake_minimum_required (VERSION 2.6)
project (SuRF)

message(STATUS "Configuring..." ${CMAKE_PROJECT_NAME})

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif()
message(STATUS "Here1.........." ${CMAKE_PROJECT_NAME})

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -g -Wall -mpopcnt -pthread -std=c++11")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3 -Wall -Werror -mpopcnt -pthread -std=c++11")

option(COVERALLS "Generate coveralls data" OFF)
message(STATUS "Here2.........." ${CMAKE_PROJECT_NAME})

if (COVERALLS)
  include("${CMAKE_CURRENT_SOURCE_DIR}/CodeCoverage.cmake")
  append_coverage_compiler_flags()
  set(COVERAGE_EXCLUDES 'ARF/*' 'bench/*' 'test/*' '/usr/*' '/lib/*')
  setup_target_for_coverage(
    NAME coverage
    EXECUTABLE make test
    )
else()
  add_definitions(-DNDEBUG)
endif()
message(STATUS "Here3.........." ${CMAKE_PROJECT_NAME})
enable_testing()
message(STATUS "Here4.........." ${CMAKE_PROJECT_NAME})
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include")

#add_subdirectory(test)
message(STATUS "Here5.........." ${CMAKE_PROJECT_NAME})
add_subdirectory(bench)

#include_directories("${CMAKE_CURRENT_SOURCE_DIR}/ARF/include")
#add_subdirectory(ARF)
