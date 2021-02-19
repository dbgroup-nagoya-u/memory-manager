cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(simple_gc
  VERSION "0.1.0"
  DESCRIPTION "An open-source simple GC library."
  LANGUAGES "CXX"
)

# define source directories
set(SIMPLE_GC_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(GOOGLETEST_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/googletest")

# set default parameters for GC
set(SIMPLE_GC_BUFFER_SIZE "4096" CACHE STRING
  "the size of epoch/garbage ring buffers"
)
set(SIMPLE_GC_PARTITION_NUM "8" CACHE STRING
  "the number of partitions of epoch/garbage ring buffers"
)
set(SIMPLE_GC_INITIAL_GARBAGE_LIST_CAPACITY "128" CACHE STRING
  "the default capacity of garbage lists"
)

# build BzTree as a header-only library
add_library(simple_gc INTERFACE)
target_compile_features(simple_gc INTERFACE "cxx_std_17")
target_compile_options(simple_gc INTERFACE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_compile_definitions(simple_gc INTERFACE
  BUFFER_SIZE=${SIMPLE_GC_BUFFER_SIZE}
  PARTITION_NUM=${SIMPLE_GC_PARTITION_NUM}
  INITIAL_GARBAGE_LIST_CAPACITY=${SIMPLE_GC_INITIAL_GARBAGE_LIST_CAPACITY}
)
target_include_directories(simple_gc INTERFACE
  "${SIMPLE_GC_SOURCE_DIR}/src"
  "${SIMPLE_GC_SOURCE_DIR}/include"
)
target_link_libraries(simple_gc INTERFACE
  pthread
)

# build unit tests if required
option(SIMPLE_GC_BUILD_TESTS, "Build unit tests for simple GC implementations" OFF)
if(${SIMPLE_GC_BUILD_TESTS})
  enable_testing()
  # use GoogleTest for unit tests
  add_subdirectory("${GOOGLETEST_SOURCE_DIR}")
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
