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

# build BzTree as a header-only library
add_library(simple_gc INTERFACE)
target_compile_features(simple_gc INTERFACE "cxx_std_20")
target_compile_options(simple_gc INTERFACE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_include_directories(simple_gc INTERFACE
  "${SIMPLE_GC_SOURCE_DIR}/src"
  "${SIMPLE_GC_SOURCE_DIR}/include"
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
