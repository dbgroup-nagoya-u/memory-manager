cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  memory_manager
  VERSION "0.1.0"
  DESCRIPTION "An open-source simple GC library."
  LANGUAGES "CXX"
)

if(NOT TARGET dbgroup::${PROJECT_NAME})
  #------------------------------------------------------------------------------------#
  # Turning parameters
  #------------------------------------------------------------------------------------#

  set(
    MEMORY_MANAGER_GARBAGE_BUFFER_SIZE
    "1024" CACHE STRING
    "the size of buffers for retaining garbages."
  )

  set(
    MEMORY_MANAGER_EXPECTED_THREAD_NUM
    "128" CACHE STRING
    "the expected number of worker threads."
  )

  #------------------------------------------------------------------------------------#
  # Configure external libraries
  #------------------------------------------------------------------------------------#

  # this library uses multi-threads
  find_package(Threads)

  #------------------------------------------------------------------------------------#
  # Build targets
  #------------------------------------------------------------------------------------#

  add_library(${PROJECT_NAME} INTERFACE)
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} INTERFACE
    "cxx_std_17"
  )
  target_compile_definitions(memory_manager INTERFACE
    MEMORY_MANAGER_GARBAGE_BUFFER_SIZE=${MEMORY_MANAGER_GARBAGE_BUFFER_SIZE}
    MEMORY_MANAGER_EXPECTED_THREAD_NUM=${MEMORY_MANAGER_EXPECTED_THREAD_NUM}
  )
  target_include_directories(${PROJECT_NAME} INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(${PROJECT_NAME} INTERFACE
    Threads::Threads
  )
endif()

#--------------------------------------------------------------------------------------#
# Build unit tests
#--------------------------------------------------------------------------------------#

# build unit tests if required
option(MEMORY_MANAGER_BUILD_TESTS "build unit tests for this repository" OFF)
if(${MEMORY_MANAGER_BUILD_TESTS})
  enable_testing()
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
