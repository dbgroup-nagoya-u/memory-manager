cmake_minimum_required(VERSION "3.14")

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
  # enable features
  include(FetchContent)

  #----------------------------------------------------------------------------#
  # Configurations
  #----------------------------------------------------------------------------#

  # this library uses multi-threads
  find_package(Threads)

  # use our thread ID manager
  FetchContent_Declare(
    cpp-utility
    GIT_REPOSITORY "https://github.com/dbgroup-nagoya-u/cpp-utility.git"
    GIT_TAG "d78f5873934ce67b73215b8d44dc97e1d7140199"
  )
  FetchContent_MakeAvailable(cpp-utility)

  #----------------------------------------------------------------------------#
  # Build targets
  #----------------------------------------------------------------------------#

  add_library(${PROJECT_NAME} INTERFACE)
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} INTERFACE
    "cxx_std_17"
  )
  target_include_directories(${PROJECT_NAME} INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(${PROJECT_NAME} INTERFACE
    Threads::Threads
    dbgroup::cpp_utility
  )

  #----------------------------------------------------------------------------#
  # Build unit tests
  #----------------------------------------------------------------------------#

  # build unit tests if required
  option(MEMORY_MANAGER_BUILD_TESTS "build unit tests for this repository" OFF)
  if(${MEMORY_MANAGER_BUILD_TESTS})
    enable_testing()
    # include unit tests
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
  endif()
endif()
