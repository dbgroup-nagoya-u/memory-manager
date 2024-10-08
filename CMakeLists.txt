cmake_minimum_required(VERSION "3.14")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  memory_manager
  VERSION "0.3.0"
  DESCRIPTION "A memory management library."
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
    GIT_TAG "0336b0448a7bb0ba2d2e78e6c32e8d9b785a317f"
  )
  FetchContent_MakeAvailable(cpp-utility)

  #----------------------------------------------------------------------------#
  # Build targets
  #----------------------------------------------------------------------------#

  add_library(${PROJECT_NAME} STATIC
    "${CMAKE_CURRENT_SOURCE_DIR}/src/component/garbage_list.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/component/reuse_list.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/mapping_table.cpp"
  )
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} PUBLIC
    "cxx_std_17"
  )
  target_include_directories(${PROJECT_NAME} PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(${PROJECT_NAME} PUBLIC
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
