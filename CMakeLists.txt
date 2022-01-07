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

# define source directories
set(MEMORY_MANAGER_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET memory_manager::memory_manager)
  # build as a header-only library
  add_library(memory_manager INTERFACE)
  add_library(memory_manager::memory_manager ALIAS memory_manager)
  target_compile_features(memory_manager INTERFACE
    "cxx_std_17"
  )
  target_include_directories(memory_manager INTERFACE
    "${MEMORY_MANAGER_SOURCE_DIR}/include"
  )
  target_link_libraries(memory_manager INTERFACE
    pthread
  )

  #------------------------------------------------------------------------------------#
  # Build options
  #------------------------------------------------------------------------------------#

  if(DEFINED MEMORY_MANAGER_GARBAGE_BUFFER_SIZE)
    target_compile_definitions(memory_manager INTERFACE
      MEMORY_MANAGER_GARBAGE_BUFFER_SIZE=${MEMORY_MANAGER_GARBAGE_BUFFER_SIZE}
    )
  endif()
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
