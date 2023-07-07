cmake_minimum_required(VERSION "3.11")

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

  option(
    MEMORY_MANAGER_USE_PERSISTENT_MEMORY
    "perform garbage collection for pages on persistent memory."
    OFF
  )

  #------------------------------------------------------------------------------------#
  # Configure external libraries
  #------------------------------------------------------------------------------------#

  # this library uses multi-threads
  find_package(Threads)

  # use our thread ID manager
  add_subdirectory("external/cpp-utility")

  #------------------------------------------------------------------------------------#
  # Build targets
  #------------------------------------------------------------------------------------#

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

  if(${MEMORY_MANAGER_USE_PERSISTENT_MEMORY})
    find_package(PkgConfig)
    pkg_check_modules(LIBPMEM REQUIRED libpmem)
    pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)

    target_compile_definitions(${PROJECT_NAME} INTERFACE
      MEMORY_MANAGER_USE_PERSISTENT_MEMORY=${MEMORY_MANAGER_USE_PERSISTENT_MEMORY}
    )
    target_include_directories(${PROJECT_NAME} INTERFACE
      "${LIBPMEM_INCLUDE_DIRS}"
      "${LIBPMEMOBJ_INCLUDE_DIRS}"
    )
    target_link_libraries(${PROJECT_NAME} INTERFACE
      ${LIBPMEM_LIBRARIES}
      ${LIBPMEMOBJ_LIBRARIES}
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
