#--------------------------------------------------------------------------------------#
# Configure GoogleTest
#--------------------------------------------------------------------------------------#

include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY "https://github.com/google/googletest.git"
  GIT_TAG "58d77fa8070e8cec2dc1ed015d66b454c8d78850" # v1.12.1
)
FetchContent_MakeAvailable(googletest)

#--------------------------------------------------------------------------------------#
# Build parameters
#--------------------------------------------------------------------------------------#

set(
  DBGROUP_TEST_THREAD_NUM
  "" CACHE STRING
  "The number of worker threads for testing."
)

set(
  DBGROUP_TEST_TMP_PMEM_PATH
  "/tmp" CACHE STRING
  "The path to a durable storage."
)

#--------------------------------------------------------------------------------------#
# Configurations for testing
#--------------------------------------------------------------------------------------#

if ("${DBGROUP_TEST_THREAD_NUM}" STREQUAL "")
  message("[${PROJECT_NAME}:test] DBGROUP_TEST_THREAD_NUM is not set, so use defaults.")

  # check the number of logical cores
  include(ProcessorCount)
  ProcessorCount(DBGROUP_TEST_MAX_CORES)
  if(DBGROUP_TEST_MAX_CORES LESS 8)
    set(DBGROUP_TEST_THREAD_NUM "2" CACHE STRING "The number of worker threads for testing." FORCE)
  else()
    set(DBGROUP_TEST_THREAD_NUM "8" CACHE STRING "The number of worker threads for testing." FORCE)
  endif()
endif()
message("[${PROJECT_NAME}:test] Use ${DBGROUP_TEST_THREAD_NUM} as the number of worker threads for testing.")

#--------------------------------------------------------------------------------------#
# Build Unit Tests
#--------------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(DBGROUP_ADD_TEST DBGROUP_TEST_TARGET)
  add_executable(${DBGROUP_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${DBGROUP_TEST_TARGET}.cpp"
  )
  target_compile_features(${DBGROUP_TEST_TARGET} PRIVATE
    "cxx_std_17"
  )
  target_compile_options(${DBGROUP_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_compile_definitions(${DBGROUP_TEST_TARGET} PRIVATE
    DBGROUP_TEST_THREAD_NUM=${DBGROUP_TEST_THREAD_NUM}
  )
  target_link_libraries(${DBGROUP_TEST_TARGET} PRIVATE
    dbgroup::${PROJECT_NAME}
    GTest::gtest_main
  )

  if(${MEMORY_MANAGER_USE_PERSISTENT_MEMORY})
    target_compile_definitions(${DBGROUP_TEST_TARGET} PRIVATE
      DBGROUP_TEST_TMP_PMEM_PATH=${DBGROUP_TEST_TMP_PMEM_PATH}
    )
  endif()

  add_test(NAME ${DBGROUP_TEST_TARGET} COMMAND $<TARGET_FILE:${DBGROUP_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
DBGROUP_ADD_TEST("epoch_test")
DBGROUP_ADD_TEST("epoch_guard_test")
DBGROUP_ADD_TEST("epoch_manager_test")
DBGROUP_ADD_TEST("garbage_list_test")
DBGROUP_ADD_TEST("epoch_based_gc_test")

if(${MEMORY_MANAGER_USE_PERSISTENT_MEMORY})
  DBGROUP_ADD_TEST("garbage_list_on_pmem_test")
  DBGROUP_ADD_TEST("epoch_based_gc_on_pmem_test")
endif()
