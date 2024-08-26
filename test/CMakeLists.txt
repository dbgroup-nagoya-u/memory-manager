#------------------------------------------------------------------------------#
# Configure GoogleTest
#------------------------------------------------------------------------------#

include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY "https://github.com/google/googletest.git"
  GIT_TAG "f8d7d77c06936315286eb55f8de22cd23c188571" # v1.14.0
)
FetchContent_MakeAvailable(googletest)

#------------------------------------------------------------------------------#
# Build parameters
#------------------------------------------------------------------------------#

set(
  DBGROUP_TEST_THREAD_NUM
  "2" CACHE STRING
  "The number of worker threads for testing."
)

#------------------------------------------------------------------------------#
# Build Unit Tests
#------------------------------------------------------------------------------#

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

  add_test(NAME ${DBGROUP_TEST_TARGET} COMMAND $<TARGET_FILE:${DBGROUP_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
DBGROUP_ADD_TEST("garbage_list_test")
DBGROUP_ADD_TEST("epoch_based_gc_test")
DBGROUP_ADD_TEST("mapping_table_test")
