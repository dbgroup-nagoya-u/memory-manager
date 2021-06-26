#--------------------------------------------------------------------------------------#
# Configure GoogleTest
#--------------------------------------------------------------------------------------#

set(GTEST_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/googletest")

# Prevent overriding the parent MWCAS_BENCH's compiler/linker settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines the gtest and gtest_main targets.
add_subdirectory("${GTEST_SOURCE_DIR}")

#--------------------------------------------------------------------------------------#
# End of copy and paste
#--------------------------------------------------------------------------------------#

# set compile options for unit tests
set(
  MEMORY_MANAGER_TEST_THREAD_NUM
  "8"
  CACHE STRING
  "The maximum number of threads to perform unit tests."
)

# define function to add unit tests in the same format
function(ADD_MEMORY_MANAGER_TEST MEMORY_MANAGER_TEST_TARGET)
  add_executable(
    ${MEMORY_MANAGER_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${MEMORY_MANAGER_TEST_TARGET}.cpp"
  )
  target_compile_features(
    ${MEMORY_MANAGER_TEST_TARGET} PRIVATE
    "cxx_std_17"
  )
  target_compile_options(
    ${MEMORY_MANAGER_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_include_directories(
    ${MEMORY_MANAGER_TEST_TARGET} PRIVATE
    "${MEMORY_MANAGER_SOURCE_DIR}/include"
  )
  target_link_libraries(
    ${MEMORY_MANAGER_TEST_TARGET} PRIVATE
    pthread
    gtest_main
  )
  add_test(NAME ${MEMORY_MANAGER_TEST_TARGET} COMMAND $<TARGET_FILE:${MEMORY_MANAGER_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_MEMORY_MANAGER_TEST("epoch_test")
ADD_MEMORY_MANAGER_TEST("epoch_guard_test")
ADD_MEMORY_MANAGER_TEST("epoch_manager_test")
ADD_MEMORY_MANAGER_TEST("garbage_list_test")
ADD_MEMORY_MANAGER_TEST("tls_based_memory_manager_test")
