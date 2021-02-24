#--------------------------------------------------------------------------------------#
# Copy and paste from GoogleTest doc.
#--------------------------------------------------------------------------------------#
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)
#--------------------------------------------------------------------------------------#
# End of copy and paste
#--------------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(ADD_SIMPLE_GC_TEST SIMPLE_GC_TARGET_DIR SIMPLE_GC_TEST_TARGET)
  set(TEST_TARGET "${SIMPLE_GC_TARGET_DIR}_${SIMPLE_GC_TEST_TARGET}")
  add_executable(${TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${SIMPLE_GC_TARGET_DIR}/${SIMPLE_GC_TEST_TARGET}.cpp"
  )
  target_compile_features(${TEST_TARGET} PRIVATE "cxx_std_17")
  target_compile_options(${TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_compile_definitions(${TEST_TARGET} PRIVATE
    BUFFER_SIZE=${SIMPLE_GC_BUFFER_SIZE}
    PARTITION_NUM=${SIMPLE_GC_PARTITION_NUM}
    INITIAL_GARBAGE_LIST_CAPACITY=${SIMPLE_GC_INITIAL_GARBAGE_LIST_CAPACITY}
  )
  target_include_directories(${TEST_TARGET} PRIVATE
    "${SIMPLE_GC_SOURCE_DIR}/include"
  )
  target_link_libraries(${TEST_TARGET} PRIVATE
    pthread
    gtest_main
  )
  add_test(NAME ${TEST_TARGET} COMMAND $<TARGET_FILE:${TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_SIMPLE_GC_TEST("ring_based" "epoch_manager_test")
ADD_SIMPLE_GC_TEST("ring_based" "epoch_guard_test")
ADD_SIMPLE_GC_TEST("ring_based" "garbage_list_test")
ADD_SIMPLE_GC_TEST("ring_based" "ring_buffer_gc_test")
ADD_SIMPLE_GC_TEST("tls_based" "epoch_test")
ADD_SIMPLE_GC_TEST("tls_based" "epoch_guard_test")
ADD_SIMPLE_GC_TEST("tls_based" "epoch_manager_test")
