# define function to add unit tests in the same format
function(ADD_SIMPLE_GC_TEST SIMPLE_GC_TARGET_DIR SIMPLE_GC_TEST_TARGET)
  add_executable(${SIMPLE_GC_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${SIMPLE_GC_TARGET_DIR}/${SIMPLE_GC_TEST_TARGET}.cpp"
  )
  target_compile_features(${SIMPLE_GC_TEST_TARGET} PRIVATE "cxx_std_20")
  target_compile_options(${SIMPLE_GC_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_include_directories(${SIMPLE_GC_TEST_TARGET} PRIVATE
    "${SIMPLE_GC_SOURCE_DIR}/src"
    "${SIMPLE_GC_SOURCE_DIR}/include"
    "${GOOGLETEST_SOURCE_DIR}/include"
  )
  target_link_libraries(${SIMPLE_GC_TEST_TARGET} PRIVATE
    pthread
    gtest_main
  )
  add_dependencies(${SIMPLE_GC_TEST_TARGET}
    gtest_main
  )
  add_test(NAME ${SIMPLE_GC_TEST_TARGET} COMMAND $<TARGET_FILE:${SIMPLE_GC_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_SIMPLE_GC_TEST("epoch_based" "epoch_manager_test")