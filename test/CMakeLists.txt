#------------------------------------------------------------------------------#
# Configure Sanitizers
#------------------------------------------------------------------------------#

set(
  MEMORY_MANAGER_TEST_SANITIZER
  "none" CACHE STRING
  "Sanitizers for unit tests and their dependencies (none, thread, or address)."
)
set_property(CACHE MEMORY_MANAGER_TEST_SANITIZER PROPERTY STRINGS none thread address)

if(MEMORY_MANAGER_TEST_SANITIZER STREQUAL "thread")
  set(DBGROUP_SANITIZER_FLAGS -fsanitize=thread)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    message(WARNING
      "[memory_manager] GCC's ThreadSanitizer may abort with \"unexpected memory "
      "mapping\" under address space layout randomization. Use Clang 18 or later, "
      "or run tests via \"setarch $(uname -m) -R\".")
  endif()
elseif(MEMORY_MANAGER_TEST_SANITIZER STREQUAL "address")
  # make UBSan errors fail unit tests
  set(DBGROUP_SANITIZER_FLAGS
    -fsanitize=address,undefined
    -fno-sanitize-recover=undefined
    -fno-omit-frame-pointer
  )
elseif(NOT MEMORY_MANAGER_TEST_SANITIZER STREQUAL "none")
  message(FATAL_ERROR
    "[memory_manager] MEMORY_MANAGER_TEST_SANITIZER must be none, thread, or address.")
endif()

if(DBGROUP_SANITIZER_FLAGS)
  # instrument GoogleTest and unit tests defined below
  add_compile_options(${DBGROUP_SANITIZER_FLAGS})
  string(REPLACE ";" " " DBGROUP_SANITIZER_LINK_FLAGS "${DBGROUP_SANITIZER_FLAGS}")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${DBGROUP_SANITIZER_LINK_FLAGS}")

  # instrument our libraries built in the parent directory
  foreach(DBGROUP_DEP_TARGET cpp_utility memory_manager)
    if(TARGET ${DBGROUP_DEP_TARGET})
      target_compile_options(${DBGROUP_DEP_TARGET} PRIVATE ${DBGROUP_SANITIZER_FLAGS})
    endif()
  endforeach()
endif()

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
    "cxx_std_20"
  )
  target_compile_options(${DBGROUP_TEST_TARGET} PRIVATE
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Release">:-march=native>
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Debug">:-g3>
    -Wall
    -Wextra
  )
  target_compile_definitions(${DBGROUP_TEST_TARGET} PRIVATE
    DBGROUP_TEST_THREAD_NUM=${DBGROUP_TEST_THREAD_NUM}
  )
  target_link_libraries(${DBGROUP_TEST_TARGET} PRIVATE
    dbgroup::${PROJECT_NAME}
    GTest::gtest_main
  )
  if(MEMORY_MANAGER_TEST_SANITIZER STREQUAL "thread")
    # embed the suppression file into default ThreadSanitizer options
    target_sources(${DBGROUP_TEST_TARGET} PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/tsan_default_options.cpp"
    )
    target_compile_definitions(${DBGROUP_TEST_TARGET} PRIVATE
      MEMORY_MANAGER_TSAN_SUPPRESSIONS="${CMAKE_CURRENT_SOURCE_DIR}/tsan.supp"
    )
  endif()

  add_test(NAME ${DBGROUP_TEST_TARGET} COMMAND $<TARGET_FILE:${DBGROUP_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
DBGROUP_ADD_TEST("list_holder_test")
DBGROUP_ADD_TEST("epoch_based_gc_test")
DBGROUP_ADD_TEST("mapping_table_test")
