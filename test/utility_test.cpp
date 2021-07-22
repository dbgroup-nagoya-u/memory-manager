/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "memory/utility.hpp"

#include <memory>

#include "gtest/gtest.h"

namespace dbgroup::memory::test
{
struct MyStruct {
  uint64_t a;
  uint32_t b;

  constexpr MyStruct() : a{}, b{} {}
  constexpr MyStruct(const uint64_t a, const uint32_t b) : a{a}, b{b} {}

  constexpr bool
  operator==(const MyStruct& obj) const
  {
    return this->a == obj.a && this->b == obj.b;
  }
};

template <class T>
class UtilityFixture : public ::testing::Test
{
 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr uint64_t kULVal = 1;

  static constexpr uint32_t kUVal = 2;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  T expected;

  /*################################################################################################
   * Test setup/teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    if constexpr (std::is_same_v<T, uint64_t>) {
      expected = kULVal;
    } else if constexpr (std::is_same_v<T, uint32_t>) {
      expected = kUVal;
    } else if constexpr (std::is_same_v<T, MyStruct>) {
      expected = MyStruct{kULVal, kUVal};
    }
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyNewWithoutArgs()
  {
    auto obj = New<T>();
    Delete(obj);
  }

  void
  VerifyNewWithArgs()
  {
    T* obj;
    if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>) {
      obj = New<T>(expected);
      EXPECT_EQ(expected, *obj);
    } else if constexpr (std::is_same_v<T, MyStruct>) {
      obj = New<T>(kULVal, kUVal);
    } else {
      return;
    }

    EXPECT_EQ(expected, *obj);

    Delete(obj);
  }

  void
  VerifyMallocNewWithoutArgs()
  {
    auto obj = MallocNew<T>(sizeof(T));
    Delete(obj);
  }

  void
  VerifyMallocNewWithArgs()
  {
    T* obj;
    if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>) {
      obj = MallocNew<T>(sizeof(T), expected);
    } else if constexpr (std::is_same_v<T, MyStruct>) {
      obj = MallocNew<T>(sizeof(T), kULVal, kUVal);
    } else {
      return;
    }

    EXPECT_EQ(expected, *obj);

    Delete(obj);
  }

  void
  VerifyCallocNewWithoutArgs()
  {
    auto obj = CallocNew<T>(sizeof(T));

    auto actual = reinterpret_cast<char*>(obj);
    for (size_t i = 0; i < sizeof(T); ++i) {
      EXPECT_EQ('\0', actual[i]);
    }

    Delete(obj);
  }

  void
  VerifyCallocNewWithArgs()
  {
    T* obj;
    if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>) {
      obj = CallocNew<T>(sizeof(T), expected);
    } else if constexpr (std::is_same_v<T, MyStruct>) {
      obj = CallocNew<T>(sizeof(T), kULVal, kUVal);
    } else {
      return;
    }

    EXPECT_EQ(expected, *obj);

    Delete(obj);
  }

  void
  VerifyDeleter()
  {
    auto obj = New<std::shared_ptr<T>>(New<T>(), Deleter<T>{});

    std::weak_ptr<T> inner{*obj};
    std::weak_ptr<std::shared_ptr<T>> outer;
    {
      std::shared_ptr<std::shared_ptr<T>> ptr{obj, Deleter<std::shared_ptr<T>>{}};
      outer = ptr;
    }

    EXPECT_TRUE(inner.expired());
    EXPECT_TRUE(outer.expired());
  }

  void
  VerifySTLAlloc()
  {
    std::vector<T, STLAlloc<T>> vec;
    for (size_t i = 0; i < 1E6; ++i) {
      vec.emplace_back();
    }
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using TestTargetTypes = ::testing::Types<uint64_t, uint32_t, MyStruct>;

TYPED_TEST_CASE(UtilityFixture, TestTargetTypes);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(UtilityFixture, New_WithoutArgs_AllocateMemory)
{  //
  TestFixture::VerifyNewWithoutArgs();
}

TYPED_TEST(UtilityFixture, New_WithArgs_ConstructExpectedInstance)
{  //
  TestFixture::VerifyNewWithArgs();
}

TYPED_TEST(UtilityFixture, MallocNew_WithoutArgs_AllocateMemory)
{  //
  TestFixture::VerifyMallocNewWithoutArgs();
}

TYPED_TEST(UtilityFixture, MallocNew_WithArgs_ConstructExpectedInstance)
{  //
  TestFixture::VerifyMallocNewWithArgs();
}

TYPED_TEST(UtilityFixture, CallocNew_WithoutArgs_AllocateMemoryWithZeroPadding)
{  //
  TestFixture::VerifyCallocNewWithoutArgs();
}

TYPED_TEST(UtilityFixture, CallocNew_WithArgs_ConstructExpectedInstance)
{  //
  TestFixture::VerifyCallocNewWithArgs();
}

TYPED_TEST(UtilityFixture, Deleter_InnerAndOuterReference_DeleteAllReferences)
{  //
  TestFixture::VerifyDeleter();
}

TYPED_TEST(UtilityFixture, STLAlloc_InnerAndOuterReference_DeleteAllReferences)
{  //
  TestFixture::VerifySTLAlloc();
}

}  // namespace dbgroup::memory::test
