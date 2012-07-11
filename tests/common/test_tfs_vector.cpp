/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_vector.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   duanfei
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "tfs_vector.h"

using namespace tfs::common;
class TestTfsVector : public virtual ::testing::Test
{
public:
  static void SetUpTestCase()
  {

  }
  static void TearDownTestCase()
  {
  }
  TestTfsVector(){}
  ~TestTfsVector(){}
};

TEST_F(TestTfsVector, TfsVector)
{
  const int32_t SIZE = 100;
  const int32_t EXPAND_SIZE = 10;
  const float  RESERVE_RATIO = 0.1;
  TfsVector<const int32_t*> tv3(SIZE, EXPAND_SIZE, RESERVE_RATIO);
  TfsVector<int32_t*> tv(SIZE, EXPAND_SIZE, RESERVE_RATIO);
  int32_t*p [SIZE];
  for (int32_t i = 0; i < SIZE; i++)
  {
    p[i] = new int32_t;
    *p[i] = i;
  }

  //initialize
  EXPECT_EQ(0, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE, tv.remain());

  //push_back
  tv.push_back(p[0]);
  EXPECT_EQ(1, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE - 1, tv.remain());

  EXPECT_EQ(0, *tv.at(0));
  EXPECT_EQ(0, *tv[0]);

  int32_t* p0 = p[0];
  //delete
  tv.erase(p0);
  EXPECT_EQ(0, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE, tv.remain());

  //insert
  tv.insert(tv.end(), p[1]);
  EXPECT_EQ(1, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE - 1, tv.remain());
  EXPECT_EQ(1, *tv.at(0));
  EXPECT_EQ(1, *tv[0]);

  tv.erase(p[1]);
  EXPECT_EQ(0, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE, tv.remain());

  //insert & expand
  tv.clear();
  for (int32_t i = 0; i < SIZE - 9; i++)
  {
    tv.insert(tv.end(), p[i]);
  }
  EXPECT_TRUE(tv.need_expand(RESERVE_RATIO));
  EXPECT_EQ(TFS_SUCCESS, tv.expand_ratio(RESERVE_RATIO));
  EXPECT_EQ((SIZE - 9), tv.size());
  EXPECT_EQ((SIZE + SIZE * RESERVE_RATIO), tv.capacity());

  //destroy
  for (int32_t i = 0; i < SIZE; i++)
  {
    delete p[i];
  }
}

struct CompareInt
{
  bool operator () (const int32_t* rl, const int32_t* ll) const
  {
    return *rl < *ll;
  }
};

TEST_F(TestTfsVector, TfsSortedVector)
{
  const int32_t SIZE = 100;
  const int32_t EXPAND_SIZE = 10;
  const float  RESERVE_RATIO = 0.1;
  TfsSortedVector<int32_t*, CompareInt> tv(SIZE, EXPAND_SIZE, RESERVE_RATIO);
  int32_t*p [SIZE];
  for (int32_t i = 0; i < SIZE; i++)
  {
    p[i] = new int32_t;
    *p[i] = i;
  }

  //initialize
  EXPECT_EQ(0, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE, tv.remain());

  //push_back
  tv.push_back(p[0]);
  EXPECT_EQ(1, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE - 1, tv.remain());

  EXPECT_EQ(0, *tv.at(0));
  EXPECT_EQ(0, *tv[0]);

  //delete
  tv.erase(p[0]);
  EXPECT_EQ(0, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE, tv.remain());

  //insert
  tv.insert(p[1]);
  EXPECT_EQ(1, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE - 1, tv.remain());
  EXPECT_EQ(1, *tv.at(0));
  EXPECT_EQ(1, *tv[0]);

  tv.erase(p[1]);
  EXPECT_EQ(0, tv.size());
  EXPECT_EQ(SIZE, tv.capacity());
  EXPECT_EQ(SIZE, tv.remain());

  //insert & expand
  tv.clear();
  for (int32_t i = 0; i < SIZE - 9; i++)
  {
    tv.insert(p[i]);
  }
  EXPECT_TRUE(tv.need_expand(RESERVE_RATIO));
  EXPECT_EQ(TFS_SUCCESS, tv.expand_ratio(RESERVE_RATIO));
  EXPECT_EQ((SIZE - 9), tv.size());
  EXPECT_EQ((SIZE + SIZE * RESERVE_RATIO), tv.capacity());

  //insert
  srand(time(NULL));
  TfsSortedVector<int32_t*, CompareInt> tv2(SIZE, EXPAND_SIZE, RESERVE_RATIO);
  EXPECT_EQ(0, tv2.size());
  for (int32_t i = SIZE - 1; i >= 0; i--)
  {
    int32_t* x = p[i];
    tv2.insert(x);
  }
  EXPECT_EQ(SIZE, tv2.size());
  for (int32_t i = 0; i < tv2.size() - 1; i++)
  {
    EXPECT_TRUE(i < *tv2.at(i+1));
  }

  //insert_unique
  tv2.clear();
  for (int32_t i = 0; i < SIZE; i++)
  {
    int32_t* x = p[random() % SIZE];
    tv2.insert_unique(x);
  }
  for (int32_t i = 0; i < tv2.size() - 1; i++)
  {
    EXPECT_TRUE(i < *tv2.at(i+1));
  }

  //destroy
  for (int32_t i = 0; i < SIZE; i++)
  {
    delete p[i];
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
