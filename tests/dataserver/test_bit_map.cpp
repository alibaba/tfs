/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_bit_map.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   daozong
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "bit_map.h"

using namespace tfs::dataserver;

class BitmapTest: public ::testing::Test
{
  public:
    BitmapTest()
    {
    }
    ~BitmapTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(BitmapTest, testSet)
{
  int32_t item_count = 3000;
  BitMap* bitmap = NULL;  
  bitmap = new BitMap(item_count, 0);

  EXPECT_EQ(bitmap->test(1), 0);
  bitmap->set(1);
  EXPECT_EQ(bitmap->test(1), 1);

  EXPECT_EQ(bitmap->test(2999), 0);
  bitmap->set(2999);
  EXPECT_EQ(bitmap->test(2999), 1);

  EXPECT_EQ(bitmap->test(0), 0);
  bitmap->set(0);
  EXPECT_EQ(bitmap->test(0), 1);
  
  EXPECT_EQ(static_cast<uint32_t>(3), bitmap->get_set_count());

  bitmap->reset(0);
  EXPECT_EQ(static_cast<uint32_t>(2), bitmap->get_set_count());

  for (int32_t i = 0; i < item_count; ++i)
  {
    bitmap->set(i);
  }

  EXPECT_EQ(bitmap->get_set_count(), static_cast<uint32_t>(item_count));

  for (int32_t i = 0; i < item_count; ++i)
  {
    bitmap->reset(i);
  }

  EXPECT_EQ(bitmap->get_set_count(), static_cast<uint32_t>(0));
  
  delete bitmap;
  bitmap = NULL;  
}

TEST_F(BitmapTest, testGetItemCount)
{
  int32_t item_count = 250;
  BitMap* bitmap = NULL;  
  bitmap = new BitMap(item_count, 0);
  EXPECT_EQ(bitmap->get_item_count(), static_cast<uint32_t>(250));

  delete bitmap;
  bitmap = NULL;
}

TEST_F(BitmapTest, testGetSlotCount)
{
  int32_t item_count = 0;
  int32_t slot_count = 0;
  BitMap* bitmap = NULL;  
  
  for (item_count=1; item_count <= 3100; ++item_count)
  { 
    bitmap = new BitMap(item_count, 0);
    slot_count = (item_count + bitmap->SLOT_SIZE - 1) >> 3;
    EXPECT_EQ(bitmap->get_slot_count(), static_cast<uint32_t>(slot_count));

    delete bitmap;
    bitmap = NULL;
  }
}

TEST_F(BitmapTest, testCopy)
{
  int32_t item_count = 0;
  int32_t slot_count = 0;
  int32_t step = 0;
  int32_t size =8;

  BitMap* bitmap = NULL;
  char* buf = NULL;

  for (item_count = 1; item_count < 3100; ++item_count)
  {
    bitmap = new BitMap(item_count, 0);
    slot_count = (item_count + bitmap->SLOT_SIZE - 1) >> 3;
    buf = new char[slot_count];
  
    memset(buf, 0xFF, slot_count);
    bitmap->copy(slot_count, buf);
    EXPECT_EQ(bitmap->get_set_count(), static_cast<uint32_t>(item_count));
    
    delete []buf;
    buf = NULL;
    delete bitmap;
    bitmap = NULL;
  }

  item_count = 3100;
  bitmap = new BitMap(item_count, 1);
  EXPECT_EQ(bitmap->get_set_count(), static_cast<uint32_t>(item_count));

  slot_count = (item_count + bitmap->SLOT_SIZE - 1) >> 3;
  buf = new char[slot_count];

  for (step = 1; step < slot_count; ++step)
  { 
    memset(buf, 0xFF, slot_count);    
    memset(buf+step, 0X0, slot_count-step);
    bitmap->copy(slot_count, buf);
    EXPECT_EQ(bitmap->get_set_count(), static_cast<uint32_t>(step * size));
  } 

  delete []buf;
  buf = NULL;
  delete bitmap;
  bitmap = NULL;
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
