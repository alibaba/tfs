/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: bit_map.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   zongdai <zongdai@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#include "bit_map.h"
#include <stdlib.h>
#include <assert.h>

namespace tfs
{
  namespace dataserver
  {
    const unsigned char BitMap::BITMAPMASK[SLOT_SIZE] =
      {
      0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01
      };

    BitMap::BitMap(const bool set_flag)
    {
      if (set_flag)
      {
        item_count_ = slot_count_ = INVALID_INDEX;
      }
      else
      {
        item_count_ = slot_count_ = 0;
      }
      data_ = NULL;
      set_count_ = 0;
      mount_ = false;
    }

    BitMap::BitMap(const uint32_t item_count, const bool set_flag)
    {
      assert(item_count != 0);
      slot_count_ = (item_count + SLOT_SIZE - 1) >> 3;
      data_ = new char[sizeof(char) * slot_count_];
      item_count_ = item_count;
      memset(data_, set_flag ? 0xFF : 0x0, slot_count_ * sizeof(char));
      set_count_ = set_flag ? item_count : 0;
      mount_ = false;
    }

    BitMap::BitMap(const BitMap& rhs)
    {
      item_count_ = rhs.item_count_;
      set_count_ = rhs.set_count_;
      slot_count_ = rhs.slot_count_;
      if (NULL != rhs.get_data())
      {
        data_ = new char[sizeof(char) * slot_count_];
        memcpy(data_, rhs.data_, slot_count_ * sizeof(char));
      }
      else
      {
        data_ = NULL;
      }
      mount_ = false;
    }

    BitMap& BitMap::operator=(const BitMap& rhs)
    {
      if (this != &rhs)
      {
        if (!mount_ && NULL != data_)
        {
          delete[] data_;
        }
        item_count_ = rhs.item_count_;
        set_count_ = rhs.set_count_;
        slot_count_ = rhs.slot_count_;
        if (NULL != rhs.get_data())
        {
          data_ = new char[sizeof(char) * slot_count_];
          memcpy(data_, rhs.data_, slot_count_ * sizeof(char));
        }
        else
        {
          data_ = NULL;
        }
        mount_ = false;
      }
      return *this;
    }

    BitMap::~BitMap()
    {
      if (!mount_ && NULL != data_)
        delete[] data_;
    }

    void BitMap::mount(const uint32_t item_count, const char* bitmap_data, const bool mount_flag)
    {
      if (!mount_)
      {
        assert(NULL == data_);
      }
      data_ = const_cast<char*> (bitmap_data);
      mount_ = mount_flag;
      item_count_ = item_count;
      slot_count_ = (item_count + SLOT_SIZE - 1) >> 3;
    }

    void BitMap::copy(const uint32_t slot_count, const char* bitmap_data)
    {
      assert(NULL != data_);
      assert(slot_count_ == slot_count);

      memcpy(data_, bitmap_data, slot_count);
      set_count_ = 0;
      for (uint32_t pos = 0; pos < item_count_; ++pos)
      {
        if (test(pos))
        {
          ++set_count_;
        }
      }
      return;
    }

    bool BitMap::alloc(const uint32_t item_count, const bool set_flag)
    {
      assert(NULL == data_);
      assert(item_count > 0);
      slot_count_ = (item_count + SLOT_SIZE - 1) >> 3;
      data_ = new char[sizeof(char) * slot_count_];
      item_count_ = item_count;
      memset(data_, set_flag ? 0xFF : 0x0, slot_count_ * sizeof(char));
      set_count_ = set_flag ? item_count : 0;
      return true;
    }

    bool BitMap::test(const uint32_t index) const
    {
      assert(index < item_count_);
      uint32_t quot = index / SLOT_SIZE;
      uint32_t rem = index % SLOT_SIZE;
      return (data_[quot] & BITMAPMASK[rem]) != 0;
    }

    void BitMap::set(const uint32_t index)
    {
      assert(index < item_count_);
      uint32_t quot = index / SLOT_SIZE;
      uint32_t rem = index % SLOT_SIZE;
      if (!(data_[quot] & BITMAPMASK[rem]))
      {
        data_[quot] |= BITMAPMASK[rem];
        ++set_count_;
      }
    }

    void BitMap::reset(const uint32_t index)
    {
      assert(index < item_count_);
      uint32_t quot = index / SLOT_SIZE;
      uint32_t rem = index % SLOT_SIZE;
      if (data_[quot] & BITMAPMASK[rem])
      {
        data_[quot] &= ~BITMAPMASK[rem];
        --set_count_;
      }
    }
  }
}
