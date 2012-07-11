/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: bit_map.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
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
#ifndef TFS_DATASERVER_BITMAP_H_
#define TFS_DATASERVER_BITMAP_H_

#include <inttypes.h> 
#include <string.h>

namespace tfs
{
  namespace dataserver
  {
    class BitMap
    {
      public:
        BitMap(const bool set_flag = false);
        BitMap(const uint32_t item_count, const bool set_flag = false);
        BitMap(const BitMap& rhs);
        BitMap& operator=(const BitMap& rhs);
        ~BitMap();

        void mount(const uint32_t item_count, const char* bitmap_data, const bool mount_flag = true);
        bool alloc(const uint32_t item_count, const bool set_flag);

        void copy(const uint32_t slot_count, const char* bitmap_data);
        bool test(const uint32_t pos) const;
        void set(const uint32_t pos);
        void reset(const uint32_t pos);

        inline void reset_all()
        {
          memset(data_, 0, slot_count_ * sizeof(char));
          set_count_ = 0;
        }

        inline char* get_data() const
        {
          return data_;
        }

        inline uint32_t get_set_count() const
        {
          return set_count_;
        }

        inline uint32_t get_slot_count() const
        {
          return slot_count_;
        }

        inline uint32_t get_item_count() const
        {
          return item_count_;
        }

      public:
        static const uint32_t SLOT_SIZE = 8 * sizeof(char);
        static const unsigned char BITMAPMASK[SLOT_SIZE];
        static const uint32_t INVALID_INDEX = 0xFFFFFFFF;

      private:
        uint32_t item_count_;
        uint32_t slot_count_;
        mutable uint32_t set_count_;
        char* data_;
        bool mount_;
    };

  }
}
#endif //TFS_DATASERVER_BITMAP_H_
