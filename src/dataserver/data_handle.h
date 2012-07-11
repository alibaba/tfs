/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: data_handle.h 326 2011-05-24 07:18:18Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_DATAHANDLE_H_
#define TFS_DATASERVER_DATAHANDLE_H_

#include "physical_block.h"
#include "common/internal.h"

namespace tfs
{
  namespace dataserver
  {
    class LogicBlock;
    class DataHandle
    {
      public:
        explicit DataHandle(LogicBlock* logic_blk) :
          logic_block_(logic_blk)
        {
        }

        ~DataHandle()
        {
        }

      public:
        int read_segment_info(common::FileInfo* inner_file_info, const int32_t offset);
        int write_segment_info(const common::FileInfo* inner_file_info, const int32_t offset);

        int write_segment_data(const char* buf, const int32_t nbytes, const int32_t offset);
        int read_segment_data(char* buf, const int32_t nbytes, const int32_t offset);


        /**
         * @brief advise prefetch data
         *
         * @param offset: offset of file in block
         * @param size: size of file
         */
        int fadvise_readahead(const int64_t offset, const int64_t size);

      private:
        int choose_physic_block(PhysicalBlock** tmp_physical_block, const int32_t offset, int32_t& inner_offset,
            int32_t& inner_len);

      private:
        DataHandle();
        DISALLOW_COPY_AND_ASSIGN(DataHandle);

        LogicBlock* logic_block_; // associate logic block id
    };

  }
}
#endif //TFS_DATASERVER_DATAHANDLE_H_
