/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: file_repair.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
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
#ifndef TFS_DATASERVER_FILEREPAIR_H_
#define TFS_DATASERVER_FILEREPAIR_H_

#include "dataserver_define.h"

#include "common/internal.h"
#include "new_client/tfs_client_impl.h"

namespace tfs
{
  namespace dataserver
  {

    class FileRepair
    {
      public:
        FileRepair();
        ~FileRepair();

        bool init(const uint64_t dataserver_id);
        int repair_file(const common::CrcCheckFile& crc_check_record, const char* tmp_file);
        int fetch_file(const common::CrcCheckFile& crc_check_record, char* tmp_file);

      private:
        static void get_tmp_file_name(char* buffer, const char* path, const char* name);
        int write_file(const int fd, const char* buffer, const int32_t length);

      private:
        bool init_status_;
        uint64_t dataserver_id_;
        char src_addr_[common::MAX_ADDRESS_LENGTH];
        client::TfsClientImpl* tfs_client_;
    };
  }
}
#endif //TFS_DATASERVER_FILEREPAIR_H_
