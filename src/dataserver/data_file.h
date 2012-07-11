/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: data_file.h 356 2011-05-26 07:51:09Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_DATAFILE_H_
#define TFS_DATASERVER_DATAFILE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <ext/hash_map>
#include <tbsys.h>
#include "common/config_item.h"
//#include "common/config.h"
#include "dataserver_define.h"

namespace tfs
{
  namespace dataserver
  {

    class DataFile
    {
      public:
        explicit DataFile(uint64_t fn);
        DataFile(uint64_t fn, char* path);
        ~DataFile();

        int set_data(const char *data, const int32_t len, const int32_t offset);
        char* get_data(char *data, int32_t *len, const int32_t offset);

        inline int32_t get_length() const
        {
          return length_;
        }

        inline int32_t get_last_update() const
        {
          return last_update_;
        }

        inline void set_last_update()
        {
          last_update_ = time(NULL);
        }

        void set_over();
        uint32_t get_crc();

        inline int add_ref()
        {
          return atomic_add_return(1, &ref_count_);
        }

        inline void sub_ref()
        {
          atomic_dec(&ref_count_);
        }

        inline int get_ref() const
        {
          return atomic_read(&ref_count_);
        }

      private:
        static const int WRITE_DATA_TMPBUF_SIZE = 2 * 1024 * 1024;

      private:
        int32_t last_update_;   // last update time
        int32_t length_;        // current max buffer write length
        char data_[WRITE_DATA_TMPBUF_SIZE]; // data buffer
        uint32_t crc_;          // crc checksum
        int fd_;                // temporary file fd
        char tmp_file_name_[common::MAX_PATH_LENGTH]; // temporary file name
        atomic_t ref_count_;                          // reference count

      private:
        DataFile();
        DISALLOW_COPY_AND_ASSIGN(DataFile);
    };

    typedef __gnu_cxx::hash_map<uint64_t, DataFile*, __gnu_cxx::hash<int> > DataFileMap; // fileid_ => DataFile
    typedef DataFileMap::iterator DataFileMapIter;

  }
}
#endif //TFS_DATASERVER_DATAFILE_H_
