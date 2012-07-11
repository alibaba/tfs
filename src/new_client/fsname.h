/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: fsname.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_FSNAME_H_
#define TFS_CLIENT_FSNAME_H_

#include <string>
#include "common/internal.h"

namespace tfs
{
  namespace client
  {
    struct FileBits
    {
      uint32_t block_id_;
      uint32_t seq_id_;
      uint32_t suffix_;
    };

    class FSName
    {
    public:
      FSName();
      FSName(const uint32_t block_id, const uint64_t file_id, const int32_t cluster_id = 0);
      FSName(const char *file_name, const char* suffix = NULL, const int32_t cluster_id = 0);
      virtual ~FSName();

      const char* get_name(const bool large_flag = false);
      void set_name(const char* file_name, const char* suffix = NULL, const int32_t cluster_id = 0);
      void set_suffix(const char* suffix);
      std::string to_string();

      static common::TfsFileType check_file_type(const char* tfs_name);

      inline bool is_valid() const
      {
        return is_valid_;
      }

      inline void set_block_id(const uint32_t id)
      {
        file_.block_id_ = id;
      }

      inline uint32_t get_block_id() const
      {
        return file_.block_id_;
      }

      inline void set_seq_id(const uint32_t id)
      {
        file_.seq_id_ = id;
      }

      inline uint32_t get_seq_id() const
      {
        return file_.seq_id_;
      }

      inline void set_suffix(const uint32_t id)
      {
        file_.suffix_ = id;
      }

      inline uint32_t get_suffix() const
      {
        return file_.suffix_;
      }

      inline void set_file_id(const uint64_t id)
      {
        file_.suffix_ = (id >> 32);
        file_.seq_id_ = (id & 0xFFFFFFFF);
      }

      inline uint64_t get_file_id()
      {
        uint64_t id = file_.suffix_;
        return ((id << 32) | file_.seq_id_);
      }

      inline void set_cluster_id(const int32_t cluster_id)
      {
        cluster_id_ = cluster_id;
      }

      inline int32_t get_cluster_id() const
      {
        return cluster_id_;
      }

    private:
      void encode(const char * input, char *output);
      void decode(const char * input, char *output);

    private:
      bool is_valid_;
      FileBits file_;
      int32_t cluster_id_;
      char file_name_[common::TFS_FILE_LEN];
    };
  }
}
#endif
