/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: fsname.cpp 19 2010-10-18 05:48:09Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include <string.h>
#include "fsname.h"

using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace client
  {
    static const char SMALL_TFS_FILE_KEY_CHAR = 'T';
    static const char LARGE_TFS_FILE_KEY_CHAR = 'L';

    static const char* KEY_MASK = "Taobao-inc";
    static const int32_t KEY_MASK_LEN = strlen(KEY_MASK);
    static const char enc_table[] = "0JoU8EaN3xf19hIS2d.6pZRFBYurMDGw7K5m4CyXsbQjg_vTOAkcHVtzqWilnLPe";
    static const char dec_table[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,18,0,0,11,16,8,36,34,19,32,4,12,0,0,0,0,0,0,0,49,24,37,29,5,23,30,52,14,1,33,61,28,7,48,62,42,22,15,47,3,53,57,39,25,21,0,0,0,0,45,0,6,41,51,17,63,10,44,13,58,43,50,59,35,60,2,20,56,27,40,54,26,46,31,9,38,55,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};

    static int xor_mask(const char* source, const int32_t len, char* target)
    {
      for (int32_t i = 0; i < len; i++)
      {
        target[i] = source[i] ^ KEY_MASK[i % KEY_MASK_LEN];
      }
      return TFS_SUCCESS;
    }

    static int32_t hash(const char *str)
    {
      if (str == NULL)
      {
        return 0;
      }
      int32_t len = strlen(str);
      int32_t h = 0;
      int32_t i = 0;
      for (i = 0; i < len; ++i)
      {
        h += str[i];
        h *= 7;
      }
      return (h | 0x80000000);
    }

    FSName::FSName() : is_valid_(true), cluster_id_(0)
    {
      file_name_[0] = '\0';
      memset(&file_, 0, sizeof(FileBits));
    }

    FSName::FSName(const uint32_t block_id, const uint64_t file_id, const int32_t cluster_id) :
      is_valid_(true), cluster_id_(cluster_id)
    {
      file_.block_id_ = block_id;
      set_file_id(file_id);
      file_name_[0] = '\0';
    }

    FSName::FSName(const char* file_name, const char* suffix, const int32_t cluster_id) : is_valid_(true)
    {
      set_name(file_name, suffix, cluster_id);
    }

    FSName::~FSName()
    {

    }

    void FSName::set_name(const char* file_name, const char* suffix, const int32_t cluster_id)
    {
      file_name_[0] = '\0';
      cluster_id_ = cluster_id;
      memset(&file_, 0, sizeof(FileBits));

      if (NULL != file_name && file_name[0] != '\0')
      {
        if (check_file_type(file_name) == INVALID_TFS_FILE_TYPE)
        {
          is_valid_ = false;
        }
        else
        {
          decode(file_name + 2, (char*) &file_);
          if (NULL == suffix)
          {
            suffix = file_name + FILE_NAME_LEN;
          }
          set_suffix(suffix);
          if (0 == cluster_id_)
          {
            cluster_id_ = file_name[1] - '0';
          }
        }
      }
    }

    const char* FSName::get_name(const bool large_flag)
    {
      if (file_name_[0] == '\0')
      {
        encode((char*) &file_, file_name_ + 2);
        if (large_flag)
        {
          file_name_[0] = LARGE_TFS_FILE_KEY_CHAR;
        }
        else
        {
          file_name_[0] = SMALL_TFS_FILE_KEY_CHAR;
        }
        file_name_[1] = static_cast<char> ('0' + cluster_id_);
        file_name_[FILE_NAME_LEN] = '\0';
      }

      return file_name_;
    }

    void FSName::set_suffix(const char *suffix)
    {
      if ((suffix != NULL) && (suffix[0] != '\0'))
      {
        file_.suffix_ = hash(suffix);
      }
    }

    string FSName::to_string()
    {
      char buffer[256];
      snprintf(buffer, 256, "block_id: %u, file_id: %"PRI64_PREFIX"u, seq_id: %u, suffix: %u, name: %s",
               file_.block_id_, get_file_id(), file_.seq_id_, file_.suffix_, get_name());
      return string(buffer);
    }

    TfsFileType FSName::check_file_type(const char* tfs_name)
    {
      TfsFileType file_type = INVALID_TFS_FILE_TYPE;
      if (NULL != tfs_name &&
          static_cast<int32_t>(strlen(tfs_name)) >= FILE_NAME_LEN)
      {
        if (LARGE_TFS_FILE_KEY_CHAR == tfs_name[0])
        {
          file_type = LARGE_TFS_FILE_TYPE;
        }
        else if (SMALL_TFS_FILE_KEY_CHAR == tfs_name[0])
        {
          file_type = SMALL_TFS_FILE_TYPE;
        }
      }
      return file_type;
    }

    void FSName::encode(const char *input, char *output)
    {
      if (input != NULL && output != NULL)
      {
        char buffer[FILE_NAME_EXCEPT_SUFFIX_LEN];
        xor_mask(input, FILE_NAME_EXCEPT_SUFFIX_LEN, buffer);

        int32_t i = 0;
        int32_t k = 0;
        uint32_t value = 0;
        for (i = 0; i < FILE_NAME_EXCEPT_SUFFIX_LEN; i += 3)
        {
          value = ((buffer[i] << 16) & 0xff0000) + ((buffer[i + 1] << 8) & 0xff00) + (buffer[i + 2] & 0xff);
          output[k++] = enc_table[value >> 18];
          output[k++] = enc_table[(value >> 12) & 0x3f];
          output[k++] = enc_table[(value >> 6) & 0x3f];
          output[k++] = enc_table[value & 0x3f];
        }
      }
    }

    void FSName::decode(const char *input, char *output)
    {
      if (input != NULL && output != NULL)
      {
        int32_t i = 0;
        int32_t k = 0;
        uint32_t value = 0;
        char buffer[FILE_NAME_EXCEPT_SUFFIX_LEN];
        for (i = 0; i < FILE_NAME_LEN - 2; i += 4)
        {
          value = (dec_table[input[i] & 0xff] << 18) + (dec_table[input[i + 1] & 0xff] << 12) +
            (dec_table[input[i + 2] & 0xff] << 6) + dec_table[input[i + 3] & 0xff];
          buffer[k++] = static_cast<char> ((value >> 16) & 0xff);
          buffer[k++] = static_cast<char> ((value >> 8) & 0xff);
          buffer[k++] = static_cast<char> (value & 0xff);
        }
        xor_mask(buffer, FILE_NAME_EXCEPT_SUFFIX_LEN, output);
      }
    }
  }
}
