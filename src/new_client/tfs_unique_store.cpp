/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release

 */
#include "Memory.hpp"

#include "common/error_msg.h"
#include "fsname.h"
#include "tfs_client_impl.h"
#include "tfs_unique_store.h"

using namespace tfs::common;

namespace tfs
{
  namespace client
  {
    TfsUniqueStore::TfsUniqueStore() : unique_handler_(NULL)
    {
    }

    TfsUniqueStore::~TfsUniqueStore()
    {
      tbsys::gDelete(unique_handler_);
    }

    int TfsUniqueStore::initialize(const char* master_addr, const char* slave_addr,
                                   const char* group_name, const int32_t area, const char* ns_addr)
    {
      int ret = TFS_SUCCESS;

      TBSYS_LOG(DEBUG, "init unique handler,  master addr: %s, slave addr: %s, group name: %s, area: %d, ns addr: %s",
                master_addr, slave_addr, group_name, area, ns_addr);

      if (NULL == ns_addr)
      {
        TBSYS_LOG(ERROR, "null ns address");
      }
      else
      {
        ns_addr_ = ns_addr;
        // reuse
        tbsys::gDelete(unique_handler_);
        unique_handler_ = new TairUniqueHandler();

        if ((ret = static_cast<TairUniqueHandler*>(unique_handler_)
             ->initialize(master_addr, slave_addr, group_name, area)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "init tair unique handler fail. master addr: %s, slave addr: %s, group name: %s, area: %d",
                    master_addr, slave_addr, group_name, area);
        }
      }
      return ret;
    }

    int64_t TfsUniqueStore::save(const char* buf, const int64_t count,
                                 const char* tfs_name, const char* suffix,
                                 char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      int ret = TFS_ERROR;

      if (NULL == buf || count <= 0)
      {
        TBSYS_LOG(ERROR, "invalie buffer and count. buffer: %p, count: %"PRI64_PREFIX"d", buf, count);
      }
      else if (check_init())
      {
        UniqueKey unique_key;
        UniqueValue unique_value;

        unique_key.data_ = buf;
        unique_key.data_len_ = count;

        UniqueAction action = check_unique(unique_key, unique_value, tfs_name, suffix);
        TBSYS_LOG(DEBUG, "tfs unique store, action: %d", action);

        ret = process(action, unique_key, unique_value, tfs_name, suffix, ret_tfs_name, ret_tfs_name_len);
      }

      return ret != TFS_SUCCESS ? INVALID_FILE_SIZE : count;
    }

    int64_t TfsUniqueStore::save(const char* local_file,
                                 const char* tfs_name, const char* suffix,
                                 char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      int ret = TFS_ERROR;
      int64_t count = INVALID_FILE_SIZE;

      if (check_init())
      {
        char* buf = NULL;
        if ((ret = read_local_file(local_file, buf, count)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "read local file data fail. ret: %d", ret);
        }
        else
        {
          count = save(buf, count, tfs_name, suffix, ret_tfs_name, ret_tfs_name_len);
        }

        tbsys::gDelete(buf);
      }

      return count;
    }

    int32_t TfsUniqueStore::unlink(const char* tfs_name, const char* suffix, int64_t& file_size, const int32_t count)
    {
      int32_t ref_count = INVALID_REFERENCE_COUNT;

      if (count <= 0)
      {
        TBSYS_LOG(ERROR, "invalid unlink count: %d", count);
      }
      else if (check_init())
      {
        char* buf = NULL;
        int64_t buf_len = 0;
        int ret = TfsClientImpl::Instance()->fetch_file_ex(buf, buf_len, tfs_name, suffix, ns_addr_.c_str());

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "read tfs file data fail. filename: %s, suffix: %s, ret: %d",
                    tfs_name, suffix, ret);
        }
        else
        {
          UniqueKey unique_key;
          UniqueValue unique_value;

          unique_key.set_data(buf, buf_len);

          if ((ret = unique_handler_->query(unique_key, unique_value)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "query unique meta info fail. filename: %s, suffix: %s, ret: %d",
                      tfs_name, suffix, ret);
          }
          else
          {
            // ignore name match, if unlink_count > ref_count, just unlink
            if (unique_value.ref_count_ <= count) // unlink tfs file
            {
              TBSYS_LOG(DEBUG, "refcnt less than unlink count. %d <= %d",unique_value.ref_count_, count);

              // CAUTION: comment this unlink for histroy problem, avoid unlinking file on line,
              // uncomment this unlink for normal use
              //ret = TfsClientImpl::Instance()->unlink(file_size, tfs_name, suffix, ns_addr_.c_str());
              ret = TFS_SUCCESS;

              if (ret != TFS_SUCCESS)
              {
                TBSYS_LOG(ERROR, "unlink tfs file fail. filename: %s, suffix: %s, ret: %d",
                          tfs_name, suffix, ret);
              }
              else
              {
                ref_count = 0;  // unlink success
              }
            }

            if (TFS_SUCCESS == ret)
            {
              // check uniquestore filename and tfs filename.
              // if not match, not modify unique store meta info and file.
              // ... CONSISTENCY ...
              if (!(check_tfsname_match(unique_value.file_name_, tfs_name, suffix)))
              {
                TBSYS_LOG(WARN, "unlink filename mismatch unique store filename: %s%s <> %s",
                          tfs_name, NULL == suffix ? "" : suffix, unique_value.file_name_);
              }
              else                // unlink success and name match, then decrease
              {
                TBSYS_LOG(DEBUG, "unique refcount: %d, decrease count: %d", unique_value.ref_count_, count);
                int32_t bak_ref_count = ref_count;
                // if count >= unqiue_value.ref_count_, will delete this key
                if ((ref_count = unique_handler_->decrease(unique_key, unique_value, count)) < 0)
                {
                  // if tfs file is already unlinked(ref_count = 0), ignore error.
                  if (0 == bak_ref_count) // not unlink file, must fail
                  {
                    ref_count = 0;
                  }
                  TBSYS_LOG(ERROR, "decrease count fail. count: %d", count);
                }
                else
                {
                  file_size = buf_len; // decrease success, set file size
                }
              }
            }
          }

          tbsys::gDelete(buf);
        }
      }
      return ref_count;
    }

    bool TfsUniqueStore::check_init()
    {
      bool ret = true;
      if (NULL == unique_handler_)
      {
        TBSYS_LOG(ERROR, "unique handler not init");
        ret = false;
      }
      return ret;
    }

    bool TfsUniqueStore::check_name_match(const char* orig_tfs_name, const char* tfs_name, const char* suffix)
    {
      bool ret = false;

      if (NULL == tfs_name || '\0' == tfs_name[0]) // no tfs name
      {
        // check suffix
        ret = check_suffix_match(orig_tfs_name, suffix);
      }
      else
      {
        // has tfs name, must explicitly check
        ret = check_tfsname_match(orig_tfs_name, tfs_name, suffix);
      }
      return ret;
    }

    bool TfsUniqueStore::check_suffix_match(const char* orig_tfs_name, const char* suffix)
    {
      bool ret = true;
      if (suffix != NULL && suffix[0] != '\0')
      {
        // historically, only check whether tailing STANDARD_SUFFIX_LEN chars in suffix match.
        // just like this:
        //
        // int32_t orig_name_len = strlen(orig_tfs_name);
        // int32_t suffix_len = strlen(suffix);
        // const char* orig_suffix = orig_tfs_name + FILE_NAME_LEN;

        // if (strlen(orig_tfs_name + FILE_NAME_LEN) > STANDARD_SUFFIX_LEN)
        // {
        //   orig_suffix = orig_tfs_name + (orig_name_len - STANDARD_SUFFIX_LEN);
        // }
        // if (suffix_len > STANDARD_SUFFIX_LEN)
        // {
        //   suffix += suffix_len - STANDARD_SUFFIX_LEN;
        // }
        // ret = strncmp(orig_suffix, suffix, STANDARD_SUFFIX_LEN) == 0 ? true : false;
        //
        // for clean meanning, change it.
        // unqiue store suffix, so just string compare.
        // not strncmp.
        ret = strcmp(orig_tfs_name + FILE_NAME_LEN, suffix) == 0 ? true : false;
      }
      return ret;
    }

    bool TfsUniqueStore::check_tfsname_match(const char* orig_tfs_name, const char* tfs_name, const char* suffix)
    {
      bool ret = false;
      FSName orig_name(orig_tfs_name);
      FSName cur_name(tfs_name, suffix);
      if (orig_name.get_block_id() == cur_name.get_block_id() &&
          orig_name.get_file_id() == cur_name.get_file_id())
      {
        ret = true;
      }

      return ret;
    }

    UniqueAction TfsUniqueStore::check_unique(UniqueKey& unique_key, UniqueValue& unique_value,
                                              const char* tfs_name, const char* suffix)
    {
      int ret = TFS_ERROR;
      UniqueAction action = UNIQUE_ACTION_NONE;

      if ((ret = unique_handler_->query(unique_key, unique_value)) != TFS_SUCCESS)
      {
        if (ret == EXIT_UNIQUE_META_NOT_EXIST) // not exist, save data and meta
        {
          action = UNIQUE_ACTION_SAVE_DATA_SAVE_META;
          TBSYS_LOG(INFO, "unique info not exist, ret: %d", ret);
        }
        else                    // query fail, just save save data
        {
          action = UNIQUE_ACTION_SAVE_DATA;
          TBSYS_LOG(INFO, "query unique meta info fail, ret: %d", ret);
        }
      }
      else if (!check_name_match(unique_value.file_name_, tfs_name, suffix))
      {
        action = UNIQUE_ACTION_SAVE_DATA; // suffix or file name not match, save data
        TBSYS_LOG(WARN, "filename not match. unique filename: %s, tfsname: %s, suffix: %s",
                  unique_value.file_name_, tfs_name, suffix);
      }
      else                      // unique info exist and suffix match
      {
        TBSYS_LOG(DEBUG, "unique meta found and name match: filename: %s, refcnt: %d, version: %d", unique_value.file_name_, unique_value.ref_count_, unique_value.version_);
        TfsFileStat file_stat;
        ret = TfsClientImpl::Instance()->stat_file(&file_stat, unique_value.file_name_, NULL,
                                               NORMAL_STAT, ns_addr_.c_str());

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(WARN, "tfs file is not normal status, resave. ret: %d, filename: %s",
                    ret, unique_value.file_name_);
          action = UNIQUE_ACTION_SAVE_DATA_UPDATE_META;
        }
        else if (file_stat.size_ != unique_key.data_len_)
        {
          TBSYS_LOG(WARN, "tfs file size conflict: %"PRI64_PREFIX"d <> %d", file_stat.size_, unique_key.data_len_);
          action = UNIQUE_ACTION_SAVE_DATA_UPDATE_META;
          // unlink this dirty file?
        } // else if check crc?
        else
        {
          action = UNIQUE_ACTION_UPDATE_META; // just update unique meta
        }
      }

      return action;
    }

    int TfsUniqueStore::process(UniqueAction action, UniqueKey& unique_key, UniqueValue& unique_value,
                                const char* tfs_name, const char* suffix,
                                char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      int ret = TFS_SUCCESS;

      // first check if write data
      switch (action)
      {
      case UNIQUE_ACTION_SAVE_DATA:
        ret = save_data(unique_key, tfs_name, suffix, ret_tfs_name, ret_tfs_name_len);
        break;
      case UNIQUE_ACTION_SAVE_DATA_SAVE_META:
        ret = save_data_save_meta(unique_key, unique_value, tfs_name, suffix, ret_tfs_name, ret_tfs_name_len);
        break;
      case UNIQUE_ACTION_SAVE_DATA_UPDATE_META:
        ret = save_data_update_meta(unique_key, unique_value, tfs_name, suffix, ret_tfs_name, ret_tfs_name_len);
        break;
      case UNIQUE_ACTION_UPDATE_META:
        ret = update_meta(unique_key, unique_value, ret_tfs_name, ret_tfs_name_len);
        break;
      default:
        TBSYS_LOG(ERROR, "unkown action: %d", action);
        break;
      }
      return ret;
    }

    int TfsUniqueStore::save_data(UniqueKey& unique_key,
                                  const char* tfs_name, const char* suffix,
                                  char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      int ret = TfsClientImpl::Instance()->
        save_buf_ex(ret_tfs_name, ret_tfs_name_len, unique_key.data_, unique_key.data_len_, T_DEFAULT,
                     tfs_name, suffix, ns_addr_.c_str()) < 0 ? TFS_ERROR : TFS_SUCCESS;

      TBSYS_LOG(DEBUG, "write tfs data ret: %d, name: %s", ret, ret != TFS_SUCCESS ? "NULL" : ret_tfs_name);
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "save data to tfs fail. tfsname: %s, suffix: %s, ret: %d",
                  tfs_name, suffix, ret);
      }
      return ret;
    }

    int TfsUniqueStore::save_data_save_meta(UniqueKey& unique_key, UniqueValue& unique_value,
                                            const char* tfs_name, const char* suffix,
                                            char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      int ret = save_data(unique_key, tfs_name, suffix, ret_tfs_name, ret_tfs_name_len);

      if (TFS_SUCCESS == ret)
      {
        unique_value.set_file_name(ret_tfs_name, suffix);
        // first insert, set magic version to avoid dirty insert when concurrency occure
        unique_value.version_ = UNIQUE_FIRST_INSERT_VERSION;
        unique_value.ref_count_ = 1;

        ret = unique_handler_->insert(unique_key, unique_value);
        // save unique meta info fail. just ignore
        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(WARN, "save unique meta info fail, ignore. filename: %s, ret: %d", unique_value.file_name_, ret);
          ret = TFS_SUCCESS;
        }
      }

      return ret;
    }

    int TfsUniqueStore::save_data_update_meta(UniqueKey& unique_key, UniqueValue& unique_value,
                                              const char* tfs_name, const char* suffix,
                                              char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      int ret = save_data(unique_key, tfs_name, suffix, ret_tfs_name, ret_tfs_name_len);

      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "update meta info, filename: %s, refcnt: %d, version: %d", ret_tfs_name, unique_value.ref_count_, unique_value.version_);
        unique_value.set_file_name(ret_tfs_name, suffix);

        int32_t ref_count = unique_handler_->increase(unique_key, unique_value);
        if (ref_count < 0)
        {
          TBSYS_LOG(WARN, "update unique meta info fail, ignore. filename: %s, refcnt: %d, version: %d, ret: %d",
                    unique_value.file_name_, unique_value.ref_count_, unique_value.version_, ret);
          // just ignore
        }
      }

      return ret;
    }

    int TfsUniqueStore::update_meta(UniqueKey& unique_key, UniqueValue& unique_value,
                                    char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      int ret = wrap_file_name(unique_value.file_name_, ret_tfs_name, ret_tfs_name_len);

      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "return name fail. ret: %d", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "update meta info, refcnt: %d, version: %d", unique_value.ref_count_, unique_value.version_);
        int32_t ref_count = unique_handler_->increase(unique_key, unique_value);
        if (ref_count < 0)
        {
          TBSYS_LOG(WARN, "update unique meta info fail, ignore. filename: %s, refcnt: %d, version: %d, ret: %d",
                    unique_value.file_name_, unique_value.ref_count_, unique_value.version_, ret);
          // just ignore
        }
      }

      return ret;
    }

    int TfsUniqueStore::wrap_file_name(const char* tfs_name, char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      int ret = TFS_ERROR;

      if (ret_tfs_name != NULL)
      {
        if (ret_tfs_name_len < TFS_FILE_LEN)
        {
          TBSYS_LOG(ERROR, "name buffer length less: %d < %d", ret_tfs_name_len, TFS_FILE_LEN);
        }
        else
        {
          // history reason. unique store file name has suffix,
          // and maybe heading FILE_NAME_LEN chars have no suffix encoded,
          // so must reencode to get name
          FSName fsname(tfs_name);
          if (fsname.is_valid())
          {
            memcpy(ret_tfs_name, fsname.get_name(), TFS_FILE_LEN);
            ret = TFS_SUCCESS;
          }
        }
      }
      return ret;
    }

    int64_t TfsUniqueStore::get_local_file_size(const char* local_file)
    {
      int64_t file_size = 0;
      if (NULL == local_file)
      {
        TBSYS_LOG(ERROR, "null local file");
      }
      else
      {
        struct stat file_stat;
        if (stat(local_file, &file_stat) < 0)
        {
          TBSYS_LOG(ERROR, "stat local file %s fail, error: %s", local_file, strerror(errno));
        }
        else
        {
          file_size = file_stat.st_size;
        }
      }
      return file_size;
    }

    int TfsUniqueStore::read_local_file(const char* local_file, char*& buf, int64_t& count)
    {
      int ret = TFS_ERROR;
      int fd = -1;
      int64_t file_length = get_local_file_size(local_file);

      if (file_length <= 0)
      {
        TBSYS_LOG(ERROR, "get local file %s size fail.", local_file);
      }
      else if (file_length > TFS_MALLOC_MAX_SIZE)
      {
        TBSYS_LOG(ERROR, "file length larger than max malloc size. %"PRI64_PREFIX"d > %"PRI64_PREFIX"d",
                  file_length, TFS_MALLOC_MAX_SIZE);
      }
      else if ((fd = ::open(local_file, O_RDONLY)) < 0)
      {
        TBSYS_LOG(ERROR, "open local file %s fail, error: %s", local_file, strerror(errno));
      }
      else
      {
        // use MUST free
        buf = new char[file_length];
        int32_t read_len = 0, alread_read_len = 0;

        while (1)
        {
          read_len = ::read(fd, buf + alread_read_len, MAX_READ_SIZE);
          if (read_len < 0)
          {
            TBSYS_LOG(ERROR, "read file %s data fail. error: %s", local_file, strerror(errno));
            break;
          }

          alread_read_len += read_len;
          if (alread_read_len >= file_length)
          {
            ret = TFS_SUCCESS;
            count = alread_read_len;
            break;
          }
        }

        if (ret != TFS_SUCCESS)
        {
          tbsys::gDeleteA(buf);
        }

        ::close(fd);
      }

      return ret;
    }

  }
}
