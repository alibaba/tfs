/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *
 */

#ifndef TFS_COMMON_BUFFER_H_
#define TFS_COMMON_BUFFER_H_
#include "internal.h"
#include "error_msg.h"

namespace tfs
{
  namespace common
  {
    class Buffer
    {
    public:
        Buffer():
          pstart_(NULL), pend_(NULL), pfree_(NULL), pdata_(NULL)
      {

      }
      virtual ~Buffer()
      {
        destroy();
      }

      inline void destroy()
      {
        if (NULL != pstart_)
        {
          free(pstart_);
          pend_ = pfree_ = pdata_ = pstart_ = NULL;
        }
      }

      inline char* get_data() const
      {
        return reinterpret_cast<char*>(pdata_);
      }

      inline int64_t get_data_length() const
      {
        return  (pfree_ - pdata_);
      }

      inline char* get_free() const
      {
        return reinterpret_cast<char*>(pfree_);
      }

      inline int64_t get_free_length() const
      {
        return (pend_ - pfree_);
      }

      inline int64_t get_buf_length() const
      {
        return (pend_ - pstart_);
      }

      inline int drain(const int64_t length)
      {
        int32_t iret = NULL != pdata_ ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          pdata_ += length;
          if (pdata_ >= pfree_)
          {
            clear();
          }
        }
        return iret;
      }

      inline int pour(const int64_t length)
      {
        assert(pend_ - pfree_ >= length);
        int32_t iret = NULL != pfree_ ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          pfree_ += length;
        }
        return iret;
      }

      inline int strip( const int64_t length)
      {
        assert(pfree_ - pdata_ >= length);
        int32_t iret = NULL != pfree_ ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          pfree_ -= length;
        }
        return iret;
      }

      inline void clear()
      {
        pdata_ = pfree_ = pstart_;
      }

      inline void expand(const int64_t need)
      {
        if (NULL == pstart_)
        {
          int64_t length = DEFAULT_MALLOC_SIZE;
          while (length < need)
          {
            length <<= EXPAND_MULTIPLE;
          }
          pfree_ = pdata_ = pstart_ = (unsigned char*)malloc(length);
          pend_ = pstart_ + length;
        }
        else if (get_free_length() < need)
        {
          int64_t free_length = get_free_length() + (pdata_ - pstart_);
          int64_t data_length = get_data_length();
          if (free_length < need || free_length * 4 < data_length)
          {
            int64_t buf_size = get_buf_length();
            buf_size <<= EXPAND_MULTIPLE;
            while ((buf_size - data_length) < need)
            {
              buf_size <<= EXPAND_MULTIPLE;
            }

            unsigned char *newbuf = (unsigned char *)malloc(buf_size);
            assert(newbuf != NULL);
            if (data_length > 0)
            {
              memcpy(newbuf, pdata_, data_length);
            }
            free(pstart_);
            pdata_ = pstart_ = newbuf;
            pfree_ = pstart_ + data_length;
            pend_ = pstart_ + buf_size;
          }
          else
          {
            memmove(pstart_, pdata_, data_length);
            pfree_ = pstart_ + data_length;
            pdata_ = pstart_;
          }
        }
      }
    private:
      inline bool shrink()
      {
        bool bret = NULL != pstart_;
        if (bret)
        {
          if ((pend_ - pstart_) <= SHRINK_BUFFER_SIZE || (pfree_ - pdata_) > SHRINK_BUFFER_SIZE)
          {
            bret = false;
          }
          if (bret)
          {
            int64_t old_length = pfree_ - pdata_;
            if (old_length < 0)
              old_length = 0;

            unsigned char *newbuf = (unsigned char*)malloc(SHRINK_BUFFER_SIZE);
            assert(NULL != newbuf);

            if (old_length > 0)
            {
              memcpy(newbuf, pdata_, old_length);
            }
            free(pstart_);
            pdata_ = pstart_ = newbuf;
            pfree_ = pstart_ + old_length;
            pend_  = pstart_ + SHRINK_BUFFER_SIZE;
          }
        }
        return bret;
      }
    #ifdef TFS_GTEST
    public:
    #else
    private:
    #endif
      DISALLOW_COPY_AND_ASSIGN(Buffer);
      static const int16_t DEFAULT_MALLOC_SIZE = 256;//bytes
      static const int16_t SHRINK_BUFFER_SIZE = 2048;//2KB
      static const int8_t EXPAND_MULTIPLE = 1;
      unsigned char *pstart_;
      unsigned char *pend_;
      unsigned char *pfree_;
      unsigned char *pdata_;
    };
  }//end namespace comon
}//end namespace tfs

#endif /*TFS_COMMON_BUFFER_H_*/
