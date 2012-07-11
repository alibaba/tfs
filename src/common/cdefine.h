/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: cdefine.h 345 2011-05-26 01:18:00Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_CDEFINE_H_
#define TFS_COMMON_CDEFINE_H_

/* include header file when c.
 * NEVER include this file in cxx, cause it open global namespace.
 * include define.h instead
 * */

#if __cplusplus
/* error code */
static const int TFS_SUCCESS = EXIT_SUCCESS;
static const int TFS_ERROR = EXIT_FAILURE;
static const int EXIT_INVALIDFD_ERROR = -1005;

/* block cache default config */
static const int32_t DEFAULT_BLOCK_CACHE_TIME = 1800;
static const int32_t DEFAULT_BLOCK_CACHE_ITEMS = 500000;

/* cluster group count and group seq default value */
static const int32_t DEFAULT_CLUSTER_GROUP_COUNT = 1;
static const int32_t DEFAULT_CLUSTER_GROUP_SEQ = 0;

/* tfs file name standard name length */
static const int32_t FILE_NAME_LEN = 18;
static const int32_t TFS_FILE_LEN = FILE_NAME_LEN + 1;
static const int32_t FILE_NAME_EXCEPT_SUFFIX_LEN = 12;
static const int32_t MAX_FILE_NAME_LEN = 128;
static const int32_t MAX_SUFFIX_LEN = MAX_FILE_NAME_LEN - TFS_FILE_LEN;
static const int32_t STANDARD_SUFFIX_LEN = 4;

#else

/* error code */
#define TFS_SUCCESS EXIT_SUCCESS
#define TFS_ERROR EXIT_FAILURE
#define EXIT_INVALIDFD_ERROR -1005

/* block cache default config */
#define DEFAULT_BLOCK_CACHE_TIME 1800
#define DEFAULT_BLOCK_CACHE_ITEMS 500000

/* tfs file name standard name length */
#define FILE_NAME_LEN 18
#define TFS_FILE_LEN (FILE_NAME_LEN + 1)
#define FILE_NAME_EXCEPT_SUFFIX_LEN 12
#define MAX_FILE_NAME_LEN 128
#define MAX_SUFFIX_LEN (MAX_FILE_NAME_LEN - TFS_FILE_LEN)
#define STANDARD_SUFFIX_LEN 4

#endif

#if __WORDSIZE == 64
#define PRI64_PREFIX "l"
#else
#define PRI64_PREFIX "ll"
#endif

typedef struct
{
  uint64_t file_id_;
  int32_t offset_; /* offset in block file */
  int64_t size_; /* file size */
  int64_t usize_; /* hold space */
  int32_t modify_time_; /* modify time */
  int32_t create_time_; /* create time */
  int32_t flag_; /* deleta flag */
  uint32_t crc_; /* crc value */
} TfsFileStat;

typedef enum
{
  T_DEFAULT = 0,
  T_READ = 1,
  T_WRITE = 2,
  T_CREATE = 4,
  T_NEWBLK = 8,
  T_NOLEASE = 16,
  T_STAT = 32,
  T_LARGE = 64,
  T_UNLINK = 128,
  T_FORCE = 256
} OpenFlag;

typedef enum
{
  T_SEEK_SET = 0,
  T_SEEK_CUR,
  T_SEEK_END
} TfsSeekType;

typedef enum
{
  NORMAL_STAT = 0,
  FORCE_STAT
} TfsStatType;


/**
 * @brief
 *
 * 0th bit is master flag
 * 0, 2, 4, 6 denote four unlink type
 * 5-7th bit for sync stat
 */
typedef enum
{
  DELETE = 0,
  UNDELETE = 2,
  CONCEAL = 4,
  REVEAL = 6,
  SYNC = 126
} TfsUnlinkType;

typedef enum
{
  TFS_FILE_DEFAULT_OPTION = 0,
  TFS_FILE_NO_SYNC_LOG = 1,
  TFS_FILE_CLOSE_FLAG_WRITE_DATA_FAILED = 2
} OptionFlag;

typedef enum
{
  READ_DATA_OPTION_FLAG_NORMAL = 0,
  READ_DATA_OPTION_FLAG_FORCE = 1
} ReadDataOptionFlag;

typedef enum
{
  LOCAL_CACHE = 0,
  REMOTE_CACHE = 1
} CacheType;

typedef enum
{
  USE_CACHE_FLAG_NO = 0x00,
  USE_CACHE_FLAG_LOCAL = 0x01,
  USE_CACHE_FLAG_REMOTE = 0x02
} UseCacheFlag;

#endif
