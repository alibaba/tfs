/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_ioapi_cases.cpp 10 2010-10-11 01:57:47Z duanfei@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include "common/func.h"
#include "message/message_factory.h"
#include "tfs_ioapi_cases.h"
#include <iostream>

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace std;

TfsClient* TfsIoApiTest::tfs_client_;
static const int32_t MAX_LOOP = 20;


void TfsIoApiTest::SetUpTestCase()
{
}

void TfsIoApiTest::TearDownTestCase()
{
  ::unlink("tmplocal");
}

int32_t TfsIoApiTest::generate_data(char* buffer, const int32_t length)
{
  srand(time(NULL) + rand() + pthread_self());
  int32_t i = 0;
  for (i = 0; i < length; i++)
  {
    buffer[i] = rand() % 90 + 32;
  }
  return length;
}

int32_t TfsIoApiTest::write_data(const char* buffer, const int32_t length)
{
  int32_t num_wrote = 0;
  int32_t left = length - num_wrote;

  int ret = TFS_SUCCESS;

  while (left > 0)
  {
    ret = tfs_client_->tfs_write(const_cast<char*>(buffer) + num_wrote, left);
    if (ret < 0)
    {
      return ret;
    }
    if (ret >= left)
    {
      return left;
    }
    else
    {
      num_wrote += ret;
      left = length - num_wrote;
    }
  }

  return num_wrote;
}

int32_t TfsIoApiTest::write_new_file(const char* buffer, const int32_t length)
{
  int32_t ret = TFS_SUCCESS;
  ret = tfs_client_->tfs_open(NULL, "", WRITE_MODE);
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }
  cout << "tfs open successful" << endl;

  ret = write_data(buffer, length);
  if (ret != length)
  {
    return ret;
  }
  cout << "write data successful" << endl;

  ret = tfs_client_->tfs_close();
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  return TFS_SUCCESS;
}

int32_t TfsIoApiTest::write_new_file(const int32_t length)
{
  char buffer[length];
  generate_data(buffer, length);
  return write_new_file(buffer, length);
}

int TfsIoApiTest::write_update_file(const char* buffer, const int32_t length, const std::string& tfs_file_name)
{
  int32_t ret = TFS_SUCCESS;
  ret = tfs_client_->tfs_open((char*) tfs_file_name.c_str(), "", WRITE_MODE);
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  ret = write_data(buffer, length);
  if (ret != length)
  {
    return ret;
  }

  ret = tfs_client_->tfs_close();
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  return TFS_SUCCESS;
}

int TfsIoApiTest::write_update_file(const int32_t length)
{
  char buffer[length];
  generate_data(buffer, length);
  int ret = write_new_file(buffer, length);
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  generate_data(buffer, length);
  return write_update_file(buffer, length, tfs_client_->get_file_name());
}

int TfsIoApiTest::stat_exist_file(char* tfs_file_name, FileInfo& file_info)
{
  int32_t ret = tfs_client_->tfs_open((char*) tfs_file_name, "", READ_MODE);
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }
  ret = tfs_client_->tfs_stat(&file_info, READ_MODE);
  tfs_client_->tfs_close();
  return ret;
}

int TfsIoApiTest::read_exist_file(const std::string& tfs_file_name, char* buffer, int32_t& length)
{
  int ret = tfs_client_->tfs_open((char*) tfs_file_name.c_str(), "", READ_MODE);
  if (ret != TFS_SUCCESS)
  {
    printf("tfsOpen :%s failed", tfs_file_name.c_str());
    return ret;
  }

  FileInfo file_info;
  ret = tfs_client_->tfs_stat(&file_info);
  if (ret != TFS_SUCCESS)
  {
    tfs_client_->tfs_close();
    printf("tfsStat:%s failed", tfs_file_name.c_str());
    return ret;
  }

  length = file_info.size_;
  int32_t num_readed = 0;
  int32_t num_per_read = min(length, 16384);
  uint32_t crc = 0;

  do
  {
    ret = tfs_client_->tfs_read(buffer + num_readed, num_per_read);
    if (ret < 0)
    {
      break;
    }
    crc = Func::crc(crc, buffer + num_readed, ret);
    num_readed += ret;
    if (num_readed >= length)
    {
      break;
    }
  }
  while (1);

  if ((ret < 0) || (num_readed < length))
  {
    printf("tfsread failed (%d), readed(%d)\n", ret, num_readed);
    ret = TFS_ERROR;
    goto error;
  }

  if (crc != file_info.crc_)
  {
    printf("crc check failed (%d), info.crc(%d)\n", crc, file_info.crc_);
    ret = TFS_ERROR;
    goto error;
  }
  else
  {
    ret = TFS_SUCCESS;
  }

error:
  tfs_client_->tfs_close();
  return ret;
}

int TfsIoApiTest::write_read_file(const int32_t length)
{
  char buffer[length];
  generate_data(buffer, length);

  if (write_new_file(buffer, length) != TFS_SUCCESS)
  {
    return TFS_ERROR;
  }

  int32_t ret_len = length;
  char result[ret_len];
  memset(result, 0, ret_len);
  std::string tfsfile(tfs_client_->get_file_name());
  if (read_exist_file(tfsfile, result, ret_len) != TFS_SUCCESS)
  {
    printf("read_exist_file failed\n");
    return TFS_ERROR;
  }

  if ((memcmp(buffer, result, ret_len) != 0) || (length != ret_len))
  {
    return EXIT_FILECONTENT_MATCHERROR;
  }
  return TFS_SUCCESS;
}

int TfsIoApiTest::rename_file(const int32_t length)
{
  char buffer[length];
  generate_data(buffer, length);
  if (write_new_file(buffer, length) != TFS_SUCCESS)
    return TFS_ERROR;

  char oldname[256], newname[256];
  strcpy(oldname, tfs_client_->get_file_name());
  FileInfo file_info;

  // remove
  if (tfs_client_->rename(oldname, "", "newprefix") != TFS_SUCCESS)
    return TFS_ERROR;

  strcpy(newname, tfs_client_->get_file_name());
  if (stat_exist_file(oldname, file_info) == TFS_SUCCESS)
    return TFS_ERROR;
  if (stat_exist_file(newname, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  return TFS_SUCCESS;
}

int TfsIoApiTest::unlink_file(const int32_t length)
{
  char buffer[length];
  generate_data(buffer, length);
  if (write_new_file(buffer, length) != TFS_SUCCESS)
    return TFS_ERROR;

  char tfsfile[256];
  strcpy(tfsfile, tfs_client_->get_file_name());
  FileInfo file_info;

  // remove
  if (tfs_client_->unlink(tfsfile, "", DELETE) != TFS_SUCCESS)
    return TFS_ERROR;
  usleep(100 * 1000);
  if (stat_exist_file(tfsfile, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  if (file_info.flag_ != 1)
  {
    printf("unlink %s failure , flag:%d\n", tfsfile, file_info.flag_);
    return TFS_ERROR;
  }

  // undelete
  if (tfs_client_->unlink(tfsfile, "", UNDELETE) != TFS_SUCCESS)
    return TFS_ERROR;
  usleep(100 * 1000);
  if (stat_exist_file(tfsfile, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  if (file_info.flag_ != 0)
  {
    printf("undelete %s failure , flag:%d\n", tfsfile, file_info.flag_);
    return TFS_ERROR;
  }

  // hide
  if (tfs_client_->unlink(tfsfile, "", CONCEAL) != TFS_SUCCESS)
    return TFS_ERROR;
  usleep(100 * 1000);
  if (stat_exist_file(tfsfile, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  if (file_info.flag_ != 4)
  {
    printf("hide %s failure , flag:%d\n", tfsfile, file_info.flag_);
    return TFS_ERROR;
  }
  //
  // unhide
  if (tfs_client_->unlink(tfsfile, "", REVEAL) != TFS_SUCCESS)
    return TFS_ERROR;
  usleep(100 * 1000);
  if (stat_exist_file(tfsfile, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  if (file_info.flag_ != 0)
  {
    printf("unhide %s failure , flag:%d\n", tfsfile, file_info.flag_);
    return TFS_ERROR;
  }
  return TFS_SUCCESS;
}

TEST_F(TfsIoApiTest, write_read_file)
{
  for (int i = 0; i < MAX_LOOP; ++i)
  {
    EXPECT_EQ(TFS_SUCCESS, TfsIoApiTest::write_read_file(200)) << TfsIoApiTest::get_error_message();
  }
}

TEST_F(TfsIoApiTest, write_new_file)
{
  for (int i = 0; i < MAX_LOOP; ++i)
  {
    EXPECT_EQ(TFS_SUCCESS, TfsIoApiTest::write_new_file(200)) << TfsIoApiTest::get_error_message();
  }
}

TEST_F(TfsIoApiTest, write_update_file)
{
  for (int i = 0; i < MAX_LOOP; ++i)
  {
    EXPECT_EQ(TFS_SUCCESS, TfsIoApiTest::write_update_file(200)) << TfsIoApiTest::get_error_message();
  }
}

TEST_F(TfsIoApiTest, rename_file)
{
  for (int i = 0; i < MAX_LOOP; ++i)
  {
    EXPECT_EQ(TFS_SUCCESS, TfsIoApiTest::rename_file(200)) << TfsIoApiTest::get_error_message();
  }
}

TEST_F(TfsIoApiTest, unlink_file)
{
  for (int i = 0; i < MAX_LOOP; ++i)
  {
    EXPECT_EQ(TFS_SUCCESS, TfsIoApiTest::unlink_file(200)) << TfsIoApiTest::get_error_message();
  }
}

void usage(char* name)
{
  printf("Usage: %s -s nsip:port\n", name);
  exit(TFS_ERROR);
}

int parse_args(int argc, char *argv[])
{
  char nsip[256];
  int i = 0;

  memset(nsip, 0, 256);
  while ((i = getopt(argc, argv, "s:i")) != EOF)
  {
    switch (i)
    {
      case 's':
        strcpy(nsip, optarg);
        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  if ('\0' == nsip[0])
  {
    usage(argv[0]);
    return TFS_ERROR;
  }

  TfsIoApiTest::tfs_client_ = new TfsClient();
	int iret = TfsIoApiTest::tfs_client_->initialize(nsip);
	if (iret != TFS_SUCCESS)
	{
		return TFS_ERROR;
	}

  return TFS_SUCCESS;
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  int ret = parse_args(argc, argv);
  if (ret == TFS_ERROR)
  {
    printf("input argument error...\n");
  }
  return RUN_ALL_TESTS();
}
