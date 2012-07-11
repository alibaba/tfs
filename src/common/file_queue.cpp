/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: file_queue.cpp 983 2011-10-31 09:59:33Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "file_queue.h"
#include "directory_op.h"
#include "error_msg.h"

using namespace tfs;
using namespace common;

FileQueue::FileQueue(const std::string& dir_path, const std::string& queue_name, const int32_t max_file_size) :
  read_fd_(-1), write_fd_(-1), information_fd_(-1), max_file_size_(max_file_size), delete_file_flag_(false),
      queue_path_(dir_path + "/" + queue_name)
{
  memset(&queue_information_header_, 0, sizeof(queue_information_header_));
}

int FileQueue::load_queue_head()
{
  if (!DirectoryOp::create_full_path(queue_path_.c_str()))
  {
    TBSYS_LOG(ERROR, "create directoy : %s failed", queue_path_.c_str());
    return EXIT_MAKEDIR_ERROR;
  }

  std::string path = queue_path_ + "/header.dat";
  information_fd_ = open(path.c_str(), O_RDWR | O_CREAT, 0600);
  if (information_fd_ == -1)
  {
    TBSYS_LOG(ERROR, "open file: %s failed", path.c_str());
    return EXIT_OPEN_FILE_ERROR;
  }

  if (read(information_fd_, &queue_information_header_, sizeof(queue_information_header_))
      != sizeof(queue_information_header_))
  {
    memset(&queue_information_header_, 0, sizeof(queue_information_header_));
    queue_information_header_.read_seqno_ = 0x01;
    queue_information_header_.write_seqno_ = 0x01;
  }
  TBSYS_LOG(DEBUG, "readno: %d, writeno: %d, queueSize: %d", queue_information_header_.read_seqno_,
      queue_information_header_.write_seqno_, queue_information_header_.queue_size_);
  return TFS_SUCCESS;
}

int FileQueue::initialize()
{
  if (open_write_file() != TFS_SUCCESS)
  {
    return EXIT_OPEN_FILE_ERROR;
  }

  struct stat st;
  if (fstat(write_fd_, &st) == 0)
  {
    queue_information_header_.write_filesize_ = st.st_size;
  }

  if (open_read_file() != TFS_SUCCESS)
  {
    return EXIT_OPEN_FILE_ERROR;
  }

  if (queue_information_header_.exit_status_ == 0)
  {
    recover_record();
  }

  queue_information_header_.exit_status_ = 0;

  return TFS_SUCCESS;
}

FileQueue::~FileQueue()
{
  queue_information_header_.exit_status_ = 1;
  memset(&queue_information_header_.pos_[0], 0, FILE_QUEUE_MAX_THREAD_SIZE * sizeof(unsettle));
  write_header();
  if (information_fd_ != -1)
  {
    close(information_fd_);
    information_fd_ = -1;
  }
  if (read_fd_ != -1)
  {
    close(read_fd_);
    read_fd_ = -1;
  }
  if (write_fd_ != -1)
  {
    close( write_fd_);
    write_fd_ = -1;
  }
}

int FileQueue::push(const void* const data, const int64_t len)
{
  if (data == NULL || len <= 0 || len > TFS_MALLOC_MAX_SIZE)
  {
    TBSYS_LOG(WARN, "invalid data: %p, length: %"PRI64_PREFIX"d", data, len);
    return EXIT_GENERAL_ERROR;
  }

  if (write_fd_ == -1)
  {
    if (open_write_file() != TFS_SUCCESS)
    {
      return EXIT_GENERAL_ERROR;
    }
  }

  int32_t size = sizeof(int32_t) + sizeof(QueueItem) + len;
  char *buffer = (char*) malloc(size);
  *((int32_t*) buffer) = size;
  QueueItem *item = (QueueItem*) (buffer + sizeof(int32_t));
  item->length_ = len;
  memcpy(&(item->data_[0]), data, len);

  if (queue_information_header_.write_filesize_ >= max_file_size_)
  {
    queue_information_header_.write_seqno_++;
    queue_information_header_.write_filesize_ = 0;
    open_write_file();
    write_header();
  }

  item->pos_.seqno_ = queue_information_header_.write_seqno_;
  item->pos_.offset_ = queue_information_header_.write_filesize_;
  int ret = write(write_fd_, buffer, size);
  if (ret > 0)
  {
    queue_information_header_.write_filesize_ += ret;
  }
  free(buffer);

  if (ret != size)
  {
    TBSYS_LOG(WARN, "write filed, path: %s, fd: %d, lenght: %"PRI64_PREFIX"d, ret: %d, size: %d", queue_path_.c_str(), write_fd_,
        len, ret, size);
    ret = size - ret;
    if (ret > 0 && ret <= size)
    {
      lseek(write_fd_, ret, SEEK_CUR);
    }
    return EXIT_GENERAL_ERROR;
  }
  queue_information_header_.queue_size_++;
  TBSYS_LOG(DEBUG, "queue_size_: %d,size: %d,iret: %d, write_filesize_: %d, max_file_size: %u, write_seqno_: %d",
      queue_information_header_.queue_size_, size, ret, queue_information_header_.write_filesize_, max_file_size_,
      queue_information_header_.write_seqno_);
  return TFS_SUCCESS;
}

QueueItem *FileQueue::pop(int index)
{
  int size = 0;
  int iret = 0;
  int count = 0;
  if (read_fd_ == -1)
  {
    TBSYS_LOG(WARN, "file %s:%d is close...", queue_path_.c_str(), queue_information_header_.read_seqno_);
    do
    {
      ++count;
      iret = open_read_file();
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "reopen file %s: %d failed", queue_path_.c_str(), queue_information_header_.read_seqno_);
        ::close( read_fd_);
        read_fd_ = -1;
        continue;
      }
    }
    while (count < 0x03);

    if (read_fd_ == -1)
    {
      if (queue_information_header_.write_seqno_ > queue_information_header_.read_seqno_)
      {
        delete_read_file();
        queue_information_header_.read_seqno_++;
        queue_information_header_.read_offset_ = 0;
        open_read_file();
      }
      return NULL;
    }
  }

  count = 0;
  QueueItem *item = NULL;
  while (true)
  {
    int32_t retsize = read(read_fd_, &size, sizeof(int32_t));
    if (retsize < 0)
    {
      TBSYS_LOG(ERROR, "read head size errors %d : %s from : %d", errno, strerror(errno), read_fd_);
      break;
    }
    if (retsize != sizeof(int32_t) && retsize != 0)
    {
      TBSYS_LOG(ERROR, "read head size: %d errors %d : %s from : %d", retsize, errno, strerror(errno), read_fd_);
      lseek(read_fd_, (0 - retsize), SEEK_CUR);
      break;
    }
    if (retsize == sizeof(int32_t))
    {
      size -= sizeof(int32_t);
      off_t curpos_ = ::lseek(read_fd_, 0, SEEK_CUR);
      item = (QueueItem*) ::malloc(size);
      if (item == NULL)
      {
        TBSYS_LOG(ERROR, "malloc failed...\n");
        break;
      }
      if ((iret = read(read_fd_, item, size)) == size)
      {
        index %= FILE_QUEUE_MAX_THREAD_SIZE;
        queue_information_header_.pos_[index].seqno_ = queue_information_header_.read_seqno_;
        queue_information_header_.pos_[index].offset_ = queue_information_header_.read_offset_;
        queue_information_header_.read_offset_ += (size + sizeof(int32_t));
        break;
      }
      if (item != NULL)
      {
        ::free(item);
        item = NULL;
      }
      TBSYS_LOG(ERROR,
          "read file failed length %d : %d form fd: %d, read_seqno_: %d, read_offset_: %d, current pos_tion: %"PRI64_PREFIX"d",
          iret, size, read_fd_, queue_information_header_.read_seqno_, queue_information_header_.read_offset_, curpos_);
      ::lseek(read_fd_, 0, SEEK_END);
      if (queue_information_header_.write_seqno_ > queue_information_header_.read_seqno_)
      {
        delete_read_file();
        queue_information_header_.read_seqno_++;
        queue_information_header_.read_offset_ = 0;
        open_read_file();
      }
    }
    else if (queue_information_header_.write_seqno_ > queue_information_header_.read_seqno_)
    {
      delete_read_file();
      queue_information_header_.read_seqno_++;
      queue_information_header_.read_offset_ = 0;
      open_read_file();
    }
    else
    {
      queue_information_header_.queue_size_ = 0;
      break;
    }
  }
  write_header();
  return item;
}

/*QueueItem *FileQueue::pop(int& offset_, int index)
 {
 offset_ = 0;
 int size = 0;
 int iret = 0;
 int count = 0;
 if (read_fd_ == -1)
 {
 TBSYS_LOG(WARN, "file %s: %d is close...", queue_path_.c_str(), queue_information_header_.read_seqno_);
 do
 {
 ++count;
 iret = open_read_file();
 if (iret != TFS_SUCCESS)
 {
 TBSYS_LOG(WARN, "reopen file %s: %d failed...", queue_path_.c_str(), queue_information_header_.read_seqno_);
 ::close( read_fd_);
 read_fd_ = -1;
 continue;
 }
 }
 while (count < 0x03);
 if (read_fd_ == -1)
 {
 if (queue_information_header_.write_seqno_ > queue_information_header_.read_seqno_)
 {
 delete_read_file();
 queue_information_header_.read_seqno_++;
 queue_information_header_.read_offset_ = 0;
 open_read_file();
 }
 return NULL;
 }
 }

 count = 0;
 QueueItem *item = NULL;
 while (true)
 {
 int retsize = read(read_fd_, &size, sizeof(int32_t));
 if (retsize < 0)
 {
 TBSYS_LOG(ERROR, "read head size errors %d : %s from : %d", errno, strerror(errno), read_fd_);
 break;
 }
 if (retsize != sizeof(int32_t) && retsize != 0)
 {
 TBSYS_LOG(ERROR, "read head size: %d errors %d : %s from : %d", retsize, errno, strerror(errno), read_fd_);
 lseek(read_fd_, (0 - retsize), SEEK_CUR);
 break;
 }
 if (retsize == sizeof(int32_t))
 {
 size -= sizeof(int32_t);
 off_t curpos_ = ::lseek(read_fd_, 0, SEEK_CUR);
 item = (QueueItem*) ::malloc(size);
 if (item == NULL)
 {
 TBSYS_LOG(ERROR, "malloc failed...\n");
 break;
 }
 if ((iret = read(read_fd_, item, size)) == size)
 {
 index %= FILE_QUEUE_MAX_THREAD_SIZE;
 queue_information_header_.pos_[index].seqno_ = queue_information_header_.read_seqno_;
 queue_information_header_.pos_[index].offset_ = queue_information_header_.read_offset_;
 queue_information_header_.read_offset_ += (size + sizeof(int32_t));
 offset_ = queue_information_header_.read_offset_;
 break;
 }
 if (item != NULL)
 {
 ::free(item);
 item = NULL;
 }
 TBSYS_LOG(ERROR,
 "read file failed length %d : %d form fd: %d, read_seqno_: %d, read_offset_: %d, current pos_tion: %d",
 iret, size, read_fd_, queue_information_header_.read_seqno_, queue_information_header_.read_offset_,
 curpos_);
 ::lseek(read_fd_, 0, SEEK_END);
 if (queue_information_header_.write_seqno_ > queue_information_header_.read_seqno_)
 {
 delete_read_file();
 queue_information_header_.read_seqno_++;
 queue_information_header_.read_offset_ = 0;
 open_read_file();
 }
 }
 else if (queue_information_header_.write_seqno_ > queue_information_header_.read_seqno_)
 {
 delete_read_file();
 queue_information_header_.read_seqno_++;
 queue_information_header_.read_offset_ = 0;
 open_read_file();
 }
 else
 {
 queue_information_header_.queue_size_ = 0;
 break;
 }
 }
 write_header();
 return item;
 }*/

int FileQueue::clear()
{
  queue_information_header_.write_seqno_++;
  queue_information_header_.read_seqno_ = queue_information_header_.write_seqno_;
  queue_information_header_.read_offset_ = 0;
  queue_information_header_.write_filesize_ = 0;
  queue_information_header_.queue_size_ = 0;
  if ((open_write_file() != TFS_SUCCESS) || (open_read_file() != TFS_SUCCESS) || (write_header() != TFS_SUCCESS))
  {
    TBSYS_LOG(ERROR, "file queue call clear error");
  }
  return TFS_ERROR;
}

bool FileQueue::empty() const
{
  return queue_information_header_.queue_size_ == 0;
}

void FileQueue::finish(int index)
{
  unsettle *pos_ = &(queue_information_header_.pos_[index % FILE_QUEUE_MAX_THREAD_SIZE]);
  pos_->seqno_ = 0;
  pos_->offset_ = 0;
}

int FileQueue::open_write_file()
{
  if (write_fd_ != -1)
  {
    close( write_fd_);
    write_fd_ = -1;
  }
  char tmp[MAX_PATH_LENGTH+1];
  tmp[MAX_PATH_LENGTH] = '\0';
  snprintf(tmp, MAX_PATH_LENGTH, "%s/%08d.dat", queue_path_.c_str(), queue_information_header_.write_seqno_);
  write_fd_ = open(tmp, O_WRONLY | O_CREAT | O_APPEND, 0600);
  if (write_fd_ == -1)
  {
    TBSYS_LOG(ERROR, "open file: %s failed", tmp);
    return TFS_ERROR;
  }
  return TFS_SUCCESS;
}

int FileQueue::delete_read_file()
{
  if (!delete_file_flag_)
  {
    return TFS_SUCCESS;
  }
  if (read_fd_ != -1)
  {
    close( read_fd_);
    read_fd_ = -1;
  }
  char tmp[MAX_PATH_LENGTH+1];
  tmp[MAX_PATH_LENGTH] = '\0';
  snprintf(tmp, MAX_PATH_LENGTH, "%s/%08d.dat", queue_path_.c_str(), queue_information_header_.read_seqno_ - 1);
  unlink(tmp);
  return TFS_SUCCESS;
}

int FileQueue::open_read_file()
{
  if (read_fd_ != -1)
  {
    close( read_fd_);
    read_fd_ = -1;
  }
  char tmp[MAX_PATH_LENGTH+1];
  tmp[MAX_PATH_LENGTH] = '\0';
  snprintf(tmp, MAX_PATH_LENGTH, "%s/%08d.dat", queue_path_.c_str(), queue_information_header_.read_seqno_);
  read_fd_ = open(tmp, O_RDONLY, 0600);
  if (read_fd_ == -1)
  {
    TBSYS_LOG(ERROR, "open file: %s failed", tmp);
    return TFS_ERROR;
  }
  int iret = lseek(read_fd_, queue_information_header_.read_offset_, SEEK_SET);
  if (iret < 0)
  {
    TBSYS_LOG(ERROR, "call lseek filed: %s", strerror(errno));
    return TFS_ERROR;
  }
  return TFS_SUCCESS;
}

int FileQueue::write_header()
{
  int iret = information_fd_ < 0 ? TFS_ERROR : TFS_SUCCESS;
  if (iret == TFS_SUCCESS)
  {
    int32_t count = 0;
    do
    {
      lseek(information_fd_, 0, SEEK_SET);
      iret = write(information_fd_, &queue_information_header_, sizeof(queue_information_header_));
      if (iret == sizeof(queue_information_header_))
      {
        break;
      }
      TBSYS_LOG(ERROR, "write queue infromation header failed, path: %s, errors: %s", queue_path_.c_str(), strerror(
          errno));
      ::close( information_fd_);
      information_fd_ = -1;
      char tmp[MAX_PATH_LENGTH+1];
      tmp[MAX_PATH_LENGTH] = '\0';
      snprintf(tmp, MAX_PATH_LENGTH, "%s/header.dat", queue_path_.c_str());
      information_fd_ = open(tmp, O_RDWR | O_CREAT, 0600);
      if (information_fd_ == -1)
      {
        TBSYS_LOG(ERROR, "open file: %s failed", tmp);
        return EXIT_GENERAL_ERROR;
      }
      lseek(information_fd_, 0, SEEK_SET);
      ++count;
    }
    while (count < 0x03);
  }
  return TFS_SUCCESS;
}

int FileQueue::recover_record()
{
  int32_t fd = -1;
  int32_t size = 0;
  int32_t count = 0;
  char tmp[MAX_PATH_LENGTH+1];

  for (int32_t i = 0; i < FILE_QUEUE_MAX_THREAD_SIZE; ++i)
  {
    unsettle *pos_ = &(queue_information_header_.pos_[i]);
    // finish
    if (pos_->seqno_ == 0)
      continue;
    if (pos_->seqno_ > queue_information_header_.read_seqno_)
      continue;
    if (pos_->offset_ >= queue_information_header_.read_offset_)
      continue;

    tmp[MAX_PATH_LENGTH] = '\0';
    snprintf(tmp, MAX_PATH_LENGTH, "%s/%08d.dat", queue_path_.c_str(), pos_->seqno_);
    fd = ::open(tmp, O_RDONLY, 0600);
    if (fd == -1)
      continue;
    lseek(fd, pos_->offset_, SEEK_SET);
    if (read(fd, &size, sizeof(int32_t)) == sizeof(int32_t))
    {
      size -= sizeof(int32_t);
      QueueItem *item = (QueueItem*) malloc(size);
      if (read(fd, item, size) == size)
      {
        push(&(item->data_[0]), item->length_);
        ++count;
      }
      free(item);
    }
    ::close(fd);
    fd = -1;
  }
  TBSYS_LOG(INFO, "%s: recoverRecord: %d", queue_path_.c_str(), count);
  memset(&queue_information_header_.pos_[0], 0, FILE_QUEUE_MAX_THREAD_SIZE * sizeof(unsettle));
  return write_header();
}
