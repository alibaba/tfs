#include <stdlib.h>
#include <sys/file.h>
#include <sys/types.h>
#include <utime.h>
#include <gtest/gtest.h>
#include "fsname.h"
#include "gc_worker.h"
#include "common/func.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

class GcWorkerTest : public testing::Test
{
public:
  GcWorkerTest() : key_("testgcworker")
  {
  }

  ~GcWorkerTest()
  {
  }

  virtual void SetUp()
  {
    srand(time(NULL) + rand() + pthread_self());
  }

  virtual void TearDown()
  {
    ::unlink(key_);
  }

  int32_t generate_data(char* buffer, const int32_t length);
  int64_t prepare_data(int64_t length);
  string get_path(const char* path);
  template<class T> int test_unlink(T& seg_info, int status);

public:
  static TfsClient* tfs_client_;
  static uint64_t id_;

protected:
  GcWorker gc_worker_;
  const char* key_;
};

TfsClient* GcWorkerTest::tfs_client_;
uint64_t GcWorkerTest::id_;
int64_t PER_SIZE = 2 << 20;

int32_t GcWorkerTest::generate_data(char* buffer, const int32_t length)
{
  int32_t i = 0;
  for (i = 0; i < length; i++)
  {
    buffer[i] = rand() % 90 + 32;
  }
  return length;
}

int64_t GcWorkerTest::prepare_data(int64_t length)
{
  int local_fd = open(key_, O_CREAT|O_WRONLY|O_TRUNC, 0644);
  if (local_fd < 0)
  {
    printf("open local key fail: %s", strerror(errno));
    return local_fd;
  }
  close(local_fd);

  int fd = tfs_client_->open(NULL, NULL, T_WRITE|T_LARGE, key_);
  int64_t ret = 0, ret_len = 0;
  char* buffer = new char[PER_SIZE];
  int64_t remain_size = length;
  int64_t cur_size = 0;
  while (remain_size > 0)
  {
    cur_size = (remain_size >= PER_SIZE) ? PER_SIZE : remain_size;
    generate_data(buffer, cur_size);
    ret = tfs_client_->write(fd, buffer, cur_size);
    ret_len += ret;
    remain_size -= ret;
  }
  return ret_len;
}

string GcWorkerTest::get_path(const char* path)
{
  char name[MAX_PATH_LENGTH];
  strncpy(name, path, MAX_PATH_LENGTH - 1);
  char* tmp_file = name + strlen(path);

  if (NULL == realpath(key_, tmp_file))
  {
    return NULL;
  }
  // convert tmp file name
  char* pos = NULL;
  while ((pos = strchr(tmp_file, '/')))
  {
    tmp_file = pos;
    *pos = '!';
  }
  int len = strlen(name);
  snprintf(name + len, MAX_PATH_LENGTH - len, "!%" PRI64_PREFIX "u", id_);
  return name;
}

template<class T> int GcWorkerTest::test_unlink(T& seg_info, int status)
{
  for (typename T::iterator it = seg_info.begin(); it != seg_info.end(); it++)
  {
    FSName fsname;
    fsname.set_block_id(it->block_id_);
    fsname.set_file_id(it->file_id_);
    int fd = tfs_client_->open(fsname.get_name(), NULL, T_READ);
    if (fd < 0)
    {
      printf("open file fail, block id: %u, file id: %"PRI64_PREFIX"u", it->block_id_, it->file_id_);
      return TFS_ERROR;
    }
    FileStat stat_info;
    if (tfs_client_->fstat(fd, &stat_info, FORCE_STAT) != TFS_SUCCESS ||
        stat_info.flag_ != status)
    {
      printf("test unlink fail, block id: %u, file id: %"PRI64_PREFIX"u, status: %d while expected %d", it->block_id_, it->file_id_, stat_info.flag_, status);
      return TFS_ERROR;
    }
  }
  return TFS_SUCCESS;
}

TEST_F(GcWorkerTest, test_gc_worker)
{
  int64_t len = 100000000;

  // generate gc file
  EXPECT_EQ(prepare_data(len), len);
  EXPECT_EQ(prepare_data(len/2), len/2);

  string key_path = get_path(LOCAL_KEY_PATH), gc_file_path = get_path(GC_FILE_PATH);
  EXPECT_EQ(access(key_path.c_str(), F_OK), 0);
  EXPECT_EQ(access(gc_file_path.c_str(), F_OK), 0);

  LocalKey local_key;
  GcFile gc_file;
  EXPECT_EQ(local_key.load_file(key_path.c_str()), TFS_SUCCESS);
  EXPECT_EQ(gc_file.load_file(gc_file_path.c_str()), TFS_SUCCESS);

  // update to now
  EXPECT_EQ(utime(key_path.c_str(), NULL), 0);
  EXPECT_EQ(utime(gc_file_path.c_str(), NULL), 0);
  // actually no gc
  EXPECT_EQ(gc_worker_.do_start(), TFS_SUCCESS);
  EXPECT_EQ(access(key_path.c_str(), F_OK), 0);
  EXPECT_EQ(access(gc_file_path.c_str(), F_OK), 0);
  // no gc now, test status ok
  EXPECT_EQ(test_unlink(local_key.get_seg_info(), 0), TFS_SUCCESS);
  EXPECT_EQ(test_unlink(gc_file.get_seg_info(), 0), TFS_SUCCESS);

  // update timestamp to expired
  struct utimbuf time_buf;
  time_buf.actime = time(NULL) - GC_EXPIRED_TIME - 10; // expired
  time_buf.modtime = time(NULL) - GC_EXPIRED_TIME - 10; // expired
  EXPECT_EQ(utime(key_path.c_str(), &time_buf), 0);
  EXPECT_EQ(utime(gc_file_path.c_str(), &time_buf), 0);

  // lock
  int key_fd = open(key_path.c_str(), O_RDONLY),
    gc_fd = open(gc_file_path.c_str(), O_RDONLY);
  EXPECT_TRUE(key_fd > 0);
  EXPECT_TRUE(gc_fd > 0);
  EXPECT_EQ(::flock(key_fd, LOCK_EX|LOCK_NB), 0);
  EXPECT_EQ(::flock(gc_fd, LOCK_EX|LOCK_NB), 0);
  // actully no gc
  EXPECT_EQ(gc_worker_.do_start(), TFS_SUCCESS);
  EXPECT_EQ(access(key_path.c_str(), F_OK), 0);
  EXPECT_EQ(access(gc_file_path.c_str(), F_OK), 0);
  // no gc, test status ok
  EXPECT_EQ(test_unlink(local_key.get_seg_info(), 0), TFS_SUCCESS);
  EXPECT_EQ(test_unlink(gc_file.get_seg_info(), 0), TFS_SUCCESS);

  // release lock
  EXPECT_EQ(::flock(key_fd, LOCK_UN), 0);
  EXPECT_EQ(::flock(gc_fd, LOCK_UN), 0);
  close(key_fd);
  close(gc_fd);
  // gc
  EXPECT_EQ(gc_worker_.do_start(), TFS_SUCCESS);
  EXPECT_TRUE(access(key_path.c_str(), F_OK) != 0);
  EXPECT_TRUE(access(gc_file_path.c_str(), F_OK) != 0);
  // test unlink success
  EXPECT_EQ(test_unlink(local_key.get_seg_info(), 1), TFS_SUCCESS);
  EXPECT_EQ(test_unlink(gc_file.get_seg_info(), 1), TFS_SUCCESS);
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

  GcWorkerTest::id_ = Func::get_host_ip(nsip);
  GcWorkerTest::tfs_client_ = TfsClient::Instance();
	int ret = GcWorkerTest::tfs_client_->initialize(nsip);
	if (ret != TFS_SUCCESS)
	{
    cout << "tfs client init fail" << endl;
		return TFS_ERROR;
	}

  return TFS_SUCCESS;
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  int ret = parse_args(argc, argv);
  if (TFS_ERROR == ret)
  {
    printf("input argument error...\n");
    return TFS_ERROR;
  }
  return RUN_ALL_TESTS();
}
