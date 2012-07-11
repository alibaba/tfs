#include <gtest/gtest.h>
#include "local_key.h"
#include "func.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

class LocalKeyTest: public ::testing::Test
{
public:
  LocalKeyTest () : count_(10), key_("/tmp/TestlocalKey")
  {
    id_ = Func::get_host_ip("127.0.0.1:3100");
  }

  ~LocalKeyTest()
  {
  }

  virtual void SetUp()
  {
    init_key();
    local_key_ = new LocalKey();

    SegmentHead seg_head;
    seg_head.count_ = count_;

    SegmentInfo seg_info;
    for (int32_t i = 0; i < count_; ++i)
    {
      seg_info.block_id_ = i*10;
      seg_info.file_id_ = i*100;
      seg_info.offset_ = i*30;
      seg_info.size_ = 30;
      seg_info.crc_ = i*1000;
      memcpy(buf_ + sizeof(SegmentHead) + sizeof(SegmentInfo) * i, &seg_info, sizeof(SegmentInfo));
      seg_head.size_ += seg_info.size_;
    }
    memcpy(buf_, &seg_head, sizeof(SegmentHead));

    snprintf(final_key_, MAX_PATH_LENGTH-1, "%s!tmp!TestlocalKey!%"PRI64_PREFIX"u", LOCAL_KEY_PATH, id_);
    // ignore error
    ::unlink(final_key_);
  }

  virtual void TearDown()
  {
    destroy_seg();
    tbsys::gDelete(local_key_);
  }

  int init_key()
  {
    int fd = open(key_, O_CREAT|O_RDONLY, 0644);
    if (fd < 0)
    {
      printf("init key fail: %s", strerror(errno));
    }
    return fd;
  }

  void destroy_seg()
  {
    for (size_t i = 0; i < seg_list_.size(); i++)
    {
      if (seg_list_[i] != NULL)
      {
        tbsys::gDelete(seg_list_[i]);
        seg_list_[i] = NULL;
      }
    }
    seg_list_.clear();
  }

protected:
  LocalKey* local_key_;
  char buf_[2048];              // just enough
  int32_t count_;
  char* key_;
  uint64_t id_;
  char final_key_[MAX_PATH_LENGTH];
  SEG_DATA_LIST seg_list_;
};


TEST_F(LocalKeyTest, test_initialize_no_init)
{
  EXPECT_EQ(::unlink(key_), 0);
  // not init
  EXPECT_EQ(TFS_ERROR, local_key_->initialize(key_, id_));

  EXPECT_TRUE(init_key() > 0);
  EXPECT_EQ(TFS_SUCCESS, local_key_->initialize(key_, id_));
  EXPECT_EQ(TFS_SUCCESS, local_key_->load(buf_));
  EXPECT_EQ(TFS_SUCCESS, local_key_->save());
  EXPECT_EQ(0, access(final_key_, F_OK));
}

TEST_F(LocalKeyTest, test_initialize_init)
{
  int32_t size = sizeof(SegmentHead) + count_ * sizeof(SegmentInfo);
  int fd = open(final_key_, O_CREAT|O_RDWR, 0644);
  EXPECT_TRUE(fd > 0);
  EXPECT_TRUE(write(fd, buf_, size) == size);
  close(fd);

  EXPECT_EQ(TFS_SUCCESS, local_key_->initialize(key_, id_));
  SEG_SET& seg_info = local_key_->get_seg_info();
  int32_t i = 0;
  for (SEG_SET_ITER it = seg_info.begin(); it != seg_info.end(); ++it, ++i)
  {
    EXPECT_EQ(it->block_id_, static_cast<uint32_t>(i*10));
    EXPECT_EQ(it->file_id_, static_cast<uint64_t>(i*100));
    EXPECT_EQ(it->offset_, i*30);
    EXPECT_EQ(it->size_, 30);
    EXPECT_EQ(it->crc_, i*1000);
  }
  EXPECT_EQ(i, count_);
}

TEST_F(LocalKeyTest, test_load)
{
  EXPECT_EQ(TFS_SUCCESS, local_key_->load(buf_));
  EXPECT_EQ(local_key_->get_data_size(), static_cast<int32_t>(count_*sizeof(SegmentInfo)+sizeof(SegmentHead)));
  EXPECT_EQ(local_key_->get_segment_size(), count_);
  SEG_SET& seg_info = local_key_->get_seg_info();
  int32_t i = 0;
  for (SEG_SET_ITER it = seg_info.begin(); it != seg_info.end(); ++it, ++i)
  {
    EXPECT_EQ(it->block_id_, static_cast<uint32_t>(i*10));
    EXPECT_EQ(it->file_id_, static_cast<uint64_t>(i*100));
    EXPECT_EQ(it->offset_, i*30);
    EXPECT_EQ(it->size_, 30);
    EXPECT_EQ(it->crc_, i*1000);
  }
  EXPECT_EQ(i, count_);
}

TEST_F(LocalKeyTest, test_load_file)
{
  int fd = open("./tmpfile", O_CREAT|O_RDWR, 0644);
  int32_t size = sizeof(SegmentHead) + count_ * sizeof(SegmentInfo);
  EXPECT_TRUE(fd > 0);
  EXPECT_EQ(size, write(fd, buf_, size));
  EXPECT_EQ(TFS_SUCCESS, local_key_->load_file("./tmpfile"));

  SEG_SET& seg_info = local_key_->get_seg_info();
  int32_t i = 0;
  for (SEG_SET_ITER it = seg_info.begin(); it != seg_info.end(); ++it, ++i)
  {
    EXPECT_EQ(it->block_id_, static_cast<uint32_t>(i*10));
    EXPECT_EQ(it->file_id_, static_cast<uint64_t>(i*100));
    EXPECT_EQ(it->offset_, i*30);
    EXPECT_EQ(it->size_, 30);
    EXPECT_EQ(it->crc_, i*1000);
  }
  EXPECT_EQ(i, count_);
  ::unlink("./tmpfile");
}

TEST_F(LocalKeyTest, test_dump_data)
{
  local_key_->load(buf_);
  int32_t size = local_key_->get_data_size();
  char* data = new char[size];
  local_key_->dump_data(data);
  EXPECT_EQ(memcmp(data, buf_, size), 0);
  tbsys::gDeleteA(data);
}

TEST_F(LocalKeyTest, test_validate)
{
  EXPECT_EQ(TFS_SUCCESS, local_key_->load(buf_));
  EXPECT_EQ(TFS_SUCCESS, local_key_->validate());

  SegmentHead* seg_head = reinterpret_cast<SegmentHead*>(buf_);
  int64_t size = seg_head->size_;
  int32_t count = seg_head->count_;

  // head size conflict
  seg_head->size_ = 0;
  EXPECT_TRUE(local_key_->load(buf_) != TFS_SUCCESS);
  seg_head->size_ = size;

  // head count conflict
  seg_head->count_ = 0;
  EXPECT_TRUE(local_key_->load(buf_) != TFS_SUCCESS);
  seg_head->count_ = count;

  SegmentInfo* seg_info = reinterpret_cast<SegmentInfo*>(buf_+sizeof(SegmentHead));
  // offset not start with 0
  seg_head->size_ -= 10;
  seg_info[0].size_ -= 10;
  seg_info[0].offset_ += 10;
  EXPECT_EQ(TFS_SUCCESS, local_key_->load(buf_));
  EXPECT_TRUE(local_key_->validate() != TFS_SUCCESS);
  seg_head->size_ += 10;
  seg_info[0].size_ += 10;

  // offset + size != next_offset
  seg_info[0].offset_ = 0;
  seg_head->size_ += 10;
  seg_info[0].size_ += 10;
  EXPECT_EQ(TFS_SUCCESS, local_key_->load(buf_));
  EXPECT_TRUE(local_key_->validate() != TFS_SUCCESS);
}

TEST_F(LocalKeyTest, test_save)
{
  EXPECT_TRUE(local_key_->save() != TFS_SUCCESS); // no file op init, should fail

  EXPECT_EQ(TFS_SUCCESS, local_key_->initialize(key_, id_));
  EXPECT_EQ(TFS_SUCCESS, local_key_->load(buf_));
  EXPECT_EQ(TFS_SUCCESS, local_key_->save());

  int fd = open(final_key_, O_RDONLY);
  EXPECT_TRUE(fd > 0);
  int32_t size = sizeof(SegmentHead) + count_ * sizeof(SegmentInfo);
  char* data = new char[size];
  EXPECT_EQ(size, read(fd, data, size));
  EXPECT_EQ(memcmp(buf_, data, size), 0);
  tbsys::gDeleteA(data);
}

TEST_F(LocalKeyTest, test_add_segment)
{
  SegmentInfo seg_info;
  EXPECT_EQ(TFS_SUCCESS, local_key_->load(buf_));
  seg_info.offset_ = 0;
  seg_info.size_ = 1024;
  int32_t old_size = local_key_->get_data_size();
  int64_t old_file_size = local_key_->get_file_size();
  EXPECT_TRUE(local_key_->add_segment(seg_info) != TFS_SUCCESS);
  seg_info.offset_ = -1;
  EXPECT_EQ(TFS_SUCCESS, local_key_->add_segment(seg_info));
  EXPECT_EQ(old_size + static_cast<int32_t>(sizeof(SegmentInfo)), local_key_->get_data_size());
  EXPECT_EQ(old_file_size + 1024, local_key_->get_file_size());
}

TEST_F(LocalKeyTest, test_remove)
{
  EXPECT_EQ(TFS_SUCCESS, local_key_->initialize(key_, id_));
  EXPECT_EQ(TFS_SUCCESS, local_key_->load(buf_));
  EXPECT_EQ(TFS_SUCCESS, local_key_->save());
  EXPECT_EQ(0, access(final_key_, F_OK));
  EXPECT_EQ(TFS_SUCCESS, local_key_->remove());
  EXPECT_TRUE(access(final_key_, F_OK) != 0);
}

TEST_F(LocalKeyTest, test_get_segment_for_read)
{
  char* buf = (char*)0xbfff1111;       // just test

  local_key_->load(buf_);

  EXPECT_EQ(TFS_SUCCESS, local_key_->get_segment_for_read(20, buf, 67, seg_list_));
  EXPECT_EQ(seg_list_.size(), static_cast<size_t>(3));

  // hard code to check
  SEG_DATA_LIST_ITER it = seg_list_.begin();
  EXPECT_EQ((*it)->seg_info_.offset_, 20);
  EXPECT_EQ((*it)->seg_info_.size_, 10);
  EXPECT_EQ((*it)->buf_, buf);
  it++;
  EXPECT_EQ((*it)->seg_info_.offset_, 30);
  EXPECT_EQ((*it)->seg_info_.size_, 30);
  EXPECT_EQ((*it)->buf_, buf+10);
  it++;
  EXPECT_EQ((*it)->seg_info_.offset_, 60);
  EXPECT_EQ((*it)->seg_info_.size_, 27);
  EXPECT_EQ((*it)->buf_, buf+40);
}

TEST_F(LocalKeyTest, test_get_segment_for_write)
{
  char buf[1024];

  // test sequence case
  local_key_->load(buf_);
  // current segment offset list this:
  // 0~30 30~60 60~90 ...
  EXPECT_EQ(TFS_SUCCESS, local_key_->get_segment_for_write(1, buf, 70, seg_list_));
  // crc error
  EXPECT_EQ(seg_list_.size(), static_cast<size_t>(3));
  EXPECT_EQ(1, seg_list_[0]->seg_info_.offset_);
  EXPECT_EQ(29, seg_list_[0]->seg_info_.size_);
  EXPECT_EQ(buf, seg_list_[0]->buf_);
  EXPECT_EQ(30, seg_list_[1]->seg_info_.offset_);
  EXPECT_EQ(30, seg_list_[1]->seg_info_.size_);
  EXPECT_EQ(buf+29, seg_list_[1]->buf_);
  EXPECT_EQ(60, seg_list_[2]->seg_info_.offset_);
  EXPECT_EQ(11, seg_list_[2]->seg_info_.size_);
  EXPECT_EQ(buf+59, seg_list_[2]->buf_);

  destroy_seg();
  local_key_->load(buf_);
  memset(buf, 0, 1024);
  EXPECT_EQ(TFS_SUCCESS, local_key_->get_segment_for_write(400, buf, SEGMENT_SIZE/2+2*SEGMENT_SIZE, seg_list_));
  EXPECT_EQ(seg_list_.size(), static_cast<size_t>(3));
  EXPECT_EQ(seg_list_[0]->seg_info_.offset_, 400);
  EXPECT_EQ(seg_list_[0]->seg_info_.size_, SEGMENT_SIZE);
  EXPECT_EQ(seg_list_[0]->buf_, buf);
  EXPECT_EQ(seg_list_[1]->seg_info_.offset_, 400+SEGMENT_SIZE);
  EXPECT_EQ(seg_list_[1]->seg_info_.size_, SEGMENT_SIZE);
  EXPECT_EQ(seg_list_[1]->buf_, buf+SEGMENT_SIZE);
  EXPECT_EQ(seg_list_[2]->seg_info_.offset_, 400+2*SEGMENT_SIZE);
  EXPECT_EQ(seg_list_[2]->seg_info_.size_, SEGMENT_SIZE/2);
  EXPECT_EQ(seg_list_[2]->buf_, buf+2*SEGMENT_SIZE);

  // test random case
  SegmentHead seg_head;
  seg_head.count_ = count_;

  SegmentInfo seg_info;
  for (int32_t i = 0; i < count_; ++i)
  {
    seg_info.block_id_ = i*10;
    seg_info.file_id_ = i*100;
    seg_info.offset_ = i*30 + 10;
    seg_info.size_ = 20;
    seg_info.crc_ = 0;
    memcpy(buf_ + sizeof(SegmentHead) + sizeof(SegmentInfo) * i, &seg_info, sizeof(SegmentInfo));
    seg_head.size_ += seg_info.size_;
  }
  memcpy(buf_, &seg_head, sizeof(SegmentHead));
  local_key_->load(buf_);

  // current segment offset like this: [--] is empty offset hole
  // [--] 10~30 [--] 40~60 [--] 70~90 [--] ...

  destroy_seg();
  EXPECT_EQ(TFS_SUCCESS, local_key_->get_segment_for_write(2, buf, 37, seg_list_));
  EXPECT_EQ(seg_list_.size(), static_cast<size_t>(2));
  EXPECT_EQ(seg_list_[0]->seg_info_.offset_, 2);
  EXPECT_EQ(seg_list_[0]->seg_info_.size_, 8); // 10 - 2
  EXPECT_EQ(seg_list_[0]->buf_, buf);
  EXPECT_EQ(seg_list_[1]->seg_info_.offset_, 30);
  EXPECT_EQ(seg_list_[1]->seg_info_.size_, 9); // 2 + 37 - 30
  EXPECT_EQ(seg_list_[1]->buf_, buf+28);       // 20 + 8

  destroy_seg();
  local_key_->load(buf_);
  // 20~40, 60~70, 70~80
  // no optimization, should merge little piece
  EXPECT_EQ(TFS_SUCCESS, local_key_->get_segment_for_write(20, buf, 60, seg_list_));
  EXPECT_EQ(seg_list_.size(), static_cast<size_t>(3));
  EXPECT_EQ(seg_list_[0]->seg_info_.offset_, 20);
  EXPECT_EQ(seg_list_[0]->seg_info_.size_, 20);
  EXPECT_EQ(seg_list_[0]->buf_, buf);
  EXPECT_EQ(seg_list_[1]->seg_info_.offset_, 60);
  EXPECT_EQ(seg_list_[1]->seg_info_.size_, 10);
  EXPECT_EQ(seg_list_[1]->buf_, buf+40);
  EXPECT_EQ(seg_list_[2]->seg_info_.offset_, 70);
  EXPECT_EQ(seg_list_[2]->seg_info_.size_, 10);
  EXPECT_EQ(seg_list_[2]->buf_, buf+50);
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
