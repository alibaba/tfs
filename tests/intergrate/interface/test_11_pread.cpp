#include"tfs_client_init.h"


TEST_F(TfsInit,01_pread_right)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 int64_t offset=0;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,02_pread_more_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=110*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 int64_t offset=0;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,03_pread_less_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=10*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 int64_t offset=0;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_EQ(10*(1<<10),ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,04_pread_wrong_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=-1;
	 
	 suffix=NULL;
	 flags=T_READ;
	 int64_t offset=0;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_GT(0,ret);
	 
	 tfsclient->close(fd);
}



TEST_F(TfsInit,05_pread_null_buf)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=0;
	 
	 suffix=NULL;
	 flags=T_READ;
	 int64_t offset=0;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, NULL,count,offset);
	 EXPECT_GT(0,ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,06_pread_wrong_fd)
{
	 
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=-1;
	 int64_t ret;
	 int64_t offset=0;
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);
}

TEST_F(TfsInit,07_pread_not_open_fd)
{
	 
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=45;
	 int64_t ret;
	 int64_t offset=0;
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);
}

TEST_F(TfsInit,08_pread_large)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 static char buf[1<<30];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a1Gjpg,T_LARGE,NULL,NULL);
	 EXPECT_EQ(1<<30,ret);
	 count=1<<30;
	 
	 
	 suffix=NULL;
	 flags=T_READ|T_LARGE;
	 int64_t offset=0;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_EQ(1<<30,ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,09_pread_with_offset)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 int64_t offset=10*(1<<10);
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_EQ(90*(1<<10),ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,10_pread_wrong_offset)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 int64_t offset=-1;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_GT(0,ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,11_pread_more_offset)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 int64_t offset=110*(1<<10);
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_GT(0,ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,12_pread_more_offset_count)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=20*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 int64_t offset=90*(1<<10);
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->pread(fd, buf,count,offset);
	 EXPECT_EQ(10*(1<<10),ret);
	 
	 tfsclient->close(fd);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}






















