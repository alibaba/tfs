#include"tfs_client_init.h"


TEST_F(TfsInit,01_readv2_right)
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
	 TfsFileStat stat;
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->readv2(fd, buf,count,&stat);
	 EXPECT_EQ(100*(1<<10),ret);
	 ASSERT_EQ(stat->size_, 100*(1<<10));
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,02_readv2_more_count)
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
	 TfsFileStat stat;
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->readv2(fd, buf,count,&stat);
	 EXPECT_EQ(100*(1<<10),ret);
	 ASSERT_EQ(stat->size_, 100*(1<<10));
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,03_readv2_less_count)
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
	 TfsFileStat stat;
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->readv2(fd, buf,count,&stat);
	 EXPECT_EQ(10*(1<<10),ret);
	 ASSERT_EQ(stat->size_, 100*(1<<10));
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,04_readv2_wrong_count)
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
	 TfsFileStat stat;
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->readv2(fd, buf,count,&stat);
	 EXPECT_GT(0,ret);
	 
	 tfsclient->close(fd);
}



TEST_F(TfsInit,05_readv2_null_buf)
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
	 TfsFileStat stat;
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->readv2(fd, NULL,count,&stat);
	 EXPECT_GT(0,ret);

	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,06_readv2_wrong_fd)
{
	 
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=-1;
	 int64_t ret;
	 TfsFileStat stat;
	 ret=tfsclient->readv2(fd, buf,count,&stat);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);

}

TEST_F(TfsInit,07_readv2_not_open_fd)
{
	 
	 int64_t count=0;
	 char buf[100*(1<<10)];
	 int fd=45;
	 int64_t ret;
	 TfsFileStat stat;
	 ret=tfsclient->readv2(fd, buf,count,&stat);
	 EXPECT_EQ(EXIT_INVALIDFD_ERROR,ret);

}

TEST_F(TfsInit,08_readv2_large)
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
	 TfsFileStat stat;
	 
	 suffix=NULL;
	 flags=T_READ|T_LARGE;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->readv2(fd, buf,count,&stat);
	 EXPECT_EQ(1<<30,ret);
	 ASSERT_EQ(stat->size_, 1<<30);
	 
	 tfsclient->close(fd);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}






























