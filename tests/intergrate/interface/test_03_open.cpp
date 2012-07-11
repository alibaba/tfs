#include"tfs_client_init.h"



TEST_F(TfsInit,01_open_T_DEFAULT)
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
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
     Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,02_open_T_READ)
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
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_READ;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 
     Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 tfsclient->close(fd);
}


TEST_F(TfsInit,03_open_T_CREATE)
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
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 suffix=NULL;
	 flags=T_CREATE;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 
     Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 tfsclient->close(fd);
}


TEST_F(TfsInit,04_open_T_NEWBLK)
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
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_NEWBLK;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
     Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,05_open_T_STAT)
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
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 TfsFileStat stat;
	 
	 suffix=NULL;
	 flags=T_STAT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 Ret=tfsclient->fstat(fd,&stat);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,Ret);
	 EXPECT_EQ(stat.size_,100*(1<<10));
	 
     Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,06_open_T_UNLINK)
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
	 int Ret;
	 
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_UNLINK;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
     Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,07_open_T_LARGE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char buf[5*(1<<20)];
	 TfsFileStat stat;
	 
	 int fd;
	 int64_t ret;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a5M,T_LARGE);
	 EXPECT_EQ(5*(1<<20),ret);
	 count=5*(1<<20);

	 suffix=NULL;
	 flags=T_LARGE|T_READ|T_STAT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(5*(1<<20),ret);
	 
	 Ret=tfsclient->fstat(fd,&stat);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,Ret);
	 EXPECT_EQ(stat.size_,5*(1<<20));
	 
     Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,08_open_small_T_LARGE)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 
	 suffix=NULL;
	 flags=T_LARGE;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,09_open_large_T_DEFAULT)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a5M,T_LARGE);
	 EXPECT_EQ(5*(1<<20),ret);
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,10_open_null_tfsname)
{
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(NULL,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
}

TEST_F(TfsInit,11_open_empty_tfsname)
{
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 
	 int fd;
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open("",suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
}

TEST_F(TfsInit,12_open_wrong_tfsname)
{
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char* tfs_name="tyuioplkjhgfrdcvwb";
	 
	 int fd;
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
}

TEST_F(TfsInit,13_open_with_suffix)
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
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=".jpg";
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
     Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,14_open_dif_suffix_1)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT,".jpg");
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 	 
	 suffix=".txt";
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,15_open_dif_suffix_2)
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
	 count=100*(1<<10);
	 
	 
	 suffix=".jpg";
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,16_open_with_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr="10.232.4.3:3100";
	 char buf[100*(1<<10)];
	 
	 
	 int fd;
	 int64_t ret;
	 int Ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 
	 ret=tfsclient->read(fd, buf,count);
	 EXPECT_EQ(100*(1<<10),ret);
	 
     Ret=tfsclient->unlink(count, tfs_name, NULL,NULL);
	 ASSERT_NE(TFS_ERROR,Ret);
	 
	 tfsclient->close(fd);
}

TEST_F(TfsInit,17_open_with_dif_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr="10.232.4.2sds:3200";//另一台服务器
	 
	 
	 int fd;
	 int64_t ret;
	
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
}

TEST_F(TfsInit,18_open_with_not_exist_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr="10.232.4.89:3300";//不存在的服务器
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
}

TEST_F(TfsInit,19_open_with_wrong_nsaddr)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	 
	 int64_t count;
	 int32_t flags;
	 char*suffix;
	 char*ns_addr="1sds.2ds.sdd.3:3300";
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a100K,T_DEFAULT);
	 EXPECT_EQ(100*(1<<10),ret);
	 count=100*(1<<10);
	  
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
}

TEST_F(TfsInit,20_open_two_times)
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
	 count=100*(1<<10);
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,21_open_with_key)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char*key=a100K;
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a5M,T_LARGE);
	 EXPECT_EQ(5*(1<<20),ret);
	
	 
	 suffix=NULL;
	 flags=T_DEFAULT;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags,key);
	 ASSERT_NE(EXIT_INVALIDFD_ERROR,fd);
	 tfsclient->close(fd);
}

TEST_F(TfsInit,22_open_wrong_key)
{
     char tfs_name[19];
	 int32_t tfs_name_len=19;
	
	 int32_t flags;
	 char*suffix;
	 char*ns_addr=NULL;
	 char*key="156468sdcjks";
	 
	 
	 int fd;
	 int64_t ret;
	 
	 ret=tfsclient->save_file(tfs_name,tfs_name_len,a1Gjpg,T_LARGE);
	 EXPECT_EQ(1<<30,ret);

	 
	 
	 suffix=NULL;
	 flags=T_LARGE;
	 fd=tfsclient->open(tfs_name,suffix,ns_addr,flags,key);
	 ASSERT_EQ(EXIT_INVALIDFD_ERROR,fd);
	 tfsclient->close(fd);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}






































