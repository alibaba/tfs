#include"tfs_client_init.h"

TEST_F(TfsInit,01_set_default_server_right)
{    
    int ret;
	
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->set_default_server(ns_addr);
	EXPECT_EQ(TFS_SUCCESS,ret);
}

TEST_F(TfsInit,02_set_default_server_null_nsaddr)
{    
    int ret;
	
    const char* ns_addr = NULL; 
	ret=tfsclient->set_default_server(ns_addr);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,03_set_default_server_empty_nsaddr)
{    
    int ret;
	
    const char* ns_addr = ""; 
	ret=tfsclient->set_default_server(ns_addr);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,04_set_default_server_wrong_nsaddr)
{    
    int ret;
	
    const char* ns_addr = "10.uyt.278.2:5320"; 
	ret=tfsclient->set_default_server(ns_addr);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,05_set_default_server_wrong_nsaddr_1)
{    
    int ret;
	
    const char* ns_addr = "10.uyt.278.2:5320"; 
	ret=tfsclient->set_default_server(ns_addr);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,06_set_default_server_wrong_nsaddr_2)
{    
    int ret;
	
    const char* ns_addr = "4164651gjhgjk"; 
	ret=tfsclient->set_default_server(ns_addr);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,07_set_default_server_not_usable)
{    
    int ret;
	
    const char* ns_addr = "10.232.4.3:5100"; 
	ret=tfsclient->set_default_server(ns_addr);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,08_iset_default_server_wrong_cachetime_1)
{    
    int32_t  cache_time; 
    int ret;
	         
    cache_time=-1;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->set_default_server(ns_addr,cache_time);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,09_set_default_server_wrong_cachetime_2)
{    
    int32_t  cache_time; 
    int ret;
	         
    cache_time=0;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->set_default_server(ns_addr,cache_time);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,10_set_default_server_small_cachetime)
{    
    int32_t  cache_time; 
    int ret;
	         
    cache_time=5;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->set_default_server(ns_addr,cache_time);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,11_set_default_server_large_cachetime)
{    
    int32_t  cache_time; 
    int ret;
	         
    cache_time=2147483647;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->set_default_server(ns_addr,cache_time);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,12_set_default_server_wrong_cacheitems_1)
{    
    int32_t  cache_time;
    int32_t  cache_items;	
    int ret;
	         
    cache_time=1800;
	cache_items=-1;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->set_default_server(ns_addr,cache_time,cache_items);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,13_set_default_server_wrong_cacheitems_2)
{    
    int32_t  cache_time; 
	int32_t  cache_items;
    int ret;
	         
    cache_time=1800;
	cache_items=0;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->set_default_server(ns_addr,cache_time,cache_items);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,14_set_default_server_small_cacheitems)
{    
    int32_t  cache_time; 
	int32_t  cache_items;
    int ret;
	         
    cache_time=1800;
	cache_items=5;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->set_default_server(ns_addr,cache_time,cache_items);
	EXPECT_EQ(TFS_ERROR,ret);
}

TEST_F(TfsInit,15_set_default_server_large_cacheitems)
{    
    int32_t  cache_time; 
	int32_t  cache_items;
    int ret;
	         
    cache_time=1800;
	cache_items=2147483647;
    const char* ns_addr = "10.232.4.3:3100"; 
	ret=tfsclient->set_default_server(ns_addr,cache_time,cache_items);
	EXPECT_EQ(TFS_ERROR,ret);
}

int main(int argc,char**argv)
{
     testing::InitGoogleTest(&argc,argv);
	 return RUN_ALL_TESTS();
}


























