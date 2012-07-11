#include <stdio.h>
#include <time.h>
#include <string>
#include <string.h>
#include "gtest/gtest.h"
#include "tfs_rc_client_api.h"


using namespace std;
using namespace tfs::client;

class TestClusterRetry: public testing::Test
{
public:

  TestClusterRetry()
  {
     rc_ip = "10.232.4.42:3300";
     app_key = "tfscom";
  }

  ~TestClusterRetry()
  {

  }

public:
  string rc_ip;
  string app_key;
};

TEST_F(TestClusterRetry, file_interface)
{
  int ret = 0;
  int64_t saved_size = -1;
  int64_t fetched_size = -1;
  int64_t uid = 100;
  const char* local_file = "test_rc_save_fetch.cpp";
  const char* tfs_file_prefix = "/test/test_rc_save_fetch.cpp.";

  RcClient rc_client;
	ret = rc_client.initialize(rc_ip.c_str(), app_key.c_str());
  ASSERT_EQ(0, ret);

  // generate a different name
  char tfs_file_name[1024];
  sprintf(tfs_file_name, "%s%d%u", tfs_file_prefix, 1, (unsigned)time(NULL));
  saved_size = rc_client.save_file(uid, local_file, tfs_file_name);
  ASSERT_LT(0, saved_size);
  fetched_size = rc_client.fetch_file(rc_client.get_app_id(),
      uid, "local_file", tfs_file_name);
  ASSERT_EQ(saved_size, fetched_size);
}

TEST_F(TestClusterRetry, buf_interface)
{
  int ret = 0;
  int64_t saved_size = -1;
  int64_t fetched_size = -1;
  int64_t uid = 100;
  const char* tfs_file_prefix = "/test/test_rc_save_fetch.cpp.";
  char buf[128] = "this is for test buffer interface";

  RcClient rc_client;
	ret = rc_client.initialize(rc_ip.c_str(), app_key.c_str());
  ASSERT_EQ(0, ret);

  // generate a different name
  char tfs_file_name[1024];
  sprintf(tfs_file_name, "%s%d%u", tfs_file_prefix, 2, (unsigned)time(NULL));
  saved_size = rc_client.save_buf(uid, buf, strlen(buf), tfs_file_name);
  ASSERT_LT(0, saved_size);
  fetched_size = rc_client.fetch_buf(rc_client.get_app_id(),
      uid, buf, 0, sizeof(buf), tfs_file_name);
  ASSERT_EQ(saved_size, fetched_size);
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
