
/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: integration.cpp 2069 2011-02-15 06:35:25Z duanfei $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <queue>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <tbsys.h>
#include <Memory.hpp>

#include "common/config.h"
#include "common/config_item.h"
#include "common/directory_op.h"
#include "common/error_msg.h"
#include "common/parameter.h"
#include "integration_instance.h"
using namespace tfs::integration;
using namespace tfs::common;
using namespace tbnet;
using namespace tbsys;
using namespace tbutil;

namespace tfs
{

namespace integration 
{
class IntegrationService 
{
public:
  IntegrationService();
  virtual ~IntegrationService();
  virtual int run(int32_t min_size, int32_t max_size, const std::string& config);
  virtual bool destroy();
  virtual int wait_for_shut_down();
private:
  IntegrationServiceInstance instance_;
};

IntegrationService::IntegrationService():
  instance_()
{

}

IntegrationService::~IntegrationService()
{

}

int IntegrationService::run(int32_t min_size, int32_t max_size,const std::string& conf)
{
 return instance_.initialize(min_size, max_size, conf);
}

bool IntegrationService::destroy()
{
  TBSYS_LOG(INFO, "%s", "integration service stoped");
  instance_.destroy();
  return true;
}

int IntegrationService::wait_for_shut_down()
{
  return instance_.wait_for_shut_down();
}

}
}

void helper()
{
  std::string options=
    "Options:\n"
    "-f FILE            Configure files...\n"
    "-d                 Run as a daemon...\n"
    "-h                 Show this message...\n"
    "-v                 Show porgram version...\n";
  fprintf(stderr,"Usage:\n%s" ,options.c_str());
}

static tfs::integration::IntegrationService* gintegration_service = NULL;

static void interruptcallback(int signal)
{
  TBSYS_LOG(INFO, "application signal[%d]", signal );
  switch( signal )
  {
     case SIGTERM:
     case SIGINT:
     default:
      if (gintegration_service != NULL)
        gintegration_service->destroy();
     break;
  }
}

int main(int argc, char* argv[])
{
  std::string conf;
  bool daemon = false;
  bool help   = false;
  bool version= false;
  int32_t index = 0;
  int32_t min_size = 1;
  int32_t max_size = 1;
  int32_t iret  = TFS_SUCCESS;
  while ((index = getopt(argc, argv, "f:m:n:dvh")) != EOF)
  {
    switch (index)
    {
    case 'f':
      conf = optarg;
    break;
    case 'm':
      min_size = atoi(optarg);
    break;
    case 'n':
      max_size = atoi(optarg);
    break;
    case 'd':
      daemon = true;
      break;
    case 'v':
      version = true;
      break;
    case 'h':
    default:
      help = true;
      break;
    }
  }

  help = (conf.empty());

  if (help)
  {
    helper();
    return TFS_ERROR;
  }

  CONFIG.load(conf.c_str());

  iret = TFS_SUCCESS;
  if ((iret = SysParam::instance().load(conf.c_str())) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "load config file(%s) fail, must be exit...\n", conf.c_str());
    return iret;
  }
  std::string work_dir  = CONFIG.get_string_value(CONFIG_PUBLIC, CONF_WORK_DIR);
  std::string log_dir= work_dir + "/logs";
  std::string log_file = log_dir + "/integration.log";
  TBSYS_LOGGER.setLogLevel(CONFIG.get_string_value(CONFIG_PUBLIC, CONF_LOG_LEVEL, "debug"));       
  TBSYS_LOGGER.setMaxFileSize(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_SIZE, 0x40000000));
  TBSYS_LOGGER.setMaxFileIndex(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_NUM, 0x0A));
  if (access(log_file.c_str(), R_OK) == 0)                                                                 
  {
    TBSYS_LOGGER.rotateLog(log_file.c_str());
  }
  else if (!DirectoryOp::create_full_path(log_dir.c_str()))
  {
    fprintf(stderr, "create file(%s)'s directory failed\n",log_dir.c_str()); 
    return EXIT_GENERAL_ERROR;
  }

  if (!DirectoryOp::create_full_path(work_dir.c_str()))
  {
    fprintf(stderr, "create file(%s)'s directory failed\n",work_dir.c_str()); 
    return EXIT_GENERAL_ERROR;
  }

  std::string pid_file = work_dir +"/logs/integration.pid";
  int32_t pid = 0;
  if ((pid = tbsys::CProcess::existPid(pid_file.c_str())))
  {
    fprintf(stderr, "integration is running\n");
    return EXIT_SYSTEM_ERROR;
  }

  pid = 0;
  if (daemon)
  {
    pid = tbsys::CProcess::startDaemon(pid_file.c_str(), log_file.c_str());
  }

  if (pid == 0)
  {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, SIG_IGN);                                                                   
    signal(SIGTERM, SIG_IGN);
    signal(SIGUSR1, SIG_IGN);
    try
    {
      gintegration_service = new IntegrationService();
      iret = gintegration_service->run(min_size, max_size, conf);
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s", "integration service initialize fail, must be exit...");
        gintegration_service->destroy();
        gintegration_service->wait_for_shut_down();
        return iret;
      }
      signal(SIGHUP, interruptcallback);
      signal(SIGINT, interruptcallback);                                                                   
      signal(SIGTERM, interruptcallback);
      signal(SIGUSR1, interruptcallback);
      gintegration_service->wait_for_shut_down();
      unlink(pid_file.c_str());
    }
    catch(std::exception& ex)
    {
      TBSYS_LOG(ERROR, "catch exception(%s),must be exit...", ex.what());
      TBSYS_LOG(ERROR, "======================catch exception(%s),must be exit...", ex.what());
      gintegration_service->destroy();
    }
    catch(...)
    {
      TBSYS_LOG(ERROR, "%s", "catch exception(unknow),must be exit...");
      TBSYS_LOG(ERROR, "%s", "=====================catch exception(unknow),must be exit...");
      gintegration_service->destroy();
    }
    tbsys::gDelete(gintegration_service);
    return iret;
  }
  return 0;
}
