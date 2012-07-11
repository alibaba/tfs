/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: transfer_block.cpp 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include <sys/types.h>
#include <pthread.h>

#include <vector>

#include "tbsys.h"
#include "Memory.hpp"
#include "Handle.h"
#include "TbThread.h"

#include "common/func.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/base_packet_streamer.h"
#include "message/message_factory.h"
#include "common/client_manager.h"
#include "dataserver/visit_stat.h"
#include "new_client/tfs_client_api.h"
#include "block_console.h"

using namespace tbsys;
using namespace tfs::common;
using namespace tfs::client;
using namespace tfs::message;
using namespace tfs::dataserver;

static BasePacketStreamer g_packet_streamer_;
static MessageFactory g_packet_factory_;

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread();
    WorkThread(BlockConsole* blk_console, tfs::client::TfsSession* src_session, tfs::client::TfsSession* dest_session,
        const std::string& dest_ns_addr, const int64_t max_traffic) :
      blk_console_(blk_console), src_session_(src_session), dest_session_(dest_session),
      dest_ns_addr_(dest_ns_addr), traffic_(max_traffic * 1024), destroy_(false)
    {
    }

    virtual ~WorkThread()
    {
    }

    void wait_for_shut_down()
    {
      join();
    }

    void destroy()
    {
      destroy_ = true;
    }

    void reload_traffic(int flag)
    {
      switch (flag)
      {
        case -1:
          traffic_ = traffic_ / 2;
          break;
        case 1:
          traffic_ = traffic_ * 2;
          break;
        case 0:
          traffic_ = traffic_ * 3;
          break;
      }
      TBSYS_LOG(INFO, "reload traffic, flag: %d, traffic: %"PRI64_PREFIX"d", flag, traffic_);
    }

    virtual void run()
    {
      int ret = TFS_SUCCESS;
      while (!destroy_)
      {
        uint32_t block_id = 0;
        uint64_t ds_id = 0;
        TIMER_START();
        // get blockid and dsid
        if ((ret = blk_console_->get_transfer_param(block_id, ds_id)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get transfer param fail, ret: %d, thread id: %"PRI64_PREFIX"d", ret, id());
          destroy_ = true;
          break;
        }
        else
        {
          TBSYS_LOG(INFO, "get transfer param succ, blockid: %u, ds: %s, thread id: %"PRI64_PREFIX"d",
              block_id, tbsys::CNetUtil::addrToString(ds_id).c_str(), id());
          TranBlock tran_block(block_id, dest_ns_addr_, ds_id, traffic_, src_session_, dest_session_);
          if ((ret = tran_block.run()) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "tran block run fail, ret: %d, thread id: %"PRI64_PREFIX"d", ret, id());
          }
          blk_console_->finish_transfer_block(block_id, ret);

          TIMER_END();
          TBSYS_LOG(INFO, "get transfer param %s, ret: %d, cost time: %"PRI64_PREFIX"d, tran size: %d",
              TFS_SUCCESS == ret ? "succ" : "fail", ret,
              TIMER_DURATION(), tran_block.get_tran_size());
        }
      }
    }

  private:
    WorkThread(const WorkThread&);
    WorkThread& operator=(const WorkThread&);

  private:
    BlockConsole* blk_console_;
    tfs::client::TfsSession* src_session_;
    tfs::client::TfsSession* dest_session_;
    std::string dest_ns_addr_;

    volatile int64_t traffic_;
    volatile bool destroy_; //used for quit
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

static int32_t thread_count = 1;
static WorkThreadPtr* gworks = NULL;

static void interruptcallback(int signal)
{
  TBSYS_LOG(INFO, "application signal: %d", signal);
  bool destory_flag = false;
  int reload_flag = 0;
  switch (signal)
  {
    case SIGUSR1:
      reload_flag = 1;
      break;
    case SIGUSR2:
      reload_flag = -1;
      break;
    case SIGTERM:
    case SIGINT:
    default:
      destory_flag = true;
      break;
  }
  if (gworks != NULL)
  {
    for (int32_t i = 0; i < thread_count; ++i)
    {
      if (gworks[i] != 0)
      {
        if (destory_flag)
        {
          gworks[i]->destroy();
        }
        else if (0 != reload_flag)
        {
          gworks[i]->reload_traffic(reload_flag);
        }
      }
    }
  }
}

void helper()
{
  std::string options=
    "Options:\n"
    "-s                 src ns ip:port\n"
    "-n                 dest ns ip:port\n"
    "-f                 dest ds ip:port list\n"
    "-b                 transfer blocks list\n"
    "-w                 traffic threshold per thread\n"
    "-t                 thread count\n"
    "-l                 log file, defalut path is tranblk.log\n"
    "-p                 pid file, defalut path is tranblk.pid\n"
    "-d                 daemon\n"
    "-h                 help\n";
  fprintf(stderr, "Usage:\n%s", options.c_str());
}

int main(int argc, char* argv[])
{
  std::string src_ns_ip_port;
  std::string dest_ns_ip_port;
  std::string dest_ds_ip_file;
  std::string ts_input_blocks_file;
  std::string log_file = "./tranblk.log";
  std::string pid_file = "./tranblk.pid";
  int64_t traffic_threshold = 1024;
  bool daemon_flag = false;
  bool help = false;

  int32_t index = 0;
  while ((index = getopt(argc, argv, "s:n:f:b:w:t:l:p:dvh")) != EOF)
  {
    switch (index)
    {
      case 's':
        src_ns_ip_port = optarg;
        break;
      case 'n':
        dest_ns_ip_port = optarg;
        break;
      case 'f':
        dest_ds_ip_file = optarg;
        break;
      case 'b':
        ts_input_blocks_file = optarg;
        break;
      case 'w':
        traffic_threshold = strtoull(optarg, NULL, 10);
        break;
      case 't':
        thread_count = atoi(optarg);
        break;
      case 'l':
        log_file = optarg;
        break;
      case 'p':
        pid_file = optarg;
        break;
      case 'd':
        daemon_flag = true;
        break;
      case 'v':
        break;
      case 'h':
      default:
        help = true;
        break;
    }
  }

  help = src_ns_ip_port.empty() || dest_ns_ip_port.empty() || dest_ds_ip_file.empty()
    || help || ts_input_blocks_file.empty();
  if (help)
  {
    helper();
    return TFS_ERROR;
  }

  // init log
  if (log_file.size() != 0 && access(log_file.c_str(), R_OK) == 0)
  {
    TBSYS_LOGGER.rotateLog(log_file.c_str());
  }
  TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);

  int pid = 0;
  if (daemon_flag)
  {
    pid = Func::start_daemon(pid_file.c_str(), log_file.c_str());
  }

  //child
  if (0 == pid)
  {
    // init block console
    BlockConsole* blk_console = new BlockConsole();
    int ret = blk_console->initialize(ts_input_blocks_file, dest_ds_ip_file);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "init block console failed, input block file: %s, ret: %d",
          ts_input_blocks_file.c_str(), ret);
    }
    else
    {
      // init src tfs session
      g_packet_streamer_.set_packet_factory(&g_packet_factory_);
      if (TFS_SUCCESS != (ret = NewClientManager::get_instance().initialize(&g_packet_factory_, &g_packet_streamer_)))
      {
        TBSYS_LOG(ERROR, "initialize NewClientManager fail, must exit, ret: %d", ret);
      }
      else
      {
        TfsSession* src_session = new TfsSession(src_ns_ip_port,
            tfs::common::DEFAULT_BLOCK_CACHE_TIME, tfs::common::DEFAULT_BLOCK_CACHE_ITEMS);
        TfsSession* dest_session = new TfsSession(dest_ns_ip_port,
            tfs::common::DEFAULT_BLOCK_CACHE_TIME, tfs::common::DEFAULT_BLOCK_CACHE_ITEMS);
        if ((ret = src_session->initialize()) != TFS_SUCCESS)
        {
          tbsys::gDelete(src_session);
          TBSYS_LOG(ERROR, "init src session failed, src ns ip: %s, ret: %d",
              src_ns_ip_port.c_str(), ret);
        }
        else if ((ret = dest_session->initialize()) != TFS_SUCCESS)
        {
          tbsys::gDelete(dest_session);
          TBSYS_LOG(ERROR, "init dest session failed, dest ns ip: %s, ret: %d",
              dest_ns_ip_port.c_str(), ret);
        }
        else
        {
          gworks = new WorkThreadPtr[thread_count];
          int i = 0;
          for ( ; i < thread_count; ++i)
          {
            gworks[i] = new WorkThread(blk_console, src_session, dest_session, dest_ns_ip_port, traffic_threshold);
          }

          for (i = 0; i < thread_count; ++i)
          {
            gworks[i]->start();
          }

          signal(SIGPIPE, SIG_IGN);
          signal(SIGHUP, SIG_IGN);
          signal(SIGINT, interruptcallback);
          signal(SIGTERM, interruptcallback);
          signal(SIGUSR1, interruptcallback);
          signal(SIGUSR2, interruptcallback);

          for (i = 0; i < thread_count; ++i)
          {
            gworks[i]->wait_for_shut_down();
          }
          tbsys::gDeleteA(gworks);
          tbsys::gDelete(src_session);
          tbsys::gDelete(dest_session);
        }
      }
    }
    tbsys::gDelete(blk_console);

    return ret;
  }

  return TFS_SUCCESS;
}
