/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: adminserver.cpp 1002 2011-11-03 03:07:41Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   nayan<nayan@taobao.com>
 *      - modify 2009-03-27
 *
 */
#include "adminserver.h"

#include "common/directory_op.h"
#include "common/status_message.h"
#include "common/config_item.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::adminserver;
namespace
{
  template<typename T> void delete_map(T& m)
  {
    for (typename T::iterator it = m.begin(); it != m.end(); it++)
    {
      tbsys::gDelete(it->second);
    }
    m.clear();
  }
}

int main(int argc, char* argv[])
{
  tfs::adminserver::AdminServer service;
  return service.main(argc, argv);
}


////////////////////////////////
namespace tfs
{
  namespace adminserver
  {
    // therer is only one AdminServer instance
    AdminServer::AdminServer() :
      stop_(false), running_(false),
      check_interval_(0), check_count_(0), warn_dead_count_(0)
    {
    }

    AdminServer::~AdminServer()
    {
    }

    int AdminServer::initialize(int argc, char* argv[])
    {
      UNUSED(argc);
      UNUSED(argv);
      int ret = TFS_SUCCESS;
      reload_config();

      ret = start_monitor();
      return ret;
    }

    int AdminServer::destroy_service()
    {
      stop_monitor();
      return TFS_SUCCESS;
    }


    void AdminServer::destruct()
    {
      delete_map(monitor_param_);
      delete_map(monitor_status_);
    }



    void AdminServer::set_ds_list(const char* index_range, vector<string>& ds_index)
    {
      if (index_range != NULL)
      {
        char buf[MAX_PATH_LENGTH + 1];
        buf[MAX_PATH_LENGTH] = '\0';
        strncpy(buf, index_range, MAX_PATH_LENGTH);
        char* index = NULL;
        char* buf_p = buf;

        while ((index = strsep(&buf_p, ", ")) != NULL)
        {
          if (strlen(index) != 0)
          {
            ds_index.push_back(index);
          }
        }
      }
    }

    void AdminServer::modify_conf(const string& index, const int32_t type)
    {
      char cmd[MAX_PATH_LENGTH];
      // use sed directly,
      if (1 == type)            // insert, strip trailing ", "
      {
        snprintf(cmd, MAX_PATH_LENGTH, "sed -i 's/\\(%s.*[^ ,]\\)[, ]*$/\\1,%s/g' %s",
                 CONF_DS_INDEX_LIST, index.c_str(), config_file_.c_str());
      }
      else                      // delete
      {
        snprintf(cmd, MAX_PATH_LENGTH, "sed -i 's/\\(%s.*[, ]*\\)\\b%s\\b[, ]*\\(.*$\\)/\\1\\2/g' %s",
                 CONF_DS_INDEX_LIST, index.c_str(), config_file_.c_str());
      }

      TBSYS_LOG(INFO, "cmd %s", cmd);
      int ret = system(cmd);
      TBSYS_LOG(INFO, "%s ds index %s %s", (1 == type) ? "add" : "delete", index.c_str(), (-1 == ret) ? "fail" : "success");
    }

    int AdminServer::get_param(const string& index)
    {
      int ret = TFS_SUCCESS;
      MonitorParam* param = new MonitorParam();

      if (index.size())
      {
        param->index_ = index;
        param->active_ = 1;
        param->fkill_waittime_ = TBSYS_CONFIG.getInt(CONF_SN_ADMINSERVER, CONF_DS_FKILL_WAITTIME);
        param->adr_.ip_ = Func::get_addr("127.0.0.1"); // just monitor local stuff
        param->script_ = TBSYS_CONFIG.getString(CONF_SN_ADMINSERVER, CONF_DS_SCRIPT, "");
        param->script_ += " -i " + index;
        param->description_ = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_MOUNT_POINT_NAME, "");
        param->description_ = FileSystemParameter::get_real_mount_name(param->description_, index);
        param->adr_.port_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_PORT);
        param->adr_.port_ = DataServerParameter::get_real_ds_port(param->adr_.port_, index);

        param->lock_file_ = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_LOCK_FILE, "./");
        param->lock_file_ = DataServerParameter::get_real_file_name(param->lock_file_, index, "pid");
        TBSYS_LOG(INFO, "load dataserver %s, desc : %s, lock_file : %s, port : %d, script : %s, waittime: %d\n",
            index.c_str(), param->description_.c_str(), param->lock_file_.c_str(), param->adr_.port_, param->script_.c_str(),
            param->fkill_waittime_);
      }

      monitor_param_.insert(MSTR_PARA::value_type(index, param));
      TBSYS_LOG(DEBUG, "get ds  index %s paramter", index.c_str());

      return ret;
    }

    void AdminServer::reload_config()
    {
      check_interval_ = TBSYS_CONFIG.getInt(CONF_SN_ADMINSERVER, CONF_CHECK_INTERVAL, 5);
      check_count_ = TBSYS_CONFIG.getInt(CONF_SN_ADMINSERVER, CONF_CHECK_COUNT, 5);
      warn_dead_count_ = TBSYS_CONFIG.getInt(CONF_SN_ADMINSERVER, CONF_WARN_DEAD_COUNT, 5);
      return;
    }

    void AdminServer::add_index(const string& index, const bool add_conf)
    {
      TBSYS_LOG(DEBUG, "add ds %s to monitor", index.c_str());
      // paramter
      get_param(index);

      // status
      MSTR_STAT_ITER it;
      if ((it = monitor_status_.find(index)) == monitor_status_.end())
      {
        monitor_status_.insert(MSTR_STAT::value_type(index, new MonitorStatus(index)));
      }
      // conf
      if (add_conf)
        modify_conf(index, 1);
    }

    void AdminServer::clear_index(const string& index, const bool del_conf)
    {
      // paramter
      tbsys::gDelete(monitor_param_[index]);
      monitor_param_.erase(index);
      // conf
      if (del_conf)
        modify_conf(index, 0);
    }

    int AdminServer::kill_process(MonitorStatus* status, const int32_t wait_time, const bool clear)
    {
      if (NULL == status) return TFS_ERROR;

      if (status->pid_ != 0)
      {
        TBSYS_LOG(WARN, "close, pid: %u", status->pid_);
        kill(status->pid_, SIGTERM);

        int32_t retry = wait_time * 10;
        // close it
        while (kill(status->pid_, 0) == 0 && retry > 0)
        {
          retry--;
          usleep(100000);
          if (stop_)
            break;
        }

        if (retry <= 0) // still not closed
        {
          kill(status->pid_, SIGKILL); // force to close
          TBSYS_LOG(WARN, "force to close, pid: %u", status->pid_);
        }
      }

      status->pid_ = 0;
      status->failure_ = 0;
      status->dead_time_ = time(NULL);
      status->start_time_ = 0;
      if (clear)
      {
        status->dead_count_ = 0;
      }
      else
      {
        status->dead_count_++;
      }
      return TFS_SUCCESS;
    }


    int AdminServer::stop_monitor()
    {
      stop_ = true;
      return TFS_SUCCESS;
    }

    int AdminServer::start_monitor()
    {
      // wait for last monitor stop, there is always only one monitor running...
      while (running_)
      {
        usleep(10000);         // sleep 10 ms
      }

      int ret = TFS_SUCCESS;

      // clean old parameters and status, reread config file to construct parameter
      destruct();

      {
        //BaseService::reload();
        TBSYS_CONFIG.load(config_file_.c_str());

        const char *index_range = TBSYS_CONFIG.getString(CONF_SN_ADMINSERVER, CONF_DS_INDEX_LIST, NULL);
        if (NULL == index_range)
        {
          TBSYS_LOG(ERROR, "ds index list not found in config file %s .", config_file_.c_str());
          return TFS_ERROR;
        }
        vector<string> ds_index;
        set_ds_list(index_range, ds_index);
        int32_t size = ds_index.size();

        for (int32_t i = 0; i < size; ++i)
        {
          add_index(ds_index[i]);
        }
      }

      pthread_t tid;
      pthread_create(&tid, NULL, AdminServer::do_monitor, this);

      if (0 == tid)
      {
        TBSYS_LOG(ERROR, "start monitor fail");
        ret = TFS_ERROR;
      }
      // detach thread, return now for handle packet
      pthread_detach(tid);

      return ret;
    }

    void* AdminServer::do_monitor(void* args)
    {
      reinterpret_cast<AdminServer*>(args)->run_monitor();
      return NULL;
    }

    int AdminServer::run_monitor()
    {
      if (0 == monitor_param_.size())
      {
        TBSYS_LOG(ERROR, "no monitor index error");
        return TFS_ERROR;
      }

      // now is running, there is always only one monitor running...
      running_ = true;
      stop_ = false;
      TBSYS_LOG(WARN, "== monitor normally start tid: %lu ==", pthread_self());

      ping_nameserver(TFS_SUCCESS); // wait for ns
      if (stop_)
        return TFS_SUCCESS;

      MonitorParam* m_param = NULL;
      MonitorStatus* m_status = NULL;


      while (!stop_)
      {
        // check process;
        // get pramameter's size every time
        for (MSTR_PARA_ITER it = monitor_param_.begin(); it != monitor_param_.end(); ++it)
        {
          m_param = it->second;
          m_status = monitor_status_[it->first];

          if (!m_param->active_) // ask for stop, not active, kill
          {
            TBSYS_LOG(DEBUG, "ask for stop index %s", it->first.c_str());
            // kill
            kill_process(m_status, m_param->fkill_waittime_, true); // ask for stop, clear dead count
            clear_index(const_cast<string&>(it->first));
            break;
          }

          if ( 0 == m_status->pid_ || kill(m_status->pid_, 0) != 0) // process not run
          {
            TBSYS_LOG(DEBUG, "lock file is %s", m_param->lock_file_.c_str());
            if ((m_status->pid_ = tbsys::CProcess::existPid(m_param->lock_file_.c_str())) == 0)
            {
              TBSYS_LOG(INFO, "start %s", m_param->script_.c_str());
              if (system(m_param->script_.c_str()) == -1)
              {
                TBSYS_LOG(ERROR, "start %s fail.", m_param->script_.c_str());
              }
              else
              {
                m_status->restarting_ = 1;
                m_status->failure_ = 0;
                m_status->start_time_ = time(NULL);
              }
            }
          }
          else if (0 == m_status->start_time_) // startup, ds/ns is already running
          {
            m_status->start_time_ = time(NULL);
          }

          if (stop_)
            break;

          // process exist. ping
          if (m_status->pid_ > 0)
          {
            uint64_t ip_address = *(uint64_t*) &(m_param->adr_);
            int32_t status = ping(ip_address);
            if (status == TFS_ERROR)
            {
              if (m_status->restarting_ == 0)
              {
                TBSYS_LOG(ERROR, "ping %s fail, ip: %s, failure: %d", m_param->description_.c_str(),
                    tbsys::CNetUtil::addrToString(ip_address).c_str(), m_status->failure_);
                m_status->failure_++;
              }
              else // do not ad failure num if the process is restarting status
              {
                TBSYS_LOG(ERROR, "restarting, desc : %s ip: %s", m_param->description_.c_str(),
                    tbsys::CNetUtil::addrToString(ip_address).c_str());
                m_status->failure_ = 0;
              }
            }
            else
            {
              TBSYS_LOG(DEBUG, "ping %s success, ip: %s", m_param->description_.c_str(),
                  tbsys::CNetUtil::addrToString(ip_address).c_str());
              m_status->failure_ = 0;
              m_status->restarting_ = 0;
            }

            // kill
            if (m_status->failure_ > check_count_)
            {
              kill_process(m_status, m_param->fkill_waittime_);
            }
          }
        }
        // sleep
        sleep(check_interval_);
      }

      // now not running
      TBSYS_LOG(WARN, "== monitor normally exit tid: %lu ==", pthread_self());
      running_ = false;
      return TFS_SUCCESS;
    }

    int AdminServer::ping(const uint64_t ip)
    {
      int32_t ret = TFS_ERROR;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL != client)
      {
        StatusMessage ping_msg(STATUS_MESSAGE_PING);
        tbnet::Packet* message = NULL;
        if(TFS_SUCCESS == send_msg_to_server(ip, client, &ping_msg, message))
        {
          if (message->getPCode() == STATUS_MESSAGE &&
              dynamic_cast<StatusMessage*> (message)->get_status()
              == STATUS_MESSAGE_PING)
          {
            ret = TFS_SUCCESS;
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      return ret;
    }

    int AdminServer::ping_nameserver(const int estatus)
    {
      uint64_t nsip;
      IpAddr* adr = reinterpret_cast<IpAddr*> (&nsip);
      string ip_addr = TBSYS_CONFIG.getString(CONF_SN_NAMESERVER, CONF_IP_ADDR, "");
      uint32_t ip = Func::get_addr(ip_addr.c_str());
      if (ip > 0)
      {
        adr->ip_ = ip;
        adr->port_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_PORT, 0);
      }

      // reuse client
      int32_t count = 0;
      while (!stop_)
      {
        if (ping(nsip) == estatus)
          break;
        sleep(check_interval_);
        ++count;
      }

      return count;
    }

    tbnet::IPacketHandler::HPRetCode AdminServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      if (NULL == connection || NULL == packet)
      {
        TBSYS_LOG(ERROR, "connection or packet ptr NULL");
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      if (!packet->isRegularPacket())
      {

        TBSYS_LOG(ERROR, "ControlPacket, cmd: %d", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }
      if (packet->getPCode() != ADMIN_CMD_MESSAGE)
      {
        TBSYS_LOG(ERROR, "Unknow packet: %d", packet->getPCode());
        packet->free();
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      BasePacket* bp = dynamic_cast<BasePacket*>(packet);
      bp->set_connection(connection);
      bp->set_direction(DIRECTION_RECEIVE);
      if (!main_workers_.push(bp, work_queue_size_))
      {
        TBSYS_LOG(ERROR, "main_workers is full ignore a packet pcode is %d", packet->getPCode());
        packet->free();
      }
      return tbnet::IPacketHandler::KEEP_CHANNEL;
    }

    bool AdminServer::handlePacketQueue(tbnet::Packet* packet, void* args)
    {
      UNUSED(args);
      AdminCmdMessage* message = dynamic_cast<AdminCmdMessage*>(packet); // only AdminCmdMessage enqueue
      if (NULL == message)
      {
        TBSYS_LOG(ERROR, "process packet NULL can not convert to message");
        return true;
      }

      int32_t cmd_type = message->get_cmd_type();
      // only start_monitor and kill_adminserver can handle when monitor is not running
      if (!running_ && cmd_type != ADMIN_CMD_START_MONITOR && cmd_type != ADMIN_CMD_KILL_ADMINSERVER)
      {
        message->reply(new StatusMessage(TFS_ERROR, "monitor is not running"));
        return TFS_SUCCESS;
      }

      int ret = TFS_SUCCESS;
      // check cmd type
      switch (cmd_type)
      {
        case ADMIN_CMD_GET_STATUS:
          ret = cmd_reply_status(message);
          break;
        case ADMIN_CMD_CHECK:
          ret = cmd_check(message);
          break;
        case ADMIN_CMD_START_MONITOR:
          ret = cmd_start_monitor(message);
          break;
        case ADMIN_CMD_RESTART_MONITOR:
          ret = cmd_restart_monitor(message);
          break;
        case ADMIN_CMD_STOP_MONITOR:
          ret = cmd_stop_monitor(message);
          break;
        case ADMIN_CMD_START_INDEX:
          ret = cmd_start_monitor_index(message);
          break;
        case ADMIN_CMD_STOP_INDEX:
          ret = cmd_stop_monitor_index(message);
          break;
        case ADMIN_CMD_KILL_ADMINSERVER:
          ret = cmd_exit(message);
          break;
        default:
          ret = TFS_ERROR;
      }

      if (ret != TFS_SUCCESS)
      {
        message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), 12,
            "execute message fail, type: %d. ret: %d\n", message->getPCode(), ret);
      }
      return true;
    }

    int AdminServer::cmd_check(AdminCmdMessage* message)
    {
      AdminCmdMessage* resp_msg = new AdminCmdMessage(ADMIN_CMD_RESP);
      for (MSTR_STAT_ITER it = monitor_status_.begin(); it != monitor_status_.end(); it++)
      {
        if (0 == it->second->pid_ || it->second->dead_count_ > warn_dead_count_) // add to confile later
        {
          resp_msg->set_status(it->second);
        }
      }

      message->reply(resp_msg);
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_reply_status(AdminCmdMessage* message)
    {
      AdminCmdMessage* resp_msg = new AdminCmdMessage(ADMIN_CMD_RESP);
      for (MSTR_STAT_ITER it = monitor_status_.begin(); it != monitor_status_.end(); it++)
      {
        resp_msg->set_status(it->second);
      }

      message->reply(resp_msg);
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_start_monitor(message::AdminCmdMessage* message)
    {
      const char* err_msg;
      int ret = TFS_ERROR;

      if (running_ && !stop_)     // is running and not will be stoped
      {
        err_msg = "monitor is already running";
      }
      else
      {
        ret = start_monitor();
        err_msg = (TFS_SUCCESS == ret) ? "start monitor SUCCESS" : "start monitor FAIL";
      }

      message->reply(new StatusMessage(ret, const_cast<char*>(err_msg)));
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_restart_monitor(message::AdminCmdMessage* message)
    {
      stop_monitor();
      return cmd_start_monitor(message);
    }

    int AdminServer::cmd_stop_monitor(message::AdminCmdMessage* message)
    {
      stop_monitor();
      message->reply(new StatusMessage(TFS_SUCCESS, const_cast<char*>("stop monitor success")));
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_start_monitor_index(AdminCmdMessage* message)
    {
      VSTRING* index = message->get_index();
      string success = "", fail = "";
      MSTR_PARA_ITER it;

      for (size_t i = 0; i < index->size(); i++)
      {
        string& cur_index = (*index)[i];
        if ((it = monitor_param_.find(cur_index)) != monitor_param_.end()) // found
        {
          if (it->second->active_)
          {
            fail += cur_index + " ";
          }
          else
          {
            it->second->active_ = 1;                    // mark dead
            success += cur_index + " ";
          }
        }
        else
        {
          add_index(cur_index, true);
          success += cur_index + " ";
        }
      }

      string err_msg = success.empty() ? "" : "start index " + success + "success\n";
      err_msg += fail.empty() ? "" : "index " + fail + "already running\n";
      message->reply(new StatusMessage(fail.empty() ? TFS_SUCCESS : TFS_ERROR,
            const_cast<char*>(err_msg.c_str())));
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_stop_monitor_index(AdminCmdMessage* message)
    {
      VSTRING* index = message->get_index();
      string success = "", fail = "";
      MSTR_PARA_ITER it;

      for (size_t i = 0; i < index->size(); i++)
      {
        string& cur_index = (*index)[i];
        if ((it = monitor_param_.find(cur_index)) != monitor_param_.end()) // found
        {
          if (!it->second->active_)
          {
            fail += cur_index + " ";
          }
          else
          {
            it->second->active_ = 0;
            success += cur_index + " ";
          }
        }
        else
        {
          fail += cur_index + " ";
        }
      }

      string err_msg = success.empty() ? "" : "stop index " + success + "success\n";
      err_msg += fail.empty() ? "" : "index " + fail + "not running\n";
      message->reply(new StatusMessage(fail.empty() ? TFS_SUCCESS : TFS_ERROR,
            const_cast<char*>(err_msg.c_str())));
      return TFS_SUCCESS;
    }

    int AdminServer::cmd_exit(message::AdminCmdMessage* message)
    {
      message->reply(new StatusMessage(TFS_SUCCESS, "adminserver exit"));
      stop();
      return TFS_SUCCESS;
    }

  }
}
////////////////////////////////
