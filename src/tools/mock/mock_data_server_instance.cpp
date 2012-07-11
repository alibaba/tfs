/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mock_data_server_instance.cpp 451 2011-06-08 09:47:31Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */

#include "mock_data_server_instance.h"

#include <tbsys.h>

#include "common/status_message.h"
#include "common/config_item.h"
#include "common/client_manager.h"

using namespace tbsys;
using namespace tfs::mock;
using namespace tfs::message;
using namespace tfs::common;
namespace tfs
{
  namespace mock
  {
    std::string get_ns_ip_addr()
    {
      return TBSYS_CONFIG.getString(CONF_SN_MOCK_DATASERVER, CONF_IP_ADDR, "");
    }
    static FileInfo gfile_info;
    static const int8_t BUF_LEN = 32;
    static char BUF[BUF_LEN] = {'1'};

    static int ns_async_callback(NewClient* client);

    MockDataService::MockDataService():
      MAX_WRITE_FILE_SIZE(0),
      block_count(0),
      block_start(0)
    {
      ns_ip_port_[0] = 0,
      ns_ip_port_[1] = 0,
      memset(&information_, 0, sizeof(information_));
      memset(&gfile_info, 0, sizeof(gfile_info));
      gfile_info.size_ = BUF_LEN;
      gfile_info.crc_  = 0;
      gfile_info.id_ = 0xff;
      gfile_info.crc_  = Func::crc(gfile_info.crc_, BUF, BUF_LEN);
      information_.total_capacity_ = 0;
      information_.use_capacity_ = 0;
    }

    MockDataService::~MockDataService()
    {

    }

    int MockDataService::parse_common_line_args(int argc, char* argv[], std::string& errmsg)
    {
      int32_t i = 0;
      while ((i = getopt(argc, argv, "i:c:t:s:")) != EOF)
      {
        switch (i)
        {
          case 'i':
            server_index_ = optarg;
            break;
          case 'c':
            information_.total_capacity_ = atoi(optarg);
            information_.total_capacity_ *= 1024 * 1024;
            break;
          case 't':
            block_count = atoi(optarg);
            block_count = std::max(0, block_count);
            break;
          case 's':
            block_start = atoi(optarg);
            block_start = std::max(0, block_start);
            break;
          default:
            break;
        }
      }
      int ret = server_index_.empty() && information_.total_capacity_ > 0 ? TFS_ERROR : TFS_SUCCESS;
      if (ret != TFS_SUCCESS)
      {
        errmsg = "use -i index -c capacity";
      }
      return ret;
    }

    int32_t MockDataService::get_listen_port() const
    {
      int32_t port = get_port() + atoi(server_index_.c_str());
      return port <= 1024 || port > 65535 ? -1 : port;
    }

    int32_t MockDataService::get_ns_port() const
    {
      int32_t port = TBSYS_CONFIG.getInt(CONF_SN_MOCK_DATASERVER, CONF_PORT, -1);
      return port <= 1024 || port > 65535 ? -1 : port;
    }

    #define CONF_NS_VIP "ns_vip"
    std::string MockDataService::get_ns_vip() const
    {
      return TBSYS_CONFIG.getString(CONF_SN_MOCK_DATASERVER, CONF_NS_VIP, "");
    }

    const char* MockDataService::get_log_file_path()
    {
      const char* log_file_path = NULL;
      const char* work_dir = get_work_dir();
      if (work_dir != NULL)
      {
        log_file_path_ = work_dir;
        log_file_path_ += "/logs/mock_dataserver_";
        log_file_path_ += server_index_;
        log_file_path_ += ".log";
        log_file_path = log_file_path_.c_str();
      }
      return log_file_path;
    }

    const char* MockDataService::get_pid_file_path()
    {
      const char* log_file_path = NULL;
      const char* work_dir = get_work_dir();
      if (work_dir != NULL)
      {
        log_file_path_ = work_dir;
        log_file_path_ += "/logs/mock_dataserver_";
        log_file_path_ += server_index_;
        log_file_path_ += ".pid";
        log_file_path = log_file_path_.c_str();
      }
      return log_file_path;
    }

    int MockDataService::keepalive(const uint64_t server)
    {
      const int32_t TIMEOUT_MS = 1500;
      information_.current_time_ = time(NULL);
      information_.current_load_ = Func::get_load_avg();
      information_.block_count_  = blocks_.size();
      SetDataserverMessage msg;
      msg.set_ds(&information_);
      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* result = NULL;
      int32_t ret = send_msg_to_server(server, client, &msg, result, TIMEOUT_MS);
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "%s", "message is null or ret(%d) !=  TFS_SUCCESS");
      }
      TBSYS_LOG(DEBUG, "keepalive ret: %d", ret);
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

    /** handle single packet */
    bool MockDataService::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      bool bret = BaseService::handlePacketQueue(packet, args);
      if (bret)
      {
        int32_t pcode = packet->getPCode();
        int32_t ret = LOCAL_PACKET == pcode ? TFS_ERROR : common::TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          TBSYS_LOG(DEBUG, "PCODE: %d", pcode);
          common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
          switch( pcode)
          {
            case CREATE_FILENAME_MESSAGE:
              ret = create_file_number(msg);
              break;
            case WRITE_DATA_MESSAGE:
              ret = write(msg);
              break;
            case READ_DATA_MESSAGE:
              ret = read(msg);
              break;
            case READ_DATA_MESSAGE_V2:
              ret = readv2(msg);
              break;
            case CLOSE_FILE_MESSAGE:
              ret = close(msg);
              break;
            case NEW_BLOCK_MESSAGE:
              ret = new_block(msg);
              break;
            case REMOVE_BLOCK_MESSAGE:
              ret = remove_block(msg);
              break;
            case FILE_INFO_MESSAGE:
              ret = get_file_info(msg);
              break;
            case REQ_CALL_DS_REPORT_BLOCK_MESSAGE:
              ret = report_block(msg);
              break;
            case WRITE_INFO_BATCH_MESSAGE:
              ret = batch_write_info(msg);
              break;
            case REPLICATE_BLOCK_MESSAGE:
              ret = replicate_block(msg);
              break;
            case COMPACT_BLOCK_MESSAGE:
              ret = compact_block(msg);
              break;
            default:
              ret = EXIT_UNKNOWN_MSGTYPE;
              TBSYS_LOG(ERROR, "unknown msg type: %d", pcode);
              break;
          }
          if (common::TFS_SUCCESS != ret)
          {
            msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
          }
        }
      }
      return bret;
    }

    int MockDataService::initialize(int argc, char* argv[])
    {
      UNUSED(argc);
      UNUSED(argv);
      int32_t ret = get_ns_port() > 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        std::vector <std::string> ips;
        const int32_t LEN = 256;
        char buf[LEN] = {'\0'};
        strncpy(buf, get_ns_ip_addr().c_str(), LEN);
        char* t = NULL, *s = buf;
        while (NULL != (t = strsep(&s, "|")))
        {
          ips.push_back(t);
        }
        int32_t index = 0;
        std::vector<std::string>::iterator iter = ips.begin();
        for (; iter != ips.end(); ++iter, ++index)
        {
          ns_ip_port_[index] = Func::str_to_addr((*iter).c_str(), get_ns_port());
        }

        ns_vip_ = Func::str_to_addr(get_ns_vip().c_str(), get_ns_port());

        IpAddr* adr = reinterpret_cast<IpAddr*>(&information_.id_);
        adr->ip_ = Func::get_local_addr(get_dev());
        adr->port_ = get_listen_port();

        for (int32_t i = block_start; i < block_count + block_start; i++)
        {
          BlockEntry entry;
          entry.info_.block_id_ = i;
          entry.info_.version_ = 1;
          insert_(entry);
        }

        int32_t heart_interval = TBSYS_CONFIG.getInt(CONF_SN_MOCK_DATASERVER,CONF_HEART_INTERVAL,2);
        KeepaliveTimerTaskPtr task = new KeepaliveTimerTask(*this, ns_ip_port_[0]);
        get_timer()->scheduleRepeated(task, tbutil::Time::seconds(heart_interval));
        KeepaliveTimerTaskPtr task2 = new KeepaliveTimerTask(*this, ns_ip_port_[1]);
        get_timer()->scheduleRepeated(task2, tbutil::Time::seconds(heart_interval));

        MAX_WRITE_FILE_SIZE = TBSYS_CONFIG.getInt(CONF_SN_MOCK_DATASERVER, CONF_MK_MAX_WRITE_FILE_SIZE, 0);
      }
      return ret;
    }

    int MockDataService::callback(common::NewClient* client)
    {
      int32_t ret = NULL != client ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
        NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
        ret = NULL != sresponse && fresponse != NULL ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == ret)
        {
          tbnet::Packet* packet = client->get_source_msg();
          assert(NULL != packet);
          bool all_success = sresponse->size() == client->get_send_id_sign().size();
          BasePacket* bpacket= dynamic_cast<BasePacket*>(packet);
          StatusMessage* reply_msg =  all_success ?
            new StatusMessage(STATUS_MESSAGE_OK, "write data complete"):
            new StatusMessage(STATUS_MESSAGE_ERROR, "write data fail");
          ret = bpacket->reply(reply_msg);
        }
      }
      return ret;

    }

    int MockDataService::write(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (WRITE_DATA_MESSAGE == msg->getPCode())) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        WriteDataMessage* message = dynamic_cast<WriteDataMessage*>(msg);
        WriteDataInfo write_info = message->get_write_info();
        uint32_t lease_id = message->get_lease_id();
        int32_t version = message->get_block_version();
        write_info.length_  = MAX_WRITE_FILE_SIZE != 0 ? MAX_WRITE_FILE_SIZE : write_info.length_;
        BlockEntry* entry = get(write_info.block_id_);
        if (NULL == entry)
        {
          ret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "failed. blockid: %u, fileid: %" PRI64_PREFIX "u.",
                write_info.block_id_, write_info.file_id_);
        }
        else
        {
          entry->info_.size_ += write_info.length_;
          information_.use_capacity_ += write_info.length_;
          information_.total_tp_.write_byte_ += write_info.length_;
          information_.total_tp_.write_file_count_++;
          if (Master_Server_Role == write_info.is_server_)
          {
            message->set_server(Slave_Server_Role);
            message->set_lease_id(lease_id);
            message->set_block_version(version);
            ret = this->post_message_to_server(message, message->get_ds_list());
            if ( 0x01 == ret)
            {
              ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
            }
            else
            {
              ret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret,
                  "write data fail to other dataserver (send): blockid: %u, fileid: %" PRI64_PREFIX "u, datalen: %d, ret: %d",
                  write_info.block_id_, write_info.file_id_, write_info.length_, ret);
            }
          }
          else
          {
            ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
          }
        }
      }
      return ret;
    }

    int MockDataService::read(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (READ_DATA_MESSAGE == msg->getPCode())) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ReadDataMessage* message = dynamic_cast<ReadDataMessage*>(msg);
        RespReadDataMessage* rmsg = new RespReadDataMessage();
        char* data = rmsg->alloc_data(BUF_LEN);
        memcpy(data, BUF, BUF_LEN);
        rmsg->set_length(BUF_LEN);
        ret = message->reply(rmsg);
        information_.total_tp_.read_byte_ += BUF_LEN;
        information_.total_tp_.read_file_count_++;
      }
      return ret;
    }

    int MockDataService::readv2(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (READ_DATA_MESSAGE_V2 == msg->getPCode())) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ReadDataMessageV2* message = dynamic_cast<ReadDataMessageV2*>(msg);
        uint32_t block_id = message->get_block_id();
        RespReadDataMessageV2* rmsg = new RespReadDataMessageV2();
        RWLock::Lock lock(blocks_mutex_, READ_LOCKER);
        BlockEntry* entry = get_(block_id);
        if (NULL == entry)
        {
          rmsg->set_length(0);
          ret = message->reply(rmsg);
        }
        else
        {
          char* data = rmsg->alloc_data(BUF_LEN);
          memcpy(data, BUF, BUF_LEN);
          rmsg->set_length(BUF_LEN);
          rmsg->set_file_info(&gfile_info);
          ret = message->reply(rmsg);
        }
      }
      return ret;
    }

    int MockDataService::close(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (CLOSE_FILE_MESSAGE == msg->getPCode())) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        CloseFileMessage* message = dynamic_cast<CloseFileMessage*>(msg);
        CloseFileInfo info = message->get_close_file_info();
        uint32_t lease_id = message->get_lease_id();
        BlockEntry* entry = get(info.block_id_);
        if (NULL == entry)
        {
          ret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret,
              "close write file failed. block is not exist. blockid: %u, fileid: %" PRI64_PREFIX "u.", info.block_id_, info.file_id_);
        }
        else
        {
          if (CLOSE_FILE_SLAVER != info.mode_)
          {
            entry->info_.version_++;
            message->set_mode(CLOSE_FILE_SLAVER);
            message->set_block(&entry->info_);
            ret = send_message_to_slave(message, message->get_ds_list());
            if (ret != TFS_SUCCESS)
            {
              ret = commit_to_nameserver(entry, info.block_id_, lease_id, TFS_ERROR);
            }
            else
            {
              ret = commit_to_nameserver(entry, info.block_id_, lease_id, TFS_SUCCESS);
            }
            if (ret == TFS_SUCCESS)
            {
              ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
            }
            else
            {
              TBSYS_LOG(ERROR, "dataserver commit fail, block_id: %u, file_id: %"PRI64_PREFIX"u, ret: %d",
                  info.block_id_, info.file_id_, ret);
              ret = message->reply(new StatusMessage(STATUS_MESSAGE_ERROR));
            }
          }
          else
          {
            const BlockInfo* copyblk = message->get_block();
            if (NULL != copyblk)
            {
              memcpy(&entry->info_, copyblk, sizeof(BlockInfo));
            }
            ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
          }
        }
      }
      return ret;
    }

    int MockDataService::create_file_number(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (CREATE_FILENAME_MESSAGE == msg->getPCode()))? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        CreateFilenameMessage* message = dynamic_cast<CreateFilenameMessage*>(msg);
        uint32_t block_id =  message->get_block_id();
        uint64_t file_id  = message->get_file_id();
        BlockEntry* entry = get(block_id);
        if (NULL == entry)
        {
          ret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret,
              "create file failed. blockid: %u, fileid: %" PRI64_PREFIX "u.", block_id, file_id);
        }
        else
        {
          file_id = ++entry->file_id_factory_;
          RespCreateFilenameMessage* rmsg = new RespCreateFilenameMessage();
          rmsg->set_block_id(block_id);
          rmsg->set_file_id(file_id);
          rmsg->set_file_number(file_id);
          ret = message->reply(rmsg);
        }
      }
      return ret;
    }

    int MockDataService::new_block(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && NEW_BLOCK_MESSAGE == msg->getPCode()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        NewBlockMessage* message = dynamic_cast<NewBlockMessage*>(msg);
        const VUINT32* blocks = message->get_new_blocks();
        VUINT32::const_iterator iter = blocks->begin();
        for (; iter != blocks->end(); ++iter)
        {
          BlockEntry entry;
          entry.info_.block_id_ = (*iter);
          entry.info_.version_ = 1;
          if (!insert(entry))
          {
            TBSYS_LOG(WARN, "block(%u) exist", (*iter));
          }
          TBSYS_LOG(INFO, "add new block(%u)", (*iter));
        }
        ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
      return ret;
    }

    int MockDataService::remove_block(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (REMOVE_BLOCK_MESSAGE == msg->getPCode())) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RemoveBlockMessage* message = dynamic_cast<RemoveBlockMessage*>(msg);
        remove(message->get());
        RemoveBlockResponseMessage* result = new RemoveBlockResponseMessage();
        result->set_seqno(message->get_seqno());
        result->set(message->get());
        ret = message->reply(result);
      }
      return ret;
    }

    int MockDataService::get_file_info(BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (FILE_INFO_MESSAGE == msg->getPCode()))? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        FileInfoMessage* message = dynamic_cast<FileInfoMessage*>(msg);
        RespFileInfoMessage* rmsg = new RespFileInfoMessage();
        rmsg->set_file_info(&gfile_info);
        ret = message->reply(rmsg);
      }
      return ret;
    }

    int MockDataService::report_block(BasePacket* packet)
    {
      int32_t ret = ((NULL != packet) && (REQ_CALL_DS_REPORT_BLOCK_MESSAGE == packet->getPCode())) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        CallDsReportBlockRequestMessage* msg = dynamic_cast<CallDsReportBlockRequestMessage*>(packet);
        ReportBlocksToNsRequestMessage req_msg;
        req_msg.set_server(information_.id_);
        {
          RWLock::Lock lock(blocks_mutex_, READ_LOCKER);
          std::map<uint32_t, BlockEntry>::iterator iter = blocks_.begin();
          for (; iter != blocks_.end(); ++iter)
          {
            req_msg.get_blocks().insert(iter->second.info_);
          }
        }
        NewClient* client = NewClientManager::get_instance().create_client();
        tbnet::Packet* message = NULL;
        ret = send_msg_to_server(msg->get_server(), client, &req_msg, message);
        TBSYS_LOG(DEBUG, "report block : %d %s", ret, tbsys::CNetUtil::addrToString(msg->get_server()).c_str());
        if (TFS_SUCCESS == ret)
        {
          ret = message->getPCode() == RSP_REPORT_BLOCKS_TO_NS_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == ret)
          {
            ReportBlocksToNsResponseMessage* msg = dynamic_cast<ReportBlocksToNsResponseMessage*>(message);
            std::vector<uint32_t>::const_iterator iter = msg->get_blocks().begin();
            for (; iter != msg->get_blocks().end(); ++iter)
            {
              RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
              remove_((*iter));
            }
            TBSYS_LOG(INFO, "nameserver %s ask for expire block, size: %zd\n", tbsys::CNetUtil::addrToString(msg->get_server()).c_str(), msg->get_blocks().size());
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }

    int MockDataService::replicate_block(BasePacket* packet)
    {
      int32_t ret = ((NULL != packet) && (REPLICATE_BLOCK_MESSAGE == packet->getPCode())) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ReplicateBlockMessage* msg = dynamic_cast<ReplicateBlockMessage*>(packet);
        ReplBlock info;
        memcpy(&info,  msg->get_repl_block(), sizeof(ReplBlock));
        packet->reply(new StatusMessage(STATUS_MESSAGE_OK));

        tbnet::Packet* result = NULL;
        RawMetaVec raw_meta_vec;
        WriteInfoBatchMessage req;
        req.set_block_id(info.block_id_);
        req.set_offset(0);
        req.set_length(raw_meta_vec.size());
        req.set_raw_meta_list(&raw_meta_vec);
        BlockEntry* entry = get_(info.block_id_);
        static const int32_t COPY_BETWEEN_CLUSTER = -1;
        req.set_block_info(NULL != entry ? &entry->info_ : NULL);
        if (COPY_BETWEEN_CLUSTER == info.server_count_)
            req.set_cluster(COPY_BETWEEN_CLUSTER);
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = send_msg_to_server(info.destination_id_, client, &req, result);
        NewClientManager::get_instance().destroy_client(client);
        TBSYS_LOG(DEBUG, "replicate block :%u, server: %s, ret: %d", info.block_id_, CNetUtil::addrToString(info.destination_id_).c_str(), ret);
        if (TFS_SUCCESS == ret)
        {
          ReplicateBlockMessage rreq;
          rreq.set_repl_block(&info);
          rreq.set_command(random() % 32 == 0 ? PLAN_STATUS_FAILURE : PLAN_STATUS_END);
          rreq.set_seqno(msg->get_seqno());
          result = NULL;
          client = NewClientManager::get_instance().create_client();
          ret = send_msg_to_server(ns_vip_, client, &rreq, result);
          TBSYS_LOG(DEBUG, "replicate block :%u, ns: %s, ret: %d", info.block_id_, CNetUtil::addrToString(ns_vip_).c_str(), ret);
          if (TFS_SUCCESS == ret)
          {
            if (STATUS_MESSAGE == result->getPCode())
            {
              StatusMessage* sm = dynamic_cast<StatusMessage*>(result);
              if (info.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES
                  && STATUS_MESSAGE_REMOVE == sm->get_status())
              {
                RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
                entry = get_(info.block_id_);
                if (NULL != entry)
                {
                  information_.use_capacity_ -= entry->info_.size_;
                }
                remove_(info.block_id_);
              }
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        TBSYS_LOG(DEBUG, "replicate block: %u, ret: %d", info.block_id_, ret);
      }
      return ret;
    }

    int MockDataService::compact_block(BasePacket* packet)
    {
      int32_t ret = ((NULL != packet) && COMPACT_BLOCK_MESSAGE == packet->getPCode()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      TBSYS_LOG(DEBUG, "compact block begin, packet == null : %d, %d, ret:%d", NULL == packet, packet->getPCode(), ret);
      if (TFS_SUCCESS == ret)
      {
        packet->reply(new StatusMessage(STATUS_MESSAGE_OK));
        usleep(200000);
        CompactBlockMessage* msg = dynamic_cast<CompactBlockMessage*>(packet);
        CompactBlockCompleteMessage result;
        result.set_seqno(msg->get_seqno());
        result.set_block_id(msg->get_block_id());
        CompactStatus status = (random() % 32 == 0) ? COMPACT_STATUS_FAILED : COMPACT_STATUS_SUCCESS;
        result.set_success(status);
        result.set_server_id(information_.id_);
        BlockEntry* entry = get(msg->get_block_id());
        if (NULL != entry)
        {
          information_.use_capacity_ -= entry->info_.size_;
          random_info(entry->info_);
          information_.use_capacity_ += entry->info_.size_;
          result.set_block_info(entry->info_);
        }
        TBSYS_LOG(DEBUG, "compact block: %u", msg->get_block_id());
        NewClient* client = NewClientManager::get_instance().create_client();
        tbnet::Packet* tmp = NULL;
        ret = send_msg_to_server(ns_vip_, client, &result, tmp);
        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }

    int MockDataService::batch_write_info(BasePacket* packet)
    {
      int32_t ret = ((NULL != packet) && (WRITE_INFO_BATCH_MESSAGE == packet->getPCode()))? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        WriteInfoBatchMessage* message = dynamic_cast<WriteInfoBatchMessage*>(packet);
        uint32_t block_id = message->get_block_id();
        const BlockInfo* blk = message->get_block_info();
        BlockEntry entry;
        if (NULL != blk)
        {
          entry.info_ = *blk;
          insert(entry);
        }
        if (message->get_cluster())
        {
          BlockEntry* pentry = get(block_id);
          UpdateBlockType repair_type = NULL == pentry ? UPDATE_BLOCK_MISSING : UPDATE_BLOCK_NORMAL;
          BlockInfo* pinfo = NULL == pentry ? NULL : &pentry->info_;
          UpdateBlockInfoMessage req;
          req.set_block_id(block_id);
          req.set_block(pinfo);
          req.set_server_id(information_.id_);
          req.set_repair(repair_type);
          tbnet::Packet* result = NULL;
          NewClient* client = NewClientManager::get_instance().create_client();
          ret = send_msg_to_server(ns_vip_, client, &req, result);
          if (TFS_SUCCESS == ret)
          {
            bool bremove = false;
            ret = TFS_ERROR;
            if (STATUS_MESSAGE == result->getPCode())
            {
              StatusMessage* sm = dynamic_cast<StatusMessage*>(result);
              if (STATUS_MESSAGE_OK == sm->get_status())
                ret = TFS_SUCCESS;
              else if (STATUS_MESSAGE_REMOVE == sm->get_status())
              {
                bremove = true;
                ret = TFS_SUCCESS;
              }
            }
            if (bremove)
              remove(block_id);
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        if (TFS_SUCCESS == ret)
        {
          BlockEntry* pentry = get(block_id);
          if (NULL != pentry)
          {
            information_.use_capacity_ += entry.info_.size_;
          }
          ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
        else
          ret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret,
              "batch write block info error, block: %u, ret: %d", block_id, ret);
      }
      return ret;
    }

    int MockDataService::post_message_to_server(BasePacket* message, const VUINT64& ds_list)
    {
      int32_t ret = ((NULL != message) && !ds_list.empty())? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        VUINT64 result;
        VUINT64::const_iterator iter = ds_list.begin();
        for (; iter != ds_list.end(); ++iter)
        {
          if ((*iter) != information_.id_)
          {
            result.push_back((*iter));
          }
        }
        if (!result.empty())
        {
          NewClient* client = NewClientManager::get_instance().create_client();
          ret = client->async_post_request(result, message, ns_async_callback);
          NewClientManager::get_instance().destroy_client(client);
        }
      }
      return ret;
    }

    int MockDataService::send_message_to_slave(BasePacket* message, const VUINT64& ds_list)
    {
      int32_t ret = ((NULL != message) && !ds_list.empty())? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint8_t send_id = 0;
        NewClient* client = NewClientManager::get_instance().create_client();
        VUINT64::const_iterator iter = ds_list.begin();
        for (; iter  != ds_list.end(); ++iter)
        {
          if ((*iter) == information_.id_)
          {
            continue;
          }
          client->post_request((*iter), message, send_id);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "send message to server to dataserver(%s) fail.", tbsys::CNetUtil::addrToString((*iter)).c_str());
            break;
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }

    int MockDataService::commit_to_nameserver(BlockEntry* entry, uint32_t block_id, uint32_t lease_id, int32_t status, common::UnlinkFlag flag)
    {
      UNUSED(status);
      UNUSED(block_id);
      int32_t ret = NULL == entry ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        BlockWriteCompleteMessage rmsg;
        rmsg.set_block(&entry->info_);
        rmsg.set_server_id(information_.id_);
        rmsg.set_lease_id(lease_id);
        rmsg.set_success(WRITE_COMPLETE_STATUS_YES);
        rmsg.set_unlink_flag(flag);
        int32_t ret = STATUS_MESSAGE_ERROR;
        ret = send_msg_to_server(ns_vip_, &rmsg, ret);
        if (TFS_SUCCESS == ret)
        {
          ret = STATUS_MESSAGE_OK == ret ? TFS_SUCCESS : TFS_ERROR;
        }
      }
      return ret;
    }

    void MockDataService::random_info(common::BlockInfo& info)
    {
      if (info.file_count_ > 0)
        info.file_count_ = random() % info.file_count_;
      if (info.size_ > 0)
        info.size_ = random() % info.size_;
      info.del_file_count_ = 0;
      info.del_size_ = 0;
    }

    bool MockDataService::remove(const uint32_t block)
    {
      RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
      return remove_(block);
    }

    bool MockDataService::insert(const BlockEntry& entry)
    {
      RWLock::Lock lock(blocks_mutex_, WRITE_LOCKER);
      return insert_(entry);
    }

    bool MockDataService::insert_(const BlockEntry& entry)
    {
      std::pair<std::map<uint32_t, BlockEntry>::iterator, bool> res =
            blocks_.insert(std::map<uint32_t, BlockEntry>::value_type(entry.info_.block_id_, entry));
      return res.second;
    }
    bool MockDataService::remove_(const uint32_t block)
    {
      std::map<uint32_t, BlockEntry>::iterator iter = blocks_.find(block);
      if (blocks_.end() != iter)
        blocks_.erase(iter);
      return true;
    }

    BlockEntry* MockDataService::get(const uint32_t block)
    {
      RWLock::Lock lock(blocks_mutex_, READ_LOCKER);
      return get_(block);
    }

    BlockEntry* MockDataService::get_(const uint32_t block)
    {
      std::map<uint32_t, BlockEntry>::iterator iter = blocks_.find(block);
      return blocks_.end() != iter ? &(iter->second) : NULL;
    }

    KeepaliveTimerTask::KeepaliveTimerTask(MockDataService& instance, const uint64_t server)
      :instance_(instance),
       server_(server)
    {

    }

    KeepaliveTimerTask::~KeepaliveTimerTask()
    {

    }

    void KeepaliveTimerTask::runTimerTask()
    {
      instance_.keepalive(server_);
    }

    int ns_async_callback(NewClient* client)
    {
      MockDataService* service = dynamic_cast<MockDataService*>(BaseMain::instance());
      int32_t ret = NULL != service ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = service->callback(client);
      }
      return ret;
    }
  }/** end namespace mock**/
}/** end namespace tfs **/
