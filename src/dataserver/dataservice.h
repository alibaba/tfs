/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataservice.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *
 */
#ifndef TFS_DATASERVER_DATASERVICE_H_
#define TFS_DATASERVER_DATASERVICE_H_

#include <Timer.h>
#include <Mutex.h>
#include <string>
#include "common/internal.h"
#include "common/base_packet.h"
#include "common/base_service.h"
#include "common/statistics.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "replicate_block.h"
#include "compact_block.h"
#include "check_block.h"
#include "sync_base.h"
#include "visit_stat.h"
#include "cpu_metrics.h"
#include "data_management.h"
#include "requester.h"
#include "block_checker.h"
#include "gc.h"


namespace tfs
{
  namespace dataserver
  {
#define WRITE_STAT_LOGGER write_stat_log_
#define WRITE_STAT_PRINT(level, ...) WRITE_STAT_LOGGER.logMessage(TBSYS_LOG_LEVEL(level), __VA_ARGS__)
#define WRITE_STAT_LOG(level, ...) (TBSYS_LOG_LEVEL_##level>WRITE_STAT_LOGGER._level) ? (void)0 : WRITE_STAT_PRINT(level, __VA_ARGS__)

#define READ_STAT_LOGGER read_stat_log_
#define READ_STAT_PRINT(level, ...) READ_STAT_LOGGER.logMessage(TBSYS_LOG_LEVEL(level), __VA_ARGS__)
#define READ_STAT_LOG(level, ...) (TBSYS_LOG_LEVEL_##level>READ_STAT_LOGGER._level) ? (void)0 : READ_STAT_PRINT(level, __VA_ARGS__)
    class DataService: public common::BaseService
    {

      friend int SyncBase::run_sync_mirror();

      public:
        DataService();

        virtual ~DataService();

        /** application parse args*/
        virtual int parse_common_line_args(int argc, char* argv[], std::string& errmsg);

        /** get listen port*/
        virtual int get_listen_port() const ;

        /** initialize application data*/
        virtual int initialize(int argc, char* argv[]);

        /** destroy application data*/
        virtual int destroy_service();

        /** create the packet streamer, this is used to create packet according to packet code */
        virtual tbnet::IPacketStreamer* create_packet_streamer()
        {
          return new common::BasePacketStreamer();
        }

        /** destroy the packet streamer*/
        virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
        {
          tbsys::gDelete(streamer);
        }

        /** create the packet streamer, this is used to create packet*/
        virtual common::BasePacketFactory* create_packet_factory()
        {
          return new message::MessageFactory();
        }

        /** destroy packet factory*/
        virtual void destroy_packet_factory(common::BasePacketFactory* factory)
        {
          tbsys::gDelete(factory);
        }

        /** handle single packet */
        virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);

        /** handle packet*/
        virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);

        bool check_response(common::NewClient* client);
        int callback(common::NewClient* client);

        int post_message_to_server(common::BasePacket* message, const common::VUINT64& ds_list);

        int stop_heart();

        std::string get_real_work_dir();

      protected:
        virtual const char* get_log_file_path();
        virtual const char* get_pid_file_path();

      private:
        int run_heart(const int32_t who);
        int run_check();

        int create_file_number(message::CreateFilenameMessage* message);
        int write_data(message::WriteDataMessage* message);
        int close_write_file(message::CloseFileMessage* message);

        int write_raw_data(message::WriteRawDataMessage* message);
        int batch_write_info(message::WriteInfoBatchMessage* message);

        int read_data(message::ReadDataMessage* message);
        int read_data_extra(message::ReadDataMessageV2* message, int32_t version);
        int read_raw_data(message::ReadRawDataMessage* message);
        int read_file_info(message::FileInfoMessage* message);

        int rename_file(message::RenameFileMessage* message);
        int unlink_file(message::UnlinkFileMessage* message);

        //NS <-> DS
        int new_block(message::NewBlockMessage* message);
        int remove_block(message::RemoveBlockMessage* message);

        int replicate_block_cmd(message::ReplicateBlockMessage* message);
        int compact_block_cmd(message::CompactBlockMessage* message);
        int crc_error_cmd(message::CrcErrorMessage* message);

        //get single blockinfo
        int get_block_info(message::GetBlockInfoMessage* message);

        int query_bit_map(message::ListBitMapMessage* message);
        //get blockinfos on this server
        int list_blocks(message::ListBlockMessage* message);
        int reset_block_version(message::ResetBlockVersionMessage* message);

        int get_server_status(message::GetServerStatusMessage* message);
        int get_ping_status(common::StatusMessage* message);
        int client_command(message::ClientCmdMessage* message);

        int reload_config(message::ReloadConfigMessage* message);
        int send_blocks_to_ns(int8_t& heart_interval, const int32_t who, const int64_t timeout);
        int send_blocks_to_ns(common::BasePacket* packet);

        int get_dataserver_information(common::BasePacket* packet);

        // check modified blocks
        int check_blocks(common::BasePacket* packet);

      private:
        bool access_deny(common::BasePacket* message);
        void do_stat(const uint64_t peer_id,
            const int32_t visit_file_size, const int32_t real_len, const int32_t offset, const int32_t mode);
        int set_ns_ip();
        void try_add_repair_task(const uint32_t block_id, const int ret);
        int init_log_file(tbsys::CLogger& LOGGER, const std::string& log_file);
        int init_sync_mirror();

      private:
      class HeartBeatThreadHelper: public tbutil::Thread
      {
        public:
          HeartBeatThreadHelper(DataService& service, const int32_t who):
              service_(service),
              who_(who)
          {
            start();
          }
          virtual ~HeartBeatThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(HeartBeatThreadHelper);
          DataService& service_;
          int32_t who_;
      };
      typedef tbutil::Handle<HeartBeatThreadHelper> HeartBeatThreadHelperPtr;

      class DoCheckThreadHelper: public tbutil::Thread
      {
        public:
          explicit DoCheckThreadHelper(DataService& service):
              service_(service)
          {
            start();
          }
          virtual ~DoCheckThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(DoCheckThreadHelper);
          DataService& service_;
      };
      typedef tbutil::Handle<DoCheckThreadHelper> DoCheckThreadHelperPtr;

      class ReplicateBlockThreadHelper: public tbutil::Thread
      {
        public:
          explicit ReplicateBlockThreadHelper(DataService& service):
              service_(service)
          {
            start();
          }
          virtual ~ReplicateBlockThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(ReplicateBlockThreadHelper);
          DataService& service_;
      };
      typedef tbutil::Handle<ReplicateBlockThreadHelper> ReplicateBlockThreadHelperPtr;

      class CompactBlockThreadHelper: public tbutil::Thread
      {
        public:
          explicit CompactBlockThreadHelper(DataService& service):
              service_(service)
          {
            start();
          }
          virtual ~CompactBlockThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(CompactBlockThreadHelper);
          DataService& service_;
      };
      typedef tbutil::Handle<CompactBlockThreadHelper> CompactBlockThreadHelperPtr;

      private:
        DISALLOW_COPY_AND_ASSIGN(DataService);

        common::DataServerStatInfo data_server_info_; //dataserver info
        std::string server_index_;
        DataManagement data_management_;
        Requester ds_requester_;
        BlockChecker block_checker_;

        int32_t server_local_port_;
        bool need_send_blockinfo_[2];
        bool set_flag_[2];
        uint64_t hb_ip_port_[2];
        uint64_t ns_ip_port_; //nameserver ip port;

        ReplicateBlock* repl_block_; //replicate
        CompactBlock* compact_block_; //compact
        CheckBlock* check_block_;  // check
#if defined(TFS_GTEST)
      public:
#else
#endif
        std::vector<SyncBase*> sync_mirror_;
        int32_t sync_mirror_status_;

        tbutil::Mutex stop_mutex_;
        tbutil::Mutex client_mutex_;
        tbutil::Mutex compact_mutext_;
        tbutil::Mutex count_mutex_;
        tbutil::Mutex read_stat_mutex_;
        tbutil::Mutex sync_mirror_mutex_;

        AccessControl acl_;
        AccessStat acs_;
        VisitStat visit_stat_;
        CpuMetrics cpu_metrics_;
        int32_t max_cpu_usage_;

        //write and read log
        tbsys::CLogger write_stat_log_;
        tbsys::CLogger read_stat_log_;
        std::vector<std::pair<uint32_t, uint64_t> > read_stat_buffer_;
        static const unsigned READ_STAT_LOG_BUFFER_LEN = 100;

        //global stat
        tbutil::TimerPtr timer_;
        common::StatManager<std::string, std::string, common::StatEntry > stat_mgr_;
        std::string tfs_ds_stat_;

        HeartBeatThreadHelperPtr heartbeat_thread_[2];
        DoCheckThreadHelperPtr   do_check_thread_;
        ReplicateBlockThreadHelperPtr* replicate_block_threads_;
        CompactBlockThreadHelperPtr  compact_block_thread_;

        std::string read_stat_log_file_;
        std::string write_stat_log_file_;
    };
  }
}

#endif //TFS_DATASERVER_DATASERVICE_H_
