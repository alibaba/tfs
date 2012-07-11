/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_NEW_CLIENT_H_
#define TFS_COMMON_NEW_CLIENT_H_
#include <tbsys.h>
#include <tbnet.h>
#include <map>
#include <ext/hash_map>
#include <Monitor.h>
#include <Mutex.h>
#include "base_packet.h"

namespace tfs
{
  namespace common 
  {
    struct WaitId
    {
      uint32_t seq_id_:24;
      uint8_t  send_id_;
    };
    class NewClientManager;
    class NewClient 
    {
      friend class NewClientManager;
      friend class LocalPacket;
      public:
        typedef std::map<uint8_t, std::pair<uint64_t, tbnet::Packet*> > RESPONSE_MSG_MAP;
        typedef RESPONSE_MSG_MAP::iterator RESPONSE_MSG_MAP_ITER;
        typedef std::pair<uint8_t, uint64_t> SEND_SIGN_PAIR;
        typedef int (*callback_func)(NewClient* client);
      public:
        explicit NewClient(const uint32_t& seq_id);
        virtual ~NewClient();
        bool wait(const int64_t timeout_in_ms = common::DEFAULT_NETWORK_CALL_TIMEOUT);
        int post_request(const uint64_t server, tbnet::Packet* packet, uint8_t& send_id);
        int async_post_request(const std::vector<uint64_t>& servers, tbnet::Packet* packet, callback_func func, bool save_source_msg = true);
        inline RESPONSE_MSG_MAP* get_success_response() { return complete_ ? &success_response_ : NULL;}
        inline RESPONSE_MSG_MAP* get_fail_response() { return complete_ ? &fail_response_ : NULL;}
        inline tbnet::Packet* get_source_msg() { return source_msg_;}
        inline std::vector<SEND_SIGN_PAIR>& get_send_id_sign() { return send_id_sign_;}

      private:
        NewClient();
        DISALLOW_COPY_AND_ASSIGN(NewClient);
        tbutil::Monitor<tbutil::Mutex> monitor_;
        RESPONSE_MSG_MAP success_response_;
        RESPONSE_MSG_MAP fail_response_;
        std::vector<SEND_SIGN_PAIR> send_id_sign_;
        callback_func callback_;
        tbnet::Packet* source_msg_;
        const uint32_t seq_id_;
        uint8_t generate_send_id_;
        static const uint8_t MAX_SEND_ID = 0xFF - 1;
        bool complete_;// receive all response(data packet, timeout packet) complete or timeout
        bool post_packet_complete_;
      private:
        SEND_SIGN_PAIR* find_send_id(const WaitId& id);
        bool push_fail_response(const uint8_t send_id, const uint64_t server);
        bool push_success_response(const uint8_t send_id, const uint64_t server, tbnet::Packet* packet);
        bool handlePacket(const WaitId& id, tbnet::Packet* packet, bool& is_callback);

        uint8_t create_send_id(const uint64_t server);
        bool destroy_send_id(const WaitId& id);

        inline const uint32_t get_seq_id() const { return seq_id_;}

        bool async_wait();
    };
    int send_msg_to_server(uint64_t server, tbnet::Packet* message, int32_t& status, 
                          const int64_t timeout = common::DEFAULT_NETWORK_CALL_TIMEOUT/*ms*/);
    int send_msg_to_server(uint64_t server, NewClient* client, tbnet::Packet* msg, tbnet::Packet*& output/*not free*/,
                          const int64_t timeout = common::DEFAULT_NETWORK_CALL_TIMEOUT/*ms*/);
    int post_msg_to_server(const std::vector<uint64_t>& servers, NewClient* client, tbnet::Packet* message, NewClient::callback_func func,
                          bool save_source_msg = true);
    int post_msg_to_server(uint64_t servers, NewClient* client, tbnet::Packet* message, NewClient::callback_func func,
                          bool save_source_msg = true);
    int test_server_alive(const uint64_t server_id, const int64_t timeout = common::DEFAULT_NETWORK_CALL_TIMEOUT/*ms*/);
  } /* message */
} /* tfs */

#endif /* TFS_NEW_CLINET_H_*/
