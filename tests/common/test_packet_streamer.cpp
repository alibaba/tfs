/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_chunk.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   duanfei 
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "common/func.h"
#include "common/base_packet.h"
#include "common/base_packet_factory.h"
#include "common/base_packet_streamer.h"
#include "message/replicate_block_message.h"
#include "new_replicate_block_msg.h"

using namespace tfs::tests;

class TestPacketStreamer : public virtual ::testing::Test
{
public:
  static void SetUpTestCase()
  {
  }
  static void TearDownTestCase()
  {
  }
  TestPacketStreamer(){}
  ~TestPacketStreamer(){}
};

class TestPacketFactory : public tfs::common::BasePacketFactory
{
public:
TestPacketFactory(){}
virtual ~TestPacketFactory(){}
tbnet::Packet* createPacket(int pcode) { return NULL;}
tbnet::Packet* clone_packet(tbnet::Packet* packet, int32_t version, bool deserialize)
{
  return NULL;
}
};

TEST_F(TestPacketStreamer, encode_and_decode)
{
  tfs::common::ReplBlock block;
  memset(&block, 0, sizeof(block));
  block.block_id_ = 0xff;
  block.source_id_ = 0xfff;
  block.destination_id_ = 0xffff;
  block.start_time_ = 0xfffff;
  block.is_move_ = 1;
  block.server_count_ = 0xf;

  const int32_t command = 2;
  const int32_t expired = 0x1234;
  const int32_t version = 2;
  const int32_t id = 0xffff;

  //encode
  NewReplicateBlockMessage msg;
  msg.set_command(command);
  msg.set_expire(expired);
  msg.set_repl_block(&block);
  msg.set_version(version);
  msg.set_id(id);

  int32_t iret = msg.serialize(msg.stream_);
  EXPECT_EQ(tfs::common::TFS_SUCCESS, iret);

  tfs::common::BasePacketStreamer streamer;
  tbnet::DataBuffer output;
  bool bret = streamer.encode(NULL,&output); 
  EXPECT_EQ(false, bret);
  bret = streamer.encode(&msg,NULL); 
  EXPECT_EQ(false, bret);
  bret = streamer.encode(NULL,NULL); 
  EXPECT_EQ(false, bret);

  //v1, v2
  output.clear();
  bret = streamer.encode(&msg,&output); 
  EXPECT_EQ(true, bret);
  EXPECT_EQ(tfs::common::TFS_PACKET_HEADER_V1_SIZE + msg.length(), output.getDataLen());
}


TEST_F(TestPacketStreamer, get_packet_info)
{
  tfs::common::ReplBlock block;
  memset(&block, 0, sizeof(block));
  block.block_id_ = 0xff;
  block.source_id_ = 0xfff;
  block.destination_id_ = 0xffff;
  block.start_time_ = 0xfffff;
  block.is_move_ = 1;
  block.server_count_ = 0xf;

  const int32_t command = 2;
  const int32_t expired = 0x1234;
  const int32_t version = 1;
  const int32_t id = 0xffff;

  //encode
  NewReplicateBlockMessage msg;
  msg.set_command(command);
  msg.set_expire(expired);
  msg.set_repl_block(&block);
  msg.set_version(version);
  msg.set_id(id);

  int32_t iret = msg.serialize(msg.stream_);
  EXPECT_EQ(tfs::common::TFS_SUCCESS, iret);

  tfs::common::BasePacketStreamer streamer;
  tbnet::DataBuffer input;
  input.clear();
  bool bret = streamer.encode(&msg,&input); 
  bool broken = false;
   tbnet::PacketHeader header;
  EXPECT_EQ(true, bret);
  bret = streamer.getPacketInfo(NULL,NULL, &broken); 
  EXPECT_EQ(false, bret);
  bret = streamer.getPacketInfo(&input,NULL, &broken); 
  EXPECT_EQ(false, bret);
  bret = streamer.getPacketInfo(NULL,&header, &broken); 
  EXPECT_EQ(false, bret);
  tbnet::DataBuffer tmp;
  bret = streamer.getPacketInfo(&tmp,&header, &broken); 
  EXPECT_EQ(false, bret);
  bret = streamer.getPacketInfo(&input,&header, &broken); 
  EXPECT_EQ(true, bret);
  EXPECT_EQ(tfs::common::TFS_PACKET_HEADER_DIFF_SIZE + msg.length(), input.getDataLen());
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
