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
#include "message/replicate_block_message.h"
#include "new_replicate_block_msg.h"

using namespace tfs::tests;

class TestPacket : public virtual ::testing::Test
{
public:
  static void SetUpTestCase()
  {
  }
  static void TearDownTestCase()
  {
  }
  TestPacket(){}
  ~TestPacket(){}
};

TEST_F(TestPacket, msg_compatible)
{
  tfs::message::ReplicateBlockMessage old_msg;
  const int32_t BUF_LEN = old_msg.length();
  char buf[BUF_LEN];
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

  old_msg.set_command(command);
  old_msg.set_expire(expired);
  old_msg.set_repl_block(&block);

  //serialize old msg && deserialize new msg
  common::Stream buf;
  int32_t iret = old_msg.serialize(buf);
  EXPECT_EQ(tfs::common::TFS_SUCCESS, iret);

  tfs::common::Stream middle_buf;
  middle_buf.set_bytes(buf, BUF_LEN);
  NewReplicateBlockMessage new_msg;
  iret = new_msg.deserialize(middle_buf);
  EXPECT_EQ(tfs::common::TFS_SUCCESS, iret);
  EXPECT_EQ(command, new_msg.get_command());
  EXPECT_EQ(expired, new_msg.get_expire());
  EXPECT_EQ(0, memcmp(&block, new_msg.get_repl_block(), sizeof(tfs::common::ReplBlock)));

  //serialize new msg && deserialize old msg
  middle_buf.get_buffer().clear();
  iret = new_msg.serialize(middle_buf);
  EXPECT_EQ(tfs::common::TFS_SUCCESS, iret);

  tfs::message::ReplicateBlockMessage old_parse_msg;
  iret = old_parse_msg.parse(middle_buf.get_data(), middle_buf.get_data_length());
  EXPECT_EQ(tfs::common::TFS_SUCCESS, iret);
  EXPECT_EQ(command, old_parse_msg.get_command());
  EXPECT_EQ(expired, old_parse_msg.get_expire());
  EXPECT_EQ(0, memcmp(&block, old_parse_msg.get_repl_block(), sizeof(tfs::common::ReplBlock)));
}

TEST_F(TestPacket, copy)
{
  NewReplicateBlockMessage msg;
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

  msg.set_command(command);
  msg.set_expire(expired);
  msg.set_repl_block(&block);
  msg.set_connection((tbnet::Connection*)(0x1234444));
  msg.set_direction(tfs::common::DIRECTION_RECEIVE);
  msg.set_version(version);
  msg.set_id(id);

  NewReplicateBlockMessage copy_msg;

  bool bret = copy_msg.copy(&msg, version, true);
  EXPECT_EQ(true, bret);
  EXPECT_EQ(command, copy_msg.get_command());
  EXPECT_EQ(expired, copy_msg.get_expire());
  EXPECT_EQ(0, memcmp(&block, copy_msg.get_repl_block(), sizeof(tfs::common::ReplBlock)));
  EXPECT_EQ((tbnet::Connection*)(0x1234444), copy_msg.get_connection());
  EXPECT_EQ(version, copy_msg.get_version());
}

TEST_F(TestPacket, encode_and_decode)
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

  tfs::common::Stream st;
  int32_t iret = msg.serialize(st);
  EXPECT_EQ(tfs::common::TFS_SUCCESS, iret);

  uint32_t crc = tfs::common::Func::crc(tfs::common::TFS_PACKET_FLAG_V1, st.get_data(), st.get_data_length());
  msg.set_crc(crc);

  tfs::common::TfsPacketNewHeaderV1 pheader;
  pheader.crc_ = msg.get_crc();
  pheader.flag_ = tfs::common::TFS_PACKET_FLAG_V1;
  pheader.id_  = msg.get_id();
  pheader.length_ = msg.length();
  pheader.type_ = msg.getPCode();
  pheader.version_ = msg.get_version();

  tbnet::DataBuffer output;
  int32_t header_length = pheader.length();
  output.ensureFree(header_length + msg.length());
  
  //serialize msg
  int64_t pos = 0;
  iret = pheader.serialize(output.getFree(), output.getFreeLen(), pos);
  output.pourData(pos);
  output.writeBytes(st.get_data(), st.get_data_length());
  EXPECT_EQ(tfs::common::TFS_SUCCESS, iret);

  //decode
  pos = 0;
  NewReplicateBlockMessage dmsg;
  tfs::common::TfsPacketNewHeaderV0 dheader;
  iret = dheader.deserialize(output.getData(), output.getDataLen(), pos); 
  EXPECT_EQ(tfs::common::TFS_SUCCESS, iret);
  output.drainData(dheader.length());

  tbnet::PacketHeader ph;
  if (tfs::common::TFS_PACKET_FLAG_V1 == pheader.flag_)
  {
    ph._pcode = dheader.type_;
    ph._pcode |= (dheader.check_ << 16);
    ph._dataLen = dheader.length_;
    ph._dataLen += tfs::common::TFS_PACKET_HEADER_DIFF_SIZE;
    ph._chid= 1;
  }
  else
  {
    ph._pcode = dheader.type_;
    ph._dataLen = dheader.length_;
    ph._chid = 1;
  }
  bool bret = dmsg.decode(&output, &ph);
  EXPECT_EQ(true, bret);

  EXPECT_EQ(command, dmsg.get_command());
  EXPECT_EQ(expired, dmsg.get_expire());
  EXPECT_EQ(0, memcmp(&block, dmsg.get_repl_block(), sizeof(tfs::common::ReplBlock)));
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
