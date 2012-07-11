#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "common/base_service.h"
#include "common/lock.h"

const int32_t TEST_PCODE = 0xff;
const int32_t TEST_PCODE_RESPONSE = 0x100;
using namespace tfs::common;
class TestPacket: public tfs::common::BasePacket
{
public:
  TestPacket(){ setPCode(TEST_PCODE); value_ = 0xff;}
  virtual ~TestPacket() {}

  void set_value(const int32_t value)
  {
    value_ = value;
  }
  int32_t get_value()
  {
    return value_;
  }
  virtual int serialize(tfs::common::Stream& output) const
  {
    return output.set_int32(value_);
  }
  virtual int deserialize(tfs::common::Stream& input)
  {
    return input.get_int32(&value_);
  }
  virtual int64_t length() const
  {
    return tfs::common::INT_SIZE;
  }
  void dump()
  {
    TBSYS_LOG(DEBUG, "send value : %d", value_);
  }
private:
  int32_t value_;
};

class TestPacketResponse: public tfs::common::BasePacket
{
public:
  TestPacketResponse(){ setPCode(TEST_PCODE_RESPONSE); value_ = 0xfff;}
  virtual ~TestPacketResponse() {}
  virtual int serialize(tfs::common::Stream& output) const
  {
    return output.set_int32(value_);
  }
  virtual int deserialize(tfs::common::Stream& input)
  {
    return input.get_int32(&value_);
  }
  virtual int64_t length() const
  {
    return tfs::common::INT_SIZE;
  }
  void set_value(const int32_t value)
  {
    value_ = value;
  }
  int32_t get_value() const
  {
    return value_;
  }
  void dump()
  {
    TBSYS_LOG(DEBUG, "receive value : %d\n", value_);
  }
private:
  int32_t value_;
};

class TestPacketFactory: public tfs::common::BasePacketFactory
{
public:
  TestPacketFactory(){}
  tbnet::Packet* createPacket(int pcode)
  {
    tbnet::Packet* packet = BasePacketFactory::createPacket(pcode);
    if (NULL == packet)
    {
      int32_t real_pcode = pcode & 0xFFFF;
      switch (real_pcode)
      {
        case TEST_PCODE:
          packet = new TestPacket();
          break;
        case TEST_PCODE_RESPONSE:
          packet = new TestPacketResponse();
          break;
      }

    }
    return packet;
  }
};


