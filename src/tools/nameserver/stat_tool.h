#ifndef TFS_TOOLS_STAT_TOOL_H_
#define TFS_TOOLS_STAT_TOOL_H_

#include <set>
#include <vector>
#include <map>
#include <string>
#include "common/new_client.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "common/status_message.h"

namespace tfs
{
  namespace tools
  {
    static const int32_t M_UNITS = 1024 *1024;
    static const int32_t THRESHOLD = 128 * (1 << 20);
    static const uint32_t MAGIC_BLOCK_ID = (1 << 24);

    static const int32_t RANGE_ARR[][2]={{0, 10},{10, 20}, {20, 30}, {30, 40}, {40, 50},
      {50, 60}, {60, 70},{70, 72}, {72,76}, {76, 80}, {80, 128}, {128, -1}, {-1, -1}};
    static const int32_t DEL_RANGE_ARR[][2]={{0, 1},{1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6},
      {6, 7}, {7, 8},{8, 9}, {9, 10}, {10, 20}, {20, 30}, {30, 40}, {40, 50},
      {50, 60}, {60, 70},{70, 80}, {80, 90}, {90, 100}, {100, -1}, {-1, -1}};

    class ValueRange
    {
      public:
        ValueRange(const int32_t min_value, const int32_t max_value);
        virtual ~ValueRange();

        virtual void dump(FILE* fp) const = 0;

        bool is_in_range(const int32_t value) const;
        void incr();
        void decr();

      protected:
        int32_t min_value_;
        int32_t max_value_;
        int64_t count_;
    };
    typedef std::vector<ValueRange> V_VALUE_RANGE;
    typedef std::vector<ValueRange>::iterator V_VALUE_RANGE_ITER;

    class BlockSizeRange : public ValueRange
    {
      public:
        BlockSizeRange(const int32_t min_value, const int32_t max_value);
        virtual ~BlockSizeRange();

        virtual void dump(FILE* fp) const;
    };
    typedef std::vector<BlockSizeRange> V_BLOCK_SIZE_RANGE;
    typedef std::vector<BlockSizeRange>::iterator V_BLOCK_SIZE_RANGE_ITER;

    class DelBlockRange : public ValueRange
    {
      public:
        DelBlockRange(const int32_t min_value, const int32_t max_value);
        virtual ~DelBlockRange();

        virtual void dump(FILE* fp) const;
    };
    typedef std::vector<DelBlockRange> V_DEL_BLOCK_RANGE;
    typedef std::vector<DelBlockRange>::iterator V_DEL_BLOCK_RANGE_ITER;

    // block base construct
    struct ServerInfo
    {
      uint64_t server_id_;
      operator uint64_t() const {return server_id_;}
      ServerInfo& operator=(const ServerInfo& a)
      {
        server_id_ = a.server_id_;
        return *this;
      }
      bool operator==(const ServerInfo& b) const
      {
        return server_id_ == b.server_id_;
      }
      bool operator<<(std::ostream& os) const
      {
        return os << server_id_;
      }
    };

    class BlockBase
    {
      public:
        tfs::common::BlockInfo info_;
        std::vector<ServerInfo> server_list_;

        BlockBase();
        virtual ~BlockBase();
        bool operator<(const BlockBase& b) const
        {
          return info_.block_id_ < b.info_.block_id_;
        }

        int32_t deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, const int8_t type);
        void dump() const;
    };

    class BlockSize
    {
      public:
        BlockSize(const uint32_t block_id, const int32_t file_size)
          : block_id_(block_id), file_size_(file_size)
        {
        }
        ~BlockSize()
        {
        }

        uint32_t block_id_;
        int32_t file_size_;
        bool operator<(const BlockSize& b) const
        {
          if (file_size_ == b.file_size_)
          {
            return block_id_ < b.block_id_;
          }
          return file_size_ < b.file_size_;
        }
    };
    typedef std::set<BlockSize> BLOCK_SIZE_SET;
    typedef std::set<BlockSize>::iterator BLOCK_SIZE_SET_ITER;

    class StatInfo
    {
      public:
        StatInfo();
        ~StatInfo();
        static float div(const int64_t a, const int64_t b);
        void add(const BlockBase& block_base);
        void dump(FILE* fp) const;
      protected:
        int32_t block_count_;
        int64_t file_count_;
        int64_t file_size_;
        int64_t del_file_count_;
        int64_t del_file_size_;
    };

    static inline uint64_t get_addr(const std::string& ns_ip_port)
    {
      std::string::size_type pos = ns_ip_port.find_first_of(":");
      if (pos == std::string::npos)
      {
        return tfs::common::TFS_ERROR;
      }
      std::string tmp = ns_ip_port.substr(0, pos);
      return tfs::common::Func::str_to_addr(tmp.c_str(), atoi(ns_ip_port.substr(pos + 1).c_str()));
    }
  } //:tools
}

#endif
