/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: statistics.h 519 2011-06-20 05:15:42Z nayan@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_STATISTICS_H_
#define TFS_COMMON_STATISTICS_H_
#include <map>

#include <Timer.h>
#include <Handle.h>
#include <Shared.h>

#include "error_msg.h"
#include "internal.h"
#include "atomic.h"

namespace tfs
{
  namespace common
  {
    // wrap atomic x86_64 limit.
#if __WORDSIZE == 32
#define stat_int_t int32_t
#define stat_uint_t uint32_t
#define STAT_INT_T_SIZE INT_SIZE
#else
#define stat_int_t int64_t
#define stat_uint_t uint64_t
#define STAT_INT_T_SIZE INT64_SIZE
#endif

    template < typename Key, typename SubKey>
      class StatEntry : public virtual tbutil::TimerTask
    {
      public:
        typedef tbutil::Handle< StatEntry< Key, SubKey > > StatEntryPtr;
      public:
        StatEntry(const Key& key, int64_t begin, bool dump_complete_reset){};
        virtual ~StatEntry(){};
        virtual void runTimerTask() = 0;
        virtual int serialize() = 0;
        virtual int serialize(char* data, int64_t& pos, int64_t length) = 0;
        virtual void reset() = 0;

        virtual int update(std::vector<stat_int_t>& update_list, bool inc = true) = 0;
        virtual int update(const SubKey& subkey, const int64_t value, bool inc = true) = 0;

      private:
        StatEntry();
        StatEntry(const StatEntry&);
        StatEntry& operator=(const StatEntry&);
    };

    template<>
      class StatEntry<int64_t, stat_int_t>: public virtual tbutil::TimerTask
      {
        public:
          typedef tbutil::Handle< StatEntry< int64_t, stat_int_t > > StatEntryPtr;

        public:
          StatEntry(const int64_t& key, int64_t begin, bool dump_complete_reset)
            :key_(key),
            begin_us_(begin),
            end_us_(begin),
            dump_complete_reset(dump_complete_reset)
        {

        }
          virtual ~StatEntry()
          {

          }
          inline virtual void runTimerTask();
          inline virtual int serialize();
          inline virtual int serialize(char* data, int64_t& pos, int64_t length);
          inline virtual void reset();
          inline virtual int update(std::vector<stat_int_t>& update_list, bool inc = true);
          inline virtual int update(const int64_t& sub_key, const stat_int_t value, bool inc = true);
          const int64_t& get_key() const { return key_;}
          inline void add_sub_key(const int64_t& sub_key);
        public:
          inline int32_t calculate_stat_entry_length();
          inline void stat_entry_serialize(std::string& result);
        private:
          int64_t key_;
          int64_t begin_us_;
          int64_t end_us_;
          bool dump_complete_reset;
          std::vector<int64_t> sub_key_;
          std::vector<stat_uint_t> count_;

        private:
          StatEntry();
          StatEntry(const StatEntry&);
          StatEntry& operator=(const StatEntry&);
      };

    template<>
      class StatEntry<std::string, std::string> : public virtual tbutil::TimerTask
      {
        public:
          typedef tbutil::Handle< StatEntry<std::string, std::string > > StatEntryPtr;
        public:
          StatEntry(const std::string& key, int64_t begin, bool dump_complete_reset)
            :key_(key),
            begin_us_(begin),
            end_us_(begin),
            dump_complete_reset(dump_complete_reset)
        {

        }
          virtual ~StatEntry(){}
          inline virtual void runTimerTask();
          inline virtual int serialize();
          inline virtual int serialize(char* data, int64_t& pos, int64_t length);
          inline virtual void reset();

          inline virtual int update(std::vector<stat_int_t>& update_list, bool inc = true);
          inline virtual int update(const std::string& sub_key, const stat_int_t value, bool inc = true);
          inline stat_uint_t get_current_value(const std::string& sub_key);

          const std::string& get_key() const { return key_;}
          inline void add_sub_key(const std::string& sub_key);
        private:
          inline int32_t calculate_stat_entry_length();
          inline void stat_entry_serialize(std::string& result);
        private:
          std::string key_;
          int64_t begin_us_;
          int64_t end_us_;
          bool dump_complete_reset;
          std::vector<std::string> sub_key_;

          std::vector<stat_uint_t> count_;

        private:
          StatEntry();
          StatEntry(const StatEntry&);
          StatEntry& operator=(const StatEntry&);
      };

    template<class Key, class SubKey, template<typename T, typename T2 > class Entry >
      class StatManager
      {
        typedef typename Entry<Key, SubKey>::StatEntryPtr EntryPtr;
        typedef typename std::map<Key, EntryPtr > STAT_MAP;
        typedef typename std::map<Key, EntryPtr >::value_type value_type;
        typedef typename std::map<Key, EntryPtr >::iterator STAT_MAP_ITER;
        public:
        StatManager();
        ~StatManager();
        bool is_init();
        int initialize(tbutil::TimerPtr timer);
        int wait_for_shut_down();
        int destroy();
        int reset_schedule_interval(const int64_t schedule_interval_us);
        int add_entry(const EntryPtr& entry, int64_t schedule_interval_us = 0);
        int remove_entry(const Key& key);
        int64_t get_stat_value(const Key& key, const SubKey& sub_key);

        int update_entry(const Key& key, std::vector<stat_int_t>& update_list, bool inc = true);
        int update_entry(const Key& key, const SubKey& subkey, const stat_int_t value, bool inc = true);
        void serialize(const Key& key) const;
        void serialize(const Key& key, char* data, int64_t& pos, int64_t length);
        private:
        bool destroy_;
        tbutil::TimerPtr timer_;
        STAT_MAP maps_;
      };

      void StatEntry<int64_t, stat_int_t>::runTimerTask()
      {
        end_us_ = tbsys::CTimeUtil::getTime();
        if (serialize() != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "%s", "dump statistics information error");
        }
        if (dump_complete_reset)
        {
          begin_us_ = end_us_;
          reset();
        }
      }

      int StatEntry<int64_t, stat_int_t>::serialize()
      {
        std::string result;
        stat_entry_serialize(result);
        TBSYS_LOG(INFO,"begin: %"PRI64_PREFIX"d, end: %"PRI64_PREFIX"d, key: %"PRI64_PREFIX"d, %s",
            begin_us_, end_us_, key_, result.c_str());
        return TFS_SUCCESS;
      }

      int StatEntry<int64_t, stat_int_t>::serialize(char* data, int64_t& pos, int64_t length)
      {
        int32_t size = INT64_SIZE * 3 + calculate_stat_entry_length();
        bool bret = (data != NULL) && (length - pos > size);
        if (bret)
        {
          memcpy((data + pos), &key_, INT64_SIZE);
          pos += INT64_SIZE;
          memcpy((data + pos), &begin_us_, INT64_SIZE);
          pos += INT64_SIZE;
          memcpy((data + pos), &end_us_, INT64_SIZE);
          pos += INT64_SIZE;
          std::vector<int64_t>::iterator siter = sub_key_.begin();
          std::vector<stat_uint_t>::iterator iter = count_.begin();

          for (; siter != sub_key_.end() && iter != count_.end(); ++siter, ++iter)
          {
            memcpy((data+pos), &(*iter), STAT_INT_T_SIZE);
            pos += STAT_INT_T_SIZE;
            memcpy((data+pos), &(*iter), STAT_INT_T_SIZE);
            pos += STAT_INT_T_SIZE;
          }
        }
        return bret ? TFS_SUCCESS : EXIT_GENERAL_ERROR;
      }

      void StatEntry<int64_t, stat_int_t>::reset()
      {
        std::vector<stat_uint_t>::iterator iter = count_.begin();
        for (; iter != count_.end(); ++iter)
        {
          atomic_exchange((&(*iter)), 0);
        }
      }

      int StatEntry<int64_t, stat_int_t>::update(std::vector<stat_int_t>& update_list, bool inc)
      {
        std::vector<stat_int_t>::iterator iter = update_list.begin();
        std::vector<stat_uint_t>::iterator citer = count_.begin();

        std::vector<int64_t>::iterator siter = sub_key_.begin();
        for (; siter != sub_key_.end() && citer != count_.end() && iter != update_list.end();
            ++siter, ++citer, ++iter)
        {
          if ((*siter) > 0 && (*iter) != 0)
          {
            inc ? atomic_add((&(*citer)), (*iter)) : atomic_add((&(*citer)), (-(*iter)));
          }
        }
        return TFS_SUCCESS;
      }

      int StatEntry<int64_t, stat_int_t>::update(const int64_t& sub_key, const stat_int_t value, bool inc)
      {
        std::vector<int64_t>::const_iterator iter = std::find(sub_key_.begin(), sub_key_.end(), sub_key);
        int32_t iret = iter == sub_key_.end() ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          int32_t offset = iter - sub_key_.begin();
          if ((*iter) > 0  && value != 0 )
          {
            inc ? atomic_add((&(count_[offset])), value) : atomic_add((&(count_[offset])), (-value));
          }
        }
        return iret;
      }

      void StatEntry<int64_t, stat_int_t>::add_sub_key(const int64_t& sub_key)
      {
        sub_key_.push_back(sub_key);
        count_.push_back(0);
      }

      int32_t StatEntry<int64_t, stat_int_t>::calculate_stat_entry_length()
      {
        int32_t length = 0;
        std::vector<int64_t>::const_iterator iter = sub_key_.begin();
        std::vector<stat_uint_t>::const_iterator citer = count_.begin();
        for (; iter != sub_key_.end() && citer != count_.end(); ++iter, ++citer)
        {
          length += INT64_SIZE + STAT_INT_T_SIZE;
        }
        return length;
      }

      void StatEntry<int64_t, stat_int_t>::stat_entry_serialize(std::string& result)
      {
        std::vector<int64_t>::const_iterator iter = sub_key_.begin();
        std::vector<stat_uint_t>::const_iterator citer = count_.begin();

        for (; iter != sub_key_.end() && citer != count_.end(); ++iter, ++citer)
        {
          std::ostringstream str;
          str << (*iter) << " : " << (*citer) << " ";
          result += str.str();
        }
      }

    void StatEntry<std::string, std::string>::runTimerTask()
    {
      end_us_ = tbsys::CTimeUtil::getTime();
      if (serialize() != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s", "dump statistics information error");
      }
      if (dump_complete_reset)
      {
        begin_us_ = end_us_;
        reset();
      }
    }

    int StatEntry<std::string, std::string>::serialize()
    {
      std::string result;
      stat_entry_serialize(result);
      TBSYS_LOG(INFO,"begin: %"PRI64_PREFIX"d, end: %"PRI64_PREFIX"d, key: %s, %s",
          begin_us_, end_us_, key_.c_str(), result.c_str());
      return TFS_SUCCESS;
    }

    int StatEntry<std::string, std::string>::serialize(char* data, int64_t& pos, int64_t length)
    {
      int32_t size = INT64_SIZE * 2 + sizeof(int16_t) + key_.length() + calculate_stat_entry_length();
      bool bret = (data != NULL) && (length - pos >= size);
      if (bret)
      {
        int16_t length = key_.length();
        memcpy((data + pos), &length, sizeof(int16_t));
        pos += sizeof(int16_t);
        memcpy((data + pos), key_.c_str(), key_.length());
        pos += key_.length();
        memcpy((data + pos), &begin_us_, INT64_SIZE);
        pos += INT64_SIZE;
        memcpy((data + pos), &end_us_, INT64_SIZE);
        pos += INT64_SIZE;
        std::vector<std::string>::iterator iter = sub_key_.begin();
        std::vector<stat_uint_t>::iterator citer = count_.begin();

        for (; iter != sub_key_.end() && citer != count_.end(); ++iter, ++citer)
        {
          length = (*iter).length();
          memcpy((data+pos), &length, sizeof(int16_t));
          pos += sizeof(int16_t);
          memcpy((data+pos), (*iter).c_str(), length);
          pos += length;
          memcpy((data+pos), &(*citer), STAT_INT_T_SIZE);
          pos += STAT_INT_T_SIZE;
        }
      }
      return bret ? TFS_SUCCESS : EXIT_GENERAL_ERROR;
    }

    void StatEntry<std::string, std::string>::reset()
    {
      std::vector<stat_uint_t>::iterator iter = count_.begin();

      for (; iter != count_.end(); ++iter)
      {
        atomic_exchange(&(*iter), 0);
      }
    }

    void StatEntry<std::string, std::string>::add_sub_key(const std::string& sub_key)
    {
      sub_key_.push_back(sub_key);
      count_.push_back(0);
    }

      int StatEntry<std::string, std::string>::update(std::vector<stat_int_t>& update_list, bool inc)
      {
        std::vector<std::string>::iterator siter = sub_key_.begin();
        std::vector<stat_uint_t>::iterator citer = count_.begin();
        std::vector<stat_int_t>::iterator iter = update_list.begin();

        for (; iter != update_list.end() && citer != count_.end() && siter != sub_key_.end();
            ++iter, ++citer, ++siter)
        {
          if (!(*siter).empty() && (*iter) != 0)
          {
            inc ? atomic_add((&(*citer)), (*iter)) : atomic_add((&(*citer)), (-(*iter)));
          }
        }
        return TFS_SUCCESS;
      }

      int StatEntry<std::string, std::string>::update(const std::string& sub_key, const stat_int_t value, bool inc)
      {
        std::vector<std::string>::const_iterator iter = std::find(sub_key_.begin(), sub_key_.end(), sub_key);
        int32_t iret = iter == sub_key_.end() ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          int32_t offset = iter - sub_key_.begin();
          if (!(*iter).empty() && value != 0 )
          {
            inc ? atomic_add((&(count_[offset])), value) : atomic_add((&(count_[offset])), (-value));
          }
        }
        return iret;
      }

      stat_uint_t StatEntry<std::string, std::string>::get_current_value(const std::string& sub_key)
      {
        stat_uint_t current_value = 0;
        std::vector<std::string>::const_iterator iter = std::find(sub_key_.begin(), sub_key_.end(), sub_key);
        int32_t iret = iter == sub_key_.end() ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          int32_t offset = iter - sub_key_.begin();
          if (!(*iter).empty())
          {
            current_value = count_[offset];
          }
        }

        return current_value;
      }

    int32_t StatEntry<std::string, std::string>::calculate_stat_entry_length()
    {
      int32_t length = 0;
      std::vector<std::string>::const_iterator iter = sub_key_.begin();
      std::vector<stat_uint_t>::const_iterator citer = count_.begin();

      for (; iter != sub_key_.end() && citer != count_.end(); ++iter, ++citer)
      {
        length += sizeof(int16_t);
        length += (*iter).length();
        length += STAT_INT_T_SIZE;
      }
      return length;
    }

    void StatEntry<std::string, std::string>::stat_entry_serialize(std::string& result)
    {
      std::vector<std::string>::iterator iter = sub_key_.begin();
      std::vector<stat_uint_t>::iterator citer = count_.begin();

      for (; iter != sub_key_.end() && citer != count_.end(); ++iter, ++citer)
      {
        std::ostringstream str;
        str << (*iter).c_str() << " : " << (*citer) << " ";
        result += str.str();
      }
    }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      StatManager<Key, SubKey, Entry>::StatManager()
      :destroy_(false),
      timer_(0)
    {

    }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      StatManager<Key, SubKey, Entry>::~StatManager()
      {

      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      bool StatManager<Key, SubKey, Entry>::is_init()
    {
      return (timer_ != 0 && !destroy_);
    }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      int StatManager<Key, SubKey, Entry>::initialize(tbutil::TimerPtr timer)
      {
        timer_ = timer;
        return TFS_SUCCESS;
      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      int StatManager<Key, SubKey, Entry>::wait_for_shut_down()
      {
        if (timer_ != 0)
        {
          STAT_MAP_ITER iter = maps_.begin();
          for (; iter != maps_.end(); ++iter)
          {
            iter->second->serialize();
            timer_->cancel(iter->second);
          }
          maps_.clear();
          timer_ = 0;
        }
        return TFS_SUCCESS;
      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      int StatManager<Key, SubKey, Entry>::destroy()
      {
        destroy_ = true;
        return TFS_SUCCESS;
      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      int StatManager<Key, SubKey, Entry>::reset_schedule_interval(const int64_t schedule_interval_us)
      {
        if (is_init())
        {
          for (STAT_MAP_ITER iter = maps_.begin(); iter != maps_.end(); ++iter)
          {
            if (schedule_interval_us > 0)
            {
              timer_->cancel(iter->second);
              timer_->scheduleRepeated(iter->second, tbutil::Time::microSeconds(schedule_interval_us));
            }
          }
        }
        return TFS_SUCCESS;
      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      int StatManager<Key, SubKey, Entry>::add_entry(const EntryPtr& entry, int64_t schedule_interval_us)
      {
        if (is_init())
        {
          STAT_MAP_ITER iter = maps_.find(entry->get_key());
          if (iter != maps_.end())
          {
            TBSYS_LOG(WARN, "%s", "entry is exist");
            return TFS_SUCCESS;
          }
          std::pair<STAT_MAP_ITER, bool> res =
            maps_.insert(value_type(entry->get_key(), entry));
          if (!res.second)
          {
            TBSYS_LOG(ERROR, "%s", "add entry failed");
            return EXIT_GENERAL_ERROR;
          }

          if (schedule_interval_us > 0)
          {
            timer_->scheduleRepeated(entry, tbutil::Time::microSeconds(schedule_interval_us));
          }
        }
        return TFS_SUCCESS;
      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      int StatManager<Key, SubKey, Entry>::remove_entry(const Key& key)
      {
        /*STAT_MAP_ITER iter = maps_.find(key);
          if (iter == maps_.end())
          {
          TBSYS_LOG(ERROR, "%s", "'stat entry' not found in maps");
          return TFS_SUCCESS;
          }
          timer_->cancel(iter->second);
          maps_.erase(iter);*/
        return TFS_SUCCESS;
      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
    int64_t StatManager<Key, SubKey, Entry>::get_stat_value(const Key& key, const SubKey& sub_key)
    {
      int64_t current_value = 0;
      if (is_init())
      {
        STAT_MAP_ITER iter = maps_.find(key);
        if (maps_.end() == iter)
        {
          TBSYS_LOG(ERROR, "%s", "'stat entry' not found in maps");
        }
        current_value = iter->second->get_current_value(sub_key);
      }
      return current_value;
    }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      int StatManager<Key, SubKey, Entry>::update_entry(const Key& key, std::vector<stat_int_t>& update_list, bool inc)
      {
        if (is_init())
        {
          STAT_MAP_ITER iter = maps_.find(key);
          if (iter == maps_.end())
          {
            TBSYS_LOG(ERROR, "%s", "'stat entry' not found in maps");
            return EXIT_GENERAL_ERROR;
          }
          return iter->second->update(update_list, inc);
        }
        return EXIT_GENERAL_ERROR;
      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      int StatManager<Key, SubKey, Entry>::update_entry(const Key& key, const SubKey& subkey, const stat_int_t value, bool inc)
      {
        if (is_init())
        {
          STAT_MAP_ITER iter = maps_.find(key);
          if (iter == maps_.end())
          {
            TBSYS_LOG(ERROR, "%s", "'stat entry' not found in maps");
            return EXIT_GENERAL_ERROR;
          }
          return iter->second->update(subkey, value, inc);
        }
        return EXIT_GENERAL_ERROR;
      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      void StatManager<Key, SubKey, Entry>::serialize(const Key& key) const
      {
        if (is_init())
        {
          STAT_MAP_ITER iter = maps_.find(key);
          if (iter == maps_.end())
          {
            TBSYS_LOG(ERROR, "%s", "'stat entry' not found in maps");
            return;
          }
          iter->second->serialize();
        }
      }

    template<typename Key, typename SubKey, template<typename T, typename T2> class Entry>
      void StatManager<Key, SubKey, Entry>::serialize(const Key& key, char* data, int64_t& pos, int64_t length)
      {
        if (!destroy_)
        {
          STAT_MAP_ITER iter = maps_.find(key);
          if (iter == maps_.end())
          {
            TBSYS_LOG(ERROR, "%s", "'stat entry' not found in maps");
            return;
          }
          iter->second->serialize(data, pos, length);
        }
      }
  }
}

#endif
