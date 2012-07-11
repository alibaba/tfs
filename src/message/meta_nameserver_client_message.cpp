/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_nameserver_client_message.cpp 864 2011-09-29 01:54:18Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "meta_nameserver_client_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    BaseMetaParameter::BaseMetaParameter() :
      app_id_(0), user_id_(0),
      version_(common::INVALID_TABLE_VERSION),
      file_path_()
    {
    }
    BaseMetaParameter::~BaseMetaParameter()
    {
    }
    int BaseMetaParameter::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;

      ret = input.get_int64(&app_id_);
      TBSYS_LOG(DEBUG, "app_id: %ld", app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&version_);
      }
      return ret;
    }
    int64_t BaseMetaParameter::length() const
    {
      return 2 * common::INT64_SIZE + common::Serialization::get_string_length(file_path_);
    }
    int BaseMetaParameter::serialize(Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = output.set_int64(app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(version_);
      }
      return ret;
    }

    FilepathActionMessage::FilepathActionMessage() :
      BaseMetaParameter(), new_file_path_(), action_(NON_ACTION)
    {
      _packetHeader._pcode = FILEPATH_ACTION_MESSAGE;
    }

    FilepathActionMessage::~FilepathActionMessage()
    {

    }

    int FilepathActionMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;

      ret = BaseMetaParameter::deserialize(input);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_string(new_file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(reinterpret_cast<int8_t*>(&action_));
      }
      return ret;
    }

    int64_t FilepathActionMessage::length() const
    {
      return BaseMetaParameter::length() + common::Serialization::get_string_length(new_file_path_) + common::INT8_SIZE;
    }

    int FilepathActionMessage::serialize(Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = BaseMetaParameter::serialize(output);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_string(new_file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(action_);
      }
      return ret;
    }

    WriteFilepathMessage::WriteFilepathMessage() :
      BaseMetaParameter()
    {
      _packetHeader._pcode = WRITE_FILEPATH_MESSAGE;
    }

    WriteFilepathMessage::~WriteFilepathMessage()
    {

    }

    int WriteFilepathMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;

      ret = BaseMetaParameter::deserialize(input);
      if (TFS_SUCCESS == ret)
      {
        ret = frag_info_.deserialize(input);
      }
      return ret;
    }

    int64_t WriteFilepathMessage::length() const
    {
      return BaseMetaParameter::length() + frag_info_.get_length();
    }

    int WriteFilepathMessage::serialize(Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = BaseMetaParameter::serialize(output);
      if (TFS_SUCCESS == ret)
      {
        ret = frag_info_.serialize(output);
      }
      return ret;
    }


    ReadFilepathMessage::ReadFilepathMessage() :
      BaseMetaParameter(), offset_(0), size_(0)
    {
      _packetHeader._pcode = READ_FILEPATH_MESSAGE;
    }

    ReadFilepathMessage::~ReadFilepathMessage()
    {

    }

    int ReadFilepathMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;

      ret = BaseMetaParameter::deserialize(input);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&size_);
      }
      return ret;
    }

    int64_t ReadFilepathMessage::length() const
    {
      return BaseMetaParameter::length() + 2 * common::INT64_SIZE;
    }

    int ReadFilepathMessage::serialize(Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = BaseMetaParameter::serialize(output);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(size_);
      }
      return ret;
    }

    RespReadFilepathMessage::RespReadFilepathMessage() :
      still_have_(0)
    {
      _packetHeader._pcode = RESP_READ_FILEPATH_MESSAGE;
    }

    RespReadFilepathMessage::~RespReadFilepathMessage()
    {

    }

    int RespReadFilepathMessage::deserialize(Stream& input)
    {
      int ret = TFS_ERROR;

      ret = input.get_int8(reinterpret_cast<int8_t*>(&still_have_));

      if (TFS_SUCCESS == ret)
      {
        ret = frag_info_.deserialize(input);
      }
      return ret;
    }

    int64_t RespReadFilepathMessage::length() const
    {
      return INT8_SIZE + frag_info_.get_length();
    }

    int RespReadFilepathMessage::serialize(Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = output.set_int8(still_have_);
      if (TFS_SUCCESS == ret)
      {
        ret = frag_info_.serialize(output);
      }
      return ret;
    }

    LsFilepathMessage::LsFilepathMessage() :
      BaseMetaParameter(), file_type_(NORMAL_FILE)
    {
      _packetHeader._pcode = LS_FILEPATH_MESSAGE;
    }

    LsFilepathMessage::~LsFilepathMessage()
    {
    }

    int64_t LsFilepathMessage::length() const
    {
      return BaseMetaParameter::length() + INT64_SIZE + INT8_SIZE;
    }

    int LsFilepathMessage::serialize(common::Stream& output) const
    {
      int ret = TFS_SUCCESS;
      ret = output.set_int64(app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(pid_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(static_cast<char>(file_type_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(version_);
      }
      return ret;
    }

    int LsFilepathMessage::deserialize(common::Stream& input)
    {
      int ret = input.get_int64(&app_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&user_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&pid_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_string(file_path_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(reinterpret_cast<int8_t*>(&file_type_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&version_);
      }
      return ret;
    }

    RespLsFilepathMessage::RespLsFilepathMessage() :
      still_have_(false),
      version_(common::INVALID_TABLE_VERSION)
    {
      meta_infos_.clear();
      _packetHeader._pcode = RESP_LS_FILEPATH_MESSAGE;
    }

    RespLsFilepathMessage::~RespLsFilepathMessage()
    {

    }
    int64_t RespLsFilepathMessage::length() const
    {
      int64_t len = INT8_SIZE + INT_SIZE;
      for (std::vector<MetaInfo>::const_iterator it = meta_infos_.begin();
           it != meta_infos_.end(); it++)
      {
        len += it->length();
      }
      return len;
    }

    int RespLsFilepathMessage::serialize(Stream& output) const
    {
      int ret = output.set_int8(still_have_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(meta_infos_.size());
      }
      if (TFS_SUCCESS == ret)
      {
        for (std::vector<MetaInfo>::const_iterator it = meta_infos_.begin();
             TFS_SUCCESS == ret && it != meta_infos_.end(); it++)
        {
          ret = it->serialize(output);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(version_);
      }
      return ret;
    }

    int RespLsFilepathMessage::deserialize(Stream& input)
    {
      int ret = input.get_int8(reinterpret_cast<int8_t*>(&still_have_));
      int32_t count = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&count);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < count; i++)
        {
          MetaInfo file_meta_info;
          ret = file_meta_info.deserialize(input);
          if (ret != TFS_SUCCESS)
          {
            break;
          }
          meta_infos_.push_back(file_meta_info);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&version_);
      }
      return ret;
    }

  }
}
