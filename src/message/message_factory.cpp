/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: message_factory.cpp 864 2011-09-29 01:54:18Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "message_factory.h"

namespace tfs
{
  namespace message
  {
    tbnet::Packet* MessageFactory::createPacket(int pcode)
    {
      tbnet::Packet* packet = BasePacketFactory::createPacket(pcode);
      if (NULL == packet)
      {
        int32_t real_pcode = pcode & 0xFFFF;
        switch (real_pcode)
        {
          case common::GET_BLOCK_INFO_MESSAGE:
            packet = new GetBlockInfoMessage();
            break;
          case common::SET_BLOCK_INFO_MESSAGE:
            packet = new  SetBlockInfoMessage();
            break;
          case common::BATCH_GET_BLOCK_INFO_MESSAGE:
            packet = new  BatchGetBlockInfoMessage();
            break;
          case common::BATCH_SET_BLOCK_INFO_MESSAGE:
            packet = new  BatchSetBlockInfoMessage();
            break;
          case common::CARRY_BLOCK_MESSAGE:
            packet = new  CarryBlockMessage();
            break;
          case common::SET_DATASERVER_MESSAGE:
            packet = new  SetDataserverMessage();
            break;
          case common::UPDATE_BLOCK_INFO_MESSAGE:
            packet = new  UpdateBlockInfoMessage();
            break;
          case common::BLOCK_WRITE_COMPLETE_MESSAGE:
            packet = new  BlockWriteCompleteMessage();
            break;
          case common::READ_DATA_MESSAGE:
            packet = new  ReadDataMessage();
            break;
          case common::RESP_READ_DATA_MESSAGE:
            packet = new  RespReadDataMessage();
            break;
          case common::FILE_INFO_MESSAGE:
            packet = new  FileInfoMessage();
            break;
          case common::RESP_FILE_INFO_MESSAGE:
            packet = new  RespFileInfoMessage();
            break;
          case common::WRITE_DATA_MESSAGE:
            packet = new  WriteDataMessage();
            break;
          case common::CLOSE_FILE_MESSAGE:
            packet = new  CloseFileMessage();
            break;
          case common::UNLINK_FILE_MESSAGE:
            packet = new  UnlinkFileMessage();
            break;
          case common::REPLICATE_BLOCK_MESSAGE:
            packet = new  ReplicateBlockMessage();
            break;
          case common::COMPACT_BLOCK_MESSAGE:
            packet = new  CompactBlockMessage();
            break;
          case common::GET_SERVER_STATUS_MESSAGE:
            packet = new  GetServerStatusMessage();
            break;
          /*case common::SUSPECT_DATASERVER_MESSAGE:
            packet = new  SuspectDataserverMessage();
            break;*/
          case common::RENAME_FILE_MESSAGE:
            packet = new  RenameFileMessage();
            break;
          case common::CLIENT_CMD_MESSAGE:
            packet = new  ClientCmdMessage();
            break;
          case common::CREATE_FILENAME_MESSAGE:
            packet = new  CreateFilenameMessage();
            break;
          case common::RESP_CREATE_FILENAME_MESSAGE:
            packet = new  RespCreateFilenameMessage();
            break;
          case common::ROLLBACK_MESSAGE:
            packet = new  RollbackMessage();
            break;
          case common::RESP_HEART_MESSAGE:
            packet = new  RespHeartMessage();
            break;
          case common::RESET_BLOCK_VERSION_MESSAGE:
            packet = new  ResetBlockVersionMessage();
            break;
          case common::BLOCK_FILE_INFO_MESSAGE:
            packet = new  BlockFileInfoMessage();
            break;
          case common::NEW_BLOCK_MESSAGE:
            packet = new  NewBlockMessage();
            break;
          case common::REMOVE_BLOCK_MESSAGE:
            packet = new  RemoveBlockMessage();
            break;
          case common::LIST_BLOCK_MESSAGE:
            packet = new  ListBlockMessage();
            break;
          case common::RESP_LIST_BLOCK_MESSAGE:
            packet = new  RespListBlockMessage();
            break;
          case common::BLOCK_RAW_META_MESSAGE:
            packet = new  BlockRawMetaMessage();
            break;
          case common::WRITE_RAW_DATA_MESSAGE:
            packet = new  WriteRawDataMessage();
            break;
          case common::WRITE_INFO_BATCH_MESSAGE:
            packet = new  WriteInfoBatchMessage();
            break;
          case common::BLOCK_COMPACT_COMPLETE_MESSAGE:
            packet = new  CompactBlockCompleteMessage();
            break;
          case common::READ_DATA_MESSAGE_V2:
            packet = new  ReadDataMessageV2();
            break;
          case common::RESP_READ_DATA_MESSAGE_V2:
            packet = new  RespReadDataMessageV2();
            break;
          case common::LIST_BITMAP_MESSAGE:
            packet = new  ListBitMapMessage();
            break;
          case common::RESP_LIST_BITMAP_MESSAGE:
            packet = new  RespListBitMapMessage();
            break;
          case common::RELOAD_CONFIG_MESSAGE:
            packet = new  ReloadConfigMessage();
            break;
          case common::READ_RAW_DATA_MESSAGE:
            packet = new  ReadRawDataMessage();
            break;
          case common::RESP_READ_RAW_DATA_MESSAGE:
            packet = new  RespReadRawDataMessage();
            break;
          case common::ACCESS_STAT_INFO_MESSAGE:
            packet = new  AccessStatInfoMessage();
            break;
          case common::READ_SCALE_IMAGE_MESSAGE:
            packet = new  ReadScaleImageMessage();
            break;
          case common::CRC_ERROR_MESSAGE:
            packet = new  CrcErrorMessage();
            break;
          case common::OPLOG_SYNC_MESSAGE:
            packet = new  OpLogSyncMessage();
            break;
          case common::OPLOG_SYNC_RESPONSE_MESSAGE:
            packet = new  OpLogSyncResponeMessage();
            break;
          case common::MASTER_AND_SLAVE_HEART_MESSAGE:
            packet = new  MasterAndSlaveHeartMessage();
            break;
          case common::MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE:
            packet = new  MasterAndSlaveHeartResponseMessage();
            break;
          case common::HEARTBEAT_AND_NS_HEART_MESSAGE:
            packet = new  HeartBeatAndNSHeartMessage();
            break;
          case common::ADMIN_CMD_MESSAGE:
            packet = new  AdminCmdMessage();
            break;
          case common::REMOVE_BLOCK_RESPONSE_MESSAGE:
            packet = new  RemoveBlockResponseMessage();
            break;
          case common::DUMP_PLAN_MESSAGE:
            packet = new  DumpPlanMessage();
            break;
          case common::DUMP_PLAN_RESPONSE_MESSAGE:
            packet = new  DumpPlanResponseMessage();
            break;
          case common::SHOW_SERVER_INFORMATION_MESSAGE:
            packet = new  ShowServerInformationMessage();
            break;
          case common::REQ_RC_LOGIN_MESSAGE:
            packet = new ReqRcLoginMessage();
            break;
          case common::RSP_RC_LOGIN_MESSAGE:
            packet = new RspRcLoginMessage();
            break;
          case common::REQ_RC_KEEPALIVE_MESSAGE:
            packet = new ReqRcKeepAliveMessage();
            break;
          case common::RSP_RC_KEEPALIVE_MESSAGE:
            packet = new RspRcKeepAliveMessage();
            break;
          case common::REQ_RC_LOGOUT_MESSAGE:
            packet = new ReqRcLogoutMessage();
            break;
          case common::REQ_RC_RELOAD_MESSAGE:
            packet = new ReqRcReloadMessage();
            break;
          case common::GET_DATASERVER_INFORMATION_MESSAGE:
            packet = new GetDataServerInformationMessage();
            break;
          case common::GET_DATASERVER_INFORMATION_RESPONSE_MESSAGE:
            packet = new GetDataServerInformationResponseMessage();
            break;
          case common::FILEPATH_ACTION_MESSAGE:
            packet = new FilepathActionMessage();
            break;
          case common::WRITE_FILEPATH_MESSAGE:
            packet = new WriteFilepathMessage();
            break;
          case common::READ_FILEPATH_MESSAGE:
            packet = new ReadFilepathMessage();
            break;
          case common::RESP_READ_FILEPATH_MESSAGE:
            packet = new RespReadFilepathMessage();
            break;
          case common::LS_FILEPATH_MESSAGE:
            packet = new LsFilepathMessage();
            break;
          case common::RESP_LS_FILEPATH_MESSAGE:
            packet = new RespLsFilepathMessage();
            break;
          case common::REQ_RT_MS_KEEPALIVE_MESSAGE:
            packet =  new RtsMsHeartMessage();
            break;
          case common::RSP_RT_MS_KEEPALIVE_MESSAGE:
            packet = new RtsMsHeartResponseMessage();
            break;
          case common::REQ_RT_RS_KEEPALIVE_MESSAGE:
            packet =  new RtsRsHeartMessage();
            break;
          case common::RSP_RT_RS_KEEPALIVE_MESSAGE:
            packet = new RtsRsHeartResponseMessage();
            break;
          case common::REQ_RT_GET_TABLE_MESSAGE:
            packet = new GetTableFromRtsMessage();
            break;
          case common::RSP_RT_GET_TABLE_MESSAGE:
            packet = new GetTableFromRtsResponseMessage();
            break;
          case common::REQ_RT_UPDATE_TABLE_MESSAGE:
            packet = new UpdateTableMessage();
            break;
          case common::RSP_RT_UPDATE_TABLE_MESSAGE:
            packet = new UpdateTableResponseMessage();
            break;
          case common::REQ_CALL_DS_REPORT_BLOCK_MESSAGE:
            packet = new CallDsReportBlockRequestMessage();
            break;
          case common::REQ_REPORT_BLOCKS_TO_NS_MESSAGE:
            packet = new ReportBlocksToNsRequestMessage();
            break;
          case common::RSP_REPORT_BLOCKS_TO_NS_MESSAGE:
            packet = new ReportBlocksToNsResponseMessage();
            break;
          case common::REQ_CHECK_BLOCK_MESSAGE:
            packet = new CheckBlockRequestMessage();
            break;
          case common::RSP_CHECK_BLOCK_MESSAGE:
            packet = new CheckBlockResponseMessage();
            break;
          default:
            TBSYS_LOG(ERROR, "pcode: %d not found in message factory", real_pcode);
            break;
        }
      }
      return packet;
    }
  }/** message **/
}/** tfs **/
