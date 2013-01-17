/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: error_msg.h 996 2011-11-02 07:10:44Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_ERROR_MSG_H_
#define TFS_COMMON_ERROR_MSG_H_

#include <stdint.h>
namespace tfs
{
  namespace common
  {
    const int32_t EXIT_GENERAL_ERROR = -1000;
    const int32_t EXIT_CONFIG_ERROR = -1001;
    const int32_t EXIT_UNKNOWN_MSGTYPE = -1002;
    const int32_t EXIT_INVALID_ARGU = -1003;
    const int32_t EXIT_ALL_SEGMENT_ERROR = -1004;
    // defined in define.h for outside use
    // static const int EXIT_INVALIDFD_ERROR = -1005;
    static const int EXIT_NOT_INIT_ERROR = -1006;
    static const int EXIT_INVALID_ARGU_ERROR = -1007;
    static const int32_t EXIT_NOT_PERM_OPER = -1008;
    static const int32_t EXIT_NOT_OPEN_ERROR = -1009;
    static const int32_t EXIT_CHECK_CRC_ERROR = -1010;
    static const int32_t EXIT_SERIALIZE_ERROR = -1011;
    static const int32_t EXIT_DESERIALIZE_ERROR = -1012;
    const int32_t EXIT_ACCESS_PERMISSION_ERROR = -1013; //access permission error
    const int32_t EXIT_SYSTEM_PARAMETER_ERROR = -1014; //system parameter error
    const int32_t EXIT_UNIQUE_META_NOT_EXIST = -1015;
    const int32_t EXIT_PARAMETER_ERROR = -1016;//fuction paameter error
    const int32_t EXIT_MMAP_FILE_ERROR = -1017;//mmap file failed
    const int32_t EXIT_LRU_VALUE_NOT_EXIST = -1018;//lru value not found by key
    const int32_t EXIT_LRU_VALUE_EXIST = -1019;//lru value existed
    const int32_t EXIT_CHANNEL_ID_INVALID = -1020;//channel id invalid
    const int32_t EXIT_DATA_PACKET_TIMEOUT = -1021;//data packet timeout
    const int32_t EXIT_LRU_BUCKET_NOT_EXIST = -1022;//lru bucket not found by key
    const int32_t EXIT_SERVICE_SHUTDOWN = -1023;
    const int32_t EXIT_ELEMENT_EXIST = -1024;

    const int32_t EXIT_FILE_OP_ERROR = -2000;
    const int32_t EXIT_OPEN_FILE_ERROR = -2001;
    const int32_t EXIT_INVALID_FD = -2002;
    const int32_t EXIT_RECORD_SIZE_ERROR = -2003;
    const int32_t EXIT_READ_FILE_ERROR = -2004;
    const int32_t EXIT_WRITE_FILE_ERROR = -2005;
    const int32_t EXIT_FILESYSTEM_ERROR = -2006;
    const int32_t EXIT_FILE_FORMAT_ERROR = -2007;
    const int32_t EXIT_SLOTS_OFFSET_SIZE_ERROR = -2008;
    const int32_t EXIT_FILE_BUSY_ERROR = -2009;

    const int32_t EXIT_NETWORK_ERROR = -3000;
    const int32_t EXIT_IOCTL_ERROR = -3001;
    const int32_t EXIT_CONNECT_ERROR = -3002;
    const int32_t EXIT_SENDMSG_ERROR = -3003;
    const int32_t EXIT_RECVMSG_ERROR = -3004;
    const int32_t EXIT_TIMEOUT_ERROR = -3005;
    const int32_t EXIT_WAITID_EXIST_ERROR = -3006;//waitid exist error
    const int32_t EXIT_WAITID_NOT_FOUND_ERROR = -3007;//waitid not found in waitid set
    const int32_t EXIT_SOCKET_NOT_FOUND_ERROR = -3008;//socket nof found in socket map
    const int32_t EXIT_SEND_RECV_MSG_COUNT_ERROR = -3009;

    const int32_t EXIT_TFS_ERROR = -5000;
    const int32_t EXIT_NO_BLOCK = -5001;
    const int32_t EXIT_NO_DATASERVER = -5002;
    const int32_t EXIT_BLOCK_NOT_FOUND = -5003;
    const int32_t EXIT_DATASERVER_NOT_FOUND = -5004;
    const int32_t EXIT_CANNOT_GET_LEASE = -5005;// lease not found
    const int32_t EXIT_COMMIT_ERROR = -5006;
    const int32_t EXIT_LEASE_EXPIRED = -5007;
    const int32_t EXIT_BINLOG_ERROR = -5008;
    const int32_t EXIT_NO_REPLICATE = -5009;
    const int32_t EXIT_BLOCK_BUSY = -5010;
    const int32_t EXIT_UPDATE_BLOCK_INFO_VERSION_ERROR = -5011;//update block information version error
    const int32_t EXIT_ACCESS_MODE_ERROR = -5012;//access mode error
    const int32_t EXIT_PLAY_LOG_ERROR = -5013;//play log error
    const int32_t EXIT_NAMESERVER_ONLY_READ = -5014;//current nameserver only read
    const int32_t EXIT_BLOCK_ALREADY_EXIST = -5015;//current block already exist
    const int32_t EXIT_CREATE_BLOCK_BY_ID_ERROR = -5016;//create block by block id failed
    const int32_t EIXT_SERVER_OBJECT_NOT_FOUND = -5017;//server object not found in XXX
    const int32_t EXIT_UPDATE_RELATION_ERROR = -5018;//update relation error
    const int32_t EXIT_DISCARD_NEWBLK_ERROR  = -5019;//nameserver in safe_mode_time, discard newblk packet
    const int32_t EXIT_BLOCK_REPLICATE_EXIST = -5020;//通过工具加载block时，block的备份已经存在
    const int32_t EXIT_CHOOSE_CREATE_BLOCK_TARGET_SERVER_ERROR = -5021;
    const int32_t EXIT_ADD_TASK_ERROR = -5022;
    const int32_t EXIT_ADD_NEW_BLOCK_ERROR = -5023;
    const int32_t EXIT_TASK_EXIST_ERROR = -5024;
    const int32_t EXIT_TASK_NO_EXIST_ERROR = -5025;
    const int32_t EXIT_CREATE_BLOCK_SEND_MSG_ERROR = -5026;
    const int32_t EXIT_RENEW_LEASE_ERROR = -5027;
    const int32_t EXIT_ROLE_ERROR = -5028;
    const int32_t EXIT_UPDATE_BLOCK_MISSING_ERROR = -5029;
    const int32_t EXIT_EXECUTE_TASK_ERROR = -5030;
    const int32_t EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR = -5031;
    const int32_t EXIT_BLOCK_ID_INVALID_ERROR = -5032;
    const int32_t EXIT_BLOCK_WRITING_ERROR = -5033;
    const int32_t EXIT_MOVE_OR_REPLICATE_ERROR = -5034;
    const int32_t EXIT_BUILD_RELATION_ERROR = -5035;//build relation error
    const int32_t EXIT_BLOCK_NO_WRITABLE = -5043;//

    const int32_t EXIT_WRITE_OFFSET_ERROR = -8001; // write offset error
    const int32_t EXIT_READ_OFFSET_ERROR = -8002; // read offset error
    const int32_t EXIT_BLOCKID_ZERO_ERROR = -8003; // block id is zero, fatal error
    const int32_t EXIT_BLOCK_EXHAUST_ERROR = -8004; // block is used up, fatal error
    const int32_t EXIT_PHYSICALBLOCK_NUM_ERROR = -8005; // need extend too much physcial block when extend block
    const int32_t EXIT_NO_LOGICBLOCK_ERROR = -8006; // can't find logic block
    const int32_t EXIT_POINTER_NULL = -8007; // input point is null
    const int32_t EXIT_CREATE_FILEID_ERROR = -8008; // cat find unused fileid in limited times
    const int32_t EXIT_BLOCKID_CONFLICT_ERROR = -8009; // block id conflict
    const int32_t EXIT_BLOCK_EXIST_ERROR = -8010; // LogicBlock already Exists
    const int32_t EXIT_COMPACT_BLOCK_ERROR = -8011; // compact block error
    const int32_t EXIT_DISK_OPER_INCOMPLETE = -8012; // read or write length is less than required
    const int32_t EXIT_DATA_FILE_ERROR = -8013; // datafile is NULL  / crc / getdata error
    const int32_t EXIT_DATAFILE_OVERLOAD = -8014; // too much data file
    const int32_t EXIT_DATAFILE_EXPIRE_ERROR = -8015; // data file is expired
    const int32_t EXIT_FILE_INFO_ERROR = -8016; // file flag or id error when read file
    const int32_t EXIT_RENAME_FILEID_SAME_ERROR = -8017; // fileid is same in rename file
    const int32_t EXIT_FILE_STATUS_ERROR = -8018; // file status error(in unlinkfile)
    const int32_t EXIT_FILE_ACTION_ERROR = -8019; // action is not defined(in unlinkfile)
    const int32_t EXIT_FS_NOTINIT_ERROR = -8020; // file system is not inited
    const int32_t EXIT_BITMAP_CONFLICT_ERROR = -8021; // file system's bit map conflict
    const int32_t EXIT_PHYSIC_UNEXPECT_FOUND_ERROR = -8022; // physical block is already exist in file system
    const int32_t EXIT_BLOCK_SETED_ERROR = -8023;
    const int32_t EXIT_INDEX_ALREADY_LOADED_ERROR = -8024; // index is loaded when create or load
    const int32_t EXIT_META_NOT_FOUND_ERROR = -8025; // meta not found in index
    const int32_t EXIT_META_UNEXPECT_FOUND_ERROR = -8026; // meta found in index when insert
    const int32_t EXIT_META_OFFSET_ERROR = -8027; // require offset is out of index size
    const int32_t EXIT_BUCKET_CONFIGURE_ERROR = -8028; // bucket size is conflict with before
    const int32_t EXIT_INDEX_UNEXPECT_EXIST_ERROR = -8029; // index already exist when create index
    const int32_t EXIT_INDEX_CORRUPT_ERROR = -8030; // index is corrupted, and index is created
    const int32_t EXIT_BLOCK_DS_VERSION_ERROR = -8031; // ds version error
    const int32_t EXIT_BLOCK_NS_VERSION_ERROR = -8332; // ns version error
    const int32_t EXIT_PHYSIC_BLOCK_OFFSET_ERROR = -8033; // offset is out of physical block size
    const int32_t EXIT_READ_FILE_SIZE_ERROR = -8034; // file size is little than fileinfo
    const int32_t EXIT_DS_CONNECT_ERROR = -8035; // connect to ds fail
    const int32_t EXIT_BLOCK_CHECKER_OVERLOAD = -8036; // too much block checker
    const int32_t EXIT_FALLOCATE_NOT_IMPLEMENT = -8037; // fallocate is not implement
    const int32_t EXIT_SYNC_FILE_ERROR = -8038;//sync file failed
    const int32_t EXIT_HALF_BLOCK_ERROR = -8039;  // half state block

    const int32_t EXIT_SESSION_EXIST_ERROR = -9001;
    const int32_t EXIT_SESSIONID_INVALID_ERROR = -9002;
    const int32_t EXIT_APP_NOTEXIST_ERROR = -9010;
    const int32_t EXIT_APPID_PERMISSION_DENY = -9011;

    const int32_t EXIT_SYSTEM_ERROR = -10000;
    const int32_t EXIT_REGISTER_OPLOG_SYNC_ERROR = -12000;
    const int32_t EXIT_MAKEDIR_ERROR = -13000;

    const int32_t EXIT_UNKNOWN_SQL_ERROR= -14000;
    const int32_t EXIT_TARGET_EXIST_ERROR = -14001;
    const int32_t EXIT_PARENT_EXIST_ERROR = -14002;
    const int32_t EXIT_DELETE_DIR_WITH_FILE_ERROR = -14003;
    const int32_t EXIT_VERSION_CONFLICT_ERROR = -14004;
    const int32_t EXIT_NOT_CREATE_ERROR = -14005;
    const int32_t EXIT_CLUSTER_ID_ERROR = -14006;
    const int32_t EXIT_FRAG_META_OVERFLOW_ERROR = -14007;
    const int32_t EXIT_UPDATE_FRAG_INFO_ERROR = -14008;
    const int32_t EXIT_WRITE_EXIST_POS_ERROR = -14009;
    const int32_t EXIT_INVALID_FILE_NAME = -14010;
    const int32_t EXIT_MOVE_TO_SUB_DIR_ERROR = -14011;
    const int32_t EXIT_OVER_MAX_SUB_DIRS_COUNT = -14012;
    const int32_t EXIT_OVER_MAX_SUB_DIRS_DEEP = -14013;
    const int32_t EXIT_OVER_MAX_SUB_FILES_COUNT = -14014;

    const int32_t EXIT_REGISTER_ERROR = -15000;// server register fail
    const int32_t EXIT_REGISTER_EXIST_ERROR = -15001;// server register fail, server is existed
    const int32_t EXIT_REGISTER_NOT_EXIST_ERROR = -15002;// renew lease fail, server is not existed
    const int32_t EXIT_TABLE_VERSION_ERROR = -15003;//table version error
    const int32_t EXIT_BUCKET_ID_INVLAID= -15004;//bucket id invalid
    const int32_t EXIT_BUCKET_NOT_EXIST= -15005;//bucket not exist
    const int32_t EXIT_NEW_TABLE_NOT_EXIST= -15005;//new table not exist
    const int32_t EXIT_NEW_TABLE_INVALID = -15005;//new table invalid

  }
}
#endif //TFS_COMMON_ERRMSG_H_
