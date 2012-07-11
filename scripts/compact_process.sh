#!/bin/bash

TFS_SUCCESS=0
TFS_ERROR=1

BASE_HOME=`dirname $(readlink -f $0)`
COMPACT_FAIL_BLK=$BASE_HOME/compact_fail_blk
#COMPACT_FAIL_BLK=$BASE_HOME/test_blk
TFSTOOL=$BASE_HOME/tfstool
ADMINTOOL=$BASE_HOME/admintool
DSTOOL=$BASE_HOME/ds_client
SYNCTOOL=$BASE_HOME/syncfile

LOG_PATH=$BASE_HOME/logs
LOG_FILE=$LOG_PATH/log_file
SYNC_LOG_FILE=$LOG_PATH/syncfile.log
ERROR_LOG=$LOG_PATH/error_log
TMP_FILE=$LOG_PATH/tmp_file
BLK_FILE=$LOG_PATH/blk_file
FAIL_BLK_FILE=$LOG_PATH/fail_blk_file
SUCC_BLK_FILE=$LOG_PATH/succ_blk_file

#$1 info
write_log()
{
  time=`date +"%Y-%m-%d %H:%M:%S"`
  echo "[$time] $1" >> $LOG_FILE
}

#$1 src ns addr
#$2 dest ns addr
#$3 block id
copy_block()
{
  write_log "copy block $3 from $1 to $2"
  modify_time=`date +"%Y%m%d%H%M%S"`
  echo $3 > $BLK_FILE
  echo "nohup $SYNCTOOL -s $1 -d $2 -t 2 -m $modify_time -B $BLK_FILE -l $SYNC_LOG_FILE &"
  nohup $SYNCTOOL -s $1 -d $2 -t 2 -m $modify_time -B $BLK_FILE -l $SYNC_LOG_FILE &
}

#$1 ns addr
#$2 block id
remove_block()
{
  iret=$TFS_SUCCESS
  ds_ip_list=`$TFSTOOL -s $1 -i "listblock $2" | egrep -o "th server.*$" | awk -F ': ' '{print $NF}'`
  if [ -n "$ds_ip_list" ]
  then
    for ds_ip in $ds_ip_list
    do
      write_log "remove block $2 from $ds_ip"
      $ADMINTOOL -s $1 -i "removeblk $2 $ds_ip" >$TMP_FILE 2>> $ERROR_LOG
      tmp_ret=`grep "success" $TMP_FILE | wc -l`
      if [ $tmp_ret -eq 0 ]
      then
        iret=$TFS_ERROR
        break
      fi
      $DSTOOL -d $ds_ip -i "remove_block $2" >$TMP_FILE 2>> $ERROR_LOG
      tmp_ret=`grep "success" $TMP_FILE | wc -l`
      if [ $tmp_ret -eq 0 ]
      then
        iret=$TFS_ERROR
        break
      fi
    done
    # make sure block collect has been moved
    if [ $iret -eq $TFS_SUCCESS ]
    then
      $ADMINTOOL -s $1 -i "removeblk $2 $ds_ip" 2>> $ERROR_LOG
    fi
  fi
  echo $iret
}

#$1 ns_addr
#$2 block_id
check_block_repl()
{
  ret=`$TFSTOOL -s $1 -i "listblock $2" | grep "replicas" | wc -l`
  echo $ret
}

#--------------------------------------------------------
#-------------------------main---------------------------

if [ $# -ne 2 ]
then
  echo "$0 master_ns_addr slave_ns_addr"
  exit
fi

MASTER_NS_ADDR=$1
SLAVE_NS_ADDR=$2

for block_id in `cat $COMPACT_FAIL_BLK`
do
  ret=`check_block_repl $MASTER_NS_ADDR $block_id`
  if [ $ret -eq 0 ]
  then
    copy_block $SLAVE_NS_ADDR $MASTER_NS_ADDR $block_id
  fi
  ret=`check_block_repl $MASTER_NS_ADDR $block_id`
  if [ $ret -eq 0 ]
  then
    echo $block_id >> $FAIL_BLK_FILE
    continue
  fi

  ret=`remove_block $SLAVE_NS_ADDR $block_id`
  if [ $ret -ne $TFS_SUCCESS ]
  then
    echo $block_id >> $FAIL_BLK_FILE
    continue
  fi
  copy_block $MASTER_NS_ADDR $SLAVE_NS_ADDR $block_id
  echo $block_id >> $SUCC_BLK_FILE
done

