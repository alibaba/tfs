USER=admin

EXEC_NAME=sync_by_log
SCRIPT_NAME=sync
CMD_HOME=`dirname $(readlink -f $0)`
SOURCE_SYNC_LOG=${CMD_HOME}/${EXEC_NAME}
SOURCE_SYNC_SCRIPT=${CMD_HOME}/${SCRIPT_NAME}
DISPATCH_DS_FILE=$CMD_HOME/ds_list
TMP_FILE=$CMD_HOME/tmp_file


print_usage()
{
  echo "$0 source_ns_ip dest_ns_ip log_path [sync day] [force_flag] [modify_time] [thread_count]"
  echo "    sync day: the day of log to be synced, default is yesterday"
  echo "  force flag: yes or no, need strong consistency, default is no"
  echo " modify time: modify time, change after modify time will be ignored."
  echo "thread count: thread count, default is 2"
}

#$1 source addr
#$2 dest addr
#$3 log file path
#$4 log day
#$5 force flag
#$6 modify time
#$7 thread count
start_sync()
{
  for ds in `cat $DISPATCH_DS_FILE`
  do
    echo -n "$ds: "
    scp -oStrictHostKeyChecking=no ${SOURCE_SYNC_LOG} $USER@$ds:${DEST_SYNC_LOG}
    scp -oStrictHostKeyChecking=no ${SOURCE_SYNC_SCRIPT} $USER@$ds:${DEST_SYNC_SCRIPT}
    ssh -o ConnectTimeout=3 -oStrictHostKeyChecking=no $USER@$ds \
    "cd $3;" \
    "chmod +x ${DEST_SYNC_LOG};" \
    "chmod +x ${DEST_SYNC_SCRIPT};" \
    "nohup sh $DEST_SYNC_SCRIPT $1 $2 $4 $5 $6 $7 > $TMP_FILE 2>&1 &"
    echo "nohup sh $DEST_SYNC_SCRIPT $1 $2 $4 $5 $6 $7 > $TMP_FILE 2>&1 &"
    echo "start sync done"
  done
}

if ! [ -f $DISPATCH_DS_FILE ]
then
  echo "ds list file not found . exit"
  exit 1
fi

if [ $# -lt 3 ]
then
  print_usage
  exit 1
fi

DEST_SYNC_LOG=$3/${LOG_NAME}
DEST_SYNC_SCRIPT=$3/${SCRIPT_NAME}

if [ -z $4 ]
then
  DAY=`date -d yesterday +%Y%m%d`
else
  DAY=$4
fi

if [ -z $5 ]
then
  FORCE_FLAG=no
else
  FORCE_FLAG=$5
fi

if [ -z $6 ]
then
  MODIFY_TIME=`date -d tomorrow +%Y%m%d`
else
  MODIFY_TIME=$6
fi

if [ -z $7 ]
then
  THREAD_COUNT=2
else
  THREAD_COUNT=$7
fi


echo "start_sync $1 $2 $3 $DAY $FORCE_FLAG $MODIFY_TIME $THREAD_COUNT"
start_sync $1 $2 $3 $DAY $FORCE_FLAG $MODIFY_TIME $THREAD_COUNT
