#!/bin/bash

REAL_FILE=`readlink -f $0`
BASE_HOME=`dirname $REAL_FILE`
FILE_NAME='total_file.log'

warn_echo()
{
  printf  "\033[36m $* \033[0m\n"
}

print_usage()
{
  warn_echo "Usage: $0 source_ns_ip dest_ns_ip [thread_count] [day] [modify_time]"
  warn_echo "          source_ns_ip: source ns ip"
  warn_echo "          dest_ns_ip:   dest ns ip"
  warn_echo "          thread_count: thread_count"
  warn_echo "          day:          the date of log to be sync, default is yesterday, ex, 20110303, optional"
  warn_echo "          modify_time:  modify_time"
}
#$1 day
next_day()
{
  oneday=`date -d $1 +%s`
  diff=`echo $oneday+86400 | bc`
  next=`date -d "1970-01-01 UTC $diff seconds" +"%Y%m%d"`
  echo $next
}
#$1: log time
#$2: day
check_date()
{
  next=`next_day $2`
  if [ $1 -gt $2'000100' ] && [ $1 -lt $next'000100' ]
  then
    echo 0
  else
    echo 1
  fi

}
#$1: op type: write|unlink
#$2: sync day
get_log_name()
{
  if [ "$1" = "write" ]
  then
    all_log=`ls write_stat_*.log*`
  elif [ "$1" = "unlink" ]
  then
    all_log=`ls dataserver_*.log* | egrep 'dataserver_[^_]*.log.*'`
  fi

  today=`date +"%Y%m%d"`
  for one in $all_log
  do
    log_time=`echo $one | awk -F '.' '{print $NF}'`
    if [ "$log_time" = 'log' ] && [ "$2" = "$today" ]
    then
      echo $one
    elif [ "$log_time" != 'log' ]
    then
      ret=`check_date $log_time $2`
      if [ $ret -eq 0 ]
      then
        echo $one
      fi
    fi
  done

}

delete_file()
{
  if [ -e "$1" ]
  then
    rm $1
  fi
}

#$1 op type: write|unlink
#$2 sync day
sync_file_by_log()
{
  if [ -e "$LOG_FILE" ]
  then
    mv $LOG_FILE $LOG_FILE'.'$2
  fi

  LOG_NAME=`get_log_name $1 $2`
  for log in $LOG_NAME
  do
    if [ $1 = "write" ]
    then
      cat $log >> $FILE_NAME
    elif [ $1 = "unlink" ]
    then
      ret=`grep "master unlink file success" $log`
      if [ -z "$ret" ]
      then
        ret=`grep "unlink file success.*master" $log`
        if [ "$ret" ]
        then
        grep "unlink file success.*master" $log | tr "[A-Z]" "[a-z]" | sed -e "s/ : /: /g" | sed -e "s/\(id: [0-9]*\) /\1, /g" >> $FILE_NAME
        fi
      else
        grep "master unlink file success" $log | tr "[A-Z]" "[a-z]" | sed -e "s/ : /: /g" | sed -e "s/\(id: [0-9]*\) /\1, /g" >> $FILE_NAME
      fi
    fi
  done
}

if [ $# -lt 2 ]
then
  print_usage
  exit 1
fi

if [ -z $3 ]
then
  DAY=`date -d yesterday +%Y%m%d`
else
  DAY=$3
fi

if [ -z $4 ]
then
  FORCE_FLAG=no
else
  FORCE_FLAG=$4
fi

if [ -z $5 ]
then
  MODIFY_TIME=`date -d tomorrow +%Y%m%d`
else
  MODIFY_TIME=$5
fi

if [ -z $6 ]
then
  THREAD_COUNT=2
else
  THREAD_COUNT=$6
fi

if [ -e "$FILE_NAME" ]
then
  rm $FILE_NAME -f
fi

sync_file_by_log "write" $DAY
sync_file_by_log "unlink" $DAY

if [ -e "$FILE_NAME" ]
then
  if [ "$FORCE_FLAG" = "no" ]
  then
    nohup $BASE_HOME/sync_by_log -s $1 -d $2 -f $FILE_NAME -m $MODIFY_TIME -t $THREAD_COUNT -l debug 2>&1 &
  else
    nohup $BASE_HOME/sync_by_log -s $1 -d $2 -f $FILE_NAME -m $MODIFY_TIME -e -t $THREAD_COUNT -l debug 2>&1 &
  fi
fi

