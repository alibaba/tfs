#!/bin/sh

TFS_HOME="`cd ..; pwd`"
CS_CONF=$TFS_HOME/conf/cs.conf
SYNC_EXEC=$TFS_HOME/bin/sync_by_blk
LOG_LEVEL=debug

work_dir=`grep ^work_dir $CS_CONF | awk -F = '{print $2}' | sed 's/^ *//'`
master_ns_ip=`grep ^master_ns_ip $CS_CONF | awk -F = '{print $2}' | sed 's/^ *//'`
master_ns_port=`grep ^master_ns_port $CS_CONF | awk -F = '{print $2}' | sed 's/^ *//'`
slave_ns_ip=`grep ^slave_ns_ip $CS_CONF | awk -F = '{print $2}' | sed 's/^ *//'`
slave_ns_port=`grep ^slave_ns_port $CS_CONF | awk -F = '{print $2}' | sed 's/^ *//'`
master_ns=$master_ns_ip:$master_ns_port
slave_ns=$slave_ns_ip:$slave_ns_port
log_dir=$work_dir/checkserver_$1

next_day()
{
  oneday=`date -d $1 +%s`
  diff=`echo $oneday+86400 | bc`
  next=`date -d "1970-01-01 UTC $diff seconds" +"%Y%m%d"`
  echo $next
}

usage()
{
  echo "$0 index(eg:1) daytime(eg:20120502)"
  exit 1
}

do_sync()
{
  file_list=`ls $log_dir`
  for name in $file_list
    do 
      # echo $name
      if [[ $name == *$2* ]]; then
        # sync to slave & not empty
        if [[ $name == *slave* ]] && [ -s $log_dir/$name ]; then
          nextday=`next_day $2`
          cmd="$SYNC_EXEC -s $master_ns -d $slave_ns -f $log_dir/$name -m
          $nextday -l $LOG_LEVEL"
            echo  $cmd
            $cmd  
        # sync to master & not empty
        elif [[ $name == *master* ]] && [ -s $log_dir/$name ]; then
          nextday=`next_day $2`
          cmd="$SYNC_EXEC -s $slave_ns -d $master_ns -f $log_dir/$name -m
          $nextday -l $LOG_LEVEL"
            echo  $cmd
            $cmd
        fi
      fi
    done
}

if [ 2 != $# ]; then
  usage
else
  do_sync $1 $2
fi



