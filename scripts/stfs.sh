#!/bin/sh

TFS_HOME="`cd ..;pwd`"
TFS_CONF=${TFS_HOME}/conf/ds.conf
BIN_DIR=${TFS_HOME}/bin
CLEAR_BIN=${BIN_DIR}/clear_file_system
FORMAT_BIN=${BIN_DIR}/format_file_system
DS_CMD="${BIN_DIR}/dataserver -f ${TFS_CONF} -d -i"
WAIT_TIME=5

warn_echo()
{
    printf "\033[36m $* \033[0m\n"
}

fail_echo()
{
    printf "\033[31m $* ... CHECK IT\033[0m\n"
}

succ_echo()
{
    printf "\033[32m $* \033[0m\n"
}

print_usage()
{
    warn_echo "Usage: $0 [ format | clear ] ds_index "
    warn_echo "ds_index format : 2-4 OR 2,4,3 OR 2-4,5,7 OR '2-4 5,6'"
}

check_run()
{
    case $1 in
	ds)
	    run_pid=`ps -ef | egrep "${DS_CMD} +$2\b" | egrep -v 'egrep' | awk '{print $2}'`
	    ;;
	*)
	    exit 1
    esac

    if [ `echo "$run_pid" | wc -l` -gt 1 ]
    then
	echo -1
    elif [ -z "$run_pid" ]
    then
	echo 0
    else
	echo $run_pid
    fi
}

get_index()
{
    local ds_index=""
  # range index type
    range_index=`echo "$1" | egrep -o '[0-9]+-[0-9]+'` # ignore non-digit and '-' signal chars
    for index in $range_index
    do
	start_index=`echo $index | awk -F '-' '{print $1}'`
	end_index=`echo $index | awk -F '-' '{print $2}'`

	if [ $start_index -gt $end_index ]
	then
	    echo ""
	    exit 1;
	fi
	ds_index="$ds_index `seq $start_index $end_index`"
    done

  # individual index type
    in_index=`echo " $1 " | tr ',' ' '| sed 's/ /  /g' | egrep -o ' [0-9]+ '`
    ds_index="$ds_index $in_index"
    if [ -z "$range_index" ] && [ -z "$in_index" ]
    then
	echo ""
    else
	echo "$ds_index"
    fi
}

do_ds()
{
    case "$1" in
	format)
	    cmd="$FORMAT_BIN -f $TFS_CONF -i"
	    name="format ds"
	    ;;
	clear)
	    cmd="$CLEAR_BIN -f $TFS_CONF -i"
	    name="clear ds"
	    ;;
	*)
	    warn_echo "wrong argument for do_ds"
    esac


    local ds_index="`get_index "$2"`"
    if [ -z "$ds_index" ]
    then
	warn_echo "invalid range"
	print_usage
	exit 1
    fi

    local run_index=""
    local ready_index=""
    for i in $ds_index
    do
	ret_pid=`check_run ds $i`
	if [ $ret_pid -gt 0 ]
	then
	    kill -15 $ret_pid
	    warn_echo "dataserver $i is running, kill first ... "
	    run_index="$run_index $i"
	elif [ $ret_pid -eq 0 ]
	then
	    ready_index="$ready_index $i"
	else
	    fail_echo "more than one same dataserver $i is running"
	fi
    done

    if [ "$run_index" ]
    then
	sleep ${WAIT_TIME}
	for i in $run_index
	do
	    ret_pid=`check_run "ds" $i`
	    if [ $ret_pid -gt 0 ]
	    then
		fail_echo "dataserver $i is still RUNNING, FAIL to $name $i"
	    else
		ready_index="$ready_index $i"
	    fi
	done
    fi

    for i in $ready_index
    do
	ret=`$cmd $i 2>&1`
	if [ $? -eq 0 ] && [ -z "`echo $ret | egrep -i ERROR`" ]
	then
	    succ_echo "$name $i SUCCESSFULLY"
	else
	    fail_echo "$name $i FAIL"
	fi
	warn_echo "$ret"
    done

    exit 0
}

############

case "$1" in
    clear)
	do_ds "clear"  "$2"
	;;
    format)
	do_ds "format"  "$2"
	;;
    *)
	print_usage
esac

############
