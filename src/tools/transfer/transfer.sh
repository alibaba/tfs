#!/bin/sh

USER=admin

BLK_ID_FILE=input_block
DEST_DS_FILE=dest_ds
IO_THRESHOLD=4096
THREAD_CNT=1

SIG_STOP=SIGTERM
SIG_SLOW=SIGUSR2
SIG_HIGH=SIGUSR1
SIG_KILL=SIGKILL

# main server
CMD_HOME=`dirname $(readlink -f $0)`
OP_LOG_FILE=${CMD_HOME}/transfer_op_log
DISPATCH_DS_FILE=$CMD_HOME/ds_list
SAVE_FILE_PATH=${CMD_HOME}/save
SAVE_BLK_FILE=$SAVE_FILE_PATH/${BLK_ID_FILE}
SAVE_DEST_DS_FILE=${SAVE_FILE_PATH}/${DEST_DS_FILE}
SPLIT_TOOL=${CMD_HOME}/split_block_tool
BIN_NAME=transfer_block
SRC_TRANSFER_BIN=${CMD_HOME}/${BIN_NAME}

# dispatch server
DEST_CMD_HOME=/home/$USER/transfer
TRANSFER_BIN=${DEST_CMD_HOME}/${BIN_NAME}
INPUT_BLK_ID=${DEST_CMD_HOME}/${BLK_ID_FILE}
INPUT_DEST_DS=${DEST_CMD_HOME}/${DEST_DS_FILE}
DEST_LOG_PATH=${DEST_CMD_HOME}/logs
LOG_FILE=${DEST_LOG_PATH}/tranblk.log
PID_FILE=${DEST_LOG_PATH}/tranblk.pid
SUC_BLK_ID=${INPUT_BLK_ID}.succ
FAIl_BLK_ID=${INPUT_BLK_ID}.fail


op_log ()
{
    echo "[ `date +"%F %T"` ] $*" >> $OP_LOG_FILE
}

print_usage ()
{
    echo "Usage: $0 [ dispatch blk_list_file ds_min_port ds_cnt | start source_ns_addr dest_ns_addr [ thread_cnt io_threshold ] | slow | high | check_suc | check_fail | check_speed | stop ]"
}

get_status ()
{
    if [ $1 -eq 0 ]
    then
        echo -n "SUCCESS"
    else
        echo -n "FAIL"
    fi
}

# $0 dispatch blk_list_file ds_min_port ds_cnt
dispatch ()
{
    if ! [ -f $1 ]
    then
        echo "block list file not exist"
        exit 1
    fi

    blk_file=$1
    ds_cnt=`wc -l $DISPATCH_DS_FILE | awk '{print $1}'`
    blk_cnt=`wc -l $blk_file | awk '{print $1}'`

    rm -rf ${SAVE_FILE_PATH}
    mkdir -p ${SAVE_FILE_PATH}

    rm -f ${SAVE_BLK_FILE}_${ds}
    ${SPLIT_TOOL} ${DISPATCH_DS_FILE} ${blk_file} ${SAVE_FILE_PATH} "${BLK_ID_FILE}_"
    for ds in `cat $DISPATCH_DS_FILE`
    do

        for port in `seq $2 2 $(($2+$3*2-2))`
        do
            echo "$ds:$port" >> ${SAVE_DEST_DS_FILE}_$ds
        done

        ssh $USER@$ds "if ! [ -e $DEST_CMD_HOME ];then mkdir -p $DEST_CMD_HOME;fi; if ! [ -e $DEST_LOG_PATH ]; then mkdir -p $DEST_LOG_PATH;fi"
        scp ${SAVE_BLK_FILE}_${ds} $USER@$ds:${DEST_CMD_HOME}/${BLK_ID_FILE} && \
            scp ${SAVE_DEST_DS_FILE}_${ds} $USER@$ds:${DEST_CMD_HOME}/${DEST_DS_FILE} && \
        	scp ${SRC_TRANSFER_BIN} $USER@$ds:${TRANSFER_BIN}

        op_log "dispatch block id to server $ds `get_status $?`, block count: `wc -l ${SAVE_BLK_FILE}_$ds | awk '{print $1}'`, ds count: $3, ds port start from: $2"
    done
}

# $0 start source_ns dest_ns [ thread_cnt io ]
start_transfer()
{
    if [ -n "$3" ] && [ $3 -gt 0 ] >/dev/null
    then
        THREAD_CNT=$3
    fi

    if [ -n "$4" ] &&  [ $4 -gt 1 ] >/dev/null
    then
        IO_THRESHOLD=$4
    fi

    for ds in `cat $DISPATCH_DS_FILE`
    do
        echo -n "$ds: "
        ssh -o ConnectTimeout=3 $USER@$ds \
            "$TRANSFER_BIN -s $1 -n $2 -f $INPUT_DEST_DS -b $INPUT_BLK_ID -w $IO_THRESHOLD -t $THREAD_CNT -l $LOG_FILE -p $PID_FILE -d && ps -C $BIN_NAME -o pid="
        op_log "start transfer on server $ds `get_status $?`, source ns: $1, dest ns: $2, thread count: $THREAD_CNT, io threshold: $IO_THRESHOLD"
    done
}

change()
{
    case $1 in
        high)
            sig=$SIG_HIGH
            op="HIGH"
            ;;
        slow)
            sig=$SIG_SLOW
            op="SLOW"
            ;;
    esac

    for ds in `cat $DISPATCH_DS_FILE`
    do
        ssh -o ConnectTimeout=3 $USER@$ds 'kill -s '$sig' `ps -C '${BIN_NAME}' -o pid=` 2>/dev/null'
        op_log "$op transfer on $ds `get_status $?`"
    done
}

stop_transfer()
{
    for ds in `cat $DISPATCH_DS_FILE`
    do
        ssh -o ConnectTimeout=3 $USER@$ds 'kill -s '$SIG_STOP' `ps -C '$BIN_NAME' -o pid=` 2>/dev/null'
        op_log "stop tranfer on $ds `get_status $?`"
    done
}

check()
{
    case $1 in
        suc)
            check_file=$SUC_BLK_ID
            ;;
        fail)
            check_file=$FAIl_BLK_ID
            ;;
        speed)
            check_file=$LOG_FILE
            ;;
    esac

    for ds in `cat $DISPATCH_DS_FILE`
    do
        pid=`ssh -o ConnectTimeout=3 $USER@$ds "ps -C $BIN_NAME -o pid="`
        if [ $? -eq 0 ]
        then
            msg="RUNNING"
        else
            msg="NOT RUNNING"
        fi
        if [ $1 = "speed" ]
        then
            status="$ds tranfer is $msg, "`ssh -o ConnectTimeout=3 $USER@$ds 'grep speed '$check_file' | tail -1 | awk -F "read data," '\''{print $2 }'\'`
        else
            block_id=`ssh -o ConnectTimeout=3 $USER@$ds "tail -1 $check_file"`
            block_cnt=`ssh -o ConnectTimeout=3 $USER@$ds 'wc -l '$check_file' | awk '\''{print $1}'\'`
            status="$ds transfer is $msg, current $1 block id: $block_id, count: $block_cnt"
        fi
        echo $status
        op_log $status
    done
}


####################
if ! [ -f $DISPATCH_DS_FILE ]
then
    echo "ds list file not found . exit"
    exit 1
fi

case "$1" in
    dispatch)
    if [ $# -lt 4 ]
    then
        print_usage
    else
        dispatch $2 $3 $4
    fi
    ;;
    start)
        if [ $# -lt 3 ]
        then
            print_usage
        else
            start_transfer $2 $3 $4 $5
        fi
        ;;
    slow)
        change slow
        ;;
    high)
        change high
        ;;
    check_suc)
        check suc
        ;;
    check_fail)
        check fail
        ;;
    check_speed)
        check speed
        ;;
    stop)
        stop_transfer
        ;;
    *)
        print_usage
esac
####################
