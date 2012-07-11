#!/bin/bash

REAL_FILE=`readlink -f $0`
BASE_HOME=`dirname $REAL_FILE`


warn_echo()
{
  printf  "\033[36m $* \033[0m\n"
}

print_usage()
{
  warn_echo "Usage: $0 source_ns_ip dest_ns_ip ds_ip [file_name]"
  warn_echo "          source_ns_ip: the cluster ns ip of broken disk"
  warn_echo "            dest_ns_ip: peer cluster ns ip"
  warn_echo "              filename: block list to be sync, default 'tmp', optional"
}
rm_block()
{
  for block in $2
  do
    $BASE_HOME/tfstool -s $1 -i "removeblock $block"
  done
}

if [ $# -lt 3 ]
then
  print_usage
  exit 1
fi

if [ -n "$4" ]
then
  FILE_NAME="$4"
else
  FILE_NAME="tmp"
fi


#CONF_PATH='/home/admin/tfs/conf/tfs.conf'
CONF_PATH='tfs.conf'

#when the broken disk can provide service,
#find blocks which only have 1 copy
#when the broken disk can not provide service,
#get all block list, and find those has 0 copy,
#then replicate blocks from slave cluster
$BASE_HOME/blocktool -s $1 -d $3 > $FILE_NAME
if [ -s "$FILE_NAME" ]
then
  warn_echo "the disk has been damaged!!!\n block need to be sync >> $FILE_NAME"
else
  BLOCK_LIST=`$BASE_HOME/showssm -f $CONF_PATH -t 1 | awk '/0$/ {print $1}'`
  echo "$BLOCK_LIST" > $FILE_NAME
  if [ -s "$FILE_NAME" ]
  then
    rm_block "$1" "$BLOCK_LIST"
    $BASE_HOME/syncfile -s $2 -d $1 -B $FILE_NAME
    rm $FILE_NAME
  else
    echo "no block found to be sync!!!"
  fi
fi

