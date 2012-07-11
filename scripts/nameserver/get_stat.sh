#!/bin/bash

warn_echo()                                                                                                                  
{
  printf  "\033[36m $* \033[0m\n"
}
print_usage()                                                                                                                
{
  warn_echo "Usage: $0 interval log_file" 
}
if [ -z $1 ] || [ $1 -lt 0 ] || [ -z $2 ]
then
  print_usage                                                                                                                
  exit 1
fi

if [ -f "./$2" ]
then
  #echo " $2 has exist"
  mv $2 $2.`date +'%Y-%m-%d-%H-%M'`
fi


while true
do
  PID=`ps -C nameserver`
  if [ -z "$PID" ]
  then
    warn_echo "ERROR: no nameserver is running..."
    exit 1
  else
    #SYS=`sar -u | egrep "Average" -v| tail -n 1 | awk '{print $3}'`
    CPU=`top -n 1 | grep "Cpu(s):" | awk '{print $5}' | egrep -o "[0-9.]*?%" | sed -e "s/\%//g" | awk '{print 100-$0}'` 
    MEM=`ps -C nameserver -o rss= -o vsz= | tail -n 1`
    IO=`iostat | egrep -w 'sda' | awk '{print $2}'`
    NET=`sar -n DEV | egrep "Average" -v | tail -n 1 | awk '{print $3,$4,$5,$6}'`
    LOAD=`uptime | egrep -o "load average:.*$" | awk '{print $3}'| sed -e "s/\([0-9.]*\),/\1/g"`
    echo "CPU: $CPU, MEM:$MEM, IO:$IO, NETWORK: $NET, LOAD: $LOAD"  >> $2
    sleep $1
  fi
done

