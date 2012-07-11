
export OCF_ROOT=/usr/lib
/usr/sbin/ocf-tester  -v \
-o basedir="/home/admin/tfs" \
-o pidfile="/home/admin/tfs/logs/nameserver.pid" \
-o user="admin" \
-o stop_timeout=100 \
-o montool="./nshamon" \
-o nsip="localhost" \
-o nsport="3100" \
/home/admin/tools/nsha/NameServer

