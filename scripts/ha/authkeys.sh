(echo -ne "auth 1\n1 sha1 ";dd if=/dev/urandom bs=512 count=1 | openssl md5) > authkeys.tmp
cp -f ha.cf /etc/ha.d/
cp -f authkeys.tmp /etc/ha.d/authkeys
chmod 600 /etc/ha.d/authkeys
chown root.root /etc/ha.d/authkeys
