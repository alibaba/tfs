pkgdata_datadir=${prefix}/scripts/ha
pkgdata_data_DATA=authkeys.sh deploy.sh ha.cf NameServer RootServer rsdep.sh nsdep.sh ns.xml rs.xml

install-data-hook:
	mv ${DESTDIR}/${prefix}/scripts/ha/authkeys.sh ${DESTDIR}/${prefix}/scripts/ha/authkeys
	mv ${DESTDIR}/${prefix}/scripts/ha/deploy.sh ${DESTDIR}/${prefix}/scripts/ha/deploy
	mv ${DESTDIR}/${prefix}/scripts/ha/nsdep.sh ${DESTDIR}/${prefix}/scripts/ha/nsdep
	mv ${DESTDIR}/${prefix}/scripts/ha/rsdep.sh ${DESTDIR}/${prefix}/scripts/ha/rsdep
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/authkeys
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/deploy
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/nsdep
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/rsdep
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/NameServer
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/RootServer
