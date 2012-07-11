pkgdata_datadir=${prefix}/scripts
pkgdata_data_DATA=stfs.sh tfs.sh start_sync_log.sh sync.sh cs_sync.sh

SUBDIRS = ha

DIST_SUBDIRS = ha

EXTRA_DIST=ha/authkeys.sh ha/deploy.sh ha/ha.cf ha/NameServer ha/nsdep.sh ha/ns.xml ha/RootServer ha/rsdep.sh ha/rs.xml

install-data-hook:
	mv ${DESTDIR}/${prefix}/scripts/stfs.sh ${DESTDIR}/${prefix}/scripts/stfs
	mv ${DESTDIR}/${prefix}/scripts/tfs.sh ${DESTDIR}/${prefix}/scripts/tfs
	mv ${DESTDIR}/${prefix}/scripts/sync.sh ${DESTDIR}/${prefix}/scripts/sync
	mv ${DESTDIR}/${prefix}/scripts/start_sync_log.sh ${DESTDIR}/${prefix}/scripts/start_sync_log
	mv ${DESTDIR}/${prefix}/scripts/cs_sync.sh ${DESTDIR}/${prefix}/scripts/cs_sync
	chmod u+x ${DESTDIR}/${prefix}/scripts/stfs
	chmod u+x ${DESTDIR}/${prefix}/scripts/tfs
	chmod u+x ${DESTDIR}/${prefix}/scripts/sync
	chmod u+x ${DESTDIR}/${prefix}/scripts/start_sync_log
	chmod u+x ${DESTDIR}/${prefix}/scripts/cs_sync
	sed -i 's#\(TFS_HOME=\).*$$#\1${prefix}#g' ${DESTDIR}/${prefix}/scripts/stfs ${DESTDIR}/${prefix}/scripts/tfs
