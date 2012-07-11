AUTOMAKE_OPTIONS=foreign

SUBDIRS = src conf scripts  sql
pkgdata_datadir=${prefix}/logs
pkgdata_data_DATA=

DIST_SUBDIRS = src conf scripts tests sql
EXTRA_DIST=build.sh \
	   conf/Makefile.am Makefile.am sql/Makefile.am\
	   scripts/Makefile.am scripts/Makefile.in scripts/tfs.sh scripts/stfs.sh scripts/start_sync_log.sh scripts/sync.sh scripts/cs_sync.sh\
	   libtool ax_lib_mysql.m4 tcmalloc.m4\
	   packages/tfs.spec

TMP_DIR := /tmp/tfs-tmp.$(shell echo $$$$)
PREFIX := $(shell [ x$(PREFIX) = x ] && PREFIX=/usr/local/tfs-$(VERSION) && echo "$${PREFIX}")
rpms: dist-gzip packages/tfs.spec
	mkdir -p $(TMP_DIR)
	mkdir -p $(TMP_DIR)/BUILD
	mkdir -p $(TMP_DIR)/RPMS
	mkdir -p $(TMP_DIR)/SOURCES
	mkdir -p $(TMP_DIR)/SRPMS
	cp tfs-${VERSION}.tar.gz $(TMP_DIR)/SOURCES
	echo $(PREFIX)
	cd $(TMP_DIR)/BUILD; \
	  tar xfz $(TMP_DIR)/SOURCES/tfs-${VERSION}.tar.gz \
	  tfs-$(VERSION)/packages/tfs.spec
	rpmbuild --define "_topdir $(TMP_DIR)" \
	  --define "NAME $(PACKAGE)" \
	  --define "VERSION $(VERSION)" \
	  --define "_prefix $(PREFIX)" \
	  -ba $(TMP_DIR)/BUILD/tfs-$(VERSION)/packages/tfs.spec
	find $(TMP_DIR)/RPMS/ -name "*.rpm" -exec mv '{}' . \;
	rm -rf $(TMP_DIR)
