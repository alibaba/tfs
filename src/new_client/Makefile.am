if WITH_UNIQUE_STORE
unique_store_source=tfs_unique_store.cpp tfs_unique_store.h unique_handler.h\
	            tair_unique_handler.cpp tair_unique_handler.h md5.h md5.cpp
trim_cmd=sed -i -e '/ifdef \+WITH_UNIQUE_STORE/{h;d}' -e '/endif/{x;/ifdef \+WITH_UNIQUE_STORE/d;x}'
else
trim_cmd=sed -i -n -e '/ifdef \+WITH_UNIQUE_STORE/{h;d}' -e '/endif/{x;/ifdef \+WITH_UNIQUE_STORE/d;x;p;d}' -e 'x;/ifdef \+WITH_UNIQUE_STORE/{x;d};x;p'
endif

if WITH_TAIR_CACHE
tair_cache_source=tair_cache_helper.cpp tair_cache_helper.h
endif

AM_CPPFLAGS=-fPIC -I${top_srcdir}/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
	    $(UNIQUE_STORE_CPPFLAGS)\
	    $(TAIR_CACHE_CPPFLAGS)

AM_LDFLAGS=-lrt -lpthread -ldl -luuid $(TAIR_CACHE_LDFLAGS)

lib_LTLIBRARIES= libtfsclient.la libtfsclient_c.la

api_source_list = tfs_file.cpp tfs_large_file.cpp tfs_small_file.cpp tfs_session.cpp \
                  fsname.cpp tfs_session_pool.cpp tfs_client_impl.cpp tfs_client_api.cpp \
                  local_key.cpp gc_file.cpp gc_worker.cpp bg_task.cpp client_config.cpp\
                  tfs_rc_helper.cpp tfs_rc_client_api.cpp tfs_rc_client_api_impl.cpp \
									tfs_meta_helper.cpp tfs_meta_manager.cpp tfs_meta_client_api_impl.cpp \
								 	tfs_meta_client_api.cpp \
									bg_task.h client_config.h fsname.h gc_file.h gc_worker.h local_key.h\
									lru.h md5.h segment_container.h tfs_client_api.h tfs_client_capi.h\
									tfs_client_impl.h tfs_client_metrics.h tfs_file.h tfs_large_file.h\
									tfs_rc_client_api.h tfs_rc_client_api_impl.h tfs_rc_helper.h tfs_session.h \
									tfs_meta_client_api.h tfs_meta_client_api_impl.h tfs_meta_helper.h tfs_meta_manager.h \
									tfs_session_pool.h tfs_small_file.h ${unique_store_source} \
                  ${tair_cache_source}

ld_fg = $(AM_LDFLAGS) \
        $(top_srcdir)/src/message/libtfsmessage.a \
        $(top_srcdir)/src/common/libtfscommon.a

libtfsclient_la_SOURCES = ${api_source_list}
libtfsclient_la_LIBADD = ${ld_fg}

libtfsclient_c_la_SOURCES = ${api_source_list} tfs_client_capi.cpp tfs_client_capi.h
libtfsclient_c_la_LIBADD = ${ld_fg}


include_HEADERS = tfs_client_api.h tfs_client_capi.h tfs_rc_client_api.h tfs_meta_client_api.h

# hardcode to extract static library in destinate static library
# consider libtool compatibility,do change in destinate directory
install-exec-hook:
	  tmp_dir=".tfs_tmp_dir";\
	  for client_lib in libtfsclient.a libtfsclient_c.a; \
	  do \
	    cd $(DESTDIR)$(libdir);\
	    test -d $$tmp_dir && rm -rf $$tmp_dir;\
	    mkdir -p $$tmp_dir && mv $$client_lib $$tmp_dir;\
	    cd  $$tmp_dir;\
	    ar x $$client_lib;\
	    rm -f $$client_lib;\
	    for i in *.a ; do\
	    lib_tmp_dir=".tmp_$$i";\
	    mkdir -p $$lib_tmp_dir;\
	    mv $$i $$lib_tmp_dir;\
	    cd $$lib_tmp_dir;\
	    ar x $$i;\
	    cd ../; done;\
	    ar cru ../$$client_lib `find . -name '*.o'`;\
	    ranlib ../$$client_lib;\
	    chmod 644 ../$$client_lib;\
	  done; \
	  cd .. && rm -rf $$tmp_dir

# installed dirctory differ from original src dirctory hierarchy, hardcode to change
install-data-hook:
	cd $(DESTDIR)$(includedir) && \
        sed -i 's#common/\(.*\.h\)#\1#g' ${include_HEADERS} && \
	${trim_cmd} ${include_HEADERS}
