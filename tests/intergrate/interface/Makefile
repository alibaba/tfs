#dir
SRC_DIR=.
OUT_DIR=./out
TFS_SRC=/home/admin/tfs-dev-1.4.1-chen/src
TBLIB_ROOT=/home/admin/ext4Common
SRC=/lib


#complie option
CPP=g++

CFLAGS=-Wall -g -I$(TFS_SRC) -I$(TFS_SRC)/common -I$(TFS_SRC)/new_client -I$(TFS_SRC)/dataserver -I$(TFS_SRC)/message -I$(TBLIB_ROOT)/include/tbnet -I$(TBLIB_ROOT)/include/tbsys 

LIBFLAGS=-L${SRC} -L${TFS_SRC}/client/ -L${TFS_SRC}/message/ -L${TFS_SRC}/messagev2/

LIBS= $(TFS_SRC)/new_client/.libs/libtfsclient.a $(TFS_SRC)/message/libtfsmessage.a $(TFS_SRC)/common/libtfscommon.a $(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a -lpthread -lgtest -lMagick++ -lpthread -lrt

#relative
obj = test_07_fetch_file_local_file

.PHONY: clear all  tear

all: mkdir $(obj) move
	@echo "make finished !"
mkdir:
	@rm -rf out
	@mkdir out

$(obj): % : %.cpp
	@${CPP} -o $@  -lgtest ${CFLAGS} $< ${LIBFLAGS} ${LIBS}

clear:
	@rm  -rf *.o
	@rm  -rf ./out
tear:
	@rm  -rf *.o

move:
	@mv ${SRC_DIR}/$(obj)  ${OUT_DIR}


