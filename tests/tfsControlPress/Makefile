#dir
SRC_DIR=.
OUT_DIR=./out
#TFS_SRC=/home/admin/tfs1.3_ha/src
TFS_SRC=/home/admin/tfs-rm-1.4.0/src
#TFS_SRC=/home/admin/tfs-rm-1.3.2/src
TBLIB_ROOT=/home/admin/lib
SRC=/lib


#complie option
CPP=g++
CFLAGS=-Wall -g -I$(TFS_SRC) -I$(TFS_SRC)/common -I$(TFS_SRC)/new_client -I$(TFS_SRC)/dataserver -I$(TFS_SRC)/message -I$(TBLIB_ROOT)/include/tbnet -I$(TBLIB_ROOT)/include/tbsys
LIBFLAGS=-L${SRC} -L${TFS_SRC}/new_client/ -L${TFS_SRC}/message/
LIBS= $(TFS_SRC)/new_client/.libs/libtfsclient.a $(TFS_SRC)/message/libtfsmessage.a $(TFS_SRC)/common/libtfscommon.a $(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a -lpthread -lpthread -lrt

#relative
obj=$(patsubst ${SRC_DIR}/%.cpp,${OUT_DIR}/%.o,$(wildcard ${SRC_DIR}/*.cpp))

.phoney:clean all

all:mkdir tfsControlPress
	@echo "Make Finished"
	
mkdir:
	@mkdir -p out

tfsControlPress:${obj}
	@echo "Make Excutable File " $@
	@${CPP} -o $@ $^ ${LIBFLAGS} ${LIBS}

${OUT_DIR}/%.o:${SRC_DIR}/%.cpp
	@echo "Compile" $@
	@${CPP} -c ${CFLAGS} -o $@ $<
	
clean:
	@echo "Clean All"
	@rm -f tfsControlPress ${OUT_DIR}/*.o
	@rm -rf ${OUT_DIR}

