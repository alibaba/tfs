#include <unistd.h>
#include <iostream>
#include "test_gfactory.h"
#include "test_tfs_api.h"

using namespace std;
using namespace tbsys;
using namespace tbnet;

sigset_t g_mask;
sigset_t g_oldmask;

void usage(char *argv)
{
  cout << "Function: saveFile,tfsNewFileName,tfs_read,tfs_write,tfs_stat,tfsRename,tfsUnlink,all,tfsControlPress,tfsSeed,tfsUnlinkuni,tfsCompact" << endl;
  cout << "Usage:" << argv << " -f config -i Function -l logfile" << endl;
  exit(1);
}

void *thsigfn(void *arg)
{
  int sig;
  pthread_detach(pthread_self());
  while(1){
    sigwait(&g_mask, &sig);
    switch(sig){
      case SIGINT:
      case SIGQUIT:
      case SIGTERM:
        TBSYS_LOG(ERROR,"receive signal,quit.");
        TestGFactory::_tfsTest.stop();
        return NULL;
      default:
        break;
    }
  }
  return NULL;
}

int main(int argc,char **argv)
{
  int i;
  int iToday = 0;
  int iHour = 0;
  int iMin = 0;
  int iSec = 0;
  char filename[128] = {'\0'};
  char testIndex[32];
  char logfile[ 128 ]={'\0'};
  char *pLogLevel;
  while((i = getopt(argc,argv,"f:i:l:")) != -1){
    switch(i){
      case 'f':
        strncpy(filename,optarg,strlen(optarg)+1);
        break;
      case 'i':
        strncpy(testIndex,optarg,strlen(optarg)+1);
        break;
      case 'l':
        strncpy(logfile,optarg,strlen(optarg)+1);
        break;
      default:
        usage(argv[0]);
    }
  }

  if(filename[0] == '\0'){
    usage(argv[0]);
  }

  time((time_t*)(&iToday));
  iHour = ((iToday%86400)/3600+8)%24;
  iMin = ((iToday)%3600)/60;
  iSec = iToday%60;
  CConfig::getCConfig().load(const_cast<char *> (filename));
  TestGFactory::_threadCount = CConfig::getCConfig().getInt("public","thread_count");
  TestGFactory::_writeFileSize = CConfig::getCConfig().getInt("public","writeFileSize");
  TestGFactory::_writeDelayTime = CConfig::getCConfig().getInt("public","writeDelayTime");
  TestGFactory::_batchCount = CConfig::getCConfig().getInt("public","batchCount");
  TestGFactory::_segSize = CConfig::getCConfig().getInt("public","segmentSize");
  pLogLevel = (char *)CConfig::getCConfig().getString("public","setLogLevel");


  if(logfile[ 0 ] == '\0'){
    sprintf(logfile, "./%s_%02d:%02d:%02d_%03dk.log",  
      testIndex, iHour, iMin, iSec, TestGFactory::_threadCount);
    cout << "use default log file===>" << logfile + 2 << "\n";
  }
  srand(time(NULL));

#if 0
  FILE *fp = NULL;
  if((fp = fopen((char *)"./filelist.txt","r")) == NULL){
    TBSYS_LOG(DEBUG,"open file_list failed.");
    return -1;
  }
  char strfilename[64];
  while(fgets(strfilename,sizeof(strfilename),fp)){
    if(filename[strlen(strfilename)-1] == '\n'){
      filename[strlen(strfilename)-1] = '\0';
    }else{
      filename[strlen(strfilename)] = '\0';
    }
    _strfilenameX.push_back(strfilename);
  }

  TBSYS_LOG(ERROR,"filename:%s", strfilename);

#endif

  sigemptyset(&g_mask);
  sigaddset(&g_mask,SIGINT);
  sigaddset(&g_mask,SIGQUIT);
  sigaddset(&g_mask,SIGTERM);
  pthread_sigmask(SIG_BLOCK, &g_mask, &g_oldmask);

  pthread_t sig_tid;
  pthread_create(&sig_tid,NULL,thsigfn,NULL);

  TBSYS_LOGGER.setLogLevel(pLogLevel);
  TBSYS_LOGGER.setFileName(logfile);
  TestGFactory::_tfsTest.init(filename);
  if(testIndex != NULL){
    TestGFactory::_tfsTest.setTestIndex(testIndex);
  }

  TestGFactory::_tfsTest.start();
  TestGFactory::_tfsTest.wait();
  //pthread_join(sig_tid,NULL);
  return 0;
}

