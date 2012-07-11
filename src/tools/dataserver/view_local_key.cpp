#include "new_client/local_key.h"
#include "new_client/gc_file.h"
#include "tblog.h"

using namespace tfs::client;
using namespace tfs::common;

template<class T> void print_segment_ex(T& container)
{
  for (typename T::iterator it = container.begin();
       it != container.end(); it++)
  {
    printf("blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %"PRI64_PREFIX"d, size: %d, crc: %d\n",
           it->block_id_, it->file_id_, it->offset_, it->size_, it->crc_);
  }
}

template<class T> int print_segment(T& meta, const char* file)
{
  int ret = TFS_ERROR;
  if ((ret = meta.load_file(file)) == TFS_SUCCESS &&
      (ret = meta.validate()) == TFS_SUCCESS)
  {
    printf("segment head. meta size: %d, count: %d, size: %"PRI64_PREFIX"d\n",
           meta.get_data_size(), meta.get_segment_size(), meta.get_file_size());
    printf("segment info: \n");
    print_segment_ex(meta.get_seg_info());
  }
  return ret;
}

int main(int argc, char* argv[])
{
  int i = 0;
  bool is_gc_file = false;
  char* file = NULL;

  TBSYS_LOGGER.setLogLevel("warn");
  while ((i = getopt(argc, argv, "f:g")) != EOF)
  {
    switch (i)
    {
    case 'f':
      file = optarg;
      break;
    case 'g':
      is_gc_file = true;
      break;
    default:
      printf("Usage: %s -f file [-g]\n -g: view gc file\n", argv[0]);
      return TFS_ERROR;
    }
  }
  if (NULL == file)
  {
    printf("Usage: %s -f file [-g]\n -g: view gc file\n", argv[0]);
    return TFS_ERROR;
  }

  int ret = TFS_ERROR;
  if (is_gc_file)
  {
    GcFile gcFile;
    printf("========= view gc file ========\n");
    ret = print_segment(gcFile, file);
  }
  else
  {
    LocalKey localKey;
    printf("========= view local key file ========\n");
    ret = print_segment(localKey, file);
  }

  return ret;
}
