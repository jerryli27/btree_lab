#include <stdlib.h>
#include <string.h>
#include "btree.h"

void usage() 
{
  cerr << "usage: btree_insert filestem cachesize key value\n";
}


int main(int argc, char **argv)
{
  char *filestem;
  SIZE_T cachesize;
  SIZE_T superblocknum;
  char *key, *value;
  bool newTree;

  if (argc!=6) { //5
    usage();
    return -1;
  }

  filestem=argv[1];
  cachesize=atoi(argv[2]);
  key=argv[3];
  value=argv[4];
  if (strcmp(argv[5],"0")==0){
    newTree=false;
  }else{
    newTree=true;
  }

  DiskSystem disk(filestem);
  BufferCache cache(&disk,cachesize);
  usage();
  BTreeIndex btree(0,0,&cache);
  
  ERROR_T rc;

  if ((rc=cache.Attach())!=ERROR_NOERROR) { 
    cerr << "Can't attach buffer cache due to error"<<rc<<endl;
    return -1;
  }

  if ((rc=btree.Attach(0,newTree))!=ERROR_NOERROR) { 
    cerr << "Can't attach to index  due to error "<<rc<<endl;
    return -1;
  } else {
    cerr << "Index attached!"<<endl;
    if ((rc=btree.Insert(KEY_T(key),VALUE_T(value)))!=ERROR_NOERROR) { 
      cerr <<"Can't insert into index due to error "<<rc<<endl;
    } else {
      cerr <<"Insert succeeded\n";
    }
    if ((rc=btree.Detach(superblocknum))!=ERROR_NOERROR) { 
      cerr <<"Can't detach from index due to error "<<rc<<endl;
      return -1;
    }
    if ((rc=cache.Detach())!=ERROR_NOERROR) { 
      cerr <<"Can't detach from cache due to error "<<rc<<endl;
      return -1;
    }
    cerr << "Performance statistics:\n";
    
    cerr << "numallocs       = "<<cache.GetNumAllocs()<<endl;
    cerr << "numdeallocs     = "<<cache.GetNumDeallocs()<<endl;
    cerr << "numreads        = "<<cache.GetNumReads()<<endl;
    cerr << "numdiskreads    = "<<cache.GetNumDiskReads()<<endl;
    cerr << "numwrites       = "<<cache.GetNumWrites()<<endl;
    cerr << "numdiskwrites   = "<<cache.GetNumDiskWrites()<<endl;
    cerr << endl;
    
    cerr << "total time      = "<<cache.GetCurrentTime()<<endl;

    return 0;
  }
}
  

  
