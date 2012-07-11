#include <iostream>
#include <time.h>
#include <stdio.h>
#include <fstream>

using namespace std;
void getByte(char*Buf,long conut)
{
     char temp;
     srand((unsigned)time(NULL));
     long i;
     for(i=0;i<conut;i++)
     {
        temp=char(33+rand()%127);
        Buf[i]=temp;
     }
    Buf[conut+1]='\0';
     
}
int main ()
{
  cout<<"start!"<<endl;
  static char Buf[1024+1];
  getByte(Buf,1024);
  
  ofstream out("../resource/1K");
  out<<Buf;
  out.close();
  cout<<"end!"<<endl;
  return 0;
}
