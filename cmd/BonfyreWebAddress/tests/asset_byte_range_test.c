/* Contract checks for the native public audio byte-range parser. */
#include <stdio.h>
#include "../src/asset_byte_range.h"
static int cases=0;
static int verify(const char *request,long len,int expected,long start,long end){
    long a=-1,b=-1;
    int result=asset_range(request,len,&a,&b);
    cases++;
    if(result!=expected||(expected==1&&(a!=start||b!=end))){
        fprintf(stderr,"range case %d: got %d %ld-%ld; expected %d %ld-%ld\n",
                cases,result,a,b,expected,start,end);
        return 0;
    }
    return 1;
}
int main(void){
    int pass=
      verify("GET / HTTP/1.1\r\nHost: example\r\n\r\n",128,0,0,0)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=0-31\r\n\r\n",128,1,0,31)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=0-31",128,1,0,31)&&
      verify("GET / HTTP/1.1\r\nrange: bytes=0-31\r\n",128,1,0,31)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=100-\r\n",128,1,100,127)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=-16\r\n",128,1,112,127)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=-999\r\n",128,1,0,127)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=120-999\r\n",128,1,120,127)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=0-31\r\nAccept: */*\r\n",128,1,0,31)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=128-\r\n",128,-1,0,0)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=3-2\r\n",128,-1,0,0)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=0-31,32-64\r\n",128,-1,0,0)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=-0\r\n",128,-1,0,0)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=18446744073709551616-\r\n",128,-1,0,0)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=0-18446744073709551616\r\n",128,-1,0,0)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=0-31junk\r\n",128,-1,0,0)&&
      verify("GET / HTTP/1.1\r\nRange: byte=0-3\r\n",128,-1,0,0)&&
      verify("GET / HTTP/1.1\r\nRange: bytes=0-31\r\nRange: bytes=32-63\r\n",128,-1,0,0);
    if(!pass)return 1;
    printf("asset byte ranges: %d PASS\n",cases);
    return 0;
}
