/* Loopback-only transport witness for contract carrier status and redirect rules. */
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
int main(void) {
    int fd=socket(AF_INET,SOCK_STREAM,0);struct sockaddr_in a={.sin_family=AF_INET,.sin_addr.s_addr=htonl(INADDR_LOOPBACK)};
    if(fd<0||bind(fd,(struct sockaddr *)&a,sizeof a)||listen(fd,8))return 1;
    socklen_t n=sizeof a;if(getsockname(fd,(struct sockaddr *)&a,&n))return 1;printf("%u\n",ntohs(a.sin_port));fflush(stdout);
    for(;;){int c=accept(fd,NULL,NULL);if(c<0)break;char b[4096]={0};ssize_t got=read(c,b,sizeof b-1);int code=500;
        if(got>0&&strstr(b,"Host: aurekai.life")&&strstr(b,"Accept: application/json"))code=strstr(b,"GET /okay ")?200:strstr(b,"GET /priced ")?402:strstr(b,"GET /redirect ")?302:503;
        char reply[256];int k=snprintf(reply,sizeof reply,"HTTP/1.1 %d Fixture\r\n%sContent-Length: 0\r\nConnection: close\r\n\r\n",code,code==302?"Location: /okay\r\n":"");write(c,reply,(size_t)k);close(c);
    }close(fd);return 0;
}
