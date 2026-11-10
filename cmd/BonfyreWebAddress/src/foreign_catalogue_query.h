#ifndef BONFYRE_FOREIGN_CATALOGUE_QUERY_H
#define BONFYRE_FOREIGN_CATALOGUE_QUERY_H
/* Optional catalogue filters and transport representation are independent. */
static int fcq_hex(unsigned char c){if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;if(c>='A'&&c<='F')return c-'A'+10;return -1;}
static int fcq_decode(const char *restrict p,size_t n,char *restrict out,size_t cap){size_t w=0;for(size_t i=0;i<n;i++){unsigned char c=(unsigned char)p[i];if(c=='%'){if(i+2>=n)return 0;int a=fcq_hex(p[i+1]),b=fcq_hex(p[i+2]);if(a<0||b<0)return 0;c=(unsigned char)(a*16+b);i+=2;}else if(c=='+')c=' ';if(c<32||c==127||w+1>=cap)return 0;out[w++]=(char)c;}out[w]=0;return 1;}
static int foreign_catalogue_query(const char *query,char search[241],char snapshot[32]){
 unsigned seen=0;search[0]=snapshot[0]=0;if(!query||!*query)return 1;if(strlen(query)>1024)return 0;
 for(const char *p=query;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[16],repr[16];unsigned bit;char *out;size_t cap;
  if(!eq||!fcq_decode(p,(size_t)(eq-p),key,sizeof(key)))return 0;
  if(!strcmp(key,"q")){bit=1;out=search;cap=241;}else if(!strcmp(key,"snapshot")){bit=2;out=snapshot;cap=32;}else if(!strcmp(key,"repr")){bit=4;out=repr;cap=sizeof(repr);}else return 0;
  if((seen&bit)||!fcq_decode(eq+1,(size_t)(end-eq-1),out,cap))return 0;seen|=bit;
  if(bit==2&&strspn(snapshot,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")!=strlen(snapshot))return 0;
  if(bit==4&&strcmp(repr,"API")&&strcmp(repr,"HTML")&&strcmp(repr,"MCP")&&strcmp(repr,"A2A")&&strcmp(repr,"BonfyreFS")&&strcmp(repr,"json")&&strcmp(repr,"html"))return 0;
  p=*end?end+1:end;if(!*p&&*end)return 0;
 }return 1;
}
#endif
