#ifndef BONFYRE_NARRATION_WORKSPACE_H
#define BONFYRE_NARRATION_WORKSPACE_H
/* Ephemeral synthesis uses the narration owner. The existing workspace blob
 * and revision owners retain reviewed takes after the caller receives them. */
static int nw_script(bf_mp_reader_t *reader,char *out,size_t cap) {
    bf_mp_value_t value;if(bf_mp_read_next(reader,&value)!=BF_MP_STR||!value.v.raw.len||value.v.raw.len>=cap||memchr(value.v.raw.ptr,0,value.v.raw.len))return 0;
    for(size_t i=0;i<value.v.raw.len;i++){unsigned char c=value.v.raw.ptr[i];if(c==127||(c<32&&c!='\n'&&c!='\r'&&c!='\t'))return 0;}
    memcpy(out,value.v.raw.ptr,value.v.raw.len);out[value.v.raw.len]=0;return 1;
}
static int nw_render(const char *method,const char *body,const char *raw,const char *host) {
    const char *binary=getenv("BONFYRE_NARRATE_BINARY");
    if(strcmp(method,"POST")){aw_error(405,"Use POST to render a reviewed script.\n");return 1;}
    char origin[512],expected[512],type[128],length[32];
    snprintf(expected,sizeof(expected),"%s://%s",host&&(!strncmp(host,"127.0.0.1:",10)||!strncmp(host,"localhost:",10))?"http":"https",host?host:"");
    if(!aw_header(raw,"Origin",origin,sizeof(origin))||strcmp(origin,expected)||
       !aw_header(raw,"Content-Type",type,sizeof(type))||strcmp(type,"application/msgpack")||
       !aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strlen(length)>5||strspn(length,"0123456789")!=strlen(length)){
        aw_error(403,"Render from this workspace with the reviewed script.\n");return 1;
    }
    size_t size=(size_t)strtoul(length,NULL,10);
    if(!body||!size||size>16384){aw_error(413,"Render one passage of at most 12000 bytes.\n");return 1;}
    bf_mp_reader_t reader;bf_mp_reader_init(&reader,body,size);
    long long version,rate;char script[12001],voice[81];
    if(!ac_array(&reader,4)||!ac_uint(&reader,&version)||version!=1||
       !nw_script(&reader,script,sizeof(script))||!*script||!ac_text(&reader,voice,sizeof(voice))||!*voice||
       strspn(voice,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ._-")!=strlen(voice)||
       !ac_uint(&reader,&rate)||rate<80||rate>300||reader.pos!=reader.len){
        aw_error(400,"Provide a script, installed voice and rate from 80 to 300.\n");return 1;
    }
    if(!binary||*binary!='/'||access(binary,X_OK)){aw_error(503,"The narration owner is not connected to this service.\n");return 1;}
    char directory[1024],input[1100],audio[1100],log[1100],rate_arg[16];
    const char *temp=getenv("TMPDIR");if(!temp||!*temp)temp="/tmp";
    if(snprintf(directory,sizeof(directory),"%s/aurekai-narration-XXXXXX",temp)>=(int)sizeof(directory)||!mkdtemp(directory)){
        aw_error(503,"Could not prepare the narration workspace.\n");return 1;
    }
    snprintf(input,sizeof(input),"%s/input.txt",directory);snprintf(audio,sizeof(audio),"%s/artifact.wav",directory);
    snprintf(log,sizeof(log),"%s/owner.log",directory);snprintf(rate_arg,sizeof(rate_arg),"%lld",rate);
    int fd=open(input,O_WRONLY|O_CREAT|O_EXCL,0600),ok=0,status=0;pid_t child=-1;
    if(fd>=0){size_t n=strlen(script);ok=write(fd,script,n)==(ssize_t)n;close(fd);}
    if(ok){
        fflush(NULL);child=fork();
        if(!child){
            setpgid(0,0);int sink=open(log,O_WRONLY|O_CREAT|O_EXCL,0600);
            if(sink<0)_exit(126);dup2(sink,STDOUT_FILENO);dup2(sink,STDERR_FILENO);close(sink);
            execl(binary,binary,input,directory,"--voice",voice,"--rate",rate_arg,
                  "--no-pitch-shift","--no-envelope",(char *)NULL);_exit(127);
        }
        if(child>0){
            setpgid(child,child);
            int ended=0;
            for(int attempt=0;attempt<3000;attempt++){
                pid_t result=waitpid(child,&status,WNOHANG);
                if(result==child){ended=1;break;}
                if(result<0&&errno!=EINTR)break;
                usleep(10000);
            }
            if(!ended){kill(-child,SIGKILL);waitpid(child,&status,0);ok=0;}
            else ok=WIFEXITED(status)&&WEXITSTATUS(status)==0;
        }else ok=0;
    }
    size_t bytes=0;unsigned char *wave=ok?aw_read_file(directory,"artifact.wav",8u*1024u*1024u,&bytes):NULL;
    if(wave&&bytes>=44&&!memcmp(wave,"RIFF",4)&&!memcmp(wave+8,"WAVE",4))
        aw_response(200,"audio/wav",wave,bytes,0);
    else aw_error(503,"No complete audio was produced. Keep the script and retry this rendition.\n");
    free(wave);
    const char *members[]={"input.txt","artifact.wav","artifact.manifest.json","narration.txt","render.log","owner.log","raw.wav"};
    for(size_t n=0;n<sizeof(members)/sizeof(members[0]);n++){char path[1100];snprintf(path,sizeof(path),"%s/%s",directory,members[n]);unlink(path);}
    rmdir(directory);return 1;
}
#endif
