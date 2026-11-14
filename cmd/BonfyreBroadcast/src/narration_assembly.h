/* Bounded native assembly of source-addressed narration takes. */
#ifndef BONFYRE_NARRATION_ASSEMBLY_H
#define BONFYRE_NARRATION_ASSEMBLY_H
#define BA_CLIPS 64
static int ba_probe_rate(const char *path){
 char *out=NULL;char *args[]={"ffprobe","-v","error","-select_streams","a:0","-show_entries","stream=sample_rate","-of","default=noprint_wrappers=1:nokey=1",(char*)path,NULL};
 int rate=0;if(!run(args,&out,256,NULL)&&out){char *end=NULL;long n=strtol(out,&end,10);if(end!=out&&(*end==0||*end=='\n')&&n>=8000&&n<=96000)rate=(int)n;}free(out);return rate;
}
static int ba_script_valid(const char *s){
 if(!s||!*s||strlen(s)>12000||strstr(s,"-->"))return 0;
 for(const unsigned char *p=(const unsigned char*)s;*p;p++)if((*p<32&&*p!='\n'&&*p!='\r'&&*p!='\t')||*p==127)return 0;
 return 1;
}
static void ba_vtt_text(FILE *out,const char *s){
 for(;*s;s++){if(*s=='&')fputs("&amp;",out);else if(*s=='<')fputs("&lt;",out);else if(*s=='>')fputs("&gt;",out);else if(*s=='\n'||*s=='\r'||*s=='\t')fputc(' ',out);else fputc(*s,out);}
}
static int cmd_assemble_narration(int argc,char **argv){
 const char *plan_path=opt(argc,argv,"--plan",NULL),*audio=opt(argc,argv,"--out",NULL),*vtt=opt(argc,argv,"--transcript",NULL),*source_dir=opt(argc,argv,"--source-dir",NULL);
 struct stat st;if(!plan_path||!audio||!vtt||!strcmp(audio,vtt)||lstat(plan_path,&st)||!S_ISREG(st.st_mode)||st.st_size<=0||st.st_size>1024*1024)return fail("assemble-narration requires a bounded plan and distinct output files");
 if(!lstat(audio,&st)||errno!=ENOENT||!lstat(vtt,&st)||errno!=ENOENT)return fail("assembly output already exists or is unavailable");
 bf_jv *plan=load_json(plan_path);if(!plan)return fail("invalid narration assembly plan");
 const bf_jv *clips=bf_jv_get(plan,"clips");size_t count=jlen(clips);double sr=jnum(plan,"sample_rate",0);
 if(strcmp(jstr(plan,"schema"),"bonfyre.narration-assembly.v1")||count<1||count>BA_CLIPS||sr<8000||sr>96000||floor(sr)!=sr){bf_jv_free(plan);return fail("assembly needs one to 64 takes and a valid sample rate");}
 char filter[32768]={0},piece[320],rate[16];size_t used=0;long long previous=0;const char *paths[BA_CLIPS],*hashes[BA_CLIPS],*scripts[BA_CLIPS];char local_paths[BA_CLIPS][1200];long long starts[BA_CLIPS],frames[BA_CLIPS];
 char *args[BA_CLIPS*2+18];int ac=0;args[ac++]="ffmpeg";args[ac++]="-nostdin";args[ac++]="-v";args[ac++]="error";args[ac++]="-y";
 for(size_t i=0;i<count;i++){
  const bf_jv *clip=bf_jv_at(clips,i);const char *path=jstr(clip,"path"),*sha=jstr(clip,"sha256"),*script=jstr(clip,"script");
  double at=jnum(clip,"at",-1),n=jnum(clip,"frames",-1),gain=jnum(clip,"gain",-1);
  if(source_dir){
   char expected[70];if(!bf_editorial_hex(sha)||snprintf(expected,sizeof(expected),"%s.wav",sha)>=(int)sizeof(expected)||strcmp(path,expected)||
      snprintf(local_paths[i],sizeof(local_paths[i]),"%s/%s",source_dir,expected)>=(int)sizeof(local_paths[i])){bf_jv_free(plan);return fail("assembly source identity is invalid");}
   path=local_paths[i];
  }
  if(!bf_editorial_local(path,sha)||!ba_script_valid(script)||
     !isfinite(at)||!isfinite(n)||!isfinite(gain)||floor(at)!=at||floor(n)!=n||at<previous||n<1||gain<0||gain>1||at+n>sr*600||
     ba_probe_rate(path)!=(int)sr||fabs(duration_of(path)-n/sr)>0.1){bf_jv_free(plan);return fail("assembly take, timing, script or digest is invalid");}
  paths[i]=path;hashes[i]=sha;scripts[i]=script;starts[i]=(long long)at;frames[i]=(long long)n;previous=(long long)(at+n);
  args[ac++]="-i";args[ac++]=(char*)path;
  int wrote=snprintf(piece,sizeof(piece),"[%zu:a]aresample=%d,aformat=sample_fmts=fltp:channel_layouts=stereo,atrim=end_sample=%lld,volume=%.9g,adelay=%lldS:all=1[a%zu];",i,(int)sr,frames[i],gain,starts[i],i);
  if(wrote<0||(size_t)wrote>=sizeof(piece)||used+(size_t)wrote>=sizeof(filter)){bf_jv_free(plan);return fail("assembly filter exceeds its bound");}memcpy(filter+used,piece,(size_t)wrote);used+=(size_t)wrote;filter[used]=0;
 }
 for(size_t i=0;i<count;i++){int wrote=snprintf(piece,sizeof(piece),"[a%zu]",i);if(wrote<0||used+(size_t)wrote>=sizeof(filter)){bf_jv_free(plan);return fail("assembly filter exceeds its bound");}memcpy(filter+used,piece,(size_t)wrote);used+=(size_t)wrote;filter[used]=0;}
 int wrote=snprintf(filter+used,sizeof(filter)-used,"amix=inputs=%zu:duration=longest:normalize=0[out]",count);if(wrote<0||(size_t)wrote>=sizeof(filter)-used){bf_jv_free(plan);return fail("assembly filter exceeds its bound");}
 snprintf(rate,sizeof(rate),"%d",(int)sr);args[ac++]="-filter_complex";args[ac++]=filter;args[ac++]="-map";args[ac++]="[out]";args[ac++]="-ar";args[ac++]=rate;args[ac++]="-c:a";args[ac++]="pcm_s16le";char temporary[] = "/tmp/bonfyre-assembly-XXXXXX";int tempfd=mkstemp(temporary);if(tempfd<0){bf_jv_free(plan);return fail("assembly temporary file unavailable");}close(tempfd);
 args[ac++]="-f";args[ac++]="wav";args[ac++]=temporary;args[ac]=NULL;
 int owned_vtt=0,owned_audio=0,ok=0;if(run(args,NULL,0,NULL))goto done;
 for(size_t i=0;i<count;i++)if(!bf_editorial_local(paths[i],hashes[i]))goto done;
 if(fabs(duration_of(temporary)-previous/sr)>0.1)goto done;
 FILE *out=fopen(vtt,"wx");if(!out)goto done;owned_vtt=1;fputs("WEBVTT\n\n",out);
 for(size_t i=0;i<count;i++){bf_editorial_vtt_time(out,starts[i]/sr);fputs(" --> ",out);bf_editorial_vtt_time(out,(starts[i]+frames[i])/sr);fputc('\n',out);ba_vtt_text(out,scripts[i]);fputs("\n\n",out);}
 if(ferror(out)){fclose(out);goto done;}if(fclose(out))goto done;
 char sha[65];if(bf_sha256_file(temporary,sha)||link(temporary,audio))goto done;owned_audio=1;
 bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"schema");bf_jsonw_str(&w,"bonfyre.narration-assembly-result.v1");bf_jsonw_key(&w,"audio_sha256");bf_jsonw_str(&w,sha);bf_jsonw_key(&w,"frames");bf_jsonw_int(&w,previous);bf_jsonw_key(&w,"sample_rate");bf_jsonw_int(&w,(int)sr);bf_jsonw_key(&w,"passages");bf_jsonw_int(&w,(long long)count);bf_jsonw_obj_end(&w);
 if(!w.oom&&w.buf){puts(w.buf);ok=1;}bf_jsonw_free(&w);
 done:unlink(temporary);if(!ok){if(owned_vtt)unlink(vtt);if(owned_audio)unlink(audio);}bf_jv_free(plan);return ok?0:fail("narration assembly failed or a retained take changed");
}
#endif
