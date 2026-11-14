/* Editorial sound design for reviewed narration takes.
 * This is a derivative renderer, never a rights grant or publication action.
 * All source music is local, explicit, digest-verified and credited.  No
 * provider dependency, automatic source harvesting, or silent promotion.
 */
#ifndef BONFYRE_BROADCAST_EDITORIAL_MIX_H
#define BONFYRE_BROADCAST_EDITORIAL_MIX_H

typedef struct {
    const char *name;
    double intro,tail,bed_gain;
    int duck_release;
} BfEditorialProfile;

static const BfEditorialProfile bf_editorial_profiles[] = {
    {"newsroom",1.6,2.5,0.14,560},
    {"feature",3.4,4.2,0.17,1050},
    {"essay",2.5,3.4,0.11,1150},
    {"conversation",2.2,3.0,0.12,850},
    {"documentary",4.0,4.6,0.16,1000}
};
static int bf_editorial_profile(const char *name,BfEditorialProfile *p) {
    if(!name||!p)return 0;
    for(size_t i=0;i<sizeof(bf_editorial_profiles)/sizeof(bf_editorial_profiles[0]);i++)
        if(!strcmp(name,bf_editorial_profiles[i].name)){*p=bf_editorial_profiles[i];return 1;}
    return 0;
}
static int cmd_editorial_profiles(void){
    puts("profile\tintro_ms\ttail_ms\tbed_gain\tduck_release_ms");
    for(size_t i=0;i<sizeof(bf_editorial_profiles)/sizeof(bf_editorial_profiles[0]);i++){
        const BfEditorialProfile *p=&bf_editorial_profiles[i];
        printf("%s\t%.0f\t%.0f\t%.3f\t%d\n",p->name,p->intro*1000,p->tail*1000,p->bed_gain,p->duck_release);
    }
    return 0;
}
static int bf_editorial_hex(const char *s) {
    return s&&strlen(s)==64&&strspn(s,"0123456789abcdef")==64;
}
static int bf_editorial_local(const char *path,const char *sha) {
    struct stat st={0};char actual[65]={0};
    return path&&*path&&bf_editorial_hex(sha)&&
       !lstat(path,&st)&&S_ISREG(st.st_mode)&&st.st_size>0&&
       st.st_size<=128LL*1024*1024&&
       bf_sha256_file(path,actual)==0&&!strcmp(actual,sha);
}
static int bf_editorial_rights(const char *license,const char *credit,const char *url) {
    if(!license||!credit||!url||!*credit||strlen(credit)>240||
       strlen(url)>600||strncmp(url,"https://",8))return 0;
    if(strpbrk(credit,"\r\n\t")||strpbrk(url,"\r\n\t"))return 0;
    if(strcmp(license,"CC0-1.0")&&strcmp(license,"CC-BY-4.0")&&
       strcmp(license,"original-owned")&&strcmp(license,"licensed-commercial"))
        return 0;
    return 1;
}
static void bf_editorial_vtt_time(FILE *f,double seconds) {
    long millis=(long)llround(seconds*1000);
    fprintf(f,"%02ld:%02ld:%02ld.%03ld",millis/3600000,
        (millis/60000)%60,(millis/1000)%60,millis%1000);
}
static int bf_editorial_transcript(const char *input,const char *output,
                                   double offset,double speech_seconds) {
    struct stat st={0};
    if(!input||!output||lstat(input,&st)||!S_ISREG(st.st_mode)||
       st.st_size<8||st.st_size>2*1024*1024)return 0;
    FILE *src=fopen(input,"r");if(!src)return 0;
    FILE *dst=fopen(output,"wx");
    if(!dst){fclose(src);return 0;}
    int valid=1,cues=0;char line[8192];
    if(!fgets(line,sizeof(line),src)||strncmp(line,"WEBVTT",6))valid=0;
    else fputs("WEBVTT\n",dst);
    while(valid&&fgets(line,sizeof(line),src)){
        if(!strchr(line,'\n')&&!feof(src)){valid=0;break;}
        int sh,sm,eh,em;double ss,es;
        if(sscanf(line,"%d:%d:%lf --> %d:%d:%lf",&sh,&sm,&ss,&eh,&em,&es)==6){
            double a=sh*3600.0+sm*60.0+ss,b=eh*3600.0+em*60.0+es;
            if(sh<0||eh<0||sm<0||em<0||sm>59||em>59||
               !isfinite(a)||!isfinite(b)||a<0||b<=a||
               b>speech_seconds+0.25){valid=0;break;}
            bf_editorial_vtt_time(dst,a+offset);fputs(" --> ",dst);
            bf_editorial_vtt_time(dst,b+offset);fputc('\n',dst);cues++;
        }else {
            if(strstr(line,"-->")){valid=0;break;}
            fputs(line,dst);
        }
    }
    if(ferror(src)||ferror(dst)||!cues)valid=0;
    fclose(src);
    if(fclose(dst))valid=0;
    if(!valid)unlink(output);
    return valid;
}
/* The same profile maps to an editorial mix plan everywhere. Voice offsets,
 * sidechain release and real credit are retained; CSS/player waveforms cannot
 * invent the audio dynamics. The bed loops, so its owner must supply a
 * loop-clean, rights-cleared asset rather than an arbitrary popular song.
 */
static int cmd_mix_article(int argc,char **argv) {
    const char *voice=opt(argc,argv,"--voice",NULL),
               *voice_sha=opt(argc,argv,"--voice-sha256",NULL),
               *bed=opt(argc,argv,"--bed",NULL),
               *bed_sha=opt(argc,argv,"--bed-sha256",NULL),
               *license=opt(argc,argv,"--bed-license",NULL),
               *credit=opt(argc,argv,"--bed-credit",NULL),
               *source_url=opt(argc,argv,"--bed-source-url",NULL),
               *transcript=opt(argc,argv,"--transcript",NULL),
               *out=opt(argc,argv,"--out",NULL),
               *program=opt(argc,argv,"--program",NULL),
               *profile_name=opt(argc,argv,"--profile","feature"),
               *revision=opt(argc,argv,"--source-sha256",NULL),
               *article_revision=opt(argc,argv,"--article-revision-sha256",NULL);
    BfEditorialProfile profile;
    if(!bf_editorial_profile(profile_name,&profile))
        return fail("choose an installed editorial profile: newsroom, feature, essay, conversation, documentary");
    if(!out||!*out||!program||!*program||strlen(program)>95||
       strspn(program,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-")!=strlen(program)||
       !transcript||!bf_editorial_local(voice,voice_sha)||
       !bf_editorial_local(bed,bed_sha)||
       !bf_editorial_rights(license,credit,source_url)||
       (revision&&!bf_editorial_hex(revision))||
       (article_revision&&!bf_editorial_hex(article_revision))||(revision&&article_revision))
        return fail("mix-article requires exact voice/music digests, a transcript, an editorial profile, and explicit music attribution");
    struct stat st={0};
    if(lstat(out,&st)==0){
        if(!S_ISDIR(st.st_mode)||S_ISLNK(st.st_mode))return fail("output must be a real directory");
    }else if(errno!=ENOENT||mkdir(out,0750))return fail("could not create output directory");
    char dest[1200],vtt[1200],chapters[1200],manifest[1200],log[1200];
    if(snprintf(dest,sizeof(dest),"%s/program.m4a",out)>=(int)sizeof(dest)||
       snprintf(vtt,sizeof(vtt),"%s/transcript.vtt",out)>=(int)sizeof(vtt)||
       snprintf(chapters,sizeof(chapters),"%s/chapters.json",out)>=(int)sizeof(chapters)||
       snprintf(manifest,sizeof(manifest),"%s/manifest.json",out)>=(int)sizeof(manifest)||
       snprintf(log,sizeof(log),"%s/editorial-mix.log",out)>=(int)sizeof(log))
        return fail("output path exceeds native bounds");
    if(!access(dest,F_OK)||!access(vtt,F_OK)||!access(chapters,F_OK)||!access(manifest,F_OK))
        return fail("output already has an audio edition; use a new reviewed destination");
    double speech=duration_of(voice),music=duration_of(bed);
    if(!isfinite(speech)||speech<1.0||speech>7200||
       !isfinite(music)||music<5.0||music>7200)
        return fail("narration must be 1 second to 2 hours and retained music 5 seconds to 2 hours");
    double total_target=speech+profile.intro+profile.tail;
    char filter[2200];
    int ms=(int)llround(profile.intro*1000);
    if(snprintf(filter,sizeof(filter),
       "[0:a]aresample=44100,aformat=channel_layouts=stereo,highpass=f=75,"
       "acompressor=threshold=0.14:ratio=2.2:attack=15:release=180,"
       "adelay=%d|%d,apad=pad_dur=%.2f,atrim=0:%.3f,asplit=2[v][side];"
       "[1:a]aresample=44100,aformat=channel_layouts=stereo,"
       "atrim=0:%.3f,asetpts=PTS-STARTPTS,lowpass=f=9000,"
       "volume=%.3f,afade=t=in:st=0:d=0.6,"
       "afade=t=out:st=%.3f:d=%.3f[bed];"
       "[bed][side]sidechaincompress=threshold=0.018:ratio=14:"
       "attack=50:release=%d[duck];"
       "[v][duck]amix=inputs=2:duration=first:normalize=0,"
       "loudnorm=I=-16:TP=-1.5:LRA=9,alimiter=limit=0.90[final]",
       ms,ms,profile.tail,total_target,total_target,
       profile.bed_gain,total_target-profile.tail,profile.tail,
       profile.duck_release)>=(int)sizeof(filter))
       return fail("editorial filter is outside bounded native output");
    char *args[]={"ffmpeg","-nostdin","-v","error","-n",
       "-i",(char*)voice,"-stream_loop","-1","-i",(char*)bed,
       "-filter_complex",filter,"-map","[final]",
       "-t",NULL,"-ar","44100","-ac","2","-c:a","aac",
       "-b:a","160k","-movflags","+faststart",dest,NULL};
    char duration_arg[40];snprintf(duration_arg,sizeof(duration_arg),"%.3f",total_target);
    args[16]=duration_arg;
    if(run(args,NULL,0,log)!=0)return fail("editorial mix failed; see the native mix log");
    if(!bf_editorial_local(voice,voice_sha)||!bf_editorial_local(bed,bed_sha)){
        unlink(dest);return fail("the narration or retained music changed during mixing");
    }
    double duration=duration_of(dest);
    if(!isfinite(duration)||fabs(duration-total_target)>1.0||
       !bf_editorial_transcript(transcript,vtt,profile.intro,speech)){
        unlink(dest);return fail("mix duration or source transcript failed verification");
    }
    char sha[65]={0},peaks[BF_AUDIO_WAVEFORM_BINS*4+1]="";
    if(bf_sha256_file(dest,sha)!=0||
       !bf_waveform_from_audio(dest,duration,peaks,sizeof(peaks))){
        unlink(dest);unlink(vtt);return fail("final recorded audio or measured waveform invalid");
    }
    bf_jv *chapter=bf_jv_obj(),*items=bf_jv_arr();
    for(int i=0;i<3;i++){
        bf_jv *c=bf_jv_obj();
        bf_jv_set(c,"startTime",bf_jv_num(i==0?0:i==1?profile.intro:profile.intro+speech));
        bf_jv_set(c,"title",bf_jv_str(i==0?"Opening music":i==1?"Narration":"Closing music"));
        bf_jv_push(items,c);
    }
    bf_jv_set(chapter,"version",bf_jv_str("1.2.0"));bf_jv_set(chapter,"chapters",items);
    if(write_json(chapters,chapter)){bf_jv_free(chapter);unlink(dest);unlink(vtt);return fail("chapters could not be retained");}
    bf_jv_free(chapter);
    bf_jv *m=bf_jv_obj(),*credits=bf_jv_arr(),*song=bf_jv_obj();
    bf_jv_set(m,"schema",bf_jv_str("bonfyre.editorial-audio.v1"));
    bf_jv_set(m,"program",bf_jv_str(program));
    bf_jv_set(m,"audio",bf_jv_str("program.m4a"));
    bf_jv_set(m,"audio_sha256",bf_jv_str(sha));
    bf_jv_set(m,"voice_sha256",bf_jv_str(voice_sha));
    bf_jv_set(m,"waveform_audio_sha256",bf_jv_str(sha));
    bf_jv_set(m,"waveform_peaks",bf_jv_str(peaks));
    bf_jv_set(m,"duration_ms",bf_jv_num(llround(duration*1000)));
    bf_jv_set(m,"chapters",bf_jv_str("chapters.json"));
    bf_jv_set(m,"transcript",bf_jv_str("transcript.vtt"));
    bf_jv_set(m,"editorial_profile",bf_jv_str(profile.name));
    bf_jv_set(m,"narration_start_ms",bf_jv_num(ms));
    if(revision)bf_jv_set(m,"narration_source_sha256",bf_jv_str(revision));
    if(article_revision)bf_jv_set(m,"narration_article_revision_sha256",bf_jv_str(article_revision));
    bf_jv_set(song,"use",bf_jv_str("background music"));
    bf_jv_set(song,"creator",bf_jv_str(credit));
    bf_jv_set(song,"licence",bf_jv_str(license));
    bf_jv_set(song,"source_url",bf_jv_str(source_url));
    bf_jv_set(song,"audio_sha256",bf_jv_str(bed_sha));
    bf_jv_push(credits,song);
    bf_jv_set(m,"credits",credits);
    bf_jv_set(m,"cannot_infer",bf_jv_str(
       "Explicit credit does not itself establish music rights. "
       "Narration, music selection, mix and publication require independent review; "
       "this derivative is a draft, not an authorized publication."));
    int ok=write_json(manifest,m)==0;
    bf_jv_free(m);
    if(!ok){unlink(dest);unlink(vtt);unlink(chapters);return fail("media mix manifest not retained");}
    printf("editorial mix: profile=%s duration=%.2f start=%.2f audio_sha256=%s rights_review=pending\n",
           profile.name,duration,profile.intro,sha);
    return 0;
}
#endif
