/*
 * bonfyre-reason — Reason-state operator over the HVCP stack.
 *
 * Not a model runner.  A reason-state operator.
 *
 * Uses BonfyrePhysics + BonfyreEmbed + BonfyreKVCache + DisCIPL
 * to manage multi-trajectory reasoning sessions:
 *
 *   bonfyre-reason start   --prompt <file|text> [--memory <ref>]
 *                          [--sigma 1.0] [--dt 0.01]
 *   bonfyre-reason branch  [--modes cautious,creative,adversarial]
 *                          [--kick <mode>=<ref>]
 *   bonfyre-reason run     [--steps 128] [--on-gap mount:auto]
 *                          [--trace <file>]
 *   bonfyre-reason diff    <mode-a> <mode-b>
 *   bonfyre-reason rebase  <mode> --onto <mode_or_ref>
 *                          [--preserve novelty]
 *   bonfyre-reason commit  [--message <msg>]
 *   bonfyre-reason status
 *   bonfyre-reason log     [--n 10]
 *
 * Session state lives in ~/.local/share/bonfyre/reason/<session_id>/
 *   state.bfps               — current phase-space state
 *   branches/                — named branch states
 *   traces/run-<N>.jsonl     — run traces
 *   session.json             — session metadata
 *
 * Determinism guarantee:
 *   same (prompt_hash, memory_ref, sigma, dt, steps) → same trajectory
 */

#define _POSIX_C_SOURCE 200809L
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <limits.h>
#include <bonfyre.h>

/* ── session paths ───────────────────────────────────────────── */
static void reason_base_(char *b, size_t sz) {
    const char *h=getenv("HOME"); if(!h)h="/tmp";
    snprintf(b,sz,"%s/.local/share/bonfyre/reason",h);
}
static void session_dir_(char *b, size_t sz, const char *sid) {
    char base[PATH_MAX]; reason_base_(base,sizeof(base));
    snprintf(b,sz,"%s/%s",base,sid);
}
static void pack_path_(char *b, size_t sz) {
    const char *h=getenv("HOME"); if(!h)h="/tmp";
    snprintf(b,sz,"%s/.local/share/bonfyre/embeds/pack.bfpack",h);
}
static void bvh_path_(char *b, size_t sz) {
    const char *h=getenv("HOME"); if(!h)h="/tmp";
    snprintf(b,sz,"%s/.local/share/bonfyre/embeds/pack.bfvh",h);
}

static void ensure_dir_(const char *path) {
    char tmp[PATH_MAX]; snprintf(tmp,sizeof(tmp),"%s",path);
    char *p=strrchr(tmp,'/');
    if(p){*p='\0';}
    for(char *q=tmp+1;*q;q++){
        if(*q=='/'){*q='\0';mkdir(tmp,0755);*q='/';}
    }
    mkdir(tmp,0755);
}

/* ── session ID: hex of timestamp ───────────────────────────── */
static void make_session_id_(char *out, size_t sz) {
    snprintf(out,sz,"%016llx",(unsigned long long)time(NULL));
}

/* ── find current session (most-recently-modified) ──────────── */
static int find_current_session_(char *out_dir, size_t sz) {
    char base[PATH_MAX]; reason_base_(base,sizeof(base));
    DIR *d=opendir(base);
    if(!d) return -1;
    struct dirent *e;
    time_t best=0; char best_name[256]="";
    while((e=readdir(d))!=NULL){
        if(e->d_name[0]=='.') continue;
        char full[PATH_MAX];
        snprintf(full,sizeof(full),"%s/%s",base,e->d_name);
        struct stat st;
        if(stat(full,&st)!=0) continue;
        if(!S_ISDIR(st.st_mode)) continue;
        if(st.st_mtime>best){ best=st.st_mtime; snprintf(best_name,sizeof(best_name),"%s",e->d_name); }
    }
    closedir(d);
    if(best_name[0]=='\0') return -1;
    snprintf(out_dir,sz,"%s/%s",base,best_name);
    return 0;
}

/* ── resolve session: --session flag or most-recent ─────────── */
static int resolve_session_(int argc, char **argv, char *sdir, size_t sz) {
    for(int i=0;i<argc;i++){
        if(!strcmp(argv[i],"--session")&&i+1<argc){
            char base[PATH_MAX]; reason_base_(base,sizeof(base));
            snprintf(sdir,sz,"%s/%s",base,argv[i+1]);
            return 0;
        }
    }
    return find_current_session_(sdir,sz);
}

/* ── vec norm ────────────────────────────────────────────────── */
static float vec_norm_(const float *v, uint32_t d) {
    float s=0.0f; for(uint32_t i=0;i<d;i++) s+=v[i]*v[i]; return sqrtf(s);
}

/* ── usage ───────────────────────────────────────────────────── */
static void usage(void) {
    printf(
"bonfyre-reason — Reason-state operator\n"
"\n"
"  Manages multi-trajectory reasoning sessions over the HVCP stack.\n"
"  Under the hood: BonfyrePhysics + BonfyreEmbed + BonfyreKVCache.\n"
"\n"
"Subcommands:\n"
"\n"
"  start  --prompt <file|text> [--memory <ref>]\n"
"           [--sigma 1.0] [--dt 0.01] [--session <id>]\n"
"           Create a new reasoning session.  If --prompt is a filename,\n"
"           its SHA-256 hash keys the embed lookup.  Initialises state\n"
"           from an embedding of the prompt hash.\n"
"\n"
"  branch [--modes cautious,creative,adversarial]\n"
"           [--kick <mode>=<ref>] [--session <id>]\n"
"           Fork current state into named trajectories.\n"
"           Default modes: cautious, creative, adversarial.\n"
"\n"
"  run    [--steps 128] [--on-gap mount:auto]\n"
"           [--trace <file>] [--mode <name>] [--session <id>]\n"
"           Run a trajectory (or all branches if --mode not specified).\n"
"\n"
"  diff   <mode-a> <mode-b> [--session <id>]\n"
"           Diff two branch traces: steps, gaps, H drift, entropy.\n"
"\n"
"  rebase <mode> --onto <mode_or_ref> [--preserve novelty]\n"
"           [--session <id>]\n"
"           Move a branch trajectory onto another memory manifold.\n"
"\n"
"  commit [--message <msg>] [--session <id>]\n"
"           Save current state to reflog with message.\n"
"           Creates a named ref: reason/<session_id>/head.\n"
"\n"
"  status [--session <id>]\n"
"           Print current session state summary.\n"
"\n"
"  log    [--n 10] [--session <id>]\n"
"           Show session run history.\n"
"\n"
"Global flags:\n"
"  --session <id>   Use a specific session (default: most recent).\n"
    );
}

/* ═══════════════════════════════════════════════════════════════ */
/* write session.json                                               */
/* ═══════════════════════════════════════════════════════════════ */
static void write_session_json_(const char *sdir, const char *sid,
                                  const char *prompt_hash_hex,
                                  const char *memory_ref,
                                  float sigma, float dt) {
    char path[PATH_MAX];
    snprintf(path,sizeof(path),"%s/session.json",sdir);
    FILE *f=fopen(path,"wb");
    if(!f) return;
    char ts[64]; bf_iso_timestamp(ts,sizeof(ts));
    fprintf(f,"{\n"
              "  \"session_id\": \"%s\",\n"
              "  \"created_at\": \"%s\",\n"
              "  \"prompt_hash\": \"%s\",\n"
              "  \"memory_ref\": \"%s\",\n"
              "  \"sigma\": %.4f,\n"
              "  \"dt\": %.6f,\n"
              "  \"runs\": 0\n"
              "}\n",
              sid, ts,
              prompt_hash_hex ? prompt_hash_hex : "",
              memory_ref      ? memory_ref      : "",
              sigma, dt);
    fclose(f);
}

static void increment_runs_(const char *sdir) {
    /* simple: read, increment counter, rewrite */
    char path[PATH_MAX];
    snprintf(path,sizeof(path),"%s/session.json",sdir);
    char *json=bf_read_file(path,NULL);
    if(!json) return;
    int runs=0; bf_json_int(json,"runs",&runs); runs++;
    free(json);
    /* rewrite runs field with sed-style replace */
    FILE *f=fopen(path,"rb"); if(!f) return;
    char buf[4096]; size_t n=fread(buf,1,sizeof(buf)-1,f); fclose(f);
    buf[n]='\0';
    char *p=strstr(buf,"\"runs\":"); if(!p) return;
    p+=7; while(*p==' ')p++;
    /* overwrite number in place */
    char newbuf[4096]; ptrdiff_t off=p-buf;
    snprintf(newbuf,sizeof(newbuf),"%.*s%d%s",(int)off,buf,runs,
             strchr(p,'\n')?strchr(p,'\n'):"");
    f=fopen(path,"wb"); if(!f) return;
    fwrite(newbuf,1,strlen(newbuf),f); fclose(f);
}

/* ═══════════════════════════════════════════════════════════════ */
/* start                                                            */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_start_(int argc, char **argv) {
    const char *prompt=NULL, *memory_ref=NULL;
    float sigma=1.0f, dt=0.01f;
    const char *session_id_arg=NULL;

    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--prompt") &&i+1<argc) prompt=argv[++i];
        if(!strcmp(argv[i],"--memory") &&i+1<argc) memory_ref=argv[++i];
        if(!strcmp(argv[i],"--sigma")  &&i+1<argc) sigma=(float)atof(argv[++i]);
        if(!strcmp(argv[i],"--dt")     &&i+1<argc) dt   =(float)atof(argv[++i]);
        if(!strcmp(argv[i],"--session")&&i+1<argc) session_id_arg=argv[++i];
    }
    if(!prompt){fprintf(stderr,"start: need --prompt <file|text>\n");return 1;}

    /* generate session ID */
    char sid[64];
    if(session_id_arg) snprintf(sid,sizeof(sid),"%s",session_id_arg);
    else               make_session_id_(sid,sizeof(sid));

    char sdir[PATH_MAX]; session_dir_(sdir,sizeof(sdir),sid);
    ensure_dir_(sdir);
    char bdir[PATH_MAX]; snprintf(bdir,sizeof(bdir),"%s/branches",sdir);
    mkdir(bdir,0755);
    char tdir[PATH_MAX]; snprintf(tdir,sizeof(tdir),"%s/traces",sdir);
    mkdir(tdir,0755);

    /* hash the prompt (file or inline text) */
    uint8_t prompt_hash[32]={0};
    char prompt_hex[65]="";
    struct stat pst;
    if(stat(prompt,&pst)==0){
        /* it's a file */
        bf_sha256_file(prompt,prompt_hex);
    } else {
        /* inline text */
        BfSha256 ctx; bf_sha256_init(&ctx);
        bf_sha256_update(&ctx,(const uint8_t*)prompt,strlen(prompt));
        bf_sha256_final(&ctx,prompt_hash);
        bf_sha256_hex(prompt_hash,32,prompt_hex);
    }

    /* try to look up embed for the prompt hash */
    BfEmbedPack pack={0};
    char pack_p[PATH_MAX]; pack_path_(pack_p,sizeof(pack_p));
    int has_pack=(bf_embed_pack_open(&pack,pack_p)==0);

    uint32_t dim = has_pack ? pack.dim : 512u;

    /* initialise phase-space state */
    BfPhysicsState *s=bf_physics_state_alloc(dim,sigma,dt);
    if(!s){ if(has_pack) bf_embed_pack_close(&pack); return 1; }

    /* try to use prompt embed as initial q */
    uint8_t ph[32]={0};
    for(int i=0;i<32;i++){
        unsigned v=0; sscanf(prompt_hex+i*2,"%02x",&v); ph[i]=(uint8_t)v;
    }
    float *pv=NULL; uint32_t pd=0;
    if(bf_embed_lookup(ph,&pv,&pd)==0 && pd==dim){
        bf_physics_init_from_embed(s,pv,dim);
        free(pv);
        printf("  q: initialised from prompt embed\n");
    } else {
        /* no embed yet: use all-zeros q with small random jitter */
        srand((unsigned)time(NULL));
        for(uint32_t d=0;d<dim;d++)
            s->q[d]=(float)rand()/(float)RAND_MAX*0.001f;
        printf("  q: no prompt embed found — using small-norm initialisation\n"
               "     run 'bonfyre-embed pack' to index embeddings first\n");
    }

    /* if --memory ref given, load it and kick */
    if(memory_ref){
        float *mv=NULL; uint32_t md=0;
        uint8_t mhash[32]={0};
        if(bf_embed_ref_read(memory_ref,mhash)==0 &&
           bf_embed_lookup(mhash,&mv,&md)==0 && md==dim){
            bf_physics_kick(s,mv,0.5f);
            free(mv);
            printf("  p: memory kick from ref '%s'\n",memory_ref);
        } else {
            printf("  p: memory ref '%s' not found — skipping kick\n",memory_ref);
        }
    }

    char state_path[PATH_MAX];
    snprintf(state_path,sizeof(state_path),"%s/state.bfps",sdir);
    bf_physics_state_save(s,state_path);

    write_session_json_(sdir,sid,prompt_hex,memory_ref,sigma,dt);

    printf("bonfyre-reason start:\n");
    printf("  session    : %s\n",sid);
    printf("  prompt     : %s\n",prompt);
    printf("  prompt_hash: %s\n",prompt_hex);
    printf("  dim        : %u\n",dim);
    printf("  sigma      : %.4f\n",sigma);
    printf("  dt         : %.6f\n",dt);
    printf("  ‖q‖       : %.4f\n",vec_norm_(s->q,dim));
    printf("  ‖p‖       : %.4f\n",vec_norm_(s->p,dim));
    printf("  state     → %s\n",state_path);
    printf("\nnext: bonfyre-reason branch --modes cautious,creative,adversarial\n");

    if(has_pack) bf_embed_pack_close(&pack);
    bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* branch                                                           */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_branch_(int argc, char **argv) {
    char sdir[PATH_MAX];
    if(resolve_session_(argc,argv,sdir,sizeof(sdir))!=0){
        fprintf(stderr,"branch: no active session — run 'bonfyre-reason start' first\n");
        return 1;
    }

    /* parse --modes as comma-separated list */
    const char *modes_str="cautious,creative,adversarial";
    const char *kicks[32]; int n_kicks=0;

    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--modes") &&i+1<argc) modes_str=argv[++i];
        if(!strcmp(argv[i],"--kick")  &&i+1<argc&&n_kicks<32) kicks[n_kicks++]=argv[++i];
    }

    /* tokenise modes */
    char modes_buf[512]; snprintf(modes_buf,sizeof(modes_buf),"%s",modes_str);
    const char *mode_names[32]; int n_modes=0;
    char *tok=strtok(modes_buf,",");
    while(tok&&n_modes<32){ mode_names[n_modes++]=tok; tok=strtok(NULL,","); }

    char state_path[PATH_MAX];
    snprintf(state_path,sizeof(state_path),"%s/state.bfps",sdir);
    BfPhysicsState *src=bf_physics_state_load(state_path);
    if(!src){fprintf(stderr,"branch: cannot load state %s\n",state_path);return 1;}

    char pack_p[PATH_MAX]; pack_path_(pack_p,sizeof(pack_p));
    BfEmbedPack pack={0};
    int has_pack=(bf_embed_pack_open(&pack,pack_p)==0);

    char bdir[PATH_MAX];
    snprintf(bdir,sizeof(bdir),"%s/branches",sdir);
    mkdir(bdir,0755);

    printf("bonfyre-reason branch: %d modes from session %s\n",
           n_modes, strrchr(sdir,'/')+1);

    for(int ni=0;ni<n_modes;ni++){
        const char *name=mode_names[ni];
        BfPhysicsState *b=bf_physics_state_alloc(src->dim,src->sigma,src->dt);
        if(!b) continue;
        memcpy(b->q,src->q,src->dim*sizeof(float));
        memcpy(b->p,src->p,src->dim*sizeof(float));
        b->step=src->step;

        /* apply --kick name=ref */
        for(int ki=0;ki<n_kicks;ki++){
            const char *ks=kicks[ki];
            size_t nl=strlen(name);
            if(strncmp(ks,name,nl)||ks[nl]!='=') continue;
            const char *ref=ks+nl+1;
            uint8_t rhash[32]={0};
            float *rv=NULL; uint32_t rd=0;
            if(bf_embed_ref_read(ref,rhash)==0&&
               bf_embed_lookup(rhash,&rv,&rd)==0&&rd==src->dim){
                bf_physics_kick(b,rv,1.0f);
                free(rv);
                printf("  %-18s kick: %s\n",name,ref);
            }
        }

        char bpath[PATH_MAX];
        snprintf(bpath,sizeof(bpath),"%s/%s.bfps",bdir,name);
        if(bf_physics_state_save(b,bpath)==0)
            printf("  %-18s → branches/%s.bfps  ‖p‖=%.4f\n",
                   name,name,vec_norm_(b->p,b->dim));
        bf_physics_state_free(b);
    }

    if(has_pack) bf_embed_pack_close(&pack);
    bf_physics_state_free(src);
    printf("\nnext: bonfyre-reason run --steps 128 --on-gap mount:auto\n");
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* run                                                             */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_run_(int argc, char **argv) {
    char sdir[PATH_MAX];
    if(resolve_session_(argc,argv,sdir,sizeof(sdir))!=0){
        fprintf(stderr,"run: no active session\n");return 1;
    }

    int max_steps=128;
    int gap_mount=0;
    const char *mode_arg=NULL;
    const char *trace_arg=NULL;

    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--steps")   &&i+1<argc) max_steps=atoi(argv[++i]);
        if(!strcmp(argv[i],"--mode")    &&i+1<argc) mode_arg=argv[++i];
        if(!strcmp(argv[i],"--trace")   &&i+1<argc) trace_arg=argv[++i];
        if(!strcmp(argv[i],"--on-gap")  &&i+1<argc){
            if(!strncmp(argv[i+1],"mount",5)) gap_mount=1;
            i++;
        }
    }

    char pack_p[PATH_MAX]; pack_path_(pack_p,sizeof(pack_p));
    char bvh_p[PATH_MAX];  bvh_path_(bvh_p,sizeof(bvh_p));

    /* collect state files to run */
    char state_paths[32][PATH_MAX]; int n_states=0;
    if(mode_arg){
        snprintf(state_paths[n_states],sizeof(state_paths[0]),
                 "%s/branches/%s.bfps",sdir,mode_arg);
        n_states=1;
    } else {
        /* run all branches if they exist, else main state */
        char bdir[PATH_MAX]; snprintf(bdir,sizeof(bdir),"%s/branches",sdir);
        DIR *d=opendir(bdir);
        if(d){
            struct dirent *e;
            while((e=readdir(d))!=NULL&&n_states<32){
                if(e->d_name[0]=='.') continue;
                size_t nl=strlen(e->d_name);
                if(nl>5&&!strcmp(e->d_name+nl-5,".bfps")){
                    snprintf(state_paths[n_states],sizeof(state_paths[0]),
                             "%s/%s",bdir,e->d_name);
                    n_states++;
                }
            }
            closedir(d);
        }
        if(n_states==0){
            snprintf(state_paths[0],sizeof(state_paths[0]),"%s/state.bfps",sdir);
            n_states=1;
        }
    }

    BfEmbedPack pack={0}; BfEmbedBVH bvh={0};
    int has_bvh=(bf_embed_pack_open(&pack,pack_p)==0 &&
                 bf_embed_bvh_open(&bvh,bvh_p)==0  &&
                 bvh.dim==pack.dim);

    char tdir[PATH_MAX]; snprintf(tdir,sizeof(tdir),"%s/traces",sdir);
    mkdir(tdir,0755);

    int run_n=0;
    /* count existing traces */
    DIR *td=opendir(tdir); if(td){ struct dirent *e;
        while((e=readdir(td))!=NULL) if(e->d_name[0]!='.')run_n++;
        closedir(td);
    }

    printf("bonfyre-reason run: steps=%d  %d trajectories\n",max_steps,n_states);

    for(int si=0;si<n_states;si++){
        BfPhysicsState *s=bf_physics_state_load(state_paths[si]);
        if(!s){ printf("  (skip: %s)\n",state_paths[si]); continue; }

        const char *label=strrchr(state_paths[si],'/');
        label=label?label+1:state_paths[si];
        /* strip .bfps */
        char label_buf[128]; snprintf(label_buf,sizeof(label_buf),"%s",label);
        char *dot=strrchr(label_buf,'.'); if(dot)*dot='\0';

        char tf[PATH_MAX];
        if(trace_arg) snprintf(tf,sizeof(tf),"%s",trace_arg);
        else snprintf(tf,sizeof(tf),"%s/run-%d-%s.jsonl",tdir,run_n,label_buf);

        BfEntropyTrace *tr=bf_trace_open(tf,0);
        BfEntropyAccum ea;
        float H0=has_bvh?bf_physics_hamiltonian(s,&bvh,&pack):
                         0.5f*vec_norm_(s->p,s->dim)*vec_norm_(s->p,s->dim);
        bf_entropy_init(&ea,H0);

        int done=0, gap=0;
        for(int i=0;i<max_steps;i++){
            int rc=has_bvh ? bf_physics_step(s,&bvh,&pack) : 0;
            done++;
            float H=has_bvh?bf_physics_hamiltonian(s,&bvh,&pack):H0;
            float ke=0.5f*vec_norm_(s->p,s->dim)*vec_norm_(s->p,s->dim);
            float gnorm=vec_norm_(s->grad_buf,s->dim);
            int is_gap=(rc==1);
            bf_entropy_update_step(&ea,H,is_gap,0,0);
            if(tr){
                BfTraceEvent ev; memset(&ev,0,sizeof(ev));
                ev.step=s->step; ev.H=H; ev.K=ke; ev.V=H-ke;
                ev.grad_norm=gnorm; ev.gap=is_gap;
                ev.entropy=bf_entropy_score(&ea);
                bf_trace_write(tr,&ev);
            }
            if(rc==1){
                gap=1;
                if(gap_mount){
                    uint8_t zero[32]={0}; BfKVMount m={.fd=-1};
                    if(bf_kvcache_mount_auto(zero,zero,&m)==0){
                        printf("  %s: auto-mount %zu B\n",label_buf,m.map_size);
                        bf_entropy_update_step(&ea,H,0,1,0);
                        gap=0; /* retry */
                        continue;
                    }
                }
                break;
            }
            if(rc<0) break;
        }

        float H1=has_bvh?bf_physics_hamiltonian(s,&bvh,&pack):H0;
        printf("  %-18s: steps=%-4d  H=%-10.4f  ΔH=%.2e  S=%.4f%s\n",
               label_buf, done, H1, (double)(H1-H0),
               bf_entropy_score(&ea), gap?" ⚠gap":"");

        if(tr) bf_trace_close(tr);
        bf_physics_state_save(s,state_paths[si]);
        bf_physics_state_free(s);
    }

    if(has_bvh){bf_embed_bvh_close(&bvh);bf_embed_pack_close(&pack);}
    increment_runs_(sdir);
    return 0;
}

/* ── diff helpers ────────────────────────────────────────────── */
typedef struct {
    uint64_t n_steps;
    float H_first, H_last, H_drift;
    uint32_t gaps, mounts;
    float entropy_last;
    uint64_t first_gap;
} DiffDS;

static int diff_stat_cb_(const char *line, void *ctx) {
    DiffDS *s=ctx;
    const char *hf=strstr(line,"\"H\":");
    const char *gf=strstr(line,"\"gap\":");
    const char *ef=strstr(line,"\"entropy\":");
    const char *sf=strstr(line,"\"step\":");
    const char *mp=strstr(line,"\"mounted\":[");
    double H  =hf?atof(hf+4) :0.0;
    double gap=gf?atof(gf+6) :0.0;
    double ent=ef?atof(ef+10):0.0;
    long long st=sf?atoll(sf+7):0LL;
    int hm=mp&&strstr(mp+11,"\"")&&strstr(mp+11,"\"")<strchr(mp+11,']');
    if(s->n_steps==0){s->H_first=(float)H;s->first_gap=UINT64_MAX;}
    float drift=fabsf((float)H-s->H_first);
    if(drift>s->H_drift) s->H_drift=drift;
    s->H_last=(float)H; s->n_steps++;
    if(gap>0.5){ if(s->gaps==0)s->first_gap=(uint64_t)st; s->gaps++; }
    if(hm) s->mounts++;
    s->entropy_last=(float)ent;
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* diff                                                             */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_diff_(int argc, char **argv) {
    char sdir[PATH_MAX];
    if(resolve_session_(argc,argv,sdir,sizeof(sdir))!=0){
        fprintf(stderr,"diff: no active session\n");return 1;
    }

    /* find the two mode names */
    const char *ma=NULL, *mb=NULL;
    for(int i=1;i<argc;i++){
        if(argv[i][0]!='-'){
            if(!ma) ma=argv[i];
            else if(!mb) mb=argv[i];
        }
    }
    if(!ma||!mb){fprintf(stderr,"diff: need <mode-a> <mode-b>\n");return 1;}

    /* find most recent trace for each mode */
    char tdir[PATH_MAX]; snprintf(tdir,sizeof(tdir),"%s/traces",sdir);
    char ta[PATH_MAX]="", tb[PATH_MAX]="";
    time_t best_a=0, best_b=0;
    DIR *d=opendir(tdir); if(!d){fprintf(stderr,"diff: no traces\n");return 1;}
    struct dirent *e;
    while((e=readdir(d))!=NULL){
        if(e->d_name[0]=='.') continue;
        char full[PATH_MAX]; snprintf(full,sizeof(full),"%s/%s",tdir,e->d_name);
        struct stat st; if(stat(full,&st)!=0) continue;
        if(strstr(e->d_name,ma)&&st.st_mtime>best_a){ best_a=st.st_mtime; snprintf(ta,sizeof(ta),"%s",full); }
        if(strstr(e->d_name,mb)&&st.st_mtime>best_b){ best_b=st.st_mtime; snprintf(tb,sizeof(tb),"%s",full); }
    }
    closedir(d);

    if(!ta[0]||!tb[0]){
        fprintf(stderr,"diff: trace not found for '%s' or '%s'\n",ma,mb);
        fprintf(stderr,"      run 'bonfyre-reason run' with --trace or default traces\n");
        return 1;
    }

    DiffDS da={0}, db={0};
    da.first_gap=UINT64_MAX; db.first_gap=UINT64_MAX;
    bf_trace_iterate(ta,diff_stat_cb_,&da);
    bf_trace_iterate(tb,diff_stat_cb_,&db);

    printf("bonfyre-reason diff: %s vs %s\n\n",ma,mb);
    printf("  %-22s  %14s  %14s\n","metric",ma,mb);
    printf("  %-22s  %14s  %14s\n","──────────────────────",
           "──────────────","──────────────");
    printf("  %-22s  %14llu  %14llu\n","steps",
           (unsigned long long)da.n_steps,(unsigned long long)db.n_steps);
    printf("  %-22s  %14u  %14u\n","gaps",da.gaps,db.gaps);
    printf("  %-22s  %14u  %14u\n","mounts",da.mounts,db.mounts);
    printf("  %-22s  %14.4f  %14.4f\n","H drift",da.H_drift,db.H_drift);
    printf("  %-22s  %14.4f  %14.4f\n","entropy_last",
           da.entropy_last,db.entropy_last);

    uint64_t div=(da.first_gap<db.first_gap)?da.first_gap:db.first_gap;
    if(div!=UINT64_MAX)
        printf("\n  divergence at step: %llu\n",(unsigned long long)div);
    else
        printf("\n  no divergence detected\n");

    printf("\n  trace-a: %s\n  trace-b: %s\n",ta,tb);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* rebase                                                           */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_rebase_(int argc, char **argv) {
    char sdir[PATH_MAX];
    if(resolve_session_(argc,argv,sdir,sizeof(sdir))!=0){
        fprintf(stderr,"rebase: no active session\n");return 1;
    }

    const char *mode=NULL, *onto=NULL;
    for(int i=1;i<argc;i++){
        if(argv[i][0]!='-'&&!mode) mode=argv[i];
        if(!strcmp(argv[i],"--onto")&&i+1<argc) onto=argv[++i];
    }
    if(!mode||!onto){
        fprintf(stderr,"rebase: need <mode> --onto <mode_or_ref>\n");return 1;
    }

    char src_path[PATH_MAX], onto_path[PATH_MAX];
    snprintf(src_path, sizeof(src_path), "%s/branches/%s.bfps",sdir,mode);
    snprintf(onto_path,sizeof(onto_path),"%s/branches/%s.bfps",sdir,onto);

    BfPhysicsState *s=bf_physics_state_load(src_path);
    if(!s){fprintf(stderr,"rebase: cannot load %s\n",src_path);return 1;}

    /* load onto state, use its q as the onto-vector */
    BfPhysicsState *o=bf_physics_state_load(onto_path);
    if(!o){
        /* try as a named ref instead */
        uint8_t ohash[32]={0};
        float *ov=NULL; uint32_t od=0;
        if(bf_embed_ref_read(onto,ohash)==0 &&
           bf_embed_lookup(ohash,&ov,&od)==0 && od==s->dim){
            float qn0=vec_norm_(s->q,s->dim), dn2=0.0f;
            for(uint32_t d=0;d<s->dim;d++){
                float delta=ov[d]-s->q[d];
                s->q[d]=ov[d];
                dn2+=delta*delta;
            }
            free(ov);
            printf("rebase: %s → ref '%s'  ‖Δq‖=%.4f\n",mode,onto,sqrtf(dn2));
            printf("  ‖q‖ %.4f → %.4f\n",qn0,vec_norm_(s->q,s->dim));
        } else {
            fprintf(stderr,"rebase: cannot resolve onto='%s'\n",onto);
            bf_physics_state_free(s);return 1;
        }
    } else {
        /* shift q toward onto.q */
        float qn0=vec_norm_(s->q,s->dim), dn2=0.0f;
        for(uint32_t d=0;d<s->dim;d++){
            float delta=o->q[d]-s->q[d];
            s->q[d]+=delta;
            dn2+=delta*delta;
        }
        printf("rebase: %s onto %s  ‖Δq‖=%.4f\n",mode,onto,sqrtf(dn2));
        printf("  ‖q‖ %.4f → %.4f  ‖p‖ %.4f (preserved)\n",
               qn0,vec_norm_(s->q,s->dim),vec_norm_(s->p,s->dim));
        bf_physics_state_free(o);
    }

    bf_physics_state_save(s,src_path);
    bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* commit                                                           */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_commit_(int argc, char **argv) {
    char sdir[PATH_MAX];
    if(resolve_session_(argc,argv,sdir,sizeof(sdir))!=0){
        fprintf(stderr,"commit: no active session\n");return 1;
    }
    const char *msg="reason commit";
    for(int i=1;i<argc;i++)
        if(!strcmp(argv[i],"--message")&&i+1<argc) msg=argv[++i];

    char state_path[PATH_MAX];
    snprintf(state_path,sizeof(state_path),"%s/state.bfps",sdir);
    BfPhysicsState *s=bf_physics_state_load(state_path);
    if(!s){fprintf(stderr,"commit: cannot load state\n");return 1;}

    /* SHA-256 of q as the commit hash */
    uint8_t commit_hash[32];
    BfSha256 ctx; bf_sha256_init(&ctx);
    bf_sha256_update(&ctx,(const uint8_t*)s->q,s->dim*sizeof(float));
    bf_sha256_final(&ctx,commit_hash);

    /* save embed-like record for the q vector */
    bf_embed_store(commit_hash,s->q,s->dim);

    /* append to reflog */
    bf_embed_reflog_append(commit_hash,msg);

    /* write named ref: reason/<sid>/head */
    const char *sid=strrchr(sdir,'/'); sid=sid?sid+1:sdir;
    char ref_name[256];
    snprintf(ref_name,sizeof(ref_name),"reason/%s/head",sid);
    bf_embed_ref_write(ref_name,commit_hash);

    char hex[65]; bf_sha256_hex(commit_hash,32,hex);
    printf("bonfyre-reason commit: %s\n",hex);
    printf("  ref: %s\n",ref_name);
    printf("  msg: %s\n",msg);
    printf("  step: %llu  ‖q‖=%.4f\n",
           (unsigned long long)s->step,vec_norm_(s->q,s->dim));

    bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* status                                                           */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_status_(int argc, char **argv) {
    char sdir[PATH_MAX];
    if(resolve_session_(argc,argv,sdir,sizeof(sdir))!=0){
        printf("no active session — run 'bonfyre-reason start'\n");
        return 0;
    }
    const char *sid=strrchr(sdir,'/'); sid=sid?sid+1:sdir;

    char state_path[PATH_MAX];
    snprintf(state_path,sizeof(state_path),"%s/state.bfps",sdir);
    BfPhysicsState *s=bf_physics_state_load(state_path);

    printf("bonfyre-reason status:\n");
    printf("  session : %s\n",sid);
    printf("  dir     : %s\n",sdir);

    if(s){
        printf("  dim     : %u\n",s->dim);
        printf("  step    : %llu\n",(unsigned long long)s->step);
        printf("  ‖q‖    : %.4f\n",vec_norm_(s->q,s->dim));
        printf("  ‖p‖    : %.4f\n",vec_norm_(s->p,s->dim));
        bf_physics_state_free(s);
    } else {
        printf("  state   : (not initialised)\n");
    }

    /* count branches */
    char bdir[PATH_MAX]; snprintf(bdir,sizeof(bdir),"%s/branches",sdir);
    DIR *d=opendir(bdir); int nb=0;
    if(d){ struct dirent *e;
        while((e=readdir(d))!=NULL) if(e->d_name[0]!='.')nb++;
        closedir(d);
    }
    printf("  branches: %d\n",nb);

    /* count traces */
    char tdir[PATH_MAX]; snprintf(tdir,sizeof(tdir),"%s/traces",sdir);
    d=opendir(tdir); int nt=0;
    if(d){ struct dirent *e;
        while((e=readdir(d))!=NULL) if(e->d_name[0]!='.')nt++;
        closedir(d);
    }
    printf("  traces  : %d\n",nt);

    /* session.json */
    char jpath[PATH_MAX]; snprintf(jpath,sizeof(jpath),"%s/session.json",sdir);
    char *json=bf_read_file(jpath,NULL);
    if(json){
        char prompt_hash[128]="";
        bf_json_str(json,"prompt_hash",prompt_hash,sizeof(prompt_hash));
        printf("  prompt  : %.16s...\n",prompt_hash);
        int runs=0; bf_json_int(json,"runs",&runs);
        printf("  runs    : %d\n",runs);
        free(json);
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* log                                                             */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_log_(int argc, char **argv) {
    char sdir[PATH_MAX];
    if(resolve_session_(argc,argv,sdir,sizeof(sdir))!=0){
        fprintf(stderr,"log: no active session\n");return 1;
    }
    int n_show=10;
    for(int i=1;i<argc;i++)
        if(!strcmp(argv[i],"--n")&&i+1<argc) n_show=atoi(argv[++i]);

    BfEmbedReflogEntry *entries=NULL; int n=0;
    if(bf_embed_reflog_read(&entries,&n)!=0||n==0){
        printf("log: no reflog entries yet — run 'bonfyre-reason commit'\n");
        return 0;
    }
    int start=n-n_show; if(start<0) start=0;
    printf("bonfyre-reason log (last %d commits):\n\n",n-start);
    static const char HEX[]="0123456789abcdef";
    for(int i=n-1;i>=start;i--){
        char hex[65];
        for(int j=0;j<32;j++){
            hex[j*2]=HEX[entries[i].hash[j]>>4];
            hex[j*2+1]=HEX[entries[i].hash[j]&0xf];
        }
        hex[64]='\0';
        printf("  %.16s  %s  %s\n",hex,entries[i].timestamp,entries[i].message);
    }
    free(entries);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* main                                                             */
/* ═══════════════════════════════════════════════════════════════ */
int main(int argc, char **argv) {
    if(argc<2){usage();return 1;}

    /* filter session flag before dispatch */
    char *sub[64]; int sub_argc=0;
    for(int i=1;i<argc&&sub_argc<62;i++){
        /* preserve --session so subcommands can access it */
        sub[sub_argc++]=argv[i];
    }
    sub[sub_argc]=NULL;

    const char *cmd=sub[0];
    if(!strcmp(cmd,"--help")||!strcmp(cmd,"-h")){ usage();return 0; }
    if(!strcmp(cmd,"start"))  return cmd_start_ (sub_argc,sub);
    if(!strcmp(cmd,"branch")) return cmd_branch_(sub_argc,sub);
    if(!strcmp(cmd,"run"))    return cmd_run_   (sub_argc,sub);
    if(!strcmp(cmd,"diff"))   return cmd_diff_  (sub_argc,sub);
    if(!strcmp(cmd,"rebase")) return cmd_rebase_(sub_argc,sub);
    if(!strcmp(cmd,"commit")) return cmd_commit_(sub_argc,sub);
    if(!strcmp(cmd,"status")) return cmd_status_(sub_argc,sub);
    if(!strcmp(cmd,"log"))    return cmd_log_   (sub_argc,sub);

    fprintf(stderr,"Unknown subcommand: %s\n",cmd);
    usage();
    return 1;
}
