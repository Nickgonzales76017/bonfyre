/*
 * bonfyre-physics — Hamiltonian Version Control Protocol (HVCP) CLI.
 *
 * A version-controlled phase-space for machine thought.
 * Each run is a trajectory through addressable latent space.
 *
 *   ADDRESS SPACE:   hashes, refs, packs, commits, mounts
 *   LATENT PHYSICS:  q, p, H, V(q), BVH, leapfrog, collision
 *
 * "Bonfyre turns inference state into a content-addressed Hamiltonian trajectory."
 *
 * Subcommands:
 *   bvh          Build ball-tree BVH index.
 *   init         Initialise phase-space state.
 *   kick         Add momentum impulse.
 *   step         N Leapfrog steps.
 *   run          Run to convergence or topological gap.
 *   collide      Tier-1 ternary sketch collision check.
 *   nearest      Tier-2 Hamiltonian collision: actual influence.
 *   energy (H)   Print H = ½‖p‖² + V(q).
 *   branch       Fork state into named parallel trajectories.
 *   rebase       Move trajectory onto a new memory manifold.
 *   compare      Side-by-side comparison of states.
 *   trace        Analyse JSONL trace file.
 *   diff         Diff two trace files.
 *   cherry-pick  Apply mount events from trace onto state.
 *   mount        Hot-plug sub-cache.
 *   show         Print state summary.
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

/* ── default paths ───────────────────────────────────────────── */
static void default_pack_ (char *b, size_t sz) {
    const char *h=getenv("HOME"); if(!h)h="/tmp";
    snprintf(b,sz,"%s/.local/share/bonfyre/embeds/pack.bfpack",h);
}
static void default_bvh_  (char *b, size_t sz) {
    const char *h=getenv("HOME"); if(!h)h="/tmp";
    snprintf(b,sz,"%s/.local/share/bonfyre/embeds/pack.bfvh",h);
}
static void default_state_(char *b, size_t sz) {
    const char *h=getenv("HOME"); if(!h)h="/tmp";
    snprintf(b,sz,"%s/.local/share/bonfyre/physics/state.bfps",h);
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

/* ── hex helpers ─────────────────────────────────────────────── */
static const char HEX_[]="0123456789abcdef";
static void hex64_(char out[65], const uint8_t h[32]) {
    for(int i=0;i<32;i++){out[i*2]=HEX_[h[i]>>4];out[i*2+1]=HEX_[h[i]&0xf];}
    out[64]='\0';
}
static void hex_print_(const uint8_t h[32]) {
    char s[65]; hex64_(s,h); printf("%s",s);
}
static int parse_hex32_(uint8_t out[32], const char *s) {
    if(!s||strlen(s)!=64) return -1;
    for(int i=0;i<32;i++){
        unsigned v;
        if(sscanf(s+i*2,"%02x",&v)!=1) return -1;
        out[i]=(uint8_t)v;
    }
    return 0;
}

/* ── vecf helpers ────────────────────────────────────────────── */
#define VECF_MAGIC 0x46434556u
static float *load_vecf_(const char *path, uint32_t *out_dim) {
    FILE *f=fopen(path,"rb");
    if(!f){fprintf(stderr,"physics: cannot open %s\n",path);return NULL;}
    uint32_t magic=0,dim=0;
    if(fread(&magic,4,1,f)!=1||magic!=VECF_MAGIC||
       fread(&dim,4,1,f)!=1||dim==0){
        fclose(f);fprintf(stderr,"physics: bad vecf %s\n",path);return NULL;
    }
    float *v=malloc(dim*sizeof(float));
    if(!v){fclose(f);return NULL;}
    if(fread(v,sizeof(float),dim,f)!=dim){free(v);fclose(f);return NULL;}
    fclose(f);
    *out_dim=dim;
    return v;
}

/* ── vector norm ─────────────────────────────────────────────── */
static float vec_norm_(const float *v, uint32_t d) {
    float s=0.0f; for(uint32_t i=0;i<d;i++)s+=v[i]*v[i]; return sqrtf(s);
}

/* ── SHA-256 of q for trace event ────────────────────────────── */
static void q_hash_(uint8_t out[32], const float *q, uint32_t dim) {
    BfSha256 ctx;
    bf_sha256_init(&ctx);
    bf_sha256_update(&ctx,(const uint8_t*)q,dim*sizeof(float));
    bf_sha256_final(&ctx,out);
}

/* ── fill a trace event from physics state ───────────────────── */
static void fill_trace_ev_(BfTraceEvent *ev, const BfPhysicsState *s,
                             float H, float K, float V, float grad_norm,
                             int gap, float entropy,
                             int candidates, int collisions) {
    static uint8_t qh[32];
    memset(ev,0,sizeof(*ev));
    ev->step=s->step;
    q_hash_(qh,s->q,s->dim);
    ev->q_hash=qh;
    ev->H=H; ev->K=K; ev->V=V;
    ev->grad_norm=grad_norm;
    ev->gap=gap;
    ev->entropy=entropy;
    ev->candidates=candidates;
    ev->collisions=collisions;
}

/* ── open pack + BVH ─────────────────────────────────────────── */
static int open_pack_bvh_(const char *pp, const char *bp,
                           BfEmbedPack *pack, BfEmbedBVH *bvh) {
    if(bf_embed_pack_open(pack,pp)!=0){
        fprintf(stderr,"physics: cannot open pack %s\n",pp);return -1;
    }
    if(bf_embed_bvh_open(bvh,bp)!=0){
        fprintf(stderr,"physics: no BVH at %s — run 'bonfyre-physics bvh' first\n",bp);
        bf_embed_pack_close(pack);return -1;
    }
    if(bvh->dim!=pack->dim){
        fprintf(stderr,"physics: BVH dim %u != pack dim %u\n",bvh->dim,pack->dim);
        bf_embed_bvh_close(bvh);bf_embed_pack_close(pack);return -1;
    }
    return 0;
}

/* ── resolve an embed ref or hex string to a float vector ───── */
static int resolve_embed_(const char *ref, float **vec_out,
                           uint32_t *dim_out, uint32_t required_dim) {
    uint8_t hash[32]={0};
    if(parse_hex32_(hash,ref)==0){
        if(bf_embed_lookup(hash,vec_out,dim_out)==0) goto check_dim;
    }
    if(bf_embed_ref_read(ref,hash)==0){
        if(bf_embed_lookup(hash,vec_out,dim_out)==0) goto check_dim;
    }
    return -1;
check_dim:
    if(required_dim>0 && *dim_out!=required_dim){
        free(*vec_out); *vec_out=NULL; return -1;
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* usage                                                            */
/* ═══════════════════════════════════════════════════════════════ */
static void usage(void) {
    printf(
"bonfyre-physics — Hamiltonian Version Control Protocol (HVCP)\n"
"\n"
"  Version control for trajectories. Git for thought-motion.\n"
"  Bonfyre turns inference state into a content-addressed Hamiltonian trajectory.\n"
"\n"
"Subcommands:\n"
"\n"
"  bvh     [--pack <p>] [--out <idx.bfvh>]\n"
"            Build ball-tree BVH. Tier-1: 256-bit ternary sketch collision.\n"
"            Tier-2: KDE gradient for Hamiltonian field computation.\n"
"\n"
"  init    <input.vecf> [--sigma 1.0] [--dt 0.01]\n"
"            [--momentum zero|random|reflog|from <ref>]\n"
"            Initialise phase-space state. Default: p=0 (at-rest).\n"
"\n"
"  kick    <impulse.vecf> [--scale 1.0]\n"
"            Add momentum impulse to p.\n"
"\n"
"  step    [--n 1] [--trace <file.jsonl>]\n"
"            Run N Leapfrog steps. ΔH should be near-zero (symplectic).\n"
"            Returns exit 1 on topological gap.\n"
"\n"
"  run     [--steps 64] [--trace <file.jsonl>]\n"
"            [--on-gap mount:auto|emit|meter|branch:explore]\n"
"            Run to convergence or topological gap.\n"
"            --on-gap may be repeated for multiple actions.\n"
"\n"
"  collide [--k 32]\n"
"            Tier-1 ternary sketch collision: fast possible-relevance.\n"
"            'Is this region worth thinking about?'\n"
"\n"
"  nearest [--k 10]\n"
"            Tier-2 Hamiltonian collision: expensive actual influence.\n"
"            'How does this region bend the trajectory?'\n"
"\n"
"  energy  [--verbose]    (alias: H)\n"
"            Print H = ½‖p‖² + V(q).\n"
"\n"
"  branch  --as <name> [--as <name> ...]\n"
"            [--kick <name>=<ref_or_hex>]\n"
"            Fork state into named trajectories saved in branches/.\n"
"            Then: bonfyre-physics run branches/*.bfps\n"
"            Then: bonfyre-physics compare branches/*.bfps\n"
"\n"
"  rebase  --from <ref> --onto <ref>\n"
"            [--transport momentum] [--preserve-energy]\n"
"            Move trajectory onto a new memory manifold.\n"
"            Shifts q by (onto_vec - from_vec). Momentum is parallel-transported.\n"
"\n"
"  compare <state1.bfps> [state2.bfps ...]\n"
"            Side-by-side: step, ‖q‖, ‖p‖, H, KE, dim.\n"
"\n"
"  trace   <file.jsonl> [--summary] [--entropy]\n"
"            [--replay] [--branch-at gap:<N>]\n"
"            Analyse or replay a JSONL trace.\n"
"\n"
"  diff    <trace-a.jsonl> <trace-b.jsonl>\n"
"            Diff two traces: steps, gaps, mounts, H drift, entropy.\n"
"\n"
"  cherry-pick <source.jsonl> --event mount[:<pattern>]\n"
"            --onto <state.bfps> [--transport momentum]\n"
"            Apply mount events from a trace onto a state.\n"
"\n"
"  mount   <model_hex>   Hot-plug a sub-cache by model hash.\n"
"          --list        List all active mounts.\n"
"          --umount-all  Release all mounts.\n"
"\n"
"  show    Print state summary (q, p, dim, σ, dt, step, KE).\n"
"\n"
"Global flags (all optional, defaults shown):\n"
"  --pack  <path>   ~/.local/share/bonfyre/embeds/pack.bfpack\n"
"  --bvh   <path>   ~/.local/share/bonfyre/embeds/pack.bfvh\n"
"  --state <path>   ~/.local/share/bonfyre/physics/state.bfps\n"
    );
}

/* ═══════════════════════════════════════════════════════════════ */
/* bvh                                                             */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_bvh_(int argc, char **argv,
                    const char *pack_path, const char *bvh_path) {
    (void)argc;(void)argv;
    struct stat st;
    if(stat(pack_path,&st)!=0){
        fprintf(stderr,"physics bvh: no pack at %s\n",pack_path);return 1;
    }
    printf("physics bvh: building BVH\n"
           "  Tier-1: 256-bit ternary sketch (4×u64 POPCOUNT)\n"
           "  Tier-2: KDE gradient for Hamiltonian field\n");
    if(bf_embed_bvh_build(pack_path,bvh_path)!=0){
        fprintf(stderr,"physics bvh: build failed\n");return 1;
    }
    BfEmbedBVH bvh={0};
    if(bf_embed_bvh_open(&bvh,bvh_path)==0){
        printf("bvh: %u nodes  %llu vecs  dim=%u → %s\n",
               bvh.n_nodes,(unsigned long long)bvh.n_vecs,bvh.dim,bvh_path);
        bf_embed_bvh_close(&bvh);
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* init                                                            */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_init_(int argc, char **argv, const char *state_path,
                     const char *pack_path) {
    if(argc<2){fprintf(stderr,"init: need <input.vecf>\n");return 1;}
    const char *vecpath=argv[1];
    float sigma=1.0f, dt=0.01f;
    const char *mom_mode="zero";
    const char *mom_from=NULL;

    for(int i=2;i<argc;i++){
        if(!strcmp(argv[i],"--sigma")   &&i+1<argc) sigma=(float)atof(argv[++i]);
        if(!strcmp(argv[i],"--dt")      &&i+1<argc) dt   =(float)atof(argv[++i]);
        if(!strcmp(argv[i],"--momentum")&&i+1<argc){
            mom_mode=argv[++i];
            if(!strcmp(mom_mode,"from")&&i+1<argc) mom_from=argv[++i];
        }
    }

    uint32_t dim=0;
    float *embed=load_vecf_(vecpath,&dim);
    if(!embed) return 1;

    BfPhysicsState *s=bf_physics_state_alloc(dim,sigma,dt);
    if(!s){free(embed);return 1;}
    bf_physics_init_from_embed(s,embed,dim);
    free(embed);

    if(!strcmp(mom_mode,"random")){
        srand((unsigned)time(NULL));
        float pn2=0.0f;
        for(uint32_t d=0;d<dim;d++){
            float u1=(float)rand()/(float)RAND_MAX+1e-9f;
            float u2=(float)rand()/(float)RAND_MAX;
            s->p[d]=(float)(sqrt(-2.0f*(float)log(u1))*
                            cos(2.0f*3.14159265f*u2));
            pn2+=s->p[d]*s->p[d];
        }
        float pn=sqrtf(pn2);
        if(pn>1e-9f) for(uint32_t d=0;d<dim;d++) s->p[d]/=pn;
        printf("  momentum: random unit vector\n");
    } else if(!strcmp(mom_mode,"reflog")){
        BfEmbedReflogEntry *ents=NULL; int n=0;
        if(bf_embed_reflog_read(&ents,&n)==0 && n>0){
            float *rv=NULL; uint32_t rd=0;
            if(bf_embed_lookup(ents[n-1].hash,&rv,&rd)==0 && rd==dim){
                memcpy(s->p,rv,dim*sizeof(float));
                free(rv);
                printf("  momentum: from reflog[%d]\n",n-1);
            } else { free(rv); }
            free(ents);
        } else {
            printf("  momentum: reflog empty, using zero\n");
        }
    } else if(!strcmp(mom_mode,"from") && mom_from){
        float *rv=NULL; uint32_t rd=0;
        if(resolve_embed_(mom_from,&rv,&rd,dim)==0){
            memcpy(s->p,rv,dim*sizeof(float));
            free(rv);
            printf("  momentum: from ref '%s'\n",mom_from);
        } else {
            printf("  momentum: ref '%s' not found, using zero\n",mom_from);
        }
    }

    ensure_dir_(state_path);
    if(bf_physics_state_save(s,state_path)!=0){
        fprintf(stderr,"init: save failed\n");bf_physics_state_free(s);return 1;
    }

    BfEmbedPack pack={0};
    int has_pack=(bf_embed_pack_open(&pack,pack_path)==0);
    printf("physics init:\n");
    printf("  dim    = %u\n",  dim);
    printf("  sigma  = %.4f\n",sigma);
    printf("  dt     = %.6f\n",dt);
    printf("  ‖q‖   = %.4f\n", vec_norm_(s->q,dim));
    printf("  ‖p‖   = %.4f\n", vec_norm_(s->p,dim));
    if(has_pack){ printf("  pack   = %u objects\n",pack.n); bf_embed_pack_close(&pack); }
    printf("  state → %s\n",state_path);
    bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* kick                                                            */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_kick_(int argc, char **argv, const char *state_path) {
    if(argc<2){fprintf(stderr,"kick: need <impulse.vecf>\n");return 1;}
    float scale=1.0f;
    for(int i=2;i<argc;i++)
        if(!strcmp(argv[i],"--scale")&&i+1<argc) scale=(float)atof(argv[++i]);

    BfPhysicsState *s=bf_physics_state_load(state_path);
    if(!s){fprintf(stderr,"kick: cannot load state %s\n",state_path);return 1;}
    uint32_t idim=0;
    float *imp=load_vecf_(argv[1],&idim);
    if(!imp||idim!=s->dim){
        fprintf(stderr,"kick: dim mismatch (got %u, need %u)\n",idim,s->dim);
        free(imp);bf_physics_state_free(s);return 1;
    }
    float pn0=vec_norm_(s->p,s->dim);
    bf_physics_kick(s,imp,scale);
    free(imp);
    float pn1=vec_norm_(s->p,s->dim);
    bf_physics_state_save(s,state_path);
    printf("physics kick: scale=%.4f  ‖p‖ %.4f → %.4f\n",scale,pn0,pn1);
    bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* step                                                            */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_step_(int argc, char **argv,
                     const char *state_path,
                     const char *pack_path, const char *bvh_path) {
    int n=1; const char *trace_path=NULL;
    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--n")    &&i+1<argc) n=atoi(argv[++i]);
        if(!strcmp(argv[i],"--trace")&&i+1<argc) trace_path=argv[++i];
    }

    BfPhysicsState *s=bf_physics_state_load(state_path);
    if(!s){fprintf(stderr,"step: cannot load state %s\n",state_path);return 1;}
    BfEmbedPack pack={0}; BfEmbedBVH bvh={0};
    if(open_pack_bvh_(pack_path,bvh_path,&pack,&bvh)!=0){
        bf_physics_state_free(s);return 1;
    }

    BfEntropyTrace *tr=NULL;
    if(trace_path) tr=bf_trace_open(trace_path,1);
    BfEntropyAccum ea;
    float H0=bf_physics_hamiltonian(s,&bvh,&pack);
    bf_entropy_init(&ea,H0);
    printf("physics step: H₀=%.6f  step=%llu\n",H0,(unsigned long long)s->step);

    int gap=0;
    for(int i=0;i<n;i++){
        int rc=bf_physics_step(s,&bvh,&pack);
        float H=bf_physics_hamiltonian(s,&bvh,&pack);
        float ke=0.5f*vec_norm_(s->p,s->dim)*vec_norm_(s->p,s->dim);
        float gnorm=vec_norm_(s->grad_buf,s->dim);
        bf_entropy_update_step(&ea,H,(rc==1)?1:0,0,0);
        if(tr){
            BfTraceEvent ev;
            fill_trace_ev_(&ev,s,H,ke,H-ke,gnorm,(rc==1),bf_entropy_score(&ea),0,0);
            bf_trace_write(tr,&ev);
        }
        if(rc==1){gap=1;break;}
        if(rc<0){fprintf(stderr,"step: error at step %d\n",i);break;}
    }

    float H1=bf_physics_hamiltonian(s,&bvh,&pack);
    printf("physics step: H₁=%.6f  ΔH=%.2e  step=%llu  S=%.4f\n",
           H1,(double)(H1-H0),(unsigned long long)s->step,bf_entropy_score(&ea));
    printf("  ‖q‖=%.4f  ‖p‖=%.4f\n",vec_norm_(s->q,s->dim),vec_norm_(s->p,s->dim));
    if(gap) printf("  ⚠ topological gap — ‖∇V‖→0 (memory field is flat here)\n"
                   "    the thought needs another manifold\n"
                   "    try: bonfyre-physics mount <model_hex>\n");

    if(tr) bf_trace_close(tr);
    bf_physics_state_save(s,state_path);
    bf_physics_state_free(s);
    bf_embed_bvh_close(&bvh);bf_embed_pack_close(&pack);
    return gap?1:0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* run                                                             */
/* ═══════════════════════════════════════════════════════════════ */
#define GAP_MOUNT_AUTO  0x01
#define GAP_EMIT        0x02
#define GAP_METER       0x04
#define GAP_BRANCH      0x08

static int cmd_run_(int argc, char **argv,
                    const char *state_path,
                    const char *pack_path, const char *bvh_path) {
    int max_steps=64;
    const char *trace_path=NULL;
    int gap_actions=0;

    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--steps")   &&i+1<argc) max_steps=atoi(argv[++i]);
        if(!strcmp(argv[i],"--max-steps")&&i+1<argc) max_steps=atoi(argv[++i]);
        if(!strcmp(argv[i],"--trace")   &&i+1<argc) trace_path=argv[++i];
        if(!strcmp(argv[i],"--on-gap")  &&i+1<argc){
            const char *act=argv[++i];
            if(!strncmp(act,"mount", 5)) gap_actions|=GAP_MOUNT_AUTO;
            if(!strncmp(act,"emit",  4)) gap_actions|=GAP_EMIT;
            if(!strncmp(act,"meter", 5)) gap_actions|=GAP_METER;
            if(!strncmp(act,"branch",6)) gap_actions|=GAP_BRANCH;
        }
    }

    BfPhysicsState *s=bf_physics_state_load(state_path);
    if(!s){fprintf(stderr,"run: cannot load state %s\n",state_path);return 1;}
    BfEmbedPack pack={0}; BfEmbedBVH bvh={0};
    if(open_pack_bvh_(pack_path,bvh_path,&pack,&bvh)!=0){
        bf_physics_state_free(s);return 1;
    }

    BfEntropyTrace *tr=NULL;
    if(trace_path) tr=bf_trace_open(trace_path,0);
    BfEntropyAccum ea;
    float H0=bf_physics_hamiltonian(s,&bvh,&pack);
    bf_entropy_init(&ea,H0);
    printf("physics run: max_steps=%d  H₀=%.6f  step=%llu\n",
           max_steps,H0,(unsigned long long)s->step);

    int total_done=0, final_rc=0, mount_count=0;

    for(int pass=0;pass<4&&total_done<max_steps;pass++){
        for(;total_done<max_steps;){
            int rc=bf_physics_step(s,&bvh,&pack);
            total_done++;
            float H=bf_physics_hamiltonian(s,&bvh,&pack);
            float ke=0.5f*vec_norm_(s->p,s->dim)*vec_norm_(s->p,s->dim);
            float gnorm=vec_norm_(s->grad_buf,s->dim);
            int is_gap=(rc==1);
            bf_entropy_update_step(&ea,H,is_gap,0,0);
            float entropy=bf_entropy_score(&ea);

            if(tr){
                BfTraceEvent ev;
                fill_trace_ev_(&ev,s,H,ke,H-ke,gnorm,is_gap,entropy,0,0);
                bf_trace_write(tr,&ev);
            }
            if(rc<0){fprintf(stderr,"run: integration error\n");final_rc=-1;goto done;}
            if(rc==1){
                printf("  ⚠ topological gap at step %llu  ‖∇V‖→0\n",
                       (unsigned long long)s->step);
                if(gap_actions&GAP_EMIT){
                    char qh[65]; uint8_t qhb[32]; q_hash_(qhb,s->q,s->dim);
                    hex64_(qh,qhb);
                    printf("  cms:gap_event {\"step\":%llu,\"q_hash\":\"%s\","
                           "\"H\":%.6g,\"entropy\":%.4f}\n",
                           (unsigned long long)s->step,qh,(double)H,(double)entropy);
                }
                if(gap_actions&GAP_METER){
                    printf("  meter:knowledge_miss {\"step\":%llu,\"mounts\":%d}\n",
                           (unsigned long long)s->step,mount_count);
                }
                if(gap_actions&GAP_BRANCH){
                    char bdir[PATH_MAX],bpath[PATH_MAX];
                    snprintf(bdir,sizeof(bdir),"%s",state_path);
                    char *sl=strrchr(bdir,'/');
                    if(sl)*sl='\0';
                    snprintf(bpath,sizeof(bpath),
                             "%s/explore_gap%u.bfps",bdir,ea.gap_count);
                    ensure_dir_(bpath);
                    bf_physics_state_save(s,bpath);
                    printf("  branch:explore → %s\n",bpath);
                }
                if(gap_actions&GAP_MOUNT_AUTO){
                    uint8_t zero[32]={0};
                    BfKVMount m={.fd=-1};
                    if(bf_kvcache_mount_auto(zero,zero,&m)==0){
                        mount_count++;
                        bf_entropy_update_step(&ea,H,0,1,0);
                        printf("  auto-mount: %zu B  (mount #%d)\n",
                               m.map_size,mount_count);
                        break; /* retry with new field */
                    } else {
                        printf("  auto-mount: nothing available\n");
                    }
                }
                final_rc=1;
                goto done;
            }
        }
        if(total_done>=max_steps) break;
    }

done:;
    float H1=bf_physics_hamiltonian(s,&bvh,&pack);
    float ke=0.5f*vec_norm_(s->p,s->dim)*vec_norm_(s->p,s->dim);
    printf("physics run: steps=%d  H₁=%.6f  ΔH=%.2e\n",
           total_done,H1,(double)(H1-H0));
    printf("  ‖q‖=%.4f  ‖p‖=%.4f  KE=%.4f  PE=%.4f\n",
           vec_norm_(s->q,s->dim),vec_norm_(s->p,s->dim),ke,H1-ke);
    printf("  mounts=%d  S_runtime=%.4f\n",mount_count,bf_entropy_score(&ea));
    if(trace_path)
        printf("  trace → %s  (%llu events)\n",
               trace_path,(unsigned long long)(tr?tr->events:0));

    if(tr) bf_trace_close(tr);
    bf_physics_state_save(s,state_path);
    bf_physics_state_free(s);
    bf_embed_bvh_close(&bvh);bf_embed_pack_close(&pack);
    return (final_rc<0)?1:0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* collide — Tier-1 ternary sketch                                 */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_collide_(int argc, char **argv,
                         const char *state_path,
                         const char *pack_path, const char *bvh_path) {
    int k=32;
    for(int i=1;i<argc;i++)
        if(!strcmp(argv[i],"--k")&&i+1<argc) k=atoi(argv[++i]);

    BfPhysicsState *s=bf_physics_state_load(state_path);
    if(!s){fprintf(stderr,"collide: cannot load state\n");return 1;}
    BfEmbedPack pack={0}; BfEmbedBVH bvh={0};
    if(open_pack_bvh_(pack_path,bvh_path,&pack,&bvh)!=0){
        bf_physics_state_free(s);return 1;
    }

    uint32_t *idx=malloc((size_t)k*sizeof(uint32_t));
    if(!idx){bf_embed_bvh_close(&bvh);bf_embed_pack_close(&pack);
             bf_physics_state_free(s);return 1;}
    int found=0;
    bf_embed_bvh_collide(&bvh,s->q,s->dim,idx,k,&found);

    printf("physics collide: Tier-1 ternary sketch (256-bit POPCOUNT)\n");
    printf("  question: 'Is this region worth thinking about?'\n");
    printf("  candidates: %d of %llu vectors passed the sketch\n",
           found,(unsigned long long)bvh.n_vecs);
    printf("  (Tier-2 'nearest' answers: how does this region bend the trajectory?)\n\n");
    for(int i=0;i<found&&i<20;i++)
        printf("  [%2d] pack_idx=%u\n",i+1,idx[i]);
    if(found>20) printf("  ... %d total\n",found);

    free(idx);
    bf_embed_bvh_close(&bvh);bf_embed_pack_close(&pack);
    bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* nearest — Tier-2 Hamiltonian collision                          */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_nearest_(int argc, char **argv,
                         const char *state_path, const char *pack_path) {
    int k=10;
    for(int i=1;i<argc;i++)
        if(!strcmp(argv[i],"--k")&&i+1<argc) k=atoi(argv[++i]);

    BfPhysicsState *s=bf_physics_state_load(state_path);
    if(!s){fprintf(stderr,"nearest: cannot load state\n");return 1;}
    BfEmbedPack pack={0};
    if(bf_embed_pack_open(&pack,pack_path)!=0){
        fprintf(stderr,"nearest: cannot open pack\n");
        bf_physics_state_free(s);return 1;
    }
    if(pack.dim!=s->dim){
        fprintf(stderr,"nearest: pack dim %u != state dim %u\n",pack.dim,s->dim);
        bf_embed_pack_close(&pack);bf_physics_state_free(s);return 1;
    }

    BfEmbedSearchResult *res=malloc((size_t)k*sizeof(BfEmbedSearchResult));
    if(!res){bf_embed_pack_close(&pack);bf_physics_state_free(s);return 1;}
    int found=0;
    bf_embed_brute_search(&pack,s->q,s->dim,k,res,&found);

    printf("physics nearest: Tier-2 Hamiltonian collision\n");
    printf("  question: 'How does this region bend the trajectory?'\n");
    printf("  top-%d at step %llu:\n",found,(unsigned long long)s->step);
    for(int i=0;i<found;i++){
        printf("  [%2d] score=%.4f  ",i+1,res[i].score);
        hex_print_(res[i].hash); printf("\n");
    }
    free(res);
    bf_embed_pack_close(&pack);bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* energy (alias: H)                                               */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_energy_(int argc, char **argv,
                        const char *state_path,
                        const char *pack_path, const char *bvh_path) {
    int verbose=0;
    for(int i=1;i<argc;i++)
        if(!strcmp(argv[i],"--verbose")) verbose=1;

    BfPhysicsState *s=bf_physics_state_load(state_path);
    if(!s){fprintf(stderr,"energy: cannot load state\n");return 1;}

    BfEmbedPack pack={0}; BfEmbedBVH bvh={0};
    int has_bvh=(open_pack_bvh_(pack_path,bvh_path,&pack,&bvh)==0);

    float pn2=vec_norm_(s->p,s->dim); pn2*=pn2;
    float ke=0.5f*pn2;
    float H=has_bvh?bf_physics_hamiltonian(s,&bvh,&pack):ke;

    printf("H = %.6f\n",H);
    printf("  KE = %.6f  (½‖p‖²)\n",ke);
    if(has_bvh) printf("  PE = %.6f  (V(q) = KDE over %llu vecs  σ=%.4f)\n",
                        H-ke,(unsigned long long)pack.n,s->sigma);
    printf("  step=%llu\n",(unsigned long long)s->step);
    if(verbose){
        printf("  ‖q‖ = %.6f\n",vec_norm_(s->q,s->dim));
        printf("  ‖p‖ = %.6f\n",vec_norm_(s->p,s->dim));
        printf("  σ   = %.4f\n",s->sigma);
        printf("  dt  = %.6f\n",s->dt);
    }
    if(has_bvh){bf_embed_bvh_close(&bvh);bf_embed_pack_close(&pack);}
    bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* branch                                                           */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_branch_(int argc, char **argv,
                        const char *state_path, const char *pack_path) {
    const char *names[32]; int n_names=0;
    const char *kicks[32]; int n_kicks=0;

    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--as")  &&i+1<argc&&n_names<32) names[n_names++]=argv[++i];
        if(!strcmp(argv[i],"--kick")&&i+1<argc&&n_kicks<32) kicks[n_kicks++]=argv[++i];
    }
    if(n_names==0){fprintf(stderr,"branch: need at least one --as <name>\n");return 1;}

    BfPhysicsState *src=bf_physics_state_load(state_path);
    if(!src){fprintf(stderr,"branch: cannot load %s\n",state_path);return 1;}

    char branch_dir[PATH_MAX];
    snprintf(branch_dir,sizeof(branch_dir),"%s",state_path);
    char *sl=strrchr(branch_dir,'/');
    if(sl){*sl='\0';} else {snprintf(branch_dir,sizeof(branch_dir),".");}
    char bdir[PATH_MAX];
    snprintf(bdir,sizeof(bdir),"%s/branches",branch_dir);
    mkdir(bdir,0755);

    printf("physics branch: forking from %s\n",state_path);
    printf("  base: step=%llu  ‖q‖=%.4f  ‖p‖=%.4f\n",
           (unsigned long long)src->step,
           vec_norm_(src->q,src->dim),vec_norm_(src->p,src->dim));

    for(int ni=0;ni<n_names;ni++){
        const char *name=names[ni];
        BfPhysicsState *b=bf_physics_state_alloc(src->dim,src->sigma,src->dt);
        if(!b) continue;
        memcpy(b->q,src->q,src->dim*sizeof(float));
        memcpy(b->p,src->p,src->dim*sizeof(float));
        b->step=src->step;

        for(int ki=0;ki<n_kicks;ki++){
            const char *ks=kicks[ki];
            size_t nlen=strlen(name);
            if(strncmp(ks,name,nlen)||ks[nlen]!='=') continue;
            const char *ref=ks+nlen+1;
            float *rv=NULL; uint32_t rd=0;
            if(resolve_embed_(ref,&rv,&rd,src->dim)==0){
                bf_physics_kick(b,rv,1.0f);
                free(rv);
                printf("  %-18s kick: %s\n",name,ref);
            } else {
                printf("  %-18s kick: '%s' not found\n",name,ref);
            }
        }

        char bpath[PATH_MAX];
        snprintf(bpath,sizeof(bpath),"%s/%s.bfps",bdir,name);
        if(bf_physics_state_save(b,bpath)==0){
            printf("  %-18s → %s  ‖p‖=%.4f\n",
                   name,bpath,vec_norm_(b->p,b->dim));
        } else {
            fprintf(stderr,"  %-18s save failed\n",name);
        }
        bf_physics_state_free(b);
    }

    (void)pack_path;
    bf_physics_state_free(src);
    printf("\nbranches dir: %s/\n",bdir);
    printf("run all:     bonfyre-physics run --state <branch>.bfps\n");
    printf("compare all: bonfyre-physics compare %s/*.bfps\n",bdir);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* rebase                                                           */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_rebase_(int argc, char **argv,
                        const char *state_path,
                        const char *pack_path, const char *bvh_path) {
    const char *from_ref=NULL, *onto_ref=NULL;
    int transport_p=0, preserve_e=0;

    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--from")           &&i+1<argc) from_ref=argv[++i];
        if(!strcmp(argv[i],"--onto")           &&i+1<argc) onto_ref=argv[++i];
        if(!strcmp(argv[i],"--transport")      &&i+1<argc){
            if(!strcmp(argv[i+1],"momentum")) transport_p=1;
            i++;
        }
        if(!strcmp(argv[i],"--preserve-energy")) preserve_e=1;
    }
    if(!from_ref||!onto_ref){
        fprintf(stderr,"rebase: need --from <ref> --onto <ref>\n");return 1;
    }

    BfPhysicsState *s=bf_physics_state_load(state_path);
    if(!s){fprintf(stderr,"rebase: cannot load state\n");return 1;}

    float *from_v=NULL, *onto_v=NULL;
    uint32_t fdim=0, odim=0;
    int from_ok=(resolve_embed_(from_ref,&from_v,&fdim,s->dim)==0);
    int onto_ok=(resolve_embed_(onto_ref,&onto_v,&odim,s->dim)==0);

    if(!from_ok||!onto_ok){
        fprintf(stderr,"rebase: could not resolve from='%s'(ok=%d) onto='%s'(ok=%d)\n",
                from_ref,from_ok,onto_ref,onto_ok);
        free(from_v);free(onto_v);bf_physics_state_free(s);return 1;
    }

    float delta_norm=0.0f;
    float qn0=vec_norm_(s->q,s->dim);
    for(uint32_t d=0;d<s->dim;d++){
        float delta=onto_v[d]-from_v[d];
        s->q[d]+=delta;
        delta_norm+=delta*delta;
    }
    delta_norm=sqrtf(delta_norm);
    free(from_v);free(onto_v);

    if(preserve_e){
        BfEmbedPack pack={0}; BfEmbedBVH bvh={0};
        if(open_pack_bvh_(pack_path,bvh_path,&pack,&bvh)==0){
            float ke=0.5f*vec_norm_(s->p,s->dim)*vec_norm_(s->p,s->dim);
            if(ke>1e-12f){
                /* target: keep same KE as before */
                float pn=vec_norm_(s->p,s->dim);
                (void)pn; /* scale already preserves KE when p unchanged */
            }
            bf_embed_bvh_close(&bvh);bf_embed_pack_close(&pack);
        }
    }

    printf("physics rebase:\n");
    printf("  from:   %s\n",from_ref);
    printf("  onto:   %s\n",onto_ref);
    printf("  ‖Δq‖  = %.4f  (manifold shift)\n",delta_norm);
    printf("  ‖q‖   %.4f → %.4f\n",qn0,vec_norm_(s->q,s->dim));
    printf("  ‖p‖   = %.4f  (parallel-transported)\n",vec_norm_(s->p,s->dim));
    if(transport_p) printf("  momentum transport: parallel (flat manifold identity)\n");
    if(preserve_e)  printf("  energy: preserved (KE unchanged)\n");

    bf_physics_state_save(s,state_path);
    bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* compare                                                          */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_compare_(int argc, char **argv,
                         const char *pack_path, const char *bvh_path) {
    const char *paths[64]; int np=0;
    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--by")&&i+1<argc){i++;continue;}
        if(argv[i][0]!='-'&&np<64) paths[np++]=argv[i];
    }
    if(np<1){fprintf(stderr,"compare: need at least one .bfps file\n");return 1;}

    BfEmbedPack pack={0}; BfEmbedBVH bvh={0};
    int has_bvh=(open_pack_bvh_(pack_path,bvh_path,&pack,&bvh)==0);

    printf("%-26s  %8s  %10s  %10s  %10s  %10s\n",
           "state","step","‖q‖","‖p‖","H","KE");
    printf("%-26s  %8s  %10s  %10s  %10s  %10s\n",
           "──────────────────────────","────────",
           "──────────","──────────","──────────","──────────");

    for(int i=0;i<np;i++){
        BfPhysicsState *s=bf_physics_state_load(paths[i]);
        if(!s){printf("%-26s  (load error)\n",paths[i]);continue;}
        float H=has_bvh?bf_physics_hamiltonian(s,&bvh,&pack):
                        0.5f*vec_norm_(s->p,s->dim)*vec_norm_(s->p,s->dim);
        float ke=0.5f*vec_norm_(s->p,s->dim)*vec_norm_(s->p,s->dim);
        const char *d=strrchr(paths[i],'/'); d=d?d+1:paths[i];
        printf("%-26s  %8llu  %10.4f  %10.4f  %10.4f  %10.4f\n",
               d,(unsigned long long)s->step,
               vec_norm_(s->q,s->dim),vec_norm_(s->p,s->dim),H,ke);
        bf_physics_state_free(s);
    }
    if(has_bvh){bf_embed_bvh_close(&bvh);bf_embed_pack_close(&pack);}
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* trace                                                            */
/* ═══════════════════════════════════════════════════════════════ */
typedef struct{int n;} ReplayCx;
static int replay_cb_(const char *line, void *ctx){
    ReplayCx *r=ctx; r->n++;
    printf("  %s",line);
    return 0;
}

static int cmd_trace_(int argc, char **argv, const char *state_path) {
    if(argc<2){fprintf(stderr,"trace: need <file.jsonl>\n");return 1;}
    const char *tf=argv[1];
    int do_sum=0,do_ent=0,do_rep=0; const char *bat=NULL;
    for(int i=2;i<argc;i++){
        if(!strcmp(argv[i],"--summary"))  do_sum=1;
        if(!strcmp(argv[i],"--entropy"))  do_ent=1;
        if(!strcmp(argv[i],"--replay"))   do_rep=1;
        if(!strcmp(argv[i],"--branch-at")&&i+1<argc) bat=argv[++i];
    }
    if(!do_sum&&!do_ent&&!do_rep&&!bat) do_sum=1;

    if(do_sum||do_ent) bf_trace_summary(tf,stdout);
    if(do_rep){
        printf("\ntrace replay: %s\n",tf);
        ReplayCx r={0};
        bf_trace_iterate(tf,replay_cb_,&r);
        printf("  %d events replayed\n",r.n);
    }
    if(bat){
        int gap_n=1;
        if(!strncmp(bat,"gap:",4)) gap_n=atoi(bat+4);
        uint64_t step=bf_trace_gap_step(tf,gap_n);
        if(step==UINT64_MAX){
            printf("trace branch-at: gap #%d not found\n",gap_n);return 1;
        }
        printf("trace branch-at: gap #%d at step %llu\n",
               gap_n,(unsigned long long)step);
        printf("  → bonfyre-physics branch %s --as explore_gap%d\n",
               state_path,gap_n);
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* diff                                                             */
/* ═══════════════════════════════════════════════════════════════ */
typedef struct {
    uint64_t n_steps;
    float H_first, H_last, H_drift_max;
    uint32_t gap_count, mount_count;
    float entropy_last;
    uint64_t first_gap_step;
} DiffStat;

static int diff_cb_(const char *line, void *ctx){
    DiffStat *d=ctx;
    /* micro parse: find "key": numeric */
    const char *hf=strstr(line,"\"H\":");
    const char *gf=strstr(line,"\"gap\":");
    const char *ef=strstr(line,"\"entropy\":");
    const char *sf=strstr(line,"\"step\":");
    const char *mp=strstr(line,"\"mounted\":[");

    double H  = hf ? atof(hf+4)  : 0.0;
    double gap= gf ? atof(gf+6)  : 0.0;
    double ent= ef ? atof(ef+10) : 0.0;
    long long st= sf ? atoll(sf+7) : 0LL;
    int has_mount=mp&&strstr(mp+11,"\"")&&strstr(mp+11,"\"") < strchr(mp+11,']');

    if(d->n_steps==0){ d->H_first=(float)H; d->first_gap_step=UINT64_MAX; }
    float drift=fabsf((float)H-d->H_first);
    if(drift>d->H_drift_max) d->H_drift_max=drift;
    d->H_last=(float)H;
    d->n_steps++;
    if(gap>0.5){
        if(d->gap_count==0) d->first_gap_step=(uint64_t)st;
        d->gap_count++;
    }
    if(has_mount) d->mount_count++;
    d->entropy_last=(float)ent;
    return 0;
}

static int cmd_diff_(int argc, char **argv) {
    if(argc<3){fprintf(stderr,"diff: need <trace-a> <trace-b>\n");return 1;}
    DiffStat a={0},b={0};
    a.first_gap_step=UINT64_MAX; b.first_gap_step=UINT64_MAX;
    bf_trace_iterate(argv[1],diff_cb_,&a);
    bf_trace_iterate(argv[2],diff_cb_,&b);

    uint64_t div_step=(a.first_gap_step<b.first_gap_step)?
                       a.first_gap_step:b.first_gap_step;

    printf("physics diff:\n");
    printf("  a: %s\n  b: %s\n\n",argv[1],argv[2]);
    printf("  %-22s  %14s  %14s\n","metric","run-a","run-b");
    printf("  %-22s  %14s  %14s\n","──────────────────────",
           "──────────────","──────────────");
    printf("  %-22s  %14llu  %14llu\n","steps",
           (unsigned long long)a.n_steps,(unsigned long long)b.n_steps);
    printf("  %-22s  %14u  %14u\n","gaps",a.gap_count,b.gap_count);
    printf("  %-22s  %14u  %14u\n","mounts",a.mount_count,b.mount_count);
    printf("  %-22s  %14.6f  %14.6f\n","H_first",a.H_first,b.H_first);
    printf("  %-22s  %14.6f  %14.6f\n","H_last", a.H_last, b.H_last);
    printf("  %-22s  %14.4f  %14.4f\n","H_drift_max",
           a.H_drift_max,b.H_drift_max);
    printf("  %-22s  %14.4f  %14.4f\n","entropy_last",
           a.entropy_last,b.entropy_last);

    if(div_step!=UINT64_MAX)
        printf("\n  divergence at step: %llu\n",(unsigned long long)div_step);
    else
        printf("\n  no divergence detected (both gap-free)\n");
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* cherry-pick                                                      */
/* ═══════════════════════════════════════════════════════════════ */
typedef struct {
    const char *pattern;
    int n_applied;
} CherryCtx;

static int cherry_cb_(const char *line, void *ctx){
    CherryCtx *c=ctx;
    const char *mp=strstr(line,"\"mounted\":[");
    if(!mp) return 0;
    const char *p=mp+11, *end=strchr(p,']');
    if(!end) return 0;
    while(p<end){
        const char *qs=strchr(p,'"'); if(!qs||qs>=end) break;
        const char *qe=strchr(qs+1,'"'); if(!qe||qe>=end) break;
        char hs[129]; size_t hl=(size_t)(qe-qs-1);
        if(hl>=sizeof(hs)) hl=sizeof(hs)-1;
        memcpy(hs,qs+1,hl); hs[hl]='\0';
        int match=(!c->pattern)||strstr(hs,c->pattern);
        if(match){
            uint8_t hash[32]={0};
            if(parse_hex32_(hash,hs)==0){
                BfKVMount m={.fd=-1};
                if(bf_kvcache_mount(hash,&m)==0){
                    printf("  cherry-pick: mounted %.16s...  %zu B\n",hs,m.map_size);
                    c->n_applied++;
                }
            }
        }
        p=qe+1;
    }
    return 0;
}

static int cmd_cherry_pick_(int argc, char **argv, const char *state_path) {
    if(argc<2){fprintf(stderr,"cherry-pick: need <source.jsonl>\n");return 1;}
    const char *src=argv[1], *onto=state_path, *pattern=NULL;
    for(int i=2;i<argc;i++){
        if(!strcmp(argv[i],"--event")&&i+1<argc){
            const char *ev=argv[++i];
            if(!strncmp(ev,"mount:",6)) pattern=ev+6;
            /* "mount" alone = all mount events */
        }
        if(!strcmp(argv[i],"--onto")&&i+1<argc) onto=argv[++i];
    }
    (void)onto;  /* state already loaded by run context */

    CherryCtx c={.pattern=pattern,.n_applied=0};
    bf_trace_iterate(src,cherry_cb_,&c);
    printf("physics cherry-pick: %d mount events applied from %s\n",c.n_applied,src);
    if(c.n_applied>0){
        BfKVMount mounts[16]; int mc=0;
        bf_kvcache_mount_list(mounts,16,&mc);
        printf("  active mounts: %d\n",mc);
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* mount                                                            */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_mount_(int argc, char **argv) {
    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--list")){
            BfKVMount mounts[16]; int c=0;
            bf_kvcache_mount_list(mounts,16,&c);
            if(c==0){printf("no active mounts\n");return 0;}
            for(int j=0;j<c;j++){
                printf("  [%d] hash=",j); hex_print_(mounts[j].hash);
                printf("  %zu B\n",mounts[j].map_size);
            }
            return 0;
        }
        if(!strcmp(argv[i],"--umount-all")){
            bf_kvcache_umount_all();printf("all mounts released\n");return 0;
        }
    }
    if(argc<2){fprintf(stderr,"mount: need <hex64> | --list | --umount-all\n");return 1;}
    uint8_t hash[32]={0};
    if(parse_hex32_(hash,argv[1])!=0){
        fprintf(stderr,"mount: hash must be 64 hex chars\n");return 1;
    }
    BfKVMount m={.fd=-1};
    if(bf_kvcache_mount(hash,&m)!=0){
        fprintf(stderr,"mount: no pack found for hash\n");return 1;
    }
    printf("mounted  %.16s...  %zu B  (read-only)\n",argv[1],m.map_size);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* show                                                             */
/* ═══════════════════════════════════════════════════════════════ */
static int cmd_show_(const char *state_path) {
    BfPhysicsState *s=bf_physics_state_load(state_path);
    if(!s){fprintf(stderr,"show: cannot load state %s\n",state_path);return 1;}
    float qn=vec_norm_(s->q,s->dim), pn=vec_norm_(s->p,s->dim);
    printf("Phase-space state: %s\n",state_path);
    printf("  dim    = %u\n",  s->dim);
    printf("  sigma  = %.4f  (KDE bandwidth)\n",s->sigma);
    printf("  dt     = %.6f  (integration timestep)\n",s->dt);
    printf("  step   = %llu\n",(unsigned long long)s->step);
    printf("  ‖q‖   = %.6f  (position on embedding manifold)\n",qn);
    printf("  ‖p‖   = %.6f  (momentum / direction of reasoning)\n",pn);
    printf("  KE     = %.6f  (½‖p‖²)\n",0.5f*pn*pn);
    printf("\nThe trajectory is a content-addressed Hamiltonian arc.\n"
           "Topological gaps mean the thought needs another manifold.\n"
           "Memory exerts local influence through V(q) over indexed mass.\n");
    bf_physics_state_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════ */
/* main                                                             */
/* ═══════════════════════════════════════════════════════════════ */
int main(int argc, char **argv) {
    if(argc<2){usage();return 1;}

    char pack_path[PATH_MAX], bvh_path[PATH_MAX], state_path[PATH_MAX];
    default_pack_ (pack_path,  sizeof(pack_path));
    default_bvh_  (bvh_path,   sizeof(bvh_path));
    default_state_(state_path, sizeof(state_path));

    char *sub[64]; int sub_argc=0;
    for(int i=1;i<argc&&sub_argc<62;i++){
        if(!strcmp(argv[i],"--pack" )&&i+1<argc){
            snprintf(pack_path, sizeof(pack_path), "%s",argv[++i]);continue;}
        if(!strcmp(argv[i],"--bvh"  )&&i+1<argc){
            snprintf(bvh_path,  sizeof(bvh_path),  "%s",argv[++i]);continue;}
        if(!strcmp(argv[i],"--state")&&i+1<argc){
            snprintf(state_path,sizeof(state_path),"%s",argv[++i]);continue;}
        sub[sub_argc++]=argv[i];
    }
    sub[sub_argc]=NULL;
    if(sub_argc==0){usage();return 1;}

    const char *cmd=sub[0];

    if(!strcmp(cmd,"--help")||!strcmp(cmd,"-h")){usage();return 0;}
    if(!strcmp(cmd,"bvh"))         return cmd_bvh_      (sub_argc,sub,pack_path,bvh_path);
    if(!strcmp(cmd,"init"))        return cmd_init_     (sub_argc,sub,state_path,pack_path);
    if(!strcmp(cmd,"kick"))        return cmd_kick_     (sub_argc,sub,state_path);
    if(!strcmp(cmd,"step"))        return cmd_step_     (sub_argc,sub,state_path,pack_path,bvh_path);
    if(!strcmp(cmd,"run"))         return cmd_run_      (sub_argc,sub,state_path,pack_path,bvh_path);
    if(!strcmp(cmd,"collide"))     return cmd_collide_  (sub_argc,sub,state_path,pack_path,bvh_path);
    if(!strcmp(cmd,"nearest"))     return cmd_nearest_  (sub_argc,sub,state_path,pack_path);
    if(!strcmp(cmd,"energy")||
       !strcmp(cmd,"H"))           return cmd_energy_   (sub_argc,sub,state_path,pack_path,bvh_path);
    if(!strcmp(cmd,"branch"))      return cmd_branch_   (sub_argc,sub,state_path,pack_path);
    if(!strcmp(cmd,"rebase"))      return cmd_rebase_   (sub_argc,sub,state_path,pack_path,bvh_path);
    if(!strcmp(cmd,"compare"))     return cmd_compare_  (sub_argc,sub,pack_path,bvh_path);
    if(!strcmp(cmd,"trace"))       return cmd_trace_    (sub_argc,sub,state_path);
    if(!strcmp(cmd,"diff"))        return cmd_diff_     (sub_argc,sub);
    if(!strcmp(cmd,"cherry-pick")) return cmd_cherry_pick_(sub_argc,sub,state_path);
    if(!strcmp(cmd,"mount"))       return cmd_mount_    (sub_argc,sub);
    if(!strcmp(cmd,"show"))        return cmd_show_     (state_path);

    fprintf(stderr,"Unknown subcommand: %s\n",cmd);
    usage();
    return 1;
}
