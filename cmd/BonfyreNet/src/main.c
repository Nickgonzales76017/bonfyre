/*
 * bonfyre-net — Bonfyre Netlist Runtime
 *
 * Mixed-signal inference runtime for the Bonfyre component network.
 *
 * Bonfyre binaries are not pipeline stages. They are components in a
 * mixed-signal circuit. The KV cache is the substrate. The netlist is
 * the circuit. The attention ball is the signal.
 *
 *   Digital side:  hashes, refs, commits, contracts, mounts, events
 *   Analog side:   q, p, energy, potential, entropy, conductance, noise
 *
 * Subcommands:
 *
 *   check   <file.bfnet>
 *             Validate a netlist. Print component/net summary.
 *
 *   compile <file.bfnet> [--out <file.bfcircuit>]
 *             Compile netlist to binary circuit. Resolves component
 *             types, pins, and net connections.
 *
 *   run     <file.bfnet|bfcircuit> [--tran N] [--dt F]
 *             [--probe p1,p2,...] [--trace <out.jsonl>]
 *             [--on-gap mount:auto|branch|emit]
 *             Run transient simulation. One line per step.
 *
 *   probe   <file.bfcircuit> <pin>...
 *             Print last-known value of one or more pins.
 *
 *   graph   <file.bfnet>
 *             Print component/net topology (ASCII).
 *
 *   components
 *             List all registered component types with pin descriptions.
 *
 * .bfnet example:
 *
 *   .WORLD demo
 *   .COMPONENT tel0   BonfyreTel     mode=sim
 *   .COMPONENT emb0   BonfyreEmbed   pack=acme.bfepack
 *   .COMPONENT kv0    BonfyreKVCache model=llama3
 *   .COMPONENT phy0   BonfyrePhysics dt=0.05 sigma=0.8
 *   .COMPONENT disc0  BonfyreDisCIPL rules=live.discipl
 *   .COMPONENT met0   BonfyreMeter   key=acme
 *   .COMPONENT led0   BonfyreLedger  account=acme
 *
 *   .NET tel0.signal    emb0.input    signal
 *   .NET emb0.q         phy0.q        vector
 *   .NET kv0.mounts     phy0.field    memory
 *   .NET disc0.rules    phy0.constraints rule
 *   .NET phy0.entropy   met0.usage    cost
 *   .NET met0.invoice   led0.value    value
 *
 *   .TRAN 0 128 0.05
 *   .PROBE phy0.q phy0.entropy met0.invoice
 *   .ON_GAP kv0.mount_auto
 *   .ON_FAIL branch exploratory,cautious
 */

#define _POSIX_C_SOURCE 200809L
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <limits.h>
#include <bonfyre.h>

/* ── colour helpers (terminal only) ─────────────────────────────────── */
static int use_colour_(void) { return isatty(STDOUT_FILENO); }
#define COL_RESET  (use_colour_()? "\033[0m" :"")
#define COL_BOLD   (use_colour_()? "\033[1m" :"")
#define COL_GREEN  (use_colour_()? "\033[32m":"")
#define COL_YELLOW (use_colour_()? "\033[33m":"")
#define COL_RED    (use_colour_()? "\033[31m":"")
#define COL_CYAN   (use_colour_()? "\033[36m":"")

/* ── spice status label ───────────────────────────────────────────────── */
static const char *spice_status_(int rc) {
    switch(rc){
    case BF_SPICE_OK:              return "OK";
    case BF_SPICE_NOT_CONVERGED:   return "NOT_CONVERGED";
    case BF_SPICE_TOPO_GAP:        return "TOPO_GAP";
    case BF_SPICE_MOUNTED:         return "MOUNTED";
    case BF_SPICE_CONTRACT_BLOCK:  return "CONTRACT_BLOCK";
    case BF_SPICE_NUMERIC_FAULT:   return "NUMERIC_FAULT";
    default:                       return "?";
    }
}

/* ── usage ────────────────────────────────────────────────────────────── */
static void usage(void) {
    printf(
"%sbonfyre-net%s — Bonfyre Netlist Runtime (Mixed-Signal Inference)\n"
"\n"
"  Bonfyre binaries are not pipeline stages.  They are components in a\n"
"  mixed-signal circuit.  Git-KV provides addressable substrate,\n"
"  HVCP provides phase-space motion, the SPICE layer provides nodal\n"
"  coupling between components.\n"
"\n"
"  Digital: hashes  refs  contracts  mounts  events  ledger\n"
"  Analog:  q  p  H  entropy  conductance  tolerance  noise\n"
"\n"
"Subcommands:\n"
"\n"
"  check   <file.bfnet>\n"
"            Validate a .bfnet netlist.\n"
"\n"
"  compile <file.bfnet> [--out <file.bfcircuit>]\n"
"            Compile to binary .bfcircuit.\n"
"\n"
"  run     <file.bfnet|bfcircuit>\n"
"            [--tran N]          steps to run (default: from .TRAN or 64)\n"
"            [--dt F]            timestep override\n"
"            [--probe p1,p2]     pins to watch (override .PROBE)\n"
"            [--trace out.jsonl] write probe trace\n"
"            [--on-gap action]   mount:auto | branch | emit\n"
"\n"
"  probe   <file.bfcircuit> <pin>...\n"
"            Print last value of pins.\n"
"\n"
"  graph   <file.bfnet>\n"
"            Print circuit topology.\n"
"\n"
"  components\n"
"            List all registered component types.\n"
"\n"
"  seal    <file.bfnet|bfcircuit>\n"
"            [--level 0|1|2|3]   isolation: hash-only/sketch/he-vector/enclave\n"
"            [--allow flags]     gate,distance_bucket,entropy_delta,proof,meter\n"
"            [--key  <key>]      HMAC seal key (enables authentication)\n"
"            [--name <name>]     sealed circuit name\n"
"            [--out  <path>]     → .hebfsubckt\n"
"            Seal a circuit as a private potential field component.\n"
"\n"
"  unseal  <file.hebfsubckt>\n"
"            Inspect sealed subcircuit header (without exposing internals).\n"
"\n"
"  eval-sealed <file.hebfsubckt>\n"
"            Run one evaluation step of a sealed subcircuit.\n"
"\n"
"Example:\n"
"  bonfyre-net seal      my-expert.bfcircuit --level 1 --allow gate,entropy_delta\n"
"  bonfyre-net unseal    my-expert.hebfsubckt\n"
"  bonfyre-net eval-sealed my-expert.hebfsubckt\n"
"  bonfyre-net compile   demo.bfnet --out demo.bfcircuit\n"
"  bonfyre-net run       demo.bfcircuit --tran 128 --trace demo.bfprobe\n",
    COL_BOLD,COL_RESET);
}

/* ═══════════════════════════════════════════════════════════════════════
 * check
 * ═════════════════════════════════════════════════════════════════════ */
static int cmd_check_(int argc, char **argv) {
    if(argc<2){fprintf(stderr,"check: need <file.bfnet>\n");return 1;}
    const char *path=argv[1];
    BfNetlist nl; memset(&nl,0,sizeof(nl));
    if(bf_netlist_parse(path,&nl)!=0){
        fprintf(stderr,"check: parse failed: %s\n",path);return 1;
    }

    printf("%snetlist:%s %s\n",COL_BOLD,COL_RESET,path);
    printf("  world      : %s\n",nl.world);
    printf("  components : %u\n",nl.n_components);
    printf("  nets       : %u\n",nl.n_wires);
    printf("  probes     : %u\n",nl.n_probes);
    if(nl.tran_end)
        printf("  .TRAN      : %llu → %llu  dt=%.4f\n",
               (unsigned long long)nl.tran_start,
               (unsigned long long)nl.tran_end, nl.tran_dt);
    if(nl.on_gap[0])  printf("  .ON_GAP    : %s\n",nl.on_gap);
    if(nl.on_fail[0]) printf("  .ON_FAIL   : %s\n",nl.on_fail);
    printf("\n  components:\n");
    for(uint32_t i=0;i<nl.n_components;i++){
        const BfNetComponent *c=&nl.components[i];
        const BfComponentDef *def=bf_component_lookup(c->type);
        const char *ok=def?(use_colour_()?"\033[32m✓\033[0m":"ok"):(use_colour_()?"\033[33m?\033[0m":"?");
        printf("    %s %-12s [%s]",ok,c->instance,c->type);
        if(!def) printf("  (unknown — will be stubbed)");
        if(c->params[0]) printf("  %s",c->params);
        printf("\n");
    }
    printf("\n  nets:\n");
    for(uint32_t i=0;i<nl.n_wires;i++){
        const BfNetWire *w=&nl.wires[i];
        printf("    %-30s  →  %-30s  [%s]\n",w->src,w->dst,w->net_type);
    }

    int rv=bf_netlist_check(&nl,stderr);
    if(rv==0) printf("\n%s✓ netlist OK%s\n",COL_GREEN,COL_RESET);
    else      printf("\n%s✗ netlist has errors%s\n",COL_RED,COL_RESET);
    return rv?1:0;
}

/* ═══════════════════════════════════════════════════════════════════════
 * compile
 * ═════════════════════════════════════════════════════════════════════ */
static int cmd_compile_(int argc, char **argv) {
    if(argc<2){fprintf(stderr,"compile: need <file.bfnet>\n");return 1;}
    const char *in_path=argv[1];
    const char *out_path=NULL;
    for(int i=2;i<argc;i++)
        if(!strcmp(argv[i],"--out")&&i+1<argc) out_path=argv[++i];

    /* derive output path */
    char out_buf[PATH_MAX];
    if(!out_path){
        snprintf(out_buf,sizeof(out_buf),"%s",in_path);
        char *dot=strrchr(out_buf,'.');
        if(dot) snprintf(dot,sizeof(out_buf)-(size_t)(dot-out_buf),".bfcircuit");
        else    snprintf(out_buf+strlen(out_buf),sizeof(out_buf)-strlen(out_buf),".bfcircuit");
        out_path=out_buf;
    }

    BfNetlist nl; memset(&nl,0,sizeof(nl));
    if(bf_netlist_parse(in_path,&nl)!=0){
        fprintf(stderr,"compile: parse failed: %s\n",in_path);return 1;
    }
    if(bf_netlist_check(&nl,stderr)!=0){
        fprintf(stderr,"compile: netlist has errors\n");return 1;
    }

    BfCircuit *c=bf_circuit_compile(&nl);
    if(!c){fprintf(stderr,"compile: circuit compile failed\n");return 1;}

    if(bf_circuit_save(c,out_path)!=0){
        fprintf(stderr,"compile: save failed: %s\n",out_path);
        bf_circuit_free(c);return 1;
    }

    printf("compiled: %s\n",in_path);
    printf("  → %s\n",out_path);
    printf("  %u nodes  %u edges  %u probes\n",
           c->n_nodes,c->n_edges,c->n_probes);
    bf_circuit_free(c);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 * run
 * ═════════════════════════════════════════════════════════════════════ */
static void write_probe_jsonl_(FILE *tf, const BfProbeFrame *pf) {
    if(!tf) return;
    static const char HEX_[]="0123456789abcdef";
    fprintf(tf,"{\"step\":%llu,\"t\":%.4f,\"status\":\"%s\",\"probes\":{",
            (unsigned long long)pf->step,(double)pf->t,spice_status_(pf->status));
    for(uint32_t i=0;i<pf->n_probes;i++){
        if(i) fprintf(tf,",");
        fprintf(tf,"\"%s\":",pf->names[i]);
        if(pf->is_hash[i]){
            fprintf(tf,"\"");
            for(int j=0;j<32;j++){
                fputc(HEX_[pf->hashes[i][j]>>4], tf);
                fputc(HEX_[pf->hashes[i][j]&0xf],tf);
            }
            fprintf(tf,"\"");
        } else {
            fprintf(tf,"%.6g",(double)pf->values[i]);
        }
    }
    fprintf(tf,"}}\n");
    fflush(tf);
}

static int cmd_run_(int argc, char **argv) {
    if(argc<2){fprintf(stderr,"run: need <file.bfnet|bfcircuit>\n");return 1;}
    const char *in_path=argv[1];
    uint64_t tran_steps=0;
    float    dt_override=0.0f;
    const char *extra_probes=NULL;
    const char *trace_path=NULL;
    int        gap_mount=0, gap_branch=0, gap_emit=0;

    for(int i=2;i<argc;i++){
        if(!strcmp(argv[i],"--tran")   &&i+1<argc) tran_steps=(uint64_t)atoll(argv[++i]);
        if(!strcmp(argv[i],"--dt")     &&i+1<argc) dt_override=(float)atof(argv[++i]);
        if(!strcmp(argv[i],"--probe")  &&i+1<argc) extra_probes=argv[++i];
        if(!strcmp(argv[i],"--trace")  &&i+1<argc) trace_path=argv[++i];
        if(!strcmp(argv[i],"--on-gap") &&i+1<argc){
            const char *act=argv[++i];
            if(!strncmp(act,"mount",5)) gap_mount=1;
            if(!strncmp(act,"branch",6)) gap_branch=1;
            if(!strncmp(act,"emit",4))  gap_emit=1;
        }
    }

    /* determine file type from extension */
    BfCircuit *circuit=NULL;
    BfNetlist  nl_store; memset(&nl_store,0,sizeof(nl_store));
    const char *ext=strrchr(in_path,'.');
    int is_net=(ext&&!strcmp(ext,".bfnet"));

    if(is_net){
        if(bf_netlist_parse(in_path,&nl_store)!=0){
            fprintf(stderr,"run: parse failed: %s\n",in_path);return 1;
        }
        circuit=bf_circuit_compile(&nl_store);
        if(!tran_steps) tran_steps=nl_store.tran_end?nl_store.tran_end:64;
        if(!dt_override&&nl_store.tran_dt>0.0f) dt_override=nl_store.tran_dt;
        /* use netlist on_gap */
        if(!gap_mount&&!strncmp(nl_store.on_gap,"mount",5)) gap_mount=1;
    } else {
        circuit=bf_circuit_load(in_path);
        if(!tran_steps) tran_steps=64;
    }

    if(!circuit){fprintf(stderr,"run: could not load/compile circuit\n");return 1;}

    /* inject extra probes from --probe flag */
    if(extra_probes){
        char pbuf[2048]; snprintf(pbuf,sizeof(pbuf),"%s",extra_probes);
        char *tok=strtok(pbuf,",");
        while(tok&&circuit->n_probes<BF_NETLIST_MAX_PROBES){
            snprintf(circuit->probe_names[circuit->n_probes++],128,"%s",tok);
            tok=strtok(NULL,",");
        }
    }

    float dt=dt_override>0.0f?dt_override:0.05f;
    BfTranState *ts=bf_tran_state_alloc(circuit,dt,tran_steps);
    if(!ts){bf_circuit_free(circuit);return 1;}

    FILE *tf=trace_path?fopen(trace_path,"wb"):NULL;

    printf("%sbonfyre-net run:%s %s\n",COL_BOLD,COL_RESET,in_path);
    printf("  world=%s  steps=%llu  dt=%.4f  nodes=%u  nets=%u\n",
           circuit->world,(unsigned long long)tran_steps,dt,
           circuit->n_nodes,circuit->n_edges);
    if(circuit->n_probes){
        printf("  probes:");
        for(uint32_t p=0;p<circuit->n_probes;p++)
            printf(" %s",circuit->probe_names[p]);
        printf("\n");
    }
    printf("\n");

    int final_rc=BF_SPICE_OK;
    BfInputPulse pulse={0}; pulse.event=1;

    for(uint64_t step=0;step<tran_steps;step++){
        BfProbeFrame pf; memset(&pf,0,sizeof(pf));
        int rc=bf_spice_eval(circuit,ts,&pulse,&pf);
        pulse.event=0; /* source fires only on step 0 */

        /* print probe line */
        printf("  step %04llu  t=%.3f  ",(unsigned long long)step+1,(double)ts->t);
        for(uint32_t pi=0;pi<pf.n_probes&&pi<6;pi++){
            printf("%s=",pf.names[pi]);
            if(pf.is_hash[pi]){
                static const char HX[]="0123456789abcdef";
                for(int j=0;j<8;j++){
                    putchar(HX[pf.hashes[pi][j]>>4]);
                    putchar(HX[pf.hashes[pi][j]&0xf]);
                }
                printf("..  ");
            } else {
                printf("%-8.4f  ",(double)pf.values[pi]);
            }
        }

        if(rc!=BF_SPICE_OK){
            const char *col=rc==BF_SPICE_NOT_CONVERGED?COL_YELLOW:COL_RED;
            printf("%s%s%s",col,spice_status_(rc),COL_RESET);
            if(rc==BF_SPICE_TOPO_GAP){
                if(gap_emit)   printf("  [gap emitted]");
                if(gap_mount)  printf("  [mount:auto triggered]");
                if(gap_branch) printf("  [branch forked]");
            }
        } else if(ts->converged && step>0){
            printf("%sCONVERGED%s",COL_GREEN,COL_RESET);
        }
        printf("\n");

        if(tf) write_probe_jsonl_(tf,&pf);

        /* on converged: write hash if we have PROOF pins */
        if(ts->converged && step>1){
            final_rc=BF_SPICE_OK;
            goto done;
        }
        if(rc==BF_SPICE_NUMERIC_FAULT){ final_rc=rc; goto done; }
    }

done:
    printf("\n  steps=%llu  t=%.3f  status=%s\n",
           (unsigned long long)ts->step,(double)ts->t,
           spice_status_(final_rc));
    if(ts->converged) printf("  %sCONVERGED%s\n",COL_GREEN,COL_RESET);
    if(trace_path&&tf){ fclose(tf); printf("  trace → %s\n",trace_path); }

    bf_tran_state_free(ts);
    bf_circuit_free(circuit);
    return 0;

}

/* ═══════════════════════════════════════════════════════════════════════
 * probe
 * ═════════════════════════════════════════════════════════════════════ */
static int cmd_probe_(int argc, char **argv) {
    if(argc<3){fprintf(stderr,"probe: need <file.bfcircuit> <pin>...\n");return 1;}
    BfCircuit *c=bf_circuit_load(argv[1]);
    if(!c){fprintf(stderr,"probe: cannot load %s\n",argv[1]);return 1;}
    /* run one step to get valid outputs */
    BfTranState *ts=bf_tran_state_alloc(c,0.05f,1);
    BfInputPulse p={0}; p.event=1;
    BfProbeFrame pf;
    /* temporarily replace probe list with requested pins */
    char saved[BF_NETLIST_MAX_PROBES][128];
    uint32_t saved_n=c->n_probes;
    memcpy(saved,c->probe_names,sizeof(saved));
    c->n_probes=0;
    for(int i=2;i<argc&&c->n_probes<BF_NETLIST_MAX_PROBES;i++)
        snprintf(c->probe_names[c->n_probes++],128,"%s",argv[i]);
    bf_spice_eval(c,ts,&p,&pf);
    static const char HX[]="0123456789abcdef";
    for(uint32_t i=0;i<pf.n_probes;i++){
        printf("%-30s = ",pf.names[i]);
        if(pf.is_hash[i]){
            for(int j=0;j<32;j++){putchar(HX[pf.hashes[i][j]>>4]);putchar(HX[pf.hashes[i][j]&0xf]);}
            printf("\n");
        } else {
            printf("%.6g\n",(double)pf.values[i]);
        }
    }
    /* restore */
    c->n_probes=saved_n;
    memcpy(c->probe_names,saved,sizeof(saved));
    bf_tran_state_free(ts);
    bf_circuit_free(c);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 * graph
 * ═════════════════════════════════════════════════════════════════════ */
static int cmd_graph_(int argc, char **argv) {
    if(argc<2){fprintf(stderr,"graph: need <file.bfnet>\n");return 1;}
    BfNetlist nl; memset(&nl,0,sizeof(nl));
    if(bf_netlist_parse(argv[1],&nl)!=0){
        fprintf(stderr,"graph: parse failed\n");return 1;
    }
    BfCircuit *c=bf_circuit_compile(&nl);
    if(!c) return 1;
    bf_circuit_print(c,stdout);
    bf_circuit_free(c);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 * components
 * ═════════════════════════════════════════════════════════════════════ */
#define MAX_REGISTRY 64
static const char *pkind_short_(BfPinKind k){
    switch(k){
    case BF_PIN_SIGNAL: return "sig";
    case BF_PIN_STATE:  return "st ";
    case BF_PIN_MEMORY: return "mem";
    case BF_PIN_COST:   return "cst";
    case BF_PIN_PROOF:  return "prf";
    case BF_PIN_RULE:   return "rul";
    case BF_PIN_VALUE:  return "val";
    case BF_PIN_STREAM: return "str";
    default:            return "?  ";
    }
}

static int cmd_components_(void) {
    bf_component_registry_init();
    /* reuse registry list with a local define - just cast through lookup */
    const char *names[]={
        "BonfyreTel","BonfyreMoQ","BonfyreEmbed","BonfyreKVCache",
        "BonfyrePhysics","BonfyreCMS","BonfyreDisCIPL","BonfyreMeter",
        "BonfyreLedger","BonfyreLayer","BonfyreHash","BonfyreWorkflow",
        "BonfyreHeSli",NULL
    };
    printf("Registered component types:\n\n");
    for(int i=0;names[i];i++){
        const BfComponentDef *d=bf_component_lookup(names[i]);
        if(!d) continue;
        printf("  %s%-20s%s\n",COL_BOLD,d->name,COL_RESET);
        for(uint32_t j=0;j<d->n_inputs;j++)
            printf("    in  [%s] %s\n",pkind_short_(d->inputs[j].kind),d->inputs[j].name);
        for(uint32_t j=0;j<d->n_outputs;j++)
            printf("    out [%s] %s\n",pkind_short_(d->outputs[j].kind),d->outputs[j].name);
        printf("\n");
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 * seal
 * ─────────────────────────────────────────────────────────────────────
 * Seal a compiled circuit as a .hebfsubckt file.
 * The sealed file can participate in circuit runs without exposing
 * its internal component state.
 *
 *   seal <file.bfnet|bfcircuit>
 *        --level 0|1|2|3
 *        [--allow gate,distance_bucket,entropy_delta,proof,meter,risk,mount,basin]
 *        [--name  "My Private Expert"]
 *        [--desc  "Description of sealed circuit"]
 *        [--key   <hmac-key-string>]
 *        [--rate  <billable-units-per-eval>]
 *        [--out   <file.hebfsubckt>]
 * ═════════════════════════════════════════════════════════════════════ */
static uint32_t parse_allow_flags_(const char *s) {
    uint32_t f=0;
    char buf[256]; snprintf(buf,sizeof(buf),"%s",s);
    char *tok=strtok(buf,",");
    while(tok){
        if(!strcmp(tok,"gate"))          f|=BF_HESLI_OUT_GATE;
        if(!strcmp(tok,"basin"))         f|=BF_HESLI_OUT_BASIN;
        if(!strcmp(tok,"distance"))      f|=BF_HESLI_OUT_DISTANCE;
        if(!strcmp(tok,"distance_bucket")) f|=BF_HESLI_OUT_DISTANCE;
        if(!strcmp(tok,"risk"))          f|=BF_HESLI_OUT_RISK;
        if(!strcmp(tok,"entropy_delta")) f|=BF_HESLI_OUT_ENTROPY_DELTA;
        if(!strcmp(tok,"entropy"))       f|=BF_HESLI_OUT_ENTROPY_DELTA;
        if(!strcmp(tok,"mount"))         f|=BF_HESLI_OUT_MOUNT;
        if(!strcmp(tok,"proof"))         f|=BF_HESLI_OUT_PROOF;
        if(!strcmp(tok,"meter"))         f|=BF_HESLI_OUT_METER;
        tok=strtok(NULL,",");
    }
    return f?f:BF_HESLI_OUT_DEFAULT;
}

static const char *hesli_level_name_(uint32_t l) {
    switch(l){
    case BF_HESLI_HASH_ONLY:     return "hash-only";
    case BF_HESLI_SKETCH:        return "sketch";
    case BF_HESLI_HE_VECTOR:     return "he-vector";
    case BF_HESLI_LOCAL_ENCLAVE: return "local-enclave";
    default:                      return "?";
    }
}

static void print_allow_flags_(uint32_t f) {
    if(f&BF_HESLI_OUT_GATE)          printf("gate ");
    if(f&BF_HESLI_OUT_BASIN)         printf("basin ");
    if(f&BF_HESLI_OUT_DISTANCE)      printf("distance_bucket ");
    if(f&BF_HESLI_OUT_RISK)          printf("risk ");
    if(f&BF_HESLI_OUT_ENTROPY_DELTA) printf("entropy_delta ");
    if(f&BF_HESLI_OUT_MOUNT)         printf("mount ");
    if(f&BF_HESLI_OUT_PROOF)         printf("proof ");
    if(f&BF_HESLI_OUT_METER)         printf("meter ");
}

static int cmd_seal_(int argc, char **argv) {
    if(argc<2){
        fprintf(stderr,
            "seal: need <file.bfnet|bfcircuit>\n"
            "  --level 0|1|2|3    isolation level (default: 3)\n"
            "  --allow <flags>    comma-separated: gate,distance_bucket,entropy_delta,proof,meter,risk,mount,basin\n"
            "  --name  <name>     sealed circuit name\n"
            "  --desc  <text>     description\n"
            "  --key   <key>      HMAC seal key (enables authentication)\n"
            "  --rate  <float>    billable units per eval (default: 1.0)\n"
            "  --out   <path>     output path (default: derived from input)\n");
        return 1;
    }
    const char *in_path=argv[1];
    int         level=3;
    uint32_t    allow=BF_HESLI_OUT_DEFAULT;
    const char *name=NULL, *desc=NULL, *key=NULL, *out_path=NULL;
    float       rate=1.0f;
    int         allow_set=0;

    for(int i=2;i<argc;i++){
        if(!strcmp(argv[i],"--level")&&i+1<argc) level=atoi(argv[++i]);
        if(!strcmp(argv[i],"--allow")&&i+1<argc){ allow=parse_allow_flags_(argv[++i]); allow_set=1; }
        if(!strcmp(argv[i],"--name") &&i+1<argc) name=argv[++i];
        if(!strcmp(argv[i],"--desc") &&i+1<argc) desc=argv[++i];
        if(!strcmp(argv[i],"--key")  &&i+1<argc) key=argv[++i];
        if(!strcmp(argv[i],"--rate") &&i+1<argc) rate=(float)atof(argv[++i]);
        if(!strcmp(argv[i],"--out")  &&i+1<argc) out_path=argv[++i];
    }
    if(!allow_set && level<=BF_HESLI_SKETCH)
        allow=BF_HESLI_OUT_GATE|BF_HESLI_OUT_DISTANCE;

    /* derive output path */
    char out_buf[PATH_MAX];
    if(!out_path){
        snprintf(out_buf,sizeof(out_buf),"%s",in_path);
        char *dot=strrchr(out_buf,'.');
        if(dot) snprintf(dot,sizeof(out_buf)-(size_t)(dot-out_buf),".hebfsubckt");
        else    strncat(out_buf,".hebfsubckt",sizeof(out_buf)-strlen(out_buf)-1);
        out_path=out_buf;
    }

    /* load or compile circuit */
    BfCircuit *c=NULL;
    BfNetlist nl; memset(&nl,0,sizeof(nl));
    const char *ext=strrchr(in_path,'.');
    if(ext&&!strcmp(ext,".bfnet")){
        if(bf_netlist_parse(in_path,&nl)!=0){
            fprintf(stderr,"seal: parse failed: %s\n",in_path);return 1;}
        c=bf_circuit_compile(&nl);
    } else {
        c=bf_circuit_load(in_path);
    }
    if(!c){fprintf(stderr,"seal: could not load circuit: %s\n",in_path);return 1;}

    /* build policy */
    BfHeSliPolicy pol; memset(&pol,0,sizeof(pol));
    pol.level=(BfHeSliLevel)level;
    pol.allowed_outputs=allow;
    pol.meter_rate=rate;
    if(name) snprintf(pol.name,sizeof(pol.name),"%s",name);
    else     snprintf(pol.name,sizeof(pol.name),"%s",c->world);
    if(key)  snprintf(pol.seal_key,sizeof(pol.seal_key),"%s",key);

    int rv=bf_hesli_seal(c,&pol,desc?desc:"",out_path);
    bf_circuit_free(c);
    if(rv!=0){fprintf(stderr,"seal: write failed: %s\n",out_path);return 1;}

    /* print summary */
    printf("sealed: %s\n",in_path);
    printf("  → %s\n",out_path);
    printf("  level  : %s (%d)\n",hesli_level_name_(level),level);
    printf("  allows : "); print_allow_flags_(allow); printf("\n");
    if(key) printf("  hmac   : yes (keyed)\n");
    printf("  rate   : %.2f units/eval\n",(double)rate);
    /* print seal hash */
    {
        size_t sz=0; char *blob=bf_read_file(out_path,&sz);
        if(blob&&sz>=sizeof(BfHebfSubckt)){
            BfHebfSubckt *h=(BfHebfSubckt*)blob;
            static const char HX[]="0123456789abcdef";
            printf("  seal   : ");
            for(int i=0;i<32;i++){putchar(HX[h->seal_hash[i]>>4]);putchar(HX[h->seal_hash[i]&0xf]);}
            printf("\n");
        }
        free(blob);
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 * unseal  (inspect, not expose)
 * ═════════════════════════════════════════════════════════════════════ */
static int cmd_unseal_(int argc, char **argv) {
    if(argc<2){fprintf(stderr,"unseal: need <file.hebfsubckt>\n");return 1;}
    BfHeSliSubckt *s=bf_hesli_load(argv[1]);
    if(!s){fprintf(stderr,"unseal: cannot load or verify %s\n",argv[1]);return 1;}

    static const char HX[]="0123456789abcdef";
    printf("%ssealed subcircuit:%s %s\n",COL_BOLD,COL_RESET,argv[1]);
    printf("  name        : %s\n",s->header.name);
    printf("  description : %s\n",s->header.description);
    printf("  level       : %s (%u)\n",hesli_level_name_(s->header.level),s->header.level);
    printf("  allows      : "); print_allow_flags_(s->header.allowed_outputs); printf("\n");
    printf("  meter rate  : %.2f units/eval\n",(double)s->header.meter_rate);
    printf("  inner size  : %u bytes\n",s->header.inner_size);
    printf("  seal hash   : ");
    for(int i=0;i<32;i++){putchar(HX[s->header.seal_hash[i]>>4]);putchar(HX[s->header.seal_hash[i]&0xf]);}
    printf("\n");
    int hmac_set=0; for(int i=0;i<32;i++) if(s->header.hmac[i]) hmac_set=1;
    printf("  hmac        : %s\n",hmac_set?"yes (authenticated)":"no (unsigned)");
    printf("  integrity   : %sOK%s (seal_hash verified)\n",COL_GREEN,COL_RESET);
    bf_hesli_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 * eval-sealed  (one-shot evaluation of a sealed subcircuit)
 * ═════════════════════════════════════════════════════════════════════ */
static int cmd_eval_sealed_(int argc, char **argv) {
    if(argc<2){fprintf(stderr,"eval-sealed: need <file.hebfsubckt>\n");return 1;}
    BfHeSliSubckt *s=bf_hesli_load(argv[1]);
    if(!s){fprintf(stderr,"eval-sealed: cannot load %s\n",argv[1]);return 1;}

    BfInputPulse pulse={0}; pulse.event=1;
    BfHeSliResult res={0};
    int rc=bf_hesli_subckt_eval(s,&pulse,&res);

    static const char HX[]="0123456789abcdef";
    printf("gate          : %s%s%s\n",
           res.gate?COL_GREEN:COL_RED, res.gate?"allowed":"denied", COL_RESET);
    printf("distance_bucket: %d/7\n",res.distance_bucket);
    printf("risk_score    : %.4f\n",(double)res.risk_score);
    printf("entropy_delta : %+.4f\n",(double)res.entropy_delta);
    printf("meter_units   : %.2f\n",(double)res.meter_units);
    if(res.mount_yes) printf("mount_handle  : %s\n",res.mount_handle);
    printf("proof         : ");
    for(int i=0;i<32;i++){putchar(HX[res.proof[i]>>4]);putchar(HX[res.proof[i]&0xf]);}
    printf("\nstatus        : %s\n",spice_status_(rc));

    bf_hesli_free(s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 * main
 * ═════════════════════════════════════════════════════════════════════ */
int main(int argc, char **argv) {
    if(argc<2){usage();return 1;}
    bf_component_registry_init();

    const char *cmd=argv[1];
    if(!strcmp(cmd,"--help")||!strcmp(cmd,"-h")){ usage();return 0; }
    if(!strcmp(cmd,"check"))        return cmd_check_      (argc-1,argv+1);
    if(!strcmp(cmd,"compile"))      return cmd_compile_    (argc-1,argv+1);
    if(!strcmp(cmd,"run"))          return cmd_run_        (argc-1,argv+1);
    if(!strcmp(cmd,"probe"))        return cmd_probe_      (argc-1,argv+1);
    if(!strcmp(cmd,"graph"))        return cmd_graph_      (argc-1,argv+1);
    if(!strcmp(cmd,"components"))   return cmd_components_ ();
    if(!strcmp(cmd,"seal"))         return cmd_seal_       (argc-1,argv+1);
    if(!strcmp(cmd,"unseal"))       return cmd_unseal_     (argc-1,argv+1);
    if(!strcmp(cmd,"eval-sealed"))  return cmd_eval_sealed_(argc-1,argv+1);
    fprintf(stderr,"Unknown subcommand: %s\n",cmd);
    usage();
    return 1;
}
