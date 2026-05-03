/*
 * bf_netlist.c — Bonfyre netlist parser (.bfnet format)
 *
 * Parses a human-readable circuit description into a BfNetlist struct.
 *
 * Syntax (case-insensitive keywords, # comments):
 *
 *   .WORLD   <name>
 *   .COMPONENT <instance> <type> [key=value ...]
 *   .NET     <src.pin>  <dst.pin>  [net_type]
 *   .TRAN    <start>  <end>  <dt>
 *   .PROBE   <pin1> [pin2 ...]
 *   .ON_GAP  <action>
 *   .ON_FAIL <action>
 *
 * Example:
 *   .WORLD demo
 *   .COMPONENT tel0  BonfyreTel   mode=sim
 *   .COMPONENT emb0  BonfyreEmbed pack=acme.bfepack
 *   .NET tel0.signal emb0.input  signal
 *   .TRAN 0 128 0.05
 *   .PROBE emb0.q phy0.H phy0.entropy
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <bonfyre.h>

/* ── utilities ───────────────────────────────────────────────────────── */
static char *ltrim_(char *s) { while(*s&&isspace((unsigned char)*s))s++; return s; }
static void rtrim_(char *s) {
    char *e=s+strlen(s);
    while(e>s&&isspace((unsigned char)*(e-1))) *--e='\0';
}
static char *normalize_(char *s) { rtrim_(s); return ltrim_(s); }

/* Split line into up to max tokens, respect quotes. Returns count. */
static int tokenize_(char *line, char **toks, int max) {
    int n=0; char *p=line;
    while(*p&&n<max){
        while(*p&&isspace((unsigned char)*p))p++;
        if(!*p||*p=='#') break;
        if(*p=='"'){
            p++;
            toks[n++]=p;
            while(*p&&*p!='"')p++;
            if(*p)*p++='\0';
        } else {
            toks[n++]=p;
            while(*p&&!isspace((unsigned char)*p)&&*p!='#')p++;
            if(*p)*p++='\0';
        }
    }
    return n;
}

/* ── public ──────────────────────────────────────────────────────────── */
int bf_netlist_parse(const char *path, BfNetlist *out) {
    FILE *f=fopen(path,"rb");
    if(!f) return -1;

    memset(out,0,sizeof(*out));
    snprintf(out->world,sizeof(out->world),"unnamed");
    out->tran_dt=0.05f;
    out->tran_end=128;

    char line[2048];
    int  lineno=0;

    while(fgets(line,sizeof(line),f)){
        lineno++;
        char *p=normalize_(line);
        if(!*p||*p=='#') continue;

        /* keyword is first token; it must start with '.' */
        if(*p!='.') continue;

        char *toks[64]; int ntok=tokenize_(p,toks,64);
        if(ntok<1) continue;

        /* uppercase keyword */
        char kw[32]; snprintf(kw,sizeof(kw),"%s",toks[0]);
        for(char *k=kw;*k;k++) *k=(char)toupper((unsigned char)*k);

        if(!strcmp(kw,".WORLD")){
            if(ntok>=2) snprintf(out->world,sizeof(out->world),"%s",toks[1]);

        } else if(!strcmp(kw,".COMPONENT")){
            if(ntok<3){ fprintf(stderr,"netlist:%d: .COMPONENT needs instance type\n",lineno); continue; }
            if(out->n_components>=BF_NETLIST_MAX_COMPONENTS){
                fprintf(stderr,"netlist:%d: too many components (max %d)\n",lineno,BF_NETLIST_MAX_COMPONENTS);
                continue;
            }
            BfNetComponent *c=&out->components[out->n_components++];
            snprintf(c->instance,sizeof(c->instance),"%s",toks[1]);
            snprintf(c->type,    sizeof(c->type),    "%s",toks[2]);
            /* join remaining tokens as params */
            size_t off=0;
            for(int i=3;i<ntok&&off<sizeof(c->params)-2;i++){
                if(off) c->params[off++]=' ';
                size_t l=strlen(toks[i]);
                if(off+l>=sizeof(c->params)-1) l=sizeof(c->params)-1-off;
                memcpy(c->params+off,toks[i],l);
                off+=l;
            }
            c->params[off]='\0';

        } else if(!strcmp(kw,".NET")){
            if(ntok<3){ fprintf(stderr,"netlist:%d: .NET needs src dst\n",lineno); continue; }
            if(out->n_wires>=BF_NETLIST_MAX_NETS){
                fprintf(stderr,"netlist:%d: too many nets\n",lineno); continue;
            }
            BfNetWire *w=&out->wires[out->n_wires++];
            snprintf(w->src,sizeof(w->src),"%s",toks[1]);
            snprintf(w->dst,sizeof(w->dst),"%s",toks[2]);
            if(ntok>=4) snprintf(w->net_type,sizeof(w->net_type),"%s",toks[3]);
            else        snprintf(w->net_type,sizeof(w->net_type),"signal");

        } else if(!strcmp(kw,".TRAN")){
            if(ntok>=4){
                out->tran_start=(uint64_t)strtoull(toks[1],NULL,10);
                out->tran_end  =(uint64_t)strtoull(toks[2],NULL,10);
                out->tran_dt   =(float)atof(toks[3]);
            } else if(ntok==3){
                out->tran_end=(uint64_t)strtoull(toks[1],NULL,10);
                out->tran_dt =(float)atof(toks[2]);
            }

        } else if(!strcmp(kw,".PROBE")){
            for(int i=1;i<ntok&&out->n_probes<BF_NETLIST_MAX_PROBES;i++)
                snprintf(out->probes[out->n_probes++],128,"%s",toks[i]);

        } else if(!strcmp(kw,".ON_GAP")){
            if(ntok>=2) snprintf(out->on_gap,sizeof(out->on_gap),"%s",toks[1]);

        } else if(!strcmp(kw,".ON_FAIL")){
            if(ntok>=2) snprintf(out->on_fail,sizeof(out->on_fail),"%s",toks[1]);

        } else {
            fprintf(stderr,"netlist:%d: unknown directive %s\n",lineno,kw);
        }
    }
    fclose(f);
    return 0;
}

int bf_netlist_check(const BfNetlist *nl, FILE *err) {
    int ok=1;
    if(!err) err=stderr;

    /* all .NET src/dst should reference known instances */
    for(uint32_t i=0;i<nl->n_wires;i++){
        const BfNetWire *w=&nl->wires[i];

        /* extract instance name (up to the '.') */
        char src_inst[64]; snprintf(src_inst,sizeof(src_inst),"%s",w->src);
        char *dot=strchr(src_inst,'.'); if(dot)*dot='\0';

        char dst_inst[64]; snprintf(dst_inst,sizeof(dst_inst),"%s",w->dst);
        dot=strchr(dst_inst,'.'); if(dot)*dot='\0';

        int src_ok=0,dst_ok=0;
        for(uint32_t j=0;j<nl->n_components;j++){
            if(!strcmp(nl->components[j].instance,src_inst)) src_ok=1;
            if(!strcmp(nl->components[j].instance,dst_inst)) dst_ok=1;
        }
        if(!src_ok){
            fprintf(err,"netlist check: net[%u] src instance '%s' not declared\n",i,src_inst);
            ok=0;
        }
        if(!dst_ok){
            fprintf(err,"netlist check: net[%u] dst instance '%s' not declared\n",i,dst_inst);
            ok=0;
        }
    }
    if(!nl->n_components){
        fprintf(err,"netlist check: no components declared\n"); ok=0;
    }
    return ok ? 0 : -1;
}

void bf_netlist_print(const BfNetlist *nl, FILE *fp) {
    if(!fp) fp=stdout;
    fprintf(fp,".WORLD %s\n\n",nl->world);
    for(uint32_t i=0;i<nl->n_components;i++){
        const BfNetComponent *c=&nl->components[i];
        fprintf(fp,".COMPONENT %-12s %-20s %s\n",c->instance,c->type,c->params);
    }
    if(nl->n_wires) fprintf(fp,"\n");
    for(uint32_t i=0;i<nl->n_wires;i++){
        const BfNetWire *w=&nl->wires[i];
        fprintf(fp,".NET %-30s %-30s %s\n",w->src,w->dst,w->net_type);
    }
    if(nl->tran_end)
        fprintf(fp,"\n.TRAN %llu %llu %.4f\n",
                (unsigned long long)nl->tran_start,
                (unsigned long long)nl->tran_end,
                nl->tran_dt);
    if(nl->n_probes){
        fprintf(fp,".PROBE");
        for(uint32_t i=0;i<nl->n_probes;i++) fprintf(fp," %s",nl->probes[i]);
        fprintf(fp,"\n");
    }
    if(nl->on_gap[0])  fprintf(fp,".ON_GAP %s\n",nl->on_gap);
    if(nl->on_fail[0]) fprintf(fp,".ON_FAIL %s\n",nl->on_fail);
}
