/*
 * bf_circuit.c — Bonfyre compiled circuit graph
 *
 * Compiles a BfNetlist into a BfCircuit:
 *   - resolves component types from registry
 *   - allocates per-instance state
 *   - wires input/output signal buffers via edge table
 *
 * File format (.bfcircuit):
 *   magic(4)  version(4)  world[64]
 *   n_nodes(4)  n_edges(4)  n_probes(4)  pad(4)
 *   BfCircuitNode[n_nodes]   (serialised without pointers)
 *   BfCircuitEdge[n_edges]
 *   probe_names[n_probes][128]
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <bonfyre.h>

#define CIRCUIT_VERSION 1u

/* ── on-disk node header (no pointers) ──────────────────────────────── */
typedef struct {
    char     instance[64];
    char     type    [64];
    float    conductance;
    float    capacitance;
    float    inductance;
    float    tolerance;
    float    aging_rate;
    float    trust;
    uint32_t n_inputs;
    uint32_t n_outputs;
    uint8_t  pad[16];
} DiskNode_;

/* ── alloc helpers ───────────────────────────────────────────────────── */
static void init_node_state_(BfCircuitNode *node, const BfComponentDef *def) {
    memset(&node->state,0,sizeof(node->state));
    node->state.conductance = 1.0f;
    node->state.capacitance = 1.0f;
    node->state.inductance  = 0.1f;
    node->state.tolerance   = 0.01f;
    node->state.aging_rate  = 0.001f;
    node->state.trust       = 1.0f;
    node->n_inputs  = def->n_inputs;
    node->n_outputs = def->n_outputs;
    memset(node->input_buf, 0, sizeof(node->input_buf));
    memset(node->output_buf,0, sizeof(node->output_buf));
    /* tag pin kinds */
    for(uint32_t i=0;i<def->n_inputs;i++)
        node->input_buf[i].kind=def->inputs[i].kind;
    for(uint32_t i=0;i<def->n_outputs;i++)
        node->output_buf[i].kind=def->outputs[i].kind;
}

static int find_node_(const BfCircuit *c, const char *inst) {
    for(uint32_t i=0;i<c->n_nodes;i++)
        if(!strcmp(c->nodes[i].instance,inst)) return (int)i;
    return -1;
}

static uint8_t find_pin_(const BfComponentDef *def, const char *pin_name, int out) {
    uint32_t n = out ? def->n_outputs : def->n_inputs;
    const BfPin *pins = out ? def->outputs : def->inputs;
    for(uint32_t i=0;i<n;i++)
        if(!strcmp(pins[i].name,pin_name)) return (uint8_t)i;
    return 0; /* default to first pin if unknown */
}

/* ── public: compile ─────────────────────────────────────────────────── */
BfCircuit *bf_circuit_compile(const BfNetlist *nl) {
    bf_component_registry_init();

    BfCircuit *c=calloc(1,sizeof(BfCircuit));
    if(!c) return NULL;
    c->magic=BF_CIRCUIT_MAGIC;
    snprintf(c->world,sizeof(c->world),"%s",nl->world);

    /* allocate nodes */
    c->n_nodes=nl->n_components;
    c->nodes=calloc(c->n_nodes,sizeof(BfCircuitNode));
    if(!c->nodes){ free(c); return NULL; }

    for(uint32_t i=0;i<nl->n_components;i++){
        const BfNetComponent *nc=&nl->components[i];
        BfCircuitNode *node=&c->nodes[i];
        snprintf(node->instance,sizeof(node->instance),"%s",nc->instance);
        snprintf(node->type,    sizeof(node->type),    "%s",nc->type);
        const BfComponentDef *def=bf_component_lookup(nc->type);
        if(!def){
            fprintf(stderr,"circuit compile: unknown component type '%s' for instance '%s'\n",
                    nc->type,nc->instance);
            /* register a stub */
            BfComponentDef stub; memset(&stub,0,sizeof(stub));
            snprintf(stub.name,sizeof(stub.name),"%s",nc->type);
            stub.n_inputs=1; stub.n_outputs=1;
            stub.inputs[0].kind=BF_PIN_SIGNAL; snprintf(stub.inputs[0].name,64,"input");
            stub.outputs[0].kind=BF_PIN_SIGNAL; snprintf(stub.outputs[0].name,64,"output");
            stub.transfer=NULL;
            bf_component_register(&stub);
            def=bf_component_lookup(nc->type);
        }
        if(def) init_node_state_(node,def);
    }

    /* allocate edges */
    c->n_edges=nl->n_wires;
    c->edges=calloc(c->n_edges,sizeof(BfCircuitEdge));
    if(!c->edges){ free(c->nodes); free(c); return NULL; }

    for(uint32_t i=0;i<nl->n_wires;i++){
        const BfNetWire *w=&nl->wires[i];
        BfCircuitEdge *e=&c->edges[i];
        snprintf(e->net_type,sizeof(e->net_type),"%s",w->net_type);

        /* split "instance.pin" */
        char src[128]; snprintf(src,sizeof(src),"%s",w->src);
        char *sdot=strchr(src,'.'); const char *spin=sdot?sdot+1:"output";
        if(sdot)*sdot='\0';
        snprintf(e->src_instance,sizeof(e->src_instance),"%s",src);

        char dst[128]; snprintf(dst,sizeof(dst),"%s",w->dst);
        char *ddot=strchr(dst,'.'); const char *dpin=ddot?ddot+1:"input";
        if(ddot)*ddot='\0';
        snprintf(e->dst_instance,sizeof(e->dst_instance),"%s",dst);

        /* resolve pin indices */
        int si=find_node_(c,src);
        int di=find_node_(c,dst);
        const BfComponentDef *sdef=(si>=0)?bf_component_lookup(c->nodes[si].type):NULL;
        const BfComponentDef *ddef=(di>=0)?bf_component_lookup(c->nodes[di].type):NULL;
        e->src_pin=sdef?find_pin_(sdef,spin,1):0;
        e->dst_pin=ddef?find_pin_(ddef,dpin,0):0;
    }

    /* probes */
    c->n_probes=nl->n_probes;
    for(uint32_t i=0;i<nl->n_probes;i++)
        snprintf(c->probe_names[i],128,"%s",nl->probes[i]);

    return c;
}

/* ── public: save ────────────────────────────────────────────────────── */
int bf_circuit_save(const BfCircuit *c, const char *path) {
    FILE *f=fopen(path,"wb");
    if(!f) return -1;

    uint32_t hdr[4]={BF_CIRCUIT_MAGIC,CIRCUIT_VERSION,c->n_nodes,c->n_edges};
    fwrite(hdr,4,4,f);
    uint32_t hdr2[2]={c->n_probes,0};
    fwrite(hdr2,4,2,f);
    fwrite(c->world,1,64,f);

    for(uint32_t i=0;i<c->n_nodes;i++){
        const BfCircuitNode *n=&c->nodes[i];
        DiskNode_ dn; memset(&dn,0,sizeof(dn));
        memcpy(dn.instance, n->instance, 64);
        memcpy(dn.type,     n->type,     64);
        dn.conductance=n->state.conductance;
        dn.capacitance=n->state.capacitance;
        dn.inductance =n->state.inductance;
        dn.tolerance  =n->state.tolerance;
        dn.aging_rate =n->state.aging_rate;
        dn.trust      =n->state.trust;
        dn.n_inputs   =n->n_inputs;
        dn.n_outputs  =n->n_outputs;
        fwrite(&dn,sizeof(dn),1,f);
    }

    fwrite(c->edges,sizeof(BfCircuitEdge),c->n_edges,f);

    for(uint32_t i=0;i<c->n_probes;i++)
        fwrite(c->probe_names[i],1,128,f);

    fclose(f);
    return 0;
}

/* ── public: load ────────────────────────────────────────────────────── */
BfCircuit *bf_circuit_load(const char *path) {
    FILE *f=fopen(path,"rb");
    if(!f) return NULL;

    uint32_t hdr[4]; if(fread(hdr,4,4,f)!=4){ fclose(f); return NULL; }
    if(hdr[0]!=BF_CIRCUIT_MAGIC){ fclose(f); return NULL; }
    uint32_t hdr2[2]; if(fread(hdr2,4,2,f)!=2){ fclose(f); return NULL; }

    BfCircuit *c=calloc(1,sizeof(BfCircuit));
    if(!c){ fclose(f); return NULL; }
    c->magic   =BF_CIRCUIT_MAGIC;
    c->n_nodes =hdr[2];
    c->n_edges =hdr[3];
    c->n_probes=hdr2[0];

    if(fread(c->world,1,64,f)!=64){ free(c); fclose(f); return NULL; }

    bf_component_registry_init();

    c->nodes=calloc(c->n_nodes,sizeof(BfCircuitNode));
    for(uint32_t i=0;i<c->n_nodes;i++){
        DiskNode_ dn; if(fread(&dn,sizeof(dn),1,f)!=1) break;
        BfCircuitNode *n=&c->nodes[i];
        memcpy(n->instance,dn.instance,64);
        memcpy(n->type,    dn.type,    64);
        n->state.conductance=dn.conductance;
        n->state.capacitance=dn.capacitance;
        n->state.inductance =dn.inductance;
        n->state.tolerance  =dn.tolerance;
        n->state.aging_rate =dn.aging_rate;
        n->state.trust      =dn.trust;
        n->n_inputs         =dn.n_inputs;
        n->n_outputs        =dn.n_outputs;
    }

    c->edges=calloc(c->n_edges,sizeof(BfCircuitEdge));
    if(c->n_edges) fread(c->edges,sizeof(BfCircuitEdge),c->n_edges,f);

    for(uint32_t i=0;i<c->n_probes&&i<BF_NETLIST_MAX_PROBES;i++)
        fread(c->probe_names[i],1,128,f);

    fclose(f);
    return c;
}

/* ── public: free ────────────────────────────────────────────────────── */
void bf_circuit_free(BfCircuit *c) {
    if(!c) return;
    free(c->nodes);
    free(c->edges);
    free(c);
}

/* ── public: print ───────────────────────────────────────────────────── */
static const char *pin_kind_name_(BfPinKind k){
    switch(k){
    case BF_PIN_SIGNAL: return "signal";
    case BF_PIN_STATE:  return "state";
    case BF_PIN_MEMORY: return "memory";
    case BF_PIN_COST:   return "cost";
    case BF_PIN_PROOF:  return "proof";
    case BF_PIN_RULE:   return "rule";
    case BF_PIN_VALUE:  return "value";
    case BF_PIN_STREAM: return "stream";
    default:            return "?";
    }
}

void bf_circuit_print(const BfCircuit *c, FILE *fp) {
    if(!fp) fp=stdout;
    fprintf(fp,"circuit: %s\n",c->world);
    fprintf(fp,"  %u components  %u nets  %u probes\n\n",
            c->n_nodes,c->n_edges,c->n_probes);

    for(uint32_t i=0;i<c->n_nodes;i++){
        const BfCircuitNode *n=&c->nodes[i];
        const BfComponentDef *d=bf_component_lookup(n->type);
        fprintf(fp,"  %-12s [%s]  G=%.3f  C=%.3f  L=%.3f  age=%.4f\n",
                n->instance,n->type,
                n->state.conductance,n->state.capacitance,
                n->state.inductance, n->state.aging_rate);
        if(d){
            for(uint32_t j=0;j<d->n_inputs;j++)
                fprintf(fp,"    in %-12s (%s)\n",
                        d->inputs[j].name,pin_kind_name_(d->inputs[j].kind));
            for(uint32_t j=0;j<d->n_outputs;j++)
                fprintf(fp,"    out %-11s (%s)\n",
                        d->outputs[j].name,pin_kind_name_(d->outputs[j].kind));
        }
    }
    fprintf(fp,"\n  nets:\n");
    for(uint32_t i=0;i<c->n_edges;i++){
        const BfCircuitEdge *e=&c->edges[i];
        fprintf(fp,"  [%s] %s[%u] → %s[%u]\n",
                e->net_type,
                e->src_instance,e->src_pin,
                e->dst_instance,e->dst_pin);
    }
    if(c->n_probes){
        fprintf(fp,"\n  probes:");
        for(uint32_t i=0;i<c->n_probes;i++) fprintf(fp," %s",c->probe_names[i]);
        fprintf(fp,"\n");
    }
}
