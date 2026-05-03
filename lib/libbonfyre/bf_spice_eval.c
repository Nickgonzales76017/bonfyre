/*
 * bf_spice_eval.c — Bonfyre mixed-signal transient step engine
 *
 * Evaluates one transient step of the global Bonfyre circuit.
 *
 * Step sequence:
 *   1. Propagate digital events  (PROOF, RULE, MEMORY pins)
 *   2. Resolve mounts / refs / contracts
 *   3. Apply input pulse to source nodes
 *   4. Execute each node's transfer function (topological order)
 *   5. Propagate analog signals over net edges
 *   6. Apply component aging
 *   7. Check convergence (max |ΔV| < tolerance)
 *   8. Fill probe frame
 *   9. Advance step counter
 *
 * Mixed-signal hybrid model:
 *   Digital nodes  : PROOF, RULE, MEMORY, VALUE  (event-driven)
 *   Analog nodes   : SIGNAL, STATE, COST, STREAM  (continuous)
 *   Coupling       : SIGNAL carries both float data and event flag
 *
 * Transient state allocation:
 *   BfTranState::analog  — n_nodes × BF_COMPONENT_MAX_PINS floats
 *   One float per output-pin scalar per node per step.
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <bonfyre.h>

/* ── BfTranState ─────────────────────────────────────────────────────── */
BfTranState *bf_tran_state_alloc(const BfCircuit *c, float dt, uint64_t end) {
    BfTranState *s=calloc(1,sizeof(BfTranState));
    if(!s) return NULL;
    s->dt       = dt>0.0f?dt:0.05f;
    s->tran_end = end;
    s->n_nodes  = c->n_nodes;
    size_t sz   = c->n_nodes * BF_COMPONENT_MAX_PINS * sizeof(float);
    s->analog      = calloc(1, sz);
    s->prev_analog = calloc(1, sz);
    if(!s->analog || !s->prev_analog){ free(s->analog); free(s->prev_analog); free(s); return NULL; }
    return s;
}

void bf_tran_state_free(BfTranState *s) {
    if(!s) return;
    free(s->analog);
    free(s->prev_analog);
    free(s);
}

/* ── internal: find node index ───────────────────────────────────────── */
static int find_node_(const BfCircuit *c, const char *inst) {
    for(uint32_t i=0;i<c->n_nodes;i++)
        if(!strcmp(c->nodes[i].instance,inst)) return (int)i;
    return -1;
}

/* ── internal: probe a named pin value ───────────────────────────────── */
static float probe_pin_(const BfCircuit *c, const BfTranState *s __attribute__((unused)),
                         const char *spec, uint8_t *hash_out, int *is_hash) {
    /* spec = "instance.pin"  or "instance" for first output */
    char inst[64]; snprintf(inst,sizeof(inst),"%s",spec);
    char *dot=strchr(inst,'.');
    const char *pin_name=dot?dot+1:"output";
    if(dot)*dot='\0';

    int ni=find_node_(c,inst);
    if(ni<0) return 0.0f;
    const BfCircuitNode *node=&c->nodes[ni];
    const BfComponentDef *def=bf_component_lookup(node->type);

    /* find output pin index */
    uint8_t pi=0;
    if(def){
        for(uint32_t j=0;j<def->n_outputs;j++)
            if(!strcmp(def->outputs[j].name,pin_name)){ pi=(uint8_t)j; break; }
    }

    const BfSignal *sig=&node->output_buf[pi];
    if(is_hash) *is_hash=0;
    if(hash_out && sig->kind==BF_PIN_PROOF){
        memcpy(hash_out,sig->hash,32);
        if(is_hash)*is_hash=1;
    }
    return sig->scalar;
}

/* ── internal: propagate edges (output → input) ──────────────────────── */
static void propagate_edges_(BfCircuit *c) {
    for(uint32_t i=0;i<c->n_edges;i++){
        const BfCircuitEdge *e=&c->edges[i];
        int si=find_node_(c,e->src_instance);
        int di=find_node_(c,e->dst_instance);
        if(si<0||di<0) continue;
        BfSignal *src_out = &c->nodes[si].output_buf[e->src_pin];
        BfSignal *dst_in  = &c->nodes[di].input_buf [e->dst_pin];
        /* copy signal: scalar, event flag, hash, data pointer, kind */
        *dst_in=*src_out;
    }
}

/* ── internal: topological sort (Kahn's BFS) ─────────────────────────── */
static void topo_sort_(const BfCircuit *c, uint32_t *order) {
    /* in-degree for each node */
    int indeg[BF_NETLIST_MAX_COMPONENTS]={0};
    for(uint32_t i=0;i<c->n_edges;i++){
        int di=find_node_(c,c->edges[i].dst_instance);
        if(di>=0) indeg[di]++;
    }
    uint32_t queue[BF_NETLIST_MAX_COMPONENTS]; int head=0,tail=0;
    for(uint32_t i=0;i<c->n_nodes;i++)
        if(indeg[i]==0) queue[tail++]=(uint32_t)i;
    int placed=0;
    while(head<tail&&placed<(int)c->n_nodes){
        uint32_t n=queue[head++];
        order[placed++]=n;
        /* reduce in-degree of successors */
        for(uint32_t e=0;e<c->n_edges;e++){
            if(!strcmp(c->edges[e].src_instance,c->nodes[n].instance)){
                int di=find_node_(c,c->edges[e].dst_instance);
                if(di>=0&&--indeg[di]==0) queue[tail++]=(uint32_t)di;
            }
        }
    }
    /* any remaining (cycles): append in original order */
    if(placed<(int)c->n_nodes){
        for(uint32_t i=0;i<c->n_nodes;i++){
            int found=0;
            for(int j=0;j<placed;j++) if(order[j]==i){found=1;break;}
            if(!found) order[placed++]=i;
        }
    }
}

/* ── public: bf_spice_eval ───────────────────────────────────────────── */
/* Convergence fix: prior implementation wrote v_new to s->analog then
 * immediately read v_old = s->analog — always zero delta, always converged.
 *
 * New scheme: two-buffer iterative Newton loop.
 *   Before iteration: copy analog → prev_analog (snapshot of last step)
 *   During each iteration: run all transfer functions, propagate edges
 *   After each iteration: compare output scalars against prev_analog
 *   Convergence when max|new-prev| < tolerance (not new vs just-written!)
 *   Write accepted scalars back to analog at end of iteration
 * ─────────────────────────────────────────────────────────────────── */
#define BF_TRAN_MAX_ITER 8
#define BF_TRAN_TOLERANCE 1e-3f

int bf_spice_eval(BfCircuit *c, BfTranState *s,
                   const BfInputPulse *input, BfProbeFrame *out) {
    if(!c||!s) return BF_SPICE_NUMERIC_FAULT;

    int rc=BF_SPICE_OK;
    int contract_blocked=0;
    int topo_gap=0;

    /* ── 1. apply input pulse to source nodes ──────────────────────────────── */
    if(input){
        for(uint32_t i=0;i<c->n_nodes;i++){
            BfCircuitNode *n=&c->nodes[i];
            if(n->n_inputs==0){
                BfSignal *sig=&n->input_buf[0];
                sig->data   =(float*)input->data;
                sig->dim    =input->dim;
                sig->kind   =BF_PIN_SIGNAL;
                sig->scalar =(input->data&&input->dim>0)?(float)input->dim:1.0f;
                sig->event  =input->event;
                memcpy(sig->hash,input->hash,32);
            }
        }
    }

    /* ── 2. compute topological execution order ────────────────────────────── */
    uint32_t order[BF_NETLIST_MAX_COMPONENTS]={0};
    topo_sort_(c,order);

    /* ── 3. snapshot previous analog state ──────────────────────────────────── */
    size_t analog_sz = (size_t)c->n_nodes * BF_COMPONENT_MAX_PINS * sizeof(float);
    memcpy(s->prev_analog, s->analog, analog_sz);

    /* ── 3b. stamp tran epoch into last_touched so stateful transfer fns
     *        (e.g. physics_transfer_) can commit state exactly once per tran
     *        step instead of once per Newton iteration.
     *        Epoch = s->step + 1 so initial value 0 is never a valid epoch. */
    uint64_t tran_epoch = s->step + 1u;
    for (uint32_t i = 0; i < c->n_nodes; i++)
        c->nodes[i].state.last_touched = tran_epoch;

    /* ── 4. iterative Newton convergence loop ─────────────────────────────── */
    s->converged = 0;
    s->n_iters   = 0;
    for(int iter=0; iter<BF_TRAN_MAX_ITER; iter++){
        s->n_iters++;
        contract_blocked = 0;
        topo_gap         = 0;

        /* evaluate all nodes in topological order */
        for(uint32_t oi=0;oi<c->n_nodes;oi++){
            BfCircuitNode *n=&c->nodes[order[oi]];
            const BfComponentDef *def=bf_component_lookup(n->type);
            if(!def||!def->transfer) continue;

            int node_rc=def->transfer(
                n->input_buf,  n->n_inputs,
                n->output_buf, n->n_outputs,
                &n->state
            );

            /* apply aging */
            n->state.conductance *= (1.0f - n->state.aging_rate);
            if(n->state.conductance<0.001f) n->state.conductance=0.001f;

            if(node_rc==BF_SPICE_CONTRACT_BLOCK) contract_blocked=1;
            if(node_rc==BF_SPICE_TOPO_GAP)      topo_gap=1;
            if(node_rc==BF_SPICE_NUMERIC_FAULT){ rc=BF_SPICE_NUMERIC_FAULT; goto fill_probe; }

            /* propagate outputs → downstream inputs */
            propagate_edges_(c);
        }

        /* ── check convergence against PREVIOUS state (not current write) ── */
        float max_dv = 0.0f;
        for(uint32_t i=0;i<c->n_nodes;i++){
            const BfCircuitNode *n=&c->nodes[i];
            for(uint32_t pi=0;pi<n->n_outputs&&pi<BF_COMPONENT_MAX_PINS;pi++){
                float v_new = n->output_buf[pi].scalar;
                float v_old = s->prev_analog[i*BF_COMPONENT_MAX_PINS+pi];
                float dv    = fabsf(v_new - v_old);
                if(dv > max_dv) max_dv = dv;
                /* update analog buffer with accepted new value */
                s->analog[i*BF_COMPONENT_MAX_PINS+pi] = v_new;
            }
        }

        if(max_dv < BF_TRAN_TOLERANCE){
            s->converged = 1;
            break;
        }
        /* copy current analog → prev for next iteration */
        memcpy(s->prev_analog, s->analog, analog_sz);
    }

    /* ── 5. advance transient time ────────────────────────────────────────────── */
    s->step++;
    s->t += s->dt;

    /* ── 6. determine return code priority ─────────────────────────────────────── */
    if(contract_blocked)            { rc=BF_SPICE_CONTRACT_BLOCK; goto fill_probe; }
    if(topo_gap)                    { rc=BF_SPICE_TOPO_GAP;       goto fill_probe; }
    if(!s->converged && s->step>1)  rc=BF_SPICE_NOT_CONVERGED;
    else                            rc=BF_SPICE_OK;

fill_probe:
    /* ── 8. fill probe frame ─────────────────────────────────────── */
    if(out){
        memset(out,0,sizeof(*out));
        out->step=s->step;
        out->t   =s->t;
        out->status=rc;
        out->n_probes=c->n_probes<BF_NETLIST_MAX_PROBES?c->n_probes:BF_NETLIST_MAX_PROBES;
        for(uint32_t i=0;i<out->n_probes;i++){
            snprintf(out->names[i],128,"%s",c->probe_names[i]);
            out->values[i]=probe_pin_(c,s,c->probe_names[i],
                                       out->hashes[i],
                                       (int*)&out->is_hash[i]);
        }
    }
    return rc;
}
