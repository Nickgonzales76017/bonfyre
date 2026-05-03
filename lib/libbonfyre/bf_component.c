/*
 * bf_component.c — Bonfyre component registry
 *
 * Each Bonfyre binary is a circuit element with a defined
 * set of input/output pins and a transfer function.
 *
 * Transfer function contract:
 *   - Called once per transient step for each active component.
 *   - May read inputs[], write outputs[].
 *   - May mutate state->priv, state->conductance, state->step.
 *   - Returns BF_SPICE_* code.
 *
 * Built-in component types (matching the barroom architecture):
 *
 *   BonfyreTel        signal source / audio event
 *   BonfyreMoQ        live bus / transmission line
 *   BonfyreEmbed      embedding source + semantic impedance
 *   BonfyreKVCache    memory capacitor / inductive state
 *   BonfyrePhysics    Hamiltonian integrator
 *   BonfyreCMS        state latch / event crystal
 *   BonfyreDisCIPL    constraint diode / rule gate
 *   BonfyreMeter      current meter
 *   BonfyreLedger     accumulated charge / value sink
 *   BonfyreLayer      ontology substrate
 *   BonfyreHash       ground truth reference node
 *   BonfyreWorkflow   netlist compiler
 *   BonfyreHeSli      dielectric isolation layer (private potential field)
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <bonfyre.h>

/* ── registry ────────────────────────────────────────────────────────── */
#define MAX_REGISTRY 64
static BfComponentDef  reg_store_[MAX_REGISTRY];
static int             reg_n_=0;
static int             reg_init_=0;

/* ── stub transfer: passthrough (identity for analog pins) ──────────── */
static int stub_transfer_(const BfSignal *in, size_t ni,
                           BfSignal *out, size_t no,
                           BfComponentState *st) {
    (void)st;
    size_t copy=ni<no?ni:no;
    for(size_t i=0;i<copy;i++) out[i]=in[i];
    return BF_SPICE_OK;
}

/* ── BonfyreTel — signal source ──────────────────────────────────────── */
/* Fix 3: forward the injected pulse payload intact.  Prior to this fix
 * Tel silently dropped in[0].data and only emitted conductance as a
 * scalar — so every downstream node saw an eventless wire, not the
 * encoded audio/token pulse.  Now Tel is a transparent source: it stamps
 * event=1, preserves the payload, and falls back to a conductance
 * scalar only when the input is empty (i.e., pure drive mode). */
static int tel_transfer_(const BfSignal *in, size_t ni,
                          BfSignal *out, size_t no,
                          BfComponentState *st) {
    (void)no;
    out[0].event = 1;
    /* NOTE: BonfyreTel has n_inputs=0 (pure source), so ni==0 always.
     * bf_spice_eval still injects the input pulse into input_buf[0] for
     * source nodes (nodes where n_inputs==0).  We read in[0] directly
     * rather than checking ni>0, because the pulse injection bypasses
     * the normal pin declaration mechanism.  This is correct: input_buf
     * is pre-allocated at BF_COMPONENT_MAX_PINS regardless of n_inputs. */
    if (in[0].data && in[0].dim > 0) {
        /* forward pulse payload from input (injected by bf_spice_eval) */
        out[0].data         = in[0].data;
        out[0].dim          = in[0].dim;
        out[0].kind         = in[0].kind;
        out[0].scalar       = in[0].scalar > 0.0f ? in[0].scalar : st->conductance;
        out[0].payload      = in[0].payload;
        out[0].payload_kind = in[0].payload_kind;
        memcpy(out[0].hash, in[0].hash, 32);
    } else {
        /* drive mode: no external pulse, emit conductance as amplitude */
        out[0].data         = NULL;
        out[0].dim          = 0;
        out[0].kind         = BF_PIN_SIGNAL;
        out[0].scalar       = st->conductance;
        out[0].payload      = NULL;
        out[0].payload_kind = BF_PAYLOAD_NONE;
    }
    st->step++;
    return BF_SPICE_OK;
}

/* ── BonfyreEmbed — vector source ────────────────────────────────────── */
static int embed_transfer_(const BfSignal *in, size_t ni,
                            BfSignal *out, size_t no,
                            BfComponentState *st) {
    (void)no;(void)st;
    if(ni<1||!in[0].data) return BF_SPICE_OK;
    /* pass embedding vector through as output q */
    out[0]=in[0];
    out[0].kind=BF_PIN_STATE;
    return BF_SPICE_OK;
}

/* ── BonfyreKVCache — memory capacitor ───────────────────────────────── */
/* Fix 6: emit a live BfMemoryField instead of a scalar.
 * Prior fix: kv_transfer_ multiplied in[0].scalar by conductance and
 * forwarded the pin unchanged — so BonfyrePhysics received a float, not
 * a BVH + pack pair, and bf_physics_step had nothing to integrate over.
 *
 * Now:
 *   st->priv → BfMemoryField* (allocated once, updated each step)
 *   out[0].payload      = st->priv
 *   out[0].payload_kind = BF_PAYLOAD_MEMORY_FIELD
 *   out[0].scalar       = conductance (analog signal for convergence)
 *
 * Pack and BVH are not opened here (no path available at transfer time);
 * the pointers remain NULL until an external call arms the component via
 * BfComponentState::priv = &pre_built_BfMemoryField.
 * This is the correct boot sequence: arm KVCache priv BEFORE running
 * the circuit, the same way a real chip loads firmware into RAM before
 * applying power. */
static int kv_transfer_(const BfSignal *in, size_t ni,
                         BfSignal *out, size_t no,
                         BfComponentState *st) {
    (void)ni; (void)no;
    /* allocate memory field on first call */
    if (!st->priv) {
        BfMemoryField *mf = calloc(1, sizeof(BfMemoryField));
        if (!mf) return BF_SPICE_NUMERIC_FAULT;
        /* seed ctx_hash from first input hash if available */
        if (ni > 0) memcpy(mf->ctx_hash, in[0].hash, 32);
        st->priv = mf;
    }
    BfMemoryField *mf = (BfMemoryField *)st->priv;
    /* refresh ctx_hash each step from query hash */
    if (ni > 0) memcpy(mf->ctx_hash, in[0].hash, 32);

    /* emit live memory field as typed payload */
    memset(&out[0], 0, sizeof(BfSignal));
    out[0].kind         = BF_PIN_MEMORY;
    out[0].scalar       = st->conductance;
    out[0].event        = (ni > 0) ? in[0].event : 0;
    out[0].payload      = mf;
    out[0].payload_kind = BF_PAYLOAD_MEMORY_FIELD;
    out[0].payload_flags= BF_PAYLOAD_F_BORROWED | BF_PAYLOAD_F_READONLY | BF_PAYLOAD_F_FRAME;
    out[0].payload_life = BF_PAYLOAD_LIFE_STEP;   /* priv lives across steps */
    out[0].payload_size = (uint32_t)sizeof(BfMemoryField);
    memcpy(out[0].hash, mf->ctx_hash, 32);

    /* analog decay (capacitor charge loss) */
    st->conductance *= (1.0f - st->aging_rate);
    if (st->conductance < 0.01f) st->conductance = 0.01f;
    st->step++;
    return BF_SPICE_OK;
}

/* ── BonfyrePhysics — Hamiltonian integrator ─────────────────────────── */
/* Fix 1: call real bf_physics_step every time a memory field is wired in.
 *
 * Pin layout (from BUILTINS_ below):
 *   in[0] = q     (BF_PIN_STATE) — current position/momentum vector
 *   in[1] = field (BF_PIN_MEMORY) — BfMemoryField* from BonfyreKVCache
 *   in[2] = constraints (BF_PIN_RULE) — BfHeSliResult* force from HE-SLI
 *
 *   out[0] = q       (BF_PIN_STATE)  — updated position after step
 *   out[1] = entropy (BF_PIN_COST)   — Hamiltonian-derived entropy scalar
 *   out[2] = hash    (BF_PIN_PROOF)  — SHA-256(q) as commitment
 *
 * st->priv → BfPhysicsState* (lazy-allocated on first call).
 *
 * Convergence note: because bf_physics_step is deterministic for the
 * same (ps, bvh, pack) inputs, iterating the Newton loop does converge
 * — the entropy output strictly decreases toward the gradient minimum. */
static int physics_transfer_(const BfSignal *in, size_t ni,
                              BfSignal *out, size_t no,
                              BfComponentState *st) {
    (void)no;
    int rc = BF_SPICE_OK;
    int topo_gap = 0;

    /* ---- lazy-allocate physics state ---- */
    if (!st->priv) {
        uint32_t dim = (ni > 0 && in[0].data && in[0].dim > 0) ? in[0].dim : 8u;
        /* sigma for the KDE kernel: must be O(mean inter-vector distance).
         * For L2-normalized unit-sphere embeddings, typical distances are
         * 0.5–1.5, so sigma=0.5 is a reasonable default.  If the user
         * wires in a specific tolerance, honour it only if it's sane. */
        float sigma = (st->tolerance > 0.05f && st->tolerance < 10.0f)
                      ? st->tolerance : 0.5f;
        float dt    = (st->inductance > 0.001f) ? st->inductance : 0.05f;
        BfPhysicsState *ps = bf_physics_state_alloc(dim, sigma, dt);
        if (!ps) return BF_SPICE_NUMERIC_FAULT;
        /* seed q from input vector */
        if (ni > 0 && in[0].data && in[0].dim == dim)
            memcpy(ps->q, in[0].data, dim * sizeof(float));
        st->priv = ps;
    }
    BfPhysicsState *ps = (BfPhysicsState *)st->priv;

    /* ---- update q from in[0] if a new pulse arrived ---- */
    if (ni > 0 && in[0].data && in[0].dim == ps->dim && in[0].event)
        memcpy(ps->q, in[0].data, ps->dim * sizeof(float));

    /* ---- apply HE-SLI force kick from in[2] ---- */
    if (ni > 2 && in[2].payload_kind == BF_PAYLOAD_HESLI_RESULT && in[2].payload) {
        const BfHeSliResult *hr = (const BfHeSliResult *)in[2].payload;
        if (hr->gate) {
            /* project 8-dim force onto q's leading dimensions */
            uint32_t kd = ps->dim < 8u ? ps->dim : 8u;
            for (uint32_t i = 0; i < kd; i++)
                ps->p[i] += hr->projected_force[i] * hr->potential_delta * ps->dt;
        }
    }

    /* ---- run Hamiltonian step if field is available ---- */
    BfEmbedBVH  *bvh  = NULL;
    BfEmbedPack *pack = NULL;
    if (ni > 1 && in[1].payload_kind == BF_PAYLOAD_MEMORY_FIELD && in[1].payload) {
        const BfMemoryField *mf = (const BfMemoryField *)in[1].payload;
        bvh  = mf->bvh;
        pack = mf->embed_pack;
    }
    /* Advance physics at most ONCE per transient step.  bf_spice_eval stamps
     * last_touched = tran_epoch (> 0) before the Newton loop; subsequent
     * iterations on the same tran step see committed_epoch == last_touched
     * and skip bf_physics_step, re-emitting the already-advanced state. */
    if (ps->committed_epoch < st->last_touched) {
        int step_rc = bf_physics_step(ps, bvh, pack);
        ps->committed_epoch = st->last_touched;
        if (step_rc == 1) topo_gap = 1;  /* topological gap detected */
    }

    /* ---- output q ---- */
    out[0].data         = ps->q;
    out[0].dim          = ps->dim;
    out[0].kind         = BF_PIN_STATE;
    out[0].scalar       = ps->q[0]; /* leading coordinate as scalar */
    out[0].event        = 0;
    out[0].payload      = ps;
    out[0].payload_kind = BF_PAYLOAD_PHYSICS_STATE;
    out[0].payload_flags= BF_PAYLOAD_F_BORROWED | BF_PAYLOAD_F_READONLY | BF_PAYLOAD_F_FRAME;
    out[0].payload_life = BF_PAYLOAD_LIFE_CIRCUIT; /* ps lives in st->priv */
    out[0].payload_size = (uint32_t)sizeof(BfPhysicsState);

    /* ---- output entropy (Hamiltonian H as proxy) ---- */
    float H = bf_physics_hamiltonian(ps, bvh, pack);
    out[1].kind         = BF_PIN_COST;
    out[1].scalar       = H;
    out[1].event        = 0;

    /* ---- output proof: SHA-256(q) ---- */
    char hex[65]; bf_sha256_hex((const uint8_t *)ps->q,
                                 ps->dim * sizeof(float), hex);
    out[2].kind  = BF_PIN_PROOF;
    out[2].event = 1;
    /* decode first 32 bytes of hex into hash bytes */
    for (int i = 0; i < 32; i++) {
        unsigned v = 0;
        sscanf(hex + i * 2, "%02x", &v);
        out[2].hash[i] = (uint8_t)v;
    }

    st->step++;
    if (topo_gap) return BF_SPICE_TOPO_GAP;
    return rc;
}

/* ── BonfyreCMS — state latch ─────────────────────────────────────────── */
static int cms_transfer_(const BfSignal *in, size_t ni,
                          BfSignal *out, size_t no,
                          BfComponentState *st) {
    (void)no;(void)ni;
    if(ni>0&&in[0].event){
        out[0]=in[0];
        out[0].event=1;
    }
    st->step++;
    return BF_SPICE_OK;
}

/* ── BonfyreDisCIPL — constraint diode / rule gate ───────────────────── */
static int discipl_transfer_(const BfSignal *in, size_t ni,
                              BfSignal *out, size_t no,
                              BfComponentState *st) {
    (void)no;(void)ni;(void)st;
    /* Diode model: forward-bias only when rule set allows */
    if(ni>=2&&in[1].event){
        /* rule violation detected on in[1] (rule pin) */
        out[0].event=0;
        out[0].scalar=0.0f;
        return BF_SPICE_CONTRACT_BLOCK;
    }
    if(ni>0) out[0]=in[0];
    return BF_SPICE_OK;
}

/* ── BonfyreMeter — current meter ────────────────────────────────────── */
static int meter_transfer_(const BfSignal *in, size_t ni,
                            BfSignal *out, size_t no,
                            BfComponentState *st) {
    (void)no;
    float current=0.0f;
    if(ni>0) current=in[0].scalar;
    out[0].kind=BF_PIN_COST;
    out[0].scalar=current*st->conductance;
    out[0].event=1;
    st->step++;
    return BF_SPICE_OK;
}

/* ── BonfyreLedger — value sink ──────────────────────────────────────── */
static int ledger_transfer_(const BfSignal *in, size_t ni,
                             BfSignal *out, size_t no,
                             BfComponentState *st) {
    (void)no;(void)out;
    float val=0.0f;
    if(ni>0) val=in[0].scalar;
    /* accumulate in capacitance slot */
    st->capacitance+=val;
    st->step++;
    return BF_SPICE_OK;
}

/* ── BonfyreMoQ — live stream bus ────────────────────────────────────── */
static int moq_transfer_(const BfSignal *in, size_t ni,
                          BfSignal *out, size_t no,
                          BfComponentState *st) {
    (void)no;(void)st;
    if(ni>0) out[0]=in[0];
    out[0].kind=BF_PIN_STREAM;
    return BF_SPICE_OK;
}

/* ── BonfyreHash — ground truth reference node ───────────────────────── */
static int hash_transfer_(const BfSignal *in, size_t ni,
                           BfSignal *out, size_t no,
                           BfComponentState *st) {
    (void)no;(void)st;
    if(ni>0){ memcpy(out[0].hash,in[0].hash,32); out[0].kind=BF_PIN_PROOF; out[0].event=1; }
    return BF_SPICE_OK;
}

/* ── BonfyreHeSli — dielectric isolation layer ───────────────────────
 *
 * private_in (SIGNAL) → [HE-SLI boundary] → public_out (SIGNAL)
 *
 * Params packed into BfComponentState:
 *   conductance → BfHeSliLevel (0-3)
 *   capacitance → allowed_outputs bitmask (BF_HESLI_OUT_* flags)
 *   inductance  → meter_rate
 *
 * The private input pin is consumed but not forwarded as-is.
 * The public output pin contains only the observable result.
 */
static int hesli_transfer_(const BfSignal *in, size_t ni,
                             BfSignal *out, size_t no,
                             BfComponentState *st) {
    BfHeSliPolicy pol; memset(&pol,0,sizeof(pol));
    pol.level          = (BfHeSliLevel)(int)(st->conductance);
    pol.allowed_outputs= (uint32_t)(st->capacitance);
    pol.meter_rate     = st->inductance;
    if(pol.allowed_outputs==0) pol.allowed_outputs=BF_HESLI_OUT_DEFAULT;

    /* Lazy-alloc a persistent BfHeSliResult in st->priv so the payload
     * pointer emitted via bf_hesli_result_to_signal is heap-stable across
     * the lifetime of this component (not a dangling stack pointer). */
    if (!st->priv) {
        st->priv = calloc(1, sizeof(BfHeSliResult));
        if (!st->priv) return BF_SPICE_NUMERIC_FAULT;
    }
    BfHeSliResult *res = (BfHeSliResult *)st->priv;
    memset(res, 0, sizeof(*res));

    const float *q=(ni>0&&in[0].data)?in[0].data:NULL;
    uint32_t dim =(ni>0)?in[0].dim:0;
    int rc=bf_hesli_eval(q,dim,&pol,res);

    if(no>0) bf_hesli_result_to_signal(res,pol.allowed_outputs,&out[0]);

    /* Update meter via conductance/step (caller sees billing via scalar) */
    st->step++;
    return rc;
}

/* ── pin definition helpers ─────────────────────────────────────────── */
static BfPin pin_(const char *name, BfPinKind k, uint32_t dim) {
    BfPin p; memset(&p,0,sizeof(p));
    snprintf(p.name,sizeof(p.name),"%s",name);
    p.kind=k; p.dim=dim; p.impedance=1.0f; p.tolerance=0.01f;
    return p;
}

/* ── built-in component definitions ─────────────────────────────────── */
static const struct {
    const char    *name;
    BfTransferFn   fn;
    uint32_t ni, no;
    const char *in_names [BF_COMPONENT_MAX_PINS];
    BfPinKind  in_kinds  [BF_COMPONENT_MAX_PINS];
    uint32_t   in_dims   [BF_COMPONENT_MAX_PINS];
    const char *out_names[BF_COMPONENT_MAX_PINS];
    BfPinKind  out_kinds [BF_COMPONENT_MAX_PINS];
    uint32_t   out_dims  [BF_COMPONENT_MAX_PINS];
} BUILTINS_[] = {
    {"BonfyreTel",
     tel_transfer_, 0, 1,
     {}, {}, {},
     {"signal"}, {BF_PIN_SIGNAL}, {0}},
    {"BonfyreMoQ",
     moq_transfer_, 1, 1,
     {"stream"}, {BF_PIN_STREAM}, {0},
     {"stream"}, {BF_PIN_STREAM}, {0}},
    {"BonfyreEmbed",
     embed_transfer_, 1, 2,
     {"input"},  {BF_PIN_SIGNAL}, {0},
     {"q","refs"},{BF_PIN_STATE,BF_PIN_MEMORY},{512,0}},
    {"BonfyreKVCache",
     kv_transfer_, 1, 1,
     {"query"}, {BF_PIN_SIGNAL}, {0},
     {"mounts"},{BF_PIN_MEMORY}, {0}},
    {"BonfyrePhysics",
     physics_transfer_, 3, 3,
     {"q","field","constraints"},{BF_PIN_STATE,BF_PIN_MEMORY,BF_PIN_RULE},{512,0,0},
     {"q","entropy","hash"},{BF_PIN_STATE,BF_PIN_COST,BF_PIN_PROOF},{512,0,0}},
    {"BonfyreCMS",
     cms_transfer_, 1, 1,
     {"ops"}, {BF_PIN_SIGNAL},{0},
     {"events"},{BF_PIN_SIGNAL},{0}},
    {"BonfyreDisCIPL",
     discipl_transfer_, 2, 1,
     {"op","rules"},{BF_PIN_SIGNAL,BF_PIN_RULE},{0,0},
     {"allowed"},{BF_PIN_SIGNAL},{0}},
    {"BonfyreMeter",
     meter_transfer_, 1, 1,
     {"usage"},{BF_PIN_COST},{0},
     {"invoice"},{BF_PIN_COST},{0}},
    {"BonfyreLedger",
     ledger_transfer_, 1, 0,
     {"value"},{BF_PIN_VALUE},{0},
     {},{},{}},
    {"BonfyreLayer",
     stub_transfer_, 1, 1,
     {"input"},{BF_PIN_SIGNAL},{0},
     {"output"},{BF_PIN_SIGNAL},{0}},
    {"BonfyreHash",
     hash_transfer_, 1, 1,
     {"input"},{BF_PIN_PROOF},{0},
     {"ref"},{BF_PIN_PROOF},{0}},
    {"BonfyreWorkflow",
     stub_transfer_, 1, 1,
     {"input"},{BF_PIN_SIGNAL},{0},
     {"output"},{BF_PIN_SIGNAL},{0}},
    {"BonfyreHeSli",
     hesli_transfer_, 2, 1,
     {"private_in","policy"},{BF_PIN_SIGNAL,BF_PIN_RULE},{0,0},
     {"public_out"},{BF_PIN_SIGNAL},{0}},
};
#define N_BUILTINS_ (int)(sizeof(BUILTINS_)/sizeof(BUILTINS_[0]))

/* ── public ──────────────────────────────────────────────────────────── */
void bf_component_registry_init(void) {
    if(reg_init_) return;
    reg_init_=1; reg_n_=0;
    for(int b=0;b<N_BUILTINS_;b++){
        BfComponentDef d; memset(&d,0,sizeof(d));
        snprintf(d.name,sizeof(d.name),"%s",BUILTINS_[b].name);
        d.n_inputs  = BUILTINS_[b].ni;
        d.n_outputs = BUILTINS_[b].no;
        d.transfer  = BUILTINS_[b].fn;
        for(uint32_t i=0;i<d.n_inputs&&i<BF_COMPONENT_MAX_PINS;i++)
            d.inputs[i]=pin_(BUILTINS_[b].in_names[i],
                             BUILTINS_[b].in_kinds[i],
                             BUILTINS_[b].in_dims[i]);
        for(uint32_t i=0;i<d.n_outputs&&i<BF_COMPONENT_MAX_PINS;i++)
            d.outputs[i]=pin_(BUILTINS_[b].out_names[i],
                              BUILTINS_[b].out_kinds[i],
                              BUILTINS_[b].out_dims[i]);
        bf_component_register(&d);
    }
}

int bf_component_register(const BfComponentDef *def) {
    if(!def) return -1;
    if(reg_n_>=MAX_REGISTRY) return -1;
    /* overwrite if already registered */
    for(int i=0;i<reg_n_;i++){
        if(!strcmp(reg_store_[i].name,def->name)){
            reg_store_[i]=*def; return 0;
        }
    }
    reg_store_[reg_n_++]=*def;
    return 0;
}

const BfComponentDef *bf_component_lookup(const char *type_name) {
    if(!reg_init_) bf_component_registry_init();
    for(int i=0;i<reg_n_;i++)
        if(!strcmp(reg_store_[i].name,type_name)) return &reg_store_[i];
    return NULL;
}

int bf_component_registry_list(const BfComponentDef **out, int max_n) {
    if(!reg_init_) bf_component_registry_init();
    int n=reg_n_<max_n?reg_n_:max_n;
    for(int i=0;i<n;i++) out[i]=&reg_store_[i];
    return n;
}
