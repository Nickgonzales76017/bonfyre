/*
 * bonfyre-violence — real coupling test harness
 *
 * This is the "real violence test": no stubs, no symbolic edges.
 *
 * What it does:
 *   1. Synthesize 128 real float32 embedding vectors (dim=32) derived
 *      from SHA-256 of the real-data/ text corpus.  Each vector is
 *      L2-normalised so the BVH cosine landscape is well-conditioned.
 *   2. Write each vector as a .bfembed file into the bonfyre embed store.
 *   3. Call bf_embed_pack_build()  → produces a .bfpk pack.
 *   4. Call bf_embed_bvh_build()   → produces a .bfvh BVH.
 *   5. Open pack + BVH.  Arm a BfMemoryField and inject it into the
 *      KVCache component's priv pointer (the correct boot sequence).
 *   6. Compile and run the full violence circuit:
 *
 *        tel0   BonfyreTel
 *          │ signal
 *        emb0   BonfyreEmbed
 *          │ q
 *        hesli0 BonfyreHeSli      (private force emitter)
 *          │ public_out  ┐
 *        kv0    BonfyreKVCache ──┐ (armed with real BfMemoryField)
 *          │ mounts             │
 *        phy0   BonfyrePhysics ──┘
 *          │ q/entropy/hash
 *        cms0   BonfyreCMS        (event latch / trace capture)
 *        mtr0   BonfyreMeter      (entropy current)
 *        lgr0   BonfyreLedger     (value accumulator)
 *        hsh0   BonfyreHash       (proof anchor)
 *
 *   7. Run 32 transient steps.  Print per-step entropy, hash, n_iters.
 *   8. Report final Hamiltonian and nearest-neighbor from real pack.
 *
 * Build:
 *   make -C cmd/BonfyreViolence
 *
 * Ownership rules exercised:
 *   - BfMemoryField owned by violence.c, injected read-only into kv0.priv.
 *   - BfHeSliResult borrowed for current frame (BF_PAYLOAD_F_FRAME).
 *   - BfPhysicsState owned by phy0.state.priv (BF_PAYLOAD_F_BORROWED).
 *   - Pack/BVH mmap-backed; valid for duration of main() (BF_PAYLOAD_F_MMAP).
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>
#include <dirent.h>
#include <bonfyre.h>

/* ── constants ──────────────────────────────────────────────────────── */
#define EMBED_DIM   32u
#define N_VECS      128u
#define N_STEPS     32
static const char *pack_path = "/tmp/bonfyre-violence.bfpk";
static const char *bvh_path = "/tmp/bonfyre-violence.bfvh";
#define REAL_DATA_DEFAULT "real-data"
/* Override corpus path via BF_REAL_DATA env var, fallback to ./real-data */
static const char *real_data_path(void) {
    const char *e = getenv("BF_REAL_DATA");
    return e ? e : REAL_DATA_DEFAULT;
}

/* ── embed generation ───────────────────────────────────────────────── */

/* Turn a 32-byte SHA-256 hash into a unit float32 vector of `dim` dims.
 * We use pairs of bytes as signed 8-bit values → rescale → L2 normalise.
 * This is deterministic: same hash always → same vector. */
static void hash_to_vec(const uint8_t h[32], float *out, uint32_t dim) {
    /* fill from hash bytes cyclically */
    for (uint32_t i = 0; i < dim; i++) {
        uint8_t b = h[i % 32];
        out[i] = ((float)(int8_t)b) / 128.0f;
    }
    /* L2 normalise */
    float norm = 0.0f;
    for (uint32_t i = 0; i < dim; i++) norm += out[i] * out[i];
    norm = sqrtf(norm);
    if (norm > 1e-6f)
        for (uint32_t i = 0; i < dim; i++) out[i] /= norm;
}

/* Read first 4096 bytes of a file and return SHA-256 of content. */
static int file_sha256(const char *path, uint8_t hash[32]) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf), f);
    fclose(f);
    BfSha256 s; bf_sha256_init(&s);
    bf_sha256_update(&s, (const uint8_t *)buf, n);
    bf_sha256_final(&s, hash);
    return 0;
}

/* Generate N_VECS embeddings from real text files, store to embed store. */
static int generate_embeds(uint32_t *n_out) {
    DIR *d = opendir(real_data_path());
    if (!d) {
        fprintf(stderr, "violence: cannot open real-data dir: %s\n", real_data_path());
        return -1;
    }
    uint32_t n = 0;
    struct dirent *ent;
    float vec[EMBED_DIM];
    while ((ent = readdir(d)) != NULL && n < N_VECS) {
        if (ent->d_name[0] == '.') continue;
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", real_data_path(), ent->d_name);
        uint8_t hash[32];
        if (file_sha256(path, hash) != 0) continue;
        hash_to_vec(hash, vec, EMBED_DIM);
        bf_embed_store(hash, vec, EMBED_DIM);
        n++;
    }
    closedir(d);
    if (n == 0) {
        fprintf(stderr, "violence: no text files found in %s\n", real_data_path());
        return -1;
    }
    printf("  ▶ generated %u embeddings (dim=%u) from real-data/\n", n, EMBED_DIM);
    *n_out = n;
    return 0;
}

/* ── netlist definition ─────────────────────────────────────────────── */
static const char NETLIST[] =
    ".WORLD violence\n"
    ".COMPONENT tel0   BonfyreTel\n"
    ".COMPONENT emb0   BonfyreEmbed\n"
    ".COMPONENT hesli0 BonfyreHeSli\n"
    ".COMPONENT kv0    BonfyreKVCache\n"
    ".COMPONENT phy0   BonfyrePhysics\n"
    ".COMPONENT cms0   BonfyreCMS\n"
    ".COMPONENT mtr0   BonfyreMeter\n"
    ".COMPONENT lgr0   BonfyreLedger\n"
    ".COMPONENT hsh0   BonfyreHash\n"
    ".NET tel0.signal    emb0.input\n"
    ".NET emb0.q         hesli0.private_in\n"
    ".NET emb0.q         kv0.query\n"
    ".NET emb0.q         phy0.q\n"
    ".NET kv0.mounts     phy0.field\n"
    ".NET hesli0.public_out phy0.constraints\n"
    ".NET phy0.entropy   mtr0.usage\n"
    ".NET phy0.entropy   lgr0.value\n"
    ".NET phy0.hash      cms0.ops\n"
    ".NET phy0.hash      hsh0.input\n"
    ".TRAN 0 32 0.05\n"
    ".PROBE phy0.entropy phy0.hash\n";

/* ── arm KVCache with real memory field ─────────────────────────────── */
static void arm_kvcache(BfCircuit *c,
                         BfEmbedPack *pack, BfEmbedBVH *bvh,
                         BfMemoryField *mf) {
    /* populate the memory field */
    memset(mf, 0, sizeof(*mf));
    mf->embed_pack = pack;
    mf->bvh        = bvh;
    mf->n_mounts   = 1;
    mf->readonly   = 0;
    /* hash "violence" as model_hash */
    BfSha256 hs;
    bf_sha256_init(&hs);
    bf_sha256_update(&hs, (const uint8_t *)"real-data-ctx", 13);
    bf_sha256_final(&hs, mf->ctx_hash);

    /* inject into kv0 priv — this IS the "firmware load" step */
    for (uint32_t i = 0; i < c->n_nodes; i++) {
        if (strcmp(c->nodes[i].instance, "kv0") == 0) {
            /* replace any auto-allocated priv with our armed field */
            if (c->nodes[i].state.priv) free(c->nodes[i].state.priv);
            c->nodes[i].state.priv = mf;
            printf("  ▶ kv0 armed: pack=%u vecs, bvh=%p\n", pack->n, (void *)bvh);
            break;
        }
    }
}

/* ── print a hex prefix of a 32-byte hash ─────────────────────────── */
static void print_hash8(const uint8_t h[32]) {
    for (int i = 0; i < 8; i++) printf("%02x", h[i]);
    printf("…");
}

/* ── main ────────────────────────────────────────────────────────────── */
int main(int argc, char **argv) {
    for (int index = 1; index < argc; index++) {
        if (strcmp(argv[index], "--data") == 0 && index + 1 < argc) {
            setenv("BF_REAL_DATA", argv[++index], 1);
        } else if (strcmp(argv[index], "--pack") == 0 && index + 1 < argc) {
            pack_path = argv[++index];
        } else if (strcmp(argv[index], "--bvh") == 0 && index + 1 < argc) {
            bvh_path = argv[++index];
        } else {
            fprintf(stderr, "usage: bonfyre-violence [--data DIR] [--pack FILE] [--bvh FILE]\n");
            return 1;
        }
    }
    printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    printf("  BONFYRE VIOLENCE TEST  —  real coupling run\n");
    printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n");

    bf_component_registry_init();

    /* ── 1. generate real embeddings ── */
    uint32_t n_embeds = 0;
    if (generate_embeds(&n_embeds) != 0) return 1;

    /* ── 2. build pack ── */
    uint32_t n_packed = 0;
    if (bf_embed_pack_build(pack_path, &n_packed) != 0) {
        fprintf(stderr, "violence: bf_embed_pack_build failed\n"); return 1;
    }
    printf("  ▶ pack built: %u vectors → %s\n", n_packed, pack_path);

    /* ── 3. build BVH ── */
    if (bf_embed_bvh_build(pack_path, bvh_path) != 0) {
        fprintf(stderr, "violence: bf_embed_bvh_build failed\n"); return 1;
    }
    printf("  ▶ BVH  built: %s\n", bvh_path);

    /* ── 4. open pack + BVH ── */
    BfEmbedPack pack = {0};
    BfEmbedBVH  bvh  = {0};
    if (bf_embed_pack_open(&pack, pack_path) != 0) {
        fprintf(stderr, "violence: cannot open pack\n"); return 1;
    }
    if (bf_embed_bvh_open(&bvh, bvh_path) != 0) {
        fprintf(stderr, "violence: cannot open BVH\n");
        bf_embed_pack_close(&pack); return 1;
    }
    printf("  ▶ pack open: n=%u dim=%u\n", pack.n, pack.dim);

    /* ── 5. write netlist to temp file, parse → circuit ── */
    const char *nl_path = "/tmp/bonfyre-violence.bfnet";
    FILE *nf = fopen(nl_path, "w");
    if (!nf) { fprintf(stderr, "violence: cannot write netlist\n"); return 1; }
    fputs(NETLIST, nf);
    fclose(nf);

    BfNetlist nl = {0};
    if (bf_netlist_parse(nl_path, &nl) != 0) {
        fprintf(stderr, "violence: netlist parse failed\n"); return 1;
    }
    BfCircuit *c = bf_circuit_compile(&nl);
    if (!c) { fprintf(stderr, "violence: circuit compile failed\n"); return 1; }
    printf("  ▶ circuit: %u nodes, %u edges\n\n", c->n_nodes, c->n_edges);

    /* ── 6. arm KVCache ── */
    /* BfMemoryField must outlive the circuit run; allocate on heap */
    BfMemoryField *mf = malloc(sizeof(BfMemoryField));
    if (!mf) return 1;
    arm_kvcache(c, &pack, &bvh, mf);

    /* ── 7. init HE-SLI to SKETCH level and physics sigma ── */
    for (uint32_t i = 0; i < c->n_nodes; i++) {
        if (strcmp(c->nodes[i].instance, "hesli0") == 0) {
            c->nodes[i].state.conductance  = (float)BF_HESLI_SKETCH;
            c->nodes[i].state.capacitance  = (float)BF_HESLI_OUT_DEFAULT;
            c->nodes[i].state.inductance   = 0.01f; /* meter_rate */
        }
        if (strcmp(c->nodes[i].instance, "phy0") == 0) {
            /* tolerance repurposed as KDE sigma: 0.5 = correct for unit sphere */
            c->nodes[i].state.tolerance    = 0.5f;
            /* inductance repurposed as dt */
            c->nodes[i].state.inductance   = 0.05f;
        }
    }

    /* ── 8. construct a seed input pulse from first pack vector ── */
    float seed_vec[EMBED_DIM];
    bf_embed_pack_vec_at(&pack, 0, seed_vec);
    const uint8_t *seed_hash = bf_embed_pack_hash_at(&pack, 0);

    BfInputPulse pulse = {0};
    pulse.data  = seed_vec;
    pulse.dim   = EMBED_DIM;
    pulse.event = 1;
    if (seed_hash) memcpy(pulse.hash, seed_hash, 32);

    /* ── 9. transient run ── */
    BfTranState *ts = bf_tran_state_alloc(c, 0.05f, N_STEPS);
    if (!ts) { fprintf(stderr, "violence: tran_state_alloc failed\n"); return 1; }

    printf("  %-6s  %-10s  %-8s  %-8s  %s\n",
           "step", "t", "entropy", "iters", "proof-hash");
    printf("  %-6s  %-10s  %-8s  %-8s  %s\n",
           "------", "----------", "--------", "------", "----------------");

    float total_entropy = 0.0f;
    int   converged_steps = 0;
    BfProbeFrame pf = {0};

    for (int step = 0; step < N_STEPS; step++) {
        /* only inject pulse on step 0; thereafter circuit self-sustains */
        const BfInputPulse *inp = (step == 0) ? &pulse : NULL;
        int rc = bf_spice_eval(c, ts, inp, &pf);

        /* extract entropy + hash from probe frame */
        float entropy = 0.0f;
        uint8_t proof[32] = {0};
        for (uint32_t p = 0; p < pf.n_probes; p++) {
            if (strstr(pf.names[p], "entropy")) entropy = pf.values[p];
            if (strstr(pf.names[p], "hash") && pf.is_hash[p])
                memcpy(proof, pf.hashes[p], 32);
        }
        total_entropy += entropy;
        if (ts->converged) converged_steps++;

        printf("  %6llu  t=%7.3f  H=%8.4f  it=%-2d  ",
               (unsigned long long)ts->step, ts->t, entropy, ts->n_iters);
        print_hash8(proof);
        if (rc == BF_SPICE_TOPO_GAP) printf(" [GAP]");
        if (!ts->converged)          printf(" [NOT_CONV]");
        printf("\n");
    }

    /* ── 10. final report ── */
    printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    printf("  FINAL REPORT\n");
    printf("  steps         : %d\n", N_STEPS);
    printf("  converged     : %d / %d\n", converged_steps, N_STEPS);
    printf("  total_entropy : %.6f\n", total_entropy);
    printf("  avg_entropy   : %.6f\n", total_entropy / N_STEPS);

    /* nearest neighbour from final physics state */
    for (uint32_t i = 0; i < c->n_nodes; i++) {
        if (strcmp(c->nodes[i].instance, "phy0") != 0) continue;
        BfPhysicsState *ps = (BfPhysicsState *)c->nodes[i].state.priv;
        if (!ps) break;
        printf("  physics.dim   : %u\n", ps->dim);
        printf("  physics.step  : %llu\n", (unsigned long long)ps->step);
        float H = bf_physics_hamiltonian(ps, &bvh, &pack);
        printf("  final H       : %.6f\n", H);

        /* nearest neighbour */
        BfEmbedSearchResult nn_res = {0}; int nn_count = 0;
        int nn_rc = bf_physics_nearest(ps, &bvh, &pack, 1, &nn_res, &nn_count);
        if (nn_rc > 0 && nn_count > 0) {
            printf("  nearest hash  : ");
            print_hash8(nn_res.hash);
            printf("\n  nearest score : %.6f\n", nn_res.score);
        }
        break;
    }
    printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n");

    /* ── 11. cleanup ── */
    /* kv0.priv points to mf — detach before circuit free so we control it */
    for (uint32_t i = 0; i < c->n_nodes; i++) {
        if (strcmp(c->nodes[i].instance, "kv0") == 0) {
            c->nodes[i].state.priv = NULL;  /* prevent double-free */
            break;
        }
    }
    bf_tran_state_free(ts);
    bf_circuit_free(c);
    bf_embed_bvh_close(&bvh);
    bf_embed_pack_close(&pack);
    free(mf);
    return 0;
}
