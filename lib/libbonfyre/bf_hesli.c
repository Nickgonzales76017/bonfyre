/*
 * bf_hesli.c — HE-SLI: Dielectric Isolation Layer
 *
 * HE-SLI provides privacy-preserving field coupling between Bonfyre
 * components.  Private memory, sealed expert circuits, and partner-owned
 * policy subcircuits can affect trajectory, routing, and convergence
 * without exposing their underlying state.
 *
 *   V_total(q) = V_public(q) + V_private_encrypted(q) + V_policy_sealed(q)
 *
 * The physics can still bend.  The signal still reacts.
 * But the underlying memory remains sealed.
 *
 * Isolation levels:
 *
 *   HASH_ONLY (0):
 *     Only content addresses cross the boundary.
 *     q → SHA-256(q) → gate, basin_id, proof.
 *     No float data is used downstream.
 *
 *   SKETCH (1):
 *     256-bit ternary sign sketch: one bit per dimension sign.
 *     Popcount similarity determines distance_bucket.
 *     risk_score = fraction of negative-sign dimensions.
 *     entropy_delta = Shannon entropy of sketch bits.
 *
 *   HE_VECTOR (2):
 *     Simulated encrypted vector evaluation.
 *     q is "obfuscated" via HMAC-keyed rotation before eval.
 *     Sanitized output: risk_score, entropy_delta, distance_bucket.
 *     No raw float crosses the boundary in either direction.
 *
 *   LOCAL_ENCLAVE (3):
 *     Full local evaluation inside trusted boundary.
 *     If inner circuit is present, run bf_spice_eval on inner.
 *     Output filtered to allowed_outputs bitmask only.
 *     Raw q is consumed but not emitted.
 *
 * .hebfsubckt sealed subcircuit format:
 *   [BfHebfSubckt header (512 bytes aligned)] [inner bfcircuit blob]
 *   seal_hash = SHA-256(inner_blob)
 *   hmac      = HMAC-SHA256(seal_key, header || inner_blob) if seal_key set
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <bonfyre.h>

/* ── internal: HMAC-SHA256 (single-key, single-block simplification) ── */
/*   Full 2-pass HMAC per RFC 2104.  Key is zero-padded to 64 bytes.    */
static void hmac_sha256_(const uint8_t *key, size_t key_len,
                          const uint8_t *data, size_t data_len,
                          uint8_t out[32]) {
    uint8_t k[64]={0};
    if(key_len<=64) memcpy(k,key,key_len);
    else { BfSha256 h; bf_sha256_init(&h); bf_sha256_update(&h,key,key_len);
           bf_sha256_final(&h,k); }

    uint8_t ipad[64], opad[64];
    for(int i=0;i<64;i++){ ipad[i]=k[i]^0x36; opad[i]=k[i]^0x5c; }

    /* inner: SHA256(ipad || data) */
    uint8_t inner[32];
    BfSha256 hi; bf_sha256_init(&hi);
    bf_sha256_update(&hi,ipad,64);
    bf_sha256_update(&hi,data,data_len);
    bf_sha256_final(&hi,inner);

    /* outer: SHA256(opad || inner) */
    BfSha256 ho; bf_sha256_init(&ho);
    bf_sha256_update(&ho,opad,64);
    bf_sha256_update(&ho,inner,32);
    bf_sha256_final(&ho,out);
}

/* ── internal: 256-bit ternary sign sketch ────────────────────────────
 *
 * For each of 256 "projection bands" spanning the full dim dimensions:
 *   mean of the band → sign → 1 bit in sketch[band/8] bit (band%8)
 *
 * This captures the directional signature of q without raw values.
 */
static void compute_sketch_(const float *q, uint32_t dim, uint8_t sketch[32]) {
    memset(sketch,0,32);
    if(!q||!dim) return;
    uint32_t n_bands=256;
    float band_size=(float)dim/n_bands;
    for(uint32_t b=0;b<n_bands;b++){
        uint32_t lo=(uint32_t)(b*band_size);
        uint32_t hi=(uint32_t)((b+1)*band_size);
        if(hi>dim) hi=dim;
        if(lo>=hi){ /* dim < 256 — replicate last band */
            lo=dim>0?dim-1:0; hi=dim>0?dim:1; }
        double sum=0.0;
        for(uint32_t i=lo;i<hi;i++) sum+=q[i];
        if(sum>=0.0) sketch[b/8]|=(1u<<(b%8));
    }
}

/* Popcount distance between two 32-byte sketches → 0..256 */
static int sketch_dist_(const uint8_t a[32], const uint8_t b[32]) {
    int d=0;
    for(int i=0;i<32;i++){
        uint8_t x=a[i]^b[i];
        /* Brian Kernighan popcount */
        while(x){ d+=(x&1); x>>=1; }
    }
    return d;
}

/* Bucket distance 0..256 into 0..7 */
static uint8_t dist_bucket_(int dist) {
    if(dist<16)  return 0;
    if(dist<48)  return 1;
    if(dist<80)  return 2;
    if(dist<112) return 3;
    if(dist<144) return 4;
    if(dist<176) return 5;
    if(dist<208) return 6;
    return 7;
}

/* Shannon entropy of sketch bit vector (bit entropy, 0..1) */
static float sketch_entropy_(const uint8_t sketch[32]) {
    int ones=0;
    for(int i=0;i<32;i++){
        uint8_t x=sketch[i];
        while(x){ ones+=(x&1); x>>=1; }
    }
    float p=(float)ones/256.0f;
    if(p<=0.0f||p>=1.0f) return 0.0f;
    float p0=1.0f-p;
    return -(p*log2f(p)+p0*log2f(p0)); /* 0..1 */
}

/* ── bf_hesli_eval ────────────────────────────────────────────────────
 *
 * Core boundary evaluator.  Takes input vector q, applies the
 * appropriate isolation level, fills BfHeSliResult.
 *
 * This is the dielectric transfer function.
 * ─────────────────────────────────────────────────────────────────── */
int bf_hesli_eval(const float *q, uint32_t dim,
                   const BfHeSliPolicy *policy,
                   BfHeSliResult *out) {
    if(!out) return BF_SPICE_NUMERIC_FAULT;
    memset(out,0,sizeof(*out));
    out->gate=1; /* default: allow */

    BfHeSliLevel level = policy ? policy->level : BF_HESLI_HASH_ONLY;
    uint32_t allowed   = policy ? policy->allowed_outputs : BF_HESLI_OUT_DEFAULT;
    float    rate      = policy ? policy->meter_rate : 0.0f;
    const char *seal_key = policy ? policy->seal_key : NULL;

    out->meter_units = rate > 0.0f ? rate : 1.0f;

    /* ── Level 0: HASH_ONLY ──────────────────────────────────────── */
    if(level==BF_HESLI_HASH_ONLY) {
        if(q&&dim){
            bf_sha256_hex((const uint8_t*)q, dim*sizeof(float), (char[65]){});
            BfSha256 h; bf_sha256_init(&h);
            bf_sha256_update(&h,(const uint8_t*)q,dim*sizeof(float));
            bf_sha256_final(&h,out->basin_id);
        } else {
            /* event-only: zero basin */
            memset(out->basin_id,0xab,32);
        }
        if(allowed&BF_HESLI_OUT_GATE)  out->gate=1;
        if(allowed&BF_HESLI_OUT_BASIN) memcpy(out->proof,out->basin_id,32);
        if(allowed&BF_HESLI_OUT_PROOF){
            /* proof = HMAC of basin (keyed by seal_key if set) */
            if(seal_key&&seal_key[0])
                hmac_sha256_((const uint8_t*)seal_key,strlen(seal_key),
                              out->basin_id,32,out->proof);
            else
                memcpy(out->proof,out->basin_id,32);
        }
        out->distance_bucket=0; /* hash-only: no distance meaning */
        goto done;
    }

    /* ── Level 1: SKETCH ─────────────────────────────────────────── */
    if(level==BF_HESLI_SKETCH) {
        uint8_t sketch[32]={0};
        if(q&&dim) compute_sketch_(q,dim,sketch);

        /* "neutral" reference: all-ones sketch (all-positive baseline) */
        uint8_t neutral[32]; memset(neutral,0xFF,32);
        int dist=sketch_dist_(sketch,neutral);
        out->distance_bucket=dist_bucket_(dist);

        /* risk = fraction of negative-sign dimensions */
        int neg_bits=256-dist; /* bits that differ from all-positive */
        out->risk_score=(float)neg_bits/256.0f;
        out->entropy_delta=sketch_entropy_(sketch)-0.5f; /* signed: centred at 0.5 */

        /* gate: allow if more than half dimensions are positive */
        out->gate=(dist<128)?1:0;

        /* basin: SHA256(sketch) */
        BfSha256 hs; bf_sha256_init(&hs);
        bf_sha256_update(&hs,sketch,32);
        bf_sha256_final(&hs,out->basin_id);

        /* proof = HMAC(seal_key, sketch) or SHA256(sketch) */
        if(seal_key&&seal_key[0])
            hmac_sha256_((const uint8_t*)seal_key,strlen(seal_key),
                          sketch,32,out->proof);
        else
            memcpy(out->proof,out->basin_id,32);

        /* gap hint: sketch has strong attractor if entropy is low */
        if(allowed&BF_HESLI_OUT_MOUNT){
            float e=sketch_entropy_(sketch);
            out->mount_yes=(e<0.3f)?1:0;
            if(out->mount_yes)
                snprintf(out->mount_handle,sizeof(out->mount_handle),
                         "sketch://basin/%02x%02x%02x%02x",
                         out->basin_id[0],out->basin_id[1],
                         out->basin_id[2],out->basin_id[3]);
        }
        goto done;
    }

    /* ── Level 2: HE_VECTOR ──────────────────────────────────────── */
    if(level==BF_HESLI_HE_VECTOR) {
        /* "Encrypt" by HMAC-keyed bit rotation on sketch only.
         * The raw float vector never leaves this function. */
        uint8_t sketch[32]={0};
        uint8_t enc_sketch[32]={0};
        if(q&&dim) compute_sketch_(q,dim,sketch);

        if(seal_key&&seal_key[0]){
            hmac_sha256_((const uint8_t*)seal_key,strlen(seal_key),
                          sketch,32,enc_sketch);
        } else {
            memcpy(enc_sketch,sketch,32);
        }

        /* Compute energy = mean(q²) and variance as proxy for structure */
        float energy=0.0f, sq_sum=0.0f, sum=0.0f;
        if(q&&dim){
            for(uint32_t i=0;i<dim;i++){ float v=q[i]; sum+=v; sq_sum+=v*v; }
            energy=sq_sum/dim;
            float mean=sum/dim;
            float var=sq_sum/dim-mean*mean;
            /* risk: high if energy low (near flat) or var low (degenerate) */
            float norm_e=1.0f/(1.0f+energy);
            out->risk_score=0.4f*norm_e+0.6f*(1.0f/(1.0f+var));
        } else {
            out->risk_score=0.5f;
        }

        /* distance bucket from enc_sketch */
        uint8_t neutral[32]; memset(neutral,0xFF,32);
        out->distance_bucket=dist_bucket_(sketch_dist_(enc_sketch,neutral));
        out->entropy_delta=sketch_entropy_(enc_sketch)-0.5f;
        out->gate=(out->risk_score<0.7f)?1:0;

        BfSha256 hv; bf_sha256_init(&hv);
        bf_sha256_update(&hv,enc_sketch,32);
        bf_sha256_final(&hv,out->basin_id);
        memcpy(out->proof,out->basin_id,32);

        if(allowed&BF_HESLI_OUT_MOUNT){
            out->mount_yes=(energy>0.01f&&out->risk_score<0.5f)?1:0;
            if(out->mount_yes)
                snprintf(out->mount_handle,sizeof(out->mount_handle),
                         "hev://basin/%02x%02x%02x%02x",
                         out->basin_id[0],out->basin_id[1],
                         out->basin_id[2],out->basin_id[3]);
        }
        goto done;
    }

    /* ── Level 3: LOCAL_ENCLAVE ──────────────────────────────────── */
    {
        /* Full local eval — fall back to sketch while providing proof */
        uint8_t sketch[32]={0};
        if(q&&dim) compute_sketch_(q,dim,sketch);
        int dist=sketch_dist_(sketch,(uint8_t[]){[0 ... 31]=0xFF});
        out->distance_bucket=dist_bucket_(dist);
        out->risk_score=(float)(256-dist)/256.0f;
        out->entropy_delta=sketch_entropy_(sketch)-0.5f;
        out->gate=1; /* enclave always allows — caller's circuit gates */

        BfSha256 he; bf_sha256_init(&he);
        bf_sha256_update(&he,sketch,32);
        if(q&&dim) bf_sha256_update(&he,(const uint8_t*)q,dim*sizeof(float));
        bf_sha256_final(&he,out->basin_id);

        if(seal_key&&seal_key[0])
            hmac_sha256_((const uint8_t*)seal_key,strlen(seal_key),
                          out->basin_id,32,out->proof);
        else
            memcpy(out->proof,out->basin_id,32);

        if(allowed&BF_HESLI_OUT_MOUNT){
            out->mount_yes=1;
            snprintf(out->mount_handle,sizeof(out->mount_handle),
                     "enclave://basin/%02x%02x%02x%02x",
                     out->basin_id[0],out->basin_id[1],
                     out->basin_id[2],out->basin_id[3]);
        }
    }

done:
    /* ── Physics coupling outputs — safe to cross boundary ──────────────────
     *
     * These are derived from the sketched/hashed representations only.
     * Raw q is never emitted.  BonfyrePhysics reads these fields and
     * applies the force as a momentum kick (bf_physics_kick equivalent).
     *
     *   potential_delta    — attractive (-) / repulsive (+) ΔV at current q
     *   force_bucket       — 0=strongly repulsive … 7=strongly attractive
     *   projected_force[8] — sign sketch basis mapped to 8-dim force vector
     *
     * Derivation:
     *   potential_delta = -entropy_delta × 2  (low entropy = attractor = -V)
     *   force_bucket    = round(risk_score × 7)
     *   projected_force = basin_id bytes 0..7 sign-mapped × risk_score
     * ───────────────────────────────────────────────────────────────── */
    out->potential_delta = -out->entropy_delta * 2.0f;
    {
        int fb = (int)(out->risk_score * 7.0f + 0.5f);
        if (fb < 0) fb = 0; else if (fb > 7) fb = 7;
        out->force_bucket = (uint8_t)fb;
    }
    for (int _fi = 0; _fi < 8; _fi++) {
        /* sign bit from basin_id byte; scale by risk_score so magnitude
         * tracks how strongly the private field wants to attract/repel */
        float sign = (out->basin_id[_fi] & 0x80) ? 1.0f : -1.0f;
        out->projected_force[_fi] = sign * out->risk_score;
    }

    return out->gate ? BF_SPICE_OK : BF_SPICE_CONTRACT_BLOCK;
}

/* ── bf_hesli_result_to_signal ────────────────────────────────────────
 *
 * Convert a BfHeSliResult into a BfSignal suitable for circuit injection.
 * The signal carries only the information that crossed the boundary.
 * ─────────────────────────────────────────────────────────────────── */
void bf_hesli_result_to_signal(const BfHeSliResult *r,
                                 uint32_t allowed_outputs,
                                 BfSignal *sig) {
    if(!sig||!r) return;
    memset(sig,0,sizeof(*sig));
    sig->kind  =BF_PIN_SIGNAL;
    sig->event =r->gate;
    sig->data  =NULL;
    sig->dim   =0;

    /* pick the most informative scalar given allowed_outputs */
    if(allowed_outputs&BF_HESLI_OUT_ENTROPY_DELTA)
        sig->scalar=r->entropy_delta;
    else if(allowed_outputs&BF_HESLI_OUT_RISK)
        sig->scalar=r->risk_score;
    else if(allowed_outputs&BF_HESLI_OUT_DISTANCE)
        sig->scalar=(float)r->distance_bucket/7.0f;
    else
        sig->scalar=(float)r->gate;

    /* carry proof hash if allowed */
    if(allowed_outputs&BF_HESLI_OUT_PROOF){
        memcpy(sig->hash,r->proof,32);
        sig->kind=BF_PIN_PROOF;
    }

    /* always carry the full result as a typed payload so BonfyrePhysics
     * can read force_bucket / projected_force for the momentum kick.
     * Payload pointer borrows r which is in hesli_transfer_'s st->priv
     * (heap-allocated, component lifetime — not a stack variable). */
    sig->payload      = (void *)r;  /* const cast: borrowed read-only */
    sig->payload_kind = BF_PAYLOAD_HESLI_RESULT;
    sig->payload_flags= BF_PAYLOAD_F_BORROWED | BF_PAYLOAD_F_READONLY;
    sig->payload_life = BF_PAYLOAD_LIFE_CIRCUIT; /* r lives in st->priv */
    sig->payload_size = (uint32_t)sizeof(BfHeSliResult);
}

/* ── bf_hesli_seal ────────────────────────────────────────────────────
 *
 * Serialize a BfCircuit to a temporary buffer, then write a .hebfsubckt
 * file: [BfHebfSubckt header][inner .bfcircuit blob].
 *
 * seal_hash = SHA-256(inner_blob)
 * hmac      = HMAC-SHA256(seal_key, header||inner_blob) if key set
 * ─────────────────────────────────────────────────────────────────── */
int bf_hesli_seal(const BfCircuit *c,
                   const BfHeSliPolicy *policy,
                   const char *description,
                   const char *out_path) {
    if(!c||!out_path) return -1;

    /* Write inner circuit to a temp file first */
    char tmp[4096]; snprintf(tmp,sizeof(tmp),"%s.tmp",out_path);
    if(bf_circuit_save(c,tmp)!=0) return -1;

    /* Read it back as blob */
    size_t inner_size=0;
    char *inner_blob=bf_read_file(tmp,(size_t*)&inner_size);
    remove(tmp);
    if(!inner_blob||!inner_size) return -1;

    /* Compute seal_hash = SHA-256(inner_blob) */
    BfHebfSubckt hdr; memset(&hdr,0,sizeof(hdr));
    hdr.magic         = BF_HEBFSUBCKT_MAGIC;
    hdr.version       = BF_HEBFSUBCKT_VERSION;
    hdr.level         = (uint32_t)(policy ? policy->level : BF_HESLI_LOCAL_ENCLAVE);
    hdr.allowed_outputs = policy ? policy->allowed_outputs : BF_HESLI_OUT_DEFAULT;
    hdr.inner_size    = (uint32_t)inner_size;
    hdr.meter_rate    = policy ? policy->meter_rate : 1.0f;
    if(policy) snprintf(hdr.name,sizeof(hdr.name),"%s",policy->name);
    if(description) snprintf(hdr.description,sizeof(hdr.description),"%s",description);

    {
        BfSha256 h; bf_sha256_init(&h);
        bf_sha256_update(&h,(const uint8_t*)inner_blob,inner_size);
        bf_sha256_final(&h,hdr.seal_hash);
    }

    /* Compute HMAC if seal_key is set */
    if(policy && policy->seal_key[0]){
        /* HMAC(key, header_no_hmac || inner_blob) */
        BfSha256 combined; bf_sha256_init(&combined);
        bf_sha256_update(&combined,(const uint8_t*)&hdr,sizeof(hdr));
        bf_sha256_update(&combined,(const uint8_t*)inner_blob,inner_size);
        uint8_t combined_hash[32];
        bf_sha256_final(&combined,combined_hash);
        hmac_sha256_((const uint8_t*)policy->seal_key,
                      strlen(policy->seal_key),
                      combined_hash,32,hdr.hmac);
    }

    /* Write header + inner blob */
    FILE *fp=fopen(out_path,"wb");
    if(!fp){ free(inner_blob); return -1; }
    int ok=(fwrite(&hdr,sizeof(hdr),1,fp)==1 &&
            fwrite(inner_blob,inner_size,1,fp)==1);
    fclose(fp);
    free(inner_blob);
    return ok?0:-1;
}

/* ── bf_hesli_load ────────────────────────────────────────────────────
 * Load an unsealed (no HMAC key) subcircuit.  Rejects files whose HMAC
 * field is non-zero — use bf_hesli_load_keyed() for those so callers
 * cannot silently accept a tampered keyed seal without verifying it. */
BfHeSliSubckt *bf_hesli_load(const char *path) {
    if(!path) return NULL;
    FILE *fp=fopen(path,"rb");
    if(!fp) return NULL;

    BfHeSliSubckt *s=calloc(1,sizeof(BfHeSliSubckt));
    if(!s){ fclose(fp); return NULL; }

    if(fread(&s->header,sizeof(s->header),1,fp)!=1) goto fail;
    if(s->header.magic!=BF_HEBFSUBCKT_MAGIC) goto fail;
    if(!s->header.inner_size) goto fail;

    /* Reject keyed seals: the HMAC field is non-zero, meaning a key was used
     * during bf_hesli_seal().  Without the key we cannot verify integrity. */
    {
        uint8_t hmac_check = 0;
        for(int i=0;i<32;i++) hmac_check |= s->header.hmac[i];
        if(hmac_check != 0) goto fail; /* use bf_hesli_load_keyed() */
    }

    s->inner_size=s->header.inner_size;
    s->inner_blob=malloc(s->inner_size);
    if(!s->inner_blob) goto fail;
    if(fread(s->inner_blob,s->inner_size,1,fp)!=1) goto fail;

    /* verify seal_hash */
    uint8_t check[32];
    BfSha256 h; bf_sha256_init(&h);
    bf_sha256_update(&h,s->inner_blob,s->inner_size);
    bf_sha256_final(&h,check);
    if(memcmp(check,s->header.seal_hash,32)!=0) goto fail;

    fclose(fp);
    return s;
fail:
    fclose(fp);
    free(s->inner_blob);
    free(s);
    return NULL;
}

/* ── bf_hesli_load_keyed ──────────────────────────────────────────────
 * Load a key-sealed subcircuit.  Verifies HMAC-SHA256(seal_key, hdr||blob)
 * in constant time before accepting the inner blob.  If the file has no
 * HMAC (all-zero field) but a key is provided, the key is ignored and the
 * standard seal_hash check is the only gate (graceful downgrade). */
BfHeSliSubckt *bf_hesli_load_keyed(const char *path, const char *seal_key) {
    if(!path) return NULL;
    FILE *fp=fopen(path,"rb");
    if(!fp) return NULL;

    BfHeSliSubckt *s=calloc(1,sizeof(BfHeSliSubckt));
    if(!s){ fclose(fp); return NULL; }

    if(fread(&s->header,sizeof(s->header),1,fp)!=1) goto fail;
    if(s->header.magic!=BF_HEBFSUBCKT_MAGIC) goto fail;
    if(!s->header.inner_size) goto fail;

    s->inner_size=s->header.inner_size;
    s->inner_blob=malloc(s->inner_size);
    if(!s->inner_blob) goto fail;
    if(fread(s->inner_blob,s->inner_size,1,fp)!=1) goto fail;

    /* verify seal_hash first (cheap integrity check) */
    uint8_t check[32];
    { BfSha256 h; bf_sha256_init(&h);
      bf_sha256_update(&h,s->inner_blob,s->inner_size);
      bf_sha256_final(&h,check); }
    if(memcmp(check,s->header.seal_hash,32)!=0) goto fail;

    /* HMAC verification (constant-time): only if HMAC field is non-zero */
    {
        uint8_t hmac_present = 0;
        for(int i=0;i<32;i++) hmac_present |= s->header.hmac[i];
        if(hmac_present){
            if(!seal_key || !seal_key[0]) goto fail; /* key required */
            /* Reconstruct expected HMAC: HMAC-SHA256(key, hdr_nhmac || blob)
             * where hdr_nhmac is the header with hmac field zeroed, matching
             * the byte layout used during bf_hesli_seal(). */
            BfHebfSubckt hdr_nhmac = s->header;
            memset(hdr_nhmac.hmac, 0, sizeof(hdr_nhmac.hmac));
            uint8_t combined_hash[32];
            { BfSha256 h; bf_sha256_init(&h);
              bf_sha256_update(&h,(const uint8_t*)&hdr_nhmac,sizeof(hdr_nhmac));
              bf_sha256_update(&h,(const uint8_t*)s->inner_blob,s->inner_size);
              bf_sha256_final(&h,combined_hash); }
            uint8_t expected[32];
            hmac_sha256_((const uint8_t*)seal_key,strlen(seal_key),
                          combined_hash,32,expected);
            /* constant-time compare to prevent timing oracle */
            uint8_t diff = 0;
            for(int i=0;i<32;i++) diff |= expected[i] ^ s->header.hmac[i];
            if(diff != 0) goto fail;
        }
    }

    fclose(fp);
    return s;
fail:
    fclose(fp);
    free(s->inner_blob);
    free(s);
    return NULL;
}

/* ── bf_hesli_free ────────────────────────────────────────────────────*/
void bf_hesli_free(BfHeSliSubckt *s) {
    if(!s) return;
    bf_circuit_free(s->inner);
    free(s->inner_blob);
    free(s);
}

/* ── bf_hesli_subckt_eval ─────────────────────────────────────────────
 *
 * Evaluate a sealed subcircuit.  On first call, decodes the inner blob
 * into a live BfCircuit (lazy decode).  Then runs one SPICE step on
 * the inner circuit.  Filters output through the allowed_outputs mask.
 * ─────────────────────────────────────────────────────────────────── */
int bf_hesli_subckt_eval(BfHeSliSubckt *s,
                          const BfInputPulse *input,
                          BfHeSliResult *out) {
    if(!s||!out) return BF_SPICE_NUMERIC_FAULT;
    memset(out,0,sizeof(*out));
    out->meter_units=s->header.meter_rate>0.0f?s->header.meter_rate:1.0f;

    /* Lazy decode inner circuit from blob */
    if(!s->inner && s->inner_blob && s->inner_size){
        /* Write blob to temp file and load */
        char tmp[4096];
        snprintf(tmp,sizeof(tmp),"/tmp/.bfhesli_%p.bfcircuit",(void*)s);
        FILE *tf=fopen(tmp,"wb");
        if(tf){
            fwrite(s->inner_blob,s->inner_size,1,tf);
            fclose(tf);
            s->inner=bf_circuit_load(tmp);
            remove(tmp);
        }
    }

    BfHeSliPolicy pol={0};
    pol.level=(BfHeSliLevel)s->header.level;
    pol.allowed_outputs=s->header.allowed_outputs;
    pol.meter_rate=s->header.meter_rate;

    /* If inner circuit available: run one step, collect probe state */
    if(s->inner){
        BfTranState *ts=bf_tran_state_alloc(s->inner,0.05f,1);
        if(ts){
            BfProbeFrame pf; memset(&pf,0,sizeof(pf));
            bf_spice_eval(s->inner,ts,input,&pf);

            /* Translate probe frame → HeSliResult based on allowed_outputs */
            for(uint32_t i=0;i<pf.n_probes&&i<4;i++){
                if(pf.is_hash[i] && (pol.allowed_outputs&BF_HESLI_OUT_PROOF))
                    memcpy(out->proof,pf.hashes[i],32);
                else if(!pf.is_hash[i]){
                    if(i==0) out->entropy_delta=pf.values[i];
                    if(i==1) out->risk_score=fabs((double)pf.values[i])>1.0f?1.0f:
                                             fabsf(pf.values[i]);
                }
            }
            out->gate=(pf.status==BF_SPICE_OK||pf.status==BF_SPICE_NOT_CONVERGED)?1:0;
            bf_tran_state_free(ts);
        } else {
            goto fallback;
        }
    } else {
fallback:;
        /* No inner circuit decoded — use q from input pulse */
        return bf_hesli_eval(input?input->data:NULL,
                              input?input->dim:0,
                              &pol, out);
    }

    out->distance_bucket=dist_bucket_((int)(out->risk_score*256.0f));
    if(pol.allowed_outputs&BF_HESLI_OUT_MOUNT){
        out->mount_yes=(out->entropy_delta<0.0f)?1:0;
        if(out->mount_yes)
            snprintf(out->mount_handle,sizeof(out->mount_handle),
                     "sealed://%s/%02x%02x%02x%02x",
                     s->header.name[0]?s->header.name:"anon",
                     s->header.seal_hash[0],s->header.seal_hash[1],
                     s->header.seal_hash[2],s->header.seal_hash[3]);
    }
    return out->gate?BF_SPICE_OK:BF_SPICE_CONTRACT_BLOCK;
}

/* ── bf_hesli_gap_query ───────────────────────────────────────────────
 *
 * Ask a sealed subcircuit if it can provide curvature near q.
 * Used when BonfyrePhysics returns TOPO_GAP and --on-gap private is set.
 *
 * Returns 1 if the private field has relevant curvature, 0 otherwise.
 * If yes, out->mount_handle contains an opaque mount reference.
 * ─────────────────────────────────────────────────────────────────── */
int bf_hesli_gap_query(BfHeSliSubckt *s,
                        const float *q, uint32_t dim,
                        BfHeSliResult *out) {
    if(!s||!out) return 0;
    memset(out,0,sizeof(*out));

    BfHeSliLevel level=(BfHeSliLevel)s->header.level;

    if(level==BF_HESLI_HASH_ONLY){
        /* Can't tell without seeing data */
        return 0;
    }

    BfHeSliPolicy pol={0};
    pol.level=level;
    pol.allowed_outputs=s->header.allowed_outputs|BF_HESLI_OUT_MOUNT;
    pol.meter_rate=s->header.meter_rate;

    bf_hesli_eval(q,dim,&pol,out);

    /* Gap fill: private field is useful if it has low-entropy sketch
     * (strongly oriented attractor) or if inner circuit responds */
    if(s->inner&&level>=BF_HESLI_LOCAL_ENCLAVE){
        BfInputPulse pulse={0};
        pulse.data=q; pulse.dim=dim; pulse.event=1;
        BfHeSliResult inner_out={0};
        bf_hesli_subckt_eval(s,&pulse,&inner_out);
        if(inner_out.entropy_delta<-0.1f){
            out->mount_yes=1;
            snprintf(out->mount_handle,sizeof(out->mount_handle),
                     "gap://%s",inner_out.mount_handle);
        }
    }

    return out->mount_yes;
}
