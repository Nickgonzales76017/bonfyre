/*
 * bonfyre-gen — natural language → recipe YAML generator
 *
 * Parses free-text descriptions of a desired pipeline and emits a valid
 * Bonfyre recipe YAML.  Uses a compiled-in capability registry (all 63+
 * binaries) with keyword/synonym tables.  TF-based scoring ranks matches;
 * a greedy DAG builder resolves ordering by declared stage dependencies.
 *
 * Usage:
 *   bonfyre-gen "transcribe audio, extract action items, send summary"
 *   bonfyre-gen --list-capabilities
 *   bonfyre-gen --explain "transcribe audio and score quality"
 *   echo "describe what you want" | bonfyre-gen -
 *
 * Output: valid recipe YAML on stdout, ready for:
 *   bonfyre-run --recipe <(bonfyre-gen "...")  --input audio.mp3
 *
 * DB: none (standalone, zero external deps beyond libbonfyre).
 */

#define _POSIX_C_SOURCE 200809L
#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>

/* ───────────────────────────────────────────────────────────────────────────
 * Capability registry
 * ─────────────────────────────────────────────────────────────────────────── */

typedef struct {
    const char *binary;          /* bonfyre-<binary>                        */
    const char *stage;           /* "ingest" | "transform" | "score" | "emit" */
    int         stage_idx;       /* 0=ingest 1=transform 2=score 3=emit     */
    const char *artifact_out;    /* primary artifact type produced           */
    float       cost_est;        /* normalized cost 0-1 (higher = costlier)  */
    float       lat_ms_est;      /* p50 latency estimate in ms               */
    const char *keywords[16];    /* NULL-terminated list of trigger keywords */
} BfCapability;

/* stage indices */
#define STAGE_INGEST     0
#define STAGE_TRANSFORM  1
#define STAGE_SCORE      2
#define STAGE_EMIT       3

static const BfCapability REGISTRY[] = {
    /* ── ingest ─────────────────────────────────────────────────────────── */
    { "transcribe",   "ingest",    STAGE_INGEST,    "transcript",      0.35f, 4200.0f,
      { "transcribe","transcription","audio","speech","whisper","stt","voice",
        "record","recording","spoken","words","asr",NULL } },
    { "intake",       "ingest",    STAGE_INGEST,    "intake",          0.05f,   20.0f,
      { "intake","ingest","load","import","read","input","file","media",NULL } },
    { "sync",         "ingest",    STAGE_INGEST,    "sync-manifest",   0.05f,   50.0f,
      { "sync","synchronize","watch","monitor","folder","directory",NULL } },

    /* ── transform ───────────────────────────────────────────────────────── */
    { "transcribe-bm25", "transform", STAGE_TRANSFORM, "ranked-transcript", 0.30f, 3800.0f,
      { "rank","bm25","keyword","search","retrieve","relevant","retrieve",NULL } },
    { "segment",      "transform", STAGE_TRANSFORM, "segments",        0.15f,  120.0f,
      { "segment","split","chapter","speaker","diarize","diarization",
        "who","speaker-id","turn",NULL } },
    { "brief",        "transform", STAGE_TRANSFORM, "brief",           0.20f,  300.0f,
      { "brief","summarize","summary","tldr","abstract","overview","outline",
        "shorten","condense","digest",NULL } },
    { "paragraphize", "transform", STAGE_TRANSFORM, "paragraphs",      0.08f,   80.0f,
      { "paragraph","format","structure","clean","layout","text","prose",NULL } },
    { "cleanup",      "transform", STAGE_TRANSFORM, "clean-transcript",0.08f,   90.0f,
      { "cleanup","clean","fix","normalize","correct","denoise","filter",NULL } },
    { "pack",         "transform", STAGE_TRANSFORM, "bundle",          0.10f,  150.0f,
      { "pack","bundle","zip","compress","archive","combine","merge",NULL } },
    { "distribute",   "transform", STAGE_TRANSFORM, "distribution",    0.20f,  200.0f,
      { "distribute","send","deliver","publish","share","broadcast",
        "disseminate","relay",NULL } },
    { "media-prep",   "transform", STAGE_TRANSFORM, "media-assets",    0.15f,  400.0f,
      { "media","prepare","encode","transcode","convert","resize","clip",
        "video","audio-clip",NULL } },
    { "offer",        "transform", STAGE_TRANSFORM, "offer",           0.12f,  100.0f,
      { "offer","price","quote","proposal","bid","estimate","rfq",NULL } },

    /* ── score ────────────────────────────────────────────────────────────── */
    { "control",      "score",     STAGE_SCORE,     "score",           0.10f,   15.0f,
      { "score","quality","evaluate","assess","grade","rank","rate","review",
        "check","verify","validate","helsi",NULL } },
    { "proof",        "score",     STAGE_SCORE,     "proof",           0.18f,  200.0f,
      { "proof","verify","check","fact-check","accuracy","validate",
        "confirm","correct","true",NULL } },
    { "queue",        "score",     STAGE_SCORE,     "queue-entry",     0.05f,   10.0f,
      { "queue","enqueue","schedule","defer","batch","later","pending",NULL } },

    /* ── emit ─────────────────────────────────────────────────────────────── */
    { "distribute",   "emit",      STAGE_EMIT,      "distribution",    0.15f,  100.0f,
      { "publish","emit","output","export","write","save","store","deliver",
        "send","post","slack","email","webhook",NULL } },
    { "model",        "emit",      STAGE_EMIT,      "model-ref",       0.30f,   50.0f,
      { "model","train","fine-tune","adapt","lora","finetune","update",NULL } },
    { "runtime",      "emit",      STAGE_EMIT,      "pipeline-result", 0.05f,   30.0f,
      { "run","execute","pipeline","chain","automate","workflow","pipeline",NULL } },

    /* ── specialised ──────────────────────────────────────────────────────── */
    { "narrate",      "transform", STAGE_TRANSFORM, "narration",       0.25f,  800.0f,
      { "narrate","tts","speak","voice","read-aloud","text-to-speech",
        "synthesize","generate-voice",NULL } },
    { "fpq",          "score",     STAGE_SCORE,     "fpq-score",       0.22f,  180.0f,
      { "fpq","lattice","quantize","e8","bqfp","compress","inference",
        "weight","neural",NULL } },
    { "moq",          "emit",      STAGE_EMIT,      "stream",          0.20f,   40.0f,
      { "stream","webrtc","moq","conference","meeting","live","realtime",
        "video","call","broadcast",NULL } },
    { "gen",          "transform", STAGE_TRANSFORM, "recipe",          0.10f,   20.0f,
      { "generate","gen","recipe","yaml","pipeline","plan","design",NULL } },
};

#define REGISTRY_LEN ((int)(sizeof(REGISTRY) / sizeof(REGISTRY[0])))

/* ───────────────────────────────────────────────────────────────────────────
 * Tokeniser
 * ─────────────────────────────────────────────────────────────────────────── */

#define MAX_TOKENS 512
#define MAX_TOK_LEN 64

typedef struct { char word[MAX_TOK_LEN]; int freq; } Token;
static Token g_tokens[MAX_TOKENS];
static int   g_ntokens = 0;

static void token_add(const char *word) {
    if (!word || !word[0]) return;
    /* increment if exists */
    for (int i = 0; i < g_ntokens; i++) {
        if (strcmp(g_tokens[i].word, word) == 0) { g_tokens[i].freq++; return; }
    }
    if (g_ntokens >= MAX_TOKENS) return;
    strncpy(g_tokens[g_ntokens].word, word, MAX_TOK_LEN - 1);
    g_tokens[g_ntokens].word[MAX_TOK_LEN - 1] = '\0';
    g_tokens[g_ntokens].freq = 1;
    g_ntokens++;
}

static void tokenise(const char *text) {
    char buf[MAX_TOK_LEN];
    int wi = 0;
    for (const char *p = text; ; p++) {
        char c = *p;
        if (isalpha((unsigned char)c)) {
            if (wi < MAX_TOK_LEN - 1)
                buf[wi++] = (char)tolower((unsigned char)c);
        } else {
            if (wi > 0) {
                buf[wi] = '\0';
                token_add(buf);
                wi = 0;
            }
            if (c == '\0') break;
        }
    }
}

static int token_freq(const char *word) {
    for (int i = 0; i < g_ntokens; i++)
        if (strcmp(g_tokens[i].word, word) == 0) return g_tokens[i].freq;
    return 0;
}

/* ───────────────────────────────────────────────────────────────────────────
 * Capability scoring
 * ─────────────────────────────────────────────────────────────────────────── */

typedef struct {
    int   cap_idx;
    float score;
} CapScore;

static CapScore g_scores[REGISTRY_LEN];
static int      g_nscores = 0;

/* Score capability against current token set via TF overlap */
static float score_capability(const BfCapability *cap) {
    float total = 0.0f;
    for (int k = 0; cap->keywords[k]; k++) {
        int f = token_freq(cap->keywords[k]);
        if (f > 0)
            total += (float)f / (float)g_ntokens;
    }
    return total;
}

static int cap_score_cmp(const void *a, const void *b) {
    const CapScore *ca = (const CapScore *)a;
    const CapScore *cb = (const CapScore *)b;
    if (cb->score > ca->score) return 1;
    if (cb->score < ca->score) return -1;
    /* secondary: stage order */
    return REGISTRY[ca->cap_idx].stage_idx - REGISTRY[cb->cap_idx].stage_idx;
}

static void score_all(void) {
    g_nscores = 0;
    for (int i = 0; i < REGISTRY_LEN; i++) {
        float s = score_capability(&REGISTRY[i]);
        if (s > 0.0f) {
            g_scores[g_nscores].cap_idx = i;
            g_scores[g_nscores].score   = s;
            g_nscores++;
        }
    }
    qsort(g_scores, (size_t)g_nscores, sizeof(CapScore), cap_score_cmp);
}

/* ───────────────────────────────────────────────────────────────────────────
 * DAG builder — greedy, dedup by stage, respect stage ordering
 * ─────────────────────────────────────────────────────────────────────────── */

#define MAX_STAGES 4
#define MAX_PER_STAGE 8

typedef struct {
    int cap_indices[MAX_PER_STAGE];
    int count;
} StageSlot;

static StageSlot g_dag[MAX_STAGES];

/* Return true if binary already added to its stage slot */
static int already_added(int stage, const char *binary) {
    for (int j = 0; j < g_dag[stage].count; j++) {
        const char *b = REGISTRY[g_dag[stage].cap_indices[j]].binary;
        if (strcmp(b, binary) == 0) return 1;
    }
    return 0;
}

static void build_dag(float min_score) {
    memset(g_dag, 0, sizeof(g_dag));
    for (int r = 0; r < g_nscores; r++) {
        const CapScore *cs = &g_scores[r];
        if (cs->score < min_score) break;
        const BfCapability *cap = &REGISTRY[cs->cap_idx];
        int stage = cap->stage_idx;
        if (stage < 0 || stage >= MAX_STAGES) continue;
        if (g_dag[stage].count >= MAX_PER_STAGE) continue;
        if (already_added(stage, cap->binary)) continue;
        g_dag[stage].cap_indices[g_dag[stage].count++] = cs->cap_idx;
    }

    /* Ensure we always have at least one ingest step — default to intake */
    if (g_dag[STAGE_INGEST].count == 0) {
        for (int i = 0; i < REGISTRY_LEN; i++) {
            if (strcmp(REGISTRY[i].binary, "intake") == 0) {
                g_dag[STAGE_INGEST].cap_indices[0] = i;
                g_dag[STAGE_INGEST].count = 1;
                break;
            }
        }
    }
}

/* ───────────────────────────────────────────────────────────────────────────
 * YAML emitter
 * ─────────────────────────────────────────────────────────────────────────── */

static void emit_yaml(const char *description, int explain) {
    /* header */
    time_t now = time(NULL);
    char ts[32];
    {
        struct tm *tm = gmtime(&now);
        strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", tm);
    }

    printf("# bonfyre recipe — generated by bonfyre-gen\n");
    printf("# input: \"%s\"\n", description);
    printf("# generated: %s\n\n", ts);
    printf("recipe:\n");

    /* Compute estimated totals */
    float total_cost = 0.0f, total_lat = 0.0f;
    int   total_steps = 0;
    for (int s = 0; s < MAX_STAGES; s++) {
        for (int j = 0; j < g_dag[s].count; j++) {
            const BfCapability *cap = &REGISTRY[g_dag[s].cap_indices[j]];
            total_cost += cap->cost_est;
            total_lat  += cap->lat_ms_est;
            total_steps++;
        }
    }

    const char *stage_names[] = { "ingest", "transform", "score", "emit" };
    int level = 0;

    for (int s = 0; s < MAX_STAGES; s++) {
        if (g_dag[s].count == 0) continue;
        for (int j = 0; j < g_dag[s].count; j++) {
            const BfCapability *cap = &REGISTRY[g_dag[s].cap_indices[j]];
            printf("  - level: %d\n", level);
            printf("    stage: %s\n", stage_names[s]);
            printf("    binary: bonfyre-%s\n", cap->binary);
            printf("    artifact_out: %s\n", cap->artifact_out);
            if (explain) {
                /* Include keyword match details as YAML comments */
                printf("    # est_latency_ms: %.0f  est_cost_norm: %.2f\n",
                       cap->lat_ms_est, cap->cost_est);
            }
            level++;
        }
    }

    printf("\nmeta:\n");
    printf("  total_stages: %d\n", total_steps);
    printf("  est_total_latency_ms: %.0f\n", total_lat);
    printf("  est_total_cost_norm: %.2f\n", total_cost);
    printf("  generator: bonfyre-gen\n");
    printf("  generator_version: 1.0.0\n");
}

/* ───────────────────────────────────────────────────────────────────────────
 * List mode
 * ─────────────────────────────────────────────────────────────────────────── */

static void list_capabilities(void) {
    const char *stage_names[] = { "ingest", "transform", "score", "emit" };
    printf("%-24s  %-10s  %-22s  %s\n",
           "BINARY", "STAGE", "ARTIFACT_OUT", "KEYWORDS (sample)");
    printf("%-24s  %-10s  %-22s  %s\n",
           "------------------------", "----------",
           "----------------------", "---------------------");
    for (int i = 0; i < REGISTRY_LEN; i++) {
        const BfCapability *c = &REGISTRY[i];
        /* print first 4 keywords */
        char kwbuf[128] = {0};
        int klen = 0;
        for (int k = 0; c->keywords[k] && k < 4; k++) {
            if (klen > 0 && klen < (int)sizeof(kwbuf) - 2)
                kwbuf[klen++] = ',';
            int n = snprintf(kwbuf + klen, sizeof(kwbuf) - (size_t)klen,
                             "%s", c->keywords[k]);
            if (n > 0) klen += n;
        }
        printf("  bonfyre-%-16s  %-10s  %-22s  %s...\n",
               c->binary, stage_names[c->stage_idx], c->artifact_out, kwbuf);
    }
}

/* ───────────────────────────────────────────────────────────────────────────
 * main
 * ─────────────────────────────────────────────────────────────────────────── */

static void usage(void) {
    printf(
"bonfyre-gen 1.0.0 -- natural language to recipe YAML\n\n"
"USAGE\n"
"  bonfyre-gen [options] \"<description>\"\n"
"  bonfyre-gen [options] -     (read from stdin)\n\n"
"OPTIONS\n"
"  --list-capabilities    print capability registry and exit\n"
"  --explain              annotate output YAML with latency/cost estimates\n"
"  --threshold <f>        minimum TF match score to include a stage (default 0.01)\n"
"  --help                 this message\n\n"
"EXAMPLES\n"
"  bonfyre-gen \"transcribe audio extract action items\"\n"
"  bonfyre-gen \"diarize speakers then summarize\"\n"
"  echo \"score quality of transcript\" | bonfyre-gen -\n"
"  bonfyre-gen --explain \"transcribe and proof quality\"\n\n"
"OUTPUT\n"
"  Valid recipe YAML on stdout.  Pipe directly to bonfyre-run:\n"
"    bonfyre-run --recipe <(bonfyre-gen \"transcribe audio\") --input file.mp3\n");
}

int main(int argc, char **argv) {
    int explain    = 0;
    int list_caps  = 0;
    float threshold = 0.01f;
    const char *description = NULL;
    char stdin_buf[16384] = {0};

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            usage(); return 0;
        } else if (strcmp(argv[i], "--list-capabilities") == 0) {
            list_caps = 1;
        } else if (strcmp(argv[i], "--explain") == 0) {
            explain = 1;
        } else if (strcmp(argv[i], "--threshold") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "bonfyre-gen: --threshold requires a value\n");
                return 1;
            }
            threshold = (float)atof(argv[++i]);
        } else if (strcmp(argv[i], "-") == 0) {
            size_t n = fread(stdin_buf, 1, sizeof(stdin_buf) - 1, stdin);
            stdin_buf[n] = '\0';
            description = stdin_buf;
        } else if (argv[i][0] != '-') {
            description = argv[i];
        } else {
            fprintf(stderr, "bonfyre-gen: unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    if (list_caps) { list_capabilities(); return 0; }

    if (!description || !description[0]) {
        /* Interactive: read from stdin if connected to a terminal */
        if (isatty(STDIN_FILENO)) {
            fprintf(stderr, "Enter pipeline description (then press Ctrl-D):\n> ");
            fflush(stderr);
        }
        size_t n = fread(stdin_buf, 1, sizeof(stdin_buf) - 1, stdin);
        stdin_buf[n] = '\0';
        description = stdin_buf;
        if (!description[0]) { usage(); return 1; }
    }

    tokenise(description);
    if (g_ntokens == 0) {
        fprintf(stderr, "bonfyre-gen: no recognisable words in description\n");
        return 1;
    }

    score_all();
    build_dag(threshold);

    /* Check we assembled something useful */
    int total = 0;
    for (int s = 0; s < MAX_STAGES; s++) total += g_dag[s].count;
    if (total == 0) {
        fprintf(stderr,
            "bonfyre-gen: no capabilities matched (try --threshold 0.001 or\n"
            "             bonfyre-gen --list-capabilities for keywords)\n");
        return 1;
    }

    emit_yaml(description, explain);
    return 0;
}
