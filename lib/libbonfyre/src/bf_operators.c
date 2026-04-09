/*
 * bf_operators.c — typed operator registry for all Bonfyre binaries.
 *
 * This is the single source of truth for:
 *   - what each binary accepts and produces
 *   - behavioral class (pure/stateful/cacheable/reversible)
 *   - exactness class (byte-exact, canonically exact, lossy)
 *   - layer membership (substrate/transform/surface/value)
 *   - logical grouping (ingest/transform/validate/package/serve/index/bill)
 *
 * Generated docs, help text, completion scripts, and pipeline validation
 * all derive from this registry.
 */
#include "bonfyre.h"
#include <string.h>

const BfOperator BF_OPERATORS[] = {
    /* ================================================================
     * SUBSTRATE — cold, formal, stable infrastructure
     * ================================================================ */
    {
        .name = "ingest",
        .binary = "bonfyre-ingest",
        .description = "Universal intake — type detection, normalization, manifest stamping",
        .input_types = {"audio", "text", "image", "url", NULL},
        .output_types = {"intake-manifest", "normalized-file", NULL},
        .input_count = 4,
        .output_count = 2,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "substrate",
        .group = "ingest"
    },
    {
        .name = "hash",
        .binary = "bonfyre-hash",
        .description = "SHA-256 content addressing (FIPS 180-4)",
        .input_types = {"*", NULL},
        .output_types = {"hash-manifest", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "substrate",
        .group = "validate"
    },
    {
        .name = "index",
        .binary = "bonfyre-index",
        .description = "SQLite artifact index + full-text search",
        .input_types = {"artifact-manifest", NULL},
        .output_types = {"index-db", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "substrate",
        .group = "index"
    },
    {
        .name = "compress",
        .binary = "bonfyre-compress",
        .description = "File compression (zstd, async, family-aware)",
        .input_types = {"*", NULL},
        .output_types = {"compressed-file", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_REVERSIBLE,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "substrate",
        .group = "package"
    },
    {
        .name = "stitch",
        .binary = "bonfyre-stitch",
        .description = "DAG materializer — plan, prune, cache-stats",
        .input_types = {"artifact-manifest", NULL},
        .output_types = {"stitch-plan", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "substrate",
        .group = "index"
    },
    {
        .name = "graph",
        .binary = "bonfyre-graph",
        .description = "Merkle-DAG artifact graph, SHA-256 content addressing",
        .input_types = {"artifact-manifest", NULL},
        .output_types = {"graph-node", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "substrate",
        .group = "index"
    },
    {
        .name = "runtime",
        .binary = "bonfyre-runtime",
        .description = "Runtime environment, process lifecycle",
        .input_types = {NULL},
        .output_types = {NULL},
        .input_count = 0,
        .output_count = 0,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "substrate",
        .group = "serve"
    },
    {
        .name = "queue",
        .binary = "bonfyre-queue",
        .description = "Persistent job queue (SQLite-backed)",
        .input_types = {"job-request", NULL},
        .output_types = {"job-status", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "substrate",
        .group = "serve"
    },
    {
        .name = "sync",
        .binary = "bonfyre-sync",
        .description = "Cross-instance replication",
        .input_types = {"sync-manifest", NULL},
        .output_types = {"sync-status", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "substrate",
        .group = "serve"
    },

    /* ================================================================
     * TRANSFORM — pure, cacheable, stateless
     * ================================================================ */
    {
        .name = "media-prep",
        .binary = "bonfyre-media-prep",
        .description = "Audio normalization (16 kHz mono, denoise)",
        .input_types = {"audio", NULL},
        .output_types = {"normalized-audio", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_LOSSY,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "transcribe",
        .binary = "bonfyre-transcribe",
        .description = "Speech-to-text (Whisper)",
        .input_types = {"normalized-audio", NULL},
        .output_types = {"transcript", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_LOSSY,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "transcript-clean",
        .binary = "bonfyre-transcript-clean",
        .description = "Remove filler words, hallucinations",
        .input_types = {"transcript", NULL},
        .output_types = {"clean-transcript", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "paragraph",
        .binary = "bonfyre-paragraph",
        .description = "Structure text into paragraphs",
        .input_types = {"clean-transcript", NULL},
        .output_types = {"paragraphed-text", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "brief",
        .binary = "bonfyre-brief",
        .description = "Extract summary + action items",
        .input_types = {"paragraphed-text", "transcript", NULL},
        .output_types = {"brief", NULL},
        .input_count = 2,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_LOSSY,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "proof",
        .binary = "bonfyre-proof",
        .description = "Quality scoring + review",
        .input_types = {"brief", "transcript", NULL},
        .output_types = {"proof-score", NULL},
        .input_count = 2,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "validate"
    },
    {
        .name = "embed",
        .binary = "bonfyre-embed",
        .description = "Text embeddings (ONNX)",
        .input_types = {"text", "transcript", "brief", NULL},
        .output_types = {"embedding-vector", NULL},
        .input_count = 3,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "narrate",
        .binary = "bonfyre-narrate",
        .description = "Text-to-speech (Piper TTS)",
        .input_types = {"brief", "text", NULL},
        .output_types = {"narration-audio", NULL},
        .input_count = 2,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_LOSSY,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "render",
        .binary = "bonfyre-render",
        .description = "Template rendering",
        .input_types = {"artifact-manifest", NULL},
        .output_types = {"rendered-output", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "emit",
        .binary = "bonfyre-emit",
        .description = "Multi-format output (pandoc: HTML/PDF/EPUB/RSS)",
        .input_types = {"rendered-output", "text", NULL},
        .output_types = {"formatted-output", NULL},
        .input_count = 2,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "package"
    },
    {
        .name = "mfa-dict",
        .binary = "bonfyre-mfa-dict",
        .description = "MFA pronunciation dictionary",
        .input_types = {"text", NULL},
        .output_types = {"pronunciation-dict", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "weaviate-index",
        .binary = "bonfyre-weaviate-index",
        .description = "Vector index (Weaviate semantic search)",
        .input_types = {"embedding-vector", NULL},
        .output_types = {"vector-index", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "transform",
        .group = "index"
    },
    {
        .name = "transcript-family",
        .binary = "bonfyre-transcript-family",
        .description = "Full transcription chain (intake → transcribe)",
        .input_types = {"audio", NULL},
        .output_types = {"transcript-family", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_LOSSY,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "repurpose",
        .binary = "bonfyre-repurpose",
        .description = "Brief → social media formats (tweets, LinkedIn, carousel, YouTube, newsletter)",
        .input_types = {"brief", NULL},
        .output_types = {"social-content", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "segment",
        .binary = "bonfyre-segment",
        .description = "Idea boundary detection + segment graph (temporal structure extraction)",
        .input_types = {"whisper-json", NULL},
        .output_types = {"segment-graph", "boundaries", "rhythm", NULL},
        .input_count = 1,
        .output_count = 3,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "clips",
        .binary = "bonfyre-clips",
        .description = "Clip discovery engine (auto-detect best clip candidates from timestamps)",
        .input_types = {"whisper-json", NULL},
        .output_types = {"clip-candidates", "clip-timestamps", NULL},
        .input_count = 1,
        .output_count = 2,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "speechloop",
        .binary = "bonfyre-speechloop",
        .description = "Whisper → transform → Piper speech transformation loop",
        .input_types = {"audio", "brief", NULL},
        .output_types = {"audio", "brief", NULL},
        .input_count = 2,
        .output_count = 2,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_LOSSY,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "tone",
        .binary = "bonfyre-tone",
        .description = "Speech tone/emotion/rhythm extraction (OpenSMILE eGeMAPSv02)",
        .input_types = {"audio", NULL},
        .output_types = {"tone-features", "tone-profile", "tone-diff", NULL},
        .input_count = 1,
        .output_count = 3,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "canon",
        .binary = "bonfyre-canon",
        .description = "Structural canonicalization + hashing (Tree-sitter AST)",
        .input_types = {"json", "text", NULL},
        .output_types = {"ast", "structural-hash", "structural-diff", NULL},
        .input_count = 2,
        .output_count = 3,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "query",
        .binary = "bonfyre-query",
        .description = "Local artifact analytics engine (DuckDB SQL over artifacts)",
        .input_types = {"json", "duckdb", NULL},
        .output_types = {"query-results", "artifact-stats", NULL},
        .input_count = 2,
        .output_count = 2,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "vec",
        .binary = "bonfyre-vec",
        .description = "Local vector search — single-file retrieval (sqlite-vec)",
        .input_types = {"embeddings", "query-embedding", NULL},
        .output_types = {"search-results", NULL},
        .input_count = 2,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },
    {
        .name = "tag",
        .binary = "bonfyre-tag",
        .description = "Instant topic/intent tagging (fastText, 2ms, no GPU)",
        .input_types = {"text", NULL},
        .output_types = {"text-tags", "language-detection", NULL},
        .input_count = 1,
        .output_count = 2,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "transform",
        .group = "transform"
    },

    /* ================================================================
     * SURFACE — product-facing, stateful services
     * ================================================================ */
    {
        .name = "cms",
        .binary = "bonfyre-cms",
        .description = "Content management with Lambda Tensors compression",
        .input_types = {"http-request", NULL},
        .output_types = {"http-response", "cms-record", NULL},
        .input_count = 1,
        .output_count = 2,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "surface",
        .group = "serve"
    },
    {
        .name = "api",
        .binary = "bonfyre-api",
        .description = "HTTP gateway, file upload, job management, static server",
        .input_types = {"http-request", NULL},
        .output_types = {"http-response", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "surface",
        .group = "serve"
    },
    {
        .name = "auth",
        .binary = "bonfyre-auth",
        .description = "User signup/login, session tokens, SHA-256 passwords",
        .input_types = {"auth-request", NULL},
        .output_types = {"auth-response", "session-token", NULL},
        .input_count = 1,
        .output_count = 2,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "surface",
        .group = "serve"
    },
    {
        .name = "pipeline",
        .binary = "bonfyre-pipeline",
        .description = "Unified in-process pipeline (5-8 ms per stage)",
        .input_types = {"audio", "text", NULL},
        .output_types = {"pipeline-bundle", NULL},
        .input_count = 2,
        .output_count = 1,
        .flags = BF_OP_CACHEABLE | BF_OP_STREAMING,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "surface",
        .group = "serve"
    },
    {
        .name = "orchestrate",
        .binary = "bonfyre-orchestrate",
        .description = "Machine-only orchestration planner (Gemma 4 optional, zero end-user prompting)",
        .input_types = {"job-request", "artifact-manifest", "intake-manifest", "transcript", "brief", NULL},
        .output_types = {"orchestration-plan", "execution-graph", NULL},
        .input_count = 5,
        .output_count = 2,
        .flags = BF_OP_STATEFUL | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "surface",
        .group = "serve"
    },
    {
        .name = "cli",
        .binary = "bonfyre-cli",
        .description = "Unified command dispatcher",
        .input_types = {"*", NULL},
        .output_types = {"*", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = 0,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "surface",
        .group = "serve"
    },
    {
        .name = "project",
        .binary = "bonfyre-project",
        .description = "Project scaffolding",
        .input_types = {"project-config", NULL},
        .output_types = {"project-layout", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "surface",
        .group = "serve"
    },

    /* ================================================================
     * VALUE — monetization, metering, delivery
     * ================================================================ */
    {
        .name = "offer",
        .binary = "bonfyre-offer",
        .description = "Dynamic pricing + proposal generation",
        .input_types = {"proof-score", NULL},
        .output_types = {"offer", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "value",
        .group = "bill"
    },
    {
        .name = "gate",
        .binary = "bonfyre-gate",
        .description = "API key/tier validation (Free/Pro/Enterprise)",
        .input_types = {"gate-request", NULL},
        .output_types = {"gate-response", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "value",
        .group = "bill"
    },
    {
        .name = "meter",
        .binary = "bonfyre-meter",
        .description = "Usage tracking + per-operation billing",
        .input_types = {"usage-event", NULL},
        .output_types = {"meter-record", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "value",
        .group = "bill"
    },
    {
        .name = "ledger",
        .binary = "bonfyre-ledger",
        .description = "Append-only financial records",
        .input_types = {"ledger-entry", NULL},
        .output_types = {"ledger-record", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "value",
        .group = "bill"
    },
    {
        .name = "finance",
        .binary = "bonfyre-finance",
        .description = "Service arbitrage, bundle pricing",
        .input_types = {"offer", "meter-record", NULL},
        .output_types = {"finance-report", NULL},
        .input_count = 2,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "value",
        .group = "bill"
    },
    {
        .name = "outreach",
        .binary = "bonfyre-outreach",
        .description = "Outreach tracking, follow-up routing",
        .input_types = {"outreach-event", NULL},
        .output_types = {"outreach-record", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "value",
        .group = "bill"
    },
    {
        .name = "pay",
        .binary = "bonfyre-pay",
        .description = "Invoicing, payments, credits",
        .input_types = {"invoice-request", NULL},
        .output_types = {"payment-record", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "value",
        .group = "bill"
    },
    {
        .name = "pack",
        .binary = "bonfyre-pack",
        .description = "Deliverable packaging (ZIP + manifest)",
        .input_types = {"*", NULL},
        .output_types = {"delivery-bundle", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_PURE | BF_OP_CACHEABLE | BF_OP_IDEMPOTENT,
        .exactness = BF_EXACT_BYTE,
        .version = "1.0.0",
        .layer = "value",
        .group = "package"
    },
    {
        .name = "distribute",
        .binary = "bonfyre-distribute",
        .description = "Distribution + messaging (email, Slack, webhooks)",
        .input_types = {"delivery-bundle", NULL},
        .output_types = {"delivery-receipt", NULL},
        .input_count = 1,
        .output_count = 1,
        .flags = BF_OP_STATEFUL,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "value",
        .group = "package"
    },
    {
        .name = "tel",
        .binary = "bonfyre-tel",
        .description = "Telephony adapter (FreeSWITCH ESL — voice, SMS, MMS)",
        .input_types = {"audio-raw", "text-plain", NULL},
        .output_types = {"transcript", "delivery-receipt", NULL},
        .input_count = 2,
        .output_count = 2,
        .flags = BF_OP_STATEFUL | BF_OP_STREAMING,
        .exactness = BF_EXACT_CANON,
        .version = "1.0.0",
        .layer = "surface",
        .group = "ingest"
    },
};

const int BF_OPERATOR_COUNT = sizeof(BF_OPERATORS) / sizeof(BF_OPERATORS[0]);

/* ── P4: FNV-1a hash table for O(1) operator lookups ─────────────── */

#define OP_HT_SIZE 128  /* power-of-2, fits 39 operators at ~30% load */

typedef struct {
    int idx[OP_HT_SIZE]; /* index into BF_OPERATORS, -1 = empty */
} OpHashTable;

static OpHashTable g_ht_name;      /* keyed by .name */
static OpHashTable g_ht_binary;    /* keyed by .binary */
static int g_ht_ready = 0;

static unsigned fnv1a(const char *s) {
    unsigned h = 2166136261u;
    for (; *s; s++) h = (h ^ (unsigned char)*s) * 16777619u;
    return h;
}

static void ht_init(OpHashTable *ht) {
    for (int i = 0; i < OP_HT_SIZE; i++) ht->idx[i] = -1;
}

static void ht_insert(OpHashTable *ht, const char *key, int val) {
    unsigned slot = fnv1a(key) & (OP_HT_SIZE - 1);
    while (ht->idx[slot] >= 0)
        slot = (slot + 1) & (OP_HT_SIZE - 1);
    ht->idx[slot] = val;
}

static int ht_find(const OpHashTable *ht, const char *key,
                   const char *(*get_key)(int)) {
    unsigned slot = fnv1a(key) & (OP_HT_SIZE - 1);
    for (int i = 0; i < OP_HT_SIZE; i++) {
        int idx = ht->idx[slot];
        if (idx < 0) return -1;
        if (strcmp(get_key(idx), key) == 0) return idx;
        slot = (slot + 1) & (OP_HT_SIZE - 1);
    }
    return -1;
}

static const char *get_name(int i)   { return BF_OPERATORS[i].name; }
static const char *get_binary(int i) { return BF_OPERATORS[i].binary; }

static void ensure_ht(void) {
    if (g_ht_ready) return;
    ht_init(&g_ht_name);
    ht_init(&g_ht_binary);
    for (int i = 0; i < BF_OPERATOR_COUNT; i++) {
        ht_insert(&g_ht_name, BF_OPERATORS[i].name, i);
        ht_insert(&g_ht_binary, BF_OPERATORS[i].binary, i);
    }
    g_ht_ready = 1;
}

const BfOperator *bf_operator_find(const char *binary_name) {
    ensure_ht();
    int i = ht_find(&g_ht_binary, binary_name, get_binary);
    return i >= 0 ? &BF_OPERATORS[i] : NULL;
}

const BfOperator *bf_operator_find_by_name(const char *name) {
    ensure_ht();
    int i = ht_find(&g_ht_name, name, get_name);
    return i >= 0 ? &BF_OPERATORS[i] : NULL;
}
