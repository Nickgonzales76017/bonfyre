#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# BONFYRE / AUREKAI CAPITAL GYM
#
# Long-running Codex CLI capitalization operator.
#
# This is intentionally NOT:
#   - a repo exploration loop
#   - a market-research loop
#   - an architecture-writing loop
#   - a grant-only loop
#   - a cloud-credit-only loop
#
# It starts with a large Bonfyre/Aurekai initial-condition packet, uses local
# Codex/Claude history only as targeted memory when a fact is missing, inherits
# the user's existing environment, and repeatedly asks Codex to convert the
# existing system into real external economic state until Codex usage/credits
# are exhausted or the account itself prevents further execution.
#
# IMPORTANT:
#   This script never buys Codex credits and never enables auto top-up.
#   If Codex auto top-up is enabled in your ChatGPT/Codex account settings,
#   disable it BEFORE running this if "stop when existing credits are gone"
#   is the intended budget.
#
# The current Bonfyre workspace is not auto-committed or auto-pushed.
###############################################################################

ROOT="${BONFYRE_ROOT:-/Users/nickgonzales/Documents/Bonfyre}"

# Mac-native state locations, matching Bonfyre's physical-estate direction.
STATE_ROOT="${BONFYRE_CAPITAL_STATE:-$HOME/Library/Application Support/Bonfyre/CapitalGym}"
CACHE_ROOT="${BONFYRE_CAPITAL_CACHE:-$HOME/Library/Caches/Bonfyre/CapitalGym}"
LOG_ROOT="${BONFYRE_CAPITAL_LOGS:-$HOME/Library/Logs/Bonfyre/CapitalGym}"

RUN_ROOT="$LOG_ROOT/runs"
PRIVATE_ROOT="$STATE_ROOT/private"
PUBLICATION_STAGE="$STATE_ROOT/publication-staging"
UPSTREAM_ROOT="$CACHE_ROOT/upstream"
BIN_ROOT="$STATE_ROOT/bin"

DB="$STATE_ROOT/capital.db"
INITIAL="$STATE_ROOT/INITIAL_CONDITIONS.md"
STATE_MD="$STATE_ROOT/STATE.md"
WINS_MD="$STATE_ROOT/WINS.md"
HUMAN_GATES_MD="$STATE_ROOT/HUMAN_GATES.md"
MEMORY_ROOTS="$STATE_ROOT/MEMORY_ROOTS.tsv"
ENV_NAMES="$STATE_ROOT/ENV_NAMES.tsv"
TOOLCHAIN="$STATE_ROOT/TOOLCHAIN.tsv"
SESSION_FILE="$STATE_ROOT/codex-session-id"
SUPERVISOR_LOG="$LOG_ROOT/supervisor.log"
PRE_RUN="$STATE_ROOT/pre-run"
TURN_PROMPT="$STATE_ROOT/NEXT_TURN.md"

MONOPOLY_URL="https://github.com/benjamin-awd/monopoly.git"

# Optional model override. Leave empty to use the user's Codex default.
CAPITAL_MODEL="${BONFYRE_CAPITAL_MODEL:-}"

# Testing switch. The normal mission has no round limit.
ONE_TURN="${BONFYRE_CAPITAL_ONE_TURN:-0}"

# Export path bindings referenced literally inside INITIAL_CONDITIONS.md.
# This exposes paths/controls only, not secret values.
export ROOT STATE_ROOT CACHE_ROOT LOG_ROOT RUN_ROOT PRIVATE_ROOT
export PUBLICATION_STAGE UPSTREAM_ROOT BIN_ROOT DB INITIAL STATE_MD
export WINS_MD HUMAN_GATES_MD MEMORY_ROOTS ENV_NAMES TOOLCHAIN SESSION_FILE
export SUPERVISOR_LOG PRE_RUN TURN_PROMPT MONOPOLY_URL CAPITAL_MODEL ONE_TURN

mkdir -p \
  "$STATE_ROOT" "$CACHE_ROOT" "$LOG_ROOT" "$RUN_ROOT" "$PRIVATE_ROOT" \
  "$PUBLICATION_STAGE" "$UPSTREAM_ROOT" "$BIN_ROOT" "$PRE_RUN"

chmod 700 "$STATE_ROOT" "$PRIVATE_ROOT" 2>/dev/null || true

touch "$SUPERVISOR_LOG" "$WINS_MD" "$HUMAN_GATES_MD" "$STATE_MD"

###############################################################################
# SINGLE-INSTANCE LOCK
###############################################################################

LOCKDIR="$STATE_ROOT/.operator.lock"
LOCKPID="$LOCKDIR/pid"
ACTIVE_CHILD=""

cleanup_lock() {
  if [[ -f "$LOCKPID" ]] && [[ "$(cat "$LOCKPID" 2>/dev/null || true)" == "$$" ]]; then
    rm -f "$LOCKPID" 2>/dev/null || true
    rmdir "$LOCKDIR" 2>/dev/null || true
  fi
}

kill_descendants() {
  local parent="$1"
  local sig="${2:-TERM}"
  local child
  while read -r child; do
    [[ -n "$child" ]] || continue
    kill_descendants "$child" "$sig"
  done < <(pgrep -P "$parent" 2>/dev/null || true)
  kill "-$sig" "$parent" 2>/dev/null || true
}

terminate_operator() {
  local code="$1"
  trap - INT TERM
  if [[ -n "${ACTIVE_CHILD:-}" ]]; then
    kill_descendants "$ACTIVE_CHILD" TERM
    sleep 1
    if kill -0 "$ACTIVE_CHILD" 2>/dev/null; then
      kill_descendants "$ACTIVE_CHILD" KILL
    fi
  fi
  cleanup_lock
  exit "$code"
}

# Recover only a lock whose recorded owner is no longer alive.
if [[ -d "$LOCKDIR" ]]; then
  prior_pid="$(cat "$LOCKPID" 2>/dev/null || true)"
  if [[ -n "$prior_pid" ]] && kill -0 "$prior_pid" 2>/dev/null; then
    echo "Another Bonfyre Capital Gym is already running (pid $prior_pid)." >&2
    exit 1
  fi
  rm -f "$LOCKPID" 2>/dev/null || true
  rmdir "$LOCKDIR" 2>/dev/null || true
fi

if ! mkdir "$LOCKDIR" 2>/dev/null; then
  echo "Could not acquire CapitalGym singleton lock: $LOCKDIR" >&2
  exit 1
fi

printf '%s\n' "$$" > "$LOCKPID"

trap cleanup_lock EXIT
trap 'terminate_operator 130' INT
trap 'terminate_operator 143' TERM

###############################################################################
# PREFLIGHT
###############################################################################

if [[ ! -d "$ROOT" ]]; then
  echo "ERROR: Bonfyre root not found: $ROOT" >&2
  exit 1
fi

if ! command -v codex >/dev/null 2>&1; then
  echo "ERROR: codex CLI is not installed." >&2
  echo "Install/update it with the official OpenAI Codex installation method." >&2
  exit 1
fi

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "ERROR: sqlite3 is required for the durable capital ledger." >&2
  exit 1
fi

###############################################################################
# CODEX AUTH
#
# Support current and older CLI spellings without depending on one version.
###############################################################################

codex_authenticated() {
  codex login status >/dev/null 2>&1 && return 0
  codex --login-status >/dev/null 2>&1 && return 0
  return 1
}

if ! codex_authenticated; then
  echo "Codex is not authenticated."
  echo "Starting normal Codex login..."
  if codex --help 2>&1 | grep -q -- '--login'; then
    codex --login || true
  else
    codex login || true
  fi

  if ! codex_authenticated; then
    echo "ERROR: Codex authentication did not complete." >&2
    exit 1
  fi
fi

###############################################################################
# SNAPSHOT THE EXISTING MIXED WORKSPACE WITHOUT COMMITTING IT
###############################################################################

if [[ -d "$ROOT/.git" ]]; then
  (
    cd "$ROOT"
    git rev-parse HEAD > "$PRE_RUN/head.txt" 2>/dev/null || true
    git status --porcelain=v1 > "$PRE_RUN/status.txt" 2>/dev/null || true
    git diff --binary > "$PRE_RUN/pre-run.diff" 2>/dev/null || true
    git ls-files --others --exclude-standard > "$PRE_RUN/untracked.txt" 2>/dev/null || true
  )
fi

###############################################################################
# DURABLE CAPITAL LEDGER
###############################################################################

sqlite3 "$DB" >/dev/null <<'SQL'
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS turns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  codex_session TEXT,
  exit_code INTEGER,
  input_tokens INTEGER,
  cached_input_tokens INTEGER,
  output_tokens INTEGER,
  note TEXT
);

CREATE TABLE IF NOT EXISTS assets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  name TEXT NOT NULL,
  class TEXT NOT NULL,
  status TEXT NOT NULL,
  source_refs TEXT,
  proof_refs TEXT,
  deployment_ref TEXT,
  offer_ref TEXT,
  notes TEXT,
  UNIQUE(name, class)
);

CREATE TABLE IF NOT EXISTS offers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  asset_id INTEGER,
  name TEXT NOT NULL,
  offer_class TEXT NOT NULL,
  asking_price REAL,
  currency TEXT DEFAULT 'USD',
  status TEXT NOT NULL,
  target_customer TEXT,
  external_ref TEXT,
  evidence_ref TEXT,
  notes TEXT,
  FOREIGN KEY(asset_id) REFERENCES assets(id)
);

CREATE TABLE IF NOT EXISTS capital_actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  action_class TEXT NOT NULL,
  target TEXT NOT NULL,
  status TEXT NOT NULL,
  face_value REAL DEFAULT 0,
  realized_value REAL DEFAULT 0,
  probability REAL,
  expected_value REAL,
  estimated_tokens INTEGER,
  external_ref TEXT,
  confirmation_id TEXT,
  evidence_ref TEXT,
  next_state TEXT,
  blocker TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS commercial_experiments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  offer_id INTEGER,
  hypothesis TEXT NOT NULL,
  variant TEXT,
  target TEXT,
  action_ref TEXT,
  result TEXT,
  response_signal TEXT,
  learned TEXT,
  FOREIGN KEY(offer_id) REFERENCES offers(id)
);

CREATE TABLE IF NOT EXISTS human_gates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  opportunity TEXT NOT NULL,
  gate_class TEXT NOT NULL,
  exact_gate TEXT NOT NULL,
  external_ref TEXT,
  completed_before_gate TEXT,
  value_at_stake REAL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'open',
  notes TEXT
);

CREATE TABLE IF NOT EXISTS wins (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  win_class TEXT NOT NULL,
  name TEXT NOT NULL,
  realized_value REAL DEFAULT 0,
  face_value REAL DEFAULT 0,
  external_ref TEXT,
  confirmation_id TEXT,
  evidence_ref TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS memory_lookups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  query TEXT NOT NULL,
  reason TEXT,
  result_ref TEXT
);

CREATE INDEX IF NOT EXISTS idx_actions_status ON capital_actions(status);
CREATE INDEX IF NOT EXISTS idx_actions_class ON capital_actions(action_class);
CREATE INDEX IF NOT EXISTS idx_offers_status ON offers(status);
CREATE INDEX IF NOT EXISTS idx_gates_status ON human_gates(status);
SQL

db_scalar() {
  sqlite3 -batch -cmd '.timeout 5000' "$DB" "$1"
}

db_exec() {
  sqlite3 -batch -cmd '.timeout 5000' "$DB" "$1" >/dev/null
}

###############################################################################
# TOOLCHAIN INVENTORY
#
# This is operational state, not exploration. It tells Codex what it can call.
###############################################################################

: > "$TOOLCHAIN"

for tool in \
  codex git gh curl wget jq sqlite3 rg fd find awk sed perl python3 node npm \
  bun zig cc clang cmake make docker podman cloudflared wrangler tor torsocks \
  playwright npx ffmpeg pdftotext ocrmypdf qpdf mutool
do
  if command -v "$tool" >/dev/null 2>&1; then
    p="$(command -v "$tool" 2>/dev/null || true)"
    v="$("$tool" --version 2>/dev/null | head -n 1 || true)"
    printf '%s\tFOUND\t%s\t%s\n' "$tool" "$p" "$v" >> "$TOOLCHAIN"
  else
    printf '%s\tMISSING\t\t\n' "$tool" >> "$TOOLCHAIN"
  fi
done

###############################################################################
# MEMORY ROOTS
#
# Claude/Codex history is an indexed memory shelf, NOT startup reading.
# Codex should search it only for an exact missing fact/decision.
###############################################################################

: > "$MEMORY_ROOTS"

add_memory_root() {
  local p="$1"
  local kind="$2"
  [[ -e "$p" ]] || return 0
  local size=""
  size="$(du -sh "$p" 2>/dev/null | awk '{print $1}' || true)"
  printf '%s\t%s\t%s\n' "$kind" "$p" "$size" >> "$MEMORY_ROOTS"
}

add_memory_root "$HOME/.codex/sessions" "codex_sessions"
add_memory_root "$HOME/.codex/history.jsonl" "codex_history"
add_memory_root "$HOME/.codex/attachments" "codex_attachments"
add_memory_root "$HOME/.codex" "codex_root"

add_memory_root "$HOME/.claude/projects" "claude_projects"
add_memory_root "$HOME/.claude/history.jsonl" "claude_history"
add_memory_root "$HOME/.claude" "claude_root"
add_memory_root "$HOME/Library/Application Support/Claude" "claude_app_support"

add_memory_root "$ROOT/isolated_session.zip" "project_history_archive"
add_memory_root "$ROOT/ai_chat_histories.zip" "project_history_archive"
add_memory_root "$ROOT/chats" "project_chat_archive"
add_memory_root "$ROOT/chat-history" "project_chat_archive"
add_memory_root "$ROOT/history" "project_history"

# Remove duplicate paths while preserving the first kind.
awk -F '\t' '!seen[$2]++' "$MEMORY_ROOTS" > "$MEMORY_ROOTS.tmp" || true
mv "$MEMORY_ROOTS.tmp" "$MEMORY_ROOTS"

###############################################################################
# SAFE TARGETED MEMORY SEARCH HELPER
#
# It never reads .env files and intentionally omits the three historical
# systems the owner excluded from this mission.
###############################################################################

cat > "$BIN_ROOT/capital-memory-search" <<'MEM_EOF'
#!/usr/bin/env bash
set -euo pipefail

STATE_ROOT="${BONFYRE_CAPITAL_STATE:-$HOME/Library/Application Support/Bonfyre/CapitalGym}"
ROOTS="$STATE_ROOT/MEMORY_ROOTS.tsv"

if [[ $# -lt 1 ]]; then
  echo "usage: capital-memory-search 'exact concept or decision'" >&2
  exit 2
fi

q="$*"

while IFS=$'\t' read -r kind path size; do
  [[ -e "$path" ]] || continue

  rg \
    -n -i -F \
    --max-count 60 \
    --hidden \
    --glob '!**/.env' \
    --glob '!**/.env.*' \
    --glob '!**/node_modules/**' \
    --glob '!**/.git/**' \
    --glob '!**/Library/Caches/**' \
    --glob '!**/Cookies*' \
    --glob '!**/Login Data*' \
    --glob '!**/Local State' \
    --glob '!**/keychain*' \
    --glob '!**/secrets/**' \
    --glob '!**/*.pem' \
    --glob '!**/*.key' \
    --glob '!**/*.p12' \
    --glob '!**/*.pfx' \
    "$q" "$path" 2>/dev/null || true
done < "$ROOTS" |
grep -Eiv '[REDACTED]|[REDACTED]|[REDACTED]|[REDACTED]' |
head -n 240
MEM_EOF

chmod +x "$BIN_ROOT/capital-memory-search"

###############################################################################
# ENVIRONMENT INVENTORY
#
# IMPORTANT: names only. Never values.
# The Codex child process already inherits the shell environment.
###############################################################################

: > "$ENV_NAMES"

record_env_names() {
  local f="$1"
  [[ -f "$f" ]] || return 0

  # Record key names only. No values enter the capital-gym logs.
  awk -v file="$f" '
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    {
      line=$0
      sub(/^[[:space:]]*export[[:space:]]+/, "", line)
      if (match(line, /^[A-Za-z_][A-Za-z0-9_]*=/)) {
        key=substr(line, 1, index(line, "=")-1)
        print file "\t" key
      }
    }
  ' "$f" >> "$ENV_NAMES" 2>/dev/null || true
}

record_env_names "$ROOT/.env"
record_env_names "$ROOT/.env.local"
record_env_names "$ROOT/.env.development"
record_env_names "$ROOT/.env.production"
record_env_names "$ROOT/Aurekai/.env"
record_env_names "$ROOT/Aurekai/.env.local"

find "$ROOT" -maxdepth 3 -type f -name '.env.*' -print0 2>/dev/null |
while IFS= read -r -d '' f; do
  record_env_names "$f"
done

sort -u "$ENV_NAMES" -o "$ENV_NAMES"

###############################################################################
# OPTIONAL GITHUB STATUS — NO CREDENTIAL DUMP
###############################################################################

{
  echo
  echo "=== $(date -Iseconds 2>/dev/null || date) ==="
  if command -v gh >/dev/null 2>&1; then
    if gh auth status >/dev/null 2>&1; then
      echo "gh_auth=available"
    else
      echo "gh_auth=not_available"
    fi
  else
    echo "gh=missing"
  fi
} >> "$SUPERVISOR_LOG"

###############################################################################
# MONOPOLY REFERENCE CHECKOUT
#
# This is a reference implementation under AGPL-3.0.
# We do NOT silently copy its Python implementation into Bonfyre.
###############################################################################

MONOPOLY_REF="$UPSTREAM_ROOT/monopoly"

if command -v git >/dev/null 2>&1; then
  if [[ ! -d "$MONOPOLY_REF/.git" ]]; then
    git clone --filter=blob:none --depth=1 "$MONOPOLY_URL" "$MONOPOLY_REF" \
      >>"$SUPERVISOR_LOG" 2>&1 || true
  else
    git -C "$MONOPOLY_REF" fetch --depth=1 origin main \
      >>"$SUPERVISOR_LOG" 2>&1 || true
    git -C "$MONOPOLY_REF" reset --hard origin/main \
      >>"$SUPERVISOR_LOG" 2>&1 || true
  fi
fi

###############################################################################
# INITIAL CONDITIONS
#
# Large on purpose. It should be injected ONCE into a durable Codex thread.
# Subsequent turns are small resumptions.
###############################################################################

cat > "$INITIAL" <<'EOF'
# BONFYRE / AUREKAI CAPITAL GYM — INITIAL CONDITIONS

## Mission

You are the capitalization process for an already-large technical system.

You are not being hired to discover what Bonfyre is.

You are not being hired to write another architecture plan.

You are not being hired to produce a market landscape.

You have a finite Codex allocation.

Convert as much of the existing Bonfyre/Aurekai estate as possible into REAL
EXTERNAL ECONOMIC STATE before that allocation is exhausted.

Operate continuously.

When one path is finished, blocked, submitted, rejected, deployed, or exhausted,
immediately move to another capital-producing path.

Research is a routing operation, not a deliverable.

The mission does not terminate because you ran out of ideas.

If the obvious opportunity book is exhausted, compose existing Bonfyre powers
into additional useful sellable things and test those.

---

# ZERO: DO NOT RE-LEARN THE SYSTEM

The following material is supplied as authoritative initial conditions.

Do not start with:

    tree
    find the architecture
    summarize the repo
    explain Bonfyre
    map the commands
    enumerate Frappe apps
    read all chat history
    inspect every environment
    propose a monetization strategy

Those are invalid opening moves.

The repository may be inspected surgically when needed to BUILD, VERIFY, DEPLOY,
SELL, BID, APPLY, or FIX something.

Disk chat history is MEMORY, not onboarding material.

Use:

    $BIN_ROOT/capital-memory-search "exact missing concept"

only when a specific earlier decision, implementation fact, benchmark, path,
name, or constraint is genuinely needed.

Do not summarize entire historical conversations.

---

# ONE: THREE HISTORICAL SYSTEMS ARE OUT OF SCOPE

Do not use, mine, commercialize, mention in sales material, or derive customer
claims from these three historical systems:

    [REDACTED]
    [REDACTED]
    [REDACTED]

They are not part of this mission's evidence base.

Do not search their old work as a source of demonstrations, customers, metrics,
data, case studies, or commercial credibility.

---

# TWO: BONFYRE'S PRODUCT THESIS

Bonfyre maintains continuity of resource identity, capability, state,
relationships, provenance, history, authority, placement and composition while
software changes representation, topology, runtime, provider, device,
collaborator and interface.

Apps do not own things.

The user's thing stays central and capabilities come to it.

The human interaction grammar is approximately:

    + Add
    Use with...
    Show as...
    Try...
    Send / Run on...
    drag one thing onto another

Stable polished micro-grammars are preferred over random generated UI.

Malleability is not low-code.

Models/agents are bounded executors, not the product metaphor.

---

# THREE: DESIGN LAWS

Preserve these unless current source explicitly proves a newer contract:

    identity continuity
    compositional continuity
    attachment closure
    reification closure
    representation plurality
    topology plurality
    execution plurality
    capability substitution
    projection plurality
    placement independence
    authority conservation
    external sovereignty
    no silent mutation
    variation is not actuality
    proportional history
    implementation factorization != semantic elimination
    promotion != authorization
    typed effects
    proof is exhaust, not the product
    apps do not own navigation
    user meaning is not prematurely classified

These laws apply to capitalization too.

A new commercial object should be able to reuse the substrate without forcing
Bonfyre into one vertical ontology.

---

# FOUR: UNIVERSAL FABRIC

Use the existing compact substrate:

    Identity
    State
    Relation
    Capability
    Actor
    Event
    Representation
    Authority
    Policy
    Time
    History
    Variant
    Provider
    Placement
    Value
    Pack

Do not promote WorkGraph, World, Institution, Agent, Graph, Artifact, Model,
App, or any one domain into a universal root merely because a commercial
experiment uses it.

---

# FIVE: ATOMIC SPECIES

Important reusable typed structures already developed include:

IDENTITY:
    Kind
    KindFamily
    ResourceRef
    ActorRef
    ArtifactRef
    ExternalRef
    ForeignTwin
    IdentityBinding
    lineage / ownership / membership edges
    canonical digest

HUMAN BOUNDARY:
    AtomicForm
    NativeFormField
    ReusableFieldGroup
    EvidenceSlot
    ArtifactSlot
    AutofillBinding
    ApprovalAuthorityEdge
    ActionSubscriberEdge
    ReviewGate
    SubmitReadyPackage
    LocalizedNativeForm
    NativeFormFork

CAPABILITY:
    CapabilityDemand
    CapabilityContract
    CapabilityClosure
    CapabilityGenome
    CapabilityIntroduction
    CapabilityLease
    CapabilityBid
    CapabilityBidSet
    CapabilitySidecar

PROVIDER / ROUTING:
    RecursiveResolutionFrame
    RecursionDepthEnvelope
    ProviderAggregationPinchPoint
    ExternalBoundaryOptimizer
    BonfyreNativeProviderBid
    FrappeDocTypeProviderBid
    CapabilitySidecarBid
    AurekaiProviderBid
    CommercialRouteBid
    CapacityLeaseBid
    HumanWorkcellBid
    SelectedCapabilityExecution

EFFECT / AUTHORITY:
    EffectClass
    TypedCommit
    ProvisionalCommit
    ExposureEnvelope
    ReviewInterrupt
    AuthorityGrant
    CapabilityGrant
    ScopedProviderCredential
    Delegation
    Revocation
    IdempotencyController

EVIDENCE:
    EvidencePacket
    Claim
    ClaimBundle
    SourceHandle
    Proof
    Receipt
    SignedReceipt
    ReceiptReplay
    TrustEnvelope
    VerificationResult
    FreshnessWindow
    Contradiction
    Invalidation

WORK:
    Mission
    Objective
    Obligation
    Task
    AgentTask
    WorkGraphNode
    WorkGraphEdge
    Lease
    Attempt
    Retry
    Timeout
    Cancellation
    HumanWait
    ExternalWait
    FanOut
    FanIn
    Compensation
    Checkpoint
    ResumePoint
    HandoffContract

VALUE:
    Offer
    Commitment
    Voucher
    VoucherProgram
    VoucherClaim
    Eligibility
    Reservation
    QuotaVoucher
    CapabilityFuture
    OutcomeContract
    Escrow
    Payment
    Settlement
    BudgetEnvelope
    CostAllocation
    MoneyRelationship
    CredentialPassport
    AssetPassport

DEVICE:
    DevicePassport
    CapabilityBeacon
    SensorBinding
    ActuatorBinding
    DeviceLease
    PlacementCandidate
    PersonalEdgeElection
    ResourceWeather
    OpticalEnvelope

CONTEXT / CONTINUITY:
    ContextABI
    ContextBundle
    WorkingSet
    ActiveSet
    ContinuityIsland
    PreparedContextPassport
    KVPassport
    MissionPacket
    HandoffPacket
    ContextBudget
    SourceSignature
    RecoveryConstitutionCapsule

ARTIFACT:
    ArtifactIR
    DeltaIR
    Fragment
    LayerArtifact
    ArtifactPacket
    CanonicalRepresentation
    SourceRepresentation
    DerivedRepresentation
    Manifest
    BFP
    YaFF object
    Pack
    AppCapsule
    RealityImage
    RuntimeImage
    PortableWorkspace

SURFACE:
    SurfaceIR
    SurfaceGrammar
    SurfaceTemplate
    SurfaceProjection
    ClosureSurfaceCard
    LocalizedSurface
    HumanLens
    ActionCard

LEARNING / PROMOTION:
    ActionTrace
    MemoryEvent
    PatternCluster
    LambdaTensorMotif
    SystemLearnerProposal
    PromotionCandidate
    CapabilityGenome
    SurfaceCandidate
    OperatorFusionCandidate
    NativeAbsorptionCandidate
    ProtocolCandidate
    InstitutionCandidate

Do not assume every commercial object requires all of these.

---

# SIX: COMPUTATIONAL FORMS

Bonfyre supports plural computational forms. Preserve their distinct semantics:

    table
    form
    document
    collection
    graph
    tree
    scene
    timeline
    node graph
    reactive graph
    dependency graph
    stream
    feed
    queue
    ledger
    market
    WorkGraph
    protocol
    room
    social network
    institution
    physics world
    simulation
    spatial field
    evidence graph
    version graph
    computational document
    render graph
    audio graph
    model graph
    world
    RuntimeImage

---

# SEVEN: NINE FRAPPE ESTATES ARE REAL CAPABILITY RESERVOIRS

Retain all nine:

    Frappe/Core
    ERPNext
    CRM
    HRMS
    LMS
    Helpdesk
    Insights
    Wiki
    Drive

They retain their mature domain semantics and UI grammars:

ERPNext:
    finance, buying, selling, inventory, manufacturing, projects, assets,
    tax, payments

CRM:
    leads, contacts, companies, deals, pipeline, communications

HRMS:
    employees, attendance, leave, shifts, payroll, expenses, performance

Helpdesk:
    tickets, SLAs, escalation, support, knowledge

LMS:
    courses, cohorts, quizzes, submissions, credentials

Insights:
    data sources, queries, semantic models, charts, dashboards

Wiki:
    hierarchy, revisions, backlinks, publishing

Drive:
    files, folders, sharing, versioning, sync

Frappe/Core:
    forms, lists, reports, dashboards, Kanban, calendars, trees, Gantt,
    workspaces, onboarding, shortcuts, cards, print, templates, permissions,
    workflow, notifications, localization, doc lifecycle

These can be used as real software surfaces, RL environments, demos,
commercial deployments, benchmarks and capability sources.

Do not flatten them into a generic task engine.

---

# EIGHT: COMMAND ESTATE

Preserve every public command identity present in the current repository.

Older architectural inventories referred to 91 native Bonfyre command names.
The current completion controller and recent runtime evidence have exercised a
93-row public command catalog.

DO NOT "fix" this discrepancy by deleting or renaming commands.

Treat the checked-in current declaration/catalog as authoritative.

Important existing command identities/families include:

IDENTITY / SEMANTIC / ACCESS:
    BonfyreAPI
    BonfyreAuth
    BonfyreCMS
    BonfyreCanon
    BonfyreCapability
    BonfyreEntity
    BonfyreFamily
    BonfyreGraph
    BonfyreHash
    BonfyreIndex
    BonfyreMFADict
    BonfyreQuery
    BonfyreTag
    BonfyreTier

ARTIFACT / LANGUAGE / PRESENTATION:
    BonfyreBrief
    BonfyreCompress
    BonfyreEmit
    BonfyreFragment
    BonfyreIngest
    BonfyreNarrate
    BonfyrePack
    BonfyreParagraph
    BonfyreStitch
    BonfyreSurface

EXECUTION / ORCHESTRATION:
    BonfyreCLI
    BonfyreControl
    BonfyreFlow
    BonfyreGate
    BonfyreOrchestrate
    BonfyrePipeline
    BonfyreProject
    BonfyreQueue
    BonfyreRecipe
    BonfyreRun
    BonfyreRuntime
    BonfyreWatch
    BonfyreWorkflow

MODEL / NUMERICAL / REASONING:
    BonfyreEmbed
    BonfyreFPQ
    BonfyreFPQx
    BonfyreFlashQLA
    BonfyreGen
    BonfyreInfer
    BonfyreKVCache
    BonfyreLayer
    BonfyreLeapfrog
    BonfyreModel
    BonfyrePhysics
    BonfyreQuant
    BonfyreQwenFPQ
    BonfyreReason
    BonfyreSAE
    BonfyreSLI
    BonfyreVec
    BonfyreViolence

MEDIA:
    BonfyreClips
    BonfyreDetectObjects
    BonfyreFrameExtract
    BonfyreMediaPrep
    BonfyreMoQ
    BonfyreRender
    BonfyreRepurpose
    BonfyreSceneDetect
    BonfyreSegment
    BonfyreSpeechLoop
    BonfyreTone
    BonfyreTranscribe
    BonfyreTranscriptClean
    BonfyreTranscriptFamily
    BonfyreVideoDemux

DISTRIBUTED / COMMUNICATION:
    BonfyreDistribute
    BonfyreNet
    BonfyreProxy
    BonfyreSpace
    BonfyreSwarm
    BonfyreSync
    BonfyreTel
    BonfyreWire

VALUE / MARKET:
    BonfyreCompete
    BonfyreEconomy
    BonfyreFinance
    BonfyreLedger
    BonfyreMeter
    BonfyreOffer
    BonfyreOutreach
    BonfyrePay

LEARNING / PROOF / TIME:
    BonfyreDiscipl
    BonfyreLearn
    BonfyreProof
    BonfyreTime
    BonfyreWeaviateIndex

The exact current catalog may include additional current names. Preserve them.

---

# NINE: CURRENT NATIVE FABRIC / EVIDENCE BASELINE

Recent Bonfyre completion work established a much more serious runtime than
"commands exist":

    deterministic compiled declarations
    command contract bindings
    contract-selected output discovery
    bounded process execution
    semantic workload probes
    quality probes
    event evidence
    receipt evidence
    generation freshness
    WorkGraph claim/lease/fence work
    retries
    failure/dead-letter semantics
    cancellation
    compensation/recovery work
    release/acceptance requirements
    Frappe compiler work
    real service proofs

Do not sell a command merely because it resolves.

Use current evidence and run the actual relevant workload before making claims.

Do not claim "93/93 quality-proven" unless current same-state evidence actually
says that.

---

# TEN: SUBSTRATE ESTATE

The Recursive Hypervisor established fourteen active substrate backends:

    HVM4
    Feldera
    egglog
    Hydro
    CubeCL
    Automerge
    Tract
    YaFF
    Lance
    Verus
    Daft
    Gigatoken
    Burn
    Restate

Important intended/working roles include:

    Automerge    distributed/local-first application state
    Restate      durable execution / continuation
    Feldera      incremental derived state / readiness
    egglog       equality saturation / rewrite
    HVM2/HVM4    reductions
    YaFF         compact native representation / artifact protocol
    Lance/Daft   data/vector/columnar operations
    Tract/Burn   model/tensor execution routes
    Hydro        distributed placement/flow experiments
    CubeCL       compute kernels
    Verus        formal/proof routes
    Gigatoken    tokenization/data tooling

Do not force all workloads through all backends.

---

# ELEVEN: MODEL / COMPUTE ESTATE

The model estate is a first-class capability ecology:

    BonfyreModel
    BonfyreInfer
    BonfyreGen
    BonfyreQuant
    BonfyreFPQ
    BonfyreFPQx
    BonfyreQwenFPQ
    BonfyreSLI
    BonfyreFlashQLA
    BonfyreKVCache
    BonfyreSAE
    BonfyreEmbed
    BonfyreVec
    BonfyreLayer
    BonfyreReason
    BonfyrePhysics
    BonfyreLeapfrog
    BonfyreNet

Also:

    native Colibri / Bonfyre MoE
    llama.cpp / GGUF MoE
    model packs
    tokenizers
    prepared state
    prefix cache
    batching
    streaming
    resident scheduling
    hydration
    quality gates
    promotion
    QUIC generation

FPQ is a serious commercializable asset.

The project has had real model conversion/runtime/fidelity work and direct
inference through the SLI bridge.

Only use benchmark numbers that are present in current verifiable evidence.

Potential commercial objects include:

    FPQ compatibility scan
    FPQ conversion
    memory-reduction assessment
    local inference packaging
    model deployment
    quantization/fidelity report
    prepared-context runtime
    model transport
    local/private model environment
    model-runtime benchmark

Do not give away the entire model/runtime estate merely to close a tiny deal.

---

# TWELVE: BONFYREFS / OS POWER

BonfyreFS is real and stays named.

It is a filesystem/POSIX materialization power over arbitrary Bonfyre
resources/scopes.

Historically implemented/proven directions include:

    Finder-visible mount
    mission-scoped mount
    query directories
    writable effect files
    relationship links
    capability files
    temporal directories
    foreign mounts
    regenerative files
    candidate directories

Its significance is interoperability with:

    Finder
    shell
    grep
    awk
    Perl
    Python
    ffmpeg
    Blender
    git
    native software

A commercial object can exploit BonfyreFS, but BonfyreFS is not itself required
to become a standalone product.

---

# THIRTEEN: ARTIFACT / REPRESENTATION ESTATE

Bonfyre has deep artifact work:

    YaFF
    BFP
    ArtifactIR
    DeltaIR
    Fragment
    Stitch
    Pack
    Canon
    Hash
    Emit
    Ingest

Representation families developed/tested across prior work include:

    PDF
    DOCX
    XLSX
    PPTX
    EPUB
    HTML
    MHTML
    WACZ
    HAR
    PNG/APNG
    WebP
    WAV
    MP4
    WebM
    subtitles
    SVG
    GeoJSON
    SQLite/Turso
    Parquet/Arrow/Avro/ORC
    fonts
    glTF/GLB
    OBJ/STL/PLY/USD/3MF
    serialization families
    WASM

Important frontier:

    application-bearing artifacts
    executable evidence objects
    one-file scopes
    portable Bonfyre capsules
    RealityImage / RuntimeImage
    procedural artifact reconstruction
    StitchWire

---

# FOURTEEN: MEDIA / CREATIVE ESTATE

Existing powers include:

    SceneIR
    VEX IR
    Blender
    Hyperframes
    FFmpeg
    VideoDemux
    FrameExtract
    SceneDetect
    DetectObjects
    Transcribe
    TranscriptClean
    TranscriptFamily
    Segment
    Clips
    Repurpose
    Distribute
    Render
    MoQ
    audio/video/stream/scene/timeline/camera/material/asset

Scene Lab is a dense creative/simulation composition and can be used as a
technical demonstration or environment source without inventing a new media
startup.

---

# FIFTEEN: SERM / SCIENTIFIC / MULTIMODAL ESTATE

Existing research/evidence powers include:

    question/contract
    minimal sufficient evidence
    YaFF evidence blocks
    Turso memoization
    embedding proposals
    HVM reductions
    residuals
    materialization
    PDF/XLSX/database/audio/video/timestamped-transcript/image/web evidence
    Ripper
    Magik
    FFmpeg
    LitParse-style extraction
    proof / evidence / source relationships

Representation-specific semantics must survive reduction.

This can be commercialized as an evidence/computation capability, benchmark or
environment without turning all Bonfyre into "document AI."

---

# SIXTEEN: DISTRIBUTED / CONNECTION / DEVICE ESTATE

Explicit powers include:

    ConnectionPack
    CapabilityIntroduction
    ForeignTwin
    APIUseTwin
    Intent ABI
    CrossBoundaryTransaction
    MCP meta-ABI
    OAuth/OIDC
    APIs
    MCP servers
    external services

Network/placement:

    QUIC
    MoQ
    BonfyreNet
    BonfyreWire
    BonfyreDistribute
    BonfyreSync
    Hydro placement
    browser
    device
    local runtime
    remote GPU
    cloud/edge

Device:

    DevicePassport
    CapabilityBeacon
    sensor/actuator binding
    placement
    resource weather
    multi-device missions
    optical/QR/BLE/NFC/LAN transfer concepts

Onions remains a separate experimental network/privacy family. It is not a
generic synonym for networking.

Tor, if installed, is ordinary public-network transport only.

Do not use Tor to bypass:

    CAPTCHAs
    rate limits
    geographic restrictions
    bans
    duplicate-account controls
    KYC
    eligibility rules
    anti-bot controls
    authorization boundaries

A blocked control is a blocked edge. Route to a different legitimate action.

---

# SEVENTEEN: AUREKAI

Aurekai is the controlled variation/candidate plane.

Bonfyre owns authoritative continuity.

Aurekai owns:

    candidate state
    alternate implementation
    alternate surface
    alternate provider route
    alternate workgraph
    alternate model route
    simulation
    comparison
    mutation
    promotion proposal

Core lifecycle:

    observe
    hypothesize
    candidate
    execute speculatively
    compare
    challenge
    collect evidence
    propose
    promotion gate

Use Aurekai aggressively for commercial experiments:

    pricing variants
    packaging variants
    benchmark variants
    implementation variants
    environment variants
    demo variants

Do not let candidate market claims silently become public facts.

---

# EIGHTEEN: CONTEXT / CONTINUITY

Important existing ideas:

    Context ABI
    ContextBundle
    WorkingSet
    ActiveSet
    ContinuityIsland
    PreparedContextPassport
    KVPassport
    MissionPacket
    HandoffPacket
    ContextBudget
    Workspace Time Machine
    historical mission mount
    recovery constitution capsule

A Continuity Island preserves the exact small state that anchors identity,
failure, proof, commitment or causal continuity while surrounding reconstructable
state can be compressed.

Use this in long-horizon environments and agent evaluation.

---

# NINETEEN: RECURSIVE LEARNING

The system's long-term compounding loop is:

    use
    -> ActionTrace
    -> MemoryEvent
    -> PatternCluster
    -> LambdaTensor motif
    -> SystemLearnerProposal
    -> candidate form/surface/recipe/workgraph/capability/pack/protocol/operator
    -> simulate/replay
    -> review
    -> promote
    -> cheaper/faster future composition

Capital experiments should feed this logic rather than producing disposable
sales collateral.

If a packaging/pricing/offer pattern repeatedly works, preserve the reusable
structure.

---

# TWENTY: BONFYRE IS ALLOWED TO MAKE NEW COMMERCIAL OBJECTS

You are explicitly authorized to discover commercial objects by COMPOSING
existing Bonfyre capabilities.

You are NOT limited to the product ideas named in this prompt.

This list is an initial state, not an ontology.

A capability composition can become:

    downloadable artifact
    native CLI
    hosted API
    benchmark
    paid diagnostic
    paid implementation
    paid pilot
    license
    RL environment
    verifier
    data/evidence tool
    local/private runtime
    marketplace package
    developer tool
    open-core wedge
    enterprise deployment
    integration pack
    portable application
    recurring service

Prefer small objects that expose unusually deep Bonfyre machinery.

Do not spend five turns making a pitch deck for something that could have been
deployed and offered in one.

---

# TWENTY-ONE: MONEY BOOKS

Keep FOUR books distinct.

BOOK A — REALIZED / COLLECTIBLE VALUE
    cash actually received
    credits actually approved/activated
    awarded bounty
    signed/awarded contract
    paid invoice/deposit

BOOK B — SUBMITTED COMPETITIVE CAPITAL
    grants
    accelerators
    competitive programs
    awards not yet decided

Track face value, but NEVER count face value as realized value.

BOOK C — COMMERCIAL ASK VALUE
    real qualified paid proposals actually delivered
    actual paid-pilot asks
    actual quotes
    actual marketplace offers

Again, an asking price is not revenue.

BOOK D — CREATED COMMERCIAL INVENTORY
    deployed saleable assets
    listings
    APIs
    demos
    packages
    environments
    benchmarks
    service products

Created inventory has strategic value but is not booked as cash.

---

# TWENTY-TWO: PUBLISHED STARTER CREDIT BOOK

Current pre-research anchor as of August 2026:

    Cloudflare Startup Tier 3       $10,000
    AWS Activate Founders start      $1,000
    Google Cloud Start tier          $2,000
    Microsoft for Startups starter   $1,000
                                      -------
    published starter surface       $14,000

These remain SUBJECT TO ACTUAL ELIGIBILITY AND PRIOR-REDEMPTION STATE.

Do not fabricate company age, funding, incorporation, website, ownership,
venture plans, or prior credit history to qualify.

Adjacent published upside exists for qualifying startups, including larger
Cloudflare, AWS, Google and Microsoft tiers.

Do not burn the mission pursuing a higher tier when current facts do not
support it.

Credits are fuel.

They are not the purpose of the company.

Once useful credits are acquired, immediately ask:

    What sellable proof/product can these credits accelerate?

---

# TWENTY-THREE: MONOPOLY NATIVE-C ABSORPTION EXERCISE

A concrete immediate external reference is:

    $MONOPOLY_URL

Current upstream characteristics:

    Python library + CLI
    bank-statement PDF -> CSV
    approximately twenty bank families
    credit/debit variants
    pdftotext dependency
    optional OCR
    password-protected PDF support
    generic parser
    totals/safety validation
    AGPL-3.0 upstream license

This is NOT a board-game environment.

Use it as a compact native-absorption gym and possible commercial capability.

The desired Bonfyre result is NOT necessarily a new public command.

Prefer extending the existing artifact/finance surface, for example:

    BonfyreIngest
    BonfyreFinance
    ArtifactIR
    YaFF
    Proof
    Canon
    Hash
    Pack

Target native architecture:

    PDF
      -> text extraction adapter
      -> bank/statement recognition
      -> declarative statement pack
      -> transaction extraction
      -> normalization
      -> totals/invariant verification
      -> StatementIR / YaFF
      -> CSV / JSON / Frappe / ledger projection

Bank differences should become DATA where practical:

    identifiers
    header/footer patterns
    date grammar
    transaction line grammar
    column rules
    amount sign rules
    balance rules
    statement-total rules

not twenty giant hard-coded C branches.

C should own:

    bounded parsing
    tokenizer/scanner
    deterministic extraction
    date/amount normalization
    invariant verification
    safe process invocation
    output emission
    contract/probe surface

Use existing system tools for PDF/OCR when appropriate rather than rewriting
a full PDF renderer.

License boundary:

    Upstream is AGPL-3.0.

Do not silently copy its implementation into proprietary Bonfyre code.

Choose one of two explicit paths:

A. CLEAN-ROOM / BEHAVIORAL REIMPLEMENTATION
    use public documented behavior, independently authored logic, synthetic or
    owner-authorized fixtures, and black-box behavioral comparison;
    do not copy implementation text.

B. AGPL ISOLATED DERIVATIVE
    if actual upstream code is reused or translated, preserve AGPL obligations
    and isolate/package it accordingly.

Record which path was chosen.

Do not use private real bank statements as public fixtures.

A strong result includes:

    native C path
    deterministic fixtures
    totals validation
    provenance
    behavior comparison
    Frappe/Finance projection
    benchmark vs Python reference
    a small commercializable object such as:
        local bank-statement extraction
        finance-import pack
        statement-to-ledger tool
        private/offline finance artifact parser
        agent/RL finance-document task environment

This exercise should MOVE THE SYSTEM and produce a capitalizable asset.

Do not spend the entire capital mission perfecting it.

---

# TWENTY-FOUR: COMMERCIAL INVENTORY TO START FROM

You already have enough technology to form offers without market discovery.

MODEL / FPQ:
    model compression compatibility audit
    FPQ conversion
    memory reduction report
    local inference deployment
    private model runtime
    model fidelity benchmark
    prepared-context/KV packaging
    QUIC model serving

AGENT / RL:
    Frappe enterprise computer-use environment
    ERPNext/CRM/Helpdesk long-horizon environment
    HRMS/LMS/Drive/Wiki cross-app environment
    WorkGraph-based verifier
    effect/authority verifier
    deterministic state/reset/replay environment
    Scene Lab procedural environment
    SERM multimodal environment
    agent governance proof environment

BUSINESS PLATFORM:
    Frappe native runtime deployment
    cross-app capability fabric
    governed agent workflows
    external capability connection packs
    local/private AI for Frappe
    durable-work modernization
    audit/replay/effect governance

ARTIFACT:
    native finance statement parser
    executable evidence pack
    portable Bonfyre capsule
    BonfyreFS materialized project
    procedural artifact transfer
    multimodal evidence package

DEVELOPER / INFRA:
    behavior verifier
    deterministic execution environment
    release/recovery verifier
    local-first runtime
    model/runtime benchmark harness
    provider contract harness

These are seed compositions, not limits.

---

# TWENTY-FIVE: OFFER LADDER

Use an adaptive ladder, not one arbitrary million-dollar pipeline.

Example asking-price bands for market experiments:

    $250–$2,500
        diagnostic
        compatibility scan
        benchmark
        technical audit
        small conversion

    $2,500–$15,000
        bounded implementation
        one environment
        one model deployment
        one governed workflow
        one integration pack
        one native artifact capability

    $15,000–$75,000
        pilot
        multi-app environment
        private model/runtime deployment
        agent governance pilot
        Frappe modernization pilot

    $75,000–$250,000+
        institutional deployment
        large environment estate
        enterprise operating fabric
        large model/runtime deployment

These are ASKING-PRICE EXPERIMENTS, not market facts.

Vary them based on response.

Track objections and conversion.

---

# TWENTY-SIX: CAPITAL DENSITY

Every candidate action consumes finite Codex cognition.

Prefer high expected-value-per-token work.

Estimate:

    expected_value =
        probability_of_success * legitimate_value
        - direct execution cost

    capital_density =
        expected_value / expected_codex_tokens

Also consider:

    time to external state
    human gate probability
    reusability
    strategic multiplier
    ability to compound into another asset

Do NOT use fake precision.

A rough ranking is enough.

Examples:

    claim straightforward $10k credit with high eligibility
        high density

    write a 70-page grant with missing legal facts
        low density until facts exist

    package an already-proven FPQ diagnostic and send 5 qualified offers
        potentially high density

    polish a giant architecture deck with no buyer
        near-zero density

---

# TWENTY-SEVEN: ACTION VERBS

Continuously select from:

    CLAIM
    BUILD
    VERIFY
    PACKAGE
    DEPLOY
    LIST
    SELL
    BID
    APPLY
    PARTNER
    FOLLOW UP
    COMPOUND
    REPRICE
    REPACKAGE
    ABSORB
    BENCHMARK

Research is allowed only as a short precursor to one of those verbs.

---

# TWENTY-EIGHT: EXTERNAL ACTION STANDARD

Every substantial Codex turn must achieve one or more of:

    account/program application actually submitted
    credits actually claimed/activated/requested
    relevant vendor registration completed
    bounty task actually implemented/submitted
    marketplace listing actually created/submitted
    commercial API/demo actually deployed
    commercial offer actually published
    qualified proposal actually delivered
    targeted pilot request actually submitted
    relevant partner application actually submitted
    sellable artifact actually published or staged for immediate submission
    technical environment actually packaged and sent
    externally verifiable benchmark actually published
    customer/prospect response actually processed
    existing pending action actually advanced

If one path hits a gate, record it and attack another path.

Do not end a turn because a single website requires human interaction.

---

# TWENTY-NINE: ACCOUNT / APPLICATION AUTHORITY

You are authorized to perform ordinary no-cost business actions for
Bonfyre/Aurekai when the facts are known and truthful:

    create a normal program/vendor/developer account
    create a normal organization/profile
    fill an application
    submit an application
    claim a free credit/benefit
    register for a relevant program
    publish a sanitized standalone demo/package
    send a targeted relevant business inquiry
    submit a paid-pilot/vendor/partner intake form
    submit work to a legitimate non-security bounty
    create a marketplace draft/listing
    use existing authenticated public developer tools normally

Do NOT:

    borrow money
    open credit
    open a loan
    trade securities
    trade crypto
    gamble
    purchase paid services
    start paid subscriptions
    enable Codex auto-top-up
    transfer owner funds
    enter debt
    provide personal guarantees
    move money from bank accounts
    falsify eligibility
    create duplicate accounts to bypass limits
    bypass KYC/MFA/CAPTCHA/anti-bot
    impersonate a handwritten/legal signature
    sign material legal agreements requiring officer certification
    submit tax attestations you cannot establish
    publish proprietary Bonfyre source accidentally

If a legal signature, banking setup, government identity, KYC, tax identity,
officer certification, or unsupported material company fact is required:

    complete all safe work before the gate
    write exact gate into human_gates
    preserve the URL/state
    move to another action

---

# THIRTY: TRUTH CONTRACT

Never fabricate:

    legal entity status
    incorporation date
    EIN / tax ID
    physical address
    founder demographics
    veteran/disability/minority status
    nonprofit status
    educational affiliation
    funding amount
    investor relationships
    accelerator membership
    revenue
    customer count
    employees
    customers
    testimonials
    security certifications
    compliance certifications
    benchmark results
    existing partnerships
    availability
    willingness
    intent
    price offered by another person

Observed behavior does not create present intent.

Correlation does not create consent.

Past activity does not create availability.

---

# THIRTY-ONE: SECRETS / ENVS

The child Codex process inherits the current shell environment.

Potential env-file KEY NAMES are indexed in:

    $ENV_NAMES

Do not print secret values.

Do not copy .env contents into reports, prompts, GitHub issues, public demos or
logs.

Do not dump:

    browser cookie DBs
    password stores
    Keychain
    OAuth tokens
    API keys
    credential files

Use normal authenticated tools/sessions where available.

If an ordinary login is required and a browser can complete it through normal
authenticated state, use it.

Do not extract credentials to make automation easier.

---

# THIRTY-TWO: PUBLICATION SAFETY

The Bonfyre workspace may contain proprietary or sensitive source/state.

DO NOT:

    git push the Bonfyre workspace
    make the Bonfyre repository public
    create a public fork containing the Bonfyre source
    upload arbitrary internal logs
    upload chat history
    upload .env files
    upload model keys/tokens
    publish customer/private data

You MAY create NEW intentionally standalone public artifacts when useful.

Stage them first under:

    $PUBLICATION_STAGE

Before publication:

    inspect exactly what is included
    remove secrets
    remove private paths
    remove private data
    include an intentional license
    verify the package independently
    verify no excluded historical systems appear
    verify no proprietary Bonfyre source was swept in accidentally

Do not auto-commit/push the current Bonfyre workspace.

For an EXTERNAL open-source bounty repository, creating a normal isolated
branch/commit/PR in that external repository is allowed if required to submit
the bounty.

---

# THIRTY-THREE: GITHUB

If gh is authenticated, use it aggressively but legitimately for:

    bounty discovery
    issue discovery
    upstream research
    public package publication
    creating isolated public-safe artifacts
    external bounty PRs
    tracking responses
    finding ecosystem programs

Do not mass-open issues or PRs.

Do not spam maintainers.

One strong relevant action is better than 100 generic ones.

---

# THIRTY-FOUR: BOUNTIES / OPEN WORK

Search for legitimate current non-security paid work where:

    payout is known or credible
    task is bounded
    repository license is compatible
    task can be verified
    expected return exceeds Codex burn

Avoid unauthorized vulnerability exploitation or security testing.

A bounty is attractive because its capital path can be:

    issue
    -> implementation
    -> tests
    -> PR/submission
    -> review
    -> payout

Do the work, not a bounty landscape report.

---

# THIRTY-FIVE: COMMERCIAL OUTBOUND

Do not spam.

Target organizations only when there is a strong technical fit.

Use:

    public pilot intake
    vendor registration
    partner form
    developer/startup form
    explicit innovation intake
    public business contact
    legitimate direct contact where available and relevant

A qualified outbound packet should usually be SHORT:

    problem
    one concrete Bonfyre proof
    what can be demonstrated
    bounded offer
    next step

Do not send the entire architecture.

Do not claim customers that do not exist.

Track:

    sent
    delivered if known
    reply
    technical interest
    meeting
    objection
    price objection
    procurement blocker
    no response

Use feedback to repackage offers.

---

# THIRTY-SIX: RL / AGENT ENVIRONMENT COMMERCIALIZATION

Bonfyre has a particularly unusual environment substrate:

    nine real Frappe application estates
    typed business semantics
    WorkGraph durable execution
    effect classes
    authority
    receipts
    RuntimeImage / RealityImage-style candidate state
    execution phenotype
    actual-vs-candidate distinction
    branch/reset/replay concepts
    model/provider variation
    application surfaces

Build saleable environment packages around EXISTING estate capability.

Strong environment product properties:

    deterministic fixture
    state identity
    reset
    branch
    replay
    task specification
    action space
    observation space
    effect boundary
    success invariants
    policy violations
    trace
    verifier dimensions
    cost
    failure taxonomy

Verifier dimensions should remain separate where useful:

    task success
    semantic consistency
    effect safety
    authority compliance
    evidence sufficiency
    recovery quality
    trajectory efficiency
    cost

Do not collapse all of this to one arbitrary scalar reward too early.

Do not spend the whole mission building a theoretical RL platform.

Produce small real environments and put them in front of current buyers/partners.

---

# THIRTY-SEVEN: AGENT GOVERNANCE COMMERCIALIZATION

Use existing mechanics:

    Context/WorkingSet
    EffectClass
    ReviewInterrupt
    evidence
    affected state
    reversibility
    authority
    TypedCommit
    Receipt
    replay
    WorkGraph
    provider contracts

A compact proof can be:

    agent proposes high-impact action
    -> bounded context
    -> explicit effect
    -> review packet
    -> approve/reject/change
    -> typed commit
    -> receipt/replay

Package and sell the behavior, not the vocabulary.

---

# THIRTY-EIGHT: FPQ / LOCAL MODEL COMMERCIALIZATION

FPQ/local-model work is allowed to produce:

    benchmark service
    compatibility scanner
    conversion service
    deployment package
    memory plan
    local/private inference proof
    runtime comparison
    model-pack product

Prefer verifiable artifacts:

    input model identity
    source hash
    representation hash
    compression ratio
    fidelity metrics
    runtime behavior
    hardware fit
    reproduction command

Do not claim fidelity from old docs when current code cannot reproduce it.

---

# THIRTY-NINE: MARKETPLACE / ECOSYSTEM TWO-WAY RULE

Whenever you enter a cloud/provider ecosystem, ask BOTH:

TAKE:
    credits
    grants
    startup benefits
    free infrastructure

SELL:
    listing
    marketplace
    partner program
    co-sell
    developer marketplace
    integration catalog
    solution directory

A cloud program is more valuable if it creates both runway and distribution.

---

# FORTY: CAPITAL COMPOUNDING

Never treat a credit win as terminal.

Example:

    credits
      -> compute/deployment
      -> proof
      -> commercial asset
      -> listing/outbound
      -> paid pilot
      -> cash
      -> more capacity

Never treat a technical build as terminal.

Example:

    Monopoly-like statement parser
      -> native Bonfyre finance capability
      -> benchmark/proof
      -> downloadable/private tool
      -> Frappe import capability
      -> finance-document RL task
      -> paid diagnostic/pilot

Every meaningful win should ask:

    What does this unlock next?

---

# FORTY-ONE: NO LOCAL-POLISH TRAP

A turn is invalid if it spends all of its time:

    renaming architecture
    cleaning docs
    refactoring unrelated internals
    making a giant deck
    building a generic ingestion pipeline
    building a scraper before a target exists
    creating speculative frameworks with no capital edge
    rewriting stable code for aesthetics
    studying old chat history

Local engineering is justified when it directly creates:

    a saleable object
    a required proof
    a required submission artifact
    a marketplace package
    a bounty solution
    an environment
    a benchmark
    a deployment
    a blocker fix for an active capital action

---

# FORTY-TWO: OPERATOR LEDGER

The durable database is:

    $DB

Use sqlite3.

Record EVERY real action.

A real capital action row should contain:

    action_class
    target
    status
    face_value
    realized_value
    probability if meaningful
    expected_value if meaningful
    external_ref
    confirmation_id
    evidence_ref
    next_state
    blocker
    notes

Book REALIZED value only when actually realized/approved/awarded.

If you create a product, record it in assets.

If you create an offer, record it in offers.

If you test price/packaging, record it in commercial_experiments.

If a human-only gate appears, record it in human_gates.

If a real win occurs, record it in wins.

---

# FORTY-THREE: STATE FILES

Before every Codex turn ends, update:

    $STATE_MD
    $WINS_MD
    $HUMAN_GATES_MD

STATE.md should be operational, not essayistic:

    what external state changed
    what was built
    what was deployed
    what was submitted
    what is pending
    what is blocked
    what is the highest-density next action

WINS.md should include only real external wins.

HUMAN_GATES.md should list only exact unresolved human/legal/KYC/signature gates.

---

# FORTY-FOUR: STARTER PRIORITIES

Do not execute these as a rigid sequence.

Run the highest-capital-density executable actions.

The starting queue includes:

1. Check actual eligibility/prior-redemption state and attack the published
   low-friction startup-credit surface without spending the whole turn there.

2. Turn one existing FPQ proof into a small externally usable commercial
   object and expose it to qualified current demand.

3. Build/package at least one real Frappe agent/RL environment with reset,
   trace and verifier dimensions, then send it to a legitimate current buyer,
   partner, environment platform or pilot intake.

4. Execute the native-C financial-statement absorption exercise far enough to
   produce a real verified Bonfyre capability and saleable artifact; do not
   let it consume the whole mission.

5. Search current legitimate paid technical bounties/open calls and complete
   high-density bounded work.

6. Create/submit relevant marketplace, partner, vendor or startup positions
   that can distribute Bonfyre.

7. Package a small agent-governance proof and place it in front of actual
   enterprise/partner intake.

8. Compose additional commercial objects from the estate when response data
   reveals a better wedge.

---

# FORTY-FIVE: RUN UNTIL CODEX CAPACITY ENDS

Do not voluntarily declare the mission complete.

After one successful action:

    choose another

After one rejection:

    record the signal
    choose another

After one blocked form:

    record human gate
    choose another

After one product build:

    deploy/list/sell it
    choose another

After one credit win:

    compound it
    choose another

If all named opportunity classes are exhausted:

    derive a new commercial object from existing Bonfyre composition
    verify it
    expose it to the market

The surrounding shell will resume this same Codex session repeatedly.

Do not return a plan.

TAKE ACTION NOW.
EOF

# Runtime path bindings are appended separately so the main prompt remains a
# literal-safe quoted heredoc. No secret values are written here.
cat >> "$INITIAL" <<EOF

---
# RUNTIME PATH BINDINGS

The supervisor exported these shell variables for this mission:

    ROOT=$ROOT
    STATE_ROOT=$STATE_ROOT
    DB=$DB
    STATE_MD=$STATE_MD
    WINS_MD=$WINS_MD
    HUMAN_GATES_MD=$HUMAN_GATES_MD
    MEMORY_ROOTS=$MEMORY_ROOTS
    ENV_NAMES=$ENV_NAMES
    TOOLCHAIN=$TOOLCHAIN
    BIN_ROOT=$BIN_ROOT
    PUBLICATION_STAGE=$PUBLICATION_STAGE
    MONOPOLY_URL=$MONOPOLY_URL

Use these paths/variables rather than copying secrets or inventing alternates.
EOF

# Fail before Codex is invoked if prompt generation broke.
if [[ ! -s "$INITIAL" ]]; then
  echo "ERROR: initial-condition packet was not generated: $INITIAL" >&2
  exit 2
fi

if ! grep -q '^# BONFYRE / AUREKAI CAPITAL GYM — INITIAL CONDITIONS' "$INITIAL"; then
  echo "ERROR: initial-condition packet failed its header integrity check." >&2
  exit 2
fi

if ! grep -q '^TAKE ACTION NOW\.$' "$INITIAL"; then
  echo "ERROR: initial-condition packet failed its tail integrity check." >&2
  exit 2
fi

###############################################################################
# INITIAL HUMAN-READABLE STATE
###############################################################################

if [[ ! -s "$STATE_MD" ]]; then
  cat > "$STATE_MD" <<EOF
# Bonfyre Capital Gym State

Mission initialized: $(date)

Durable ledger:
$DB

Initial conditions:
$INITIAL

Memory roots:
$MEMORY_ROOTS

Environment key-name inventory:
$ENV_NAMES

Toolchain:
$TOOLCHAIN

No economic actions have been recorded yet.
EOF
fi

###############################################################################
# CURRENT LEDGER SUMMARY FOR SHORT RESUME TURNS
###############################################################################

write_turn_prompt() {
  local round="$1"

  local actions_pending wins_count realized ask_value assets_count gates_count
  actions_pending="$(db_scalar "SELECT count(*) FROM capital_actions WHERE status NOT IN ('complete','won','rejected','ineligible','abandoned');" 2>/dev/null || echo 0)"
  wins_count="$(db_scalar "SELECT count(*) FROM wins;" 2>/dev/null || echo 0)"
  realized="$(db_scalar "SELECT printf('%.2f',coalesce(sum(realized_value),0)) FROM wins;" 2>/dev/null || echo 0)"
  ask_value="$(db_scalar "SELECT printf('%.2f',coalesce(sum(face_value),0)) FROM capital_actions WHERE action_class IN ('proposal','pilot','quote','commercial_outbound') AND status NOT IN ('draft');" 2>/dev/null || echo 0)"
  assets_count="$(db_scalar "SELECT count(*) FROM assets WHERE status NOT IN ('abandoned');" 2>/dev/null || echo 0)"
  gates_count="$(db_scalar "SELECT count(*) FROM human_gates WHERE status='open';" 2>/dev/null || echo 0)"

  cat > "$TURN_PROMPT" <<EOF
Continue the BONFYRE / AUREKAI CAPITAL GYM from the same durable mission.

DO NOT re-explain Bonfyre.
DO NOT summarize the repository.
DO NOT reread all chat history.
DO NOT produce a market landscape.
DO NOT stop after a single blocked website.
DO NOT use the three excluded historical systems.

Round: $round

Current ledger summary:
    pending actions: $actions_pending
    wins: $wins_count
    realized/approved value booked: \$$realized
    delivered commercial ask face value: \$$ask_value
    active commercial assets: $assets_count
    open human gates: $gates_count

Read only the operational state you need:

    $STATE_MD
    $WINS_MD
    $HUMAN_GATES_MD
    $DB

Use targeted history search ONLY if a specific missing historical fact blocks
execution:

    $BIN_ROOT/capital-memory-search "exact fact"

This turn must MOVE ECONOMIC STATE.

Choose the highest-capital-density executable action now.

Valid verbs:

    CLAIM BUILD VERIFY PACKAGE DEPLOY LIST SELL BID APPLY PARTNER
    FOLLOW-UP COMPOUND REPRICE REPACKAGE ABSORB BENCHMARK

If local engineering is needed, connect it directly to an external capital
action in this turn whenever technically possible.

If a prior asset is already built, prefer exposing/selling/submitting it over
polishing it.

If one path is blocked, record the gate and attack another path in the SAME
turn.

Before ending the turn, update the capital DB and STATE.md.
EOF
}

###############################################################################
# CODEX CLI FEATURE DETECTION
###############################################################################

CODEX_HELP="$(codex --help 2>&1 || true)"
CODEX_EXEC_HELP="$(codex exec --help 2>&1 || true)"
CODEX_RESUME_HELP="$(codex exec resume --help 2>&1 || true)"

SEARCH_FLAGS=()
EXEC_FLAGS=()

if printf '%s\n' "$CODEX_HELP" | grep -q -- '--search'; then
  SEARCH_FLAGS+=(--search)
elif printf '%s\n' "$CODEX_EXEC_HELP" | grep -q -- '--search'; then
  EXEC_FLAGS+=(--search)
fi

# The owner explicitly requested broad local permissions for this operator.
# Prefer the current explicit dangerous/full-access flag when available.
if printf '%s\n' "$CODEX_EXEC_HELP" | grep -q -- '--dangerously-bypass-approvals-and-sandbox'; then
  EXEC_FLAGS+=(--dangerously-bypass-approvals-and-sandbox)
elif printf '%s\n' "$CODEX_EXEC_HELP" | grep -q -- '--yolo'; then
  EXEC_FLAGS+=(--yolo)
elif printf '%s\n' "$CODEX_EXEC_HELP" | grep -q -- '--full-auto'; then
  EXEC_FLAGS+=(--full-auto)
fi

if printf '%s\n' "$CODEX_EXEC_HELP" | grep -q -- '--json'; then
  EXEC_FLAGS+=(--json)
fi

if printf '%s\n' "$CODEX_EXEC_HELP" | grep -q -- '--cd'; then
  EXEC_FLAGS+=(--cd "$ROOT")
elif printf '%s\n' "$CODEX_EXEC_HELP" | grep -q -- '--cwd'; then
  EXEC_FLAGS+=(--cwd "$ROOT")
elif printf '%s\n' "$CODEX_EXEC_HELP" | grep -qE -- '(^|[[:space:]])-C([,[:space:]]|$)'; then
  EXEC_FLAGS+=(-C "$ROOT")
fi

if [[ -n "$CAPITAL_MODEL" ]]; then
  if printf '%s\n' "$CODEX_EXEC_HELP" | grep -q -- '--model'; then
    EXEC_FLAGS+=(--model "$CAPITAL_MODEL")
  elif printf '%s\n' "$CODEX_EXEC_HELP" | grep -qE -- '(^|[[:space:]])-m([,[:space:]]|$)'; then
    EXEC_FLAGS+=(-m "$CAPITAL_MODEL")
  fi
fi

{
  echo "codex_path=$(command -v codex)"
  echo "codex_version=$(codex --version 2>/dev/null | head -n 1 || true)"
  printf 'search_flags='
  printf '%q ' "${SEARCH_FLAGS[@]}"
  echo
  printf 'exec_flags='
  printf '%q ' "${EXEC_FLAGS[@]}"
  echo
} > "$STATE_ROOT/CODEX_PREFLIGHT.txt"

###############################################################################
# EVENT / ERROR HELPERS
###############################################################################

is_hard_capacity_stop() {
  local f="$1"

  # 2026-08-12 postmortem: the real Codex message is "You've hit your usage
  # limit ... or try again at Aug 18th, 2026 8:45 PM." None of the prior
  # patterns matched "hit" (only "reached"), so this fell through to the
  # generic sleep-10-and-retry path for 1,366 rounds over ~13 hours against a
  # provider that had already stated its own recovery time. Match on the
  # stable "usage limit" / "out of credits" / "purchase more credits" phrases
  # regardless of the verb in front of them.
  grep -Eiq \
    'out of credits|no credits remaining|insufficient credits|credit balance.*(zero|exhausted)|usage limit|purchase more credits|insufficient_quota|quota exhausted|no remaining quota|agentic usage limit|credit quota exhausted|not enough credits' \
    "$f"
}

is_transient_failure() {
  local f="$1"

  grep -Eiq \
    'temporarily unavailable|service unavailable|connection reset|connection refused|network error|timed out|timeout|gateway|429|502|503|504|rate.?limit.*retry|try again later' \
    "$f"
}

is_invalid_session() {
  local f="$1"

  grep -Eiq \
    'thread.*not found|session.*not found|unknown thread|invalid.*session|cannot resume|failed to resume|conversation.*not found' \
    "$f"
}

extract_session_id() {
  local f="$1"

  if command -v jq >/dev/null 2>&1; then
    jq -r '
      select(.type == "thread.started" or .event == "thread.started") |
      (.thread_id // .threadId // .thread.id // .session_id // .id // empty)
    ' "$f" 2>/dev/null | head -n 1
  else
    sed -nE 's/.*"thread[_-]?id"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' "$f" | head -n 1
  fi
}

codex_thread_started() {
  local f="$1"
  [[ -s "$f" ]] || return 1
  grep -Eq '"(type|event)"[[:space:]]*:[[:space:]]*"thread\.started"' "$f"
}

record_usage() {
  local turn_id="$1"
  local f="$2"

  local in_tok="" cache_tok="" out_tok=""

  if command -v jq >/dev/null 2>&1; then
    in_tok="$(jq -s '
      [ .[] |
        (.usage.input_tokens //
         .usage.inputTokens //
         .input_tokens //
         empty)
      ] | map(select(type=="number")) | add // 0
    ' "$f" 2>/dev/null || echo 0)"

    cache_tok="$(jq -s '
      [ .[] |
        (.usage.cached_input_tokens //
         .usage.cachedInputTokens //
         .cached_input_tokens //
         empty)
      ] | map(select(type=="number")) | add // 0
    ' "$f" 2>/dev/null || echo 0)"

    out_tok="$(jq -s '
      [ .[] |
        (.usage.output_tokens //
         .usage.outputTokens //
         .output_tokens //
         empty)
      ] | map(select(type=="number")) | add // 0
    ' "$f" 2>/dev/null || echo 0)"
  fi

  sqlite3 "$DB" \
    "UPDATE turns
       SET input_tokens=COALESCE(NULLIF('$in_tok',''),0),
           cached_input_tokens=COALESCE(NULLIF('$cache_tok',''),0),
           output_tokens=COALESCE(NULLIF('$out_tok',''),0)
     WHERE id=$turn_id;" 2>/dev/null || true
}

###############################################################################
# CODEX INVOCATION
###############################################################################

run_new_thread() {
  local prompt_file="$1"
  local event_file="$2"
  local err_file="$3"
  local final_file="$4"

  local flags=("${EXEC_FLAGS[@]}")

  if printf '%s\n' "$CODEX_EXEC_HELP" | grep -q -- '--output-last-message'; then
    flags+=(--output-last-message "$final_file")
  fi

  (
    cd "$ROOT"
    exec codex "${SEARCH_FLAGS[@]}" exec "${flags[@]}" - \
      < "$prompt_file" \
      > "$event_file" \
      2> "$err_file"
  ) &
  ACTIVE_CHILD=$!
  wait "$ACTIVE_CHILD"
  local rc=$?
  ACTIVE_CHILD=""
  return "$rc"
}

run_resume_thread() {
  local session_id="$1"
  local prompt_file="$2"
  local event_file="$3"
  local err_file="$4"
  local final_file="$5"

  local flags=("${EXEC_FLAGS[@]}")

  if printf '%s\n' "$CODEX_RESUME_HELP" | grep -q -- '--output-last-message'; then
    flags+=(--output-last-message "$final_file")
  fi

  (
    cd "$ROOT"
    exec codex "${SEARCH_FLAGS[@]}" exec "${flags[@]}" resume "$session_id" - \
      < "$prompt_file" \
      > "$event_file" \
      2> "$err_file"
  ) &
  ACTIVE_CHILD=$!
  wait "$ACTIVE_CHILD"
  local rc=$?
  ACTIVE_CHILD=""
  return "$rc"
}

resume_supported() {
  codex exec resume --help >/dev/null 2>&1
}

###############################################################################
# SUPERVISOR
###############################################################################

ROUND=0
FIRST_THREAD=1

if [[ -s "$SESSION_FILE" ]]; then
  existing_session="$(tr -d '\r\n[:space:]' < "$SESSION_FILE" 2>/dev/null || true)"
  if [[ -z "$existing_session" ]]; then
    rm -f "$SESSION_FILE"
  elif resume_supported; then
    FIRST_THREAD=0
  fi
fi

echo
echo "======================================================================"
echo " BONFYRE / AUREKAI CAPITAL GYM"
echo "======================================================================"
echo " Root:       $ROOT"
echo " State:      $STATE_ROOT"
echo " Ledger:     $DB"
echo " Runs:       $RUN_ROOT"
echo " Memory:     $MEMORY_ROOTS"
echo " Env names:  $ENV_NAMES"
echo " Monopoly:   $MONOPOLY_REF"
echo " Initial:    $INITIAL ($(wc -c < "$INITIAL" | tr -d ' ') bytes)"
echo " Preflight:  $STATE_ROOT/CODEX_PREFLIGHT.txt"
echo
echo " The supervisor will continue until Codex reports exhausted/unavailable"
echo " capacity, or until you interrupt it."
echo
echo " This script does NOT purchase Codex credits or enable auto top-up."
echo "======================================================================"
echo

while true; do
  ROUND=$((ROUND + 1))

  TS="$(date '+%Y%m%d-%H%M%S')"
  RUN_DIR="$RUN_ROOT/${TS}-round-${ROUND}"
  mkdir -p "$RUN_DIR"

  EVENTS="$RUN_DIR/events.jsonl"
  STDERR="$RUN_DIR/stderr.log"
  COMBINED="$RUN_DIR/combined.log"
  FINAL="$RUN_DIR/final.md"

  echo | tee -a "$SUPERVISOR_LOG"
  echo "======================================================================" | tee -a "$SUPERVISOR_LOG"
  echo "ROUND $ROUND" | tee -a "$SUPERVISOR_LOG"
  echo "START $TS" | tee -a "$SUPERVISOR_LOG"
  echo "PREPARING prompt/session/ledger..." | tee -a "$SUPERVISOR_LOG"

  # This is the only point in the loop where no Codex turn is active: the
  # previous turn's ended_at was just written, and the next turn's row has
  # not been inserted yet. The manager side-car tries to catch this same gap
  # by polling codex_turn_active() externally, but during productive runs the
  # gap between one turn's end and the next turn's start collapses to a few
  # bash statements (sub-second), so external polling can miss it for the
  # entire life of a session (observed: turns 106-134 ran unbroken on one
  # session despite a pending rollover request). Checking the request here,
  # synchronously in the only process that actually knows the turn boundary,
  # makes the rollover deterministic instead of a race.
  ROLL_REQUEST="$STATE_ROOT/claude-manager/RESET_CODEX_SESSION.request"
  if [[ -f "$ROLL_REQUEST" && -s "$SESSION_FILE" ]]; then
    OLD_SESSION_FOR_ROLL="$(cat "$SESSION_FILE" 2>/dev/null || true)"
    rm -f "$SESSION_FILE" "$ROLL_REQUEST"
    echo "SESSION ROLLOVER applied at round boundary (was $OLD_SESSION_FOR_ROLL)" \
      | tee -a "$SUPERVISOR_LOG"
  fi

  if [[ "$FIRST_THREAD" -eq 1 ]]; then
    PROMPT_FILE="$INITIAL"
  else
    write_turn_prompt "$ROUND"
    PROMPT_FILE="$TURN_PROMPT"
  fi

  SESSION_ID=""
  if [[ -s "$SESSION_FILE" ]]; then
    SESSION_ID="$(cat "$SESSION_FILE" 2>/dev/null || true)"
  fi

  escaped_session="$(printf "%s" "$SESSION_ID" | sed "s/'/''/g")"
  TURN_ID="$(
    db_scalar "INSERT INTO turns(started_at,codex_session,note)
               VALUES(datetime('now'),'$escaped_session','round $ROUND');
               SELECT last_insert_rowid();" 2>/dev/null
  )"

  if [[ ! "$TURN_ID" =~ ^[0-9]+$ ]]; then
    echo "FATAL: could not allocate a durable turn id in $DB" >&2
    exit 2
  fi

  if [[ -n "$SESSION_ID" ]]; then
    echo "SESSION $SESSION_ID" | tee -a "$SUPERVISOR_LOG"
  else
    echo "SESSION new" | tee -a "$SUPERVISOR_LOG"
  fi
  echo "PROMPT $(wc -c < "$PROMPT_FILE" | tr -d ' ') bytes" | tee -a "$SUPERVISOR_LOG"
  echo "LEDGER turn_id=$TURN_ID" | tee -a "$SUPERVISOR_LOG"
  echo "LAUNCHING CODEX..." | tee -a "$SUPERVISOR_LOG"
  echo "======================================================================" | tee -a "$SUPERVISOR_LOG"

  set +e

  if [[ -n "$SESSION_ID" ]] && resume_supported; then
    run_resume_thread "$SESSION_ID" "$PROMPT_FILE" "$EVENTS" "$STDERR" "$FINAL"
    RC=$?
  else
    run_new_thread "$PROMPT_FILE" "$EVENTS" "$STDERR" "$FINAL"
    RC=$?
  fi

  set -e

  cat "$EVENTS" "$STDERR" > "$COMBINED" 2>/dev/null || true

  NEW_SESSION="$(extract_session_id "$EVENTS" || true)"

  if [[ -n "$NEW_SESSION" ]]; then
    printf '%s\n' "$NEW_SESSION" > "$SESSION_FILE"
    SESSION_ID="$NEW_SESSION"
  fi

  escaped_session="$(printf "%s" "$SESSION_ID" | sed "s/'/''/g")"
  db_exec "UPDATE turns
             SET ended_at=datetime('now'),
                 codex_session='$escaped_session',
                 exit_code=$RC
           WHERE id=$TURN_ID;" 2>/dev/null || true

  record_usage "$TURN_ID" "$EVENTS"

  ###########################################################################
  # HARD CODEX CAPACITY STOP
  ###########################################################################

  if is_hard_capacity_stop "$COMBINED"; then
    RETRY_AT="$(grep -Eio 'try again at [^.]*' "$COMBINED" | head -n 1 | sed -E 's/^try again at //I')"

    {
      echo
      echo "======================================================================"
      echo "CODEX CAPACITY / CREDIT LIMIT REACHED"
      echo "Time:       $(date)"
      echo "Round:      $ROUND"
      echo "Retry at:   ${RETRY_AT:-unknown; provider gave no recovery time}"
      echo "======================================================================"
    } | tee -a "$SUPERVISOR_LOG"

    RETRY_AT_ESCAPED="$(printf "%s" "${RETRY_AT:-}" | sed "s/'/''/g")"
    db_exec "INSERT OR REPLACE INTO meta(key,value)
               VALUES('terminal_state','codex_capacity_exhausted'),
                     ('terminal_time',datetime('now')),
                     ('capacity_retry_at','$RETRY_AT_ESCAPED');" 2>/dev/null || true

    break
  fi

  ###########################################################################
  # LOST SESSION: REHYDRATE FROM DURABLE STATE, DON'T KILL THE MISSION
  ###########################################################################

  if [[ "$RC" -ne 0 ]] && is_invalid_session "$COMBINED"; then
    echo "Codex session could not be resumed; rehydrating a new thread." \
      | tee -a "$SUPERVISOR_LOG"

    rm -f "$SESSION_FILE"
    FIRST_THREAD=1
    sleep 3
    continue
  fi

  ###########################################################################
  # TRANSIENT FAILURE
  ###########################################################################

  if [[ "$RC" -ne 0 ]] && is_transient_failure "$COMBINED"; then
    echo "Transient Codex/network failure; backing off and continuing." \
      | tee -a "$SUPERVISOR_LOG"

    sleep 45
    continue
  fi

  ###########################################################################
  # SUPERVISOR / CLI FAILURE
  #
  # If Codex returned nonzero before a thread ever started, endlessly retrying
  # cannot advance the mission. Stop loudly with the diagnostic paths.
  ###########################################################################

  if [[ "$RC" -ne 0 ]] && ! codex_thread_started "$EVENTS"; then
    {
      echo
      echo "FATAL: Codex did not start a thread (exit $RC)."
      echo "This is a CLI/config/supervisor failure, not a blocked capital action."
      echo "stderr: $STDERR"
      echo "events: $EVENTS"
      echo "preflight: $STATE_ROOT/CODEX_PREFLIGHT.txt"
      echo
      echo "--- stderr tail ---"
      tail -n 80 "$STDERR" 2>/dev/null || true
    } | tee -a "$SUPERVISOR_LOG" >&2

    db_exec "INSERT OR REPLACE INTO meta(key,value)
               VALUES('terminal_state','codex_supervisor_failure'),
                     ('terminal_time',datetime('now'));" >/dev/null 2>&1 || true

    exit 2
  fi

  ###########################################################################
  # OTHER FAILURE AFTER A REAL THREAD STARTED
  ###########################################################################

  if [[ "$RC" -ne 0 ]]; then
    echo "Codex returned exit code $RC after starting a thread; mission remains active." \
      | tee -a "$SUPERVISOR_LOG"

    sleep 10

    if [[ -z "$SESSION_ID" ]]; then
      FIRST_THREAD=1
    else
      FIRST_THREAD=0
    fi

    continue
  fi

  ###########################################################################
  # SUCCESSFUL TURN
  ###########################################################################

  FIRST_THREAD=0

  echo "Round $ROUND completed; resuming the same capital mission." \
    | tee -a "$SUPERVISOR_LOG"

  if [[ "$ONE_TURN" == "1" ]]; then
    echo "BONFYRE_CAPITAL_ONE_TURN=1 set; stopping after test turn."
    break
  fi

  sleep 2
done

###############################################################################
# FINAL LOCAL SUMMARY
###############################################################################

echo
echo "======================================================================"
echo " CAPITAL GYM STOPPED"
echo "======================================================================"

sqlite3 -header -column "$DB" <<'SQL' || true
SELECT
  (SELECT count(*) FROM wins) AS wins,
  (SELECT printf('$%.2f',coalesce(sum(realized_value),0)) FROM wins) AS realized_value,
  (SELECT count(*) FROM assets WHERE status NOT IN ('abandoned')) AS assets,
  (SELECT count(*) FROM offers WHERE status NOT IN ('abandoned')) AS offers,
  (SELECT count(*) FROM capital_actions) AS actions,
  (SELECT count(*) FROM human_gates WHERE status='open') AS human_gates;
SQL

echo
echo "State:"
echo "  $STATE_MD"
echo
echo "Wins:"
echo "  $WINS_MD"
echo
echo "Human gates:"
echo "  $HUMAN_GATES_MD"
echo
echo "Ledger:"
echo "  $DB"
echo
echo "Logs:"
echo "  $RUN_ROOT"
echo
