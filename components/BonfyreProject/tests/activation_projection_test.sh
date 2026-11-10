#!/bin/sh
set -eu

bin=${1:-./bonfyre-project}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-activation-projection.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

db="$tmp/control-plane.db"
evidence="$tmp/demand-evidence.jsonl"

# Start from the exact pre-selection ResourceActivation schema so this witness
# proves the additive migration rather than only a fresh database.
python3 - "$db" <<'PY'
import sqlite3, sys
con = sqlite3.connect(sys.argv[1])
con.executescript("""
CREATE TABLE resource_candidates(
  resource_id TEXT PRIMARY KEY,
  mechanism TEXT NOT NULL DEFAULT 'service',
  qualified INTEGER NOT NULL DEFAULT 0,
  eligible INTEGER NOT NULL DEFAULT 0,
  activate_actor TEXT NOT NULL DEFAULT '',
  detail TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
""")
con.commit()
con.close()
PY

cat >"$evidence" <<'JSONL'
{"schema":"bonfyre-owner-participation.v1","occurrence_id":"external:test-service","axis":"discovery","state":"routable","owner":"DemandBook","from_owner":"ForeignTwin","to_owner":"DemandBook","basis_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","evidence_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
{"schema":"bonfyre-owner-participation.v1","occurrence_id":"external:test-service","axis":"capability","state":"eligible","owner":"SupplyMatch","from_owner":"DemandBook","to_owner":"SupplyMatch","basis_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","evidence_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
JSONL

"$bin" activation project-demand "$evidence" --db "$db" >"$tmp/first.json"

python3 - "$db" "$tmp/first.json" <<'PY'
import json, sqlite3, sys
db, receipt = sys.argv[1:]
out = json.load(open(receipt))
assert out["eligible"] == 1 and out["projected"] == 1
assert out["authority_actor_written"] == 0
con = sqlite3.connect(db)
cols = {row[1] for row in con.execute("PRAGMA table_info(resource_candidates)")}
assert "selection_detail" in cols
row = con.execute(
    "SELECT mechanism,qualified,eligible,activate_actor,detail,selection_detail "
    "FROM resource_candidates WHERE resource_id='external:test-service'"
).fetchone()
assert row is not None
assert row[:4] == ("service", 1, 1, "")
assert row[4].startswith("DemandBook/SupplyMatch:evidence=")
assert row[5] == ""
con.close()
PY

# Selection is an explicit fact, but not an AuthorityGraph grant or binding.
"$bin" activation select external:test-service \
  --actor person:approver \
  --mechanism bound-service \
  --evidence selection:operator \
  --db "$db" >"$tmp/selection.json"

python3 - "$db" "$tmp/selection.json" <<'PY'
import json, sqlite3, sys
db, receipt = sys.argv[1:]
out = json.load(open(receipt))
assert out["actor"] == "person:approver"
assert out["authority_inferred"] is False
assert out["activated_inferred"] is False
con = sqlite3.connect(db)
row = con.execute(
    "SELECT mechanism,qualified,eligible,activate_actor,detail,selection_detail "
    "FROM resource_candidates WHERE resource_id='external:test-service'"
).fetchone()
assert row[:4] == ("bound-service", 1, 1, "person:approver")
assert row[4].startswith("DemandBook/SupplyMatch:evidence=")
assert row[5] == "selection:operator"
con.close()
PY

# Re-projecting DemandBook evidence must preserve the independent selection.
"$bin" activation project-demand "$evidence" --db "$db" >"$tmp/refresh.json"
python3 - "$db" <<'PY'
import sqlite3, sys
con = sqlite3.connect(sys.argv[1])
row = con.execute(
    "SELECT mechanism,qualified,eligible,activate_actor,detail,selection_detail "
    "FROM resource_candidates WHERE resource_id='external:test-service'"
).fetchone()
assert row[:4] == ("bound-service", 1, 1, "person:approver")
assert row[4].startswith("DemandBook/SupplyMatch:evidence=")
assert row[5] == "selection:operator"
con.close()
PY

# Binding without the separate AuthorityGraph bind right must be refused.
set +e
"$bin" activation bind external:test-service \
  --service service:test-provider \
  --actor person:approver \
  --evidence receipt:bind \
  --db "$db" >"$tmp/bind-refused.json"
rc=$?
set -e
[ "$rc" -eq 3 ]
python3 - "$tmp/bind-refused.json" <<'PY'
import json, sys
out=json.load(open(sys.argv[1]))
assert out["bound"] is False
assert out["authority_inferred"] is False
PY

# Install only the explicit bind grant.  The binding may now exist, but
# ResourceActivation still must not infer activate authority.
python3 - "$db" <<'PY'
import sqlite3, sys
con=sqlite3.connect(sys.argv[1])
con.execute(
    """INSERT INTO authority_edges(
       edge_id,actor,permission,subject,purpose,scope,effective_from,expires_at,
       delegable,revoked,evidence,originating_authority,created_at)
       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    ("grant:bind","person:approver","bind","service:test-provider",
     "service-binding","",None,None,0,0,"test:explicit-bind","","2026-09-21T00:00:00+00:00"),
)
con.commit()
con.close()
PY

"$bin" activation bind external:test-service \
  --service service:test-provider \
  --actor person:approver \
  --evidence receipt:bind \
  --db "$db" >"$tmp/bound.json"

"$bin" activation reconcile external:test-service --db "$db" >"$tmp/bound-only.json"
python3 - "$db" "$tmp/bound.json" "$tmp/bound-only.json" <<'PY'
import json, sqlite3, sys
db, bound_file, rec_file = sys.argv[1:]
bound=json.load(open(bound_file))
rec=json.load(open(rec_file))
assert bound["bound"] is True
assert rec["candidates"] == 1
assert rec["eligible"] == 1
assert rec["authorized"] == 0
assert rec["activated"] == 0
con=sqlite3.connect(db)
assert con.execute("SELECT COUNT(*) FROM service_bindings WHERE state='bound'").fetchone()[0] == 1
con.close()
PY

# Only the explicit activate grant closes the three-owner join.
python3 - "$db" <<'PY'
import sqlite3, sys
con=sqlite3.connect(sys.argv[1])
con.execute(
    """INSERT INTO authority_edges(
       edge_id,actor,permission,subject,purpose,scope,effective_from,expires_at,
       delegable,revoked,evidence,originating_authority,created_at)
       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    ("grant:activate","person:approver","activate","external:test-service",
     "resource-activation","",None,None,0,0,"test:explicit-activate","","2026-09-21T00:00:01+00:00"),
)
con.commit()
con.close()
PY

"$bin" activation reconcile external:test-service --db "$db" >"$tmp/activated.json"
python3 - "$tmp/activated.json" <<'PY'
import json, sys
out=json.load(open(sys.argv[1]))
assert out["candidates"] == 1
assert out["eligible"] == 0
assert out["authorized"] == 0
assert out["activated"] == 1
assert out["writes"] is False
PY

# A routine DemandBook refresh must not disturb selection, binding, or authority.
"$bin" activation project-demand "$evidence" --db "$db" >"$tmp/refresh-activated.json"
"$bin" activation reconcile external:test-service --db "$db" >"$tmp/refresh-reconciled.json"
python3 - "$db" "$tmp/refresh-reconciled.json" <<'PY'
import json, sqlite3, sys
db, receipt=sys.argv[1:]
out=json.load(open(receipt))
assert out["activated"] == 1
con=sqlite3.connect(db)
row=con.execute(
    "SELECT mechanism,qualified,eligible,activate_actor,detail,selection_detail "
    "FROM resource_candidates WHERE resource_id='external:test-service'"
).fetchone()
assert row[:4] == ("bound-service",1,1,"person:approver")
assert row[4].startswith("DemandBook/SupplyMatch:evidence=")
assert row[5] == "selection:operator"
con.close()
PY

# If SupplyMatch retracts eligibility, it must dominate the independent later
# facts: the service is no longer usable, but those facts are not erased.
cat >"$evidence" <<'JSONL'
{"schema":"bonfyre-owner-participation.v1","occurrence_id":"external:test-service","axis":"discovery","state":"routable","owner":"DemandBook","from_owner":"ForeignTwin","to_owner":"DemandBook","basis_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","evidence_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
JSONL

"$bin" activation project-demand "$evidence" --db "$db" >"$tmp/retracted.json"
"$bin" activation reconcile external:test-service --db "$db" >"$tmp/retracted-reconciled.json"
python3 - "$db" "$tmp/retracted.json" "$tmp/retracted-reconciled.json" <<'PY'
import json, sqlite3, sys
db, projected_file, rec_file=sys.argv[1:]
projected=json.load(open(projected_file))
rec=json.load(open(rec_file))
assert projected["eligible"] == 0 and projected["retracted"] == 1
assert rec["activated"] == 0 and rec["authorized"] == 0 and rec["eligible"] == 0
con=sqlite3.connect(db)
row=con.execute(
    "SELECT mechanism,qualified,eligible,activate_actor,detail,selection_detail "
    "FROM resource_candidates WHERE resource_id='external:test-service'"
).fetchone()
assert row[:4] == ("bound-service",1,0,"person:approver")
assert row[4] == "DemandBook/SupplyMatch:retracted"
assert row[5] == "selection:operator"
assert con.execute("SELECT COUNT(*) FROM service_bindings WHERE state='bound'").fetchone()[0] == 1
assert con.execute("SELECT COUNT(*) FROM authority_edges WHERE revoked=0").fetchone()[0] == 2
con.close()
PY

# Restoring current eligibility re-opens the already-existing independent facts;
# no new authority or binding is inferred by the projection.
cat >"$evidence" <<'JSONL'
{"schema":"bonfyre-owner-participation.v1","occurrence_id":"external:test-service","axis":"discovery","state":"routable","owner":"DemandBook","from_owner":"ForeignTwin","to_owner":"DemandBook","basis_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","evidence_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
{"schema":"bonfyre-owner-participation.v1","occurrence_id":"external:test-service","axis":"capability","state":"eligible","owner":"SupplyMatch","from_owner":"DemandBook","to_owner":"SupplyMatch","basis_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","evidence_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
JSONL

"$bin" activation project-demand "$evidence" --db "$db" >"$tmp/restored.json"
"$bin" activation reconcile external:test-service --db "$db" >"$tmp/restored-reconciled.json"
python3 - "$tmp/restored-reconciled.json" <<'PY'
import json, sys
out=json.load(open(sys.argv[1]))
assert out["activated"] == 1
PY

echo "activation_projection_test: PASS"
