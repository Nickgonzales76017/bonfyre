-- Bonfyre readiness pipeline -- real Feldera SQL program definition.
--
-- This is the source/sink contract for Feldera's continuous incremental
-- materialization of the same readiness semantics the deterministic
-- SQLite adapter already computes live (engine/core/src/workgraph_schema.c,
-- schema version 8: bf_readiness_mission, bf_readiness_lease_pressure,
-- bf_readiness_effect_backlog, bf_readiness_compensation_backlog,
-- bf_readiness_capability). Standard ANSI SQL (Feldera is Calcite-based),
-- deliberately kept identical in relational logic to the SQLite views so
-- the two are provably the same computation over the same inputs.
--
-- Status: written and reviewable, NOT yet executed against a real Feldera
-- instance (feldera.local_image_pull=capacity_pending, see
-- docs/FELDERA_STATUS.md). The semantic parity between this SQL and the
-- live SQLite views is proven independently by
-- tests/requirements/feldera_source_sink_parity.sh, which recomputes the
-- same aggregates from the exact NDJSON export format below and checks
-- for an exact match against the live deterministic adapter -- the real
-- execution-engine swap is the only remaining unverified step.

-- Source tables: each is a keyed upsert stream. A real deployment binds
-- these to Bonfyre's fabric.db via Feldera's file/HTTP connector reading
-- the NDJSON produced by scripts/bonfyre-feldera-export (one file per
-- table, {"insert": {...}} per row, matching Feldera's documented
-- insert/delete NDJSON connector format).

CREATE TABLE workgraph_nodes (
    mission_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    status TEXT NOT NULL,
    lease_expires_at_ms BIGINT,
    PRIMARY KEY (mission_id, node_id)
);

CREATE TABLE effects (
    id TEXT NOT NULL PRIMARY KEY,
    mission_id TEXT NOT NULL,
    state TEXT NOT NULL
);

CREATE TABLE workgraph_compensations (
    mission_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    effect_id TEXT NOT NULL,
    state TEXT NOT NULL,
    PRIMARY KEY (mission_id, node_id, effect_id)
);

CREATE TABLE catalog_bindings (
    operator_id TEXT NOT NULL PRIMARY KEY,
    binding_state TEXT NOT NULL
);

CREATE TABLE fabric_meta (
    key TEXT NOT NULL PRIMARY KEY,
    value TEXT NOT NULL
);

-- Sink views: materialized, queried via Feldera's /query endpoint (the
-- same poll-based consumption pattern the fabric already uses for the
-- SQLite adapter via the `readiness` CLI verb -- no change to how
-- consumers read readiness, only to what computes it).

CREATE VIEW bf_readiness_mission AS
SELECT
    mission_id,
    COUNT(*) AS total_nodes,
    SUM(CASE WHEN status = 'complete' THEN 1 ELSE 0 END) AS complete_nodes,
    SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) AS blocked_nodes,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_nodes,
    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_nodes
FROM workgraph_nodes
GROUP BY mission_id;

CREATE VIEW bf_readiness_lease_pressure AS
SELECT
    mission_id,
    node_id,
    lease_expires_at_ms,
    CAST(EXTRACT(EPOCH FROM NOW()) * 1000 AS BIGINT) AS observed_at_ms,
    CASE WHEN lease_expires_at_ms < CAST(EXTRACT(EPOCH FROM NOW()) * 1000 AS BIGINT)
         THEN 1 ELSE 0 END AS expired
FROM workgraph_nodes
WHERE status = 'running' AND lease_expires_at_ms IS NOT NULL;

CREATE VIEW bf_readiness_effect_backlog AS
SELECT
    mission_id,
    COUNT(*) AS backlog_count
FROM effects
WHERE state NOT IN ('committed', 'compensated')
GROUP BY mission_id;

CREATE VIEW bf_readiness_compensation_backlog AS
SELECT
    mission_id,
    COUNT(*) AS backlog_count
FROM workgraph_compensations
WHERE state NOT IN ('compensated', 'rolled_back')
GROUP BY mission_id;

CREATE VIEW bf_readiness_capability AS
SELECT
    b.operator_id,
    b.binding_state,
    (SELECT value FROM fabric_meta WHERE key = 'catalog_generation') AS catalog_generation
FROM catalog_bindings b
WHERE b.operator_id LIKE 'command.%';
