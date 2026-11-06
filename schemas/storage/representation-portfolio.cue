package storage

representationPortfolio: {
  schema: "bonfyre-representation-portfolio.v1"
  law: "one semantic identity; every useful representation; representations never own semantics"
  // Byte policy seeds missing manifold rows. The manifold row remains the
  // estate's authority after a policy is installed; a coordinator must not
  // overwrite a locally reviewed policy during routine execution.
  byte_economics: {
    parquet: {byte_policy: "canonical", preferred_temperature: "pinned", canonical_body_kind: "parquet", rationale: "canonical immutable columnar body"}
    iceberg: {byte_policy: "metadata_only", preferred_temperature: "warm", canonical_body_kind: "parquet", rationale: "snapshot/schema/history metadata over canonical Parquet"}
    delta: {byte_policy: "metadata_only_preferred", preferred_temperature: "warm", canonical_body_kind: "parquet", rationale: "prefer metadata overlay over shared Parquet"}
    hudi: {byte_policy: "metadata_only_preferred", preferred_temperature: "warm", canonical_body_kind: "parquet", rationale: "prefer bootstrap/XTable metadata over shared Parquet"}
    arrow: {byte_policy: "execution_on_demand", preferred_temperature: "reconstructable", canonical_body_kind: "parquet", rationale: "stream Arrow IPC on demand from canonical Parquet"}
    duckdb: {byte_policy: "catalog_only", preferred_temperature: "hot", canonical_body_kind: "parquet", rationale: "views/catalog over canonical Parquet"}
    datafusion: {byte_policy: "execution_only", preferred_temperature: "hot", canonical_body_kind: "parquet", rationale: "TableProvider execution; no copied body"}
    polars: {byte_policy: "execution_only", preferred_temperature: "hot", canonical_body_kind: "parquet", rationale: "lazy scan; no copied body"}
    adbc: {byte_policy: "transport_only", preferred_temperature: "hot", canonical_body_kind: "parquet", rationale: "Arrow-native transport"}
    "graph-virtual": {byte_policy: "metadata_only", preferred_temperature: "hot", canonical_body_kind: "parquet", rationale: "typed graph projection over source columns"}
    lance: {byte_policy: "distinct_layout", preferred_temperature: "warm", canonical_body_kind: "parquet", rationale: "materialize when random/vector/multimodal layout earns bytes"}
    zarr: {byte_policy: "distinct_layout", preferred_temperature: "warm", canonical_body_kind: "parquet", rationale: "materialize tensor/multiscale families"}
  }
  classes: {
    table: ["parquet", "arrow", "iceberg", "hudi", "delta", "paimon", "xtable"]
    stream: ["fluss", "feldera", "materialize", "risingwave"]
    graph: ["graphar", "kuzu"]
    multimodal: ["lance", "zarr"]
    query: ["datafusion", "duckdb", "flight-sql", "adbc"]
    kv: ["lmdb", "slatedb", "foundationdb"]
    candidate: ["automerge"]
    carrier: ["r2", "iroh", "ipfs", "bonfyrefs"]
    catalog: ["polaris", "nessie", "gravitino"]
  }
  capability_unlocks: {
    parquet: ["columnar_scan", "range_query", "cross_engine_read"]
    iceberg: ["snapshot_query", "schema_evolution", "time_travel"]
    hudi: ["incremental_read", "cdc", "mutation_history"]
    delta: ["optimized_layout", "deletion_vector", "versioned_table"]
    paimon: ["keyed_stream_state", "changelog_read"]
    fluss: ["realtime_table", "stream_scan", "hot_kv"]
    feldera: ["incremental_consequence", "retraction_propagation"]
    materialize: ["maintained_relational_view"]
    risingwave: ["streaming_application_view"]
    graphar: ["out_of_core_graph", "csr_csc_traversal"]
    kuzu: ["interactive_property_graph", "cypher_query"]
    lance: ["ann", "multimodal_retrieval", "similarity_join"]
    zarr: ["tensor_window", "multiscale_array"]
    datafusion: ["native_columnar_query"]
    duckdb: ["local_analytics", "browser_analytics"]
    "flight-sql": ["arrow_native_remote_query"]
    adbc: ["arrow_native_database_interop"]
    lmdb: ["zero_copy_local_lookup"]
    slatedb: ["object_store_lsm"]
    foundationdb: ["distributed_transactional_coordination"]
    automerge: ["offline_candidate_state", "collaborative_merge"]
    r2: ["durable_object_carrier"]
    iroh: ["content_addressed_transfer"]
    ipfs: ["federated_content_retrieval"]
    bonfyrefs: ["filesystem_projection"]
    polaris: ["iceberg_rest_catalog"]
    nessie: ["branchable_lake_state"]
    gravitino: ["federated_physical_metadata"]
    xtable: ["multi_format_metadata"]
    arrow: ["zero_copy_interchange"]
  }
  substrate_coordinates: {
    datafusion: ["feldera/source"]
    iceberg: ["feldera/source"]
    delta: ["feldera/source"]
    lmdb: []
    bonfyrefs: []
  }
}
