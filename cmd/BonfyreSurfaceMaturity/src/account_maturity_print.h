#ifndef BONFYRE_ACCOUNT_MATURITY_PRINT_H
#define BONFYRE_ACCOUNT_MATURITY_PRINT_H
static void print_estate_metrics_account(const EstateMetrics *m) {
    const int cut = axis_open_count(m);
    printf("{\"schema\":\"bonfyre-estate-maturity-audit.v2\","
        "\"owner_databases\":{\"cms\":%s,\"items\":%s,\"frontier\":%s,\"subject_fabric\":%s,\"account_work\":%s,\"meter\":%s,\"control\":%s,\"missing\":%d},"
        "\"sources\":{\"published\":%d,\"publisher_identity\":%d,\"retained_items\":%d,\"retained_sources\":%d,\"retained_publishers\":%d,\"retained_source_identity_gaps\":%d,\"identity_coverage_bps\":%d},"
        "\"subjects\":{\"eligible_publishers\":%d,\"addressed_publishers\":%d,\"publisher_address_gaps\":%d,\"bundles\":%d,\"readers\":%d,\"actions\":%d,\"scenes\":%d,\"addressed_without_bundle\":%d,\"addressed_without_reader\":%d,\"address_coverage_bps\":%d},"
        "\"frontier\":{\"admitted_channels\":%d,\"admitted_published_sources\":%d,\"with_reader\":%d,\"without_reader\":%d,\"reader_coverage_bps\":%d},"
        "\"programs\":{\"instances\":%d,\"source_collections\":%d,\"subjects_without_collection\":%d,\"collection_coverage_bps\":%d},"
        "\"account\":{\"tasks\":%d,\"runs\":%d,\"steps\":%d,\"outputs\":%d,\"inbox_states\":%d,\"inbox_operations\":%d,\"meter_events\":%d,\"meter_rates\":%d,\"meter_unpriced_events\":%d,\"index_current\":%s},"
        "\"evidence\":{\"relations\":%d,\"foreign_observations\":%d,\"frontier_layers\":%d,\"measured_invariants\":%d,\"proven_invariants\":%d,\"challenged_invariants\":%d,\"known_noncauses\":%d,\"sync_considered\":%d,\"sync_exact\":%d,\"sync_unresolved\":%d,\"sync_current\":%s},"
        "\"lake\":{\"measured\":%s,\"source_roots\":%d,\"roots_with_identity\":%d,\"root_identity_bps\":%d,\"unresolved_identity_gaps\":%d,"
        "\"items_with_subject\":%d,\"item_subject_bps\":%d,\"subjects\":%d,\"subjects_with_site\":%d,\"subjects_with_reader\":%d,"
        "\"subjects_with_site_and_reader\":%d,\"institution_sites_without_reader\":%d},"
        "\"surface_scenes\":%d,\"maturity_debt\":%d,\"core_maturity_debt\":%d,"
        "\"axes\":{\"identity\":\"%s\",\"address\":\"%s\",\"material\":\"%s\",\"subject\":\"%s\",\"program\":\"%s\",\"surface\":\"%s\",\"action\":\"%s\",\"account\":\"%s\",\"evidence\":\"%s\"},"
        "\"cross_axis_open\":%d,\"whole_estate_ready\":%s,\"gate_scope\":\"local_estate_plus_account_and_evidence\","
        "\"truth\":\"row debt and axis maturity are separate; evidence coverage proves current owner measurement only; measured is never promoted to proven\"}\n",
        m->cms_db ? "true" : "false", m->items_db ? "true" : "false", m->frontier_db ? "true" : "false", m->fabric_db ? "true" : "false",
        m->account_db ? "true" : "false", m->meter_db ? "true" : "false", m->control_db ? "true" : "false", m->missing_owner_dbs,
        m->published_sources, m->sources_with_publisher_identity, m->retained_items, m->retained_sources, m->retained_publishers, m->retained_source_identity_gaps,
        bps(m->published_sources - m->retained_source_identity_gaps, m->published_sources),
        m->eligible_publishers, m->addressed_publishers, m->publisher_address_gaps, m->subject_bundles, m->subject_readers, m->subject_actions, m->subject_scenes,
        m->addressed_publishers_without_bundle, m->addressed_publishers_without_reader, bps(m->addressed_publishers, m->eligible_publishers),
        m->admitted_channels, m->admitted_published_sources, m->admitted_published_sources_with_reader, m->admitted_published_sources_without_reader,
        bps(m->admitted_published_sources_with_reader, m->admitted_published_sources),
        m->program_instances, m->source_collections, m->program_subjects_without_collection, bps(m->program_instances - m->program_subjects_without_collection, m->program_instances),
        m->account_tasks, m->account_runs, m->account_steps, m->account_outputs, m->inbox_states, m->inbox_operations, m->meter_events, m->meter_rates, m->meter_unpriced_events,
        m->account_index_current ? "true" : "false",
        m->evidence_relations, m->foreign_observations, m->frontier_layers, m->measured_invariants, m->proven_invariants, m->challenged_invariants, m->known_noncauses,
        m->evidence_sync_considered, m->evidence_sync_exact, m->evidence_sync_unresolved, m->evidence_sync_current ? "true" : "false",
        m->lake_measured ? "true" : "false", m->lake_roots, m->lake_roots_with_identity, bps(m->lake_roots_with_identity, m->lake_roots),
        m->identity_gaps_open, m->items_with_subject, bps(m->items_with_subject, m->retained_items), m->lake_subjects,
        m->lake_subjects_with_site, m->lake_subjects_with_reader, m->lake_subjects_with_site_and_reader, m->lake_sites_without_reader,
        m->surface_scenes, m->maturity_debt, m->maturity_debt,
        axis_identity(m), axis_address(m), axis_material(m), axis_subject(m), axis_program(m), axis_surface(m), axis_action(m), axis_account(m), axis_evidence(m),
        cut, (m->maturity_debt == 0 && cut == 0) ? "true" : "false");
}
#endif
