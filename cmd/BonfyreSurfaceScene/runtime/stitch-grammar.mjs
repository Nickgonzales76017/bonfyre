// Consolidated presentation vocabulary from 91 Google Stitch reference screens (2026-11-04).
// The examples are not source records, programs, grants, or publishable content.
export const STITCH_LOOKS = Object.freeze({
  editorial: Object.freeze({ground:"#0f1511", ink:"#dee4dd", muted:"#c5c9af", accent:"#d0fc4f", panel:"#1b211d", line:"#444935", display:"DM Sans", body:"DM Sans"}),
  paper: Object.freeze({ground:"#fff8f0", ink:"#1d1b16", muted:"#494740", accent:"#8e4e1e", panel:"#f3ede4", line:"#cbc6bd", display:"Newsreader", body:"DM Sans"}),
  electric: Object.freeze({ground:"#0a0a12", ink:"#eff7f4", muted:"#c4c4d2", accent:"#00ffcc", panel:"#151722", line:"#354451", display:"Sora", body:"Inter"})
});

// Topology preferences, not executable markup, queries or action grants.
// Native Scene regions, not Stitch screenshots, determine what actually exists.
export const STITCH_PATTERNS = Object.freeze({
  profile:      {shell:"identity", rhythm:["lead","shelf","feed","communities","list"], posture:"public-identity", density:"medium"},
  organization: {shell:"identity", rhythm:["lead","grid","communities","feed","list"], posture:"account-and-community", density:"high"},
  directory:    {shell:"index", rhythm:["lead","results","grid","list"], posture:"filtered-discovery", density:"high"},
  detail:       {shell:"record", rhythm:["lead","field","index","shelf","thread"], posture:"evidence-and-context", density:"medium"},
  discover:     {shell:"editorial", rhythm:["lead","shelf","grid","feed","index"], posture:"discovery", density:"medium"},
  reader:       {shell:"reading", rhythm:["lead","index","thread","list"], posture:"longform", density:"low"},
  connections:  {shell:"workspace", rhythm:["lead","table","results","list"], posture:"source-and-provider", density:"high"},
  workbench:    {shell:"workspace", rhythm:["lead","field","table","results","list"], posture:"authored-work", density:"very_high"},
  review:       {shell:"decision", rhythm:["lead","field","table","thread","list"], posture:"governed-decision", density:"high"},
  feed:         {shell:"stream", rhythm:["lead","feed","thread","list"], posture:"continuous-reading", density:"medium"},
  market:       {shell:"market", rhythm:["lead","grid","table","results","list"], posture:"offers-and-terms", density:"high"},
  media:        {shell:"player", rhythm:["lead","tracks","shelf","thread","list"], posture:"watch-and-listen", density:"medium"},
  spatial:      {shell:"immersive", rhythm:["map","field","grid","index"], posture:"spatial-inspection", density:"high"},
  device:       {shell:"device", rhythm:["lead","field","list"], posture:"responsive-transfer", density:"low"}
});
for (const value of Object.values(STITCH_PATTERNS)) {
  Object.freeze(value.rhythm);
  Object.freeze(value);
}

// Each reference chooses a reusable pattern; none supplies its fictional text or values.
const prototypeGroups = {
  profile: "aurekai_member_profile_living_feeds_agent_hub aurekai_operator_working_profile_jurisdiction aurekai_wayfarer_profile_the_murmuring_woods aurekai_people_credited_makers_sovereign_addresses",
  organization: "aurekai_organization_profile_workspaces_1 aurekai_organization_profile_workspaces_2 aurekai_guilds_hearth_care_culinary_ecological_commons aurekai_guilds_physical_verification_security_unconventional_networks aurekai_guilds_sovereign_transit_delivery_logistics_federations aurekai_living_neighborhoods_actors_physical_commons aurekai_internet_operators_living_circles aurekai_family_health_living_hearth_care_circle_mira_vane",
  directory: "aurekai_people_credited_makers_directory aurekai_100_operator_roles_directory_registry aurekai_local_hiring_apprenticeship_commons aurekai_degrees_of_proximity_companionship aurekai_relations_crm_family_trees_sub_accounts_vouch_network aurekai_multi_role_federation_cross_jurisdiction_delegations",
  detail: "9k_mile_2009_ferrari_430_scuderia_aurekai_detail_1 9k_mile_2009_ferrari_430_scuderia_aurekai_detail_2 aurekai_cinema_film_detail_guild_rights_mod_price_list aurekai_guild_71_heavy_machinery_engine_bore_inspection aurekai_monograph_no._04_the_typographic_commons_material_ledger_1482_1928",
  discover: "aurekai_discover aurekai_editorial_discover_hub aurekai_what_s_on aurekai_what_things_cost aurekai_what_things_cost_the_maternal_quant_ledger_collegiate_family_union aurekai_agrarian_common_market_guild_trade_index",
  reader: "aurekai_source_library_reading_room aurekai_knowledge_wiki_living_charters_hypertext_commons_sops aurekai_epistemic_almanac_field_registries_provenance_choirs_the_un_indexed aurekai_the_sovereign_estate_abstract_manifesto aurekai_living_feeds_distraction_free_zen_reading_mode",
  connections: "aurekai_connections_identity_studio aurekai_carrier_protocols_multi_identity_hub aurekai_gmail_connection_sync_settings aurekai_gmail_oauth_authorization_modal aurekai_bluesky_at_protocol_car_repository_sync aurekai_links_known_sources_curated_ingestion_hub aurekai_websub_hub_webhook_subscription_setup aurekai_websub_active_lease_renewal_hmac_challenge_debug_console aurekai_websub_rss_push_mcp_a2a_protocol_gateway aurekai_tool_packs_workspace_setup",
  workbench: "aurekai_atelier_cms_bonfyre_policy_workbench aurekai_atelier_cms_spatial_archival_studio aurekai_publishing_cms_artboard_imposition_editor aurekai_linotype_monotype_hot_metal_casting_setup aurekai_atelier_cms_mcp_tool_definition_studio aurekai_mcp_client_inspector_tool_execution_sandbox aurekai_mcp_tool_sandbox_execution_runner aurekai_inter_guild_co_job_work_order_composer aurekai_agent_permissions_feed_syndication_studio aurekai_newsletter_typography_custom_css_studio aurekai_downstream_works_manifestation_assemblers",
  review: "aurekai_apprentice_portal_leo_vane_bench_proofs_escrow_allowance aurekai_autonomous_mutual_surety_escrow_dashboard aurekai_client_milestone_release_multi_sig_modal aurekai_guild_steward_mobile_attestation aurekai_master_sign_off_bench_proof_review_leo_vane aurekai_operator_appointment_vouching_desk aurekai_peer_dispute_chamber_hearing_room_disp_042 aurekai_cryptographic_ratification_ledger_key_ceremony aurekai_physical_liaison_field_tools_dispatch",
  feed: "aurekai_living_feeds_reader_mcp_coprocessor aurekai_mobile_living_feeds_reader_companion aurekai_add_new_feed_ingress_transform_modal",
  market: "aurekai_local_trade_selling_barter_radius aurekai_guilds_sponsorships_patronage_grants aurekai_operator_compensation_mutual_dividends aurekai_seasonal_barter_ledger_grain_assaying_laboratory aurekai_apprenticeships_craft_lms_guild_curriculums_shop_hours aurekai_household_pantry_seasonal_foraging_preservation_calendar",
  media: "aurekai_aural_translation_synthesizer_player_for_the_lathe_score aurekai_cinema_living_moving_image_commons_guild_catalog aurekai_listen aurekai_music_sovereign_audio_soundfield_player aurekai_tv_roku_10_foot_living_commons_cinema aurekai_watch_listen_cinema_projection_operators_living_moving_image_commons",
  spatial: "aurekai_spatial_cartography_deep_planetary_instrumentarium aurekai_spatial_orrery_armillary_planetary_instrumentarium aurekai_high_level_system_topology_architecture_map aurekai_interactive_topological_node_inspector_detail_modal aurekai_generative_visual_graphic_notation_monograph shader three.js",
  device: "aurekai_desktop_p2p_enclave_migration_sender aurekai_mobile_key_enrollment_hardware_attestation aurekai_mobile_p2p_enclave_migration_receiver aurekai_offline_sqlite_local_enclave_backup_migration aurekai_watchos_wrist_enclave_living_feeds_glance"
};
const references = {};
for (const [pattern, group] of Object.entries(prototypeGroups)) {
  for (const reference of group.split(/\s+/)) {
    if (references[reference]) throw new Error("duplicate Stitch reference: " + reference);
    references[reference] = pattern;
  }
}
export const STITCH_REFERENCES = Object.freeze(references);
export const STITCH_REFERENCE_COUNT = Object.keys(references).length;
if (STITCH_REFERENCE_COUNT !== 91) throw new Error("Stitch reference coverage changed: " + STITCH_REFERENCE_COUNT);
