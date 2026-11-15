/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREWIRE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREWIRE_H
/* cmd/BonfyreWire/src/wire.c */
#define BF_STORE_WIRE_WIRE_CAPTURES \
    "CREATE TABLE IF NOT EXISTS wire_captures( capture_id TEXT PRIMARY KEY, source_kind TEXT, source_path TEXT, interface_name TEXT, authorized INTEGER, dumb_device INTEGER DEFAULT 0, metadata_only INTEGER, payload_enabled INTEGER, unencrypted_only INTEGER, save_raw INTEGER, packet_count INTEGER, byte_count INTEGER, status TEXT, created_at TEXT);" \
    "CREATE TABLE IF NOT EXISTS wire_flows( capture_id TEXT NOT NULL, flow_id TEXT NOT NULL, src_ip TEXT, dst_ip TEXT, src_port INTEGER, dst_port INTEGER, l4_proto TEXT, app_proto TEXT, media_kind TEXT, domain TEXT, dns_name TEXT, sni TEXT, process_name TEXT, first_ts TEXT, last_ts TEXT, packets INTEGER, bytes INTEGER, encrypted INTEGER, probable_media INTEGER, probable_control INTEGER, hls_manifest INTEGER, dash_manifest INTEGER, websocket_media INTEGER, pipeline_trigger INTEGER, vendor_dependency TEXT, suggested_integration_point TEXT, payload_buffer_path TEXT, PRIMARY KEY(capture_id, flow_id));" \
    "CREATE TABLE IF NOT EXISTS wire_meter( capture_id TEXT, flow_id TEXT, record_hash TEXT PRIMARY KEY, record_json TEXT);" \
    "CREATE TABLE IF NOT EXISTS wire_scaling( capture_id TEXT, flow_id TEXT, event_hash TEXT PRIMARY KEY, event_json TEXT);" \
    "CREATE TABLE IF NOT EXISTS wire_route( capture_id TEXT, flow_id TEXT, event_hash TEXT PRIMARY KEY, event_json TEXT);" \
    "CREATE TABLE IF NOT EXISTS wire_devices( device_id TEXT PRIMARY KEY, capture_id TEXT, ip TEXT, vendor_hint TEXT, device_class TEXT, open_ports TEXT, protocol_fingerprint TEXT, media_capable INTEGER, asr_candidate INTEGER, encrypted_only INTEGER, total_flows INTEGER, total_bytes INTEGER, total_packets INTEGER, first_seen TEXT, last_seen TEXT, bonfyre_chain TEXT, artifact_id TEXT);" \
    "CREATE INDEX IF NOT EXISTS idx_wire_devices_capture ON wire_devices(capture_id);"
/* cmd/BonfyreWire/src/wire.c */
#define BF_STORE_WIRE_SPACES \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS spaces(  name TEXT PRIMARY KEY,  created INTEGER NOT NULL,  accessed INTEGER NOT NULL,  mode TEXT NOT NULL DEFAULT 'rw');" \
    "CREATE TABLE IF NOT EXISTS entries(  space TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,  key TEXT NOT NULL,  value BLOB NOT NULL,  written INTEGER NOT NULL,  ttl_s INTEGER,  PRIMARY KEY(space,key));" \
    "CREATE TABLE IF NOT EXISTS attachments(  space TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,  stage TEXT NOT NULL,  attached INTEGER NOT NULL,  detached INTEGER,  PRIMARY KEY(space,stage));" \
    "CREATE INDEX IF NOT EXISTS idx_entries_space ON entries(space);"
#endif
