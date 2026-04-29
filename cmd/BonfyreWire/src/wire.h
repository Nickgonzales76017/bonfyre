#ifndef BONFYRE_WIRE_H
#define BONFYRE_WIRE_H

#include <limits.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct {
    char capture_id[65];
    char source_kind[32];
    char source_path[PATH_MAX];
    char interface_name[64];
    int authorized;
    int dumb_device;        /* 1 = operator-owned infrastructure with no consent capability;
                               ownership constitutes implicit authorization (metadata-only only) */
    int metadata_only;
    int payload_enabled;
    int unencrypted_only;
    int save_raw;
    long long packet_count;
    long long byte_count;
    char status[64];
    char created_at[32];
} BfWireCapture;

typedef struct {
    char flow_id[65];
    char src_ip[64];
    char dst_ip[64];
    int src_port;
    int dst_port;
    char l4_proto[16];
    char app_proto[32];
    char media_kind[32];
    char domain[256];
    char dns_name[256];
    char sni[256];
    char process_name[128];
    char first_ts[32];
    char last_ts[32];
    long long packets;
    long long bytes;
    int encrypted;
    int probable_media;
    int probable_control;
    int hls_manifest;
    int dash_manifest;
    int websocket_media;
    int pipeline_trigger;
    char vendor_dependency[128];
    char suggested_integration_point[128];
    char payload_buffer_path[PATH_MAX];
} BfWireFlow;

int bf_wire_ensure_schema(const char *root, char *db_path, size_t db_path_sz);
int bf_wire_capture_create(const char *root, BfWireCapture *capture);
int bf_wire_capture_update_summary(const char *root, const BfWireCapture *capture);
int bf_wire_save_raw_copy(const char *root, const char *src_path, char *out_path, size_t out_path_sz);
int bf_wire_ingest_file(const char *root, BfWireCapture *capture, const char *path, int payload_requested);
int bf_wire_ingest_stream(FILE *in, const char *root, BfWireCapture *capture, int payload_requested);
int bf_wire_emit_flows_json(const char *root, const char *capture_id);
int bf_wire_emit_media_detect_json(const char *root, const char *capture_id);
int bf_wire_emit_meter_json(const char *root, const char *capture_id);
int bf_wire_emit_scale_json(const char *root, const char *capture_id);
int bf_wire_emit_route_json(const char *root, const char *capture_id);
int bf_wire_write_report(const char *root, const char *capture_id);
int bf_wire_doctor_json(const char *root);
int bf_wire_space_export_capture(const char *root, const char *capture_id, const char *space_name);

/* ================================================================
 * Device-level discovery — sits above the flow model.
 *
 * Every IP observed across flows is fingerprinted into a BfWireDevice
 * profile: port pattern → device class, DNS/SNI pattern → vendor hint,
 * protocol fingerprint → bonfyre chain suggestion.
 *
 * Operator ownership (--dumb-device) is the key that unlocks this:
 * devices that can't consent are still fully utilizable by bonfyre
 * systems because the operator's ownership constitutes authorization.
 * ================================================================ */

typedef struct {
    char device_id[65];           /* SHA-256(ip + ports + class + capture_id) */
    char capture_id[65];
    char ip[64];
    char vendor_hint[64];         /* from DNS/SNI/mDNS pattern matching       */
    char device_class[32];        /* camera, speaker, sensor, voip_phone, ... */
    char open_ports[512];         /* comma-separated observed src+dst ports   */
    char protocol_fingerprint[256]; /* comma-separated app_protos             */
    int  media_capable;           /* 1 if any flow had probable_media=1       */
    int  asr_candidate;           /* 1 if RTP/SIP/RTSP/WebSocket observed     */
    int  encrypted_only;          /* 1 if ALL flows are encrypted             */
    long long total_flows;
    long long total_bytes;
    long long total_packets;
    char first_seen[32];
    char last_seen[32];
    char bonfyre_chain[256];      /* e.g. "ingest,hash,media-prep,transcribe,..." */
    char artifact_id[65];         /* populated by bf_wire_artifacts()         */
} BfWireDevice;

/* Deep device fingerprinting: aggregate all flows by src_ip, classify
 * each IP into a BfWireDevice profile, suggest bonfyre pipeline chain.
 * Outputs JSON device manifest to stdout. */
int bf_wire_probe(const char *root, const char *capture_id);

/* Materialize all discovered devices as canonical BfArtifact JSON files
 * under <root>/wire/artifacts/<capture_id>/device_<id>.json.
 * Uses libbonfyre BfArtifact contract — every downstream binary can ingest
 * these directly. Outputs artifact index JSON to stdout. */
int bf_wire_artifacts(const char *root, const char *capture_id);

/* Generate a stitch-compatible pipeline recipe JSON for all discovered
 * devices. Each device maps to its optimal bonfyre chain. Pipe directly
 * into `bonfyre stitch plan` or `bonfyre stitch compile`. */
int bf_wire_recipe(const char *root, const char *capture_id);

#endif
