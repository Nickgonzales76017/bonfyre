#include "wire.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void usage(void) {
    fprintf(stderr,
            "BonfyreWire — consent-based network event layer\n\n"
            "Usage:\n"
            "  bonfyre-wire doctor [--root DIR]\n"
            "  bonfyre-wire listen --interface <iface> --authorized --metadata-only [--root DIR]\n"
            "  bonfyre-wire listen --interface <iface> --dumb-device --metadata-only [--root DIR]\n"
            "  bonfyre-wire ingest-pcap <file> --authorized [--root DIR] [--metadata-only]\n"
            "  bonfyre-wire ingest-pcap <file> --authorized --payload --unencrypted-only [--root DIR]\n"
            "  bonfyre-wire ingest-pcap <file> --dumb-device [--root DIR]\n"
            "  bonfyre-wire space-export <capture_id> --space NAME [--root DIR]\n"
            "  bonfyre-wire flows <capture_id> [--root DIR]\n"
            "  bonfyre-wire media-detect <capture_id> [--root DIR]\n"
            "  bonfyre-wire meter <capture_id> [--root DIR]\n"
            "  bonfyre-wire scale <capture_id> [--root DIR]\n"
            "  bonfyre-wire route <capture_id> [--root DIR]\n"
            "  bonfyre-wire report <capture_id> [--root DIR]\n"
            "  bonfyre-wire probe <capture_id> [--root DIR]\n"
            "  bonfyre-wire artifacts <capture_id> [--root DIR]\n"
            "  bonfyre-wire recipe <capture_id> [--root DIR]\n\n"
            "Discovery (operator-owned / dumb-device sandbox):\n"
            "  probe      — fingerprint all IPs in a capture: port patterns, protocol\n"
            "               fingerprint, vendor hint, device class, bonfyre chain suggestion\n"
            "  artifacts  — materialize devices as canonical BfArtifact JSON files;\n"
            "               output feeds directly into bonfyre ingest/index/stitch\n"
            "  recipe     — generate stitch-compatible pipeline recipe; pipe into\n"
            "               bonfyre stitch plan / compile for zero-touch execution\n\n"
            "Safety:\n"
            "  - every capture requires --authorized or --dumb-device\n"
            "  - --dumb-device: operator-owned infrastructure with no consent capability;\n"
            "    ownership constitutes implicit authorization; locked to metadata-only mode\n"
            "  - metadata-only is the default and recommended mode\n"
            "  - payload reconstruction requires all of: --payload --authorized --unencrypted-only\n"
            "    (--dumb-device cannot be combined with --payload)\n"
            "  - encrypted payload reconstruction is refused\n\n"
            "Environment:\n"
            "  - BONFYRE_LAB_UNRESTRICTED=1 may default authorized offline ingest to payload mode\n"
            "    only when --unencrypted-only is also present; it does not bypass authorization or encryption refusal\n");
}

static int parse_common(int argc, char **argv,
                        const char **root, int *authorized, int *dumb_device,
                        int *metadata_only, int *payload, int *unencrypted_only,
                        int *save_raw, const char **iface, const char **path_or_id,
                        const char **space_name) {
    *root = "layeros/state";
    *authorized = 0;
    *dumb_device = 0;
    *metadata_only = 1;
    *payload = 0;
    *unencrypted_only = 0;
    *save_raw = 0;
    *iface = NULL;
    *path_or_id = NULL;
    *space_name = NULL;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) *root = argv[++i];
        else if (strcmp(argv[i], "--authorized") == 0) *authorized = 1;
        else if (strcmp(argv[i], "--dumb-device") == 0) *dumb_device = 1;
        else if (strcmp(argv[i], "--metadata-only") == 0) *metadata_only = 1;
        else if (strcmp(argv[i], "--payload") == 0) { *payload = 1; *metadata_only = 0; }
        else if (strcmp(argv[i], "--unencrypted-only") == 0) *unencrypted_only = 1;
        else if (strcmp(argv[i], "--save-raw") == 0) *save_raw = 1;
        else if (strcmp(argv[i], "--interface") == 0 && i + 1 < argc) *iface = argv[++i];
        else if (strcmp(argv[i], "--space") == 0 && i + 1 < argc) *space_name = argv[++i];
        else if (argv[i][0] != '-' && !*path_or_id) *path_or_id = argv[i];
    }
    return 0;
}

static int env_truthy(const char *name) {
    const char *v = getenv(name);
    if (!v || !v[0]) return 0;
    return strcmp(v, "1") == 0 || strcmp(v, "true") == 0 || strcmp(v, "TRUE") == 0 || strcmp(v, "yes") == 0;
}

static void apply_env_policy(const char *cmd, int authorized, int *metadata_only, int *payload, int unencrypted_only) {
    if (!cmd || !metadata_only || !payload) return;
    if (!env_truthy("BONFYRE_LAB_UNRESTRICTED")) return;
    if (!authorized || !unencrypted_only || *payload) return;
    if (strcmp(cmd, "ingest-pcap") != 0 && strcmp(cmd, "listen") != 0) return;
    *payload = 1;
    *metadata_only = 0;
}

/* dumb_device: operator-owned infrastructure with no consent capability.
 * Ownership constitutes implicit authorization but is locked to metadata-only;
 * it cannot be combined with --payload. */
static int enforce_authorized(int authorized, int dumb_device, int payload) {
    if (!authorized && !dumb_device) {
        fprintf(stderr, "bonfyre-wire: refusing operation without --authorized or --dumb-device\n");
        return 1;
    }
    if (dumb_device && payload) {
        fprintf(stderr, "bonfyre-wire: --dumb-device cannot be combined with --payload (no consent capability)\n");
        return 1;
    }
    return 0;
}

static int enforce_payload_flags(int payload, int authorized, int unencrypted_only) {
    if (payload && (!authorized || !unencrypted_only)) {
        fprintf(stderr, "bonfyre-wire: payload reconstruction requires --payload --authorized --unencrypted-only\n");
        return 1;
    }
    return 0;
}

int main(int argc, char **argv) {
    const char *cmd, *root, *iface, *path_or_id, *space_name;
    int authorized, dumb_device, metadata_only, payload, unencrypted_only, save_raw;
    if (argc < 2) { usage(); return 1; }
    cmd = argv[1];
    parse_common(argc, argv, &root, &authorized, &dumb_device, &metadata_only, &payload, &unencrypted_only, &save_raw, &iface, &path_or_id, &space_name);
    apply_env_policy(cmd, authorized, &metadata_only, &payload, unencrypted_only);

    if (strcmp(cmd, "doctor") == 0) {
        return bf_wire_doctor_json(root);
    }

    if (strcmp(cmd, "listen") == 0) {
        BfWireCapture capture;
        if (enforce_authorized(authorized, dumb_device, payload) != 0) return 1;
        if (!iface || !iface[0]) {
            fprintf(stderr, "bonfyre-wire listen: requires --interface <iface>\n");
            return 1;
        }
        if (enforce_payload_flags(payload, authorized, unencrypted_only) != 0) return 1;
        memset(&capture, 0, sizeof(capture));
        snprintf(capture.source_kind, sizeof(capture.source_kind), "live");
        snprintf(capture.interface_name, sizeof(capture.interface_name), "%s", iface);
        capture.authorized = authorized;
        capture.dumb_device = dumb_device;
        capture.metadata_only = metadata_only;
        capture.payload_enabled = payload;
        capture.unencrypted_only = unencrypted_only;
        capture.save_raw = save_raw;
        snprintf(capture.status, sizeof(capture.status), "%s", isatty(STDIN_FILENO) ? "listening_stub_ready" : "ingesting_stream");
        bf_wire_capture_create(root, &capture);
        if (!isatty(STDIN_FILENO)) {
            if (bf_wire_ingest_stream(stdin, root, &capture, payload) != 0) return 1;
            snprintf(capture.status, sizeof(capture.status), "ingested_stream");
            bf_wire_capture_update_summary(root, &capture);
        }
        printf("{\"capture_id\":\"%s\",\"status\":\"%s\",\"mode\":\"%s\",\"interface\":\"%s\",\"note\":\"live capture requires an authorized packet source or external adapter; synthetic stdin events are supported in this build\"}\n",
               capture.capture_id, capture.status, metadata_only ? "metadata-only" : "payload", capture.interface_name);
        return 0;
    }

    if (strcmp(cmd, "ingest-pcap") == 0) {
        BfWireCapture capture;
        char raw_copy[PATH_MAX];
        if (!path_or_id) {
            fprintf(stderr, "bonfyre-wire ingest-pcap: requires <file>\n");
            return 1;
        }
        if (enforce_authorized(authorized, dumb_device, payload) != 0) return 1;
        if (enforce_payload_flags(payload, authorized, unencrypted_only) != 0) return 1;
        memset(&capture, 0, sizeof(capture));
        snprintf(capture.source_kind, sizeof(capture.source_kind), "pcap");
        snprintf(capture.source_path, sizeof(capture.source_path), "%s", path_or_id);
        capture.authorized = authorized;
        capture.dumb_device = dumb_device;
        capture.metadata_only = metadata_only;
        capture.payload_enabled = payload;
        capture.unencrypted_only = unencrypted_only;
        capture.save_raw = save_raw;
        snprintf(capture.status, sizeof(capture.status), "ingesting");
        bf_wire_capture_create(root, &capture);
        if (save_raw) {
            if (bf_wire_save_raw_copy(root, path_or_id, raw_copy, sizeof(raw_copy)) == 0) {
                snprintf(capture.source_path, sizeof(capture.source_path), "%s", raw_copy);
            }
        }
        if (bf_wire_ingest_file(root, &capture, path_or_id, payload) != 0) {
            fprintf(stderr, "bonfyre-wire ingest-pcap: could not ingest %s "
                            "(not a readable pcap file or recognized synthetic JSON capture)\n",
                    path_or_id);
            return 1;
        }
        snprintf(capture.status, sizeof(capture.status), "ingested");
        bf_wire_capture_update_summary(root, &capture);
        printf("{\"capture_id\":\"%s\",\"packet_count\":%lld,\"byte_count\":%lld,\"mode\":\"%s\",\"source\":",
               capture.capture_id, capture.packet_count, capture.byte_count, metadata_only ? "metadata-only" : "payload");
        printf("\"%s\"}\n", path_or_id);
        return 0;
    }

    if (strcmp(cmd, "space-export") == 0) {
        if (!path_or_id) {
            fprintf(stderr, "bonfyre-wire space-export: requires <capture_id>\n");
            return 1;
        }
        if (!space_name || !space_name[0]) {
            fprintf(stderr, "bonfyre-wire space-export: requires --space NAME\n");
            return 1;
        }
        return bf_wire_space_export_capture(root, path_or_id, space_name);
    }

    if (!path_or_id) {
        usage();
        return 1;
    }
    if (strcmp(cmd, "flows") == 0)        return bf_wire_emit_flows_json(root, path_or_id);
    if (strcmp(cmd, "media-detect") == 0) return bf_wire_emit_media_detect_json(root, path_or_id);
    if (strcmp(cmd, "meter") == 0)         return bf_wire_emit_meter_json(root, path_or_id);
    if (strcmp(cmd, "scale") == 0)         return bf_wire_emit_scale_json(root, path_or_id);
    if (strcmp(cmd, "route") == 0)         return bf_wire_emit_route_json(root, path_or_id);
    if (strcmp(cmd, "report") == 0)        return bf_wire_write_report(root, path_or_id);
    /* ── Discovery commands — full bonfyre pyramid from sandbox observation ── */
    if (strcmp(cmd, "probe") == 0)         return bf_wire_probe(root, path_or_id);
    if (strcmp(cmd, "artifacts") == 0)     return bf_wire_artifacts(root, path_or_id);
    if (strcmp(cmd, "recipe") == 0)        return bf_wire_recipe(root, path_or_id);

    usage();
    return 1;
}
