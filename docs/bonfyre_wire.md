# Bonfyre Wire

`bonfyre wire` is a consent-based network event layer.

It is designed for owned or explicitly authorized networks only. Its default mode
is metadata-only capture and flow classification. It can turn authorized flow
metadata into:

- ASR pipeline trigger suggestions
- usage metering records
- dynamic scaling signals
- dependency and sovereignty reports

## Safety model

- `--authorized` is required for all live or offline capture actions.
- metadata-only is the default behavior.
- payload reconstruction requires all of:
  - `--payload`
  - `--authorized`
  - `--unencrypted-only`
- encrypted sessions are never decrypted and never reconstructed.
- no TLS/SRTP/DTLS/QUIC bypass
- no MITM
- no credential or token extraction
- no raw packet persistence unless `--save-raw` is explicitly passed

## Commands

```bash
bonfyre wire doctor
bonfyre wire listen --interface en0 --authorized --metadata-only
bonfyre wire ingest-pcap capture.pcap --authorized
bonfyre wire flows <capture_id>
bonfyre wire media-detect <capture_id>
bonfyre wire meter <capture_id>
bonfyre wire scale <capture_id>
bonfyre wire route <capture_id>
bonfyre wire report <capture_id>
```

## Offline-first workflow

```bash
bonfyre wire ingest-pcap cmd/BonfyreWire/fixtures/media_fixture.jsonl --authorized --root layeros/state
bonfyre wire flows <capture_id> --root layeros/state
bonfyre wire media-detect <capture_id> --root layeros/state
bonfyre wire meter <capture_id> --root layeros/state
bonfyre wire scale <capture_id> --root layeros/state
bonfyre wire route <capture_id> --root layeros/state
bonfyre wire report <capture_id> --root layeros/state
```

## Report outputs

`bonfyre wire report` writes:

- `wire_flows.jsonl`
- `media_candidates.json`
- `usage_meter.json`
- `scaling_events.jsonl`
- `dependency_map.json`
- `sovereignty_report.md`

under:

```text
<root>/wire/reports/<capture_id>/
```
