#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state=${BONFYRE_STATE_DIR:-"${TMPDIR:-/tmp}/bonfyre-completion"}/completion
# A requirement can be rerun against the same controller root.  Keep every
# Fabric invocation isolated so mission identity remains an integrity check,
# rather than masking collisions in the mission table.
run_suffix="$(date -u +%Y%m%dT%H%M%SZ)-$$"
matrix="$state/command-evidence-${run_suffix}.tsv"

# A generic executor may not retain a legacy command-specific dispatch.
if rg -q 'strcmp\(op, "command\.hash"\)' "$root/engine/core/src"; then
  echo 'legacy hash-only execution dispatch remains' >&2
  exit 1
fi

# Build the matrix from a real Fabric runtime before assessing coverage.  The
# matrix will deliberately leave unexecuted commands non-promoted.
runtime="${BONFYRE_STATE_DIR:-"${TMPDIR:-/tmp}/bonfyre-completion"}/runtime/major-${run_suffix}"
make -C "$root/cmd/BonfyreIngest" >/dev/null
make -C "$root/cmd/BonfyreMediaPrep" >/dev/null
make -C "$root/cmd/BonfyreBrief" >/dev/null
make -C "$root/cmd/BonfyreNarrate" >/dev/null
make -C "$root/cmd/BonfyreRender" >/dev/null
make -C "$root/cmd/BonfyreQwenFPQ" >/dev/null
make -C "$root/cmd/BonfyreQuant" >/dev/null
make -C "$root/cmd/BonfyreMoE" >/dev/null
make -C "$root/cmd/BonfyreGraph" >/dev/null
make -C "$root/cmd/BonfyreCMS" >/dev/null
make -C "$root/cmd/BonfyreQueue" >/dev/null
make -C "$root/cmd/BonfyreMeter" >/dev/null
make -C "$root/cmd/BonfyreWire" >/dev/null
make -C "$root/cmd/BonfyreAuth" >/dev/null
make -C "$root/cmd/BonfyreProject" >/dev/null
make -C "$root/cmd/BonfyreSpace" >/dev/null
BONFYRE_STATE_DIR="$runtime" sh "$root/tests/fabric_smoke.sh" >/dev/null
fabric="$root/programs/bonfyre/bonfyre"
# Every input below is material somebody actually has, named by the caller.
# Rows, a PDF, a tone and a prompt typed into this script used to stand in for
# them, and the families were certified on those.
: "${BONFYRE_TEST_CSV:?set BONFYRE_TEST_CSV to a real table}"
: "${BONFYRE_TEST_PDF:?set BONFYRE_TEST_PDF to a real PDF}"
: "${BONFYRE_TEST_AUDIO:?set BONFYRE_TEST_AUDIO to a real recording}"
: "${BONFYRE_TEST_DOCUMENT:?set BONFYRE_TEST_DOCUMENT to a real text document}"
: "${BONFYRE_TEST_MODEL:?set BONFYRE_TEST_MODEL to a real .safetensors model}"
fixture="$BONFYRE_TEST_CSV"
document_uri=$(BONFYRE_STATE_DIR="$runtime/state" "$fabric" artifact ingest "$fixture" text/csv | sed -n 's/^uri=//p')
BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create document-table >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add document-table normalize command.ingest "$document_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run document-table | grep -q 'node=normalize status=complete output=bonfyre://artifact/'

# A real PDF exercises the document extraction path.
pdf="$BONFYRE_TEST_PDF"
pdf_uri=$(BONFYRE_STATE_DIR="$runtime/state" "$fabric" artifact ingest "$pdf" application/pdf | sed -n 's/^uri=//p')
BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create pdf-extract >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add pdf-extract extract command.ingest "$pdf_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run pdf-extract | grep -q 'node=extract status=complete output=bonfyre://artifact/'

malformed="$runtime/malformed.pdf"
printf 'not a PDF' >"$malformed"
if "$root/cmd/BonfyreIngest/bonfyre-ingest" "$malformed" "$runtime/malformed-output"; then
  echo 'malformed PDF was accepted as a typed document' >&2
  exit 1
fi

# Exercise the media command through its own contract and real ffmpeg-backed
# normalization.  The output is a bounded, content-addressed WAV artifact.
audio="$BONFYRE_TEST_AUDIO"
audio_uri=$(BONFYRE_STATE_DIR="$runtime/state" "$fabric" artifact ingest "$audio" audio/wav | sed -n 's/^uri=//p')
BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create media-normalize >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add media-normalize normalize command.mediaprep "$audio_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run media-normalize | grep -q 'node=normalize status=complete output=bonfyre://artifact/'

render_input="$BONFYRE_TEST_DOCUMENT"
render_uri=$(BONFYRE_STATE_DIR="$runtime/state" "$fabric" artifact ingest "$render_input" text/plain | sed -n 's/^uri=//p')
BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create render-artifact >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add render-artifact render command.render "$render_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run render-artifact | grep -q 'node=render status=complete output=bonfyre://artifact/'

# Model inference is a governed native-FP16 Qwen route.  The output token and
# its run manifest are produced by the actual sampler; this is deliberately
# not a CLI health check or a fixture substitution.
model_prompt="$runtime/model-prompt.txt"
printf 'Write exactly one valid Blender Python line that creates a cube.\n' >"$model_prompt"
model_uri=$(BONFYRE_STATE_DIR="$runtime/state" "$fabric" artifact ingest "$model_prompt" text/plain | sed -n 's/^uri=//p')
BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create model-inference >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add model-inference generate command.model "$model_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run model-inference | grep -q 'node=generate status=complete output=bonfyre://artifact/'

BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create fpq-quant-prepare >/dev/null
# Quantization is measured on a model, not on the prompt the sampler was given.
quant_uri=$(BONFYRE_STATE_DIR="$runtime/state" "$fabric" artifact ingest "$BONFYRE_TEST_MODEL" application/octet-stream | sed -n 's/^uri=//p')
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add fpq-quant-prepare roundtrip command.quant "$quant_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run fpq-quant-prepare | grep -q 'node=roundtrip status=complete output=bonfyre://artifact/'

BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create embedding-vector >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add embedding-vector encode command.embed "$render_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run embedding-vector | grep -q 'node=encode status=complete output=bonfyre://artifact/'

BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create graph-store-query >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add graph-store-query mutate-query command.graph "$render_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run graph-store-query | grep -q 'node=mutate-query status=complete output=bonfyre://artifact/'

BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create cms-api >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add cms-api schema command.cms "$render_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run cms-api | grep -q 'node=schema status=complete output=bonfyre://artifact/'

BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create queue-flow >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add queue-flow enqueue command.queue "$render_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run queue-flow | grep -q 'node=enqueue status=complete output=bonfyre://artifact/'

BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create meter-ledger >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add meter-ledger record command.meter "$render_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run meter-ledger | grep -q 'node=record status=complete output=bonfyre://artifact/'

# command.wire's contract (wire_ingest_v1) ingests a packet capture. It is given
# a real one: BONFYRE_TEST_PCAP names a capture somebody recorded. A list of
# described flows used to be fed here, and the organ no longer reads those.
: "${BONFYRE_TEST_PCAP:?set BONFYRE_TEST_PCAP to a real packet capture}"
wire_uri=$(BONFYRE_STATE_DIR="$runtime/state" "$fabric" artifact ingest "$BONFYRE_TEST_PCAP" application/vnd.tcpdump.pcap | sed -n 's/^uri=//p')
BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create transport-wire >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add transport-wire doctor command.wire "$wire_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run transport-wire | grep -q 'node=doctor status=complete output=bonfyre://artifact/'

BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create auth-gate >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add auth-gate signup command.auth "$render_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run auth-gate | grep -q 'node=signup status=complete output=bonfyre://artifact/'

BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create frappe-application >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add frappe-application context command.project "$render_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run frappe-application | grep -q 'node=context status=complete output=bonfyre://artifact/'

BONFYRE_STATE_DIR="$runtime/state" "$fabric" mission create agent-project-space >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work add agent-project-space put command.space "$render_uri" >/dev/null
BONFYRE_STATE_DIR="$runtime/state" "$fabric" work run agent-project-space | grep -q 'node=put status=complete output=bonfyre://artifact/'
BONFYRE_STATE_DIR="$runtime/state" BONFYRE_COMMAND_EVIDENCE="$matrix" "$root/scripts/bonfyre-command-evidence" >/dev/null
expected_header='operator	family	binding_state	contract_generation	workload_command	input_artifact	output_artifact	event_id	receipt_id	exit_status	quality_result	maturity'
actual_header=$(head -n 1 "$matrix")
[ "$actual_header" = "$expected_header" ] || { echo 'invalid command evidence header' >&2; exit 1; }
[ "$(awk -F '\t' 'NR > 1 { seen[$1] = 1 } END { print length(seen) }' "$matrix")" -eq 93 ] || { echo 'command evidence does not contain 93 unique identities' >&2; exit 1; }
[ "$(awk 'NR > 1 { rows++ } END { print rows + 0 }' "$matrix")" -eq 93 ] || { echo 'command evidence has duplicate rows' >&2; exit 1; }
for family in artifact_intake_archive document_pdf_table audio_video_transcript render_pack_publish model_inference fpq_quant_prepare embedding_vector graph_store_query cms_api queue_flow_pipeline meter_economy_ledger_time_finance_control transport_quic_moq frappe_application agent_project_space auth_gate; do
  awk -F '\t' -v family="$family" 'NR > 1 && $2 == family && $5 != "" && $6 ~ /^bonfyre:\/\/artifact\// && $7 ~ /^bonfyre:\/\/artifact\// && $8 != "" && $9 != "" && $10 == "0" && $11 != "not_evaluated" && $11 != "unknown" && ($12 == "workload_proven" || $12 == "quality_proven") { found=1 } END { exit found ? 0 : 1 }' "$matrix" || { echo "missing executable evidence for $family" >&2; exit 1; }
done
