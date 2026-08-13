#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

mission=substrate-capabilities
"$requirement_fabric" mission create "$mission" >/dev/null
families='artifact_intake_archive document_pdf_table audio_video_transcript render_pack_publish model_inference fpq_quant_prepare embedding_vector graph_store_query cms_api queue_flow_pipeline meter_economy_ledger_time_finance_control transport_quic_moq frappe_application agent_project_space'
node_number=0
for family in $families; do
  node_number=$((node_number + 1))
  node=$(printf 'substrate-%02d' "$node_number")
  "$requirement_fabric" work add "$mission" "$node" core.intake --family "$family" >/dev/null
done

node_number=0
for family in $families; do
  node_number=$((node_number + 1))
  node=$(printf 'substrate-%02d' "$node_number")
  claim=$("$requirement_fabric" work claim-next substrate-worker --lease-ms 30000 --family "$family")
  printf '%s\n' "$claim" | grep -q "node_id=$node"
  token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
  attempt=$(printf '%s\n' "$claim" | sed -n 's/^attempt=//p')
  "$requirement_fabric" work complete "$mission" "$node" substrate-worker "$token" --attempt "$attempt" >/dev/null
done

[ "$(sqlite3 "$requirement_db" "SELECT count(DISTINCT family) FROM workgraph_nodes WHERE mission_id='$mission' AND status='complete';")" -eq 14 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM workgraph_attempts WHERE mission_id='$mission' AND outcome='complete';")" -eq 14 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM workgraph_transitions WHERE mission_id='$mission' AND to_status='complete';")" -eq 14 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM events WHERE mission_id='$mission' AND status='completed';")" -eq 14 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM receipts WHERE subject_kind='workgraph-transition' AND json_extract(payload,'$.mission_id')='$mission';")" -eq 42 ]
"$requirement_fabric" mission show "$mission" | grep -q '^status=complete$'
echo '14 substrate capabilities: passed'
