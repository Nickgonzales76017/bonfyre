#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap
make -C "$requirement_root/cmd/BonfyreIngest" >/dev/null
make -C "$requirement_root/cmd/BonfyreMediaPrep" >/dev/null

csv="$requirement_runtime/ledger.csv"
pdf="$requirement_runtime/operating-procedure.pdf"
audio="$requirement_runtime/tone.wav"
printf 'name,amount\nalpha,10\nbeta,20\n' >"$csv"
printf '%%PDF-1.4\n1 0 obj\n<<>>\nstream\n(Operating Procedure)\n(Alpha,10)\nendstream\nendobj\n%%EOF\n' >"$pdf"
ffmpeg -v error -f lavfi -i sine=frequency=440:duration=0.05 -ac 1 -ar 8000 "$audio"

csv_uri=$(fabric_test_ingest "$csv" text/csv)
pdf_uri=$(fabric_test_ingest "$pdf" application/pdf)
audio_uri=$(fabric_test_ingest "$audio" audio/wav)

csv_run=$(fabric_test_run_node mixed-csv normalize command.ingest "$csv_uri")
pdf_run=$(fabric_test_run_node mixed-pdf extract command.ingest "$pdf_uri")
audio_run=$(fabric_test_run_node mixed-audio normalize command.mediaprep "$audio_uri")
printf '%s\n' "$csv_run" | grep -q 'status=complete output=bonfyre://artifact/'
printf '%s\n' "$pdf_run" | grep -q 'status=complete output=bonfyre://artifact/'
printf '%s\n' "$audio_run" | grep -q 'status=complete output=bonfyre://artifact/'

[ "$(sqlite3 "$requirement_db" "SELECT count(DISTINCT media_type) FROM artifacts WHERE uri IN ('$csv_uri','$pdf_uri','$audio_uri');")" -eq 3 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM events WHERE mission_id LIKE 'mixed-%' AND operator_id LIKE 'command.%' AND status='complete';")" -eq 3 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM execution_metrics m JOIN events e ON e.id=m.event_id WHERE e.mission_id LIKE 'mixed-%' AND m.bytes_in>0 AND m.bytes_out>0;")" -eq 3 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM receipts r JOIN events e ON e.receipt_id=r.id WHERE e.mission_id LIKE 'mixed-%' AND e.operator_id LIKE 'command.%' AND e.status='complete';")" -eq 3 ]

sqlite3 "$requirement_db" "SELECT receipt_id FROM events WHERE mission_id LIKE 'mixed-%' AND operator_id LIKE 'command.%' AND status='complete' ORDER BY mission_id;" |
while IFS= read -r receipt_id; do
  "$requirement_fabric" receipt show "$receipt_id" | grep -q '^content_hash=[0-9a-f]\{64\}$'
done

echo 'mixed artifact intake: passed'
