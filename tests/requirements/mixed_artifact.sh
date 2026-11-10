#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap
make -C "$requirement_root/cmd/BonfyreIngest" >/dev/null
make -C "$requirement_root/cmd/BonfyreMediaPrep" >/dev/null

csv="$requirement_runtime/ledger.csv"
pdf="$requirement_runtime/operating-procedure.pdf"
audio="$requirement_runtime/tone.wav"
misnamed_image="$requirement_runtime/misnamed-flyer.png"
# The three artifacts are real ones the caller names. A typed-in table, a PDF
# written with printf and a 50 millisecond tone used to stand in for them.
: "${BONFYRE_TEST_CSV:?set BONFYRE_TEST_CSV to a real table}"
: "${BONFYRE_TEST_PDF:?set BONFYRE_TEST_PDF to a real PDF}"
: "${BONFYRE_TEST_AUDIO:?set BONFYRE_TEST_AUDIO to a real recording}"
cp "$BONFYRE_TEST_CSV" "$csv"
cp "$BONFYRE_TEST_PDF" "$pdf"
cp "$BONFYRE_TEST_AUDIO" "$audio"
magick -size 4x4 xc:white -set comment 'source-only metadata' jpeg:"$misnamed_image"

"$requirement_root/cmd/BonfyreIngest/bonfyre-ingest" "$misnamed_image" "$requirement_runtime/misnamed-intake" >/dev/null
grep -q '"source_media_type": "image/jpeg"' "$requirement_runtime/misnamed-intake/intake-manifest.json"
grep -q '"normalized_path": "normalized.jpg"' "$requirement_runtime/misnamed-intake/intake-manifest.json"
grep -q '"normalized_media_type": "image/jpeg"' "$requirement_runtime/misnamed-intake/intake-manifest.json"
grep -q '"media_type": "image/jpeg"' "$requirement_runtime/misnamed-intake/intake-manifest.json"
grep -q '"media_type": "image/jpeg"' "$requirement_runtime/misnamed-intake/artifact.json"

csv_uri=$(fabric_test_ingest "$csv" text/csv)
pdf_uri=$(fabric_test_ingest "$pdf" application/pdf)
audio_uri=$(fabric_test_ingest "$audio" audio/wav)
misnamed_uri=$(fabric_test_ingest "$misnamed_image" image/jpeg)

csv_run=$(fabric_test_run_node mixed-csv normalize command.ingest "$csv_uri")
pdf_run=$(fabric_test_run_node mixed-pdf extract command.ingest "$pdf_uri")
audio_run=$(fabric_test_run_node mixed-audio normalize command.mediaprep "$audio_uri")
misnamed_run=$(fabric_test_run_node mixed-misnamed normalize command.ingest "$misnamed_uri")
printf '%s\n' "$csv_run" | grep -q 'status=complete output=bonfyre://artifact/'
printf '%s\n' "$pdf_run" | grep -q 'status=complete output=bonfyre://artifact/'
printf '%s\n' "$audio_run" | grep -q 'status=complete output=bonfyre://artifact/'
printf '%s\n' "$misnamed_run" | grep -q 'status=complete output=bonfyre://artifact/'

misnamed_output=$(printf '%s\n' "$misnamed_run" | sed -n 's/.*output=\([^ ]*\).*/\1/p')
[ "$misnamed_output" != "$misnamed_uri" ]
[ "$(sqlite3 "$requirement_db" "SELECT media_type FROM artifacts WHERE uri='$misnamed_output';")" = "image/jpeg" ]

[ "$(sqlite3 "$requirement_db" "SELECT count(DISTINCT media_type) FROM artifacts WHERE uri IN ('$csv_uri','$pdf_uri','$audio_uri');")" -eq 3 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM events WHERE mission_id LIKE 'mixed-%' AND operator_id LIKE 'command.%' AND status='complete';")" -eq 4 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM execution_metrics m JOIN events e ON e.id=m.event_id WHERE e.mission_id LIKE 'mixed-%' AND m.bytes_in>0 AND m.bytes_out>0;")" -eq 4 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM receipts r JOIN events e ON e.receipt_id=r.id WHERE e.mission_id LIKE 'mixed-%' AND e.operator_id LIKE 'command.%' AND e.status='complete';")" -eq 4 ]

sqlite3 "$requirement_db" "SELECT receipt_id FROM events WHERE mission_id LIKE 'mixed-%' AND operator_id LIKE 'command.%' AND status='complete' ORDER BY mission_id;" |
while IFS= read -r receipt_id; do
  "$requirement_fabric" receipt show "$receipt_id" | grep -q '^content_hash=[0-9a-f]\{64\}$'
done

echo 'mixed artifact intake: passed'
