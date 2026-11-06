#!/bin/sh
# A bonfyre-cms hook. Registered for a content type compiled from a DocType, it
# asks every donor Notification for that DocType whether this write fires it,
# and files each message that was written in full as a held message. It sends
# nothing: the queue holds the message for a person to decide about.
#
#   held_message_hook.sh DEFINITIONS_DIR DOCUMENT_TYPE QUEUE_DB
#
# bonfyre-cms hands the write over in BONFYRE_CMS_* (see run_hooks).
set -eu

definitions="$1"
document_type="$2"
queue_db="$3"
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compiler=${BONFYRE_FRAPPE_COMPILER:-"$here/bonfyre-frappe-compiler"}
queue=${BONFYRE_QUEUE:-"$here/../BonfyreQueue/bonfyre-queue"}

case "${BONFYRE_CMS_EVENT:-}" in
  after_create) event=New ;;
  after_update) event=Save ;;
  *) exit 0 ;;
esac

# The store keeps these columns for itself; a donor field of the same name is
# not carried, so a rule reading one cannot be answered from this row.
kept=id,namespace,created_at,updated_at,published_at,status

tab=$(printf '\t')
if [ -n "${BONFYRE_CMS_PREVIOUS_JSON:-}" ]; then
  "$compiler" --notify "$definitions" --document-type "$document_type" --event "$event" \
    --doc "$BONFYRE_CMS_ENTRY_JSON" --previous-doc "$BONFYRE_CMS_PREVIOUS_JSON" \
    --name "$BONFYRE_CMS_TYPE:$BONFYRE_CMS_ENTRY_ID" --not-carried "$kept" --queue-lines
else
  "$compiler" --notify "$definitions" --document-type "$document_type" --event "$event" \
    --doc "$BONFYRE_CMS_ENTRY_JSON" \
    --name "$BONFYRE_CMS_TYPE:$BONFYRE_CMS_ENTRY_ID" --not-carried "$kept" --queue-lines
fi | while IFS="$tab" read -r key held; do
  [ -n "$key" ] || continue
  "$queue" enqueue notification.compose "$held" --family held-message \
    --mission "held-$key" --if-absent --db "$queue_db" >/dev/null
done
