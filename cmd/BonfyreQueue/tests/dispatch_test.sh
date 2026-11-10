#!/bin/sh
set -eu

queue=${1:?queue binary required}
database=$(mktemp "${TMPDIR:-/tmp}/bonfyre-queue-dispatch.XXXXXX")
trap 'rm -f "$database" "$database-unknown" "$database-inbox-refusal.mp"' EXIT

set +e
"$queue" unknown-command --db "$database-unknown" >/dev/null 2>&1
unknown_status=$?
"$queue" enqueue type >/dev/null 2>&1
incomplete_status=$?
"$queue" account-work unknown --db "$database-unknown" >/dev/null 2>&1
account_work_status=$?
"$queue" account-inbox unknown --db "$database-unknown" >"$database-inbox-refusal.mp" 2>/dev/null
account_inbox_status=$?
set -e
if [ "$unknown_status" -ne 2 ] || [ "$incomplete_status" -ne 2 ] ||
   [ "$account_work_status" -ne 2 ] || [ "$account_inbox_status" -ne 0 ]; then
    printf '%s\n' 'unknown or incomplete verb did not return usage status' >&2
    exit 1
fi
if [ -e "$database-unknown" ]; then
    printf '%s\n' 'unknown verb opened a database' >&2
    exit 1
fi
# Inbox refuses invalid operations through a MessagePack failure packet.
LC_ALL=C grep -a -q "invalid_request" "$database-inbox-refusal.mp" || {
    printf '%s\n' 'Inbox invalid operation failed to emit a refusal packet' >&2
    exit 1
}
"$queue" account-work read --db "$database-unknown" --subject user:19 --scope self >/dev/null 2>&1 && {
    printf '%s\\n' 'missing account work index was fabricated' >&2
    exit 1
}
if [ -e "$database-unknown" ]; then
    printf '%s\\n' 'account work read created a database' >&2
    exit 1
fi
"$queue" fixture --db "$database" >/dev/null
"$queue" depth --db "$database" >/dev/null
"$queue" mission queue-success --db "$database" >/dev/null
printf '%s\n' 'queue dispatch: PASS'
