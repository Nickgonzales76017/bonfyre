package control

import "strings"

// Shapes bonfyre-obsidian (cmd/BonfyreObsidian) writes; schemas/control/obsidian.yaff
// carries their meaning. README.md in the organ is the caller contract.

#ObsidianAccount: string & =~"^[a-z0-9_@+-][a-z0-9._@+-]*$" & strings.MaxRunes(128)
#ObsidianName: string & =~"^[A-Za-z0-9_][A-Za-z0-9._-]*$" & strings.MaxRunes(64)
#ObsidianSha: string & =~"^[0-9a-f]{64}$"
#ObsidianReceiptId: string & =~"^obr-[0-9a-f]{24}$"
#ObsidianNotePath: string & strings.MinRunes(1) & strings.MaxRunes(1024) &
	!~"^/" & !~"(^|/)\\.[^/]*(/|$)" & !~"//" & !~"/$" & !~"[\\x00-\\x1f\\x7f\\\\]"

#ObsidianVaultBinding: {
	account:           #ObsidianAccount
	name:              #ObsidianName
	vault_id:          string & =~"^([0-9a-f]{16}|local-[0-9a-f]{16})$"
	local_path:        string & =~"^/"
	transport:         "cli" | "headless" | "files"
	sync_mode:         "bidirectional" | "pull-only" | "mirror-remote"
	conflict_strategy: "merge" | "conflict"
	excluded_folders: [...string]
	device_name:  string & strings.MinRunes(1) & strings.MaxRunes(119)
	remote_vault: null | (string & strings.MaxRunes(249))
	config_dir:   string & =~"^\\.[^/]+$" & strings.MaxRunes(63)
	state:        "bound" | "sync-configured" | "syncing"
	created_at:   string
	updated_at:   string
}

#ObsidianNoteRef: {
	path:    #ObsidianNotePath
	sha256?: #ObsidianSha
}

#ObsidianTaskRef: {
	ref:    string & =~"^.+:[1-9][0-9]*$"
	path:   #ObsidianNotePath
	line:   int & >=1
	status: "todo" | "done" | "other"
	mark:   string & strings.MinRunes(1) & strings.MaxRunes(1)
	text:   string
}

#ObsidianGuardDecision: {
	decision: "allow" | "refuse"
	code:     "clear" | "transport" | "desktop-sync-enabled" | "desktop-sync-reported" | "headless-other-account" | "headless-unscoped"
	reason:   string & strings.MinRunes(1)
	evidence: null | string
	if decision == "allow" {code: "clear"}
	if code == "clear" {decision: "allow"}
}

#ObsidianSyncRun: {
	run_id:    int & >=1
	argv:      string & !~"--password|--email|--mfa"
	exit_code: int
	outcome:   "ok" | "failed" | "timed-out"
	log:       string & =~"^/"
	receipt:   #ObsidianReceiptId
	...
}

#ObsidianSnapshot: {
	snapshot_id:     int & >=1
	previous:        null | (int & >=1 & <snapshot_id)
	files:           int & >=0
	rehashed:        int & >=0
	reused:          int & >=0
	manifest_digest: #ObsidianSha
	receipt:         #ObsidianReceiptId
	...
}

// Every answer carries ok; a failure also carries exit 1..5, an error code and a reason.
#ObsidianFailure: {
	verb:     string
	ok:       false
	exit:     1 | 2 | 3 | 4 | 5
	error:    string & strings.MinRunes(1)
	reason:   string
	by_hand?: string
	detail?:  string
}
