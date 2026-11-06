# bonfyre-obsidian

Local-first: Obsidian vaults are folders of Markdown and attachments. Native
reading, writing, search, tasks, snapshots and changes need **no Obsidian
account, Sync subscription, enabled CLI or external hosted service**.
Obsidian Headless is an optional vendor-hosted sync adapter, never a default
prerequisite. Account-authorized replication over Aurekai's own infrastructure
is a separate, **not yet wired** data path; a successful snapshot is not a
remote backup, merge, or restore.

Binds Obsidian vault folders on this device to Bonfyre accounts and gives every
flow one command for reading, writing and syncing them. It drives Obsidian's two
official tools and falls back to its own readers and writers over the files:

| tool | what it is | when the organ uses it |
|---|---|---|
| `obsidian` | the desktop app's CLI; if no global command is registered, the bundled executable in `Obsidian.app/Contents/MacOS/obsidian-cli` is considered | reads and writes for a vault bound with `--transport cli`, only while the app is already running |
| `ob` | Obsidian Headless (`npm install -g obsidian-headless`, Node 22 or later, needs an Obsidian Sync subscription) | the `sync-*` verbs, for a vault bound with `--transport headless` |
| native | C readers and writers in this organ | every read and write when the CLI is not in play |

Atlas entries: `obsidian-vault-binding` (architecture/external/obsidian-vault.yaff)
and `cmd-obsidian`. Shapes: schemas/control/obsidian.yaff and obsidian.cue.

## Build and test

```sh
make                 # bonfyre-obsidian, C over lib/libbonfyre, links sqlite3
make test            # tests/obsidian_test.sh over throwaway vaults
make real REAL_VAULT=~/Documents/SomeVault   # read-only run; fails if any file changes
```

## Accounts and vaults

Every verb except `doctor` requires `--account A`. An account is 1 to 128 of
`a-z 0-9 - _ . @ +` or exactly `user:<positive decimal id>` with no leading
zeroes. The canonical subject is never collapsed to a hyphen slug; launchd
labels include a subject digest to prevent collisions. The organ trusts
the account it is given; the caller (web route, MCP tool, flow) must have
authenticated it.

A folder is bound to one account under a per-account `--name`. Binding resolves
the folder's real path and refuses it when:

- it lies outside every root in `BONFYRE_OBSIDIAN_ROOTS` (a symlink that leads out counts as outside),
- it is already bound by any account, or it holds or sits inside a bound folder.

A verb that names a vault another account bound gets the same answer as a name
nobody bound (`not-bound`, exit 3).

Note paths are relative to the vault with `/` separators. Empty, `.`, `..` and
dot-led components are refused, and the vault's config folder (`.obsidian`, any
case) is never read or written through a note verb. Reads must resolve inside
the vault's real root. Writes refuse a symlink anywhere on the path and land by
write, fsync and rename in the same folder.

## Output and exit codes

Every run prints exactly one JSON object on stdout, written with `bf_jsonw`.
Each has `"verb"` and `"ok"`. A failure looks like this:

```json
{"verb":"read","ok":false,"exit":3,"error":"path-refused","reason":"the note path leaves the vault"}
```

It may also carry `"by_hand"` (a command only a person should run) and
`"detail"` (tool stderr).

| exit | meaning | common `error` codes |
|---|---|---|
| 0 | done | |
| 1 | the operation failed | `store`, `write-failed`, `ob-failed`, `cli-failed`, `start-failed`, `timed-out` |
| 2 | usage | `usage` |
| 3 | refused | `not-bound`, `already-bound`, `nested`, `outside-roots`, `name-taken`, `path-refused`, `exists`, `not-a-task`, `not-owned`, `syncing`, and the guard codes |
| 4 | not found | `not-found`, `vault-missing`, `no-folder`, `no-snapshots` |
| 5 | a required tool is missing | `ob-missing`, `cli-unavailable` |

## Verbs

`--via auto|cli|native` applies to the read and write verbs. With `auto` (the
default) the CLI is used when the binding's transport is `cli`, the CLI is
registered, and the app is running. `cli` forces it and fails with exit 5 when
it is unavailable. `native` never calls it.

### doctor `[--account A]`

Reports what is on the machine; it changes nothing. The `native` section
reports subscription-free local file/snapshot capability separately from the
optional hosted `headless` path. `cross_device_replication` is `not-wired`
until a real account-authorized Aurekai transport, conflict resolution and
restore roundtrip passes acceptance. Hosted login/setup prompts appear only
for an account that explicitly binds a hosted Headless transport.

```json
{"verb":"doctor",
 "app":{"path":"/Applications/Obsidian.app","installed":true,"version":"1.12.7","running":true},
 "cli":{"registered":false,"path":null,"bundled":"/Applications/Obsidian.app/Contents/MacOS/obsidian-cli","reachable":null},
 "headless":{"installed":true,"path":"...","version":"0.0.14","node":{"path":"...","version":"v26.13.0","meets_22":true},
             "unscoped_login_present":false,"env_token_present":false,"env_token_account":null},
 "account":"A","login":{"home":".../accounts/A/home","token_file_present":false,"env_token_applies":false,"login_command":"HOME='...' ob login"},
 "desktop_vaults":[{"id":"3399433bbf7eec7f","name":"AurekaiV2","path":"...","real_path":"...","exists":true,"open":true,"desktop_sync_plugin":true}],
 "store":{"db":"...","exists":true,"state":"...","roots":["..."],"supervisor":"child"},
 "by_hand":[{"why":"...","do":"..."}],
 "ok":true}
```

`cli.reachable` is `null` unless the CLI is registered and the app is running;
then the organ runs `obsidian version`. `desktop_sync_plugin` is `null` when the
vault has no `core-plugins.json`.

### vaults

`{"account","bindings":[Binding...],"desktop_vaults":[{...doctor fields, "bound_to_this_account","bound_elsewhere","allowed_root"}]}`.
It does not say which account holds a vault bound elsewhere.

Binding:

```json
{"account":"a1","name":"notes","vault_id":"3399433bbf7eec7f","local_path":"/Users/x/Documents/Notes",
 "transport":"cli|headless|files","sync_mode":"bidirectional|pull-only|mirror-remote","conflict_strategy":"merge|conflict",
 "excluded_folders":["Archive"],"device_name":"bonfyre-a1","remote_vault":null,"config_dir":".obsidian",
 "state":"bound|sync-configured|syncing","created_at":"2026-10-31T20:00:00Z","updated_at":"..."}
```

`vault_id` is the desktop app's id when the folder is in its vault list, otherwise `local-` plus 16 hex.

### bind `--name N --path DIR [--transport files] [--mode bidirectional] [--conflict merge] [--exclude a,b] [--device-name D] [--remote-vault V] [--config-dir .obsidian]`

`{"binding":Binding,"desktop_registered":bool,"desktop_sync_plugin":bool|null,"receipt":"obr-..."}`

### unbind `--name N`

Refused (`syncing`) while continuous sync runs. Removes the binding and its
snapshots and leaves the files and any `ob` setup alone.
`{"unbound":"N","note":"...","receipt":"obr-..."}`

### status `[--name N]`

`{"account","login":Login,"vaults":[{"binding":Binding,"folder_present","desktop_sync_plugin","cli_usable","headless_guard"?:Guard,"continuous_sync"?:{"running","pid","supervisor"},"last_sync_run":Run|null,"last_snapshot":{...}|null}]}`.
`headless_guard` and `continuous_sync` appear for headless bindings.

### receipts `[--name N] [--limit 50]`

`{"receipts":[{"receipt","at","actor","verb","binding","target","outcome":"ok|refused|failed","code","reason","via","sha_before","sha_after"}]}`.
Receipts never hold note content or a credential.

### read `--name N --path NOTE`

`{"via":"native|cli","path","bytes","sha256","content"}`. A note larger than 16 MiB or holding a NUL byte is refused.

### Read verbs over the vault

All take `--name N`. Native answers sit under `"answer"` with
`"shape":"bonfyre-native"`. When the CLI answers, the top level carries
`"shape":"obsidian-cli","via":"cli","command":"...","result":<the CLI's JSON as given>`
(or `"text"` when it did not print JSON). If the CLI fails in `auto` mode the
native answer is returned with `"cli_fallback":"reason"`. A caller should branch on `shape`.

| verb | options | native `answer` |
|---|---|---|
| search | `--query Q [--prefix FOLDER/] [--limit 50]` | `{"query","file_matches":[path],"matches":[{"path","line","text"}],"files_scanned","truncated"}`; case-insensitive, `text` cut to 240 bytes |
| tasks | `[--status all\|todo\|done\|other] [--path NOTE]` | `{"tasks":[{"ref":"path:line","path","line","status","mark","text"}],"counts":{"done","other","todo"}}`; counts ignore the filter; fenced code is skipped |
| backlinks | `--path NOTE` | `{"target","backlinks":[{"source","line","kind":"wikilink\|embed\|markdown","raw","target"}],"count"}` |
| links | `--path NOTE` | `{"source","links":[{"line","kind","raw","target","resolved":path\|null}],"count"}` |
| unresolved | | `{"unresolved":[{"source","line","kind","raw","target"}],"count"}` |
| tags | `[--path NOTE]` | `{"tags":[{"tag","count","files"}]}`; lower-cased, from frontmatter `tags`/`tag` and `#tag` in the body |
| properties | `[--path NOTE]` | with a path: `{"path","properties":{key: string\|[string]\|null}}`; without: `{"keys":[{"key","files"}]}` |

Link resolution follows Obsidian's rules: a markdown link is relative to its
note; a wikilink with a folder matches a path ending in it; a bare name matches
any file of that name without case, preferring the note's folder, then the
shortest path. Frontmatter parsing covers `key: value`, `key: [a, b]` and
`- item` lists; nested maps and multi-line strings are skipped.

### Write verbs

All take `--name N` and content as `--content TEXT` or `--stdin` (at most 1 MiB, no NUL).

| verb | options | effect |
|---|---|---|
| create | `--path NOTE [--overwrite]` | new note; `.md` is added when the name has no extension; refused (`exists`) unless `--overwrite`; missing folders are made |
| append | `--path NOTE` | adds the content on a new line at the end; the note must exist |
| prepend | `--path NOTE` | inserts after the frontmatter, or at the top |
| daily-append | `[--date YYYY-MM-DD]` | the daily note from `.obsidian/daily-notes.json` (`folder`, `format`; default `YYYY-MM-DD`), created when missing |
| daily-path | `[--date YYYY-MM-DD]` | `{"path","exists"}`, writes nothing |
| task | `--ref NOTE:LINE [--done\|--todo\|--toggle]` | sets the checkbox on that line; refused (`not-a-task`) if the line is not a task |

Answer: `{"via","name","path","created","sha_before","sha_after","receipt"}` (`task` gives `ref` and `set` instead of `path` and `created`).
Through the CLI, newlines and tabs in content are sent as `\n` and `\t`, which
is how the CLI takes them; a literal backslash-n in content is therefore
ambiguous there. After a CLI write, `sha_after` is read from disk and can lag
the app.

### Headless sync

All take `--name N` and need a binding with `--transport headless`.

`ob` runs with `HOME=<state>/accounts/<account>/home`, so each account has its
own Obsidian login and its own per-vault sync state. `OBSIDIAN_AUTH_TOKEN` is
removed from `ob`'s environment unless `BONFYRE_OBSIDIAN_TOKEN_ACCOUNT` names
the account. No password, email or MFA code is ever put on argv. Output and
stderr of every run are appended to `<state>/accounts/<account>/logs/<name>.log`.

The guard runs before `sync-setup`, `sync-config`, `sync-once` and `sync-start`:

```json
{"decision":"allow|refuse","code":"clear|transport|desktop-sync-enabled|desktop-sync-reported|headless-other-account|headless-unscoped","reason":"...","evidence":"...|null"}
```

It refuses when the vault's desktop Sync core plugin is on (`core-plugins.json`,
object or array form), when the running app's `sync:status` reports Sync, or
when another account's or an unscoped `ob` setup already syncs the folder.
Obsidian says desktop Sync and Headless Sync must never run on one device for
one vault. A refusal exits 3 with the guard's code as `error`, and `ob` is not run.

| verb | options | answer |
|---|---|---|
| guard | | `{"name","guard":Guard}`; never refuses itself |
| sync-setup | `[--remote-vault V]` | runs `ob sync-setup --vault V --path DIR --device-name D [--config-dir C] --json`, then `sync-config`; state becomes `sync-configured` |
| sync-config | `[--mode M] [--conflict S] [--exclude a,b] [--device-name D]` | saves the binding, runs `ob sync-config ... --json` |
| sync-once | | `ob sync --path DIR`, deadline `BONFYRE_OBSIDIAN_SYNC_TIMEOUT` (900 s); refused while continuous sync runs |
| sync-start | | starts `ob sync --continuous --path DIR` under the supervisor; `{"continuous_sync":{"already_running","supervisor","pid","pidfile","log"} ...}` or for launchd `{"label","plist","log"}`; state becomes `syncing` |
| sync-stop | | `{"was_running","stale_pidfile"?,"supervisor"}` |
| sync-status | | `{"continuous_sync":{"running","pid"},"login":Login,"ob":{"exit_code","configured","status":<ob JSON>\|null,"message"},"last_sync_run","last_snapshot"}` |

Each `ob` run answers under its kind (`"sync-setup"`, `"sync-config"`, `"sync-once"`):
`{"run_id","argv","exit_code","outcome":"ok|failed|timed-out","output":<JSON or text>,"stderr","log","receipt"}`.

When `sync-setup` fails (no login, or an end-to-end encrypted remote that needs
its password typed), `by_hand` carries the exact command for a person.

Supervisor (`BONFYRE_OBSIDIAN_SUPERVISOR` or `--supervisor`):

- `child` (default): a detached process in its own session, pid in
  `<state>/accounts/<account>/run/<name>.pid`. Stop signals it only when its
  command line is still `ob`'s continuous sync for this folder, so a reused pid
  is reported as stale and left alone.
- `launchd`: writes `com.bonfyre.obsidian.<account>.<name>.plist` to
  `BONFYRE_OBSIDIAN_LAUNCH_AGENTS` (KeepAlive, RunAtLoad, the account HOME) and
  runs `launchctl bootstrap gui/<uid>`; stop runs `bootout` and removes the plist.

### Snapshots

| verb | options | answer |
|---|---|---|
| snapshot | `[--include-config]` | `{"snapshot_id","previous","taken_at","files","bytes","rehashed","reused","symlinks_skipped","include_config","manifest_digest","since_previous"?:{"added","removed","changed"},"receipt"}` |
| diff | `[--from ID] [--to ID]` | `{"from","to","added":[{"path","sha256","size"}],"removed":[...],"changed":[{"path","from_sha256","to_sha256","size"}],"counts"}`; defaults to the last two; both must be this account's snapshots of this vault |

A snapshot walks every file except dot-led ones, symlinks and excluded folders
(the config folder too, unless `--include-config`). A file whose size and mtime
match the previous snapshot keeps its recorded sha256; the rest are hashed.
`manifest_digest` is sha256 over `path NUL sha256 NUL size LF` lines in byte order.

### Native replica planning (local, read-only)

`replica-plan --account A --name SOURCE --peer PEER` requires two distinct
bound vaults under the **same named account**, distinct permitted real
roots, and identical exclusion/configuration scopes. It hashes actual file
bytes in both replicas and reports identical, source-only, peer-only and
divergent paths (plus per-file SHA-256) without copying or deleting anything.
It refuses unbound accounts, changed roots, symlinked file paths and oversized
plans. At most 256 entries are displayed; all counts are computed up to 4096
files per vault. Source and peer paths are only visible with that account
identifier; a public/API caller must independently authenticate and authorize it.

With both \`--base-source ID\` and \`--base-peer ID\`, each snapshot must be
owned by its respective account-bound replica, omit Obsidian configuration,
and contain exactly the same paths, SHA-256 digests and sizes. The planner
then reports \`unchanged\`, \`source-changed\`, \`peer-changed\`, \`both-same\`
and \`conflict\` for current contents, including added and deleted paths.
The paired snapshots are user-selected recorded manifests; matching bytes
do not independently prove common history or authorize any transfer.
All classifications are **candidate changes only**, not an auto-merge or
permission to overwrite or delete either replica.
This is not bidirectional sync, a merge or a backup. Without a common ancestor,
a divergent file is **not** evidence of which version is newer. Cross-device
encrypted transfer, tombstones, revision CAS and restore acceptance remain open.


## Configuration

| variable | default | |
|---|---|---|
| `BONFYRE_OBSIDIAN_DB` (or `--db`) | `~/.bonfyre/state/obsidian.db` | the SQLite store |
| `BONFYRE_OBSIDIAN_STATE` | `~/.bonfyre/state/obsidian` | per-account homes, logs, pidfiles |
| `BONFYRE_OBSIDIAN_ROOTS` (or `--roots`) | `~/Documents` | `:`-separated folders a vault must resolve inside |
| `OBSIDIAN_CLI` | `obsidian` on `PATH` | the desktop CLI |
| `BONFYRE_OBSIDIAN_OB` | `ob` on `PATH` | Obsidian Headless |
| `BONFYRE_OBSIDIAN_NODE` | `node` | reported by doctor |
| `BONFYRE_OBSIDIAN_APP` | `/Applications/Obsidian.app` | |
| `BONFYRE_OBSIDIAN_APP_SUPPORT` | `~/Library/Application Support/obsidian` | holds `obsidian.json`, the desktop vault list |
| `BONFYRE_OBSIDIAN_APP_RUNNING` | probe with `pgrep -x Obsidian` | `1` or `0` overrides the probe |
| `BONFYRE_OBSIDIAN_CLI_LAUNCH` | `0` | `1` lets a call start the desktop app |
| `BONFYRE_OBSIDIAN_TIMEOUT` | `10` | seconds for one tool call |
| `BONFYRE_OBSIDIAN_SYNC_TIMEOUT` | `900` | seconds for `sync-once` |
| `BONFYRE_OBSIDIAN_SUPERVISOR` (or `--supervisor`) | `child` | `child` or `launchd` |
| `BONFYRE_OBSIDIAN_LAUNCHCTL` | `/bin/launchctl` | |
| `BONFYRE_OBSIDIAN_LAUNCH_AGENTS` | `~/Library/LaunchAgents` | |
| `BONFYRE_OBSIDIAN_TOKEN_ACCOUNT` | unset | the one account `OBSIDIAN_AUTH_TOKEN` may reach |

`--actor NAME` is recorded on receipts (default `operator-cli`).

## Done by a person

The organ never logs in, types a password, creates a remote vault or changes
Obsidian's settings. `doctor` lists what is missing as `by_hand` steps:

- Desktop CLI: in Obsidian, Settings > General > Command line interface. This registers `/usr/local/bin/obsidian`.
- Headless login for an account: `mkdir -p '<state>/accounts/<account>/home' && HOME='<state>/accounts/<account>/home' ob login`.
- An end-to-end encrypted remote vault: run the `sync-setup` command from `by_hand` at a terminal so `ob` can ask for the vault password.
