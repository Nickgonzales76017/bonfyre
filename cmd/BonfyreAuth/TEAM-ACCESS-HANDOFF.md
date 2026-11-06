# Team access integration

Codex implemented the native commands after the task message to the existing Claude session was delivered without an ownership acknowledgment. A follow-up explicitly withdrew the implementation assignment before Codex edited Auth. Independent review is still welcome; no Claude review is claimed.

`institution-members-stdin --institution I [--after-id ID]` reads current direct grants, 50 members per page, with `next_user_id` for continuation. `institution-member-set-stdin --institution I --email E --role read|write|admin|none` changes a direct grant for an existing active account. Both read the session token from stdin and require current administrator authority. Errors include a numeric HTTP-compatible `status` and a user-facing `error`.

The implementation is in `src/institution_members.h`, included by the existing native Auth owner. It uses the generated institution ancestry and existing grant table. Mutation takes a SQLite write transaction before authorization, rejects self-changes, and caps delegated grant expiry at the administrator's best current authority. Removing a direct grant does not remove inherited authority. It does not invite people, create accounts or bind actors.

The training creator HTTP adapter permits only the configured studio's request, program or appearance scope and repeats actor/admin checks after receiving the body. The browser offers review before mutation. Native fixtures are in `tests/institution_members_test.py`; HTTP fixtures are in `components/BonfyreServiceSite/tests/training-creator.test.mjs`; full workspace browser checks are in `components/BonfyreServiceSite/tests/training-workspace-browser.mjs`.

Product follow-through: training has separate URLs and reusable workspace navigation, per-studio access summaries, searchable request views and team administration. This remains a local workspace. Public demo hosting, invitations and account provisioning are not completed by this change.

`account-profile-stdin` returns only the current account ID, name and email with JSON escaping. The studio uses it for the signed-in identity. Tokens, gate keys and password material are excluded.

Validation passed: native Auth suites and team transaction checks; 26 focused navigation/training API tests; 16 workspace browser checks; the original 12 delivery browser checks; and 871 control-plane tests (259 skipped). The atlas was exported and the installed ExperienceMap corpus refreshed. Source hashes and results are in `packs/no-simulation/product-maturity-20261015/workspace/verification-summary.json`.
