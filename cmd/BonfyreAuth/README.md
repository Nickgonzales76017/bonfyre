# BonfyreAuth

User management and sessions: signup, login, tokens, team membership with
expiry-bounded delegation, and actor closure over native sessions. Passwords
never touch argv or stdout.

## Build, test, install

```sh
make -C cmd/BonfyreAuth
make -C cmd/BonfyreAuth test
make -C cmd/BonfyreAuth install MANAGED_PREFIX=~/.bonfyre
```

Tests cover team changes under a concurrent writer, stale-review refusal,
expired grants, self-change denial, revocation, and actor closure.

## Use

```sh
bonfyre-auth [--db FILE] signup --email E --name N --password P
bonfyre-auth [--db FILE] login --email E --password P
bonfyre-auth [--db FILE] verify --token T
bonfyre-auth [--db FILE] external-link-claim --token T --provider P --remote-account A --profile-url HTTPS
```

Session team mutations take the native write transaction before checking
admin authority; delegated grants cannot outlive that authority.

## Contract and limits

- The command existing never authorizes an effect; Authority gates it.
- An ActorClosure is a projected coordinate, not identity verification,
  relationship, or authority over anything.
- Membership rows are direct grants, not inherited memberships or
  provisioned accounts.

No version is pinned and none is published as a package yet. The live
contract is `cmd-auth` in the Architecture Atlas.
