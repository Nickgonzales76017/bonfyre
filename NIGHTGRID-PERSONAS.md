# Night Grid demo personas

Generated 2026-09-02T17:41:21Z. Synthetic accounts in a DemoInstitution
(world DEMO). Addresses use the reserved .invalid TLD and can never receive mail.
This file is chmod 600 and lives outside the repository.

| persona | email | password | institution role | may touch |
|---------|-------|----------|------------------|-----------|
| organizer | `organizer@nightgrid.demo.invalid` | `A36AjOoH8RAbVVEtch4005P` | admin | events,members,publication |
| venue | `venue@nightgrid.demo.invalid` | `S8iy2SWNLlyC8HZSI2xvaUz` | write | merchant,offers,capacity |
| creator | `creator@nightgrid.demo.invalid` | `p5RiF66kUbun1GgD2n6SjMm` | write | creator,deliverables |
| sponsor | `sponsor@nightgrid.demo.invalid` | `DmdKkFPwrphgpASk3Agdxd3R` | write | finance,campaign-budget |
| editor | `editor@nightgrid.demo.invalid` | `KHS4fOmEzkaS0kTSjeJncGm4` | write | publication |
| safety | `safety@nightgrid.demo.invalid` | `6Nyf6q4CjWn2Zjqh1pjuWO8` | write | safety,operations |
| member | `member@nightgrid.demo.invalid` | `kjfUGeriy9X4wmyHqnlKQczW` | read | participation |
| machine | `machine@nightgrid.demo.invalid` | `fwNnjqZTe4kGwO00waml9` | read | machine-closure |

## What each persona must NOT gain

The lattice is declared in `architecture/institutional/namespaces.yaff`.
A venue operator must not reach district administration; a sponsor must not
gain generic institution admin; an editor must not reach finance. Those are
the assertions `nightgrid_lattice_test.sh` makes.

## Revoking all of them

```sh
for id in $(sudo sqlite3 /var/lib/bonfyre/auth.db "SELECT id FROM users WHERE email LIKE '%@nightgrid.demo.invalid';"); do
  sudo /usr/local/bin/bonfyre-auth --db /var/lib/bonfyre/auth.db institution-revoke --user-id $id --institution nightgrid
done
```
