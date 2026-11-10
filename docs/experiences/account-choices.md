# Account choices

## Implementation status

Source integration was applied on October 21, 2026. It has not been promoted.
The live account still uses the earlier template until release verification is
complete; the previously deployed participant-return work remains live.

Nine isolated native/browser checks pass for the new account choices. The wider
account/source suite returned five navigation timeouts when the public institution
directory is unavailable. The reader reports the outage but does not consume or
cancel its error response. The proposed correction was blocked before execution
and was not retried through another route. Do not describe this source state as a
completed or deployed account release.

## What the applied source implements

The private account entrance links to actual activity, saved material and
community continuations, then lets the person choose a personal or shared space.
Current choices come from the existing authenticated destination reader. A name
search is not an identifier, and a selected destination is not a permission grant.

The institution picker searches admitted public SourceFrontier roots by name,
shows the domain and retained public source count, and carries an exact selected
host into the existing institution page. Duplicate labels remain distinguishable.
Withheld and ambiguous roots are not turned into guessed choices. Opening an
institution does not join it or establish a household relationship.

For public profile links, the user enters one address and reviews the recognized
service/account before the existing Auth operation saves it. The raw provider and
remote-account inputs are no longer visible. An unknown site stays a website link;
recognizing the URL does not establish control, OAuth consent, imported data or
external publication. A custom display name remains optional creative text.

Four named profile entrances use the existing session handoff. They replace the
old inline LIFE-only composer; the current account/site-scoped profile owner still
controls introductions and public content. No saved link automatically appears on
any published profile.

## Reusable selection behavior

integrations/browser/reference-picker.mjs is a presentation controller. Its caller
provides bounded records and the actual receiving action. It preserves distinct
identity under duplicate labels, uses keyboard selection and inspection, clears
stale choices immediately on typing, rejects late responses, supports native
pagination and retry, and clears private context after an account change.

It creates no identity catalogue, permission or persistence store. The account
consumers reuse SourceFrontier, Auth, application destination and session owners.
The actual account choices are tested in
cmd/BonfyreWebAddress/tests/account_choices.test.mjs. Artificial programme labels
in component tests test rendering and races, not a completed grant programme.

## Wider use

Existing recipients, products, programmes, sources, records and sites should be
chosen or inherited from context, not entered as memorized identifiers. New
content still needs useful text entry. Creating a new organisation, describing a
proposal, composing a post or inviting a genuinely new email address is not the
same operation as finding an existing record.

The shared selector does not complete the 388 skill-derived applications, grant
programme service flows, account authorization providers or their specialist
editors. Each real consumer needs its own scoped data owner, relevant preview,
empty/no-access/revoked states and end-to-end continuation tests.
