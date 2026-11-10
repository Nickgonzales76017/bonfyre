# Household membership and shared audio workspaces

## Implemented and verified in this continuation

Household membership is retained by the native CMS. `household_access.h` is the
shared read projection; `household_members.h` owns revisioned mutations. The
native `/households/access` page resolves an existing account, presents its
current access, and submits an explicit reviewed change. Only the recorded
steward manages memberships. Read, write and admin grants respectively permit
inspection, shared draft edits and configuration changes. A delegated admin
cannot confer further household access.

Membership changes compare both record revision and household authority
generation. Steward changes, withdrawal, deletion and row reuse invalidate the
generation; changing a steward away and back cannot revive old grants. The
native installation and draft-save transactions check current household access
again. Expired and revoked members lose access without deleting household work.
These forms do not send invitations, create accounts or transfer stewardship.
They preserve an existing expiry; browser-based expiry renewal remains open.

`materialize_creator_workspace.mjs` packages the existing ServiceSite audio
arrangement editor, native/Wasm mix kernel and generated sample through the
same retained-package owner used for the catalogue. The connected pack is
`experience.native-creator-studio.workspace`; its record family is
`creator-audio`, with only `draft:creator:` records admitted. Independent personal,
organization and household destinations do not inherit each other's recordings.

The native binary profile preserves original ArrayBuffer WAV sources. Profile 2
records and Blob/File values retain their existing meaning. Profile 3 explicitly
identifies buffer references, so a recording reopens as the same binary value,
not a base64 field or a byte array in a text snapshot. Unknown restoration types
are refused. Audio source selection excludes private training records.

Source selection and destination adoption are separate user actions. Adoption
retains original recordings and clip coordinates, leaves the source browser's
record and revision unchanged, and resets destination recording permission to
unreviewed. The source's permission statement is retained as sourcePermission,
not as permission for the recipient to distribute the recording.

## Executed checks

The final native regression run passed 31 record-owner checks, 28 installation
lifecycle checks, 25 household membership checks, 32 workspace HTTP/publication
checks and 41 setup HTTP checks. Receipt:
`87f61a2f-65aa-47d8-a6a0-dbb862f431c6`.

`creator_workspace_test.mjs` passed 20 checks with real Chrome interactions,
including playback, native saving, exact recordings, a second authorized member,
stale-save refusal, a downloaded and decoded WAV, phone layout and the actual
household member-review forms. Receipt:
`59d6e25e-63ef-46d7-88c2-6777c4b1ffe0`.

`creator_adoption_test.mjs` passed seven two-origin browser checks. Receipt:
`889a1a21-ce3f-4d5f-9d4c-2a52d42a2878`.

All authenticated checks used isolated native accounts and databases. The audio
package is `0049afa318eb159a43d05cdb5bf3eae637de43f09b03fbea926d2a74536e49c4`.
Release deployment must be read from the separate continuation release receipt;
source-build tests do not establish a production account walkthrough.

## Atlas registration handoff

The connector refused writes under architecture/, so these changes are not yet
registered in the atlas. Register HouseholdMemberAccess with the CMS household
headers, WebAddress household_members_web.h, its 25-check test and the actual
browser form test above. Its interaction with PackGraph preserves the household,
actor, authority generation and saved work while refusing implicit membership.
Register NativeDestinationWorkspace with the workspace store/record/blob owners,
