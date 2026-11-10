# Native campaign links

`bonfyre-campaign request` reads one JSON request from standard input and writes
one JSON response. It builds or reads manual campaign coordinates without
storage or network access. The reusable entry point is `bf_campaign_request`
in `src/campaign.h`; native web callers can use it directly.

Installed source-campaign workspaces expose "Build campaign link (new tab)"
in their native navigation. The form and JSON endpoint at
`/apps/<workspace-binding>/_campaign-link` check the current account grant and
editor binding on every request. Reading requires workspace read access;
generating or inspecting a submitted link requires write access. The form keeps
the existing artwork tab open and returns a selectable URL. It does not save
marketing metadata, load Google tags, or send campaign events.

Build and run the native witness:

```sh
make -C cmd/BonfyreCampaign test
make -C cmd/BonfyreCampaign sanitizer-test
```

The executable is `cmd/BonfyreCampaign/build/bonfyre-campaign`. Exit status is
0 for a successful result, 2 for a refused request, and 3 for an output failure.
Each error has a stable `error` code and omits the submitted URL and values.

Build requests have this shape:

```json
{
  "version": 1,
  "operation": "build",
  "url": "https://example.com/work?view=cards#start",
  "utm": {
    "utm_source": "newsletter",
    "utm_medium": "email",
    "utm_campaign": "autumn-2026",
    "utm_id": "campaign-42",
    "utm_term": "native tools",
    "utm_content": "top-link"
  }
}
```

`utm_source`, `utm_medium`, and `utm_campaign` are required for build. The other
three fields are optional. Every supplied coordinate is a nonempty UTF-8 string
of at most 256 bytes. Values preserve case. Generation uses the fixed field
order above and uppercase percent escapes; spaces become `%20` and literal
plus signs become `%2B`.

`parse` and `validate` accept only `version`, `operation`, and `url`. Both check
the destination and read its coordinates. Neither operation verifies a remote
destination. Their `tagging` field is `absent` when none of the six fields is
present, `complete` when source, medium and campaign are present, and `partial`
otherwise. A partial coordinate set remains visible without filling gaps.

Successful responses contain `schema: "bonfyre.campaign-link.v1"`, `ok: true`,
`operation`, `url`, `tagging`, `utm`, `provenance`, and
`external_effect_executed: false`. `provenance` is `authored_coordinates` for
build and `untrusted_url_coordinates` for parse or validate. The returned UTM
object contains only fields present in the request or URL. It carries no actor,
account, session, consent or conversion claim.

Requests are limited to 16384 bytes and URLs to 8192 UTF-8 bytes, including
generated tags. JSON must have exactly the declared fields, with no duplicate
keys, trailing input or invalid escapes. ASCII controls, invalid UTF-8 and
backslashes are refused in coordinates and URLs. URL queries decode `+` as a
space. Percent-encoded parameter names are decoded before duplicate checks.
Mixed-case aliases of the six owned names are refused; value case is retained.

Build refuses a URL already carrying any of the six owned fields. Other query
fields, their order and encoding, and the fragment retain their original bytes.
This includes `gclid`, `utm_source_platform` and unsupported UTM extensions.
The CLI does not determine how Google Analytics will combine manual tagging
with an advertising platform's auto-tagging.

Destination validation accepts HTTP or HTTPS with an ASCII DNS hostname
(including punycode), with an optional port from 1 through 65535. It refuses
credentials, IP literals, numeric final labels, trailing hostname dots,
single-label hosts, and the suffixes `.localhost`, `.local`, `.internal`,
`.lan`, `.home`, `.test`, `.invalid`, `.onion`, and `.home.arpa`. A DNS-looking
name can still resolve privately or redirect elsewhere. Any future fetch owner
must independently enforce its own address and redirect policy.

Campaign coordinates remain separate from source citation and the source URL
canonicalizer that removes tracking parameters. Account persistence must use
the current native Auth/CMS binding. An arrival, qualified lead or conversion
requires its own observed event or owner receipt. The current block supplies
none of those records.

Google's [current manual campaign URL documentation](https://support.google.com/analytics/answer/10917952)
defines the six fields and case-sensitive values. It also recommends relevant
additional dimensions, including `utm_source_platform`, which v1 preserves
without interpreting. This is a deliberate six-field contract.

The future Google Tag Manager adapter must follow the
[data layer contract](https://developers.google.com/tag-platform/tag-manager/datalayer):
an `event` and its variables can travel in one push, queued messages are processed
in order, and data does not automatically persist between page loads. Consent
defaults and updates must precede the corresponding measurement using
[Tag Manager consent APIs](https://developers.google.com/tag-platform/security/guides/consent).
This implementation does not load a container, publish its configuration, or
dispatch events. These primary sources were checked on 2026-10-26.
