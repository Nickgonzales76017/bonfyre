# Campaign link helper integration

The new `src/campaign_workspace.h` serves a native form and the campaign JSON
protocol. It performs no storage, network requests or navigation to submitted
destinations. It depends on the existing `aw_header`, `aw_response`, `aw_response_with_referrer`, and
`aw_error` helpers, the native Lex HTML/form helpers, and the campaign library.

`application_workspace.h` includes it after the `aw_*` helpers are defined:

```c
#include "campaign_workspace.h"
```

Within `serve_application_workspace`, `_campaign-link` follows the existing
binding lookup and current Auth permission check. It requires the source-campaign
pack, `campaigns/` entry, and successful `ac_verified`. The existing permission
check requires read access for GET and write access for POST.

```c
if (!strcmp(slash + 1, "_campaign-link")) {
    if (strcmp(pack, "experience.source-campaign.workspace") ||
        strcmp(entry, "campaigns/") ||
        !ac_verified(id, subject, scope, pack, release, entry, actor)) {
        aw_error(409, "Open a current campaign workspace to build a link.\n");
        return 1;
    }
    char back[128];
    if (snprintf(back, sizeof(back), "/apps/%s/campaigns/", id) >= (int)sizeof(back)) {
        aw_error(409, "The campaign workspace address is unavailable.\n");
        return 1;
    }
    return cwl_serve(method, query, body, raw, host, back);
}
```

`return_path` must be server-derived. The helper accepts an exact
`/apps/ws-<40 lowercase hex>/...` path with ordinary path segments. The back
link uses that path and the form action uses the same binding's
`_campaign-link` route. The exact query `repr=API` is admitted only on JSON POST,
matching the reader's projection of `Accept: application/json`. All other
query strings, including additional or duplicate fields, are refused. Add an
ordinary workspace link to `../_campaign-link` from the `campaigns/` entry.

WebAddress links `../BonfyreCampaign/src/campaign.c` and the shared `bf_utf8.c`
implementation, reusing its existing `bf_json`, `bf_jsonw`, and SHA-256 objects.
The main object depends on `src/campaign_workspace.h`. No separately installed
campaign binary is needed.

GET returns an accessible form. Form POST accepts only `url` and the six named
UTM fields. Empty optional fields are omitted before the native build request.
Malformed, repeated or unknown form fields produce a blank correction form.
Other owner refusals preserve safe authored fields. Unsafe destinations are
cleared, including when another field caused the owner to refuse first.
All field values and the result are HTML-escaped. The result is a read-only
textarea; there is no destination link, script, redirect or external resource.

JSON POST takes the unchanged versioned campaign request, with `build`, `parse`
or `validate`. A valid native result returns HTTP 200; a native refusal returns
HTTP 400 with its JSON error envelope. HTTP transport refusals use the existing
plain-text `aw_error` response. Both response forms are private and uncached.

POST requires an exact same-origin Origin and a single decimal Content-Length
matching the NUL-terminated text body from the HTTP reader. Both form and JSON
bodies are bounded to 16384 bytes. Supported media types are
`application/x-www-form-urlencoded` and `application/json`, optionally with a
single UTF-8 charset parameter. Folded headers, content encodings and transfer
encodings are refused. The helper accepts the reader's NUL-terminated header
block: the last header value has no trailing CRLF because the reader replaces
the first byte of the final CRLFCRLF with NUL. Its LF framing path is also
accepted. A full framed input is bounded at its first blank line before header
lookup, so body text cannot become an Origin or Content-Length field.

Run the isolated tests without rebuilding WebAddress:

```sh
make -C cmd/BonfyreWebAddress -f tests/campaign_workspace_test.mk test
make -C cmd/BonfyreWebAddress -f tests/campaign_workspace_test.mk sanitizer-test
```

These tests capture the response boundary and exercise the helper with the
real native campaign library. They cover form rendering, escaping and
correction; duplicate, malformed and unknown fields; JSON results; origins,
headers, media and length limits; and generated query/fragment preservation.
The default POST fixtures reproduce the reader's in-buffer header/body split,
and explicit fixtures cover its last-header-without-CRLF and `repr=API` paths.
They do not exercise the outer Auth checks, workspace binding, HTTP reader,
`ac_verified`, a browser, or an installed release. Those require integration
tests after the route is wired.

`scripts/tests/campaign-workspace.test.mjs` checks the wired route with actual
isolated Auth and CMS owners, including foreign actors, read-only organization
members, revoked writers and cross-origin submissions. Chrome submits the native
form and verifies its result and phone width. HTML responses use a same-origin
referrer policy so form submissions retain Origin; external navigation receives
no referrer. The current-account bridge is recorded in
`architecture/external/campaign-attribution.yaff`.
