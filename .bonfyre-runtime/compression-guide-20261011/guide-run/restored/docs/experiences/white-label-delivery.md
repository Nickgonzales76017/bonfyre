# White-label delivery

Run three customer sites from one Bonfÿre installation, then check which content and operations each site can access. This local walkthrough uses invented businesses and keeps its records in a fresh directory.

| Business | Local address | Example content |
|---|---|---|
| Fernhollow Pottery | fernhollow.localhost:8926 | Pottery lessons |
| Tallgrass Bike Works | tallgrass.localhost:8926 | Bicycle repairs |
| Juniper Lane Bakery | juniper.localhost:8926 | Bakery orders |

Visitors browse the websites directly. Authorized assistants use each site's MCP endpoint to read published content or submit an enquiry. Published pages remain public; a read key identifies a caller and does not make those pages private.

## Create the customer sites

Start in a configured Bonfÿre checkout with the native Site and CMS commands, `curl` and `jq`. Run the blocks in one shell. Port 8926 must be free; change `bonfyre_port` if needed.

```sh Terminal
make -C cmd/BonfyreWebAddress bonfyre-site
bonfyre_agency=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-agency-guide.XXXXXX")
bonfyre_port=8926
printf '%s\n' "$bonfyre_agency"
bonfyre site new fernhollow --title 'Fernhollow Pottery' \
  --tagline 'Pottery lessons and workshops' --accent '#886B58' \
  --host fernhollow.localhost --no-open \
  --home "$bonfyre_agency" --port "$bonfyre_port"
bonfyre site new tallgrass --title 'Tallgrass Bike Works' \
  --tagline 'Keep moving' --accent '#C46C36' \
  --host tallgrass.localhost --no-open \
  --home "$bonfyre_agency" --port "$bonfyre_port"
bonfyre site new juniper --title 'Juniper Lane Bakery' \
  --tagline 'Fresh from the neighborhood' --accent '#7A8D65' \
  --host juniper.localhost --no-open \
  --home "$bonfyre_agency" --port "$bonfyre_port"
```

Each site has its own ID within the shared local store. Keep `--home` and `--port` consistent throughout the exercise. This separation does not remove the machine operator's access to the underlying records.

## Publish content at the same route

Give every site a `/services` page. The host and site ID determine which page a visitor or assistant receives.

```sh Terminal
bonfyre site page fernhollow --title 'Pottery lessons' --route /services \
  --text 'Learn to throw a bowl at Fernhollow Pottery.' --no-open \
  --home "$bonfyre_agency" --port "$bonfyre_port"
bonfyre site page tallgrass --title 'Bicycle repairs' --route /services \
  --text 'Book a puncture repair at Tallgrass Bike Works.' --no-open \
  --home "$bonfyre_agency" --port "$bonfyre_port"
bonfyre site page juniper --title 'Bakery orders' --route /services \
  --text 'Order a sourdough loaf from Juniper Lane Bakery.' --no-open \
  --home "$bonfyre_agency" --port "$bonfyre_port"
bonfyre site page fernhollow --title 'Studio visits' --route /studio \
  --text 'Arrange a visit to the pottery studio.' --no-open \
  --home "$bonfyre_agency" --port "$bonfyre_port"

for bonfyre_customer in fernhollow tallgrass juniper; do
  curl --fail --silent --show-error \
    --resolve "$bonfyre_customer.localhost:$bonfyre_port:127.0.0.1" \
    -H 'Accept: text/html' \
    "http://$bonfyre_customer.localhost:$bonfyre_port/services" \
    > "$bonfyre_agency/$bonfyre_customer.html"
  curl --fail --silent --show-error \
    "http://127.0.0.1:$bonfyre_port/mcp/$bonfyre_customer" \
    -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_page","arguments":{"route":"/services"}}}' \
    > "$bonfyre_agency/$bonfyre_customer-page.json"
  jq -e '.result.structuredContent | {title, text}' \
    "$bonfyre_agency/$bonfyre_customer-page.json"
done
test "$(curl --silent --show-error -o /dev/null -w '%{http_code}' \
  --resolve "tallgrass.localhost:$bonfyre_port:127.0.0.1" \
  -H 'Accept: text/html' \
  "http://tallgrass.localhost:$bonfyre_port/studio")" = 404
```

Expect three different page titles and bodies. The HTML files should carry the matching customer name and content. Tallgrass must return HTTP 404 for Fernhollow's `/studio` route.

## Scope access keys to one customer

Create a reader for Fernhollow and a writer for Tallgrass. Keep their values in shell variables. An explicitly supplied key for the wrong site must be rejected, even when the requested operation is a public read.

```sh Terminal
bonfyre_reader=$(bonfyre site key fernhollow --label tutorial-reader --json \
  --home "$bonfyre_agency" --port "$bonfyre_port" | jq -er '.key')
bonfyre_writer=$(bonfyre site key tallgrass --label tutorial-desk --write --json \
  --home "$bonfyre_agency" --port "$bonfyre_port" | jq -er '.key')

curl --fail --silent --show-error \
  "http://127.0.0.1:$bonfyre_port/mcp/fernhollow" \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $bonfyre_reader" \
  --data '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_page","arguments":{"route":"/services"}}}' \
  > "$bonfyre_agency/keyed-page.json"
jq -e '.result.structuredContent.title == "Pottery lessons"' \
  "$bonfyre_agency/keyed-page.json"
test "$(curl --silent --show-error -o "$bonfyre_agency/foreign-key.json" \
  -w '%{http_code}' "http://127.0.0.1:$bonfyre_port/mcp/juniper" \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $bonfyre_reader" \
  --data '{"jsonrpc":"2.0","id":3,"method":"ping"}')" = 401
```

## Check browser origins

Browser requests carry an Origin. The endpoint accepts the selected site's named host on the local endpoint's port, and rejects a sibling site or a lookalike domain. Native clients without an Origin can still call the endpoint. The [MCP transport specification](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports) describes origin validation for HTTP transports.

```sh Terminal
test "$(curl --silent --show-error -o /dev/null -w '%{http_code}' \
  "http://127.0.0.1:$bonfyre_port/mcp/fernhollow" \
  -H "Origin: http://fernhollow.localhost:$bonfyre_port" \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":4,"method":"ping"}')" = 200
for bonfyre_origin in \
  "http://fernhollow.localhost.evil:$bonfyre_port" \
  "http://juniper.localhost:$bonfyre_port" 'null'; do
  test "$(curl --silent --show-error -o /dev/null -w '%{http_code}' \
    "http://127.0.0.1:$bonfyre_port/mcp/fernhollow" \
    -H "Origin: $bonfyre_origin" -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","id":5,"method":"ping"}')" = 403
done
```

## Write an enquiry and inspect every inbox

The reader cannot submit an enquiry. Tallgrass's writer can leave one in Tallgrass's inbox. These example contact details and messages stay in the local store.

```sh Terminal
cat > "$bonfyre_agency/inquiry.json" <<'JSON'
{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"send_inquiry","arguments":{"name":"Ada Example","contact":"ada@example.test","message":"Can I bring my bicycle in for a puncture repair?"}}}
JSON
curl --fail --silent --show-error \
  "http://127.0.0.1:$bonfyre_port/mcp/fernhollow" \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $bonfyre_reader" \
  --data-binary @"$bonfyre_agency/inquiry.json" \
  > "$bonfyre_agency/reader-write.json"
jq -e '.result.isError == true' "$bonfyre_agency/reader-write.json"
curl --fail --silent --show-error \
  "http://127.0.0.1:$bonfyre_port/mcp/tallgrass" \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $bonfyre_writer" \
  --data-binary @"$bonfyre_agency/inquiry.json" \
  > "$bonfyre_agency/writer-write.json"
jq -e '.result.structuredContent.delivered == true' \
  "$bonfyre_agency/writer-write.json"
for bonfyre_customer in fernhollow tallgrass juniper; do
  bonfyre site inbox "$bonfyre_customer" --json \
    --home "$bonfyre_agency" --port "$bonfyre_port" \
    > "$bonfyre_agency/$bonfyre_customer-inbox.json"
done
jq -e '.count == 1 and .messages[0].contact == "ada@example.test"' \
  "$bonfyre_agency/tallgrass-inbox.json"
jq -e '.count == 0' "$bonfyre_agency/fernhollow-inbox.json"
jq -e '.count == 0' "$bonfyre_agency/juniper-inbox.json"
```

Tool permission failures return a JSON-RPC result with `isError: true`; the HTTP response itself can be 200. Inspect both layers. A successful enquiry records a message for the operator; it does not send a reply to the customer.

## Update one site and withdraw access

Change Fernhollow's tagline without recreating its identity. Retain the three site records for comparison, then withdraw both tutorial keys and check that the writer stops working.

```sh Terminal
bonfyre site set fernhollow --tagline 'Workshops for all levels' \
  --home "$bonfyre_agency" --port "$bonfyre_port"
for bonfyre_customer in fernhollow tallgrass juniper; do
  bonfyre site show "$bonfyre_customer" --json \
    --home "$bonfyre_agency" --port "$bonfyre_port" \
    > "$bonfyre_agency/$bonfyre_customer-site.json"
  curl --fail --silent --show-error \
    "http://127.0.0.1:$bonfyre_port/mcp/$bonfyre_customer" \
    -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"site_info","arguments":{}}}' \
    > "$bonfyre_agency/$bonfyre_customer-info.json"
done
jq -e '.result.structuredContent.about == "Workshops for all levels"' \
  "$bonfyre_agency/fernhollow-info.json"
jq -e '.result.structuredContent.about == "Keep moving"' \
  "$bonfyre_agency/tallgrass-info.json"
jq -e '.result.structuredContent.about == "Fresh from the neighborhood"' \
  "$bonfyre_agency/juniper-info.json"
bonfyre site key fernhollow --withdraw tutorial-reader \
  --home "$bonfyre_agency" --port "$bonfyre_port"
bonfyre site key tallgrass --withdraw tutorial-desk \
  --home "$bonfyre_agency" --port "$bonfyre_port"
test "$(curl --silent --show-error -o "$bonfyre_agency/revoked-key.json" \
  -w '%{http_code}' "http://127.0.0.1:$bonfyre_port/mcp/tallgrass" \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $bonfyre_writer" \
  --data '{"jsonrpc":"2.0","id":7,"method":"ping"}')" = 401
unset bonfyre_reader bonfyre_writer
bonfyre site stop --home "$bonfyre_agency" --port "$bonfyre_port"
```

The records and response files remain in the printed directory. Only Fernhollow's tagline should change. The existing owner-level regression suite can also be run with `make -C cmd/BonfyreWebAddress site-product-witness`.

## Prepare a customer delivery

These commands establish local identities, content routing and scoped operations. Each business still needs its own typography, page composition, images, navigation, favicon and social previews. A title and accent color do not establish a finished visual identity. Review every customer site on desktop and mobile before delivery.

| Perspective | What it needs to show |
|---|---|
| Customer website | The customer's name, services, content and contact routes |
| Agency operations | The selected customer, deployment, keys and usage |
| Developer CLI | Commands and receipts addressed to that customer |
| MCP client | Site-specific content, arguments and read/write authority |

For a custom domain, verify DNS, certificates, canonical links and assistant metadata before applying the deployment. Local `.localhost` checks do not establish those properties. When billing is connected, verify that offers, commitments, invoices and payment receipts keep the same customer identity. Presentation rollback must preserve received enquiries and financial records.

Continue with [recording-to-workspace](/recording-to-workspace) for a searchable clinic or [request-to-books](/request-to-books) for a booking, invoice and payment record. [Integration and migration](/integration-and-migration) covers importing customer content.
