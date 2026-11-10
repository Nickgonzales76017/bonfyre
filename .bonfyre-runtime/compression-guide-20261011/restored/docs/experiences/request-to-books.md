# From request to books

Take a booking request through acceptance, invoicing, a payment record and delivery. Read the resulting journal from the same records the site commands create.

## The operation

A visitor requests a private pottery lesson. The operator accepts the requested time at $85, records payment and marks the lesson delivered. This walkthrough uses an invented business, an `example.test` customer address and a tutorial payment reference in a fresh local store. No bank or payment processor is connected.

The current booking operation allows one accepted request for an offer at a named time. Use a private lesson for this example; group capacity and timezone normalization need separate scheduling rules.

## Create a local workspace

Start in a configured Bonfÿre checkout with the native Site, CMS, Meter, Pay and Finance commands available, plus `curl` and `jq`. Keep the commands in one shell. Port 8914 must be free; change `bonfyre_port` if another local service uses it.

```sh Terminal
make -C cmd/BonfyreWebAddress bonfyre-site
bonfyre_books=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-books-guide.XXXXXX")
bonfyre_port=8914
printf '%s\n' "$bonfyre_books"
bonfyre site new fernhollow --title 'Fernhollow Pottery' \
  --tagline 'Private pottery lessons' --host fernhollow.localhost --no-open \
  --home "$bonfyre_books" --port "$bonfyre_port"
```

The build keeps the `bonfyre site` command attached to the current resolver. The new site should report `resolved: true` and `page_served: true`. Its records live in the directory printed above.

## List a priced lesson

```sh Terminal
bonfyre site offer fernhollow --title 'Private pottery lesson' \
  --text 'One lesson with materials included' --price '$85 per lesson' \
  --bookable --home "$bonfyre_books" --port "$bonfyre_port"

curl --fail --silent --show-error \
  "http://127.0.0.1:$bonfyre_port/mcp/fernhollow" \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_offers","arguments":{}}}' \
  > "$bonfyre_books/offers.json"
bonfyre_offer=$(jq -er '.result.structuredContent.offers[0].id' \
  "$bonfyre_books/offers.json")
jq '.result.structuredContent.offers' "$bonfyre_books/offers.json"
```

The returned offer should carry an amount of `85`, currency `USD` and `bookable: true`. Single quotes preserve the dollar sign in the shell. A price beginning with `$85` supplies a fixed amount; wording such as `from $600` remains descriptive and does not supply an invoice amount.

## Submit a booking request

Issue a local write key for this example and submit the same MCP operation an assistant uses. Keep the key in the shell variable rather than printing it:

```sh Terminal
bonfyre_key=$(bonfyre site key fernhollow --label tutorial-desk --write --json \
  --home "$bonfyre_books" --port "$bonfyre_port" | jq -er '.key')
jq -n --argjson offer "$bonfyre_offer" \
  '{jsonrpc:"2.0",id:2,method:"tools/call",params:{name:"request_booking",arguments:{offer_id:$offer,when:"2026-10-18 14:00",name:"Ada Example",contact:"ada@example.test"}}}' \
  > "$bonfyre_books/request.json"
curl --fail --silent --show-error \
  "http://127.0.0.1:$bonfyre_port/mcp/fernhollow" \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $bonfyre_key" \
  --data-binary @"$bonfyre_books/request.json" \
  > "$bonfyre_books/request-result.json"
bonfyre_request=$(jq -er '.result.structuredContent.reference' \
  "$bonfyre_books/request-result.json")
jq '.result.structuredContent' "$bonfyre_books/request-result.json"
bonfyre site inbox fernhollow --home "$bonfyre_books" --port "$bonfyre_port"
```

The response reports `requested: true`, `confirmed: false` and a reference. The inbox should show Ada and that same reference. No invoice exists yet. The date is a fixed tutorial slot; use the actual agreed date and time for a customer booking.

## Accept the request and inspect the invoice

Acceptance records the commitment, a metered event at the offer's rate, an invoice in Pay and an opportunity in Finance:

```sh Terminal
bonfyre site accept fernhollow --request "$bonfyre_request" --json \
  --home "$bonfyre_books" --port "$bonfyre_port" \
  > "$bonfyre_books/accept.json"
cat "$bonfyre_books/accept.json"
jq -e '.invoiced == true and .finance_opportunity == "recorded"' \
  "$bonfyre_books/accept.json"
bonfyre finance books --pay-db "$bonfyre_books/pay.db" \
  > "$bonfyre_books/books-before.json"
cat "$bonfyre_books/books-before.json"
```

Before payment, expect one invoice, zero payments, `receivable_outstanding: 85000000` and `cash_evidenced: 0`. Finance reports millionths of a dollar, so `85000000` means $85. The opportunity is separate from the journal; creating it does not record cash.

If acceptance reports `invoiced: false`, inspect the failure before continuing. A commitment may already exist even when invoicing failed. Repeating acceptance is not a repair: the commitment owner refuses an already promised slot. The current command does not send a confirmation to the customer.

## Record the tutorial payment

Only use this invented reference in the isolated example store. In a customer operation, record the reference from the received transaction.

```sh Terminal
bonfyre site paid fernhollow --request "$bonfyre_request" \
  --reference TUTORIAL-PAYMENT-001 \
  --home "$bonfyre_books" --port "$bonfyre_port"
bonfyre finance books --pay-db "$bonfyre_books/pay.db" \
  > "$bonfyre_books/books-after.json"
cat "$bonfyre_books/books-after.json"
```

The payment result should report `amount_cents: 8500`. The books should now report one invoice, one payment, zero outstanding receivable and `cash_evidenced: 85000000`. That field reflects the stored payment record; this tutorial does not verify a bank balance.

Check that repeating the operation leaves the records alone:

```sh Terminal
if bonfyre site paid fernhollow --request "$bonfyre_request" \
  --reference TUTORIAL-PAYMENT-001 \
  --home "$bonfyre_books" --port "$bonfyre_port"; then
  printf '%s\n' 'Unexpected success: payment was already recorded.' >&2
  exit 1
fi
bonfyre finance books --pay-db "$bonfyre_books/pay.db" \
  > "$bonfyre_books/books-retry.json"
cmp "$bonfyre_books/books-after.json" "$bonfyre_books/books-retry.json"
```

The repeated payment should be refused, and `cmp` should return without output.

## Record delivery and read the journal

```sh Terminal
bonfyre site done fernhollow --request "$bonfyre_request" \
  --note 'Tutorial lesson completed' \
  --home "$bonfyre_books" --port "$bonfyre_port"
bonfyre site inbox fernhollow --json \
  --home "$bonfyre_books" --port "$bonfyre_port"
bonfyre finance books --pay-db "$bonfyre_books/pay.db" --journal \
  > "$bonfyre_books/journal.tsv"
cat "$bonfyre_books/journal.tsv"
```

Delivery should return `state: discharged` and `delivered: true`. The inbox records the delivery against the original request. The journal has two source-linked entries: the invoice debits receivable and credits invoiced revenue; the payment debits recorded cash and credits receivable. Delivery discharges the commitment without creating a second payment.

## See the business state

| Stage | Native owner | What it means |
|---|---|---|
| Enquiry | WebAddress / CMS | A customer asked for something |
| Accepted booking | Commitment | The business agreed to the work and the time |
| Offer and terms | Offer / Finance | Scope, price and approval belong to the commercial record |
| Invoice | Pay / Meter | An amount is due, with its originating occurrence |
| Received payment | Pay | A payment record references evidence that money arrived |
| Books | Finance | Source-linked journal lines and balances |
| Delivery and retention | Finance / Work | Work delivered and customer relationship continue |

An agency may have several clients in the same software installation. Each client’s service engagement retains its identity and permissions. The physical-goods CommerceChain uses inventory and custody stages; a service booking should not be made to pass through goods stages merely to reuse the word “commerce”.

## Include model costs when recorded

Finance can also read an Economy store through `--economy-db FILE`. Use the store holding the actual model-cost records for the operation. The lesson example creates no model call, so it supplies only its Pay store. A missing Economy store is not a prerequisite for reading invoices and payments.

The books aggregate all records in the supplied stores. A shared site home may contain several customers; this command does not filter the journal to one site. Review the relevant periods, currencies, tax handling and settlement integrations before using the result for business reporting.

## Finish the local example

Withdraw the example key and stop only this workspace's server. Its records remain available for inspection:

```sh Terminal
bonfyre site key fernhollow --withdraw tutorial-desk \
  --home "$bonfyre_books" --port "$bonfyre_port"
unset bonfyre_key
bonfyre site stop --home "$bonfyre_books" --port "$bonfyre_port"
```

## Follow more than the money

One customer relationship may also carry an agency outreach record, a source message, a requirement, a CRM deal and a partner capacity obligation. The opportunity identity connects their views. One status timeline is preferable to several incompatible “customer state” tables.

For a useful operator view, show the request and source, decision or refusal, accepted terms, generated invoice, recorded payment evidence and remaining delivery tasks in time order. Show “unknown” when a cost or payment is not observed rather than presenting a proposed amount as realized margin.

## Adapt the workflow

This sequence applies to classes, repairs, professional services, creative production, contract integrations, subscription operations and program enrollment. Keep the domain-specific decision and entitlement rules; reuse identities, permissions, actions and records.

Continue with [white-label delivery](/white-label-delivery) for partner-operated customers or [integration and migration](/integration-and-migration) when the customer's invoicing and accounting remain in external systems.
