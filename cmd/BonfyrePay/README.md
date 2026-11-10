# BonfyrePay

Invoicing and payment recording against metered usage: invoices from
admitted rates, partial payments with idempotent references, paid and
pending states. Records money movement; moves none itself.

## Build, test, install

```sh
make -C cmd/BonfyrePay
make -C cmd/BonfyrePay test
make -C cmd/BonfyrePay install MANAGED_PREFIX=~/.bonfyre
```

Tests bill priced and unpriced events, refuse empty invoices, record
partial payments, reject double-counted references, and replay behavior
vectors — all against throwaway meter fixtures.

## Use

```sh
bonfyre-pay --db FILE invoice --user-id ID --api-key K --period P --meter-db METER
bonfyre-pay --db FILE pay --invoice-id ID --amount CENTS --method bank --reference REF
```

## Contract and limits

- An invoice line is a metered event priced by the rate the meter admitted
  when recorded; nothing is priced by operation name.
- No priced events in the period means no invoice row.
- An invoice is not a payment request sent, a settlement, or cash. Payment
  references below are test-written; no processor or bank is asked.

No version is pinned and none is published as a package yet. The live
contract is `cmd-pay` in the Architecture Atlas.
