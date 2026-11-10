# SourceFrontier retained source review

`classified-market` runs the collector through the native command and MCP owner.
Install it with `make -C cmd/BonfyreSourceFrontier install-classified-market`.
It supports `refresh`, `replay`, `enrich` and `route` without opening a ledger.

For referral research, `route --evidence FILE[,FILE] --providers FILE --out DIR`
matches retained seller bodies to operator-authored buyer rules. Each rule has
`id`, `name`, `source_url`, `receipt_path`, `terms_stated`, `terms_excerpt`,
`match_any` and optional `match_groups`, `exclude_any` and `review_questions`. At least one
`match_any` term must occur. When `match_groups` is supplied, every group must
also contribute at least one literal term from the retained body. For example,
`[["lot", "pallet", "bulk", "fleet"]]` narrows equipment matches to posts that
also contain one of those terms. This is a reading filter, not a quantity check.
Use `exclude_any` for seller wording that rules out the proposed approach, such
as `"don't need help selling this item"`. It applies to the verified posting
body, with the same literal word boundaries as positive matches. Matching
associations go to `routing-withheld.jsonl` with their receipts and matched
exclusion terms; they do not enter `routing-candidates.jsonl`. The summary
counts matched, candidate and withheld associations separately. A hold is an
operator-authored reading rule, not a definitive interpretation of a seller's
intent. Read the full post before reconsidering it. An empty hold file or an
unmatched phrase does not establish permission to contact.

Set `transaction_model` to `purchase_resale` when seeking a buyer bid against a
separate acquisition. The default `referral` retains introduction and fee
questions. Purchase/resale candidates instead retain the missing seller terms,
buyer bid, transaction costs and payment/delivery conditions. Choose exclusions
for that model: a seller declining sales help has not necessarily refused a
direct purchase. A model label neither authorizes a purchase nor establishes
title, resale rights, margin or funds.

The excerpt must occur in the
provider's retained page. Both parties' observation digests and source identities
are checked. Routing makes no network calls and writes review candidates, with
the fee terms and unresolved commercial questions beside each association.
Input files remain unchanged. `terms_stated` and the matching vocabulary are
reviewer interpretations; a matching excerpt does not validate the whole summary.
The output does not establish program eligibility, compatible equipment or an
accepted introduction. Search-market locations and prices remain index evidence.

MCP example: `native_tool_invoke` with family `BonfyreSourceFrontier` and args
`["classified-market", "route", "--evidence", "/absolute/posts.jsonl", "--providers", "/absolute/buyers.json", "--out", "/absolute/review"]`.

The existing classifieds collector also covers wanted goods and inventory:

```sh
rtk proxy bun cmd/BonfyreSourceFrontier/tools/classified_market.mjs refresh --profile wholesale --markets dallas,houston --out .bonfyre/revenue/wholesale --body-dir .bonfyre/revenue/wholesale/bodies
```

The wholesale profile reads wanted, business inventory, computers, computer parts, electronics, tools and materials. `--frontier DB` imports the resulting candidate channels into the existing SourceFrontier ledger. Goods keep their own demand and supply labels. Displayed prices remain text: a dollar placeholder or an asking price does not establish lot cost or margin. Read the complete post and establish quantity, condition, fulfillment and terms before proposing a transaction. The default profile still covers services; `--markets` limits either profile to named metros.

Enrich selected advertisements through the native ForeignTwin observer:

```sh
rtk proxy bun cmd/BonfyreSourceFrontier/tools/classified_market.mjs enrich --rows .bonfyre/revenue/wholesale/classified-rows.jsonl --out .bonfyre/revenue/wholesale/buyers --side demand --query servers,inventory,overstock,bulk --limit 100
```

Selection rotates across markets and retains alternative addresses for repeated titles. Every selected read goes through the existing public-observation policy and keeps its receipt and exact bytes. The output checks body digests and retains full posting text, dates and post IDs; failures stay visible. Matching body text and post IDs are reported separately. None of these counts is a count of distinct buyers. Inspect the complete terms before matching a seller: the price in an index can describe just one configuration or be a placeholder.

Pass `--evidence FILE[,FILE...]` to reuse earlier enriched JSONL files. Each reused receipt must name the selected source and its retained bytes must still match the digest. Invalid evidence stays a failure; it does not silently trigger another fetch. Add `--offline` to review only retained observations, with zero network calls. Use a separate output directory. Observation dates are preserved, so reuse never makes an old listing appear freshly checked. The summary separates new observations from reused evidence.

Use `discourse-review` when qualifying a prospect from a Discourse topic. It shows the original poster's updates and identifies posts missing from the retained topic response. Read those updates before drafting: a later closure can change the meaning of the opening request.

Through the connected native tool:

```json
{
  "family": "BonfyreSourceFrontier",
  "args": ["discourse-review", "--receipt", "/absolute/path/topic-receipt.json"]
}
```

The receipt must come from a permitted ForeignTwin observation of the topic's JSON endpoint. If `complete_for_retained_stream` is false, `next_observation_urls` lists candidate reads. Observe each needed URL through `foreign_twin_observe`, then add each resulting receipt with `--supplement FILE`. The review command does not fetch those URLs.

`original_poster_posts` excludes other applicants' pitches. Its HTML is inert source text. Read a truncated buyer post in full with `--post ID`, using its post ID rather than its displayed post number. Check the observation dates; complete coverage describes the retained stream, not the current remote thread. An open forum topic does not establish that work is still available.

The reader checks body digests, topic identity, stream membership and author identity. It returns nonzero for invalid evidence or a missing runtime. It does not open the Frontier ledger, submit applications or send messages.

To review a board before choosing threads, observe its category endpoint ordered by creation, such as `https://community.n8n.io/c/jobs/13/l/latest.json?order=created`, through ForeignTwin. Pass that receipt to the same reader with `--board`:

```json
{
  "family": "BonfyreSourceFrontier",
  "args": ["discourse-review", "--board", "--receipt", "/absolute/path/board-receipt.json"]
}
```

The board review accepts nested category paths too, such as `https://community.make.com/c/hire-a-pro/hire-help/74/l/latest.json?order=created`. It checks every topic against the final category ID, sorts by original posting date and preserves reply activity separately. Each topic has a JSON observation URL for the thread review above. It covers one retained page. Titles can describe someone seeking work, and recent replies can belong to applicants on an old request; neither establishes buyer demand. The source's pagination link is retained as evidence, never fetched by this reader.

Build and install the native entry point and reader:

```sh
rtk proxy make -C cmd/BonfyreSourceFrontier install-discourse-review
```

The default destination is `~/.bonfyre`. Use `MANAGED_PREFIX=/path` to stage an installation. Runtime overrides are `BONFYRE_DISCOURSE_READER` and `BONFYRE_BUN_BINARY`; these are operator configuration, not source-page input.

Focused checks:

```sh
rtk proxy bun test cmd/BonfyreSourceFrontier/tests/discourse_thread.test.mjs scripts/tests/native_command_tables.test.mjs
```

The reader suite is also included in this owner's `make test` target.
