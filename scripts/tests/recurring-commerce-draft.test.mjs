import { test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { basketPreview, compileDraft, cycleDates, nativeRecords, selectItems, stockDecision, tryOnCheckout } from "../recurring-commerce/draft.mjs";

const catalog = JSON.parse(readFileSync(join(import.meta.dir, "../recurring-commerce/catalog.json"), "utf8"));
const of = pattern => catalog.templates.find(template => template.pattern === pattern);
const terms = more => ({ currency: "USD", price_cents: 4999, shipping_cents: 500, max_members: 40, start_date: "2026-01-31", cadence: { unit: "month", every: 1 },
  auto_renew: true, cancellation: "any time before the cycle closes", return_policy: "refund on damage", ...more });
const draft = (template, more = {}, id = "draft:" + template.id) => compileDraft(catalog, { program_id: id, template_id: template.id, owner_subject: "person:test", name: template.name, terms: terms(more) });

test("a monthly anchor on the 31st lands on each month's last day and returns to the 31st", () => {
  expect(cycleDates("2026-01-31", { unit: "month", every: 1 }, 4)).toEqual(["2026-01-31", "2026-02-28", "2026-03-31", "2026-04-30"]);
  expect(cycleDates("2028-01-31", { unit: "month", every: 1 }, 2)[1]).toBe("2028-02-29");
  expect(cycleDates("2026-03-02", { unit: "week", every: 2 }, 3)).toEqual(["2026-03-02", "2026-03-16", "2026-03-30"]);
  expect(() => cycleDates("2026-02-30", { unit: "month", every: 1 }, 1)).toThrow("Invalid calendar date");
  expect(() => cycleDates("2026-01-01", { unit: "usage", every: 1 }, 1)).toThrow("stock evidence");
});

test("a styling fee already paid is credited against what is kept, and is not a transferable credit", () => {
  expect(tryOnCheckout(2000, [{ price_cents: 5000, decision: "keep" }, { price_cents: 8000, decision: "return" }]))
    .toMatchObject({ kept_items_cents: 5000, applied_credit_cents: 2000, remaining_balance_cents: 3000, total_paid_cents: 5000, status: "calculation_only_no_charge" });
  expect(tryOnCheckout(2000, [{ price_cents: 5000, decision: "return" }])).toMatchObject({ kept_items_cents: 0, applied_credit_cents: 0, remaining_balance_cents: 0, total_paid_cents: 2000 });
  expect(tryOnCheckout(2000, [{ price_cents: 1500, decision: "keep" }])).toMatchObject({ applied_credit_cents: 1500, remaining_balance_cents: 0, total_paid_cents: 2000 });
  expect(() => tryOnCheckout(2000, [{ price_cents: 5000, decision: "maybe" }])).toThrow("Unresolved");
});

test("replenishment is proposed from a recent stated observation and never grants a charge", () => {
  const stock = { basis: "measurement", observed_on: "2026-10-01", quantity: 300, daily_use: 30, lead_days: 5, buffer_days: 2 };
  expect(stockDecision(stock, "2026-10-03")).toMatchObject({ days_remaining: 8, proposal: "keep_current_stock", permission_to_charge: false });
  expect(stockDecision({ ...stock, quantity: 240 }, "2026-10-03")).toMatchObject({ days_remaining: 6, proposal: "review_reorder", permission_to_charge: false });
  expect(() => stockDecision(stock, "2026-10-20")).toThrow("stale");
  expect(() => stockDecision({ ...stock, basis: "guess" }, "2026-10-03")).toThrow("observation basis");
  expect(() => stockDecision({ ...stock, observed_on: "2026-10-05" }, "2026-10-03")).toThrow("future-dated");
});

test("selection screens on repeats, exclusions, heat and verified stock, and missing ingredients are not a pass", () => {
  const items = [
    { id: "a", ingredients: ["chili", "garlic"], heat: 3, available_units: 5 },
    { id: "b", ingredients: ["chili", "peanut"], heat: 2, available_units: 5 },
    { id: "c", heat: 2, available_units: 5 },
    { id: "d", ingredients: ["chili"], heat: 9, available_units: 5 },
    { id: "e", ingredients: ["chili"], heat: 1, available_units: 0 },
    { id: "f", ingredients: ["chili"], heat: 1, available_units: 2 },
  ];
  const out = selectItems(items, { no_repeats: true, received_ids: ["a"], excluded_ingredients: ["peanut"], max_heat: 5 });
  expect(out.eligible.map(item => item.id)).toEqual(["f"]);
  expect(Object.fromEntries(out.rejected.map(row => [row.id, row.reasons]))).toEqual({ a: ["already_received"], b: ["ingredient_excluded"], c: ["ingredients_unverified"], d: ["heat_outside_preference"], e: ["no_verified_inventory"] });
  expect(out.meaning).toContain("not an allergen-safety certification");
});

test("a finite gift ends, cannot auto-renew, and is counted once rather than every shipment", () => {
  const gift = of("finite_gift");
  expect(() => draft(gift, { gift_cycles: 3 })).toThrow("without auto-renewal");
  const made = draft(gift, { gift_cycles: 3, auto_renew: false });
  expect(made.cycle_preview).toHaveLength(3);
  expect(made.cycle_preview.every(cycle => cycle.effect === "none")).toBe(true);
  const preview = basketPreview([made], 20000);
  expect(preview.months).toEqual([{ month: "2026-01", modeled_cents: (4999 + 500) * 3, over_budget: false }]);
  expect(preview).toMatchObject({ complete_price: true, taxes: "not_quoted", checkout: "not_connected" });
});

test("a budget preview leaves variable programs unpriced and skips a requested pause", () => {
  const box = draft(of("discovery_box"), { pause_dates: ["2026-02-28"] });
  expect(box.cycle_preview.find(cycle => cycle.due_local_date === "2026-02-28").state).toBe("skip_requested");
  const tryOn = draft(of("try_before_buy"), { style_fee_cents: 2000 });
  const preview = basketPreview([box, tryOn], 5000);
  expect(preview.months.map(row => row.month)).not.toContain("2026-02");
  expect(preview.months[0]).toEqual({ month: "2026-01", modeled_cents: 5499, over_budget: true });
  expect(preview.variable_priced_programs).toEqual([tryOn.program_id]);
  expect(preview.complete_price).toBe(false);
});

test("a draft says what it has not got, and only replenishment may wait on stock", () => {
  const made = draft(of("recurring_service"));
  expect(made).toMatchObject({ status: "draft", readiness: { state: "configured_draft" } });
  expect(made.readiness.unresolved).toEqual(["merchant_identity_and_authority", "fulfillment_capacity", "billing_provider", "buyer_consent", "tax_and_delivery_quote"]);
  expect(made.stage_plan.every(stage => stage.state === "not_executed")).toBe(true);
  expect(() => draft(of("discovery_box"), { cadence: { unit: "usage", every: 1 } })).toThrow("Only replenishment");
  expect(draft(of("replenishment"), { cadence: { unit: "usage", every: 1 } }).cycle_preview).toEqual([]);
  expect(() => draft(of("discovery_box"), { cancellation: " " })).toThrow("cancellation");
  expect(() => compileDraft(catalog, { program_id: "x", template_id: of("hybrid").id, owner_subject: "nobody", name: "abc", terms: terms() })).toThrow();
});

test("the catalogue maps to the same native records the loader wrote", () => {
  const records = nativeRecords(catalog);
  expect(records.profiles).toHaveLength(10);
  expect(records.templates).toHaveLength(36);
  expect(new Set(records.templates.map(row => row.archetype)).size).toBe(8);
  expect(records.templates.every(row => row.page_policy === "no_dedicated_page_required" && row.funding_candidates.length === 0)).toBe(true);
  expect(records.profiles.every(row => row.cannot_infer.includes("Not affiliated"))).toBe(true);
  expect(catalog.bundles).toHaveLength(12);
  for (const bundle of catalog.bundles) for (const id of bundle.programs) expect(catalog.templates.some(template => template.id === id)).toBe(true);
});
