import { test, expect } from "bun:test";
import { commandParts, execute, itemsFrom, owners, splitCommand } from "../residual-to-work.mjs";
import { decisionRequest } from "../../components/BonfyreDecision/decision-plane.mjs";

const policy = owners();

test("source arguments are tokenized without a shell or command interpolation", () => {
  expect(splitCommand("BONFYRE_CMS_DB='a b' bun worker.mjs --name=\"two words\"")).toEqual([
    "BONFYRE_CMS_DB=a b", "bun", "worker.mjs", "--name=two words",
  ]);
  const parsed = commandParts("BONFYRE_CMS_DB='a b' bun worker.mjs");
  expect(parsed.words).toEqual(["bun", "worker.mjs"]);
  expect(parsed.env.BONFYRE_CMS_DB).toBe("a b");
  expect(() => splitCommand("bun 'unterminated")).toThrow();
});

test("all receipt families preserve their owner and human-line distinction", () => {
  const receipt = {
    schema: "bonfyre-product-close.v1", namespace: "n", unreconciled_under: 2,
    unreconciled_over: 1, net_amount_left: 7,
    residual: {
      TemporalResidual: { value: 5, attribution: "stale" },
      CapitalResidual: { count: 1, requirements: [{ requirement: "C-1", because: "approval" }] },
      NewResidual: { value: 1, attribution: "unknown owner" },
    },
    binding_conditions: [["permit", 4]],
    trials: [{ representation: "compressed", false_prunes: 3, bytes_per_body: 12 }],
  };
  const items = itemsFrom(receipt, receipt.schema, policy);
  expect(items.map(item => item.kind)).toEqual([
    "TemporalResidual", "CapitalResidual", "NewResidual", "BindingCondition",
    "ReconciliationResidual", "PreservationResidual",
  ]);
  expect(items.find(item => item.kind === "CapitalResidual").closable).toBe(false);
  expect(items.find(item => item.kind === "NewResidual").owner).toBe("unattributed");
  expect(items.find(item => item.kind === "ReconciliationResidual").size).toBe(3);
});

test("dry run ranks closable before decisions and never submits", async () => {
  const sourceRunner = async source => ({ receipt: source === "empty" ? { schema: "empty.v1" } : {
    schema: source, residual: {
      CapitalResidual: { value: 80, attribution: "funding" },
      TemporalResidual: { value: 2, attribution: "stale" },
    },
  } });
  const result = await execute(["--from", "one", "--from", "empty"], sourceRunner, policy);
  expect(result.sources_read).toBe(1);
  expect(result.sources_without_a_recognised_residual).toEqual(["empty.v1"]);
  expect(result.buildable.map(item => item.kind)).toEqual(["TemporalResidual"]);
  expect(result.waiting_on_a_decision.map(item => item.kind)).toEqual(["CapitalResidual"]);
  expect(result.written).toBe(false);
  expect(result.submitted).toEqual([]);
  expect(result.decision_frontiers).toHaveLength(2);
  const [buildableCase, waitingCase] = result.decision_frontiers;
  expect(buildableCase.contract).toMatchObject({ program: "work_triage", owner: "Flow",
    information_policy_class: "private" });
  expect(buildableCase.contract.subject_id).toMatch(/^residual:[a-f0-9]{16}$/);
  expect(buildableCase.contract.mask_digest).toMatch(/^[a-f0-9]{64}$/);
  expect(buildableCase.contract.frontier).toEqual([{ id: expect.stringMatching(/^role_[a-f0-9]{16}$/),
    description: "refresh schedule", owner_ref: "ResidualWork:TemporalResidual" }]);
  expect(decisionRequest(buildableCase, "clef-flash").questions).toHaveProperty("phenotype");
  expect(waitingCase.contract.frontier[0].owner_ref).toBe("ResidualWork:CapitalResidual");
  expect(buildableCase.cannot_infer).toContain("a typed model assessment -> candidate admission, source truth, evidence, authorization, effect, or calibrated correctness on an unevaluated domain");
});

test("CLI source subprocess returns JSON without a shell and without a write", async () => {
  const proc = Bun.spawn(["bun", "scripts/residual-to-work.mjs", "--from", "bun scripts/tests/fixtures/residual-source.mjs", "--json"], { stdout: "pipe", stderr: "pipe" });
  const output = JSON.parse(await new Response(proc.stdout).text());
  expect(await proc.exited).toBe(0);
  expect(output.sources_read).toBe(1);
  expect(output.buildable[0].kind).toBe("TemporalResidual");
  expect(output.waiting_on_a_decision[0].kind).toBe("CapitalResidual");
  expect(output.decision_frontiers).toHaveLength(2);
  expect(output.decision_frontiers[0].contract.program).toBe("work_triage");
  expect(output.decision_frontiers[0].contract.mask_digest).toMatch(/^[a-f0-9]{64}$/);
  expect(output.written).toBe(false);
});
