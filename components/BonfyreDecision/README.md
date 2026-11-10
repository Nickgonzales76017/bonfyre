# Bonfÿre DecisionPlane

DecisionPlane is a shared typed-judgment boundary over finite, exact-owner
frontiers. It is not a new source of truth or an effect engine. Its common CUE
programs and Bun runtime project ContextCut passages, read-only MCP tool
shortlists, and Flow work-role candidates into a state plus atomic Noul,
Choice, or Score questions. Ordinary assessment now selects the installed,
pinned native Clef-Flash owner; hosted Clef or Jev requires explicit selection.
A model response becomes a digest-bound diagnostic
receipt containing every returned option probability, not an authorization.
For ContextCut, the provider sees purpose and chunk text, not the candidate ID;
identical content under the same owner mask, program and policy/gate can share one provider assessment
within a bounded run while each exact candidate keeps its own receipt.

The ContextCut compiler writes a purpose-bound reconstructable decision
frontier without duplicating source text. To plan its cases without a network
call:

```sh
bun components/BonfyreDecision/decision-plane.mjs plan-context \
  context.json planned.jsonl
```

Ordinary ContextCut compilation now avoids tokenizing ranked suffix bodies that
cannot enter a full candidate-capped prefix. When missing or over-budget prefix
material makes the suffix reachable, one fallback batch preserves the previous
selection. The manifest reports native batches, source bytes encoded and exact
pruned bodies. This is deterministic eliminated work, not model relevance or a
new opt-in assessment; all material receipts are still validated.

Executing a hosted assessment is separate and requires public-classified state,
credentials, and an explicit maximum number of provider calls:

```sh
bun components/BonfyreDecision/decision-plane.mjs run-context \
  context.json diagnostic.jsonl clef-flash 20
```

Local execution uses the same command with `local` instead of `clef-flash`.
It supports private state without hosted egress and never falls back to a
remote model. The explicit call budget still applies. The shared installed
owner verifies the complete model and native binary SHA-256 before starting
one authenticated loopback child. Concurrent callers share startup and a
serialized execution lane; idle ownership ends after 60 seconds. CLI shutdown
awaits child termination. Planning reads only the small runtime descriptor,
not model weights.

The native runtime is pinned by `native/clef-runtime.lock.json`: llama.cpp's
unmerged Clef implementation at `1e265f2111a894c85560d667216d4dd7930474dc`,
and `ggml-org/Clef-Flash-GGUF` at
`4a7a08c09bc63baf043b62b5ba89dd67a0357d95`. The 6,486,448,192-byte
`Clef-Flash-Q4_K_M.gguf` includes both the post-trained backbone and decision
head; an ordinary Qwen backbone is not interchangeable. The current build
passport is macOS arm64-specific. Other builds require a reviewed binary
passport, not a relaxed hash check. This path is text-only, one joint sequence,
with a 2,048-token context/microbatch bound and no silent truncation.
The local build carries a hash-pinned Clef-only loader patch: `output.weight`
is consumed by embedding row lookup, so its buffer must support `GET_ROWS`,
not only matrix multiplication. The unpatched CPU path could select an
incompatible repacked buffer. Other architectures retain their original
matrix-multiplication layout. Placement is explicitly CPU on this machine;
the initial Metal path exhausted GPU memory alongside the existing Qwen
service. The existing service was not stopped or replaced.
Its TypeSafe-style uniform-adjusted confidence is recorded explicitly, not
misrepresented as Cloudflare's maximum-option probability. Quantization and
native kernels do not establish BF16 reference parity.

`tests/trained-runtime-witness.mjs` exercises the installed default without an
injected evaluator: ContextCut, MCP, Flow, a repeated ContextCut assessment and
ExternalEstate source review. The real-run receipt is retained in
`tests/trained-runtime-witness.json`, including hardware, model/implementation
identity, full answers and per-case timings. It uses private authored state,
refuses any hosted fetch, checks that each assessment actually executed rather
than hitting a result cache, and joins native shutdown before reporting success.
These five integration examples are not held-out quality or throughput evidence.
Run it explicitly; the offline unit suite never loads the trained weights:

```sh
bun components/BonfyreDecision/tests/trained-runtime-witness.mjs \
  components/BonfyreDecision/tests/trained-runtime-witness.json
```

The text-only admission boundary distinguishes question/option names from
media-bearing state: an option called `images` is legitimate text. Unknown
question-descriptor fields are refused instead of being silently discarded by
the native parser. State media refusal, reserved-marker checks and request
size/depth bounds still apply.

`decision-cut.mjs` can reuse verified ContextCut token spans to emit a smaller
purpose-specific representation from diagnostic receipts. The original context
and omitted candidate identities remain recoverable. A derived cut stays
`diagnostic_only` unless the original rank was ADMIT and distinct, labeled,
held-out cases show an acceptable false-omission rate and positive measured
end-to-end savings for the same model passport. Even then the output is only an
`execution_candidate`; no effect or truth boundary changes.
The decision-cut CLI accepts held-out evaluation rows and an explicit
evaluation policy; without both, it can only write `diagnostic_only`.

The live MCP gateway now exposes `decision_tool_shortlist` over its current
read-only catalogue. It returns a non-effecting plan or diagnostic proposal;
it never calls the proposed tool. `execute=true` uses the default local owner
unless a hosted provider is explicitly selected. Information-policy classification is supplied by the authenticated
caller; the gateway cannot infer whether arbitrary intent text is public. Its
call ledger retains only the intent digest and byte count. The normal
residual-to-work planner also emits private `Flow/work_triage` cases from its
CUE owner mapping, bound to the full source receipt. It does not call a model or
change goal submission; native WorkGraph execution remains separate.
Held-out labels and cost measurements are supplied by the
evaluator; the current fixture only proves that the gate enforces the supplied
evidence, not that a production corpus is calibrated.

Normal `ObligationWork.lowerEnactment` now also exposes a compact planned
decision fingerprint on each returned tuple's `decision_frontier` property.
The existing three-item tuple and its serialized shape are unchanged.
`decisionFrontier(contract, commitmentId)` reconstructs the case from current
commitment events, the existing queue node and authored protocol roles; no
second decision-state table or duplicated work body is stored.
`assessDecisionFrontier` uses the shared pinned local evaluator and rechecks the
exact read after inference. A changed queue/commitment invalidates the proposal.
It does not claim the work, reassign its committer or discharge an obligation.

The important negative result remains open: no Bonfÿre production corpus has
yet demonstrated that this decision layer avoids enough downstream work to
outweigh model latency and cost. The code makes that value claim testable and
refuses to promote a provider benchmark into a Bonfÿre measurement.

## Compiled decisions, not interchangeable provider replies

The v2 runtime makes typed decisions an executable intermediate representation.
CUE owns questions, output projections and relative diagnostic costs. The Bun
compiler lowers them to bounded, cached column-offset programs. All normal
`assessDecision` calls now execute those programs; ContextCut, MCP and Flow's
adapter do not have separate hand-coded proposal algorithms. ExternalEstate
uses the same provider registry, probability decoder and bounded scheduler.
Flow's production work executor and the Source Fabric cycle are **not** newly
model-driven by this change.

ExternalEstate's `decision_program_plane.mjs` now also defaults to this same
installed owner and execution lane, instead of creating another backend.
Its CLI requires `--execute --limit N --max-calls N` for execution. Hosted
selection additionally requires `--provider jev|clef|clef-flash` and explicit
public-state classification; admission or a public-looking URL is not an
egress grant. Hosted retries consume the same total call budget. Source review
remains diagnostic and does not become a recurring Source Fabric step without
quality and avoided-work evidence.

The runtime retains all option probabilities in contiguous `Float64Array`
columns. It checks rounded mass, maximum-probability choices and ordinal
expectations, and records entropy, margin and the original provider confidence.
It does not normalize malformed replies. Conjunctions use Fréchet bounds, not
an invented product of marginal probabilities. MCP and Flow suggestions use
authored expected-loss comparisons with an explicit review outcome; equal
risks or a review signal defer. These are diagnostic costs, not calibrated
financial costs or authority to run the suggested operation.

An embedded owner can supply `localEvaluator` with `model`, `artifact_digest`,
`question_semantics`, optional `confidence_semantics`, and
`evaluate({state, questions, schema, signal})`, returning finite
`Float32Array`/`Float64Array` logits in schema field/option order or validated
`{answers}` from a native SystemOne runtime. Typed answers preserve exact zero
probabilities without a synthetic log/softmax round trip. Stable
per-field softmax lowers these through the same program without a network
call. Local receipts and ContextCuts retain artifact-bound model passports.
The callback is trusted in-process code, not a sandbox; cancellation is
cooperative. The standalone C head described below implements the published
head computation with fixture-backbone tests. The separate installed GGUF
runtime described above carries the real post-trained backbone and head;
these are distinct realizations, not a claim that the standalone C binding
has gained a trained backbone. No model was trained in this change.

The scheduler preserves record order, deduplicates only compatible cases,
cancels sibling calls after failure and stops new work. ExternalEstate now
issues one record per source, with bounded concurrency. This increases its
request count relative to the old multi-source batches; it establishes the
correct isolation boundary, not a speedup claim.

Deterministic artifact-bound local evaluators now reuse **complete joint-request
logits** across ordinary assessments. This is a default shared execution feature,
not a separate ContextCut/MCP/Flow command. The local owner must explicitly
declare `deterministic: true`; a model hash alone does not rule out dropout or
sampling. CUE bounds residency to 128 entries and 1 MiB of logit payload per
evaluator. Requests, private text and authority receipts are not retained.
The current owner contract is revalidated and projected anew on every call.
Request, model, owner, mask, gate, information policy and program changes
invalidate reuse. Cancellation and failed inference never populate the cache.
`clearLocalDecisionInference(owner)` evicts an owner's retained logits.
Receipts distinguish computation from reuse; batch summaries count actual local
executions separately. This is an in-process derived cache, not a durable store,
an RSS sandbox, or a hosted-provider cache.
Calls sharing one local owner execute in a serial lane through output snapshot;
independent owners can run concurrently. Native prefill adapters also share a
lane per backend, so two adapters cannot overwrite each other's recycled
activations. Aborting a caller does not prematurely release a running backend.
Timeouts are cooperative: an owner that ignores cancellation can keep its lane
occupied. Owner identity changes during queued or active inference are refused.

Recipe changes invalidate old receipts. Held-out ContextCut samples cannot
reuse a target request, body digest or source identity under a new subject ID.
Neither a hosted alias nor a distinct provider-reported model string proves an
immutable revision. Hosted cuts remain diagnostic; executable promotion
currently requires locally pinned model artifacts.

## Source review and consequences

Reviewed the complete released
[Clef-flash implementation at revision 17f0b0a](https://huggingface.co/Cloudflare/clef-flash/blob/17f0b0ad64efb65d273590632833508766b2aae6/joint_schema_model.py)
and [head configuration](https://huggingface.co/Cloudflare/clef-flash/blob/17f0b0ad64efb65d273590632833508766b2aae6/joint_head_config.json):

- `encode_record` builds explicit question/option token spans. Its default is
  16,384 tokens and it truncates state to fit. A local port must make omitted
  state explicit rather than silently treating a partial input as a whole cut.
- The head routes option queries into backbone memory, summarizes options,
  then runs cross-field decoder attention. It combines a lexical embedding
  prior, cosine scoring and a learned residual. It is not just a replacement
  softmax or a vocabulary-constrained text decoder.
- Flash's head uses hidden size 4096, width 1024, two evidence layers, four
  decoder layers and 16 heads. The backbone is Qwen3.5-9B. A Qwen2.5 runtime
  cannot be assumed compatible. The loader disables the normal generation KV
  cache; existing token/KV pages are not automatically cached head state.
- `systemone_answer` uses maximum option probability as confidence and rounds
  results to four decimals; `systemone` echoes the requested model name.

[Cloudflare's training account](https://blog.cloudflare.com/clef-decision-models/)
describes smoothed cross-entropy, Brier loss, schema permutations and RLCD.
Those are training mechanisms, not calibration guarantees for Bonfÿre data.
[TypeSafe's primer](https://docs.typesafe.ai/introduction/machine-learning-primer)
describes RLCD, but does not publish Jev's weights or internal model source.
The available implementations reviewed were its
[server transport](https://github.com/TypeSafeAI/typesafe-playground/blob/main/lib/serverJev.ts)
and [pinned proposal-review harness](https://github.com/TypeSafeAI/typesafe-playground/blob/main/lib/vendor/jev-harness/src/benchmark/run.ts),
alongside its independent-question and confidence contracts. The harness
explicitly records pending proposals without applying them; it is not evidence
of an autonomous production executor.

Consequently, the shared API shape does not permit us to equate field coupling,
confidence statistics, model identity, calibration, or caching semantics.

## Shared outcome and promotion algebra

Every new assessment binds its validated answer distribution with an
`answers_digest`. The common receipt validator checks that distribution,
current question program, source case, owner contract and complete request
before downstream evaluation or ContextCut consumption. This is corruption
detection, not a signature or proof that a caller-supplied label is true.

`evaluateDecisionOutcomes` evaluates externally labeled ordinary receipts from
any shared program, including MCP and Flow. It reports categorical Brier loss
(sum-of-squares convention, 0–2), log loss, normalized ordinal ranked probability
score, per-field accuracy and exact-record accuracy. Zero probability on the
observed answer is retained as an explicit infinite-loss count, not clipped
away. Results are partitioned by model passport, program, question schema,
owner and information policy. Repeated requests, repeated evaluation units,
target-set overlap, stale programs and changed answers are refused. External
label receipts bind labels, case, evaluation unit and source reference;
their hashes are verified, but source truth still needs a trusted evaluator. Reports
retain evidence hashes and metrics, not private source text. These losses are
evaluation primitives inspired by the published training objectives, **not an
implementation of Cloudflare's or TypeSafe's unpublished RLCD trainer**.

The same outcome pass now replays authored choice-versus-review loss tables.
It reports candidate suggestions, review deferrals, wrong suggestions,
review-required bypasses, realized relative loss and the always-review baseline.
Positive saved relative-loss units do not imply saved milliseconds or money;
those require independent operational measurements. Supplied score legends must
match the authored rubric. Missing legends remain accepted for internal/native
answers; this compatibility is not full hosted Jev wire conformance.

The default selective gate and ContextCut promotion now use a one-sided exact
binomial upper risk bound, not only the observed error fraction. For example,
zero errors in one accepted case has a 95% upper risk bound of 95%; it is not a
zero-risk result. Policy must explicitly declare
`sampling_assumption: "fixed_policy_independent_holdout"`; CUE defaults
`risk_alpha` to 0.05. Labels and timings still need a trusted evaluator, and
independence/representativeness cannot be proved from distinct hashes.
Threshold tuning on the same sample or repeated adaptive promotion attempts
needs a separate statistical design. No finite sample establishes zero risk.
Full decision plus fallback cost remains part of the gate.
ContextCut evaluation currently admits only complete, hash-verified single-body
examples and refuses duplicate body/source identities. Chunked evaluation
needs a grouped whole-body design; treating every chunk as an independent
trial is explicitly refused. Multi-chunk target cuts remain supported.

Research basis: [proper scoring rules](https://sites.stat.washington.edu/people/raftery/Research/PDF/Gneiting2007jasa.pdf)
and [exact binomial confidence limits](https://itl.nist.gov/div898/software/dataplot/refman2/auxillar/exacbino.htm).

Verification:

```sh
bun test components/BonfyreDecision/tests \
  cmd/BonfyreExternalEstate/tests/decision_program_plane.test.mjs \
  integrations/mcp/tests/recursive_overlay.test.mjs
```

## Native joint head (not another provider adapter)

`libbonfyre` now includes `bf_decision_head.h` / `bf_decision_head.c`. They
implement the released head's option evidence routing, weighted summaries,
pre-norm cross-field decoder, lexical prior, cosine term and gated residual
scorer. The shape configuration supports different hidden widths, head counts,
feed-forward widths and layer counts. The native layout owns exact upstream
tensor names, ranks and offsets; the Bun importer does not maintain a second
weight-layout schema. Apache attribution is under `native/`.

The C operation takes complete backbone hidden states, schema spans and pooled
**output-embedding** option vectors. It emits logits for every allowed option.
It does not generate text, open a network connection or import Python. Native
callers use its C ABI; Bun uses stable Node-API, not experimental FFI. Imported
weights are snapshotted once. The kernel uses caller-owned scratch with no
per-call heap allocation, streams attention softmax without a query-by-token
score matrix, and reuses scratch between layers. The binding allocates small
field/span descriptors and the output, while the evaluator reuses its arena.

Repeated inference now uses an opaque C-owned immutable model snapshot through
the existing Node-API binding. Its copied weights are checked once, instead of
rescanning the entire head per request. The raw pointer-based C ABI still
validates mutable weights on every invocation. Option-summary exponentials
are computed once, and the residual scorer packs up to 32 options per matrix
multiply, including across field boundaries. Scratch stays bounded; the
wide-option benchmark trades 672,256 for 828,928 workspace bytes.

Three interleaved synthetic native-head trials measured median-of-medians
improvements of **2.16×, 2.20× and 4.47×** across short-schema, wide-option and
large-head/short-input workloads. Import, tokenizer, backbone and application
latency are excluded. Full timings, source/binary hashes and caveats are in
`tests/native-head-benchmark.json`; `make -C native benchmark` compares the
current raw and immutable-owner paths. These are not trained-model benchmarks.

```sh
make -C components/BonfyreDecision/native
bun test components/BonfyreDecision/tests/native-head.test.mjs
make -C components/BonfyreDecision/native sanitize
```

`compileNativeDecisionHead(config, namedTensors)` binds F32 head tensors.
`createNativeDecisionEvaluator({model, head, backbone})` supplies the existing
shared `localEvaluator` interface. The trusted backbone owner must implement
`encodeAndForward`, returning hidden states, lexical option means, typed spans,
the complete request digest, encoder/backbone digests and `truncated: false`.
The evaluator validates and maps option identities back into the shared schema;
its model passport includes the head, backbone, encoder and native binary hash.
It refuses excessive token/workspace residency and never falls back to hosted
inference. Native execution is synchronous; cancellation is checked before and
after it, not a preemptive execution limit.

`importNativeClefHead({configPath, weightsPath, expectedConfigDigest,
expectedWeightDigest})` now imports the released standalone
`joint_head.safetensors` layout directly into that C operation. It supports
F32/F16/BF16 storage, retains exact source-file SHA256s and refuses malformed
headers, duplicate keys, layout drift, gaps, overlaps, nonfinite values and
concurrent file changes. It does not load pickle, execute model-repository code,
download weights or start Python. The published Flash head expands to
487,051,280 F32 bytes. The current compiler temporarily holds roughly three
decoded copies; import budgets account for that, without claiming a hard RSS
limit or zero-copy weight loading.

`createClefTextEncoder` ports the pinned release's text segmentation and
question/option spans. It requires a pinned tokenizer and refuses truncation,
media, unsafe integers, fractional JSON numbers and negative zero. These are
explicit supported-input restrictions, not full upstream processor parity.
For deterministic tokenizers it now compiles complete ordered schema token
programs once and reuses them by default, rather than tokenizing every question,
option and fixed template for each new state. Bounds are configurable through
`maxCompiledSchemas` and `maxCompiledSchemaBytes` (64 and 1 MiB by default).
The latter charges serialized program payload, not total JS heap usage.
`residency()` reports compilation/reuse and retained payload bytes;
`clearResidency()` evicts them. No state text or state token IDs enter this cache.
Changing a rubric or field order recompiles it. Warm calls still enforce all
state, byte and token budgets, and returned arrays cannot mutate the program.
The 100-record fixture preserves every token/span with **126 tokenizer calls
instead of 2,700**; this is an encoder-work measurement, not full-model speedup.

`createNativeDecisionBackbone({encoder, runtime})` joins it to a compatible
prefill owner with `forward({input_ids, signal})` and
`gatherOutputEmbeddings({token_ids, signal})`. Both return F32 arrays. The bridge
gathers only distinct option-token **output** embeddings, computes their span
means and feeds the existing head—no autoregressive answer generation or full
sequence-by-vocabulary logit matrix. Its activation budget is checked before
calling the backend. The encoder and runtime must bind the same model artifact;
deterministic reuse is enabled only when both declare it. This interface still
needs an actual compatible trained Qwen3.5 runtime; the existing Qwen2.5 runtime
is not substituted.

The native boundary reads real typed-array backing buffers without invoking
replaceable `.buffer` getters. Both raw Node-API and wrapped calls reject shared
or detached storage, including spoofed properties; intrinsic helpers must be
trusted at module initialization. Resource limits are also substantive: the
default 256 MiB head arena does not support Flash's full upstream 16k sequence
length. A compatible backbone, head arena and activation admission must be
budgeted together; a token-limit setting alone cannot make that allocation fit.

ContextCut, MCP and Flow contract tests now execute this actual C head through
the shared assessment path, using synthetic weights and an explicitly synthetic
backbone. They compare scalar and Accelerate results against an independently
expressed double-precision dense reference, check field-permutation equivariance
and cross-field dependence, and test buffer/shape/provenance refusal. The
standalone sanitizer witness covers 183 raw/immutable-owner cases. The normal
tests have no Python dependency.

Additional witnesses import synthetic safetensors into actual C execution,
exercise every finite F16/BF16 bit pattern, and run the released text-layout
encoder through token-based fixture prefill into the native head. In the shared
reuse test, 33 ContextCut/MCP/Flow assessments execute the head three times;
that proves avoided duplicate computation on the fixture, not production
throughput, accuracy, or cost savings.

**Not yet demonstrated:** upstream PyTorch numerical parity, production
trained-weight accuracy or system-wide production speedup. A temporary PyTorch
oracle install was removed, not added as a repository dependency. The standalone
C binding still needs a compatible Qwen3.5 prefill owner; existing Qwen2.5
token/KV state cannot simply be passed into that head. The installed quantized
runtime has its own implementation and artifact passport. Shared assessment
defaults to local execution, but exact source admission, work execution and
effect authorization do not become model-controlled.

All ordinary shared decision results additionally expose TypeSafe's public
categorical concentration and ordinal concentration statistics, separately from
peak probability and the unchanged provider confidence. These measures describe
distribution shape; they are not empirical calibration or correctness claims.

Sources: [Cloudflare Clef model interface](https://developers.cloudflare.com/workers-ai/models/clef/),
[Clef-flash released model card and implementation](https://huggingface.co/Cloudflare/clef-flash),
[TypeSafe System One](https://docs.typesafe.ai/introduction),
[confidence semantics](https://docs.typesafe.ai/confidence), and
[RAG passage recipe](https://docs.typesafe.ai/cookbooks/classifying_rag_passages).
