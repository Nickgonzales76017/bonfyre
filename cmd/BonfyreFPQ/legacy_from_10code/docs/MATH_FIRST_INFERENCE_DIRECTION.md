# Math-First Inference Direction (Superseded)

> **This document has been superseded by [SPECTRAL_LATTICE_INFERENCE.md](SPECTRAL_LATTICE_INFERENCE.md).**
>
> The theory below was the initial exploration. The full mathematical treatment,
> including proved theorems, bandwidth analysis, and quality bounds, is in the
> Spectral Lattice Inference document.

## Original Core Claim (Preserved for History)

The usual path is:

1. decode FPQ seed programs into dense Cartesian weights
2. run standard GEMM

The opposite path is:

1. keep weights as programs
2. map activations into the same program/projection domain
3. compute logits from domain interactions directly

If this works, native FPQ inference is solved mathematically first, then engineered.

## Reframing the Layer Equation

Standard linear layer:

$$y = W x$$

Current FPQ interpretation:

$$W = \mathrm{Decode}(S, Q, \sigma)$$

So inference is:

$$y = \mathrm{Decode}(S, Q, \sigma) x$$

This forces full or partial reconstruction into Cartesian coordinates.

## Opposite-Direction Formulation

Represent each row vector $w_i$ by a compact FPQ object:

$$\mathcal{W}_i = (S_i, q_i, r_i)$$

where:

- $S_i$ is the seed program (combinator tree)
- $q_i \in \{-1,+1\}^m$ are projection bits
- $r_i$ is radius/scale

Instead of reconstructing $w_i$, define a feature lift for activations:

$$\Psi(x) = \big[\psi_1(x), \dots, \psi_m(x), \chi_1(x), \dots, \chi_p(x)\big]$$

with:

- projection channel: $\psi_j(x) = \mathrm{sign}(\langle \phi_j, x \rangle)$
- seed channel: $\chi_k(x)$ are direct responses of basis operators induced by seed primitives

Then define row scoring in FPQ space:

$$\hat{y}_i = r_i \cdot \Big(\alpha \cdot \frac{1}{m} \sum_{j=1}^{m} q_{ij} \psi_j(x) + \beta \cdot G(S_i, x)\Big)$$

where $G(S_i,x)$ evaluates seed-program interactions without full dense reconstruction.

This replaces decode-plus-dot-product by direct program-domain scoring.

## Why This Is Mathematically Plausible

### 1) JL/Binary projection channel already approximates inner products

For random projections with sufficient $m$, sign correlation tracks angular similarity. The $q_{ij}\psi_j(x)$ term is a compact angular estimator.

### 2) Seed programs encode structured residual geometry

The tree is not random metadata; it is a low-description map of directional corrections. Treating it as an operator family $G(S_i,\cdot)$ is mathematically cleaner than treating it as a temporary compression artifact.

### 3) Inference objective only needs ranking stability

For generation, exact Cartesian recovery is stronger than required. We need top-k logit ordering stability:

$$\operatorname*{arg\,topk}(Wx) \approx \operatorname*{arg\,topk}(\hat{y})$$

The mathematical target can be ranking-preserving bounds, not exact weight reconstruction.

## Theorem Target (What to Prove)

Define exact score $s_i = \langle w_i, x \rangle$ and FPQ-space score $\hat{s}_i$ as above.

Target bound:

$$|s_i - \hat{s}_i| \leq \epsilon_{\mathrm{proj}}(m) + \epsilon_{\mathrm{seed}}(d)$$

where:

- $m$ is projection width
- $d$ is seed depth/complexity budget

Then derive sufficient margin condition for ranking stability:

$$\Delta_{k} > 2(\epsilon_{\mathrm{proj}} + \epsilon_{\mathrm{seed}}) \Rightarrow \text{top-k preserved}$$

with $\Delta_k$ the k-th logit margin.

This is the core mathematical milestone.

## Minimal Mathematical Prototype (No Kernel Work Yet)

For one linear layer:

1. Load FPQ object per row: $(S_i,q_i,r_i)$
2. Compute $\psi(x)$ once for the token hidden state
3. Compute projection score term per row
4. Add lightweight seed functional $G(S_i,x)$ term
5. Compare:
   - Pearson/Spearman with exact logits
   - top-1/top-5 agreement
   - KL on softmax

If ranking holds at useful rates, only then move to runtime engineering.

## Novelty Compared to Standard Quantization

Standard quantization asks:

"How do we store $W$ with fewer bits but still reconstruct it fast?"

This direction asks:

"Can we infer from a non-Cartesian representation directly, where reconstruction is optional, not mandatory?"

That is a mathematical inversion of the inference problem, not a speed tweak.

## Immediate Next Experiment

Build a script-level proof on one projection-heavy layer:

- compute exact logits for sampled hidden states
- compute FPQ-space logits without dense decode
- report top-k preservation vs projection width $m$
- report effect of adding/removing seed functional term $G$

Pass condition:

- strong top-k preservation at practical $m$
- clear improvement from seed functional term over projection-only baseline

Only after this passes should we commit to fused runtime engineering.

## 30 Iteration Mathematical Refinement Program

Each iteration tightens one part of the theory, then defines a concrete deliverable.

### Iteration 1: Formal spaces

Define activation space $\mathcal{X}$, FPQ object space $\mathcal{F}$, and score map $T: \mathcal{F}\times\mathcal{X}\to\mathbb{R}$.

Deliverable: precise notation table and assumptions.

### Iteration 2: Projection estimator baseline

Use $\hat{s}^{(P)}_i=\frac{r_i}{m}\sum_j q_{ij}\psi_j(x)$ and bound its bias/variance.

Deliverable: closed-form $\epsilon_{\mathrm{proj}}(m)$ upper bound.

### Iteration 3: Seed functional class

Define $G(S_i,x)$ as a compositional operator family with Lipschitz constant $L_G$.

Deliverable: admissible seed-function class and complexity measure $d(S_i)$.

### Iteration 4: Combined estimator identifiability

Study when $\hat{s}_i=\alpha\hat{s}^{(P)}_i+\beta G(S_i,x)$ is identifiable.

Deliverable: constraints for unique $(\alpha,\beta)$ calibration.

### Iteration 5: Uniform score error bound

Derive $\sup_i |s_i-\hat{s}_i|\le \epsilon_{\mathrm{proj}}+\epsilon_{\mathrm{seed}}+\epsilon_{\mathrm{cal}}$.

Deliverable: theorem statement with explicit terms.

### Iteration 6: Top-k stability theorem

If margin $\Delta_k$ exceeds twice the score error, top-k ranking is preserved.

Deliverable: formal proof for deterministic top-k preservation.

### Iteration 7: Probabilistic ranking guarantee

Convert score concentration to probability bound:

$$\Pr[\mathrm{top}\text{-}k\ \mathrm{mismatch}]\le \delta(m,d).$$

Deliverable: tail bound in $m,d$.

### Iteration 8: Temperature-aware KL bound

Relate score error to softmax divergence at temperature $\tau$.

Deliverable: $\mathrm{KL}(p\|\hat{p})$ bound in terms of $\|s-\hat{s}\|_\infty$.

### Iteration 9: Sequence-level compounding bound

Propagate one-step distribution error over autoregressive horizon $T$.

Deliverable: cumulative drift bound and safe $T$ region.

### Iteration 10: Attention-logit sensitivity

Map query-key score error to attention matrix perturbation.

Deliverable: bound on attention output deviation per head.

### Iteration 11: Layerwise contraction analysis

Identify layers where residual and normalization reduce propagated error.

Deliverable: per-layer stability profile and critical-layer set.

### Iteration 12: Adaptive projection budget

Allocate $m_\ell$ per layer from margin statistics instead of fixed $m$.

Deliverable: optimization objective for projection budget allocation.

### Iteration 13: Adaptive seed depth budget

Allocate seed complexity $d_\ell$ per layer/head from sensitivity.

Deliverable: constrained minimization of expected ranking error.

### Iteration 14: Joint budget Pareto frontier

Solve tradeoff between compute and ranking preservation.

Deliverable: Pareto curve in $(m,d)$ space.

### Iteration 15: Information-theoretic lower bound

Estimate minimum bits/features required for stable top-k at target confidence.

Deliverable: lower bound baseline for feasibility checks.

### Iteration 16: Program-domain kernel analysis

Model score map as random-feature kernel approximation.

Deliverable: kernel equivalence and approximation error characterization.

### Iteration 17: Non-Cartesian manifold geometry

Treat seed-projection coordinates as a manifold chart and analyze curvature effects.

Deliverable: geometric interpretation of where approximation fails.

### Iteration 18: Margin-shaping calibration

Calibrate $(\alpha,\beta)$ to maximize rank margins, not MSE.

Deliverable: rank-aware calibration objective.

### Iteration 19: Distillation in FPQ space

Train calibrators to match teacher rank distributions directly in score space.

Deliverable: teacher-student loss for FPQ-domain inference.

### Iteration 20: Rare-token robustness theorem

Analyze tail-vocabulary margins and derive required confidence inflation.

Deliverable: separate bound for low-frequency token region.

### Iteration 21: Worst-case adversarial activations

Construct $x$ that maximizes ranking instability under fixed $(m,d)$.

Deliverable: adversarial stress test criterion and guardrail bound.

### Iteration 22: PAC-style generalization bound

Bound mismatch risk on unseen activations from sampled calibration set size $N$.

Deliverable: sample-complexity estimate for calibration data.

### Iteration 23: Multilingual/domain shift stability

Model feature drift across domains and derive recalibration trigger condition.

Deliverable: domain-shift detector threshold in FPQ score space.

### Iteration 24: Mixture-of-evaluators

Combine multiple cheap seed functionals $G_t$ with learned gates.

Deliverable: MoE score estimator and bound tightening criteria.

### Iteration 25: Certifiable fallback policy

Define when to invoke partial Cartesian decode fallback for uncertain logits.

Deliverable: confidence certificate and selective fallback theorem.

### Iteration 26: End-to-end objective unification

Unify ranking, KL, and compute cost in one loss:

$$\mathcal{L}=\lambda_1\mathcal{L}_{\mathrm{rank}}+\lambda_2\mathcal{L}_{\mathrm{KL}}+\lambda_3\mathcal{C}(m,d).$$

Deliverable: optimization blueprint and hyperparameter semantics.

### Iteration 27: Closed-form policy approximation

Fit analytical rules for choosing $(m,d)$ from simple layer statistics.

Deliverable: no-search policy approximation with regret bound.

### Iteration 28: Theorem-to-benchmark bridge

Map each error term to measurable benchmark metrics.

Deliverable: metric mapping table and acceptance thresholds.

### Iteration 29: Full theorem package

Assemble all lemmas into one coherent statement of FPQ-domain inference validity.

Deliverable: draft proof appendix structure.

### Iteration 30: Mathematical readiness criterion

Define final pass condition for moving into runtime engineering.

Required:

1. proven top-k stability region
2. bounded sequence drift under target horizon
3. measurable compute-quality advantage over decode-first baseline

Deliverable: go/no-go theorem checklist.

## Execution Rule

Do not claim native inference solved until Iteration 30 criteria pass on real layers and sequence traces.

This preserves the inversion: math validity first, engineering implementation second.

## Unified Master Formulation

Let exact score vector be $s(x)=Wx$ and FPQ-domain score be $\hat{s}(x)=T(\mathcal{W},x;\theta)$.

Define total error decomposition:

$$e(x)=s(x)-\hat{s}(x)=e_P(x)+e_G(x)+e_C(x),$$

where $e_P$ is projection error, $e_G$ is seed-functional approximation error, and $e_C$ is calibration error.

Assume high-probability bounds:

$$\|e_P(x)\|_\infty \le a\sqrt{\frac{\log V}{m}},\quad
\|e_G(x)\|_\infty \le b\,d^{-\gamma},\quad
\|e_C(x)\|_\infty \le cN^{-1/2}.$$

Then:

$$\|e(x)\|_\infty \le a\sqrt{\frac{\log V}{m}} + b\,d^{-\gamma} + cN^{-1/2} =: \varepsilon(m,d,N).$$

For vocabulary size $V$ and top-k margin $\Delta_k(x)$, ranking preservation holds whenever:

$$\Delta_k(x) > 2\varepsilon(m,d,N).$$

Hence the optimization target is:

$$\min_{m,d,\theta} \ \mathbb{E}_{x}\big[\ell_{\mathrm{rank}}(s,\hat{s})\big] + \lambda\,\mathcal{C}(m,d)
\ \text{s.t.}\ \Pr_x[\Delta_k(x)>2\varepsilon(m,d,N)]\ge 1-\delta.$$

This turns the method into a certifiable constrained optimization problem.

## Full-Potential Criteria

The method is mathematically mature only when all are true:

1. Error decomposition terms are measurable and bounded in practice.
2. The $(m,d,N)$ feasibility region is estimated per critical layer.
3. Top-k preservation probability is validated on sequence traces, not only single-step logits.
4. The constrained optimum beats decode-first on at least one quality-compute frontier.
5. A selective fallback certificate is defined for out-of-region activations.

If any one fails, the method remains a promising conjecture rather than a solved mathematical inference framework.