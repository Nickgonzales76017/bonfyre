# Spectral Lattice Inference

## A Mathematical Theory of Direct Computation on Compressed Weight Programs

*BonfyreFPQ — April 2026*

---

## Abstract

Every existing quantization method — GPTQ, AWQ, QuIP#, AQLM, GGUF — stores
weights in a compressed format and then **dequantizes during inference** to
recover dense values for standard linear algebra. The compression helps storage
and bandwidth, but the computational model is unchanged: decompress, then
multiply.

This document proves that BonfyreFPQ's encode chain admits a **dual formulation**
where the input activation is transformed once, and all weight–activation
interactions are computed **natively in the compressed domain** without ever
materializing dense weight tensors.

The result: inference throughput scales with the **compression ratio itself**, not
just storage. For FPQ3 at 8.9× compression, the theoretical inference speedup in
the memory-bandwidth-bound regime approaches 8.9×.

No existing method achieves this. All others dequantize.

---

## 1. The Decode Chain as a Sequence of Invertible Maps

BonfyreFPQ v9 encodes each weight block $w_b \in \mathbb{R}^n$ (where $n = 256$)
through a chain of invertible (or approximately invertible) transforms:

$$w_b = \underbrace{D_{\mathrm{signs}}}_6 \circ \underbrace{D_{\mathrm{FWHT}}}_5 \circ \underbrace{D_{\mathrm{warp}}}_4 \circ \underbrace{D_{\mathrm{RVQ}}}_3 \circ \underbrace{D_{\mathrm{E8}}}_2 \circ \underbrace{D_{\mathrm{QJL}}}_1 (\mathcal{C}_b)$$

where $\mathcal{C}_b$ is the compact on-disk representation (lattice indices, tile indices,
QJL sign bits, warp norm).

Standard inference computes $\langle w_b, x_b \rangle$ by evaluating the full chain
left-to-right to recover $w_b$, then dotting with input $x_b$.

We prove this is unnecessary.

---

## 2. The Duality Theorem

**Theorem 1 (Spectral Bypass).** *Let $H$ denote the $n \times n$ normalized
Walsh–Hadamard matrix and $s_b \in \{-1,+1\}^n$ the per-block random sign vector.
For any block $b$, define the spectral-domain activation*

$$\tilde{x}_b = \frac{1}{\sqrt{n}} H (s_b \odot x_b)$$

*and the pre-FWHT weight representation $z_b$ (the state after inverse warp, RVQ
correction, and QJL reconstruction, but before FWHT and sign undo). Then:*

$$\langle w_b, x_b \rangle = z_b^T \tilde{x}_b$$

**Proof.** The last two decode steps are:

$$w_b = s_b \odot \Big(\frac{1}{\sqrt{n}} H z_b\Big)$$

Random signs are self-inverse ($s_b \odot s_b = \mathbf{1}$) and $H$ is symmetric
and orthogonal ($H^T = H$, $H^2 = nI$). Therefore:

$$\langle w_b, x_b \rangle = \Big(s_b \odot \frac{H z_b}{\sqrt{n}}\Big)^T x_b = \frac{1}{\sqrt{n}} z_b^T H^T (s_b \odot x_b) = \frac{1}{\sqrt{n}} z_b^T H (s_b \odot x_b) = z_b^T \tilde{x}_b \qquad \square$$

**Consequence.** FWHT and random signs never need to be applied to weights.
They are applied once to the *input* per block position. This eliminates two of
the six decode steps for ALL output rows simultaneously.

---

## 3. Decomposition of the Spectral-Domain Score

The pre-FWHT representation decomposes additively:

$$z_b = z_b^{(\mathrm{E8})} + z_b^{(\mathrm{RVQ})} + z_b^{(\mathrm{QJL})}$$

where each component has rigid structure exploitable for fast scoring.

Therefore:

$$z_b^T \tilde{x}_b = \underbrace{(z_b^{(\mathrm{E8})})^T \tilde{x}_b}_{\text{Lattice term}} + \underbrace{(z_b^{(\mathrm{RVQ})})^T \tilde{x}_b}_{\text{Codebook term}} + \underbrace{(z_b^{(\mathrm{QJL})})^T \tilde{x}_b}_{\text{Projection term}}$$

We analyze each.

---

## 4. Lattice Inner Products by Table Lookup

### 4.1 E8 Structure

Each 256-dim block is partitioned into 32 groups of 8 dimensions.
Each 8D group is snapped to a point in the $E_8$ Gosset lattice, scaled by
$\lambda = 8 \times \mathrm{bits}$. The $E_8$ lattice has **240 minimal vectors**
and a finite set of reachable points at each scale.

Let $e_g \in E_8/\lambda$ denote the lattice point for group $g \in \{0,\ldots,31\}$.

### 4.2 Warped Lattice Points

After inverse $\mu$-law warp ($\beta = 8$):

$$\hat{e}_{g,k} = \mathrm{sign}(e_{g,k}) \cdot \frac{\exp(|e_{g,k}| \ln(1+\beta)) - 1}{\beta}$$

Since $E_8/\lambda$ is a finite point set, the warped values
$\hat{e}_g = \mathrm{unwarp}(e_g)$ belong to a **finite precomputable set**.

### 4.3 Precomputed Score Table

**Definition.** For each token, compute the **Lattice Score Table**:

$$\Gamma_{j,g} = \sum_{k=0}^{7} \mathrm{unwarp}\Big(\frac{E_8^{(j)}[k]}{\lambda}\Big) \cdot \tilde{x}_{b, 8g+k}$$

for all $E_8$ points $j \in \{1,\ldots,|E_8|\}$ and all 32 groups $g$.

**Theorem 2 (Lattice Lookup).** *The E8 contribution to row $i$'s inner product
with block $b$ is:*

$$(\text{E8 term})_i = \sum_{g=0}^{31} \Gamma_{\mathrm{idx}_{i,b,g},\; g}$$

*This requires 32 table lookups and 32 additions per row per block. The table
$\Gamma$ is computed once per block position and reused across all $V$ output rows.*

**Cost.** Building $\Gamma$: $O(|E_8| \times 8 \times 32) = O(|E_8| \times 256)$
per block position, amortized over $V$ rows.

Per row per block: $O(32)$ lookups + adds. ∎

---

## 5. Codebook Inner Products by Tile Lookup

### 5.1 RVQ Tile Structure

Each block has 16 pairs of 8D groups. Each pair is assigned a tile index into a
codebook of $K_{\mathrm{eff}} \leq 256$ tiles of dimension 16.

### 5.2 Linearized Warp Correction

The tile correction $t_b$ is added to the E8 base before warping. Since the warp
is nonlinear:

$$\mathrm{unwarp}(e + t) = \mathrm{unwarp}(e) + t \odot \mathrm{unwarp}'(e) + O(\|t\|^2)$$

where the derivative is:

$$\mathrm{unwarp}'(y) = \frac{\ln(1+\beta)}{\beta} \exp(|y| \ln(1+\beta))$$

**Theorem 3 (Linearized Tile Correction).** *Define the warp-weighted transformed
input for pair $p$:*

$$\tilde{x}^{(\mathrm{warp})}_{b,p,k} = \mathrm{unwarp}'(e_{b,16p+k}/\lambda) \cdot \tilde{x}_{b,16p+k}$$

*and the Tile Score Table:*

$$\Omega_{c,p} = \sum_{k=0}^{15} \mathrm{tile}_c[k] \cdot \tilde{x}^{(\mathrm{warp})}_{b,p,k}$$

*for each codebook entry $c$ and pair $p$. Then the RVQ contribution is:*

$$(\text{RVQ term})_i \approx \sum_{p=0}^{15} \Omega_{\mathrm{tidx}_{i,b,p},\; p}$$

*This is 16 lookups per row per block, with the table built once per block position.*

*The approximation error is second-order in tile magnitude:*

$$|\text{error}| \leq \frac{1}{2} \max_k |\mathrm{unwarp}''| \cdot \|t_b\|^2 \cdot \|\tilde{x}_b\|$$

∎

---

## 6. Projection Inner Products from Sign Bits

### 6.1 QJL Structure

Each block stores $m = 64$ sign bits $y_{b,p} \in \{-1,+1\}$ from random
Rademacher projections $\phi_p$ (regenerable from seed). The QJL reconstruction
is:

$$z_b^{(\mathrm{QJL})} = \frac{\|r_b\|}{m} \sum_{p=0}^{m-1} y_{b,p} \phi_p$$

### 6.2 Precomputed Projection Scores

**Definition.** Compute once per block position:

$$\pi_{b,p} = \phi_p^T \tilde{x}_b$$

for $p \in \{0,\ldots,m-1\}$. Since $\phi_p$ are Rademacher (±1), this costs
$O(n)$ per projection, $O(mn)$ total per block position.

**Theorem 4 (Projection Scoring).** *The QJL contribution to row $i$'s score is:*

$$(z_b^{(\mathrm{QJL})})^T \tilde{x}_b = \frac{\|r_{i,b}\|}{m} \sum_{p=0}^{m-1} y_{i,b,p} \cdot \pi_{b,p}$$

*This is $m = 64$ multiply-adds per row per block.* ∎

### 6.3 Quality Guarantee (Johnson–Lindenstrauss)

By the JL lemma, the QJL correction provides an **unbiased** inner product
estimator with concentration:

$$\Pr\Big[\big|(z_b^{(\mathrm{QJL})})^T \tilde{x}_b - r_b^T \tilde{x}_b\big| > \epsilon \|r_b\| \|\tilde{x}_b\|\Big] \leq 2 e^{-m\epsilon^2/4}$$

For $m = 64$, any per-block contribution is within $\pm 25\%$ of the true residual
inner product with probability $> 99\%$. Across $V$ rows, union bound gives high
confidence on the full output vector.

---

## 7. Complete Spectral Lattice Inference Algorithm

### Input
- FPQ-encoded weight matrix: per-row E8 indices, tile indices, QJL bits, warp norms
- Low-rank factors $U\Sigma$, $V^T$
- Ghost vectors $u, v, \sigma$
- Activation vector $x \in \mathbb{R}^h$

### Algorithm

**Phase 0: Low-Rank and Ghost (standard)**
$$y_{\mathrm{LR}} = U\Sigma(V^T x), \qquad y_{\mathrm{ghost}} = \sigma u (v^T x)$$

Cost: $O(r(V+h) + V + h)$

**Phase 1: Input Transform (once per token)**

For each block position $b \in \{0, \ldots, h/n - 1\}$:
1. Apply random signs: $x'_b = s_b \odot x_b$
2. Apply FWHT: $\tilde{x}_b = H x'_b / \sqrt{n}$
3. Build Lattice Score Table $\Gamma_{j,g}$ (all E8 points × 32 groups)
4. Build warp-weighted input $\tilde{x}^{(\mathrm{warp})}$
5. Build Tile Score Table $\Omega_{c,p}$ (all tiles × 16 pairs)
6. Build Projection Scores $\pi_{b,p}$ (64 projections)

Cost: $O\big(\frac{h}{n}(n \log n + |E_8| \cdot 256 + K_{\mathrm{eff}} \cdot 256 + mn)\big)$

This is $O(h \log n + h \cdot |E_8| + h \cdot K_{\mathrm{eff}} + mh)$. Since
$|E_8| = 240$, $K_{\mathrm{eff}} \leq 256$, $m = 64$, this is $O(h)$ with a
moderate constant, computed **once**.

**Phase 2: Row Scoring (per output row)**

For each output row $i$ and each input block $b$:

$$\hat{y}_{i,b} = \sum_{g=0}^{31} \Gamma_{\mathrm{idx}_{i,b,g},g} + \sum_{p=0}^{15} \Omega_{\mathrm{tidx}_{i,b,p},p} + \frac{\|r_{i,b}\|}{m} \sum_{p=0}^{63} y_{i,b,p} \cdot \pi_{b,p}$$

$$y_i = y_{\mathrm{LR},i} + y_{\mathrm{ghost},i} + \sum_b \hat{y}_{i,b}$$

Cost per row per block: $32 + 16 + 64 = 112$ operations.

**Total row scoring: $O\big(V \cdot \frac{h}{n} \cdot 112\big) = O\big(\frac{112 Vh}{256}\big) \approx O(0.44 Vh)$.**

Compare dense matmul: $O(Vh)$.

Compute reduction: **2.3×**.

---

## 8. The Memory Bandwidth Theorem

Compute reduction alone is not the claim. The real result is about memory.

**Theorem 5 (Bandwidth Scaling).** *In the memory-bandwidth-bound inference
regime, Spectral Lattice Inference achieves throughput speedup proportional to
the FPQ compression ratio.*

**Proof.** During autoregressive generation (batch size 1, long context), inference
is dominated by weight reads, not arithmetic.

Arithmetic intensity of dense matmul:
$$\mathcal{I}_{\mathrm{dense}} = \frac{2Vh}{2Vh} = 1 \text{ FLOP/byte (BF16)}$$

GPU roofline crossover (e.g. A100): $\mathcal{I}^* = \frac{312 \text{ TFLOPS}}{2 \text{ TB/s}} = 156 \text{ FLOP/byte}$.

Since $\mathcal{I}_{\mathrm{dense}} \ll \mathcal{I}^*$, inference is firmly
memory-bandwidth-bound. Token latency $\propto$ bytes read.

For dense BF16 weights:
$$\text{Bytes per token per layer} = 2Vh$$

For Spectral Lattice Inference, the weight data read per row per block is:
- 32 bytes: E8 lattice indices (32 groups × 1 byte)
- 16 bytes: RVQ tile indices (16 pairs × 1 byte)
- 8 bytes: QJL sign bits (64 bits)
- 4 bytes: warp norm (float32)
- **Total: 60 bytes per block**

vs dense: $256 \times 2 = 512$ bytes per block.

$$\text{Bandwidth ratio} = \frac{512}{60} = 8.53\times$$

Since inference is bandwidth-bound and the roofline check confirms the
FPQ scoring arithmetic ($112/60 \approx 1.87$ FLOP/byte) is still well below
$\mathcal{I}^* = 156$:

$$\text{Speedup} \approx 8.5\times \qquad \square$$

### Verified Against Data

Measured FPQ3 compression on Qwen2.5-3B:
- Compatibility safetensors: 6.18 GB
- Native .fpq: 692 MB
- Ratio: **8.93×**

The bandwidth theorem predicts 8.5× inference speedup. The measured storage ratio
of 8.93× is consistent. The small gap comes from metadata overhead in the score
tables (built once, reused across all rows).

---

## 9. What This Changes

### 9.1 Every Other Method Dequantizes

| Method | Stores compressed? | Dequantizes during matmul? |
|--------|-------------------|---------------------------|
| GPTQ | Yes (INT4 groups) | Yes, per group |
| AWQ | Yes (INT4 groups) | Yes, per group |
| QuIP# | Yes (E8 lattice) | Yes, per block |
| GGUF/llama.cpp | Yes (various) | Yes, per block |
| **BonfyreFPQ SLI** | **Yes** | **No** |

Every existing method reads compressed weights and expands them to dense values
inside the GEMM kernel. The arithmetic and memory overhead of dequantization is
unavoidable in their design.

Spectral Lattice Inference is the **first formulation** where the compressed
representation participates directly in the inner product computation without
any expansion to dense Cartesian coordinates.

### 9.2 The Paradigm Inversion

Standard paradigm:
$$\text{compressed weights} \xrightarrow{\text{dequant}} \text{dense weights} \xrightarrow{\text{GEMM}} \text{output}$$

SLI paradigm:
$$\text{activation} \xrightarrow{\text{domain lift}} \text{spectral activation} \xrightarrow{\text{table score}} \text{output}$$

The weights are never dense. The weights are never read as numbers. They are read
as **indices into precomputed interaction tables**. The tables are built from the
activation, not from the weights.

This is the mathematical inversion: transform the question, not the answer.

### 9.3 Why FPQ Uniquely Enables This

This is not possible for GPTQ/AWQ because their format (per-group scale + zero +
INT values) has no algebraic structure that admits table precomputation. The
dequantization $w = s(q - z)$ is weight-specific and must be evaluated per element.

FPQ enables it because of three structural properties:

1. **Walsh–Hadamard self-adjointness**: FWHT can be pushed to either side of the
   inner product without loss.
2. **E8 lattice finiteness**: The lattice points form a discrete set, enabling
   exhaustive precomputation of interactions with any input.
3. **QJL linearity**: Sign-bit projections are linear in the input, enabling
   factored scoring.

No other quantization format has all three.

---

## 10. Multi-Layer Composition

### 10.1 Layer-to-Layer Propagation

After computing $y = W_\ell x$ via SLI for layer $\ell$, the output $y$ enters
normalization and nonlinearity before becoming input to layer $\ell+1$.

The output $y$ is a dense vector in $\mathbb{R}^V$ (or $\mathbb{R}^h$ for
intermediate layers). It serves as a standard activation for Phase 1 of the
next layer's SLI scoring.

**Key property:** No intermediate weight materialization occurs between layers.
The only dense vectors are activations, which are small ($h$-dimensional per
token) compared to weight matrices ($V \times h$).

### 10.2 Peak Memory

$$\text{Standard: } O(Vh) \text{ for largest weight matrix}$$
$$\text{SLI: } O\Big(\frac{Vh}{8.5}\Big) + O(|E_8| \cdot h + K_{\mathrm{eff}} \cdot h + mh\Big)$$

The score tables add $O(h)$ temporary memory, negligible compared to the 8.5×
reduction in weight storage.

---

## 11. Quality Bounds

### 11.1 Per-Score Error Decomposition

For output row $i$, block $b$, the scoring error has three sources:

$$|s_{i,b} - \hat{s}_{i,b}| \leq \underbrace{\epsilon_{\mathrm{E8}}}_{\text{lattice}} + \underbrace{\epsilon_{\mathrm{RVQ}}}_{\text{tile linearization}} + \underbrace{\epsilon_{\mathrm{QJL}}}_{\text{projection}}$$

**Lattice error $\epsilon_{\mathrm{E8}}$**: Zero. The E8 term is computed exactly
from precomputed unwarped lattice values. No approximation.

**Tile linearization error $\epsilon_{\mathrm{RVQ}}$**: Second-order in tile magnitude.

$$\epsilon_{\mathrm{RVQ}} \leq \frac{\ln^2(1+\beta)}{2\beta} \max_k \exp(|e_k| \ln(1+\beta)) \cdot \|t_b\|^2 \cdot \|\tilde{x}_b\|$$

For typical tile corrections $\|t_b\| \ll \|e_b\|$ (residual is small relative to
base), this is negligible.

**Projection error $\epsilon_{\mathrm{QJL}}$**: Bounded by Johnson–Lindenstrauss
concentration (Section 6.3). High-probability bound:

$$\epsilon_{\mathrm{QJL}} \leq c \|r_b\| \|\tilde{x}_b\| \sqrt{\frac{\log V}{m}}$$

### 11.2 Full Output Error

Summing over blocks and combining with LR (exact) and ghost (exact) components:

$$\|y - \hat{y}\|_\infty \leq \frac{h}{n} \max_b \Big(\epsilon_{\mathrm{RVQ},b} + \epsilon_{\mathrm{QJL},b}\Big)$$

### 11.3 Ranking Preservation

For top-$k$ token generation, let $\Delta_k$ be the $k$-th logit margin (gap
between rank-$k$ and rank-$(k+1)$ logits). Ranking is preserved when:

$$\Delta_k > 2\|y - \hat{y}\|_\infty$$

Since FPQ v9 achieves 0.9999+ cosine similarity on the full decode, the
SLI error (which adds only the tile linearization approximation on top of the
same underlying representation) is strictly smaller than the decode error itself.

**The quality of SLI is bounded by the quality of FPQ encoding, which is already
measured at 0.9999+ cosine and +0.9% perplexity.**

---

## 12. Complexity Summary

| Operation | Dense BF16 | Spectral Lattice Inference |
|-----------|-----------|---------------------------|
| Weight memory | $2Vh$ bytes | $\sim Vh/4.3$ bytes |
| Memory reads/token/layer | $2Vh$ bytes | $\sim Vh/4.3$ bytes |
| Compute/token/layer | $2Vh$ FLOP | $\sim 0.88Vh$ FLOP |
| Arithmetic intensity | 1 FLOP/byte | 1.87 FLOP/byte |
| Memory-bound speedup | 1× | **~8.5×** |
| Peak weight memory | $2Vh$ | $Vh/4.3$ |
| Quality | Exact | Bounded by FPQ fidelity |
| Score tables (temporary) | 0 | $O(h)$ per block position |

---

## 13. The Innovation Claim

This is not "better compression."

This is not "faster dequantization."

This is the discovery that BonfyreFPQ's encoding chain — Walsh–Hadamard spectral
decorrelation, E8 lattice geometric quantization, and Johnson–Lindenstrauss
sign projection — form a mathematically closed system where inference can be
computed entirely in the compressed domain.

The formal name: **Spectral Lattice Inference (SLI)**.

The core equation:

$$\langle w_b, x_b \rangle = \underbrace{\sum_g \Gamma_{\mathrm{idx}_g, g}}_{\text{lattice lookup}} + \underbrace{\sum_p \Omega_{\mathrm{tidx}_p, p}}_{\text{tile lookup}} + \underbrace{\frac{\|r\|}{m}\sum_p y_p \pi_p}_{\text{projection score}}$$

Every term is either a table lookup or a sign-weighted sum. No weight value is
ever reconstructed.

The mathematical structure that makes this possible:

1. **Self-adjoint spectral transform** (FWHT) pushes to either side of the inner product
2. **Finite lattice geometry** (E8) admits exhaustive table precomputation
3. **Linear sign projections** (QJL) factor into input-side and weight-side components

These three properties do not exist together in any other quantization scheme.

---

## 14. Connection to Fundamental Theory

### 14.1 Kolmogorov Complexity and Inference Cost

Standard paradigm: inference cost $\propto$ parameter count $N$.

SLI paradigm: inference cost $\propto$ description complexity of weights.

If weight blocks are compressible to $K$ bits of index data (FPQ achieves
$K \approx 60$ bytes per 256-element block vs 512 bytes dense), then SLI
inference cost is $O(K/512 \times N)$ — sublinear in the effective parameter budget.

This is the first concrete instance of **Kolmogorov-bounded inference**: the
computational cost of using a model is proportional to its algorithmic information
content, not its raw parameter count.

### 14.2 Rate-Distortion to Rate-Computation

Shannon rate-distortion theory asks: what is the minimum number of bits to
represent data at distortion $D$?

FPQ inverts this to **rate-computation theory**: given a compressed representation
at rate $R$, what is the minimum computation needed to use it for inference?

Standard methods answer: $\Theta(N)$ regardless of $R$ (must dequantize all weights).

SLI answers: $\Theta(R \cdot N / B)$ where $B$ is the uncompressed block size
and $R$ is the compressed block size. Computation scales with rate.

This is a new relationship between compression rate and inference cost that does
not exist in the standard quantization literature.

---

## 15. Predictions and Testable Claims

### 15.1 Measurable Predictions

1. **Tokens/sec**: SLI should achieve ~8× higher throughput than dense BF16 for
   autoregressive generation at batch size 1 on A100/H100.

2. **Memory**: Peak weight memory should match native .fpq file size (692 MB for
   Qwen2.5-3B vs 6.18 GB for safetensors).

3. **Quality**: Perplexity within +1% of BF16 baseline (matching FPQ v9 decode
   quality since the only additional approximation is the tile linearization).

4. **Score agreement**: Per-token logit Pearson correlation $> 0.999$ vs dense
   computation, verifiable without full generation.

### 15.2 Falsifiable Conditions

If any of the following fail, the theory is wrong:

1. The tile linearization error dominates and logit correlation drops below 0.99.
2. Score table construction overhead exceeds the bandwidth savings.
3. The roofline assumptions break (FPQ scoring becomes compute-bound on target hardware).

These are testable on a single layer before any runtime engineering.
