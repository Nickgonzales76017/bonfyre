---
slug: the-memory-wall-in-local-models
title: We found the memory wall before we found the speed
description: A note from the early FPQ and Qwen experiments about model loading, prepared state and actual generation speed.
category: Engineering
icon: cpu
published: 2026-11-07
status: edited-public
topics: models, FPQ, Qwen, performance, local compute
origin: Qwen FPQ runtime and model-performance conversations, April–July 2026
---

Our local-model work taught us an uncomfortable lesson: a program can look busy, keep a model resident, and still be too slow to use. In the early Qwen/FPQ experiments we were seeing long first-token paths and trying to understand how much preparation the runtime really needed to repeat.

It was tempting to treat the slow path as a single optimization problem. In practice, memory layout, model format, decoding kernels, tokenization, cache behavior and machine capacity were interacting.

## Resident doesn't automatically mean ready

A resident model avoids repeated loading, but it also keeps prepared weights, metadata and context in memory. On a smaller machine that may leave little room for the actual computation. Rebuilding too much state costs time; retaining too much state risks memory pressure or termination.

We studied how quantization, tensor representation and precomputed structures could change that balance. Some experiments drew inspiration from cookbook-style quantization and lattice structures. Others compared native kernels against established inference libraries.

## The number that matters is a complete answer

First-token time, sustained tokens per second, memory footprint and fidelity are all useful measurements, but none tells the whole story on its own. A fast decoder with poor output quality isn't a better assistant. A compact model that takes forever to prepare doesn't improve a real workflow.

For that reason, the [Bonfÿre model systems](https://nickgonzales76017.github.io/bonfyre/docs.html#fpq) are treated as part of Model Commons, with execution routes and resource placement alongside representation work.

The broader lesson applies to our agent architecture. If a tool needs a local model, the system should know which model is installed, which runtime can serve it, what resources are available and what the task requires. A file in a models directory is only the starting point.

We're still interested in making local inference inexpensive enough to be ordinary infrastructure for small businesses. The hard part has been making every layer of the path work together.
