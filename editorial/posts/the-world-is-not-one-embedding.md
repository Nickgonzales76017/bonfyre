---
slug: the-world-is-not-one-embedding
title: One embedding space is not a model of the whole world
description: Why local geography, images, text, time and operational actions need different measures of similarity.
category: Mathematics
icon: layers
published: 2026-11-08
status: edited-public
topics: embeddings, manifolds, multimodal, retrieval
origin: Mathematical research notes from archived experiments and external literature
source_url: https://arxiv.org/abs/2004.12832
---

Imagine a regional research system combining a weather observation, an aerial image, an insurance report, a video and a public building permit. Each observation carries useful information. None has exactly the same idea of what it means for two things to be similar.

We explored this when combining text, spatial and media sources in older Akai experiments. A universal embedding sounds convenient, but it may compress away distinctions that one of the applications urgently needs.

## Similarity depends on the question

Two damaged roofs may look nearly identical in an image model while being in different counties and subject to different building regulations. Two paragraphs may have similar text embeddings but refer to events years apart. Nearby locations may have unrelated supply chains.

Instead of forcing every signal into one geometry, use a collection of domain-specific measures. A candidate score might be a weighted sum of text relevance, geographic proximity, event-time agreement and verified source relationships. Those weights should depend on the task.

This is related to the broader idea of late interaction in retrieval. [ColBERT](https://arxiv.org/abs/2004.12832) demonstrates an approach that retains token-level interactions rather than reducing a passage to one coarse similarity score.

## Fusion is an experiment, not a magic word

A useful benchmark needs held-out tasks: find the correct property record, identify a video segment, retrieve a relevant contract or explain a regional change. Compare text-only, image-only, location-only and combined retrieval. Measure false positives as well as recall.

If an added modality does not improve the task, its computational cost may not be justified. If modalities share an upstream source, they may also add less independent information than the score suggests.

The purpose of several embeddings is not mathematical decoration. It is to preserve the kinds of differences that matter to the action someone is trying to take.
