---
slug: a-release-number-is-not-the-binary-you-ran
title: Version 2.4.1 is not evidence of what the server ran
description: Sources, dependencies, build environments, artifacts, deployment and readback are separate identity and verification problems.
category: Engineering
icon: cpu
published: 2026-11-09
status: edited-public
topics: versions, build systems, release identity, reproducible builds, provenance, CI
origin: Original Aurekai editorial investigation informed by primary standards and technical documentation
source_url: https://reproducible-builds.org/docs/definition/
---

A team tests a release candidate, tags it version 2.4.1, and deploys another build carrying the same label. A bug appears only in production. The release notes match and the commit identifier looks familiar. But a dependency or build environment changed between the two artifacts. The version string was a useful name, not proof of byte identity.

The [Reproducible Builds definition](https://reproducible-builds.org/docs/definition/) asks whether the same declared sources, build instructions and environment can produce bit-for-bit identical artifacts. That property is more precise than a release number and more demanding than a successful compilation.

## What a build actually consumes

The compiler version, flags, linked libraries, generated inputs, timestamps, environment settings and packaging tools can influence the output. Some inputs are obvious in a manifest; others arrive from a dependency resolver or the build machine's state.

A reproducible process identifies the complete relevant environment and allows independent parties to compare resulting artifacts. A file hash proves only byte identity with a known artifact. It does not prove the build was honest, authorized or safe to run.

## A four-identity chain

There is the authored source revision; the built artifact digest; the deployed artifact or image identity; and the live response actually served to a user. Those four can diverge independently. A release pipeline might verify the first two while an edge cache continues serving older public HTML.

That last problem is examined in [the Journal's deployment analysis](https://nickgonzales76017.github.io/bonfyre/blog/the-deployment-was-green-but-the-site-was-old/). A green CI badge is a statement about the tests that ran, not automatic evidence that every browser now sees those bytes.

## Why release labels still matter

Human-friendly versions communicate intent, compatibility and history. A semantic versioning convention can help consumers plan upgrades, but a label cannot reveal every security patch, feature flag, database schema or runtime dependency in an installation.

There are also legitimate reasons two deployments of the same source differ in configuration. That makes clear runtime declarations and environment-scoped verification even more valuable.

## An operationally mature release receipt

Retain source revision, build inputs, artifact hashes, provenance, migration identity, target environment, activation record and exact live readback. Test failure paths as well as happy paths. If the live site does not match the approved artifact, label the release incomplete rather than treating deployment initiation as completion.

The purpose of this rigor is not to make shipping harder. It is to make a promise about running software verifiable by somebody other than the person who clicked Deploy.
