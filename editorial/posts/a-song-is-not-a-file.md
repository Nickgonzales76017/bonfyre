---
slug: a-song-is-not-a-file
title: The song is not the file that plays it
description: Compositions, recordings, tracks, releases, performers and rights need a relational model, not one media row.
category: Publishing
icon: radio
published: 2026-11-09
status: edited-public
topics: music, recordings, compositions, rights, releases, metadata, credits
origin: Original Aurekai editorial analysis using primary institutional documentation
source_url: https://ddex.net/standards/electronic-release-notification-message-suite/
---

A creator uploads an audio file and types the title of a song. A publishing tool announces that the work is ready for distribution. The interface looks complete, but it has not answered which composition was performed, which recording was made, who contributed, who may distribute it, or where that right applies.

The problem is visible in the [MusicBrainz description of a recording](https://musicbrainz.org/doc/Recording). A recording is related to tracks on releases, but it is not identical to a particular track on an album. The same recording can appear in several releases; a live rendition and a studio rendition can be separate recordings of one underlying work.

## The useful distinctions

The work is an intellectual creation: composition or song. A recording is a particular realized performance or production. A track is a position on a release; a release can be a single, album or another commercial package. A digital file is a representation of recorded audio, often one among several encodings.

Collapsing these into one row makes it difficult to credit a songwriter who did not perform, connect multiple masterings, identify a remix correctly or trace which recording a publisher has licensed.

## Metadata is part of distribution authority

The [DDEX Electronic Release Notification standard](https://ddex.net/standards/electronic-release-notification-message-suite/) transmits metadata for releases and contained resources, along with the conditions under which releases can be made available. The existence of a high-quality file alone does not convey those distribution terms.

Consider a label with rights to distribute one recording in selected countries during a defined period. The audio may be identical when a region changes, yet the authorization to make it available is not. An app that copies the file's metadata into a global 'public' flag erases the restrictions that govern the transaction.

## The archival perspective

An audio archive faces a different challenge. It may preserve a field recording, a radio broadcast or an interview without owning commercial distribution rights. Preserving the source matters, as do artist credits, dates, context, capture format, technical quality and any restrictions on access.

A clip used in a commentary article should retain its relationship to the full recording and to the permissions under which it is used. A waveform, transcript and chapter markers can improve discovery, but cannot replace the recording or make an unlawful copy legitimate.

## What a creator platform should actually connect

Aurekai's creator infrastructure needs separate records for works, recordings, releases, contributors, files, distribution permissions and uses. A listener's playback action can then resolve a lawful, available representation without asking the catalog to pretend a file is an owned work.

This is not bureaucracy for its own sake. It is what lets an independent creator preserve credit while their music appears in many contexts, and what lets a future editor explain which recording was heard rather than offering another anonymous MP3.
