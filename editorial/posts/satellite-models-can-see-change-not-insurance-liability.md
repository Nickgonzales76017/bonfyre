---
slug: satellite-models-can-see-change-not-insurance-liability
title: Satellite models can see landscape change. They cannot settle an insurance claim.
description: The promise and limits of Prithvi-EO and remote sensing for regional operational intelligence.
category: Science
icon: globe
published: 2026-11-08
status: edited-public
topics: Prithvi, satellite, disaster, remote sensing
origin: External market and scientific sources, with Aurekai opportunity analysis
source_url: https://huggingface.co/ibm-nasa-geospatial/Prithvi-EO-2.0-300M
---

A satellite image can show flooding, a wildfire scar or changes in land cover. That makes Earth-observation models powerful tools for understanding regional events. It does not mean the model knows which building was insured, how the damage occurred or what a policy covers.

Our earlier research into regional-state reconstruction tried to connect multiple types of evidence: site records, weather, satellite observations, infrastructure data, video and fresh publication feeds. The attraction was a more complete picture of what changed in a place.

## What Prithvi actually provides

[Prithvi-EO-2.0](https://huggingface.co/ibm-nasa-geospatial/Prithvi-EO-2.0-300M), developed by IBM, NASA and collaborators, is an Earth-observation foundation-model family. Its published model card describes pretraining using multispectral satellite data and variants with temporal and location information. It offers a way to develop downstream geospatial applications, not a universal interpretation of all scenes.

A 30-meter-scale satellite observation can capture regional patterns that a person on the ground might miss. It can also blur small structures, encounter clouds or represent conditions from a different day than a property visit.

## Evidence types need to remain distinct

A useful investigation might combine satellite imagery showing broad flood extent, a weather record, a building permit and a dated photograph. The system should preserve the source and uncertainty of each observation.

It should not infer a precise repair estimate, a building's safety or an insurer's responsibility from an embedding alone. Those judgments need property-level context, qualified inspection and applicable records.

For Aurekai, the opportunity is in research and operations: locating relevant public evidence, comparing changes over time and making it easier for specialists to investigate a region. The model can help ask where to look next. It should not quietly become the final adjudicator.
