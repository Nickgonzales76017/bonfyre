# Source Of Truth Map

This map answers one question for every `/10-Code/*` runtime-adjacent directory and a few `bonfyre-oss/cmd/*` modules that do not yet have a vault peer: where should the canonical source of truth live after cleanup?

Important correction:

- the live Bonfyre site at `https://nickgonzales76017.github.io/bonfyre`
- and its publishing repo

should be treated as the current product source of truth.

This file is therefore a migration map for lagging or duplicate material in the vault, not a claim that the vault is currently canonical.

Legend:

- `canonical duplicate`: exists in both the vault and runtime repo; keep one canonical code host and retire the duplicate
- `fold-in runtime` / `near-match module`: should become part of a layer repo, not a standalone product repo
- `adjacent product`: product/system code that should live outside core Bonfyre runtime repos
- `generated output`: not source, should not drive architecture decisions

| Name | Classification | Current Truth | Target Repo | Action | Notes |
|---|---|---|---|---|---|
| --out | generated output | vault only | archive/generated | remove from source-of-truth map after audit | Generated output, not source. |
| _shared | shared substrate | partial migration in progress | bonfyre-core | continue moving shared utilities into bonfyre-core; retire vault copies after cutover | `bonfyre_toolkit.py` imported into `bonfyre-core/scripts/`; broader `_shared` audit still pending. |
| AcousticClassifier | near-match module | vault only | bonfyre-intelligence | fold into layer repo | Likely classifier support for tone/tag stages. |
| AIOverseasLaborPipeline | adjacent product | vault only | separate product repo | spin off if still active | Not core Bonfyre runtime. |
| AmbientLogisticsLayer | adjacent product | vault only | separate product repo | spin off if still active | Product/system concept rather than core Bonfyre runtime. |
| ArtifactManifest | shared substrate | migrated to bonfyre-core scaffold | bonfyre-core | continue using bonfyre-core as canonical source; retire vault copy after cutover | Imported into `bonfyre-core/schemas/artifact-manifest/`. |
| AudioIntakeNormalizer | near-match module | vault only | bonfyre-intake | fold into layer repo | Intake preprocessing utility. |
| BatchFailureQueue | ops helper | vault only | bonfyre-control | fold into layer repo | Queue/orchestration support. |
| BatchJobRunner | ops helper | vault only | bonfyre-control | fold into layer repo | Runner/orchestration support. |
| BonfyreAPI | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreAuth | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreBrief | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intelligence | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreCanon | fold-in runtime | migrated to bonfyre-core scaffold | bonfyre-core | continue using bonfyre-core as canonical source; retire bonfyre-oss copy after cutover | Imported into `bonfyre-core/cmd/bonfyre-canon/` from the live runtime tree. |
| BonfyreCLI | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreClips | fold-in runtime | bonfyre-oss only | bonfyre-publish | keep canonical in runtime repo for now; create layer destination | No exact vault peer. |
| BonfyreCMS | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreCompress | canonical duplicate | migrated to bonfyre-core scaffold | bonfyre-core | continue using bonfyre-core as canonical source; retire old copies after cutover | Imported into `bonfyre-core/cmd/bonfyre-compress/` from the live runtime tree. |
| BonfyreDistribute | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-publish | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreEmbed | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intelligence | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreEmit | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-publish | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreFinance | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreGate | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreGraph | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intelligence | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreHash | canonical duplicate | migrated to bonfyre-core scaffold | bonfyre-core | continue using bonfyre-core as canonical source; retire old copies after cutover | Imported into `bonfyre-core/cmd/bonfyre-hash/` from the live runtime tree. |
| BonfyreIndex | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intelligence | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreIngest | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intake | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreLedger | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreMediaPrep | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intake | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreMeter | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreMFADict | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intake | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreNarrate | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-publish | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreOffer | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreOutreach | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyrePack | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-publish | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreParagraph | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intake | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyrePay | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyrePipeline | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreProject | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreProof | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intelligence | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreProxy | fold-in runtime | bonfyre-oss only | bonfyre-control | keep canonical in runtime repo for now; create layer destination | Operational/API surface. |
| BonfyreQuery | fold-in runtime | bonfyre-oss only | bonfyre-intelligence | keep canonical in runtime repo for now; create layer destination | Retrieval/query layer. |
| BonfyreQueue | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreRender | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-publish | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreRepurpose | fold-in runtime | bonfyre-oss only | bonfyre-publish | keep canonical in runtime repo for now; create layer destination | Repurposing/output stage. |
| BonfyreRuntime | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreSegment | fold-in runtime | bonfyre-oss only | bonfyre-intake | keep canonical in runtime repo for now; create layer destination | Segmentation / preprocessing stage. |
| BonfyreSpeechLoop | fold-in runtime | bonfyre-oss only | bonfyre-control | keep canonical in runtime repo for now; create layer destination | Operational speech workflow helper. |
| BonfyreStitch | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreSync | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-control | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreTag | fold-in runtime | bonfyre-oss only | bonfyre-intelligence | keep canonical in runtime repo for now; create layer destination | Tagging/semantic layer. |
| BonfyreTel | fold-in runtime | bonfyre-oss only | bonfyre-control | keep canonical in runtime repo for now; create layer destination | Telemetry/control-plane surface. |
| BonfyreTone | fold-in runtime | bonfyre-oss only | bonfyre-intelligence | keep canonical in runtime repo for now; create layer destination | Classification/analysis stage. |
| BonfyreTranscribe | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intake | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreTranscriptClean | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intake | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreTranscriptFamily | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intake | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| BonfyreVec | fold-in runtime | bonfyre-oss only | bonfyre-intelligence | keep canonical in runtime repo for now; create layer destination | Vector/retrieval stage. |
| BonfyreWeaviateIndex | canonical duplicate | bonfyre-oss/cmd + vault duplicate | bonfyre-intelligence | move canonical source to layer repo; archive vault duplicate | Exact runtime module exists in both places. |
| DeliverableFormatterEngine | publish helper | vault only | bonfyre-publish | fold into layer repo | Output formatting/presentation layer. |
| DiarizationRESTService | near-match module | vault only | bonfyre-intake | fold into layer repo | Speech intake/diarization service. |
| DuckDBAnalytics | analytics helper | vault only | bonfyre-intelligence | fold into layer repo | Retrieval/analysis support. |
| FaissLocalSearch | retrieval helper | vault only | bonfyre-intelligence | fold into layer repo | Vector search support. |
| FswatchReactor | ops helper | vault only | bonfyre-control | fold into layer repo | Automation/watch runtime helper. |
| ImageMagickOps | publish helper | vault only | bonfyre-publish | fold into layer repo | Rendering/output asset helper. |
| JqCanon | canon helper | migrated to bonfyre-core scaffold | bonfyre-core | continue using bonfyre-core as canonical source; retire vault copy after cutover | Imported into `bonfyre-core/tools/jq-canon/`. |
| LanguageRouting | analysis helper | vault only | bonfyre-intelligence | fold into layer repo | Routing/classification helper. |
| liblambda-tensors | shared substrate | migrated to bonfyre-core scaffold | bonfyre-core | continue using bonfyre-core as canonical source; retire vault copy after cutover | Imported into `bonfyre-core/lib/liblambda-tensors/`; extracted copy was cleaned of build artifacts. |
| LocalAITranscriptionService | adjacent product | vault only | separate product repo | spin off if still active | Product built on Bonfyre-like pipeline, not core runtime itself. |
| LocalBootstrapKit | ops helper | vault only | bonfyre-control | fold into layer repo | Bootstrap/developer tooling. |
| MFA_DictBuilder | near-match module | vault only | bonfyre-intake | fold into layer repo | Natural companion to BonfyreMFADict. |
| NightlyBrainstorm | ops/productivity tool | vault only | separate ops/tooling repo | spin off or keep in vault | Not core runtime. |
| ONNXEmbedder | retrieval helper | vault only | bonfyre-intelligence | fold into layer repo | Embedding helper. |
| PandocFormatter | publish helper | vault only | bonfyre-publish | fold into layer repo | Output formatting. |
| PersonalDataEngine | adjacent product | vault only | separate product repo | spin off if still active | Product/system, not core runtime. |
| PersonalMarketLayer | adjacent product | vault only | separate product repo | spin off if still active | Product, not core runtime. |
| PredictionMarketDataTool | adjacent product | vault only | separate product repo | spin off if still active | Not core Bonfyre runtime. |
| ProductPipelines | ops helper | vault only | bonfyre-control | fold into layer repo | Pipeline orchestration support. |
| PublishAudio | publish helper | vault only | bonfyre-publish | fold into layer repo | Publishing/output tool. |
| QualityBenchmarkPack | ops/testing helper | vault only | bonfyre-control | fold into layer repo | Benchmark/test infrastructure. |
| QualityScoringLoop | ops/testing helper | vault only | bonfyre-control | fold into layer repo | Quality/loop infrastructure. |
| QuietDistributionEngine | publish helper | vault only | bonfyre-publish | fold into layer repo | Distribution/output helper. |
| RepackagedServiceMarketplace | adjacent product | vault only | separate product repo | spin off if still active | Product repo candidate. |
| RNNTGrpcServer | near-match module | vault only | bonfyre-intake | fold into layer repo | Speech/transcription serving support. |
| ServiceArbitrageHub | adjacent product | vault only | separate product repo | spin off if still active | Not core runtime. |
| SileroVADCLI | near-match module | vault only | bonfyre-intake | fold into layer repo | Speech preprocessing helper. |
| SimpleIntakePortal | adjacent product | vault only | separate product repo | spin off if still active | App/product shell, not runtime. |
| SoxAudioOps | near-match module | vault only | bonfyre-intake | fold into layer repo | Audio preprocessing helper. |
| SpeakerDiarization | near-match module | vault only | bonfyre-intake | fold into layer repo | Speech intake helper. |
| SpeakerSegmentationLayer | near-match module | vault only | bonfyre-intake | fold into layer repo | Segmentation helper. |
| SQLiteGraphStore | retrieval helper | vault only | bonfyre-intelligence | fold into layer repo | Graph/query storage support. |
| StreamingASR | near-match module | vault only | bonfyre-intake | fold into layer repo | Speech/transcription support. |
| SummaryPromptPack | publish/helper content | vault only | bonfyre-publish | fold into layer repo or docs | Prompt/output shaping support. |
| TesseractOCR | near-match module | vault only | bonfyre-intake | fold into layer repo | Document intake helper. |
| TokenizerLayer | analysis helper | vault only | bonfyre-intelligence | fold into layer repo | Text analysis support. |
| TranscriptAssetStore | publish helper | vault only | bonfyre-publish | fold into layer repo | Output/published assets. |
| TranscriptCleanupLayer | near-match module | vault only | bonfyre-intake | fold into layer repo | Closest peer to TranscriptClean family. |
| TranscriptParagraphizer | near-match module | vault only | bonfyre-intake | fold into layer repo | Closest peer to BonfyreParagraph. |
| TranscriptQACleaner | analysis helper | vault only | bonfyre-intelligence | fold into layer repo | Quality analysis on transcripts. |
| TreeSitterCanon | canon helper | migrated to bonfyre-core scaffold | bonfyre-core | continue using bonfyre-core as canonical source; retire vault copy after cutover | Imported into `bonfyre-core/tools/tree-sitter-canon/`. |
| WeaviateIndexer | retrieval helper | vault only | bonfyre-intelligence | fold into layer repo | Indexing support. |
| WebWorkerSaaS | adjacent product | vault only | separate product repo | spin off if still active | Product repo candidate. |
| WhisperAlignment | near-match module | vault only | bonfyre-intake | fold into layer repo | Transcription post-process helper. |
| WhisperFFmpegWrapperKit | near-match module | vault only | bonfyre-intake | fold into layer repo | Audio/transcription preprocessing helper. |
| WhisperModelCacheManager | near-match module | vault only | bonfyre-intake | fold into layer repo | Model/runtime support helper. |
| YtdlpIngest | near-match module | vault only | bonfyre-intake | fold into layer repo | Source acquisition helper. |
| ZstdFamilyCompressor | shared substrate | migrated to bonfyre-core scaffold | bonfyre-core | continue using bonfyre-core as canonical source; retire vault copy after cutover | Imported into `bonfyre-core/tools/zstd-family-compressor/`. |

## Summary

- the live Bonfyre site and its publishing repo should be treated as current product truth.
- `bonfyre-oss/cmd/*` should be treated as stage/runtime truth where it matches the promoted build.
- `/Users/nickgonzales/Documents/Bonfyre/10-Code/*` should be treated as mixed lagging/experimental/archive workspace until each row above is migrated.
- First extraction should still be `bonfyre-core`, because many rows above depend on shared canon/compression/library substrate.
- After that, migrate by layer: `bonfyre-intake`, `bonfyre-intelligence`, `bonfyre-publish`, `bonfyre-control`.
