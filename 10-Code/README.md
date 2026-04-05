# 10-Code

This folder contains implementation work for the projects tracked in the vault.

## Current Build Order

### Core Projects
- `LocalAITranscriptionService`
- `PersonalMarketLayer`
- `ServiceArbitrageHub`
- `QuietDistributionEngine`
- `PersonalDataEngine`
- `RepackagedServiceMarketplace`
- `AIOverseasLaborPipeline`
- `WebWorkerSaaS`
- `ProductPipelines` — named product loops that tie the active systems together
- `BonfyreMediaPrep` — first native low-level audio binary

### Standalone Projects
- `AmbientLogisticsLayer` — local logistics coordination
- `PredictionMarketDataTool` — prediction market signal detection

### Transcription Pipeline Components
- `AudioIntakeNormalizer` — ffmpeg audio preprocessing
- `BatchJobRunner` — multi-file batch processing
- `BatchFailureQueue` — error capture + retry manifests
- `DeliverableFormatterEngine` — buyer-ready output formatting
- `LocalBootstrapKit` — one-command machine setup
- `QualityBenchmarkPack` — human-rated transcript benchmarks
- `QualityScoringLoop` — heuristic quality scoring
- `SimpleIntakePortal` — customer-facing upload page
- `SpeakerSegmentationLayer` — speaker diarization
- `SummaryPromptPack` — buyer-tuned prompt templates
- `TranscriptAssetStore` — indexed deliverable storage
- `TranscriptCleanupLayer` — filler removal + punctuation fix
- `TranscriptParagraphizer` — readable paragraph splitting
- `WhisperModelCacheManager` — model preflight + cache management

## Rule
Build the smallest useful thing that supports the active project notes first. Documentation lives in the vault; implementation lives here.
