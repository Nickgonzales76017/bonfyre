# Bonfyre Queue

Tiny native file-backed queue utility for shared local job staging.

## What It Does

- `enqueue` a job into a queue file
- `list` queued and claimed jobs
- `claim` the next queued job
- `complete` a claimed job
- `fail` a claimed job
- `stats` for queue counts

## Build

```bash
cd 10-Code/BonfyreQueue
make
```

## Usage

```bash
arch -arm64 ./bonfyre-queue enqueue ../../.bonfyre-runtime/native-test-queue.tsv demo-job /tmp/demo.json --source BonfyreSync --priority 5
arch -arm64 ./bonfyre-queue list ../../.bonfyre-runtime/native-test-queue.tsv
arch -arm64 ./bonfyre-queue claim ../../.bonfyre-runtime/native-test-queue.tsv --worker local-dev
arch -arm64 ./bonfyre-queue complete ../../.bonfyre-runtime/native-test-queue.tsv 1
arch -arm64 ./bonfyre-queue stats ../../.bonfyre-runtime/native-test-queue.tsv
```
