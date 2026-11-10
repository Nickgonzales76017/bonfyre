#include "bonfyre_arrow_c.h"
#include "sha256.h"

#include <arrow/c/bridge.h>
#include <arrow/io/file.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <exception>
#include <memory>

namespace {

class PinnedParquetReader final : public arrow::RecordBatchReader {
 public:
  PinnedParquetReader(std::unique_ptr<parquet::arrow::FileReader> file,
                      std::shared_ptr<arrow::RecordBatchReader> reader)
      : file_(std::move(file)), reader_(std::move(reader)) {}

  std::shared_ptr<arrow::Schema> schema() const override { return reader_->schema(); }
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    return reader_->ReadNext(batch);
  }
  arrow::Status Close() override { return reader_->Close(); }

 private:
  // ArrowArrayStream release drops reader_ before the Parquet file it uses.
  std::unique_ptr<parquet::arrow::FileReader> file_;
  std::shared_ptr<arrow::RecordBatchReader> reader_;
};

int Fail(const arrow::Status& status, char* error, size_t capacity) {
  if (error && capacity) std::snprintf(error, capacity, "%s", status.ToString().c_str());
  return EIO;
}

arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> VerifiedFile(
    const char* path, const char* expected) {
  ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto actual, bonfyre_arrow::Sha256File(file));
  if (actual != expected)
    return arrow::Status::Invalid("canonical Parquet drift: ", path);
  return file;
}

int ExportStream(const char* path, const char* expected, int64_t batch_size,
                 struct ArrowArrayStream* out, char* error_buffer,
                 size_t error_capacity) {
  if (error_buffer && error_capacity) error_buffer[0] = '\0';
  if (!out) return EINVAL;
  std::memset(out, 0, sizeof(*out));
  if (!path || !*path || batch_size <= 0 || (expected && std::strlen(expected) != 64))
    return EINVAL;
  try {
    parquet::ArrowReaderProperties properties;
    properties.set_batch_size(batch_size);
    parquet::arrow::FileReaderBuilder builder;
    arrow::Status status;
    if (expected) {
      auto file = VerifiedFile(path, expected);
      if (!file.ok()) return Fail(file.status(), error_buffer, error_capacity);
      status = builder.Open(std::move(*file));
    } else {
      status = builder.OpenFile(path);
    }
    if (!status.ok()) return Fail(status, error_buffer, error_capacity);
    builder.properties(properties);
    auto file = builder.Build();
    if (!file.ok()) return Fail(file.status(), error_buffer, error_capacity);
    auto reader = (*file)->GetRecordBatchReader();
    if (!reader.ok()) return Fail(reader.status(), error_buffer, error_capacity);
    std::shared_ptr<arrow::RecordBatchReader> shared(std::move(*reader));
    auto pinned = std::make_shared<PinnedParquetReader>(std::move(*file), shared);
    status = arrow::ExportRecordBatchReader(pinned, out);
    if (!status.ok()) return Fail(status, error_buffer, error_capacity);
    return 0;
  } catch (const std::exception& error) {
    if (error_buffer && error_capacity)
      std::snprintf(error_buffer, error_capacity, "%s", error.what());
    return EIO;
  }
}

}  // namespace

extern "C" __attribute__((visibility("default")))
int bf_parquet_export_stream(const char* path, int64_t batch_size,
                             struct ArrowArrayStream* out,
                             char* error_buffer, size_t error_capacity) {
  return ExportStream(path, nullptr, batch_size, out, error_buffer, error_capacity);
}

extern "C" __attribute__((visibility("default")))
int bf_parquet_export_verified_stream(const char* path, const char* sha256,
                                      int64_t batch_size, struct ArrowArrayStream* out,
                                      char* error_buffer, size_t error_capacity) {
  return ExportStream(path, sha256 ? sha256 : "", batch_size, out,
                      error_buffer, error_capacity);
}
