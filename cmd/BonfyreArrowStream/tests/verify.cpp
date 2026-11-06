#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include <iostream>
#include <memory>

arrow::Status Verify(const char* parquet_path, const char* stream_path) {
  parquet::ArrowReaderProperties properties;
  properties.set_batch_size(1);
  parquet::arrow::FileReaderBuilder builder;
  ARROW_RETURN_NOT_OK(builder.OpenFile(parquet_path));
  builder.properties(properties);
  ARROW_ASSIGN_OR_RAISE(auto parquet, builder.Build());
  std::shared_ptr<arrow::Schema> schema;
  ARROW_RETURN_NOT_OK(parquet->GetSchema(&schema));
  ARROW_ASSIGN_OR_RAISE(auto expected, parquet->GetRecordBatchReader());
  ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(stream_path));
  ARROW_ASSIGN_OR_RAISE(auto actual, arrow::ipc::RecordBatchStreamReader::Open(input));
  if (!schema->Equals(*actual->schema(), true)) {
    return arrow::Status::Invalid("schema or metadata differs");
  }
  int batches = 0;
  int64_t rows = 0;
  for (;;) {
    ARROW_ASSIGN_OR_RAISE(auto left, expected->Next());
    ARROW_ASSIGN_OR_RAISE(auto right, actual->Next());
    if (!left || !right) {
      if (left || right) return arrow::Status::Invalid("batch count differs");
      break;
    }
    if (!left->Equals(*right, true)) {
      return arrow::Status::Invalid("record batch differs");
    }
    ++batches;
    rows += right->num_rows();
  }
  if (batches != 2 || rows != 2) {
    return arrow::Status::Invalid("expected two one-row batches");
  }
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc != 3) return 2;
  const auto status = Verify(argv[1], argv[2]);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    return 1;
  }
  std::cout << "Arrow IPC schema, metadata, batches, and values match Parquet\n";
  return 0;
}
