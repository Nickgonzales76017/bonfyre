#include <bonfyre_arrow_c.h>

#include <arrow/c/bridge.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>

#include <iostream>

arrow::Status Verify(const char* path, const char* digest) {
  ArrowArrayStream stream{};
  char error[256]{};
  if (bf_parquet_export_verified_stream(path, digest, 1, &stream,
                                        error, sizeof(error)) != 0)
    return arrow::Status::IOError(error);
  if (!stream.release) return arrow::Status::Invalid("C Stream has no release callback");
  ARROW_ASSIGN_OR_RAISE(auto imported, arrow::ImportRecordBatchReader(&stream));
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  for (;;) {
    ARROW_ASSIGN_OR_RAISE(auto batch, imported->Next());
    if (!batch) break;
    batches.push_back(std::move(batch));
  }
  if (batches.size() != 2) return arrow::Status::Invalid("C Stream batch count differs");
  ARROW_ASSIGN_OR_RAISE(auto actual, arrow::Table::FromRecordBatches(batches));
  parquet::arrow::FileReaderBuilder builder;
  ARROW_RETURN_NOT_OK(builder.OpenFile(path));
  ARROW_ASSIGN_OR_RAISE(auto parquet, builder.Build());
  ARROW_ASSIGN_OR_RAISE(auto expected, parquet->ReadTable());
  if (!actual->Equals(*expected, true))
    return arrow::Status::Invalid("C Stream values or schema differ");
  ArrowArrayStream refused{};
  if (bf_parquet_export_verified_stream(path,
      "0000000000000000000000000000000000000000000000000000000000000000",
      1, &refused, error, sizeof(error)) == 0 || refused.release != nullptr ||
      std::string(error).find("canonical Parquet drift") == std::string::npos)
    return arrow::Status::Invalid("wrong digest was accepted");
  if (bf_parquet_export_stream(path, 0, &refused, error, sizeof(error)) == 0 ||
      refused.release != nullptr)
    return arrow::Status::Invalid("invalid batch size was accepted");
  if (bf_parquet_export_stream("/missing-bonfyre-parquet", 1, &refused,
                               error, sizeof(error)) == 0 ||
      refused.release != nullptr || error[0] == '\0')
    return arrow::Status::Invalid("missing source was accepted");
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc != 3) return 2;
  auto status = Verify(argv[1], argv[2]);
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  std::cout << "Arrow C Stream ABI matches Parquet\n";
  return 0;
}
