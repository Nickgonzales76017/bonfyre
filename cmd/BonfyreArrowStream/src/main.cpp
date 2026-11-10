// Stream canonical Parquet as Arrow IPC without materializing a second estate.
// This source intentionally does not depend on Python or a wheel-specific path;
// packaging must supply the Arrow C++ and Parquet libraries before deployment.
#include "../../BonfyreArrowFlight/src/sha256.h"

#include <arrow/io/file.h>
#include <arrow/io/stdio.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include <charconv>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string_view>

namespace {

arrow::Status Stream(const char* path, int64_t batch_size,
                     std::string_view expected_digest) {
  parquet::ArrowReaderProperties properties;
  properties.set_batch_size(batch_size);
  parquet::arrow::FileReaderBuilder builder;
  if (expected_digest.empty()) {
    ARROW_RETURN_NOT_OK(builder.OpenFile(path));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto source, arrow::io::ReadableFile::Open(path));
    ARROW_ASSIGN_OR_RAISE(auto digest, bonfyre_arrow::Sha256File(source));
    if (digest != expected_digest)
      return arrow::Status::Invalid("canonical Parquet drift: ", path);
    ARROW_RETURN_NOT_OK(builder.Open(std::move(source)));
  }
  builder.properties(properties);
  ARROW_ASSIGN_OR_RAISE(auto file, builder.Build());
  std::shared_ptr<arrow::Schema> schema;
  ARROW_RETURN_NOT_OK(file->GetSchema(&schema));
  ARROW_ASSIGN_OR_RAISE(auto batches, file->GetRecordBatchReader());

  auto sink = std::make_shared<arrow::io::StdoutStream>();
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink, schema));
  for (;;) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batches->Next());
    if (!batch) break;
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  ARROW_RETURN_NOT_OK(writer->Close());
  return sink->Close();
}

}  // namespace
int bonfyre_density_index(int argc,char** argv);


int main(int argc, char** argv) {
  if (argc>=2 && std::string_view(argv[1])=="index") {
    return bonfyre_density_index(argc,argv);
  }
  if (argc == 2 && (std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "-h" || std::string_view(argv[1]) == "help")) {
    std::cout << "usage: bonfyre parquet PARQUET [--batch-size POSITIVE_INTEGER] [--expected-sha256 HEX]" << '\n';
    std::cout << "  Streams digest-pinned Parquet as Arrow IPC on stdout; no publication or installation" << '\n';
    std::cout << "  bonfyre parquet index --cut CUT.yaff --pack-image PACK.bfp --out NEW.parquet" << '\n';
    return 0;
  }
  if (argc < 2 || (argc - 2) % 2 != 0) {
    std::cerr << "usage: bonfyre-arrow-stream PARQUET [--batch-size POSITIVE_INTEGER] [--expected-sha256 HEX]\n";
    return 2;
  }
  int64_t batch_size = 65536;
  std::string_view expected_digest;
  bool batch_seen = false, digest_seen = false;
  for (int i = 2; i < argc; i += 2) {
    const std::string_view option(argv[i]), value(argv[i + 1]);
    if (option == "--batch-size" && !batch_seen) {
      const auto parsed = std::from_chars(value.data(), value.data() + value.size(), batch_size);
      if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() ||
          batch_size <= 0) {
        std::cerr << "batch size must be a positive integer\n";
        return 2;
      }
      batch_seen = true;
    } else if (option == "--expected-sha256" && !digest_seen) {
      if (value.size() != 64 || value.find_first_not_of("0123456789abcdef") != value.npos) {
        std::cerr << "expected SHA-256 must be 64 lowercase hex digits\n";
        return 2;
      }
      expected_digest = value;
      digest_seen = true;
    } else {
      std::cerr << "unknown or repeated option: " << option << '\n';
      return 2;
    }
  }
  const auto status = Stream(argv[1], batch_size, expected_digest);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    return 1;
  }
  return 0;
}
/* The Arrow owner builds one translation unit; this is a derived index, not another runtime. */
#include "density_index.cpp"
