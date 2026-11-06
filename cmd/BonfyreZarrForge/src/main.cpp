// Bounded Arrow/Parquet batch lowering into Zarr v2 numeric arrays.
// Output is staged by the caller. Non-numeric columns remain in canonical
// Parquet; every numeric value has a separate validity array for exact nulls.
#include "../../BonfyreArrowFlight/src/sha256.h"

#include <arrow/array.h>
#include <arrow/io/file.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <algorithm>
#include <bit>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <limits>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>
#include <sys/statvfs.h>

namespace {
namespace fs = std::filesystem;

std::string JsonString(std::string_view value) {
  std::string out = "\"";
  constexpr char hex[] = "0123456789abcdef";
  for (const unsigned char ch : value) {
    if (ch == '"' || ch == '\\') { out += '\\'; out += ch; }
    else if (ch < 0x20) {
      out += "\\u00";
      out += hex[ch >> 4];
      out += hex[ch & 15];
    } else out += ch;
  }
  return out + '"';
}

struct Column {
  int index = 0;
  std::string name;
  std::string dtype;
  size_t width = 0;
  std::vector<char> values;
  std::vector<char> valid;
  int64_t chunk = 0;
  size_t used = 0;
};

std::pair<std::string, size_t> NumericDtype(arrow::Type::type type) {
  switch (type) {
    case arrow::Type::INT8: return {"|i1", 1};
    case arrow::Type::UINT8: return {"|u1", 1};
    case arrow::Type::INT16: return {"<i2", 2};
    case arrow::Type::UINT16: return {"<u2", 2};
    case arrow::Type::INT32: return {"<i4", 4};
    case arrow::Type::UINT32: return {"<u4", 4};
    case arrow::Type::INT64: return {"<i8", 8};
    case arrow::Type::UINT64: return {"<u8", 8};
    case arrow::Type::FLOAT: return {"<f4", 4};
    case arrow::Type::DOUBLE: return {"<f8", 8};
    default: return {"", 0};
  }
}

const void* RawValues(const arrow::Array& array) {
  switch (array.type_id()) {
    case arrow::Type::INT8: return static_cast<const arrow::Int8Array&>(array).raw_values();
    case arrow::Type::UINT8: return static_cast<const arrow::UInt8Array&>(array).raw_values();
    case arrow::Type::INT16: return static_cast<const arrow::Int16Array&>(array).raw_values();
    case arrow::Type::UINT16: return static_cast<const arrow::UInt16Array&>(array).raw_values();
    case arrow::Type::INT32: return static_cast<const arrow::Int32Array&>(array).raw_values();
    case arrow::Type::UINT32: return static_cast<const arrow::UInt32Array&>(array).raw_values();
    case arrow::Type::INT64: return static_cast<const arrow::Int64Array&>(array).raw_values();
    case arrow::Type::UINT64: return static_cast<const arrow::UInt64Array&>(array).raw_values();
    case arrow::Type::FLOAT: return static_cast<const arrow::FloatArray&>(array).raw_values();
    case arrow::Type::DOUBLE: return static_cast<const arrow::DoubleArray&>(array).raw_values();
    default: return nullptr;
  }
}

arrow::Status Write(const fs::path& path, const std::string& body) {
  std::ofstream file(path, std::ios::binary | std::ios::trunc);
  if (!file) return arrow::Status::IOError("cannot open Zarr member: ", path.string());
  file.write(body.data(), static_cast<std::streamsize>(body.size()));
  if (!file) return arrow::Status::IOError("cannot write Zarr member: ", path.string());
  return arrow::Status::OK();
}

arrow::Status Write(const fs::path& path, const std::vector<char>& body) {
  std::ofstream file(path, std::ios::binary | std::ios::trunc);
  if (!file) return arrow::Status::IOError("cannot open Zarr chunk: ", path.string());
  file.write(body.data(), static_cast<std::streamsize>(body.size()));
  if (!file) return arrow::Status::IOError("cannot write Zarr chunk: ", path.string());
  return arrow::Status::OK();
}

arrow::Status Flush(const fs::path& out, Column& column) {
  if (!column.used) return arrow::Status::OK();
  ARROW_RETURN_NOT_OK(Write(out / ("c" + std::to_string(column.index)) / std::to_string(column.chunk), column.values));
  ARROW_RETURN_NOT_OK(Write(out / ("m" + std::to_string(column.index)) / std::to_string(column.chunk), column.valid));
  ++column.chunk;
  column.used = 0;
  std::fill(column.values.begin(), column.values.end(), 0);
  std::fill(column.valid.begin(), column.valid.end(), 0);
  return arrow::Status::OK();
}

arrow::Status ArrayMetadata(const fs::path& path, int64_t rows, int64_t chunk,
                            std::string_view dtype) {
  const std::string body = "{\"zarr_format\":2,\"shape\":[" + std::to_string(rows) +
      "],\"chunks\":[" + std::to_string(chunk) + "],\"dtype\":" + JsonString(dtype) +
      ",\"compressor\":null,\"fill_value\":0,\"order\":\"C\",\"filters\":null}\n";
  return Write(path / ".zarray", body);
}

arrow::Status Lower(const fs::path& source, const fs::path& out,
                    std::string_view expected_digest, int64_t chunk_size) {
  if constexpr (std::endian::native != std::endian::little)
    return arrow::Status::NotImplemented("Zarr numeric writer requires little-endian host");
  ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(source.string()));
  ARROW_ASSIGN_OR_RAISE(auto digest, bonfyre_arrow::Sha256File(input));
  if (digest != expected_digest) return arrow::Status::Invalid("canonical Parquet digest drift");
  parquet::ArrowReaderProperties properties;
  properties.set_batch_size(chunk_size);
  parquet::arrow::FileReaderBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Open(std::move(input)));
  builder.properties(properties);
  ARROW_ASSIGN_OR_RAISE(auto reader, builder.Build());
  std::shared_ptr<arrow::Schema> schema;
  ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));
  const int64_t row_count = reader->parquet_reader()->metadata()->num_rows();
  std::vector<Column> columns;
  for (int i = 0; i < schema->num_fields(); ++i) {
    const auto [dtype, width] = NumericDtype(schema->field(i)->type()->id());
    if (width) columns.push_back({i, schema->field(i)->name(), dtype, width,
      std::vector<char>(static_cast<size_t>(chunk_size) * width, 0),
      std::vector<char>(static_cast<size_t>(chunk_size), 0)});
  }
  if (columns.empty()) return arrow::Status::Invalid("no top-level numeric columns for Zarr");
  // Admission is based on the actual uncompressed output shape and live
  // volume capacity, not Parquet's often misleading compressed byte count.
  const uint64_t chunks = (static_cast<uint64_t>(row_count) + chunk_size - 1) / chunk_size;
  const uint64_t bytes_per_row = std::accumulate(columns.begin(), columns.end(), uint64_t{0},
      [](uint64_t sum, const Column& column) { return sum + column.width + 1; });
  if (chunks > std::numeric_limits<uint64_t>::max() / static_cast<uint64_t>(chunk_size) /
                   bytes_per_row)
    return arrow::Status::CapacityError("Zarr size estimate overflow");
  const uint64_t expected_bytes = chunks * static_cast<uint64_t>(chunk_size) * bytes_per_row;
  const uint64_t expected_files = chunks * columns.size() * 2 + columns.size() * 4 + 2;
  struct statvfs volume {};
  if (statvfs(out.parent_path().c_str(), &volume) != 0)
    return arrow::Status::IOError("cannot inspect Zarr output volume");
  const uint64_t free_bytes = static_cast<uint64_t>(volume.f_bavail) * volume.f_frsize;
  if (expected_bytes > free_bytes / 2 || expected_files > volume.f_favail / 2)
    return arrow::Status::CapacityError("Zarr output exceeds half of available bytes or inodes");
  if (fs::exists(out)) return arrow::Status::Invalid("Zarr output already exists: ", out.string());
  fs::create_directories(out);
  ARROW_RETURN_NOT_OK(Write(out / ".zgroup", "{\"zarr_format\":2}\n"));
  std::string attributes = "{\"bonfyre:sourceParquetSha256\":" + JsonString(digest) + ",\"bonfyre:columns\":[";
  for (auto& column : columns) {
    const auto value = out / ("c" + std::to_string(column.index));
    const auto mask = out / ("m" + std::to_string(column.index));
    fs::create_directories(value);
    fs::create_directories(mask);
    ARROW_RETURN_NOT_OK(ArrayMetadata(value, row_count, chunk_size, column.dtype));
    ARROW_RETURN_NOT_OK(ArrayMetadata(mask, row_count, chunk_size, "|u1"));
    if (&column != &columns.front()) attributes += ',';
    attributes += "{\"name\":" + JsonString(column.name) + ",\"values\":" +
      JsonString("c" + std::to_string(column.index)) + ",\"validity\":" +
      JsonString("m" + std::to_string(column.index)) + ",\"dtype\":" +
      JsonString(column.dtype) + "}";
  }
  attributes += "]}\n";
  ARROW_RETURN_NOT_OK(Write(out / ".zattrs", attributes));
  ARROW_ASSIGN_OR_RAISE(auto batches, reader->GetRecordBatchReader());
  int64_t consumed = 0;
  for (;;) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batches->Next());
    if (!batch) break;
    for (int64_t row = 0; row < batch->num_rows(); ++row) {
      for (auto& column : columns) {
        const auto& array = *batch->column(column.index);
        if (array.IsValid(row)) {
          const char* raw = static_cast<const char*>(RawValues(array));
          std::memcpy(column.values.data() + column.used * column.width,
                      raw + row * column.width, column.width);
          column.valid[column.used] = 1;
        }
        ++column.used;
        if (column.used == static_cast<size_t>(chunk_size)) ARROW_RETURN_NOT_OK(Flush(out, column));
      }
    }
    consumed += batch->num_rows();
  }
  if (consumed != row_count) return arrow::Status::Invalid("Parquet metadata row count differs from batches");
  for (auto& column : columns) ARROW_RETURN_NOT_OK(Flush(out, column));
  ARROW_ASSIGN_OR_RAISE(auto final_input, arrow::io::ReadableFile::Open(source.string()));
  ARROW_ASSIGN_OR_RAISE(auto final_digest, bonfyre_arrow::Sha256File(final_input));
  if (final_digest != digest) return arrow::Status::Invalid("canonical Parquet changed during Zarr lowering");
  std::cout << "{\"rows\":" << row_count << ",\"numeric_columns\":" << columns.size() << "}\n";
  return arrow::Status::OK();
}
}  // namespace

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cerr << "usage: bonfyre-zarr-forge PARQUET OUTPUT_DIRECTORY EXPECTED_SHA256 CHUNK_SIZE\n";
    return 2;
  }
  const std::string_view digest(argv[3]), size(argv[4]);
  int64_t chunk_size = 0;
  const auto parsed = std::from_chars(size.data(), size.data() + size.size(), chunk_size);
  if (digest.size() != 64 || digest.find_first_not_of("0123456789abcdef") != digest.npos ||
      parsed.ec != std::errc{} || parsed.ptr != size.data() + size.size() ||
      chunk_size < 1 || chunk_size > 1'000'000) {
    std::cerr << "invalid digest or chunk size\n";
    return 2;
  }
  const auto status = Lower(argv[1], argv[2], digest, chunk_size);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    return 1;
  }
  return 0;
}
