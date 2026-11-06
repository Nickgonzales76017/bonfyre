// Read-only Arrow Flight view of the canonical Parquet catalog.
// The catalog is an input receipt, not an authority or a second data body.
#include "sha256.h"

#include <arrow/flight/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <simdjson.h>

#include <charconv>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace {

struct Dataset {
  std::string parquet;
  std::string digest;
  std::string frontier;
  uintmax_t verified_bytes = 0;
  std::filesystem::file_time_type verified_mtime;
};

arrow::Status VerifyDataset(Dataset* dataset) {
  std::error_code error;
  const auto before_bytes = std::filesystem::file_size(dataset->parquet, error);
  if (error) return arrow::Status::IOError("cannot stat ", dataset->parquet);
  const auto before_mtime = std::filesystem::last_write_time(dataset->parquet, error);
  if (error) return arrow::Status::IOError("cannot stat ", dataset->parquet);
  ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(dataset->parquet));
  ARROW_ASSIGN_OR_RAISE(auto digest, bonfyre_arrow::Sha256File(file));
  const auto after_bytes = std::filesystem::file_size(dataset->parquet, error);
  if (error) return arrow::Status::IOError("cannot stat ", dataset->parquet);
  const auto after_mtime = std::filesystem::last_write_time(dataset->parquet, error);
  if (error) return arrow::Status::IOError("cannot stat ", dataset->parquet);
  if (before_bytes != after_bytes || before_mtime != after_mtime ||
      digest != dataset->digest)
    return arrow::Status::Invalid("canonical Parquet drift: ", dataset->parquet);
  dataset->verified_bytes = after_bytes;
  dataset->verified_mtime = after_mtime;
  return arrow::Status::OK();
}

arrow::Status CheckDataset(const Dataset& dataset) {
  std::error_code error;
  const auto bytes = std::filesystem::file_size(dataset.parquet, error);
  if (error) return arrow::Status::IOError("cannot stat ", dataset.parquet);
  const auto mtime = std::filesystem::last_write_time(dataset.parquet, error);
  if (error) return arrow::Status::IOError("cannot stat ", dataset.parquet);
  if (bytes != dataset.verified_bytes || mtime != dataset.verified_mtime)
    return arrow::Status::Invalid("canonical Parquet changed after verification: ",
                                  dataset.parquet);
  return arrow::Status::OK();
}

arrow::Result<std::map<std::string, Dataset>> LoadCatalog(const char* path) {
  simdjson::dom::parser parser;
  simdjson::dom::element document;
  auto error = parser.load(path).get(document);
  if (error) return arrow::Status::Invalid("cannot parse Flight catalog: ", simdjson::error_message(error));
  simdjson::dom::array tables;
  error = document["tables"].get(tables);
  if (error) return arrow::Status::Invalid("Flight catalog has no tables array");
  std::map<std::string, Dataset> out;
  for (auto row : tables) {
    std::string_view name, parquet, digest, frontier;
    if (row["name"].get(name) || row["parquet"].get(parquet) ||
        row["digest"].get(digest) || row["frontier"].get(frontier) ||
        name.empty() || parquet.empty()) {
      return arrow::Status::Invalid("invalid Flight catalog table");
    }
    auto [it, inserted] = out.emplace(std::string(name),
                                      Dataset{std::string(parquet), std::string(digest),
                                              std::string(frontier), 0, {}});
    if (!inserted) return arrow::Status::Invalid("duplicate Flight table: ", name);
    ARROW_RETURN_NOT_OK(VerifyDataset(&it->second));
  }
  return out;
}

arrow::Result<std::unique_ptr<parquet::arrow::FileReader>> OpenParquet(
    const Dataset& dataset, bool verify_digest = false) {
  parquet::arrow::FileReaderBuilder builder;
  if (verify_digest) {
    // The Parquet reader receives the same open file handle whose bytes were
    // verified, so a path replacement between verification and open cannot
    // redirect the stream to a different body.
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(dataset.parquet));
    ARROW_ASSIGN_OR_RAISE(auto digest, bonfyre_arrow::Sha256File(file));
    if (digest != dataset.digest)
      return arrow::Status::Invalid("canonical Parquet drift: ", dataset.parquet);
    ARROW_RETURN_NOT_OK(builder.Open(std::move(file)));
  } else {
    ARROW_RETURN_NOT_OK(builder.OpenFile(dataset.parquet));
  }
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(builder.Build(&reader));
  return reader;
}

class ParquetFlightStream final : public arrow::flight::FlightDataStream {
 public:
  ParquetFlightStream(std::unique_ptr<parquet::arrow::FileReader> file,
                     std::shared_ptr<arrow::RecordBatchReader> reader)
      : file_(std::move(file)), stream_(std::make_unique<arrow::flight::RecordBatchStream>(reader)) {}

  std::shared_ptr<arrow::Schema> schema() override { return stream_->schema(); }
  arrow::Result<arrow::flight::FlightPayload> GetSchemaPayload() override {
    return stream_->GetSchemaPayload();
  }
  arrow::Result<arrow::flight::FlightPayload> Next() override { return stream_->Next(); }
  arrow::Status Close() override { return stream_->Close(); }

 private:
  // Declaration order guarantees stream_ releases before its Parquet source.
  std::unique_ptr<parquet::arrow::FileReader> file_;
  std::unique_ptr<arrow::flight::RecordBatchStream> stream_;
};

class EstateFlight final : public arrow::flight::FlightServerBase {
 public:
  explicit EstateFlight(std::map<std::string, Dataset> tables)
      : tables_(std::move(tables)) {}

  arrow::Status ListFlights(const arrow::flight::ServerCallContext&,
                            const arrow::flight::Criteria*,
                            std::unique_ptr<arrow::flight::FlightListing>* listing) override {
    std::vector<arrow::flight::FlightInfo> flights;
    flights.reserve(tables_.size());
    for (const auto& [name, dataset] : tables_) {
      ARROW_ASSIGN_OR_RAISE(auto info, Info(name, dataset));
      flights.push_back(std::move(info));
    }
    *listing = std::make_unique<arrow::flight::SimpleFlightListing>(std::move(flights));
    return arrow::Status::OK();
  }

  arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext&,
                              const arrow::flight::FlightDescriptor& request,
                              std::unique_ptr<arrow::flight::FlightInfo>* info) override {
    const std::string name = request.type == arrow::flight::FlightDescriptor::PATH
                                 ? (request.path.empty() ? "" : request.path[0])
                                 : request.cmd;
    auto it = tables_.find(name);
    if (it == tables_.end()) return arrow::Status::KeyError("unknown dataset ", name);
    ARROW_ASSIGN_OR_RAISE(auto value, Info(it->first, it->second));
    *info = std::make_unique<arrow::flight::FlightInfo>(std::move(value));
    return arrow::Status::OK();
  }

  arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                      const arrow::flight::Ticket& ticket,
                      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
    auto it = tables_.find(ticket.ticket);
    if (it == tables_.end()) return arrow::Status::KeyError("unknown ticket ", ticket.ticket);
    ARROW_RETURN_NOT_OK(CheckDataset(it->second));
    ARROW_ASSIGN_OR_RAISE(auto file, OpenParquet(it->second, true));
    file->set_batch_size(65536);
    ARROW_ASSIGN_OR_RAISE(auto reader, file->GetRecordBatchReader());
    std::shared_ptr<arrow::RecordBatchReader> shared_reader(std::move(reader));
    *stream = std::make_unique<ParquetFlightStream>(std::move(file), shared_reader);
    return arrow::Status::OK();
  }

 private:
  arrow::Result<arrow::flight::FlightInfo> Info(const std::string& name,
                                                 const Dataset& dataset) const {
    ARROW_RETURN_NOT_OK(CheckDataset(dataset));
    ARROW_ASSIGN_OR_RAISE(auto file, OpenParquet(dataset));
    std::shared_ptr<arrow::Schema> schema;
    ARROW_RETURN_NOT_OK(file->GetSchema(&schema));
    const auto rows = file->parquet_reader()->metadata()->num_rows();
    std::error_code error;
    const auto bytes = std::filesystem::file_size(dataset.parquet, error);
    if (error) return arrow::Status::IOError("cannot stat ", dataset.parquet);
    return arrow::flight::FlightInfo::Make(
        schema, arrow::flight::FlightDescriptor::Path({name}),
        {arrow::flight::FlightEndpoint{arrow::flight::Ticket{name}, {}, std::nullopt, ""}}, rows,
        static_cast<int64_t>(bytes));
  }

  const std::map<std::string, Dataset> tables_;
};

}  // namespace

int main(int argc, char** argv) {
  const char* catalog = nullptr;
  std::string host = "127.0.0.1";
  int port = 8815;
  bool invalid = false;
  for (int i = 1; i < argc; i += 2) {
    if (i + 1 == argc) { invalid = true; break; }
    const std::string_view option(argv[i]);
    if (option == "--catalog") catalog = argv[i + 1];
    else if (option == "--host") host = argv[i + 1];
    else if (option == "--port") {
      auto value = std::string_view(argv[i + 1]);
      auto parsed = std::from_chars(value.data(), value.data() + value.size(), port);
      if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() ||
          port < 0 || port > 65535) port = -1;
    } else { invalid = true; break; }
  }
  if (invalid || !catalog || port < 0 || argc < 3 || (argc - 1) % 2) {
    std::cerr << "usage: bonfyre-arrow-flight --catalog JSON [--host HOST] [--port PORT]\n";
    return 2;
  }
  if (host != "127.0.0.1" && host != "localhost" && host != "::1") {
    std::cerr << "Flight has no remote authentication; bind loopback only\n";
    return 2;
  }
  auto catalog_result = LoadCatalog(catalog);
  if (!catalog_result.ok()) {
    std::cerr << catalog_result.status() << '\n';
    return 1;
  }
  auto location = arrow::flight::Location::ForGrpcTcp(host, port);
  if (!location.ok()) {
    std::cerr << location.status() << '\n';
    return 2;
  }
  EstateFlight server(std::move(catalog_result).ValueOrDie());
  const auto init = server.Init(arrow::flight::FlightServerOptions{location.ValueOrDie()});
  if (!init.ok()) {
    std::cerr << init << '\n';
    return 1;
  }
  std::cout << "Bonfyre Estate Flight listening on grpc://" << host << ':' << server.port()
            << std::endl;
  const auto serve = server.Serve();
  if (!serve.ok()) {
    std::cerr << serve << '\n';
    return 1;
  }
  return 0;
}
