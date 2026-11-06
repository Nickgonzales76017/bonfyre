#include <arrow/flight/api.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>

#include <iostream>

arrow::Status Verify(const char* parquet_path, int port, const std::string& dataset) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("127.0.0.1", port));
  ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(location));
  std::cerr << "connected; ";
  ARROW_ASSIGN_OR_RAISE(auto listing, client->ListFlights());
  std::cerr << "listed; ";
  ARROW_ASSIGN_OR_RAISE(auto listed, listing->Next());
  if (!listed || listed->descriptor().path != std::vector<std::string>{dataset}) {
    return arrow::Status::Invalid("Flight listing did not expose expected dataset");
  }
  ARROW_ASSIGN_OR_RAISE(auto end, listing->Next());
  if (end) return arrow::Status::Invalid("Flight listing has extra tables");
  ARROW_ASSIGN_OR_RAISE(
      auto info, client->GetFlightInfo(arrow::flight::FlightDescriptor::Path({dataset})));
  std::cerr << "info; ";
  if (info->total_records() != 2 || info->endpoints().size() != 1 ||
      info->endpoints()[0].ticket.ticket != dataset) {
    return arrow::Status::Invalid("Flight metadata differs");
  }
  ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(info->endpoints()[0].ticket));
  std::cerr << "stream; ";
  ARROW_ASSIGN_OR_RAISE(auto batches, stream->ToRecordBatches());
  ARROW_ASSIGN_OR_RAISE(auto actual, arrow::Table::FromRecordBatches(batches));
  parquet::arrow::FileReaderBuilder builder;
  ARROW_RETURN_NOT_OK(builder.OpenFile(parquet_path));
  ARROW_ASSIGN_OR_RAISE(auto parquet, builder.Build());
  ARROW_ASSIGN_OR_RAISE(auto expected, parquet->ReadTable());
  if (!actual->Equals(*expected, true)) {
    return arrow::Status::Invalid("Flight table or schema differs from canonical Parquet");
  }
  auto unknown = client->GetFlightInfo(arrow::flight::FlightDescriptor::Path({"absent"}));
  if (unknown.ok()) return arrow::Status::Invalid("unknown dataset was accepted");
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc != 3 && argc != 4) return 2;
  int port = 0;
  try { port = std::stoi(argv[2]); } catch (...) { return 2; }
  const auto status = Verify(argv[1], port, argc == 4 ? argv[3] : "sample");
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  std::cout << "Arrow Flight listing, metadata, stream, and values match Parquet\n";
  return 0;
}
