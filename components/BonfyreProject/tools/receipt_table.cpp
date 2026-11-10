// Receipts as a table. The pack holds each observation receipt as a JSON
// document, which is what a receipt is. A question over all of them -- which
// addresses changed, what was refused, how much was retained by host -- is a
// question about a set, and a set of 136,000 documents parsed one by one in a
// scripting language is the slow way to ask it. This reads the pack once and
// writes the fields those questions use as typed Parquet columns, so the
// analytical engine already here answers them without any JSON in between.
//
//   bonfyre-receipt-table <receipts.pack> <out.parquet>
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <simdjson.h>
#include <sqlite3.h>
#include <zstd.h>
#include <cstdio>
#include <string>
#include <vector>

static arrow::Status run(const char *pack, const char *out, long long *rows, long long *unparsed) {
  sqlite3 *db = nullptr;
  if (sqlite3_open_v2(pack, &db, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) return arrow::Status::IOError("cannot open the receipt pack");
  sqlite3_stmt *st = nullptr;
  ZSTD_DDict *dict = nullptr;
  if (sqlite3_prepare_v2(db, "SELECT value FROM meta WHERE key='dictionary'", -1, &st, nullptr) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW && sqlite3_column_bytes(st, 0) > 0)
    dict = ZSTD_createDDict(sqlite3_column_blob(st, 0), (size_t)sqlite3_column_bytes(st, 0));
  sqlite3_finalize(st);
  ZSTD_DCtx *dctx = ZSTD_createDCtx();

  arrow::StringBuilder name, twin, url, at, kind, policy, right, digest;
  arrow::Int32Builder status, robots_status;
  arrow::Int64Builder body_bytes;
  arrow::BooleanBuilder attempted, retained;
  simdjson::dom::parser parser;
  std::string text;

  if (sqlite3_prepare_v2(db, "SELECT name, bytes, z FROM receipt", -1, &st, nullptr) != SQLITE_OK) return arrow::Status::IOError("the pack has no receipts table");
  while (sqlite3_step(st) == SQLITE_ROW) {
    size_t bytes = (size_t)sqlite3_column_int64(st, 1);
    text.resize(bytes);
    size_t got = dict ? ZSTD_decompress_usingDDict(dctx, text.data(), bytes, sqlite3_column_blob(st, 2), (size_t)sqlite3_column_bytes(st, 2), dict)
                      : ZSTD_decompressDCtx(dctx, text.data(), bytes, sqlite3_column_blob(st, 2), (size_t)sqlite3_column_bytes(st, 2));
    if (ZSTD_isError(got) || got != bytes) return arrow::Status::IOError("a packed receipt is damaged");
    // A receipt file may hold several lines; the first is the observation the owner reads.
    size_t line = text.find('\n');
    simdjson::dom::element doc;
    if (parser.parse(text.data(), line == std::string::npos ? bytes : line).get(doc)) { (*unparsed)++; continue; }
    auto str = [&](const char *key, arrow::StringBuilder &to) -> arrow::Status {
      std::string_view v;
      return doc[key].get(v) ? to.AppendNull() : to.Append(v.data(), (int32_t)v.size());
    };
    auto num32 = [&](const char *key, arrow::Int32Builder &to) -> arrow::Status { int64_t v; return doc[key].get(v) ? to.AppendNull() : to.Append((int32_t)v); };
    std::string_view source;
    if (doc["source_url"].get(source) || source.empty()) { (*unparsed)++; continue; }
    ARROW_RETURN_NOT_OK(name.Append((const char *)sqlite3_column_text(st, 0)));
    ARROW_RETURN_NOT_OK(str("twin_id", twin));
    ARROW_RETURN_NOT_OK(url.Append(source.data(), (int32_t)source.size()));
    ARROW_RETURN_NOT_OK(str("observed_at", at));
    ARROW_RETURN_NOT_OK(str("observation_kind", kind));
    ARROW_RETURN_NOT_OK(str("policy_decision", policy));
    ARROW_RETURN_NOT_OK(str("observe_right", right));
    std::string_view sha; bool has_sha = !doc["content_sha256"].get(sha) && !sha.empty() && sha != "null";
    ARROW_RETURN_NOT_OK(has_sha ? digest.Append(sha.data(), (int32_t)sha.size()) : digest.AppendNull());
    ARROW_RETURN_NOT_OK(num32("http_status", status));
    ARROW_RETURN_NOT_OK(num32("robots_http_status", robots_status));
    int64_t size; ARROW_RETURN_NOT_OK(doc["body_bytes"].get(size) ? body_bytes.AppendNull() : body_bytes.Append(size));
    bool tried; ARROW_RETURN_NOT_OK(doc["fetch_attempted"].get(tried) ? attempted.AppendNull() : attempted.Append(tried));
    std::string_view k; bool observed = !doc["observation_kind"].get(k) && k == "source_observed";
    // The owner's own rule for "this observation holds bytes".
    ARROW_RETURN_NOT_OK(retained.Append(observed && has_sha));
    (*rows)++;
  }
  sqlite3_finalize(st); sqlite3_close(db); ZSTD_freeDCtx(dctx); if (dict) ZSTD_freeDDict(dict);

  auto schema = arrow::schema({ arrow::field("receipt", arrow::utf8()), arrow::field("twin_id", arrow::utf8()), arrow::field("source_url", arrow::utf8()),
    arrow::field("observed_at", arrow::utf8()), arrow::field("observation_kind", arrow::utf8()), arrow::field("policy_decision", arrow::utf8()),
    arrow::field("observe_right", arrow::utf8()), arrow::field("content_sha256", arrow::utf8()), arrow::field("http_status", arrow::int32()),
    arrow::field("robots_http_status", arrow::int32()), arrow::field("body_bytes", arrow::int64()), arrow::field("fetch_attempted", arrow::boolean()),
    arrow::field("retained", arrow::boolean()) });
  std::vector<std::shared_ptr<arrow::Array>> columns(13);
  ARROW_RETURN_NOT_OK(name.Finish(&columns[0])); ARROW_RETURN_NOT_OK(twin.Finish(&columns[1])); ARROW_RETURN_NOT_OK(url.Finish(&columns[2]));
  ARROW_RETURN_NOT_OK(at.Finish(&columns[3])); ARROW_RETURN_NOT_OK(kind.Finish(&columns[4])); ARROW_RETURN_NOT_OK(policy.Finish(&columns[5]));
  ARROW_RETURN_NOT_OK(right.Finish(&columns[6])); ARROW_RETURN_NOT_OK(digest.Finish(&columns[7])); ARROW_RETURN_NOT_OK(status.Finish(&columns[8]));
  ARROW_RETURN_NOT_OK(robots_status.Finish(&columns[9])); ARROW_RETURN_NOT_OK(body_bytes.Finish(&columns[10])); ARROW_RETURN_NOT_OK(attempted.Finish(&columns[11]));
  ARROW_RETURN_NOT_OK(retained.Finish(&columns[12]));
  auto table = arrow::Table::Make(schema, columns);
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::FileOutputStream::Open(out));
  auto props = parquet::WriterProperties::Builder().compression(arrow::Compression::ZSTD)->build();
  return parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 64 * 1024, props);
}

int main(int argc, char **argv) {
  if (argc != 3) { std::fprintf(stderr, "usage: bonfyre-receipt-table <receipts.pack> <out.parquet>\n"); return 2; }
  long long rows = 0, unparsed = 0;
  auto status = run(argv[1], argv[2], &rows, &unparsed);
  if (!status.ok()) { std::fprintf(stderr, "%s\n", status.ToString().c_str()); return 1; }
  std::printf("{\"schema\":\"bonfyre-foreign-receipt-table.v1\",\"observations\":%lld,\"receipts_without_an_observation\":%lld,"
              "\"cannot_infer\":\"a column in this table -> more than the receipt said; it is the receipt's own fields, typed\"}\n", rows, unparsed);
  return 0;
}
