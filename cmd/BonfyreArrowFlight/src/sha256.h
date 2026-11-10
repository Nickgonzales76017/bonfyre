#ifndef BONFYRE_ARROW_SHA256_H
#define BONFYRE_ARROW_SHA256_H

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <CommonCrypto/CommonDigest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <string>

namespace bonfyre_arrow {

// Hash the bytes on an already-open handle. Callers pass this same handle to
// Parquet, avoiding a pathname swap between digest verification and opening.
inline arrow::Result<std::string> Sha256File(
    const std::shared_ptr<arrow::io::RandomAccessFile>& file) {
  ARROW_ASSIGN_OR_RAISE(const auto size, file->GetSize());
  CC_SHA256_CTX hash;
  CC_SHA256_Init(&hash);
  for (int64_t offset = 0; offset < size;) {
    ARROW_ASSIGN_OR_RAISE(auto block,
                          file->ReadAt(offset, std::min<int64_t>(65536, size - offset)));
    if (block->size() == 0) return arrow::Status::IOError("short Arrow body read");
    CC_SHA256_Update(&hash, block->data(), static_cast<CC_LONG>(block->size()));
    offset += block->size();
  }
  ARROW_ASSIGN_OR_RAISE(const auto after_size, file->GetSize());
  if (after_size != size) return arrow::Status::IOError("Arrow body size changed while hashing");
  std::array<unsigned char, CC_SHA256_DIGEST_LENGTH> bytes;
  CC_SHA256_Final(bytes.data(), &hash);
  constexpr char hex[] = "0123456789abcdef";
  std::string digest(bytes.size() * 2, '\0');
  for (size_t i = 0; i < bytes.size(); ++i) {
    digest[2 * i] = hex[bytes[i] >> 4];
    digest[2 * i + 1] = hex[bytes[i] & 15];
  }
  return digest;
}

}  // namespace bonfyre_arrow

#endif
