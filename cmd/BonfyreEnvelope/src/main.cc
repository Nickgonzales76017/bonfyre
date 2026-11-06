// bonfyre-envelope: the typed operation messages, at the command line.
//
//   bonfyre-envelope encode <Message> < json   > binary
//   bonfyre-envelope decode <Message> < binary > json
//   bonfyre-envelope check  <Message> < json            (exit 0 if it is that message)
//   bonfyre-envelope digest <Message> < json|binary     (canonical identity, hex)
//
// JSON is read strictly: a field the message does not have, or a value of the
// wrong type, is refused. That is the point of lowering a JSON boundary once.
#include "aurekai/operation.pb.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>
#include <google/protobuf/util/json_util.h>
#include <CommonCrypto/CommonDigest.h>
#include <simdjson.h>
#include <algorithm>
#include <cstdio>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

using google::protobuf::FieldDescriptor;
using google::protobuf::Message;

static std::unique_ptr<Message> make(const std::string &name) {
  const auto *descriptor = google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName("aurekai.operation.v1." + name);
  if (!descriptor) return nullptr;
  return std::unique_ptr<Message>(google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor)->New());
}

// Identity: every field in field-number order, each as "<number>:<type>:<length>:<bytes>",
// defaults included, so two serialisations of one message have one digest and
// the digest does not depend on any serialiser's choices.
static std::string canonical(const Message &message) {
  std::string out = std::string(message.GetDescriptor()->full_name()) + "\n";
  const auto *reflection = message.GetReflection();
  std::vector<const FieldDescriptor *> fields;
  for (int i = 0; i < message.GetDescriptor()->field_count(); i++) fields.push_back(message.GetDescriptor()->field(i));
  std::sort(fields.begin(), fields.end(), [](auto *a, auto *b) { return a->number() < b->number(); });
  for (const auto *field : fields) {
    std::string value, type;
    switch (field->cpp_type()) {
      case FieldDescriptor::CPPTYPE_STRING: value = reflection->GetString(message, field); type = field->type() == FieldDescriptor::TYPE_BYTES ? "bytes" : "string"; break;
      case FieldDescriptor::CPPTYPE_BOOL: value = reflection->GetBool(message, field) ? "1" : "0"; type = "bool"; break;
      case FieldDescriptor::CPPTYPE_UINT64: value = std::to_string(reflection->GetUInt64(message, field)); type = "uint64"; break;
      default: value = "?"; type = "unsupported"; break;
    }
    out += std::to_string(field->number()) + ":" + type + ":" + std::to_string(value.size()) + ":" + value + "\n";
  }
  return out;
}

static std::string sha256_hex(const std::string &bytes) {
  unsigned char digest[CC_SHA256_DIGEST_LENGTH];
  CC_SHA256(bytes.data(), static_cast<CC_LONG>(bytes.size()), digest);
  char hex[2 * CC_SHA256_DIGEST_LENGTH + 1];
  for (int i = 0; i < CC_SHA256_DIGEST_LENGTH; i++) std::snprintf(hex + 2 * i, 3, "%02x", digest[i]);
  return hex;
}

int main(int argc, char **argv) {
  if (argc != 3) { std::fprintf(stderr, "usage: bonfyre-envelope encode|decode|check|digest <Message>\n"); return 2; }
  const std::string verb = argv[1];
  auto message = make(argv[2]);
  if (!message) { std::fprintf(stderr, "no message named %s\n", argv[2]); return 2; }
  std::string input((std::istreambuf_iterator<char>(std::cin)), std::istreambuf_iterator<char>());
  auto from_json = [&]() {
    // The library's JSON reader is forgiving in ways a consent flag cannot
    // afford: it reads the text "yes" as true. Each value's JSON type is
    // therefore checked against its field before the library sees it.
    {
      simdjson::dom::parser parser;
      simdjson::dom::object object;
      if (parser.parse(input).get(object)) { std::fprintf(stderr, "not a %s: not a JSON object\n", argv[2]); return false; }
      for (auto [key, value] : object) {
        const auto *field = message->GetDescriptor()->FindFieldByName(std::string(key));
        if (!field) { std::fprintf(stderr, "not a %s: no field named %.*s\n", argv[2], static_cast<int>(key.size()), key.data()); return false; }
        bool right = false;
        switch (field->cpp_type()) {
          case FieldDescriptor::CPPTYPE_BOOL: right = value.is_bool(); break;
          case FieldDescriptor::CPPTYPE_UINT64: right = value.is_uint64(); break;
          case FieldDescriptor::CPPTYPE_STRING: right = value.is_string(); break;
          default: right = false; break;
        }
        if (!right) { std::fprintf(stderr, "not a %s: %s has the wrong kind of value\n", argv[2], std::string(field->name()).c_str()); return false; }
      }
    }
    google::protobuf::util::JsonParseOptions strict;
    strict.ignore_unknown_fields = false;
    auto status = google::protobuf::util::JsonStringToMessage(input, message.get(), strict);
    if (!status.ok()) { std::fprintf(stderr, "not a %s: %s\n", argv[2], std::string(status.message()).c_str()); return false; }
    return true;
  };
  if (verb == "encode" || verb == "check") {
    if (!from_json()) return 1;
    if (verb == "encode") {
      std::string bytes;
      if (!message->SerializeToString(&bytes)) return 1;
      std::cout.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    return 0;
  }
  if (verb == "decode") {
    if (!message->ParseFromString(input)) { std::fprintf(stderr, "not a serialised %s\n", argv[2]); return 1; }
    google::protobuf::util::JsonPrintOptions options;
    options.always_print_fields_with_no_presence = true;
    options.preserve_proto_field_names = true;
    std::string json;
    if (!google::protobuf::util::MessageToJsonString(*message, &json, options).ok()) return 1;
    std::cout << json << "\n";
    return 0;
  }
  if (verb == "digest") {
    // Either form is accepted; the identity is the same.
    const bool looks_json = !input.empty() && (input[0] == '{' || input[0] == ' ' || input[0] == '\n');
    if (looks_json ? !from_json() : !message->ParseFromString(input)) { if (!looks_json) std::fprintf(stderr, "not a serialised %s\n", argv[2]); return 1; }
    std::cout << sha256_hex(canonical(*message)) << "\n";
    return 0;
  }
  std::fprintf(stderr, "usage: bonfyre-envelope encode|decode|check|digest <Message>\n");
  return 2;
}
