#!/bin/sh
# schemas/control/numeric-presentation.cue -> the C header and the browser table.
# One declaration, two generated readers; neither is edited by hand.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH='' cd -- "$here/../.." && pwd)
cue_file="$repo/schemas/control/numeric-presentation.cue"
header="$here/generated/native_numeric_presentation.h"
table="$repo/components/BonfyreServiceSite/authoring/numeric-presentation.vocabulary.json"
cue vet -c "$cue_file"
vocab=$(cue export "$cue_file" -e numeric_presentation --out json)
mkdir -p "$here/generated"
tmp="$header.tmp.$$"
{
  echo "/* Generated from schemas/control/numeric-presentation.cue by tools/compile_numeric_vocabulary.sh. Do not edit. */"
  echo "#ifndef BONFYRE_NATIVE_NUMERIC_PRESENTATION_H"
  echo "#define BONFYRE_NATIVE_NUMERIC_PRESENTATION_H"
  printf '%s\n' "$vocab" | jq -r '
    def cstr: "\"" + (. | gsub("\\\\"; "\\\\\\\\") | gsub("\""; "\\\"")) + "\"";
    def up: ascii_upcase | gsub("-"; "_");
    "#define BF_NP_SCHEMA \(.schema | cstr)",
    "#define BF_NP_VIEWPORTS \(.viewports | length)",
    "#define BF_NP_PROPS \(.properties | length)",
    "#define BF_NP_SELECT_MAX \(.selection.max_candidates)",
    "#define BF_NP_MAX_FAMILY \(.max_family)",
    "enum { " + ([.viewports | to_entries[] | "BF_NP_VP_\(.value.name | up) = \(.key)"] | join(", ")) + " };",
    "enum { " + ([.properties | to_entries[] | "BF_NP_\(.value.name | up) = \(.key)"] | join(", ")) + " };",
    "typedef struct { const char *name; int range; int min, max; const char *const *values; int nvalues; } BfNpProp;",
    "__attribute__((unused)) static const char *const bf_np_viewport[] = {" + ([.viewports[] | .name | cstr] | join(", ")) + "};",
    "__attribute__((unused)) static const char *const bf_np_media[] = {" + ([.viewports[] | .media | cstr] | join(", ")) + "};",
    (.properties[] | select(.kind == "enum") |
      "__attribute__((unused)) static const char *const bf_np_values_\(.name | up | ascii_downcase)[] = {" + ([.values[] | cstr] | join(", ")) + "};"),
    "__attribute__((unused)) static const BfNpProp bf_np_props[] = {",
    (.properties[] | if .kind == "enum"
       then "    {\(.name | cstr), 0, 0, 0, bf_np_values_\(.name | up | ascii_downcase), \(.values | length)},"
       else "    {\(.name | cstr), 1, \(.min), \(.max), 0, 0}," end),
    "};",
    "__attribute__((unused)) static const char *const bf_np_select_fields[] = {" + ([.selection.fields[] | cstr] | join(", ")) + "};",
    "#define BF_NP_SELECT_FIELDS \(.selection.fields | length)",
    "__attribute__((unused)) static const char *const bf_np_select_ops[] = {" + ([.selection.ops[] | cstr] | join(", ")) + "};",
    "#define BF_NP_SELECT_OPS \(.selection.ops | length)",
    "#define BF_NP_STORE_DDL " + ([.store[] | . + ";" | cstr] | join(" "))'
  echo "#endif"
} > "$tmp"
mv "$tmp" "$header"
printf '%s\n' "$vocab" | jq -c '{schema, devices: [.viewports[].name], media: [.viewports[].media],
  props: [.properties[].name],
  enums: ([.properties[] | select(.kind == "enum") | {(.name): .values}] | add),
  ranges: ([.properties[] | select(.kind == "range") | {(.name): [.min, .max]}] | add)}' > "$table.tmp.$$"
mv "$table.tmp.$$" "$table"
