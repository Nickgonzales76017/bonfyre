/* Generated from schemas/control/numeric-presentation.cue by tools/compile_numeric_vocabulary.sh. Do not edit. */
#ifndef BONFYRE_NATIVE_NUMERIC_PRESENTATION_H
#define BONFYRE_NATIVE_NUMERIC_PRESENTATION_H
#define BF_NP_SCHEMA "bonfyre.numeric-presentation.v1"
#define BF_NP_VIEWPORTS 3
#define BF_NP_PROPS 11
#define BF_NP_SELECT_MAX 12
#define BF_NP_MAX_FAMILY 512
enum { BF_NP_VP_DESKTOP = 0, BF_NP_VP_TABLET = 1, BF_NP_VP_PHONE = 2 };
enum { BF_NP_ALIGN = 0, BF_NP_BACKGROUND = 1, BF_NP_COLUMNS = 2, BF_NP_GAP = 3, BF_NP_JUSTIFY = 4, BF_NP_LAYOUT = 5, BF_NP_PADDING = 6, BF_NP_RADIUS = 7, BF_NP_SPAN = 8, BF_NP_TYPOGRAPHY = 9, BF_NP_WIDTH = 10 };
typedef struct { const char *name; int range; int min, max; const char *const *values; int nvalues; } BfNpProp;
__attribute__((unused)) static const char *const bf_np_viewport[] = {"desktop", "tablet", "phone"};
__attribute__((unused)) static const char *const bf_np_media[] = {"(min-width:1025px)", "(min-width:761px) and (max-width:1024px)", "(max-width:760px)"};
__attribute__((unused)) static const char *const bf_np_values_align[] = {"start", "center", "end", "stretch", "between"};
__attribute__((unused)) static const char *const bf_np_values_background[] = {"default", "surface", "panel", "muted", "accent", "reversed", "dark", "light"};
__attribute__((unused)) static const char *const bf_np_values_justify[] = {"start", "center", "end", "stretch", "between"};
__attribute__((unused)) static const char *const bf_np_values_layout[] = {"stack", "grid", "columns", "carousel", "masonry", "split", "full-bleed", "feature"};
__attribute__((unused)) static const char *const bf_np_values_typography[] = {"body", "display", "serif", "sans", "mono", "editorial"};
__attribute__((unused)) static const char *const bf_np_values_width[] = {"auto", "narrow", "medium", "wide", "full"};
__attribute__((unused)) static const BfNpProp bf_np_props[] = {
    {"align", 0, 0, 0, bf_np_values_align, 5},
    {"background", 0, 0, 0, bf_np_values_background, 8},
    {"columns", 1, 1, 12, 0, 0},
    {"gap", 1, 0, 64, 0, 0},
    {"justify", 0, 0, 0, bf_np_values_justify, 5},
    {"layout", 0, 0, 0, bf_np_values_layout, 8},
    {"padding", 1, 0, 64, 0, 0},
    {"radius", 1, 0, 64, 0, 0},
    {"span", 1, 1, 12, 0, 0},
    {"typography", 0, 0, 0, bf_np_values_typography, 6},
    {"width", 0, 0, 0, bf_np_values_width, 5},
};
__attribute__((unused)) static const char *const bf_np_select_fields[] = {"gap", "padding", "radius", "columns", "span"};
#define BF_NP_SELECT_FIELDS 5
__attribute__((unused)) static const char *const bf_np_select_ops[] = {"<", "<=", ">", ">=", "=="};
#define BF_NP_SELECT_OPS 5
#define BF_NP_STORE_DDL "CREATE TABLE IF NOT EXISTS _scene_numeric_body (digest TEXT PRIMARY KEY CHECK(length(digest)=64), body TEXT NOT NULL);" "CREATE TABLE IF NOT EXISTS _scene_numeric_ref (namespace TEXT NOT NULL, route TEXT NOT NULL, coordinate TEXT NOT NULL,source_revision TEXT NOT NULL, digest TEXT NOT NULL REFERENCES _scene_numeric_body(digest),PRIMARY KEY(namespace,route,coordinate));"
#endif
