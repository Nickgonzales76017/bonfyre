#include "bf_text.h"
#include <re2/re2.h>
#include <utf8proc.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

long bf_text_identity_key(const char *text, char *out, size_t cap) {
  if (!text || !out || !cap) return -1;
  utf8proc_uint8_t *mapped = nullptr;
  // Compose, fold case, unify compatibility forms, drop what is invisible.
  utf8proc_ssize_t got = utf8proc_map(reinterpret_cast<const utf8proc_uint8_t *>(text), 0, &mapped,
    static_cast<utf8proc_option_t>(UTF8PROC_NULLTERM | UTF8PROC_STABLE | UTF8PROC_COMPOSE | UTF8PROC_COMPAT |
                                   UTF8PROC_CASEFOLD | UTF8PROC_IGNORE | UTF8PROC_STRIPCC));
  if (got < 0) return -1;
  // White space of any kind becomes one space; none at either end.
  size_t used = 0;
  bool pending = false, any = false;
  for (utf8proc_ssize_t at = 0; at < got;) {
    utf8proc_int32_t point;
    utf8proc_ssize_t step = utf8proc_iterate(mapped + at, got - at, &point);
    if (step <= 0) { free(mapped); return -1; }
    const utf8proc_category_t category = utf8proc_category(point);
    const bool space = category == UTF8PROC_CATEGORY_ZS || category == UTF8PROC_CATEGORY_ZL || category == UTF8PROC_CATEGORY_ZP || point == ' ';
    if (space) { pending = any; at += step; continue; }
    if (used + static_cast<size_t>(step) + (pending ? 1 : 0) + 1 > cap) { free(mapped); return -1; }
    if (pending) { out[used++] = ' '; pending = false; }
    std::memcpy(out + used, mapped + at, static_cast<size_t>(step));
    used += static_cast<size_t>(step); at += step; any = true;
  }
  out[used] = '\0';
  free(mapped);
  return static_cast<long>(used);
}

struct bf_match { RE2 *re; };

bf_match_t *bf_match_compile(const char *pattern, char *why, size_t why_cap) {
  RE2::Options options;
  options.set_log_errors(false);
  options.set_max_mem(8 << 20);          // a bound on what one pattern may cost to hold
  RE2 *re = new RE2(pattern ? pattern : "", options);
  if (!re->ok()) {
    if (why && why_cap) std::snprintf(why, why_cap, "%s", re->error().c_str());
    delete re;
    return nullptr;
  }
  if (why && why_cap) why[0] = '\0';
  return new bf_match{re};
}
int bf_match_search(const bf_match_t *match, const char *text, size_t len) {
  return match && RE2::PartialMatch(re2::StringPiece(text, len), *match->re) ? 1 : 0;
}
int bf_match_whole(const bf_match_t *match, const char *text, size_t len) {
  return match && RE2::FullMatch(re2::StringPiece(text, len), *match->re) ? 1 : 0;
}
void bf_match_free(bf_match_t *match) { if (match) { delete match->re; delete match; } }
