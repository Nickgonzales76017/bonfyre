#ifndef BONFYRE_AUTHORING_MARKDOWN_H
#define BONFYRE_AUTHORING_MARKDOWN_H
#include <stdio.h>
#include <stddef.h>
/* First-party bounded Markdown renderer based on the vendored MD4C parser.
 * No arbitrary HTML, inline media, unapproved link schemes or partial output.
 * Callers retain CMS/source-rights and publication admission separately. */
int bf_authoring_markdown_render(FILE *out,const char *markdown,size_t max_html);
#endif
