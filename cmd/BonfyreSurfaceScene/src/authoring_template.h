#ifndef BONFYRE_AUTHORING_TEMPLATE_H
#define BONFYRE_AUTHORING_TEMPLATE_H
#include <stdio.h>
#include <stddef.h>
/* Render one compiled trusted Mustache template with a bounded JSON document.
 * Template grammar is data-only: no unescaped variables, scripts, eval, CSS
 * injection, source discovery, account rights or publishing decisions.
 * Output is buffered until a valid complete render exists. */
int bf_authoring_template_render(FILE *out,const char *trusted_template,
                                 const char *json_data,size_t max_output);
#endif
