#ifndef BONFYRE_AUTHORING_EDITORIAL_H
#define BONFYRE_AUTHORING_EDITORIAL_H
#include <stdio.h>
#include <stddef.h>
/* Render Editor.js's eight typed tools with first-party generated Mustach
 * templates, bounded output and fully escaped user content. No raw HTML
 * passthrough, publication, source rights or external effect ownership. */
int bf_authoring_editorial_render(FILE *out,const char *editorial,size_t maximum);
#endif
