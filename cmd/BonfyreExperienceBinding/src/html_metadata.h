#ifndef BONFYRE_EXPERIENCE_BINDING_HTML_METADATA_H
#define BONFYRE_EXPERIENCE_BINDING_HTML_METADATA_H

typedef struct {
    char *name;
    char *authority;
    char *description;
    char *source_url;
    char *updated_at;
} BfHtmlMetadata;

int bf_html_metadata_parse_file(const char *path, const char *fallback_url, BfHtmlMetadata *out);
void bf_html_metadata_free(BfHtmlMetadata *metadata);

#endif
