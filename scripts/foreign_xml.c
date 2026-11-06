// Native XML carrier decoder. Callers verify observation receipts before
// passing the exact verified bytes through stdin.
#include <json-c/json.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *ATOM = "http://www.w3.org/2005/Atom";
static const char *DC = "http://purl.org/dc/elements/1.1/";
static const char *SM = "http://www.sitemaps.org/schemas/sitemap/0.9";
static const char *NC = "http://api.namecheap.com/xml.response";

static int is_node(const xmlNode *node, const char *name, const char *space) {
    if (node->type != XML_ELEMENT_NODE || xmlStrcmp(node->name, BAD_CAST name)) return 0;
    if (!space) return !node->ns || !node->ns->href;
    return node->ns && node->ns->href && !xmlStrcmp(node->ns->href, BAD_CAST space);
}

static xmlNode *child(xmlNode *parent, const char *name, const char *space) {
    for (xmlNode *node = parent->children; node; node = node->next)
        if (is_node(node, name, space)) return node;
    return NULL;
}

static char *node_text(xmlNode *node) {
    if (!node) return NULL;
    xmlChar *raw = xmlNodeGetContent(node);
    if (!raw) return NULL;
    char *start = (char *)raw;
    while (*start == ' ' || *start == '\n' || *start == '\t' || *start == '\r') start++;
    char *end = start + strlen(start);
    while (end > start && (end[-1] == ' ' || end[-1] == '\n' || end[-1] == '\t' || end[-1] == '\r')) --end;
    char *out = strndup(start, (size_t)(end - start));
    xmlFree(raw);
    return out;
}

static char *field(xmlNode *parent, const char *name, const char *space, int href) {
    xmlNode *node = child(parent, name, space);
    char *text = node_text(node);
    if (text && *text) return text;
    free(text);
    if (href && node) {
        xmlChar *raw = xmlGetProp(node, BAD_CAST "href");
        if (raw) {
            char *value = strdup((char *)raw);
            xmlFree(raw);
            return value;
        }
    }
    return strdup("");
}

static void put(json_object *object, const char *key, char *value) {
    json_object_object_add(object, key, json_object_new_string(value ? value : ""));
    free(value);
}

static json_object *entry(xmlNode *node, int atom) {
    json_object *item = json_object_new_object();
    char *id = field(node, atom ? "id" : "guid", atom ? ATOM : NULL, 0);
    char *url = field(node, "link", atom ? ATOM : NULL, atom);
    if (!id || !*id) { free(id); id = strdup(url ? url : ""); }
    put(item, "declared_id", id);
    put(item, "title", field(node, "title", atom ? ATOM : NULL, 0));
    put(item, "url", url);
    char *time = field(node, atom ? "updated" : "pubDate", atom ? ATOM : NULL, 0);
    if (!time || !*time) {
        free(time);
        time = field(node, atom ? "published" : "date", atom ? ATOM : DC, 0);
    }
    put(item, "declared_time", time);
    char *summary = field(node, atom ? "summary" : "description", atom ? ATOM : NULL, 0);
    if (atom && (!summary || !*summary)) {
        free(summary);
        summary = field(node, "content", ATOM, 0);
    }
    put(item, "summary", summary);
    char *author = NULL;
    if (atom) {
        xmlNode *person = child(node, "author", ATOM);
        author = person ? field(person, "name", ATOM, 0) : strdup("");
    } else {
        author = field(node, "creator", DC, 0);
        if (!author || !*author) {
            free(author);
            author = field(node, "author", NULL, 0);
        }
    }
    put(item, "author", author);
    return item;
}

static void rss_items(xmlNode *node, json_object *items) {
    for (; node; node = node->next) {
        if (is_node(node, "item", NULL)) json_object_array_add(items, entry(node, 0));
        else if (node->children) rss_items(node->children, items);
    }
}

static int sitemap(xmlNode *root) {
    int index = root && is_node(root, "sitemapindex", SM);
    if (!index && (!root || !is_node(root, "urlset", SM))) return 3;
    json_object *out = json_object_new_object();
    json_object *entries = json_object_new_array();
    json_object_object_add(out, "kind", json_object_new_string(index ? "sitemapindex" : "urlset"));
    for (xmlNode *node = root->children; node; node = node->next) {
        if (!is_node(node, index ? "sitemap" : "url", SM)) continue;
        char *loc = field(node, "loc", SM, 0);
        if (!loc || !*loc) { free(loc); continue; }
        json_object *entry = json_object_new_object();
        put(entry, "loc", loc);
        put(entry, "lastmod", field(node, "lastmod", SM, 0));
        json_object_array_add(entries, entry);
    }
    json_object_object_add(out, "entries", entries);
    puts(json_object_to_json_string_ext(out, JSON_C_TO_STRING_PLAIN));
    json_object_put(out);
    return 0;
}

static int prop_is(xmlNode *node, const char *name, const char *expected) {
    xmlChar *value = xmlGetProp(node, BAD_CAST name);
    int equal = value && !xmlStrcmp(value, BAD_CAST expected);
    xmlFree(value);
    return equal;
}

static xmlNode *only_child(xmlNode *parent, const char *name, const char *space) {
    xmlNode *found = NULL;
    for (xmlNode *node = parent->children; node; node = node->next) {
        if (!is_node(node, name, space)) continue;
        if (found) return NULL;
        found = node;
    }
    return found;
}

static void put_prop(json_object *object, xmlNode *node, const char *name) {
    xmlChar *value = xmlGetProp(node, BAD_CAST name);
    json_object_object_add(object, name, json_object_new_string(value ? (char *)value : ""));
    xmlFree(value);
}

// Strict registrar response boundary; XML entity decoding is libxml's job,
// never a regex over tags. No DTDs, ambiguous result nodes, or error envelopes.
static int namecheap(xmlDoc *doc, xmlNode *root, int hosts) {
    if (doc->intSubset || doc->extSubset || !root || !is_node(root, "ApiResponse", NC) ||
        !prop_is(root, "Status", "OK")) return 3;
    xmlNode *errors = only_child(root, "Errors", NC);
    xmlNode *command = only_child(root, "CommandResponse", NC);
    xmlNode *requested = only_child(root, "RequestedCommand", NC);
    const char *method = hosts ? "namecheap.domains.dns.getHosts" : "namecheap.domains.dns.setCustom";
    if (!errors || !command || !requested || !prop_is(command, "Type", method)) return 3;
    for (xmlNode *node = errors->children; node; node = node->next)
        if (node->type == XML_ELEMENT_NODE) return 3;
    char *request = node_text(requested);
    int matches = request && !strcmp(request, method);
    free(request);
    if (!matches) return 3;
    xmlNode *result = only_child(command, hosts ? "DomainDNSGetHostsResult" : "DomainDNSSetCustomResult", NC);
    if (!result || (hosts ? !prop_is(result, "IsUsingOurDNS", "true") : !prop_is(result, "Updated", "true"))) return 3;
    json_object *out = json_object_new_object();
    put_prop(out, result, "Domain");
    if (hosts) {
        json_object *rows = json_object_new_array();
        json_object_object_add(out, "hosts", rows);
        for (xmlNode *node = result->children; node; node = node->next) {
            if (node->type != XML_ELEMENT_NODE) continue;
            if (!is_node(node, "Host", NC) && !is_node(node, "host", NC)) { json_object_put(out); return 3; }
            json_object *row = json_object_new_object();
            const char *fields[] = {"Name", "Type", "Address", "TTL", "MXPref"};
            for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); i++) put_prop(row, node, fields[i]);
            json_object_array_add(rows, row);
        }
    } else json_object_object_add(out, "updated", json_object_new_boolean(1));
    puts(json_object_to_json_string_ext(out, JSON_C_TO_STRING_PLAIN));
    json_object_put(out);
    return 0;
}

int main(int argc, char **argv) {
    int as_sitemap = argc == 2 && !strcmp(argv[1], "--sitemap");
    int nc_hosts = argc == 2 && !strcmp(argv[1], "--namecheap-hosts");
    int nc_updated = argc == 2 && !strcmp(argv[1], "--namecheap-updated");
    if (argc > 1 && !as_sitemap && !nc_hosts && !nc_updated) return 2;
    size_t capacity = 65536, used = 0;
    char *buffer = malloc(capacity);
    if (!buffer) return 2;
    for (;;) {
        if (used == capacity) {
            if ((nc_hosts || nc_updated) && capacity >= 4 * 1024 * 1024) { free(buffer); return 2; }
            if (capacity > INT_MAX / 2) { free(buffer); return 2; }
            capacity *= 2;
            char *grown = realloc(buffer, capacity);
            if (!grown) { free(buffer); return 2; }
            buffer = grown;
        }
        size_t got = fread(buffer + used, 1, capacity - used, stdin);
        used += got;
        if (!got) break;
    }
    if (ferror(stdin) || used > INT_MAX) { free(buffer); return 2; }
    xmlDoc *doc = xmlReadMemory(buffer, (int)used, "feed.xml", NULL,
                                XML_PARSE_NONET | XML_PARSE_NOERROR | XML_PARSE_NOWARNING);
    free(buffer);
    if (!doc) return 3;
    xmlNode *root = xmlDocGetRootElement(doc);
    if (nc_hosts || nc_updated) {
        int result = namecheap(doc, root, nc_hosts);
        xmlFreeDoc(doc);
        return result;
    }
    if (as_sitemap) {
        int result = sitemap(root);
        xmlFreeDoc(doc);
        return result;
    }
    int atom = root && is_node(root, "feed", ATOM);
    if (!atom && (!root || !is_node(root, "rss", NULL))) {
        xmlFreeDoc(doc);
        return 3;
    }
    json_object *out = json_object_new_object();
    json_object *items = json_object_new_array();
    json_object_object_add(out, "carrier", json_object_new_string(atom ? "atom" : "rss"));
    if (atom) {
        for (xmlNode *node = root->children; node; node = node->next)
            if (is_node(node, "entry", ATOM)) json_object_array_add(items, entry(node, 1));
    } else if (root) rss_items(root, items);
    json_object_object_add(out, "items", items);
    puts(json_object_to_json_string_ext(out, JSON_C_TO_STRING_PLAIN));
    json_object_put(out);
    xmlFreeDoc(doc);
    return 0;
}
