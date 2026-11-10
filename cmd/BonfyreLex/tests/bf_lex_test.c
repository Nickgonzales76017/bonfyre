/* The shared extractors, exercised on the exact inputs that broke their
 * hand-rolled copies. Each case names the failure it is standing in for. */
#include "../src/bf_lex.h"
#include <assert.h>
#include <stdio.h>

static int checks = 0;
#define CHECK(cond, what) do { checks++; if (!(cond)) { \
    printf("FAIL %s (%s:%d)\n", what, __FILE__, __LINE__); return 1; } } while (0)

int main(void) {
    char out[128];
    const char *end;

    /* An ETag is `W/"abc"`, which in JSON is `W/\"abc\"`. The copy that stopped
     * at the first quote returned `W/\` and every conditional request failed in
     * a way that looked like a source with nothing new. */
    CHECK(bf_lex_json_string("{\"etag\":\"W/\\\"a1b2\\\"\"}", "etag", out, sizeof out), "etag found");
    CHECK(!strcmp(out, "W/\"a1b2\""), "etag unescaped whole");

    /* A 64-character digest into a 65-byte buffer. Testing capacity before the
     * closing quote reported this absent. */
    {
        char digest[65];
        const char *doc = "{\"digest\":\"" "0123456789abcdef0123456789abcdef"
                          "0123456789abcdef0123456789abcdef" "\"}";
        CHECK(bf_lex_json_string(doc, "digest", digest, sizeof digest), "exact-fit digest found");
        CHECK(strlen(digest) == 64, "exact-fit digest whole");
    }

    /* One byte too small is absence, not a prefix used as if it were whole. */
    {
        char small[8];
        CHECK(!bf_lex_json_string("{\"k\":\"abcdefghij\"}", "k", small, sizeof small), "overflow is absent");
        CHECK(small[0] == '\0', "overflow leaves nothing");
    }

    /* A key must be a key. The copy that searched for `key` rather than `"key":`
     * matched inside other values. */
    CHECK(!bf_lex_json_string("{\"other\":\"contains id here\"}", "id", out, sizeof out), "no match inside a value");

    /* Present and empty is not absent. */
    CHECK(bf_lex_json_string("{\"k\":\"\"}", "k", out, sizeof out), "empty string present");
    CHECK(out[0] == '\0', "empty string empty");

    /* \u decoding: BMP, and a surrogate pair. The copy that wrote `?` and the
     * copy that took the high half alone both lost the character. */
    CHECK(bf_lex_json_string("{\"t\":\"\\u00e9t\\u00e9\"}", "t", out, sizeof out), "bmp escape found");
    CHECK(!strcmp(out, "\xc3\xa9t\xc3\xa9"), "bmp escape is utf-8");
    CHECK(bf_lex_json_string("{\"t\":\"\\ud83d\\ude00\"}", "t", out, sizeof out), "surrogate pair found");
    CHECK(!strcmp(out, "\xf0\x9f\x98\x80"), "surrogate pair is one codepoint");

    /* The cursor form keeps its place past a value it could not keep. */
    {
        char tiny[4];
        const char *doc = "abcdefgh\",\"next\"";
        CHECK(!bf_lex_json_string_at(doc, tiny, sizeof tiny, &end), "truncated reports partial");
        CHECK(*end == ',', "cursor still advanced past the close");
    }

    /* A scalar is not a string and a string is not a scalar. */
    CHECK(bf_lex_json_scalar("{\"http_status\":304}", "http_status", out, sizeof out), "scalar found");
    CHECK(!strcmp(out, "304"), "scalar as written");
    CHECK(!bf_lex_json_scalar("{\"http_status\":\"304\"}", "http_status", out, sizeof out), "quoted is not scalar");

    /* Header names are case-insensitive. */
    CHECK(bf_lex_http_header("Content-Digest: sha-256=:abc:", "content-digest", out, sizeof out), "header found");
    CHECK(!strcmp(out, "sha-256=:abc:"), "header value trimmed");

    /* Both spellings of a Link relation appear in the wild. */
    CHECK(bf_lex_http_link_rel("Link: <a>; rel=\"license\"", "license", out, sizeof out), "quoted rel");
    CHECK(bf_lex_http_link_rel("Link: <a>; rel=license", "license", out, sizeof out), "bare rel");

    /* A trailing empty column is present and empty; a naive split loses it. */
    CHECK(bf_lex_tsv_column("a\tb\t", 3, out, sizeof out), "trailing column present");
    CHECK(out[0] == '\0', "trailing column empty");
    CHECK(!bf_lex_tsv_column("a\tb", 4, out, sizeof out), "past the end is absent");

    /* Column zero is a block header, not a field. */
    CHECK(bf_lex_yaff_field("  maturity measured\n", "maturity", out, sizeof out), "indented field read");
    CHECK(!strcmp(out, "measured"), "field value without the newline");
    CHECK(!bf_lex_yaff_field("maturity measured\n", "maturity", out, sizeof out), "column zero refused");

    /* Query decoding: the strict reading refuses what the lax copy accepted. */
    CHECK(bf_lex_url_query("at=2026-01-01T00%3A00%3A00Z", "at", out, sizeof out), "query found");
    CHECK(!strcmp(out, "2026-01-01T00:00:00Z"), "query decoded");
    CHECK(!bf_lex_url_query("at=2026%", "at", out, sizeof out), "truncated escape refused");
    CHECK(!bf_lex_url_query("at=2026%zz", "at", out, sizeof out), "non-hex escape refused");
    CHECK(!bf_lex_url_query("at=a%00b", "at", out, sizeof out), "control byte refused");
    CHECK(bf_lex_url_query("at=&at=2026", "at", out, sizeof out), "empty value keeps scanning");
    CHECK(!strcmp(out, "2026"), "second value taken");
    CHECK(!bf_lex_url_query("cell=4", "at", out, sizeof out), "absent key absent");
    CHECK(!bf_lex_url_query("as-of=1&at=2", "a", out, sizeof out), "prefix is not the key");
    CHECK(bf_lex_form_query("text=a%0D%0Ab%09c", "text", out, sizeof out) == 1, "form lines and tabs present");
    CHECK(!strcmp(out, "a\r\nb\tc"), "form lines and tabs retained");
    CHECK(bf_lex_form_query("text=", "text", out, sizeof out) == 1 && !out[0], "explicit empty form value");
    CHECK(bf_lex_form_query("other=x", "text", out, sizeof out) == 0, "absent form value");
    CHECK(bf_lex_form_query("text=%00", "text", out, sizeof out) == -1 && !out[0], "form NUL refused");
    CHECK(bf_lex_form_query("text=%01", "text", out, sizeof out) == -1, "form nontext control refused");
    CHECK(bf_lex_form_query("text=%7f", "text", out, sizeof out) == -1, "form DEL refused");
    CHECK(bf_lex_form_query("text=%z0", "text", out, sizeof out) == -1, "form malformed escape is not missing");
    CHECK(bf_lex_form_query("text=a&text=b", "text", out, sizeof out) == -1 && !out[0], "duplicate authored fields refused");
    CHECK(bf_lex_form_query("text=abcdef", "text", out, 4) == -1 && !out[0], "form overflow is not omission");
    CHECK(!bf_lex_url_query("at=a%0Ab", "at", out, sizeof out), "coordinate controls stay refused");

    /* A flag's value, and a flag that is not there. */
    {
        char *argv[] = {"bin", "cmd", "--at", "now", "--bare"};
        CHECK(bf_lex_argv_option(5, argv, "--at") != NULL, "option found");
        CHECK(!strcmp(bf_lex_argv_option(5, argv, "--at"), "now"), "option value");
        CHECK(bf_lex_argv_option(5, argv, "--bare") == NULL, "trailing flag has no value");
        CHECK(bf_lex_argv_option(5, argv, "--none") == NULL, "absent flag absent");
    }

    /* A key at depth is not a key of this object. A record was selected by the
     * email inside its own profile rather than by its own email. */
    CHECK(bf_lex_json_string("{\"id\":\"a\",\"profile\":{\"email\":\"nested@t\"},\"email\":\"own@t\"}",
                             "email", out, sizeof out), "own key found");
    CHECK(!strcmp(out, "own@t"), "own key, not the nested one");
    CHECK(!bf_lex_json_string("{\"profile\":{\"email\":\"nested@t\"}}", "email", out, sizeof out),
          "a key only present at depth is absent here");
    CHECK(!bf_lex_json_scalar("{\"a\":{\"n\":5}}", "n", out, sizeof out), "nested scalar absent too");
    CHECK(bf_lex_json_scalar("{\"a\":{\"n\":5},\"n\":7}", "n", out, sizeof out), "own scalar found");
    CHECK(!strcmp(out, "7"), "own scalar value");

    /* A padded report table. Three witnesses read one with a JSON pattern. */
    CHECK(bf_lex_text_column("1    w@t.test    W    free", 2, out, sizeof out), "column found");
    CHECK(!strcmp(out, "w@t.test"), "column value unpadded");
    CHECK(bf_lex_text_column("  7 a b", 1, out, sizeof out), "leading padding skipped");
    CHECK(!strcmp(out, "7"), "first column past the padding");
    CHECK(!bf_lex_text_column("1 2 3", 4, out, sizeof out), "past the last column is absent");
    CHECK(!bf_lex_text_column("   ", 1, out, sizeof out), "a blank line has no columns");

    /* A dotted path addresses the field rather than hunting for the name. */
    {
        const char *reply = "{\"session\":{\"token\":\"bfy_a\"},\"user\":{\"id\":1,\"token\":\"bfy_b\"}}";
        CHECK(bf_lex_json_string(reply, "session.token", out, sizeof out), "path found");
        CHECK(!strcmp(out, "bfy_a"), "path picked the session token");
        CHECK(bf_lex_json_string(reply, "user.token", out, sizeof out), "other path found");
        CHECK(!strcmp(out, "bfy_b"), "path picked the user token");
        CHECK(!bf_lex_json_string(reply, "token", out, sizeof out), "bare name is ambiguous and absent");
        CHECK(bf_lex_json_scalar(reply, "user.id", out, sizeof out), "scalar by path");
        CHECK(!strcmp(out, "1"), "scalar by path value");
        CHECK(!bf_lex_json_string(reply, "session.nope.token", out, sizeof out), "missing segment is absent");
    }

    /* Records, however they are written. */
    {
        const char *begin, *end;
        const char *arr = "[{\"i\":1},{\"i\":2}]";
        CHECK(bf_lex_json_record(arr, 1, &begin, &end), "second record found");
        CHECK(end - begin == 7 && !strncmp(begin, "{\"i\":2}", 7), "second record bounded exactly");
        CHECK(!bf_lex_json_record(arr, 2, &begin, &end), "no third record");
        CHECK(bf_lex_json_record("{\"a\":{\"b\":1}}", 0, &begin, &end), "nested braces are one record");
        CHECK(!bf_lex_json_record("{\"s\":\"}{\"}", 1, &begin, &end), "a brace in a string is not a record");
    }

    /* ---- emitters. Each case is a defect one of the nineteen copies had. */

    /* A control byte must not reach the document raw. The address plane's own
     * advertisement writer emitted \x01 and \t unescaped. */
    CHECK(bf_lex_json_escape("a\tb\x01" "c", out, sizeof out), "control escaped");
    CHECK(!strcmp(out, "a\\tb\\u0001c"), "control escaped correctly");

    /* A newline must not vanish. One copy dropped every byte below 0x20, so a
     * two-line description was emitted as one line. */
    CHECK(bf_lex_json_escape("line one\nline two", out, sizeof out), "newline kept");
    CHECK(!strcmp(out, "line one\\nline two"), "newline escaped not dropped");

    /* A buffer that cannot hold a whole escape truncates before it, not
     * through it. The copy that reserved four bytes for a six-byte escape
     * advanced six and wrote its terminator past the end. */
    {
        char tight[8];
        CHECK(!bf_lex_json_escape("ab\x01" "cdefgh", tight, sizeof tight), "tight reports partial");
        CHECK(!strcmp(tight, "ab"), "truncated before the escape, not through it");
        CHECK(strlen(tight) < sizeof tight, "terminator inside the buffer");
    }

    /* UTF-8 passes through; escaping it would encode it twice. */
    CHECK(bf_lex_json_escape("caf\xc3\xa9", out, sizeof out), "utf-8 kept");
    CHECK(!strcmp(out, "caf\xc3\xa9"), "utf-8 not re-encoded");

    /* The allocating form sizes itself exactly. */
    {
        const char *in = "a\"b\nc\x01";
        char *heap = bf_lex_json_escape_alloc(in, strlen(in));
        CHECK(heap != NULL, "alloc form returns");
        CHECK(!strcmp(heap, "a\\\"b\\nc\\u0001"), "alloc form escapes");
        CHECK(strlen(heap) == bf_lex_json_escape_len(in, strlen(in)), "length agrees with output");
        free(heap);
    }

    /* The stream form quotes, and a null value is an empty string. */
    {
        char buf[64];
        FILE *mem = fmemopen(buf, sizeof buf, "w");
        CHECK(mem != NULL, "memstream opened");
        bf_lex_json_emit(mem, NULL);
        fclose(mem);
        CHECK(!strcmp(buf, "\"\""), "null emits an empty string");
        mem = fmemopen(buf, sizeof buf, "w");
        bf_lex_json_emit(mem, "he said \"hi\"\n");
        fclose(mem);
        CHECK(!strcmp(buf, "\"he said \\\"hi\\\"\\n\""), "quotes and newline escaped");
    }

    /* What is written can be read back. */
    {
        char doc[256], round[64];
        const char *awkward = "tab\there \"quoted\" back\\slash \xc3\xa9";
        FILE *mem = fmemopen(doc, sizeof doc, "w");
        fputs("{\"k\":", mem);
        bf_lex_json_emit(mem, awkward);
        fputs("}", mem);
        fclose(mem);
        CHECK(bf_lex_json_string(doc, "k", round, sizeof round), "round trip found");
        CHECK(!strcmp(round, awkward), "round trip is the same bytes");
    }

    {
        char body[128] = {0};
        FILE *mem = fmemopen(body, sizeof body, "w");
        bf_lex_html_emit(mem, "plain & <tag> \"double\" 'single'");
        fclose(mem);
        CHECK(!strcmp(body, "plain &amp; &lt;tag&gt; &quot;double&quot; &#039;single&#039;"),
              "html text and both attribute quote styles escaped");
    }

    printf("{\"schema\":\"bonfyre-lex-test.v1\",\"checks\":%d,\"passed\":true,"
           "\"cannot_infer\":[\"these extractors pass -> their callers were converted\","
           "\"these extractors pass -> the documents they read are valid\"]}\n", checks);
    return 0;
}
