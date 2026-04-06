/*
 * Lambda Tensors Compression Demo
 *
 * Usage:
 *   ./compress-demo generate <count>     Generate sample JSON records
 *   ./compress-demo compress <in> <out>  Compress JSON with Lambda Tensors
 *   ./compress-demo read <file> <idx>    Read a single record (random access)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <lambda_tensors.h>

static void generate_records(int count) {
    const char *names[] = {
        "Alice", "Bob", "Charlie", "Diana", "Eve",
        "Frank", "Grace", "Hank", "Iris", "Jack"
    };
    const char *cities[] = {
        "New York", "London", "Tokyo", "Paris", "Berlin",
        "Sydney", "Toronto", "Seoul", "Mumbai", "Lagos"
    };

    printf("[\n");
    for (int i = 0; i < count; i++) {
        printf("  {\"id\": %d, \"name\": \"%s\", \"city\": \"%s\", "
               "\"score\": %d, \"active\": %s}%s\n",
               i,
               names[i % 10],
               cities[i % 10],
               (i * 17 + 31) % 100,
               (i % 3 == 0) ? "true" : "false",
               (i < count - 1) ? "," : "");
    }
    printf("]\n");
}

static void compress_file(const char *input_path, const char *output_path) {
    FILE *f = fopen(input_path, "r");
    if (!f) {
        fprintf(stderr, "Error: cannot open %s\n", input_path);
        exit(1);
    }

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);

    char *data = malloc(len + 1);
    fread(data, 1, len, f);
    data[len] = '\0';
    fclose(f);

    LT_Family *fam = lt_family_create();

    /* Parse the JSON array and add each record */
    char *p = data;
    while ((p = strchr(p, '{')) != NULL) {
        char *end = strchr(p, '}');
        if (!end) break;

        size_t record_len = (end - p) + 1;
        lt_family_add(fam, p, record_len);
        p = end + 1;
    }

    lt_family_finalize(fam);

    /* Write compressed output */
    size_t out_len = 0;
    const void *compressed = lt_family_serialize(fam, &out_len);

    FILE *out = fopen(output_path, "wb");
    fwrite(compressed, 1, out_len, out);
    fclose(out);

    printf("Compressed %ld bytes → %zu bytes (%.1f%% of original)\n",
           len, out_len, (double)out_len / len * 100.0);

    lt_family_destroy(fam);
    free(data);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "  %s generate <count>\n", argv[0]);
        fprintf(stderr, "  %s compress <input.json> <output.lt>\n", argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "generate") == 0 && argc >= 3) {
        generate_records(atoi(argv[2]));
    } else if (strcmp(argv[1], "compress") == 0 && argc >= 4) {
        compress_file(argv[2], argv[3]);
    } else {
        fprintf(stderr, "Unknown command: %s\n", argv[1]);
        return 1;
    }

    return 0;
}
