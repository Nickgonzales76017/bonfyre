#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define MAX_LINE 4096
#define MAX_RECORDS 1024

typedef struct {
    int id;
    char status[32];
    char job_slug[256];
    char payload_path[1024];
    char source[128];
    int priority;
    char worker[128];
    char created_at[32];
    char updated_at[32];
} QueueRecord;

static void iso_timestamp(char *buffer, size_t size) {
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    strftime(buffer, size, "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
}

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-queue\n\n"
            "Usage:\n"
            "  bonfyre-queue enqueue <queue-file> <job-slug> <payload-path> [--source NAME] [--priority N]\n"
            "  bonfyre-queue list <queue-file>\n"
            "  bonfyre-queue claim <queue-file> [--worker NAME]\n"
            "  bonfyre-queue complete <queue-file> <job-id>\n"
            "  bonfyre-queue fail <queue-file> <job-id>\n"
            "  bonfyre-queue stats <queue-file>\n");
}

static int ensure_parent_dirs(const char *path) {
    char buffer[2048];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(buffer)) return 1;
    snprintf(buffer, sizeof(buffer), "%s", path);
    for (size_t i = 1; i < len; i++) {
        if (buffer[i] == '/') {
            buffer[i] = '\0';
            if (mkdir(buffer, 0755) != 0 && errno != EEXIST) {
                return 1;
            }
            buffer[i] = '/';
        }
    }
    return 0;
}

static int parse_record(const char *line, QueueRecord *record) {
    char fields[9][1024];
    size_t field = 0;
    size_t index = 0;
    memset(fields, 0, sizeof(fields));

    for (size_t i = 0; line[i] != '\0' && line[i] != '\n'; i++) {
        if (line[i] == '\t') {
            if (++field >= 9) return 0;
            index = 0;
            continue;
        }
        if (index + 1 < sizeof(fields[field])) {
            fields[field][index++] = line[i];
        }
    }
    if (field != 8) return 0;

    record->id = atoi(fields[0]);
    snprintf(record->status, sizeof(record->status), "%s", fields[1]);
    snprintf(record->job_slug, sizeof(record->job_slug), "%s", fields[2]);
    snprintf(record->payload_path, sizeof(record->payload_path), "%s", fields[3]);
    snprintf(record->source, sizeof(record->source), "%s", fields[4]);
    record->priority = atoi(fields[5]);
    snprintf(record->worker, sizeof(record->worker), "%s", fields[6]);
    snprintf(record->created_at, sizeof(record->created_at), "%s", fields[7]);
    snprintf(record->updated_at, sizeof(record->updated_at), "%s", fields[8]);
    return 1;
}

static int load_records(const char *queue_file, QueueRecord *records, int *count) {
    FILE *fp = fopen(queue_file, "r");
    *count = 0;
    if (!fp) {
        if (errno == ENOENT) return 0;
        perror("fopen");
        return 1;
    }

    char line[MAX_LINE];
    while (fgets(line, sizeof(line), fp)) {
        if (line[0] == '\n' || line[0] == '\0') continue;
        if (*count >= MAX_RECORDS) {
            fclose(fp);
            fprintf(stderr, "Too many records.\n");
            return 1;
        }
        if (!parse_record(line, &records[*count])) {
            fclose(fp);
            fprintf(stderr, "Failed to parse queue record.\n");
            return 1;
        }
        (*count)++;
    }
    fclose(fp);
    return 0;
}

static int save_records(const char *queue_file, QueueRecord *records, int count) {
    if (ensure_parent_dirs(queue_file) != 0) {
        fprintf(stderr, "Failed to create queue parent dirs.\n");
        return 1;
    }
    FILE *fp = fopen(queue_file, "w");
    if (!fp) {
        perror("fopen");
        return 1;
    }
    for (int i = 0; i < count; i++) {
        fprintf(fp, "%d\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
                records[i].id,
                records[i].status,
                records[i].job_slug,
                records[i].payload_path,
                records[i].source,
                records[i].priority,
                records[i].worker,
                records[i].created_at,
                records[i].updated_at);
    }
    fclose(fp);
    return 0;
}

static int next_id(QueueRecord *records, int count) {
    int max_id = 0;
    for (int i = 0; i < count; i++) {
        if (records[i].id > max_id) max_id = records[i].id;
    }
    return max_id + 1;
}

static int command_enqueue(int argc, char **argv) {
    if (argc < 5) {
        print_usage();
        return 1;
    }
    const char *queue_file = argv[2];
    const char *job_slug = argv[3];
    const char *payload_path = argv[4];
    const char *source = "unknown";
    int priority = 100;

    for (int i = 5; i < argc; i++) {
        if (strcmp(argv[i], "--source") == 0 && i + 1 < argc) {
            source = argv[++i];
        } else if (strcmp(argv[i], "--priority") == 0 && i + 1 < argc) {
            priority = atoi(argv[++i]);
        } else {
            fprintf(stderr, "Unknown enqueue option: %s\n", argv[i]);
            return 1;
        }
    }

    QueueRecord records[MAX_RECORDS];
    int count = 0;
    if (load_records(queue_file, records, &count) != 0) return 1;
    if (count >= MAX_RECORDS) {
        fprintf(stderr, "Queue is full.\n");
        return 1;
    }

    QueueRecord record;
    memset(&record, 0, sizeof(record));
    record.id = next_id(records, count);
    snprintf(record.status, sizeof(record.status), "%s", "queued");
    snprintf(record.job_slug, sizeof(record.job_slug), "%s", job_slug);
    snprintf(record.payload_path, sizeof(record.payload_path), "%s", payload_path);
    snprintf(record.source, sizeof(record.source), "%s", source);
    record.priority = priority;
    iso_timestamp(record.created_at, sizeof(record.created_at));
    snprintf(record.updated_at, sizeof(record.updated_at), "%s", record.created_at);

    records[count++] = record;
    if (save_records(queue_file, records, count) != 0) return 1;

    printf("{\"status\":\"queued\",\"id\":%d,\"jobSlug\":\"%s\",\"queueFile\":\"%s\"}\n",
           record.id, record.job_slug, queue_file);
    return 0;
}

static int compare_claim_order(const QueueRecord *left, const QueueRecord *right) {
    if (left->priority != right->priority) {
        return left->priority - right->priority;
    }
    return left->id - right->id;
}

static int command_claim(int argc, char **argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }
    const char *queue_file = argv[2];
    const char *worker = "local-worker";

    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--worker") == 0 && i + 1 < argc) {
            worker = argv[++i];
        } else {
            fprintf(stderr, "Unknown claim option: %s\n", argv[i]);
            return 1;
        }
    }

    QueueRecord records[MAX_RECORDS];
    int count = 0;
    if (load_records(queue_file, records, &count) != 0) return 1;

    int best_index = -1;
    for (int i = 0; i < count; i++) {
        if (strcmp(records[i].status, "queued") != 0) continue;
        if (best_index < 0 || compare_claim_order(&records[i], &records[best_index]) < 0) {
            best_index = i;
        }
    }

    if (best_index < 0) {
        printf("{\"status\":\"empty\",\"queueFile\":\"%s\"}\n", queue_file);
        return 0;
    }

    snprintf(records[best_index].status, sizeof(records[best_index].status), "%s", "claimed");
    snprintf(records[best_index].worker, sizeof(records[best_index].worker), "%s", worker);
    iso_timestamp(records[best_index].updated_at, sizeof(records[best_index].updated_at));

    if (save_records(queue_file, records, count) != 0) return 1;

    printf("{\"status\":\"claimed\",\"id\":%d,\"jobSlug\":\"%s\",\"payloadPath\":\"%s\",\"worker\":\"%s\"}\n",
           records[best_index].id,
           records[best_index].job_slug,
           records[best_index].payload_path,
           records[best_index].worker);
    return 0;
}

static int update_record_status(const char *queue_file, int target_id, const char *new_status) {
    QueueRecord records[MAX_RECORDS];
    int count = 0;
    if (load_records(queue_file, records, &count) != 0) return 1;

    for (int i = 0; i < count; i++) {
        if (records[i].id == target_id) {
            snprintf(records[i].status, sizeof(records[i].status), "%s", new_status);
            iso_timestamp(records[i].updated_at, sizeof(records[i].updated_at));
            if (save_records(queue_file, records, count) != 0) return 1;
            printf("{\"status\":\"%s\",\"id\":%d,\"jobSlug\":\"%s\"}\n",
                   new_status,
                   records[i].id,
                   records[i].job_slug);
            return 0;
        }
    }

    fprintf(stderr, "Job id not found: %d\n", target_id);
    return 1;
}

static int command_list(const char *queue_file) {
    QueueRecord records[MAX_RECORDS];
    int count = 0;
    if (load_records(queue_file, records, &count) != 0) return 1;

    printf("[\n");
    for (int i = 0; i < count; i++) {
        printf("  {\"id\":%d,\"status\":\"%s\",\"jobSlug\":\"%s\",\"payloadPath\":\"%s\",\"source\":\"%s\",\"priority\":%d,\"worker\":\"%s\"}%s\n",
               records[i].id,
               records[i].status,
               records[i].job_slug,
               records[i].payload_path,
               records[i].source,
               records[i].priority,
               records[i].worker,
               (i + 1 < count) ? "," : "");
    }
    printf("]\n");
    return 0;
}

static int command_stats(const char *queue_file) {
    QueueRecord records[MAX_RECORDS];
    int count = 0;
    if (load_records(queue_file, records, &count) != 0) return 1;

    int queued = 0;
    int claimed = 0;
    int completed = 0;
    int failed = 0;
    for (int i = 0; i < count; i++) {
        if (strcmp(records[i].status, "queued") == 0) queued++;
        else if (strcmp(records[i].status, "claimed") == 0) claimed++;
        else if (strcmp(records[i].status, "completed") == 0) completed++;
        else if (strcmp(records[i].status, "failed") == 0) failed++;
    }

    printf("{\"queueFile\":\"%s\",\"total\":%d,\"queued\":%d,\"claimed\":%d,\"completed\":%d,\"failed\":%d}\n",
           queue_file, count, queued, claimed, completed, failed);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }

    if (strcmp(argv[1], "enqueue") == 0) {
        return command_enqueue(argc, argv);
    }
    if (strcmp(argv[1], "list") == 0) {
        return command_list(argv[2]);
    }
    if (strcmp(argv[1], "claim") == 0) {
        return command_claim(argc, argv);
    }
    if (strcmp(argv[1], "complete") == 0) {
        if (argc != 4) {
            print_usage();
            return 1;
        }
        return update_record_status(argv[2], atoi(argv[3]), "completed");
    }
    if (strcmp(argv[1], "fail") == 0) {
        if (argc != 4) {
            print_usage();
            return 1;
        }
        return update_record_status(argv[2], atoi(argv[3]), "failed");
    }
    if (strcmp(argv[1], "stats") == 0) {
        return command_stats(argv[2]);
    }

    print_usage();
    return 1;
}
