#include "bf_semantic_derivation.h"

#include "bonfyre.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int present(const char *value)
{
    return value != NULL && value[0] != '\0';
}

static int copy_text(char *out, size_t out_size, const char *value)
{
    return present(value) && snprintf(out, out_size, "%s", value) < (int)out_size;
}

static int contains(char values[][BF_DERIVATION_REF_MAX], size_t count, const char *value)
{
    for (size_t index = 0; index < count; index++) {
        if (strcmp(values[index], value) == 0) return 1;
    }
    return 0;
}

static int append_unique(char values[][BF_DERIVATION_REF_MAX], size_t *count,
                         size_t capacity, const char *value)
{
    if (contains(values, *count, value)) return 1;
    if (*count == capacity || !copy_text(values[*count], BF_DERIVATION_REF_MAX, value)) return 0;
    (*count)++;
    return 1;
}

const char *bf_derivation_status_name(BfDerivationStatus status)
{
    switch (status) {
    case BF_DERIVATION_OK: return "ok";
    case BF_DERIVATION_INVALID: return "invalid";
    case BF_DERIVATION_CAPACITY: return "capacity";
    case BF_DERIVATION_DUPLICATE_PROGRAM: return "duplicate_program";
    case BF_DERIVATION_DUPLICATE_REFERENCE: return "duplicate_reference";
    }
    return "unknown";
}

void bf_hlc_tick(const BfHybridLogicalClock *clock, uint64_t physical_ms,
                 BfHybridLogicalClock *out)
{
    if (clock == NULL || out == NULL) return;
    *out = *clock;
    if (physical_ms > clock->physical_ms) {
        out->physical_ms = physical_ms;
        out->logical = 0;
    } else {
        out->logical++;
    }
}

void bf_hlc_merge(const BfHybridLogicalClock *left, const BfHybridLogicalClock *right,
                  uint64_t physical_ms, BfHybridLogicalClock *out)
{
    uint64_t top;
    if (left == NULL || right == NULL || out == NULL) return;
    top = left->physical_ms > right->physical_ms ? left->physical_ms : right->physical_ms;
    if (physical_ms > top) top = physical_ms;
    *out = *left;
    out->physical_ms = top;
    if (top == left->physical_ms && top == right->physical_ms) {
        out->logical = (left->logical > right->logical ? left->logical : right->logical) + 1;
    } else if (top == left->physical_ms) {
        out->logical = left->logical + 1;
    } else if (top == right->physical_ms) {
        out->logical = right->logical + 1;
    } else {
        out->logical = 0;
    }
}

void bf_semantic_derivation_init(BfSemanticDerivationGraph *graph)
{
    if (graph != NULL) memset(graph, 0, sizeof(*graph));
}

static BfDerivationStatus copy_refs(char destination[][BF_DERIVATION_REF_MAX], size_t *count,
                                    const char *const *source, size_t source_count)
{
    if (source_count > BF_DERIVATION_REF_MAX_PER_PROGRAM ||
        (source_count > 0 && source == NULL)) return BF_DERIVATION_CAPACITY;
    for (size_t index = 0; index < source_count; index++) {
        if (!present(source[index])) return BF_DERIVATION_INVALID;
        if (contains(destination, *count, source[index])) return BF_DERIVATION_DUPLICATE_REFERENCE;
        if (!append_unique(destination, count, BF_DERIVATION_REF_MAX_PER_PROGRAM, source[index])) {
            return BF_DERIVATION_CAPACITY;
        }
    }
    return BF_DERIVATION_OK;
}

BfDerivationStatus bf_semantic_derivation_add(BfSemanticDerivationGraph *graph,
                                               const BfSemanticProgramSpec *program)
{
    BfSemanticProgram stored;
    BfDerivationStatus status;
    if (graph == NULL || program == NULL || !present(program->program_id) ||
        !present(program->operator_name)) return BF_DERIVATION_INVALID;
    if (graph->program_count == BF_DERIVATION_PROGRAM_MAX) return BF_DERIVATION_CAPACITY;
    for (size_t index = 0; index < graph->program_count; index++) {
        if (strcmp(graph->programs[index].program_id, program->program_id) == 0) {
            return BF_DERIVATION_DUPLICATE_PROGRAM;
        }
    }
    if (program->constraint_count > BF_DERIVATION_CONSTRAINT_MAX ||
        (program->constraint_count > 0 && program->constraints == NULL)) return BF_DERIVATION_CAPACITY;
    memset(&stored, 0, sizeof(stored));
    if (!copy_text(stored.program_id, sizeof(stored.program_id), program->program_id) ||
        !copy_text(stored.operator_name, sizeof(stored.operator_name), program->operator_name)) {
        return BF_DERIVATION_CAPACITY;
    }
    status = copy_refs(stored.reads, &stored.read_count, program->reads, program->read_count);
    if (status != BF_DERIVATION_OK) return status;
    status = copy_refs(stored.writes, &stored.write_count, program->writes, program->write_count);
    if (status != BF_DERIVATION_OK) return status;
    for (size_t index = 0; index < program->constraint_count; index++) {
        const BfConstraintSpec *constraint = &program->constraints[index];
        if (!copy_text(stored.constraints[index].constraint_id,
                       sizeof(stored.constraints[index].constraint_id), constraint->constraint_id) ||
            !copy_text(stored.constraints[index].source_ref,
                       sizeof(stored.constraints[index].source_ref), constraint->source_ref)) {
            return BF_DERIVATION_INVALID;
        }
        stored.constraints[index].hard = constraint->hard != 0;
        stored.constraints[index].satisfied = constraint->satisfied != 0;
        stored.constraint_count++;
    }
    graph->programs[graph->program_count++] = stored;
    return BF_DERIVATION_OK;
}

static int program_reads_changed(const BfSemanticProgram *program,
                                 char changed[][BF_DERIVATION_REF_MAX], size_t changed_count)
{
    for (size_t read = 0; read < program->read_count; read++) {
        if (contains(changed, changed_count, program->reads[read])) return 1;
    }
    return 0;
}

BfDerivationStatus bf_semantic_derivation_slice(const BfSemanticDerivationGraph *graph,
                                                 const BfFactDelta *delta,
                                                 BfSemanticSlice *out)
{
    char changed[BF_DERIVATION_NODE_MAX][BF_DERIVATION_REF_MAX] = {{0}};
    unsigned char pending[BF_DERIVATION_PROGRAM_MAX] = {0};
    size_t changed_count = 0;
    if (graph == NULL || delta == NULL || out == NULL || !present(delta->delta_id) ||
        !present(delta->causal_frontier) || delta->write_count > BF_DERIVATION_NODE_MAX ||
        (delta->write_count > 0 && delta->writes == NULL)) return BF_DERIVATION_INVALID;
    memset(out, 0, sizeof(*out));
    if (!copy_text(out->delta_id, sizeof(out->delta_id), delta->delta_id)) return BF_DERIVATION_CAPACITY;
    for (size_t index = 0; index < delta->write_count; index++) {
        if (!present(delta->writes[index])) return BF_DERIVATION_INVALID;
        if (!append_unique(changed, &changed_count, BF_DERIVATION_NODE_MAX, delta->writes[index])) {
            return BF_DERIVATION_CAPACITY;
        }
    }
    memset(pending, 1, graph->program_count);
    for (;;) {
        int progressed = 0;
        for (size_t index = 0; index < graph->program_count; index++) {
            const BfSemanticProgram *program = &graph->programs[index];
            if (!pending[index] || !program_reads_changed(program, changed, changed_count)) continue;
            if (out->affected_count == BF_DERIVATION_PROGRAM_MAX ||
                !copy_text(out->affected_programs[out->affected_count], BF_DERIVATION_ID_MAX,
                           program->program_id)) return BF_DERIVATION_CAPACITY;
            out->affected_count++;
            for (size_t read = 0; read < program->read_count; read++) {
                if (!append_unique(out->read_closure, &out->read_closure_count,
                                   BF_DERIVATION_NODE_MAX, program->reads[read])) {
                    return BF_DERIVATION_CAPACITY;
                }
            }
            for (size_t write = 0; write < program->write_count; write++) {
                if (!append_unique(changed, &changed_count, BF_DERIVATION_NODE_MAX,
                                   program->writes[write])) return BF_DERIVATION_CAPACITY;
            }
            pending[index] = 0;
            progressed = 1;
        }
        if (!progressed) break;
    }
    return BF_DERIVATION_OK;
}

static int compare_refs(const void *left, const void *right)
{
    return strcmp(*(const char *const *)left, *(const char *const *)right);
}

BfDerivationStatus bf_semantic_derivation_decide(const BfSemanticProgram *program,
                                                  BfDecisionSnapshot *out)
{
    const char *ordered[BF_DERIVATION_REF_MAX_PER_PROGRAM];
    BfSha256 hash;
    uint8_t digest[32];
    if (program == NULL || out == NULL || !present(program->program_id) ||
        program->read_count > BF_DERIVATION_REF_MAX_PER_PROGRAM ||
        program->constraint_count > BF_DERIVATION_CONSTRAINT_MAX) return BF_DERIVATION_INVALID;
    memset(out, 0, sizeof(*out));
    if (!copy_text(out->program_id, sizeof(out->program_id), program->program_id)) return BF_DERIVATION_INVALID;
    out->admitted = 1;
    for (size_t index = 0; index < program->constraint_count; index++) {
        const char *id = program->constraints[index].constraint_id;
        if (!present(id)) return BF_DERIVATION_INVALID;
        if (program->constraints[index].hard && !program->constraints[index].satisfied) {
            if (!copy_text(out->hard_failures[out->hard_failure_count++], BF_DERIVATION_ID_MAX, id)) {
                return BF_DERIVATION_CAPACITY;
            }
            out->admitted = 0;
        } else if (!program->constraints[index].hard) {
            if (!copy_text(out->learned_signals[out->learned_signal_count++], BF_DERIVATION_ID_MAX, id)) {
                return BF_DERIVATION_CAPACITY;
            }
        }
    }
    for (size_t index = 0; index < program->read_count; index++) ordered[index] = program->reads[index];
    qsort(ordered, program->read_count, sizeof(ordered[0]), compare_refs);
    bf_sha256_init(&hash);
    for (size_t index = 0; index < program->read_count; index++) {
        if (index > 0) bf_sha256_update(&hash, (const uint8_t *)"\n", 1);
        bf_sha256_update(&hash, (const uint8_t *)ordered[index], strlen(ordered[index]));
    }
    bf_sha256_final(&hash, digest);
    bf_sha256_digest_hex(digest, out->read_closure_sha256);
    return BF_DERIVATION_OK;
}
