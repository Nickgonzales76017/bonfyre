#include "bf_matrix_closure.h"

#include <string.h>

static void json(FILE *out, const char *value) {
    const unsigned char *p = (const unsigned char *)(value ? value : "");
    fputc('"', out);
    for (; *p; p++) {
        switch (*p) {
        case '"': fputs("\\\"", out); break;
        case '\\': fputs("\\\\", out); break;
        case '\n': fputs("\\n", out); break;
        case '\r': fputs("\\r", out); break;
        case '\t': fputs("\\t", out); break;
        default:
            if (*p < 0x20) fprintf(out, "\\u%04x", *p);
            else fputc(*p, out);
        }
    }
    fputc('"', out);
}

static int subject(BfMatrix *matrix, const BfMatrixAddressResolution *resolution,
                   BfMatrixSubjectRecord *record) {
    return matrix && resolution && bf_matrix_subject_read(matrix, resolution->subject_id, record) == 1;
}

static void matrix_cut(FILE *out, const BfMatrixSubjectRecord *record,
                       const BfMatrixAddressResolution *r) {
    fputs("{\"subject\":{\"id\":", out); json(out, record->subject_id);
    fputs(",\"kind\":", out); json(out, record->subject_kind);
    fputs(",\"identityDigest\":", out); json(out, record->identity_digest);
    fputs("},\"presence\":", out); json(out, r->presence_id);
    fputs(",\"address\":", out); json(out, r->canonical_address);
    fputs(",\"territory\":", out); json(out, r->territory);
    fputs(",\"world\":", out); json(out, bf_matrix_world_name(r->world));
    fputs(",\"audience\":", out); json(out, bf_matrix_scope_name(r->scope));
    fputs(",\"representation\":", out); json(out, r->representation);
    fputc('}', out);
}

typedef struct ClosureContributionOut {
    FILE *out;
    int first;
} ClosureContributionOut;

static int emit_contribution(const BfMatrixClosureContributionSpec *value,
                             void *context) {
    ClosureContributionOut *state = context;
    if (!state->first) fputc(',', state->out);
    state->first = 0;
    fputs("{\"plane\":", state->out); json(state->out, value->plane);
    fputs(",\"owner\":", state->out); json(state->out, value->owner);
    fputs(",\"resource\":", state->out); json(state->out, value->resource_uri);
    fputs(",\"representation\":", state->out); json(state->out, value->representation);
    fputs(",\"digest\":", state->out); json(state->out, value->digest);
    fputs(",\"evidence\":", state->out); json(state->out, value->evidence_ref);
    fputc('}', state->out);
    return 0;
}

static int count_contribution(const BfMatrixClosureContributionSpec *value,
                              void *context) {
    (void)value;
    (void)context;
    return 0;
}

static int plane_reference_count(BfMatrix *matrix,
                                 const BfMatrixAddressResolution *r,
                                 const char *plane) {
    return bf_matrix_closure_contributions(matrix, r->subject_id, r->presence_id,
                                           plane, count_contribution, NULL);
}

static void plane(FILE *out, BfMatrix *matrix, const BfMatrixAddressResolution *r,
                  const char *name, const char *fallback_owner,
                  const char *fallback_reason) {
    int references = plane_reference_count(matrix, r, name);
    fputs("{\"name\":", out); json(out, name);
    if (references > 0) {
        fputs(",\"state\":\"referenced\",\"reason\":\"receipted owner reference(s) are listed below; the owner remains the authority\"}", out);
        return;
    }
    fputs(",\"state\":\"unresolved\",\"reason\":", out); json(out, fallback_reason);
    if (fallback_owner && *fallback_owner) { fputs(",\"owner\":", out); json(out, fallback_owner); }
    fputc('}', out);
}

static void owner_contributions(FILE *out, BfMatrix *matrix,
                                const BfMatrixAddressResolution *r) {
    ClosureContributionOut state = {out, 1};
    fputs("\"bonfyre:ownerContributions\":[", out);
    (void)bf_matrix_closure_contributions(matrix, r->subject_id, r->presence_id,
                                          NULL, emit_contribution, &state);
    fputc(']', out);
}

int bf_matrix_closure_emit_ro_crate(FILE *out, BfMatrix *matrix,
                                    const BfMatrixAddressResolution *r,
                                    const char *crate_url, const char *zarr_url) {
    BfMatrixSubjectRecord record;
    if (!out || !crate_url || !zarr_url || !subject(matrix, r, &record)) return -1;
    fputs("{\"@context\":\"https://w3id.org/ro/crate/1.1/context\",\"@graph\":[", out);
    fputs("{\"@id\":\"ro-crate-metadata.json\",\"@type\":\"CreativeWork\",\"about\":{\"@id\":\"#matrix-closure\"},\"conformsTo\":{\"@id\":\"https://w3id.org/ro/crate/1.1\"}},", out);
    fputs("{\"@id\":\"#matrix-closure\",\"@type\":[\"Dataset\",\"BonfyreMatrixClosure\"],\"identifier\":", out); json(out, crate_url);
    fputs(",\"name\":\"Bonfyre Matrix closure\",\"bonfyre:matrixCut\":", out); matrix_cut(out, &record, r);
    fputs(",\"hasPart\":[{\"@id\":", out); json(out, zarr_url);
    fputs(",\"@type\":\"Dataset\"}],\"bonfyre:planes\":[", out);
    fputs("{\"name\":\"exact-semantic\",\"state\":\"resolved\",\"owner\":\"BonfyreMatrix\"},", out);
    plane(out, matrix, r, "material", "ObjectFabric", "ObjectFabric or Format Commons must bind a lawful body"); fputc(',', out);
    plane(out, matrix, r, "latent", "LatentCoordinateFabric", "LatentCoordinateFabric must provide a passported projection"); fputc(',', out);
    plane(out, matrix, r, "execution", "WorkGraph", "Work, Provider, Resource and Authority owners remain independent"); fputc(',', out);
    plane(out, matrix, r, "economic", "FinancialMetabolism", "FinancialMetabolism and MoneyGraph remain independent"); fputs("],", out);
    owner_contributions(out, matrix, r); fputs(",\"bonfyre:forbiddenInference\":\"a closure contribution is a receipted owner reference, never a body read, viewer closure, authority to act, settlement, publication right, or proof that a referenced plane is current\"}", out);
    fputs("]}\n", out);
    return 0;
}

int bf_matrix_closure_emit_zarr_group(FILE *out, BfMatrix *matrix,
                                      const BfMatrixAddressResolution *r,
                                      const char *crate_url) {
    BfMatrixSubjectRecord record;
    if (!out || !crate_url || !subject(matrix, r, &record)) return -1;
    fputs("{\"zarr_format\":3,\"node_type\":\"group\",\"attributes\":{", out);
    fputs("\"bonfyre:virtual\":true,\"bonfyre:crate\":", out); json(out, crate_url);
    fputs(",\"bonfyre:matrixCut\":", out); matrix_cut(out, &record, r);
    fputs(",\"bonfyre:planes\":{\"exact-semantic\":\"resolved\",\"material\":", out);
    json(out, plane_reference_count(matrix, r, "material") > 0 ? "referenced" : "unresolved");
    fputs(",\"latent\":", out); json(out, plane_reference_count(matrix, r, "latent") > 0 ? "referenced" : "unresolved");
    fputs(",\"execution\":", out); json(out, plane_reference_count(matrix, r, "execution") > 0 ? "referenced" : "unresolved");
    fputs(",\"economic\":", out); json(out, plane_reference_count(matrix, r, "economic") > 0 ? "referenced" : "unresolved");
    fputs("},", out); owner_contributions(out, matrix, r);
    fputs(",\"bonfyre:materializationRule\":\"No dense array exists until its named owner produces a receipted lawful body; an owner reference preserves attribution but does not make Matrix the owner or assert availability/currentness.\"}}\n", out);
    return 0;
}
