/* Native lowering for ProtocolContract. It owns no new durable protocol store:
 * its only effect is a conformance-gated call into CommitmentGraph's shared
 * SQLite relation. That preserves the single semantic owner of obligation. */
#include "bf_protocol_contract.h"

#include <sqlite3.h>

#include <stdio.h>
#include <string.h>

static int present(const char *value) { return value && value[0]; }
static int copy_text(char out[BF_PROTOCOL_ID_MAX], const char *value) { size_t length; if (!present(value)) return 0; length = strlen(value); if (length >= BF_PROTOCOL_ID_MAX) return 0; memcpy(out, value, length + 1); return 1; }
static uint64_t bit(size_t index) { return UINT64_C(1) << index; }

const char *bf_protocol_status_name(BfProtocolStatus status) { switch (status) { case BF_PROTOCOL_OK: return "ok"; case BF_PROTOCOL_INVALID: return "invalid"; case BF_PROTOCOL_EXISTS: return "exists"; case BF_PROTOCOL_NOT_FOUND: return "not_found"; case BF_PROTOCOL_CAPACITY: return "capacity"; case BF_PROTOCOL_NONCONFORMANT: return "nonconformant"; case BF_PROTOCOL_STORAGE: return "storage"; } return "unknown"; }

void bf_protocol_contract_init(BfProtocolContract *contract, const char *name) { if (!contract) return; memset(contract, 0, sizeof(*contract)); if (name) copy_text(contract->name, name); }

static BfProtocolStatus parameter(BfProtocolContract *contract, const char *name, size_t *out) {
    size_t index;
    if (!contract || !present(name)) return BF_PROTOCOL_INVALID;
    for (index = 0; index < contract->parameter_count; index++) if (strcmp(contract->parameters[index], name) == 0) { *out = index; return BF_PROTOCOL_OK; }
    if (contract->parameter_count == BF_PROTOCOL_MAX_PARAMETERS) return BF_PROTOCOL_CAPACITY;
    if (!copy_text(contract->parameters[contract->parameter_count], name)) return BF_PROTOCOL_CAPACITY;
    *out = contract->parameter_count++; return BF_PROTOCOL_OK;
}

static BfProtocolStatus parameter_mask(BfProtocolContract *contract, const char *const *values, size_t count, uint64_t *out) {
    size_t index, parameter_index; BfProtocolStatus status; *out = 0;
    if (count && !values) return BF_PROTOCOL_INVALID;
    for (index = 0; index < count; index++) { status = parameter(contract, values[index], &parameter_index); if (status != BF_PROTOCOL_OK) return status; *out |= bit(parameter_index); }
    return BF_PROTOCOL_OK;
}

BfProtocolStatus bf_protocol_contract_given(BfProtocolContract *contract, const char *value) { size_t index; BfProtocolStatus status = parameter(contract, value, &index); if (status == BF_PROTOCOL_OK) contract->given |= bit(index); return status; }
BfProtocolStatus bf_protocol_contract_key(BfProtocolContract *contract, const char *value) { size_t index; BfProtocolStatus status = parameter(contract, value, &index); if (status == BF_PROTOCOL_OK) contract->keys |= bit(index); return status; }
BfProtocolStatus bf_protocol_contract_evidence_required(BfProtocolContract *contract, const char *claim) {
    if (!contract || !present(claim)) return BF_PROTOCOL_INVALID;
    for (size_t index = 0; index < contract->evidence_required_count; index++)
        if (strcmp(contract->evidence_required[index], claim) == 0) return BF_PROTOCOL_EXISTS;
    if (contract->evidence_required_count == BF_PROTOCOL_MAX_EVIDENCE_REQUIREMENTS ||
        !copy_text(contract->evidence_required[contract->evidence_required_count], claim)) return BF_PROTOCOL_CAPACITY;
    contract->evidence_required_count++;
    return BF_PROTOCOL_OK;
}

BfProtocolStatus bf_protocol_contract_role(BfProtocolContract *contract, const BfProtocolRoleSpec *spec) {
    BfProtocolRole *role; size_t index; BfProtocolStatus status;
    if (!contract || !spec || !present(spec->name)) return BF_PROTOCOL_INVALID;
    for (index = 0; index < contract->role_count; index++) if (strcmp(contract->roles[index].name, spec->name) == 0) return BF_PROTOCOL_EXISTS;
    if (contract->role_count == BF_PROTOCOL_MAX_ROLES) return BF_PROTOCOL_CAPACITY;
    role = &contract->roles[contract->role_count];
    if (!copy_text(role->name, spec->name) || (present(spec->authority) && !copy_text(role->authority, spec->authority))) return BF_PROTOCOL_CAPACITY;
    status = parameter_mask(contract, spec->requires, spec->requires_count, &role->requires); if (status != BF_PROTOCOL_OK) return status;
    status = parameter_mask(contract, spec->produces, spec->produces_count, &role->produces); if (status != BF_PROTOCOL_OK) return status;
    if (spec->reads) status = parameter_mask(contract, spec->reads, spec->reads_count, &role->reads); else role->reads = role->requires;
    if (status != BF_PROTOCOL_OK) return status; contract->role_count++; return BF_PROTOCOL_OK;
}

static int role_index(const BfProtocolContract *contract, const char *name, size_t *out) { for (size_t index = 0; index < contract->role_count; index++) if (strcmp(contract->roles[index].name, name) == 0) { *out = index; return 1; } return 0; }

BfProtocolStatus bf_protocol_contract_commit(BfProtocolContract *contract, const BfProtocolCommitmentSpec *spec) {
    BfProtocolCommitment *commitment;
    if (!contract || !spec || !present(spec->key) || !present(spec->committer) || !present(spec->beneficiary) || !present(spec->resource)) return BF_PROTOCOL_INVALID;
    if (contract->commitment_count == BF_PROTOCOL_MAX_COMMITMENTS) return BF_PROTOCOL_CAPACITY;
    commitment = &contract->commitments[contract->commitment_count];
    if (!role_index(contract, spec->committer, &commitment->committer) || !role_index(contract, spec->beneficiary, &commitment->beneficiary)) return BF_PROTOCOL_NOT_FOUND;
    if (!copy_text(commitment->key, spec->key) || !copy_text(commitment->resource, spec->resource)) return BF_PROTOCOL_CAPACITY;
    if (present(spec->discharged_by) && !copy_text(commitment->discharged_by, spec->discharged_by)) return BF_PROTOCOL_CAPACITY;
    contract->commitment_count++; return BF_PROTOCOL_OK;
}

BfProtocolStatus bf_protocol_contract_conformance(const BfProtocolContract *contract, const BfInformationPolicy *policy, BfDisclosureAudience audience, BfProtocolConformance *out) {
    uint64_t available, remaining, producers[BF_PROTOCOL_MAX_PARAMETERS] = {0}; int progressed = 1;
    if (!contract || !out || !present(contract->name)) return BF_PROTOCOL_INVALID;
    memset(out, 0, sizeof(*out)); available = contract->given | contract->keys; remaining = contract->role_count == 64 ? UINT64_MAX : ((UINT64_C(1) << contract->role_count) - 1);
    for (size_t role_index_value = 0; role_index_value < contract->role_count; role_index_value++) for (size_t parameter_index = 0; parameter_index < contract->parameter_count; parameter_index++) if (contract->roles[role_index_value].produces & bit(parameter_index)) producers[parameter_index]++;
    for (size_t parameter_index = 0; parameter_index < contract->parameter_count; parameter_index++) if (producers[parameter_index] > 1) out->safety_parameters |= bit(parameter_index);
    while (progressed) { progressed = 0; for (size_t index = 0; index < contract->role_count; index++) if ((remaining & bit(index)) && (contract->roles[index].requires & ~available) == 0) { available |= contract->roles[index].produces; remaining &= ~bit(index); progressed = 1; } }
    out->liveness_roles = remaining;
    for (size_t index = 0; index < contract->role_count; index++) {
        const BfProtocolRole *role = &contract->roles[index]; uint64_t bad = role->requires & ~role->reads;
        if (role->produces && policy) for (size_t parameter_index = 0; parameter_index < contract->parameter_count; parameter_index++) if ((role->requires & role->reads & bit(parameter_index)) != 0) { BfDisclosureDecision decision; if (bf_information_policy_field_decision(policy, contract->parameters[parameter_index], audience, &decision) != BF_INFORMATION_POLICY_OK || !decision.allowed) bad |= bit(parameter_index); }
        if (role->produces && bad) out->integrity_roles |= bit(index);
    }
    return BF_PROTOCOL_OK;
}

BfProtocolStatus bf_protocol_contract_waves(const BfProtocolContract *contract, BfProtocolWaves *out) {
    BfProtocolConformance conformance; uint64_t available, remaining;
    BfProtocolStatus status = bf_protocol_contract_conformance(contract, NULL, BF_AUDIENCE_PUBLIC, &conformance);
    if (status != BF_PROTOCOL_OK) return status; if (conformance.liveness_roles) return BF_PROTOCOL_NONCONFORMANT;
    memset(out, 0, sizeof(*out)); available = contract->given | contract->keys; remaining = contract->role_count == 64 ? UINT64_MAX : ((UINT64_C(1) << contract->role_count) - 1);
    while (remaining) { size_t wave = out->wave_count++; for (size_t index = 0; index < contract->role_count; index++) if ((remaining & bit(index)) && (contract->roles[index].requires & ~available) == 0) { out->role_indices[wave][out->counts[wave]++] = index; } if (!out->counts[wave]) return BF_PROTOCOL_NONCONFORMANT; for (size_t item = 0; item < out->counts[wave]; item++) { size_t index = out->role_indices[wave][item]; available |= contract->roles[index].produces; remaining &= ~bit(index); } }
    return BF_PROTOCOL_OK;
}

static const char *actor_for(const BfProtocolActor *actors, size_t count, const char *role) { for (size_t index = 0; index < count; index++) if (present(actors[index].role) && strcmp(actors[index].role, role) == 0 && present(actors[index].actor)) return actors[index].actor; return NULL; }

BfProtocolStatus bf_protocol_contract_authorize(const BfProtocolContract *contract, void *authority_database, const BfProtocolActor *actors, size_t actor_count, const char *subject, const char *at_iso, const char *purpose, BfProtocolAuthorityRefusal *out_refusal) {
    BfAuthority *authority = NULL;
    if (!contract || !authority_database || !actors || !present(subject)) return BF_PROTOCOL_INVALID;
    if (out_refusal) memset(out_refusal, 0, sizeof(*out_refusal));
    if (bf_authority_open_database(authority_database, &authority, NULL) != 0) return BF_PROTOCOL_STORAGE;
    for (size_t index = 0; index < contract->role_count; index++) {
        const BfProtocolRole *role = &contract->roles[index]; const char *actor;
        if (!present(role->authority)) continue;
        actor = actor_for(actors, actor_count, role->name);
        if (!actor || bf_authority_has(authority, actor, role->authority, subject, at_iso, purpose) != 1) {
            if (out_refusal) { copy_text(out_refusal->role, role->name); if (actor) copy_text(out_refusal->actor, actor); else copy_text(out_refusal->actor, "<unassigned>"); copy_text(out_refusal->permission, role->authority); }
            bf_authority_close(authority); return BF_PROTOCOL_NONCONFORMANT;
        }
    }
    bf_authority_close(authority); return BF_PROTOCOL_OK;
}

BfProtocolStatus bf_protocol_contract_enact(const BfProtocolContract *contract, void *sqlite_database, const char *enactment_id, const BfProtocolActor *actors, size_t actor_count, const char *at, BfProtocolEnactment *out) {
    BfProtocolConformance conformance; BfCommitment *commitment = NULL; sqlite3 *db = sqlite_database; BfProtocolStatus status; int savepoint = 0;
    if (!contract || !db || !present(enactment_id) || !actors || !out) return BF_PROTOCOL_INVALID;
    status = bf_protocol_contract_conformance(contract, NULL, BF_AUDIENCE_PUBLIC, &conformance); if (status != BF_PROTOCOL_OK) return status;
    if (conformance.liveness_roles || conformance.safety_parameters || conformance.integrity_roles) return BF_PROTOCOL_NONCONFORMANT;
    for (size_t index = 0; index < contract->role_count; index++) if (!actor_for(actors, actor_count, contract->roles[index].name)) return BF_PROTOCOL_NOT_FOUND;
    if (bf_commitment_open_database(db, &commitment, NULL) != BF_COMMITMENT_OK) return BF_PROTOCOL_STORAGE;
    if (sqlite3_exec(db, "SAVEPOINT bf_protocol_enact", NULL, NULL, NULL) != SQLITE_OK) { bf_commitment_close(commitment); return BF_PROTOCOL_STORAGE; } savepoint = 1; memset(out, 0, sizeof(*out));
    for (size_t index = 0; index < contract->commitment_count; index++) {
        const BfProtocolCommitment *spec = &contract->commitments[index]; BfCommitmentSpec opening = {0}; int length;
        length = snprintf(out->commitment_ids[index], sizeof(out->commitment_ids[index]), "%s:%s", enactment_id, spec->key); if (length < 0 || length >= (int)sizeof(out->commitment_ids[index])) { status = BF_PROTOCOL_CAPACITY; goto fail; }
        opening.commitment_id = out->commitment_ids[index]; opening.committer = actor_for(actors, actor_count, contract->roles[spec->committer].name); opening.beneficiary = actor_for(actors, actor_count, contract->roles[spec->beneficiary].name); opening.resource = spec->resource; opening.conditions = spec->discharged_by; opening.authority = contract->roles[spec->committer].authority; opening.enactment = enactment_id; opening.at = at;
        if (bf_commitment_record(commitment, &opening) != BF_COMMITMENT_OK) { status = BF_PROTOCOL_STORAGE; goto fail; } out->opened_count++;
    }
    if (sqlite3_exec(db, "RELEASE bf_protocol_enact", NULL, NULL, NULL) != SQLITE_OK) { status = BF_PROTOCOL_STORAGE; goto fail; } savepoint = 0; bf_commitment_close(commitment); return BF_PROTOCOL_OK;
fail:
    if (savepoint) sqlite3_exec(db, "ROLLBACK TO bf_protocol_enact; RELEASE bf_protocol_enact", NULL, NULL, NULL);
    bf_commitment_close(commitment); return status;
}

BfProtocolStatus bf_protocol_contract_enact_authorized(const BfProtocolContract *contract, void *sqlite_database, void *authority_database, const char *enactment_id, const BfProtocolActor *actors, size_t actor_count, const char *subject, const char *at_iso, const char *purpose, const char *at, BfProtocolEnactment *out) {
    BfProtocolStatus status = bf_protocol_contract_authorize(contract, authority_database, actors, actor_count, subject, at_iso, purpose, NULL);
    if (status != BF_PROTOCOL_OK) return status;
    return bf_protocol_contract_enact(contract, sqlite_database, enactment_id, actors, actor_count, at, out);
}

BfProtocolStatus bf_protocol_contract_outstanding(const BfProtocolContract *contract, void *sqlite_database, const char *enactment_id, BfProtocolEnactment *out) {
    BfCommitment *commitment = NULL; sqlite3 *db = sqlite_database;
    if (!contract || !db || !present(enactment_id) || !out) return BF_PROTOCOL_INVALID;
    if (bf_commitment_open_database(db, &commitment, NULL) != BF_COMMITMENT_OK) return BF_PROTOCOL_STORAGE; memset(out, 0, sizeof(*out));
    for (size_t index = 0; index < contract->commitment_count; index++) { BfCommitmentState state; int found; char id[BF_COMMITMENT_ID_MAX]; if (snprintf(id, sizeof(id), "%s:%s", enactment_id, contract->commitments[index].key) >= (int)sizeof(id)) { bf_commitment_close(commitment); return BF_PROTOCOL_CAPACITY; } if (bf_commitment_status(commitment, id, &state, &found) != BF_COMMITMENT_OK) { bf_commitment_close(commitment); return BF_PROTOCOL_STORAGE; } if (found && state != BF_COMMITMENT_DISCHARGED && state != BF_COMMITMENT_COMPENSATED) { if (out->opened_count == BF_PROTOCOL_MAX_COMMITMENTS) { bf_commitment_close(commitment); return BF_PROTOCOL_CAPACITY; } snprintf(out->commitment_ids[out->opened_count++], BF_COMMITMENT_ID_MAX, "%s", id); } }
    bf_commitment_close(commitment); return BF_PROTOCOL_OK;
}

BfProtocolStatus bf_protocol_contract_close(const BfProtocolContract *contract,
                                             void *commitment_database,
                                             void *evidence_database,
                                             const char *enactment_id,
                                             BfProtocolClosure *out) {
    BfEvidence *evidence = NULL;
    BfProtocolStatus status;
    if (!contract || !commitment_database || !evidence_database || !present(enactment_id) || !out)
        return BF_PROTOCOL_INVALID;
    memset(out, 0, sizeof(*out));
    status = bf_protocol_contract_outstanding(contract, commitment_database, enactment_id,
                                              &out->outstanding);
    if (status != BF_PROTOCOL_OK) return status;
    out->obligations_terminal = out->outstanding.opened_count == 0;
    if (bf_evidence_open_database(evidence_database, &evidence, NULL) != 0)
        return BF_PROTOCOL_STORAGE;
    for (size_t index = 0; index < contract->evidence_required_count; index++) {
        BfEvidenceVerdict verdict;
        if (bf_evidence_verdict(evidence, contract->evidence_required[index], &verdict) != BF_EVIDENCE_OK) {
            bf_evidence_close(evidence);
            return BF_PROTOCOL_STORAGE;
        }
        if (verdict != BF_EVIDENCE_SUPPORTED && verdict != BF_EVIDENCE_REPRODUCED) {
            size_t slot = out->unevidenced_count++;
            copy_text(out->unevidenced_claims[slot], contract->evidence_required[index]);
            out->evidence_verdicts[slot] = verdict;
        }
    }
    bf_evidence_close(evidence);
    out->evidence_closed = out->unevidenced_count == 0;
    return BF_PROTOCOL_OK;
}
