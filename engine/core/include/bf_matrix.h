#ifndef BF_MATRIX_H
#define BF_MATRIX_H

/*
 * Bonfyre Matrix: the native, factorized substrate for semantic subjects,
 * address constellations, recursive institutions, and publication presences.
 *
 * It deliberately does not contain a tenant model, router, feed, or generic
 * business ontology. A subject has one stable semantic identity and may acquire
 * many addresses and presences. AuthorityGraph remains the authority owner:
 * delegation and publication ask it to admit a named effect before Matrix
 * records the resulting relationship/projection.
 */

#include <stddef.h>
#include <stdio.h>

#include "bf_authority.h"

typedef struct BfMatrix BfMatrix;

typedef enum BfMatrixStatus {
    BF_MATRIX_OK = 0,
    BF_MATRIX_INVALID,
    BF_MATRIX_INVALID_WORLD,
    BF_MATRIX_INVALID_TERRITORY,
    BF_MATRIX_INVALID_SCOPE,
    BF_MATRIX_INVALID_ADDRESS_KIND,
    BF_MATRIX_INVALID_DIGEST,
    BF_MATRIX_MISSING_EVIDENCE,
    BF_MATRIX_UNKNOWN_SUBJECT,
    BF_MATRIX_UNKNOWN_INSTITUTION,
    BF_MATRIX_UNKNOWN_PRESENCE,
    BF_MATRIX_ADDRESS_CONFLICT,
    BF_MATRIX_IDENTITY_CONFLICT,
    BF_MATRIX_DUPLICATE_PRESENCE,
    BF_MATRIX_SCOPE_REGRESSION,
    BF_MATRIX_CYCLE,
    BF_MATRIX_UNAUTHORIZED,
    BF_MATRIX_STORAGE_ERROR
} BfMatrixStatus;

typedef enum BfMatrixWorld {
    BF_MATRIX_LIVE = 0,
    BF_MATRIX_SHADOW,
    BF_MATRIX_REPLAY,
    BF_MATRIX_CANDIDATE,
    /* DEMO was declared in architecture/institutional/worlds.yaff and absent
     * here, so the world the demo estate is supposed to execute in could not be
     * bound to a host: the contract named four worlds, this enum named four
     * worlds, and only three were the same ones. Appended rather than inserted
     * so no existing ordinal moves. */
    BF_MATRIX_DEMO
} BfMatrixWorld;

/* Ordered disclosure membrane, not a taxonomy of semantic meaning. */
typedef enum BfMatrixParticipationScope {
    BF_MATRIX_SELF = 0,
    BF_MATRIX_HOUSEHOLD,
    BF_MATRIX_GROUP,
    BF_MATRIX_COMMUNITY,
    BF_MATRIX_PUBLIC,
    BF_MATRIX_MARKET,
    BF_MATRIX_INSTITUTION
} BfMatrixParticipationScope;

typedef struct BfMatrixSubjectSpec {
    const char *subject_id;       /* stable semantic identity, required */
    const char *subject_kind;     /* extensible domain meaning, required */
    const char *identity_digest;  /* 64 hexadecimal chars, required */
    const char *evidence_ref;     /* receipt, origin, or verified creation, required */
} BfMatrixSubjectSpec;

typedef struct BfMatrixInstitutionSpec {
    const char *institution_id;   /* stable institutional identity, required */
    const char *institution_kind; /* company, co-op, group, market, etc.; required */
    BfMatrixWorld world;
    const char *evidence_ref;     /* required; institution declaration is evidence-bound */
} BfMatrixInstitutionSpec;

typedef struct BfMatrixPresenceSpec {
    const char *presence_id;      /* projection identity, required */
    const char *subject_id;       /* one semantic subject, required */
    const char *institution_id;   /* optional host/owner institution */
    const char *territory;        /* life|live|shop|work|independent */
    BfMatrixParticipationScope scope;
    BfMatrixWorld world;
    const char *representation;  /* HTML, API, MCP, Habitat, media, ... */
    const char *evidence_ref;     /* required */
} BfMatrixPresenceSpec;

typedef struct BfMatrixAddressResolution {
    char subject_id[256];
    char presence_id[256];
    char territory[32];
    char representation[64];
    char address_kind[32];
    char requested_address[1536];
    char canonical_address[1536];
    int is_alias;
    BfMatrixParticipationScope scope;
    BfMatrixWorld world;
} BfMatrixAddressResolution;

/* Read-only identity binding for a compiled closure. It is deliberately
 * narrower than a subject object: relations, evidence bodies, authority,
 * providers, work and economics stay with their respective owners. */
typedef struct BfMatrixSubjectRecord {
    char subject_id[256];
    char subject_kind[128];
    char identity_digest[65];
} BfMatrixSubjectRecord;

/* A closure contribution is an immutable, evidence-bound pointer from one
 * already-published Matrix presence to a body owned elsewhere.  It is an
 * address/compiler index only: the named owner retains its semantics,
 * availability, read policy, authority, lifecycle and any effect boundary.
 * There is intentionally no generic value/document payload here. */
typedef struct BfMatrixClosureContributionSpec {
    const char *subject_id;
    const char *presence_id;
    const char *plane;            /* material|latent|execution|economic */
    const char *owner;            /* canonical owner name, required */
    const char *resource_uri;     /* bonfyre://, https://, or urn: reference */
    const char *representation;   /* Zarr, Parquet, RO-Crate, HTMX, ... */
    const char *digest;           /* exact body/manifest digest, required */
    const char *evidence_ref;     /* owner receipt/provenance, required */
} BfMatrixClosureContributionSpec;

typedef int (*BfMatrixClosureContributionVisitor)(
    const BfMatrixClosureContributionSpec *contribution, void *context);

/* A host is a public address context, not a tenant and not an authority grant.
 * More-specific registered hosts inherit nothing from a broader host except
 * where their own domain record deliberately names it as canonical. */
typedef struct BfMatrixDomainSpec {
    const char *hostname;            /* lower-case DNS name, required */
    const char *institution_id;      /* optional institutional host */
    const char *territory;           /* life|live|shop|work|independent */
    BfMatrixParticipationScope default_scope;
    BfMatrixWorld default_world;
    const char *default_representation;
    const char *canonical_hostname;  /* optional existing canonical host */
    const char *evidence_ref;        /* ownership/verification receipt, required */
} BfMatrixDomainSpec;

typedef struct BfMatrixDomainResolution {
    char requested_hostname[256];
    char matched_hostname[256];
    char canonical_hostname[256];
    char institution_id[256];
    char territory[32];
    char representation[64];
    BfMatrixParticipationScope scope;
    BfMatrixWorld world;
} BfMatrixDomainResolution;

/* A StandingAddress is a stable address whose members are derived from the
 * current Matrix, not a copied result array. Every filter is optional; -1
 * means that world/scope dimension is not constrained. */
typedef struct BfMatrixStandingAddressSpec {
    const char *address;
    const char *subject_kind;
    const char *territory;
    int scope;                  /* BfMatrixParticipationScope or -1 */
    int world;                  /* BfMatrixWorld or -1 */
    const char *representation;
    const char *evidence_ref;
} BfMatrixStandingAddressSpec;

int bf_matrix_open(const char *database_path, BfMatrix **out, FILE *err);
int bf_matrix_open_database(void *sqlite_database, BfMatrix **out, FILE *err);
void bf_matrix_close(BfMatrix *matrix);

/* Bind an immutable semantic identity. Reusing an id with different binding
 * facts is refused; make a new subject or model an explicit relation instead. */
BfMatrixStatus bf_matrix_subject_bind(BfMatrix *matrix, const BfMatrixSubjectSpec *spec);

/* Add a lawful location without copying the subject. The address is globally
 * unambiguous, while one subject may own any number of distinct addresses. */
BfMatrixStatus bf_matrix_address_bind(BfMatrix *matrix, const char *subject_id,
                                      const char *address_kind, const char *address,
                                      const char *evidence_ref);
int bf_matrix_address_subject(BfMatrix *matrix, const char *address,
                              char *subject_id, size_t subject_id_size);
/* Resolve a lawful address through an active presence. Address identity alone
 * never implies disclosure: territory/world/audience/representation must name
 * a matching published presence. */
int bf_matrix_address_resolve(BfMatrix *matrix, const char *address,
                              const char *territory, BfMatrixWorld world,
                              BfMatrixParticipationScope scope,
                              const char *representation,
                              BfMatrixAddressResolution *out);
int bf_matrix_subject_address_count(BfMatrix *matrix, const char *subject_id);
/* Returns 1 when the canonical identity binding exists, 0 when absent, and
 * -1 on storage failure. This is a read primitive for closure compilation;
 * it does not grant a reader any subject closure. */
int bf_matrix_subject_read(BfMatrix *matrix, const char *subject_id,
                           BfMatrixSubjectRecord *out);
/* Publishes a non-semantic owner reference only after the exact subject and
 * active Presence are already bound.  A contribution cannot create a
 * subject, Presence, disclosure, owner state, authority, or an effect. */
BfMatrixStatus bf_matrix_closure_contribution_bind(
    BfMatrix *matrix, const BfMatrixClosureContributionSpec *spec);
/* Visits immutable owner references for one exact public/published cut. */
int bf_matrix_closure_contributions(
    BfMatrix *matrix, const char *subject_id, const char *presence_id,
    const char *plane, BfMatrixClosureContributionVisitor visitor,
    void *context);
typedef int (*BfMatrixAddressVisitor)(const char *address, const char *address_kind,
                                      const char *canonical_address, const char *evidence_ref,
                                      void *context);
/* Visits every address carrier of one stable subject. Returning nonzero from
 * the visitor stops iteration without changing the Matrix. */
int bf_matrix_subject_addresses(BfMatrix *matrix, const char *subject_id,
                                BfMatrixAddressVisitor visitor, void *context);

/* Binds an alias only when both endpoints already address the same subject.
 * The requested address remains a lawful coordinate, while canonical_address
 * gives carriers a stable redirect/projection target without copying state. */
BfMatrixStatus bf_matrix_address_alias(BfMatrix *matrix, const char *alias_address,
                                       const char *canonical_address, const char *evidence_ref);

BfMatrixStatus bf_matrix_domain_bind(BfMatrix *matrix, const BfMatrixDomainSpec *spec);
/* Exact host wins; otherwise the longest registered DNS suffix provides the
 * controlled host context. Returns 1 found, 0 no controlled domain, -1 error. */
int bf_matrix_domain_resolve(BfMatrix *matrix, const char *hostname,
                             BfMatrixDomainResolution *out);

BfMatrixStatus bf_matrix_standing_address_bind(BfMatrix *matrix,
                                                const BfMatrixStandingAddressSpec *spec);
/* 1 means this address is standing, 0 means ordinary/nonexistent, -1 error. */
int bf_matrix_standing_address_member_count(BfMatrix *matrix, const char *address,
                                            int *member_count);

/* 1 when the subject exists, 0 when it does not, -1 on error. */
typedef struct BfMatrixDuplicateIdentity {
    char a[256];
    char b[256];
    char identity_digest[80];
} BfMatrixDuplicateIdentity;

/* Subjects of one kind sharing an identity digest: one thing named twice.
 * Returns how many pairs were written, or -1 on error. */
int bf_matrix_duplicate_identities(BfMatrix *matrix, const char *subject_kind,
                                   BfMatrixDuplicateIdentity *out, int max);
/* Build a closure resolution from a subject and one of its active presences,
 * for subjects that are not reachable at any host. 1 on success, 0 when no such
 * active presence exists, -1 on error. */
int bf_matrix_presence_resolution(BfMatrix *matrix, const char *subject_id,
                                  const char *presence_id,
                                  BfMatrixAddressResolution *out);
int bf_matrix_subject_exists(BfMatrix *matrix, const char *subject_id);

/* An edge between a subject and another subject or institution. The Matrix owns
 * subjects, so it owns the edges between them; a relation with no evidence_ref
 * is refused for the same reason every other binding here requires one. */
BfMatrixStatus bf_matrix_subject_relate(BfMatrix *matrix, const char *from_subject,
                                        const char *relation, const char *to_subject,
                                        const char *evidence_ref);
int bf_matrix_subject_relation_from_count(BfMatrix *matrix, const char *from_subject,
                                          const char *relation);
int bf_matrix_subject_relation_count(BfMatrix *matrix, const char *to_subject,
                                     const char *relation);

BfMatrixStatus bf_matrix_institution_bind(BfMatrix *matrix,
                                           const BfMatrixInstitutionSpec *spec);
/* Adds parent -> child only after AuthorityGraph admits delegate over parent. */
BfMatrixStatus bf_matrix_institution_delegate(BfMatrix *matrix, BfAuthority *authority,
                                               const char *actor, const char *parent_id,
                                               const char *child_id, const char *authority_ref,
                                               const char *at_iso, const char *purpose);

/* A presence is a projection of a subject, never a duplicate semantic object. */
BfMatrixStatus bf_matrix_presence_bind(BfMatrix *matrix, const BfMatrixPresenceSpec *spec);
/* Publishes a new, more-disclosed presence after AuthorityGraph admits publish.
 * The original presence remains, retaining the publication membrane. */
BfMatrixStatus bf_matrix_presence_project(BfMatrix *matrix, BfAuthority *authority,
                                           const char *actor, const char *source_presence_id,
                                           const BfMatrixPresenceSpec *target,
                                           const char *authority_ref, const char *at_iso,
                                           const char *purpose);
BfMatrixStatus bf_matrix_presence_withdraw(BfMatrix *matrix, BfAuthority *authority,
                                            const char *actor, const char *presence_id,
                                            const char *authority_ref, const char *at_iso,
                                            const char *purpose);
int bf_matrix_subject_presence_count(BfMatrix *matrix, const char *subject_id,
                                     int active_only);

const char *bf_matrix_status_name(BfMatrixStatus status);
const char *bf_matrix_world_name(BfMatrixWorld world);
const char *bf_matrix_scope_name(BfMatrixParticipationScope scope);

#endif
