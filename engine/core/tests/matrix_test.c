/* Matrix conformance: one semantic subject, many lawful addresses/presences;
 * recursive institutions are authority-gated; publication preserves the source
 * presence and can move only outward through the disclosure membrane. */

#include "bf_matrix.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *message) { fprintf(stderr, "FAIL: %s\n", message); return 1; }
static int count_address(const char *address, const char *kind, const char *canonical, const char *evidence, void *context) { int *count = context; (void)address; (void)kind; (void)canonical; (void)evidence; (*count)++; return 0; }

static BfAuthorityEdge grant(const char *id, const char *permission, const char *subject) {
    BfAuthorityEdge edge;
    memset(&edge, 0, sizeof(edge));
    edge.edge_id = id; edge.actor = "person:nick"; edge.permission = permission; edge.subject = subject;
    edge.evidence = "evidence:authority";
    return edge;
}

int main(void) {
    char path[] = "/tmp/bf_matrix_XXXXXX"; int fd = mkstemp(path); BfMatrix *matrix = NULL; BfAuthority *authority = NULL; char subject[128]; BfMatrixAddressResolution address_resolution; BfMatrixDomainResolution domain_resolution;
    BfMatrixSubjectSpec plant = {"object:plant:luna", "PlantPassport", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "receipt:plant:luna"};
    BfMatrixSubjectSpec copy = {"object:plant:copy", "PlantPassport", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "receipt:plant:copy"};
    BfMatrixSubjectSpec standing = {"standing:plants:life", "StandingAddress", "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", "receipt:standing:plants"};
    BfMatrixInstitutionSpec aurekai = {"institution:aurekai", "company", BF_MATRIX_LIVE, "receipt:aurekai"};
    BfMatrixInstitutionSpec market = {"institution:plant-collective", "co-op", BF_MATRIX_LIVE, "receipt:collective"};
    BfMatrixPresenceSpec self = {"presence:luna:self", "object:plant:luna", NULL, "life", BF_MATRIX_SELF, BF_MATRIX_LIVE, "HTML", "receipt:presence:self"};
    BfMatrixPresenceSpec public_live = {"presence:luna:public", "object:plant:luna", "institution:plant-collective", "live", BF_MATRIX_PUBLIC, BF_MATRIX_LIVE, "Habitat", "receipt:presence:public"};
    BfMatrixPresenceSpec shop = {"presence:luna:market", "object:plant:luna", "institution:plant-collective", "shop", BF_MATRIX_MARKET, BF_MATRIX_LIVE, "MCP", "receipt:presence:market"};
    BfMatrixPresenceSpec copy_self = {"presence:copy:self", "object:plant:copy", NULL, "life", BF_MATRIX_SELF, BF_MATRIX_LIVE, "HTML", "receipt:presence:copy"};
    if (fd >= 0) close(fd); unlink(path);
    if (bf_matrix_open(path, &matrix, stderr) != 0 || bf_authority_open(path, &authority, stderr) != 0) return fail("open shared estate");
    if (bf_matrix_subject_bind(matrix, &plant) != BF_MATRIX_OK || bf_matrix_subject_bind(matrix, &plant) != BF_MATRIX_OK) return fail("idempotent semantic bind");
    plant.identity_digest = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    if (bf_matrix_subject_bind(matrix, &plant) != BF_MATRIX_IDENTITY_CONFLICT) return fail("identity cannot silently mutate");
    plant.identity_digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    if (bf_matrix_subject_bind(matrix, &copy) != BF_MATRIX_OK || bf_matrix_subject_bind(matrix, &standing) != BF_MATRIX_OK) return fail("bind distinct subject");
    if (bf_matrix_address_bind(matrix, "object:plant:luna", "web_url", "https://aurekai.life/plants/luna", "receipt:url") != BF_MATRIX_OK ||
        bf_matrix_address_bind(matrix, "object:plant:luna", "historical_alias", "https://luna.aurekai.life/plants/luna", "receipt:url:alias") != BF_MATRIX_OK ||
        bf_matrix_address_bind(matrix, "object:plant:luna", "fs_path", "/Bonfyre/Life/Plants/luna", "receipt:fs") != BF_MATRIX_OK ||
        bf_matrix_address_bind(matrix, "object:plant:luna", "mcp", "mcp://bonfyre/object/plant/luna", "receipt:mcp") != BF_MATRIX_OK ||
        bf_matrix_address_bind(matrix, "standing:plants:life", "standing_query", "https://aurekai.life/plants", "receipt:standing:url") != BF_MATRIX_OK) return fail("address constellation");
    if (bf_matrix_address_bind(matrix, "object:plant:copy", "web_url", "https://aurekai.life/plants/luna", "receipt:copy") != BF_MATRIX_ADDRESS_CONFLICT) return fail("one address cannot copy a subject");
    int address_count = 0;
    if (bf_matrix_address_subject(matrix, "mcp://bonfyre/object/plant/luna", subject, sizeof(subject)) != 1 || strcmp(subject, "object:plant:luna") != 0 || bf_matrix_subject_address_count(matrix, subject) != 4 || bf_matrix_subject_addresses(matrix, subject, count_address, &address_count) != 4 || address_count != 4) return fail("addresses resolve one semantic identity");
    if (bf_matrix_institution_bind(matrix, &aurekai) != BF_MATRIX_OK || bf_matrix_institution_bind(matrix, &market) != BF_MATRIX_OK) return fail("institution bind");
    BfMatrixDomainSpec life_domain = {"aurekai.life", "institution:aurekai", "life", BF_MATRIX_SELF, BF_MATRIX_LIVE, "HTML", NULL, "receipt:domain:life"};
    BfMatrixDomainSpec leaf_domain = {"theleaflounge.store", "institution:plant-collective", "independent", BF_MATRIX_PUBLIC, BF_MATRIX_LIVE, "HTML", NULL, "receipt:domain:leaf"};
    if (bf_matrix_domain_bind(matrix, &life_domain) != BF_MATRIX_OK || bf_matrix_domain_bind(matrix, &leaf_domain) != BF_MATRIX_OK ||
        bf_matrix_domain_resolve(matrix, "luna.aurekai.life", &domain_resolution) != 1 || strcmp(domain_resolution.matched_hostname, "aurekai.life") != 0 ||
        strcmp(domain_resolution.territory, "life") != 0 || domain_resolution.scope != BF_MATRIX_SELF ||
        bf_matrix_domain_resolve(matrix, "theleaflounge.store", &domain_resolution) != 1 || strcmp(domain_resolution.territory, "independent") != 0) return fail("controlled hosts resolve inherited public context");
    if (bf_matrix_institution_delegate(matrix, authority, "person:nick", "institution:aurekai", "institution:plant-collective", "grant:delegate", "2026-08-28T00:00:00+00:00", NULL) != BF_MATRIX_UNAUTHORIZED) return fail("delegation requires authority");
    BfAuthorityEdge delegate = grant("grant:delegate", "delegate", "institution:aurekai");
    if (bf_authority_grant(authority, &delegate, NULL) != 0 || bf_matrix_institution_delegate(matrix, authority, "person:nick", "institution:aurekai", "institution:plant-collective", "grant:delegate", "2026-08-28T00:00:00+00:00", NULL) != BF_MATRIX_OK) return fail("authority-gated delegation");
    BfAuthorityEdge reverse = grant("grant:reverse", "delegate", "institution:plant-collective"); bf_authority_grant(authority, &reverse, NULL);
    if (bf_matrix_institution_delegate(matrix, authority, "person:nick", "institution:plant-collective", "institution:aurekai", "grant:reverse", "2026-08-28T00:00:00+00:00", NULL) != BF_MATRIX_CYCLE) return fail("institution recursion cannot cycle");
    if (bf_matrix_presence_bind(matrix, &self) != BF_MATRIX_OK) return fail("private presence bind");
    BfMatrixStandingAddressSpec plants_in_life = {"https://aurekai.life/plants", "PlantPassport", "life", BF_MATRIX_SELF, BF_MATRIX_LIVE, "HTML", "receipt:standing:definition"};
    int standing_members = 0;
    if (bf_matrix_standing_address_bind(matrix, &plants_in_life) != BF_MATRIX_OK || bf_matrix_standing_address_member_count(matrix, "https://aurekai.life/plants", &standing_members) != 1 || standing_members != 1 ||
        bf_matrix_presence_bind(matrix, &copy_self) != BF_MATRIX_OK || bf_matrix_standing_address_member_count(matrix, "https://aurekai.life/plants", &standing_members) != 1 || standing_members != 2) return fail("standing address maintains membership without a copied result set");
    if (bf_matrix_address_alias(matrix, "https://luna.aurekai.life/plants/luna", "https://aurekai.life/plants/luna", "receipt:alias") != BF_MATRIX_OK ||
        bf_matrix_address_resolve(matrix, "https://luna.aurekai.life/plants/luna", "life", BF_MATRIX_LIVE, BF_MATRIX_SELF, "HTML", &address_resolution) != 1 ||
        !address_resolution.is_alias || strcmp(address_resolution.canonical_address, "https://aurekai.life/plants/luna") != 0) return fail("alias retains one subject and canonical coordinate");
    if (bf_matrix_presence_project(matrix, authority, "person:nick", "presence:luna:self", &public_live, "grant:publish", "2026-08-28T00:00:00+00:00", NULL) != BF_MATRIX_UNAUTHORIZED) return fail("publication requires authority");
    BfAuthorityEdge publish = grant("grant:publish", "publish", "object:plant:luna"); bf_authority_grant(authority, &publish, NULL);
    if (bf_matrix_presence_project(matrix, authority, "person:nick", "presence:luna:self", &public_live, "grant:publish", "2026-08-28T00:00:00+00:00", NULL) != BF_MATRIX_OK ||
        bf_matrix_presence_project(matrix, authority, "person:nick", "presence:luna:public", &shop, "grant:publish", "2026-08-28T00:00:00+00:00", NULL) != BF_MATRIX_OK ||
        bf_matrix_subject_presence_count(matrix, "object:plant:luna", 1) != 3) return fail("publication accumulates presences, not copies");
    if (bf_matrix_presence_project(matrix, authority, "person:nick", "presence:luna:market", &self, "grant:publish", "2026-08-28T00:00:00+00:00", NULL) != BF_MATRIX_SCOPE_REGRESSION) return fail("publication cannot silently regress a presence");
    if (bf_matrix_presence_withdraw(matrix, authority, "person:nick", "presence:luna:market", "grant:publish", "2026-08-28T00:00:00+00:00", NULL) != BF_MATRIX_OK || bf_matrix_subject_presence_count(matrix, "object:plant:luna", 1) != 2) return fail("withdraw preserves other projections");
    bf_authority_close(authority); bf_matrix_close(matrix); unlink(path);
    printf("matrix_test: PASS\n"); return 0;
}
