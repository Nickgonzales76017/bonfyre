/* Generated from architecture/institutional/namespaces.yaff.
 *
 * Do not edit. Change the contract and re-run
 * cmd/BonfyreAtlas/bonfyre-atlas compile-contracts institutions.
 *
 * Cannot infer: a declared estate -> a granted one. This names which estates
 * exist; whether an account may enter one is the grant's answer.
 */
#ifndef BF_INSTITUTIONS_H
#define BF_INSTITUTIONS_H

#define BF_INSTITUTION_ROOTS     { "bonfyre", "child", "second", "umbrella" }
#define BF_INSTITUTION_ROOT_N    4

#define BF_MANAGED_ESTATES       { "alpha", "child" }
#define BF_MANAGED_ESTATE_N      2

/* Every estate an account can hold a grant in. `bonfyre` is not here: it is the
 * superuser closure, not one slice among several. */
#define BF_SLICE_ESTATES         { "alpha", "child", "second", "umbrella" }
#define BF_SLICE_ESTATE_N        4

/* The institutional ancestry graph. A parent grant opens its declared
 * descendants; a child grant never climbs to a parent or crosses to a sibling.
 * Deliberately a small closed graph, not a prefix convention. */
#define BF_INSTITUTION_PARENTS   { {"alpha", "umbrella"}, {"child", "explicit-parent"}, {"umbrella", "bonfyre"} }
#define BF_INSTITUTION_PARENT_N  3

/* The role a parent's leadership inherits in each managed estate. A managed
 * estate declared without one inherits an aperture and no role, which is two
 * authority organs disagreeing rather than a smaller grant. */
#define BF_INHERITED_ROLES       { {"child", "child-admin"} }
#define BF_INHERITED_ROLE_N      1

/* read < write < admin. A rank decides whether a held grant satisfies a
 * required role. */
#define BF_ROLE_RANKS            { {"read", 0}, {"reader", 0}, {"write", 1}, {"admin", 2} }
#define BF_ROLE_RANK_N           4

/* The delegated parent and the tuple its leadership is held under. The vertical
 * organ asserted all four as literals, so "who leads a delegated vertical" was
 * answered in C rather than by the contract, and a second delegated parent
 * meant editing an organ. Upper90 remains a real institution; it is no longer
 * the type of every delegated institution. */
#define BF_DELEGATED_PARENT      "umbrella"
#define BF_DELEGATED_LEAD_ROLE   "lead"
#define BF_DELEGATED_LEAD_PERM   "private:read"
#define BF_DELEGATED_LEAD_OBJECT "workspace"
#define BF_DELEGATED_LEAD_SCOPE  "institution:umbrella"

/* The world each institution executes in. A DEMO institution admits real
 * public observation and never a live external effect against a third party,
 * so an organ that can raise an external effect has to be able to ask. An
 * institution with no declared world is LIVE: forgetting must fail toward
 * real, never toward silently sandboxed. */
#define BF_INSTITUTION_WORLDS    { {"bonfyre", "LIVE"}, {"child", "DEMO"}, {"second", "LIVE"}, {"umbrella", "LIVE"} }
#define BF_INSTITUTION_WORLD_N   4

#endif /* BF_INSTITUTIONS_H */
