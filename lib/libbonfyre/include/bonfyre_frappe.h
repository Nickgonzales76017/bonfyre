/**
 * bonfyre_frappe.h
 *
 * Bonfyre-Frappe Universal Uplift Runtime
 * Core types and interfaces for dependent, stratified, proof-carrying
 * business computation over Frappe application schemas.
 */

#ifndef BONFYRE_FRAPPE_H
#define BONFYRE_FRAPPE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * Core Type System
 * ======================================================================== */

/**
 * Dependent field type representation
 */
typedef enum {
    BF_TYPE_UNIT,          /* Terminal unit type */
    BF_TYPE_STRING,
    BF_TYPE_INT,
    BF_TYPE_FLOAT,
    BF_TYPE_CURRENCY,
    BF_TYPE_DATE,
    BF_TYPE_DATETIME,
    BF_TYPE_TIME,
    BF_TYPE_TEXT,
    BF_TYPE_LONG_TEXT,
    BF_TYPE_MARKDOWN,
    BF_TYPE_HTML,
    BF_TYPE_SELECT,        /* Enum type */
    BF_TYPE_LINK,          /* Reference type */
    BF_TYPE_DYNAMIC_LINK,  /* Dependent reference */
    BF_TYPE_TABLE,         /* List(ChildDocType) */
    BF_TYPE_TABLE_MULTISELECT,
    BF_TYPE_ATTACH,
    BF_TYPE_ATTACH_IMAGE,
    BF_TYPE_CHECK,         /* Boolean */
    BF_TYPE_PASSWORD,
    BF_TYPE_DATA,
    BF_TYPE_BARCODE,
    BF_TYPE_BUTTON,
    BF_TYPE_CODE,
    BF_TYPE_COLOR,
    BF_TYPE_COLUMN_BREAK,
    BF_TYPE_DURATION,
    BF_TYPE_GEOLOCATION,
    BF_TYPE_HEADING,
    BF_TYPE_ICON,
    BF_TYPE_IMAGE,
    BF_TYPE_JSON,
    BF_TYPE_RATING,
    BF_TYPE_READ_ONLY,
    BF_TYPE_SECTION_BREAK,
    BF_TYPE_SIGNATURE,
    BF_TYPE_SMALL_TEXT,
    BF_TYPE_TAB_BREAK,
    BF_TYPE_TEXT_EDITOR,
    BF_TYPE_CURRENCY_DEPENDENT, /* Money(currency_value) */
    BF_TYPE_REFINEMENT,         /* {x : T | P(x)} */
    BF_TYPE_DEPENDENT_SUM,      /* Σ(x : A). B(x) */
    BF_TYPE_DEPENDENT_PRODUCT,  /* Π(x : A). B(x) */
} BfFieldType;

/**
 * DocType as dependent record type
 * Represents: Σ(f₁ : T₁). Σ(f₂ : T₂(f₁)). ... . Unit
 */
typedef struct BfDocType {
    char *name;
    char *module;
    char *app;
    bool is_submittable;
    bool is_child;
    bool is_single;
    bool is_tree;
    bool istable;
    bool editable_grid;
    bool track_changes;
    bool track_seen;
    bool track_views;
    char *autoname;
    char *naming_rule;
    char *title_field;
    char *sort_field;
    char *sort_order;
    bool allow_copy;
    bool allow_import;
    bool allow_rename;
    bool allow_auto_repeat;
    bool read_only;
    bool in_create;
    bool allow_guest_to_view;
    int max_attachments;
    char *description;
    char *documentation;

    /* Fields as dependent sum chain */
    struct BfField *fields;
    size_t field_count;

    /* Relations */
    struct BfRelation *relations;
    size_t relation_count;

    /* Controllers and hooks */
    char *controller_path;
    char **hooks;
    size_t hook_count;

    /* Workflow state machine */
    char *workflow_name;
    struct BfWorkflow *workflow;

    /* Permissions */
    struct BfPermission *permissions;
    size_t permission_count;

    /* Schema version and hash */
    uint64_t schema_version;
    uint8_t schema_hash[32];
} BfDocType;

/**
 * Field as type component in dependent sum
 */
typedef struct BfField {
    char *fieldname;
    char *label;
    BfFieldType fieldtype;
    bool reqd;           /* Mandatory = refinement type */
    bool unique;
    bool read_only;
    bool hidden;
    bool allow_in_quick_entry;
    bool allow_on_submit;
    bool bold;
    bool collapsible;
    bool columns;
    bool depends_on;
    char *depends_on_expr;
    char *default_value;
    char *description;
    char *fetch_from;
    char *fetch_if_empty;
    bool in_filter;
    bool in_global_search;
    bool in_list_view;
    bool in_preview;
    bool in_standard_filter;
    int length;
    bool no_copy;
    char *options;       /* For Select/Link/etc */
    int precision;
    bool print_hide;
    bool report_hide;
    bool search_index;
    bool translatable;
    char *width;

    /* Dynamic link dependency */
    char *dynamic_link_reference_doctype; /* Field that determines target type */

    /* Dependent type context */
    size_t depends_on_field_count;
    char **depends_on_fields;

    /* Position in dependent sum chain */
    size_t position;
} BfField;

/**
 * Relation types in schema graph
 */
typedef enum {
    BF_REL_LINK,
    BF_REL_DYNAMIC_LINK,
    BF_REL_CHILD_TABLE,
    BF_REL_TREE_PARENT,
    BF_REL_ATTACHMENT,
    BF_REL_WORKFLOW_SUBJECT,
    BF_REL_EVENT_SUBJECT,
    BF_REL_PERMISSION_SCOPE,
    BF_REL_SEMANTIC,
    BF_REL_EVIDENCE,
    BF_REL_POLICY,
    BF_REL_RULE_TRIGGER,
    BF_REL_PROOF_DEPENDENCY,
} BfRelationType;

/**
 * Typed relation edge
 */
typedef struct BfRelation {
    BfRelationType type;
    char *source_doctype;
    char *source_field;
    char *target_doctype;
    char *target_field;
    bool cascade_delete;
    bool required;
    char *on_delete;
} BfRelation;

/* ========================================================================
 * Document Terms
 * ======================================================================== */

/**
 * Document as term inhabiting DocType
 * doc : D
 */
typedef struct BfDocument {
    char *doctype;
    char *name;
    char *owner;
    char *creation;
    char *modified;
    char *modified_by;
    int docstatus;  /* 0=Draft, 1=Submitted, 2=Cancelled */
    int idx;

    /* Field values as dependent tuple */
    struct BfFieldValue *values;
    size_t value_count;

    /* Canonical form */
    uint8_t canonical_hash[32];
    bool is_canonical;

    /* Proof context */
    char **fragment_ids;
    size_t fragment_count;
} BfDocument;

/**
 * Field value in document term
 */
typedef struct BfFieldValue {
    char *fieldname;
    BfFieldType type;

    union {
        char *string_val;
        int64_t int_val;
        double float_val;
        bool bool_val;
        struct BfDocument **table_val;  /* For child tables */
        size_t table_count;
    } value;

    /* Dynamic link resolution */
    char *dynamic_doctype; /* For dynamic links */
} BfFieldValue;

/* ========================================================================
 * Schema Graph
 * ======================================================================== */

/**
 * Complete schema graph for a Frappe app
 */
typedef struct BfSchemaGraph {
    char *app_name;
    char *app_version;

    BfDocType **doctypes;
    size_t doctype_count;

    BfRelation **relations;
    size_t relation_count;

    /* Topology information */
    char **modules;
    size_t module_count;

    /* Graph metrics */
    size_t hub_count;
    size_t reference_chain_count;
    size_t sibling_group_count;
} BfSchemaGraph;

/* ========================================================================
 * Binding System (Datalog-style)
 * ======================================================================== */

/**
 * Schema fact for binding derivation
 */
typedef enum {
    BF_FACT_DOCTYPE,
    BF_FACT_FIELD,
    BF_FACT_FIELD_TYPE,
    BF_FACT_MODULE,
    BF_FACT_LINK,
    BF_FACT_HAS_WORKFLOW,
    BF_FACT_HAS_AMENDED_FROM,
    BF_FACT_HAS_ATTACH,
    BF_FACT_HAS_FINANCIAL_FIELD,
    BF_FACT_HAS_CHILD_TABLE,
    BF_FACT_HAS_COMMUNICATION,
} BfFactType;

typedef struct BfSchemaFact {
    BfFactType type;
    char *doctype;
    char *field;
    char *value;
    void *data;
} BfSchemaFact;

/**
 * Binding rule
 */
typedef struct BfBindingRule {
    char *capability_id;

    /* Horn clause conditions */
    BfSchemaFact *conditions;
    size_t condition_count;

    /* Priority for conflict resolution */
    int priority;
} BfBindingRule;

/**
 * Derived binding set
 */
typedef struct BfBinding {
    char *doctype;
    char **capability_ids;
    size_t capability_count;

    /* Derivation proof */
    BfBindingRule **applied_rules;
    size_t applied_rule_count;
} BfBinding;

/* ========================================================================
 * BonfyreRules System
 * ======================================================================== */

/**
 * Rule variable source
 */
typedef enum {
    BF_VAR_FIELD,
    BF_VAR_COMPUTED,
    BF_VAR_RELATION,
    BF_VAR_WORKFLOW,
    BF_VAR_LEDGER,
    BF_VAR_METER,
    BF_VAR_TIME,
    BF_VAR_SEMANTIC,
    BF_VAR_TONE,
    BF_VAR_ENTITY,
    BF_VAR_TAG,
    BF_VAR_AI_OUTPUT,
    BF_VAR_PHYSICS,
    BF_VAR_REASON,
} BfRuleVarSource;

/**
 * Rule variable
 */
typedef struct BfRuleVariable {
    char *variable_id;
    char *name;
    char *label;
    BfRuleVarSource source;
    BfFieldType type;

    char **allowed_operators;
    size_t operator_count;

    bool privacy_restricted;
    bool requires_proof;
    float min_confidence;
    int freshness_seconds;
} BfRuleVariable;

/**
 * Rule action
 */
typedef struct BfRuleAction {
    char *action_id;
    char *name;
    char *label;
    char *capability_id;

    /* Parameters schema */
    BfField *params;
    size_t param_count;

    /* Safety constraints */
    char *required_gate;
    char **effects;
    size_t effect_count;
    int risk_level;  /* 0-10 */
    bool requires_review;

    /* Proof obligations */
    char **proof_claims_required;
    size_t proof_claim_count;
} BfRuleAction;

/**
 * Rule condition tree node
 */
typedef enum {
    BF_COND_LEAF,
    BF_COND_ALL,
    BF_COND_ANY,
    BF_COND_NOT,
} BfConditionType;

typedef struct BfCondition {
    BfConditionType type;

    /* For leaf nodes */
    char *variable_id;
    char *operator;
    char *value;

    /* For composite nodes */
    struct BfCondition **children;
    size_t child_count;
} BfCondition;

/**
 * Rule definition
 */
typedef struct BfRule {
    char *rule_id;
    char *name;
    char *doctype;
    BfCondition *condition;

    BfRuleAction **actions;
    size_t action_count;

    int priority;
    bool enabled;
} BfRule;

/**
 * Rule set
 */
typedef struct BfRuleSet {
    char *rule_set_id;
    char *app;
    char *doctype;
    char *name;
    char *description;

    BfRuleVariable *variables;
    size_t variable_count;

    char **operators;
    size_t operator_count;

    BfRuleAction *actions;
    size_t action_count;

    BfRule *rules;
    size_t rule_count;

    /* Status */
    enum {
        BF_RULE_DRAFT,
        BF_RULE_SIMULATED,
        BF_RULE_REVIEW_REQUIRED,
        BF_RULE_ACTIVE,
        BF_RULE_PAUSED,
        BF_RULE_DEPRECATED,
        BF_RULE_FAILED_TYPECHECK,
        BF_RULE_FAILED_POLICY,
        BF_RULE_ARCHIVED,
    } status;

    char *created_by;
    char *approved_by;
    uint64_t version;
    uint8_t derivation_hash[32];
} BfRuleSet;

/**
 * Rule universe for a DocType
 */
typedef struct BfRuleUniverse {
    char *doctype;

    BfRuleVariable *variables;
    size_t variable_count;

    char **operators;
    size_t operator_count;

    BfRuleAction *actions;
    size_t action_count;

    /* Constraints */
    char **forbidden_fields;
    size_t forbidden_field_count;

    /* Policies */
    char *proof_policy;
    char *activation_policy;
} BfRuleUniverse;

/* ========================================================================
 * Proof System
 * ======================================================================== */

/**
 * Proof claim types
 */
typedef enum {
    BF_CLAIM_INTEGRITY,
    BF_CLAIM_PROVENANCE,
    BF_CLAIM_EXECUTION,
    BF_CLAIM_POLICY,
    BF_CLAIM_SEMANTIC,
    BF_CLAIM_PROBABILISTIC,
    BF_CLAIM_SURFACE,
    BF_CLAIM_MIGRATION,
    BF_CLAIM_RULE_EVALUATION,
    BF_CLAIM_RULE_ACTIVATION,
    BF_CLAIM_PROMOTION,
} BfClaimType;

/**
 * Proof claim
 */
typedef struct BfProofClaim {
    BfClaimType type;
    char *claim_id;
    char *fragment_id;

    /* Claim content */
    char *predicate;
    char **subjects;
    size_t subject_count;

    float confidence;  /* 0.0-1.0 for probabilistic claims */

    /* Timestamp */
    uint64_t timestamp;
} BfProofClaim;

/**
 * Proof term
 */
typedef struct BfProofTerm {
    char *proof_id;

    /* Claim being proven */
    BfProofClaim *claim;

    /* Proof structure */
    enum {
        BF_PROOF_AXIOM,
        BF_PROOF_AND_INTRO,
        BF_PROOF_SEQ_COMPOSE,
        BF_PROOF_WEAKEN,
        BF_PROOF_PROMOTE,
    } proof_type;

    /* Sub-proofs */
    struct BfProofTerm **subproofs;
    size_t subproof_count;

    /* Witness for promotion */
    char *approval_witness;
} BfProofTerm;

/**
 * Proof bundle
 */
typedef struct BfProofBundle {
    char *bundle_id;

    BfProofTerm **proofs;
    size_t proof_count;

    /* Composed claim */
    BfProofClaim **claims;
    size_t claim_count;

    uint8_t bundle_hash[32];
} BfProofBundle;

/* ========================================================================
 * Semantic Substrate
 * ======================================================================== */

/**
 * Semantic substrate manifest
 */
typedef struct BfSemanticSubstrate {
    char *substrate_id;
    char *source_family;

    /* Model configuration */
    char *model_id;
    bool verify_required;
    char *tier_required;

    /* Embedding configuration */
    enum {
        BF_GRAN_DOCUMENT,
        BF_GRAN_PARAGRAPH,
        BF_GRAN_SEGMENT,
        BF_GRAN_SENTENCE,
        BF_GRAN_UTTERANCE,
    } *granularities;
    size_t granularity_count;

    /* Branching policy */
    char *document_branch;
    char *query_branch;
    bool branch_isolation;

    int dims;
    char *normalization;

    /* Vector index config */
    enum {
        BF_INDEX_FLAT,
        BF_INDEX_IVF,
        BF_INDEX_HNSW,
        BF_INDEX_BVH,
    } index_type;

    enum {
        BF_METRIC_COSINE,
        BF_METRIC_L2,
        BF_METRIC_DOT,
    } metric;

    /* Quality policy */
    int default_quality_level;  /* 0-5 */
    bool allow_sae_rerank;
    bool allow_physics_search;
    float min_completeness;
    bool require_model_index_consistency;

    /* Required proof claims */
    BfClaimType *claims;
    size_t claim_count;
} BfSemanticSubstrate;

/* ========================================================================
 * App Pack
 * ======================================================================== */

/**
 * Compiled Frappe app pack
 */
typedef struct BfAppPack {
    char *app_name;
    char *app_version;

    /* Schema and types */
    BfSchemaGraph *schema;
    BfDocType **types;
    size_t type_count;

    /* Families and relations */
    char **families;
    size_t family_count;
    BfRelation **relations;
    size_t relation_count;

    /* Bindings */
    BfBinding **bindings;
    size_t binding_count;

    /* Rule universes */
    BfRuleUniverse **rule_universes;
    size_t rule_universe_count;

    BfRuleSet **rule_sets;
    size_t rule_set_count;

    /* Routes and surfaces */
    char **api_routes;
    size_t route_count;

    char **surfaces;
    size_t surface_count;

    /* Policies */
    char **proof_policies;
    size_t proof_policy_count;

    char **context_policies;
    size_t context_policy_count;

    /* Semantic substrates */
    BfSemanticSubstrate **substrates;
    size_t substrate_count;

    /* Migration recipes */
    char **migrations;
    size_t migration_count;

    /* Scheduler recipes */
    char **scheduler_jobs;
    size_t scheduler_job_count;

    /* Compatibility routes */
    char **compat_routes;
    size_t compat_route_count;

    /* Coverage audit */
    float coverage_score;
    char **gaps;
    size_t gap_count;

    /* Pack hash */
    uint8_t pack_hash[32];
} BfAppPack;

/* ========================================================================
 * Compiler API
 * ======================================================================== */

/**
 * Compiler context
 */
typedef struct BfCompilerCtx BfCompilerCtx;

/**
 * Create a new compiler context
 */
BfCompilerCtx *bf_compiler_create(void);

/**
 * Free compiler context
 */
void bf_compiler_free(BfCompilerCtx *ctx);

/**
 * Phase 0: Generate manifests
 */
int bf_compiler_phase0_inventory(BfCompilerCtx *ctx, const char *bonfyre_root);

/**
 * Phase 1: Build schema graph from Frappe app
 */
BfSchemaGraph *bf_compiler_phase1_schema_graph(
    BfCompilerCtx *ctx,
    const char *app_path
);

/**
 * Phase 2: Elaborate dependent types
 */
int bf_compiler_phase2_type_elaboration(
    BfCompilerCtx *ctx,
    BfSchemaGraph *schema
);

/**
 * Phase 3: Extract signals
 */
int bf_compiler_phase3_signal_extraction(
    BfCompilerCtx *ctx,
    BfSchemaGraph *schema
);

/**
 * Phase 4: Derive bindings (Datalog least fixed-point)
 */
BfBinding **bf_compiler_phase4_binding_derivation(
    BfCompilerCtx *ctx,
    BfSchemaGraph *schema,
    size_t *binding_count
);

/**
 * Phase 5: Type elaboration
 */
int bf_compiler_phase5_capability_types(
    BfCompilerCtx *ctx,
    BfBinding **bindings,
    size_t binding_count
);

/**
 * Phase 6: Proof elaboration
 */
int bf_compiler_phase6_proof_elaboration(
    BfCompilerCtx *ctx,
    BfBinding **bindings,
    size_t binding_count
);

/**
 * Phase 7: Derive rule universes
 */
BfRuleUniverse **bf_compiler_phase7_rule_universe(
    BfCompilerCtx *ctx,
    BfSchemaGraph *schema,
    BfBinding **bindings,
    size_t binding_count,
    size_t *universe_count
);

/**
 * Phase 8: Rule checks
 */
int bf_compiler_phase8_rule_checks(
    BfCompilerCtx *ctx,
    BfRuleSet *rule_set
);

/**
 * Phase 9: Generate routes and surfaces
 */
int bf_compiler_phase9_route_surface_gen(
    BfCompilerCtx *ctx,
    BfSchemaGraph *schema,
    BfBinding **bindings,
    size_t binding_count
);

/**
 * Phase 10: Storage optimization
 */
int bf_compiler_phase10_storage_optimization(
    BfCompilerCtx *ctx,
    BfSchemaGraph *schema
);

/**
 * Phase 11: Semantic substrate build
 */
BfSemanticSubstrate **bf_compiler_phase11_semantic_substrate(
    BfCompilerCtx *ctx,
    BfSchemaGraph *schema,
    BfBinding **bindings,
    size_t binding_count,
    size_t *substrate_count
);

/**
 * Phase 12: Coverage and theorem audit
 */
int bf_compiler_phase12_coverage_audit(
    BfCompilerCtx *ctx,
    BfAppPack *pack
);

/**
 * Compile complete app pack
 */
BfAppPack *bf_compile_app(
    const char *app_path,
    const char *bonfyre_root,
    int *error_code
);

/**
 * Compile app pack up to a specific phase.
 */
BfAppPack *bf_compile_app_to_phase(
    const char *app_path,
    const char *bonfyre_root,
    int max_phase,
    int *error_code
);

/**
 * Free app pack
 */
void bf_app_pack_free(BfAppPack *pack);

/* ========================================================================
 * Canonicalization
 * ======================================================================== */

/**
 * Canonicalize document term
 * Must be terminating and locally confluent
 */
int bf_canon_document(BfDocument *doc, const BfDocType *doctype);

/**
 * Compute canonical hash
 */
int bf_hash_canonical(const BfDocument *doc, uint8_t hash_out[32]);

/* ========================================================================
 * Rule System Runtime
 * ======================================================================== */

/**
 * Evaluate rule condition on document
 */
typedef struct BfRuleEvalResult {
    bool matched;
    BfProofTerm *condition_proof;

    /* Variable bindings */
    char **variable_ids;
    char **variable_values;
    size_t binding_count;
} BfRuleEvalResult;

BfRuleEvalResult *bf_rule_eval_condition(
    const BfRule *rule,
    const BfDocument *doc,
    const BfRuleUniverse *universe
);

/**
 * Generate action plan from rule
 */
typedef struct BfActionPlan {
    BfRuleAction **actions;
    size_t action_count;

    /* Parameter bindings */
    BfFieldValue **params;
    size_t param_count;
} BfActionPlan;

BfActionPlan *bf_rule_gen_action_plan(
    const BfRule *rule,
    const BfRuleEvalResult *eval_result
);

/**
 * Check rule type safety
 */
int bf_rule_typecheck(
    const BfRule *rule,
    const BfRuleUniverse *universe,
    char **error_msg
);

/**
 * Check rule confluence
 */
int bf_rule_check_confluence(
    const BfRuleSet *rule_set,
    char **error_msg
);

/**
 * Check rule termination over abstract states
 */
int bf_rule_check_termination(
    const BfRuleSet *rule_set,
    char **error_msg
);

/* ========================================================================
 * Proof System Runtime
 * ======================================================================== */

/**
 * Construct proof bundle from fragments
 */
BfProofBundle *bf_proof_construct_bundle(
    const char **fragment_ids,
    size_t fragment_count
);

/**
 * Verify proof term
 */
bool bf_proof_verify(const BfProofTerm *proof);

/**
 * Promote probabilistic claim to semantic claim
 */
BfProofTerm *bf_proof_promote(
    const BfProofClaim *probabilistic_claim,
    const char *approval_witness
);

#ifdef __cplusplus
}
#endif

#endif /* BONFYRE_FRAPPE_H */
