#ifndef BF_FORM_H
#define BF_FORM_H

/*
 * FormGraph and AtomicForm: the human boundary as a compiled package.
 *
 * AtomicForm = compile(FormGraph, values, state), never AtomicForm == FormGraph.
 * A FormGraph is the reusable shape (fields, evidence slots, the conditions
 * under which a field is required, authorities, a review gate); compiling it
 * against values resolves each field's requiredness for this submission.
 * submit_ready names every blocker; an AtomicForm never submits itself, and
 * submission stays a human decision.
 *
 * Shapes (JSON; schemas/control/form-graph.cue):
 *   FormGraph  {form_id, title, fields:[{name, kind?, base_required?,
 *               required_when?:{field, equals}, autofill_actor?}],
 *               evidence:[{name, required?, match_kind?, match_name?, match_tags?}],
 *               authority_required?:[..], review_gate?}
 *   values     {field: string}
 *   state      {authorities_present?:[..], review_passed?:bool,
 *               evidence?:{slot: {artifact_digest? | from_twin?}}}
 *
 * Ported from components/BonfyreControlPlane/{form_graph,atomic_forms}.py.archive.
 */
#include "bf_jv.h"
#include <sqlite3.h>

/* Is this field required under these values? 1 yes, 0 no. */
int bf_form_field_required(const bf_jv *field_spec, const bf_jv *values);

/* Compile a FormGraph into an AtomicForm (caller frees). NULL on a malformed graph. */
bf_jv *bf_form_compile(const bf_jv *form_graph, const bf_jv *values, const bf_jv *state);

/* Fill empty fields from actors whose confidence is "verified" (actor_nodes
 * table).  An unverified or unknown actor never fills.  Returns fields filled, -1 on error. */
int bf_form_autofill(bf_jv *atomic, sqlite3 *actors_db);

/* {form_id, ready, blockers:[..]} (caller frees).  Ready only when every
 * required field, evidence slot, authority and review gate is satisfied. */
bf_jv *bf_form_submit_ready(const bf_jv *atomic);

/* EvidenceBindingGraph: fill the AtomicForm's matcher slots from candidates
 * [{digest, kind?, name?, tags?:[..]}] by semantic test over each candidate's
 * own metadata, never by position.  A slot with no matcher binds nothing; a
 * candidate is consumed by one slot; most-constrained slots bind first; the
 * most specific match wins, ties by digest.  Each bound slot gets a
 * binding_witness saying why.  Returns {bound:{slot:{digest,witness}},
 * unbound:[..]} (caller frees), NULL on error.  Ported from
 * components/BonfyreControlPlane/evidence_binding.py.archive. */
bf_jv *bf_form_bind_evidence(bf_jv *atomic, const bf_jv *candidates);

#endif
