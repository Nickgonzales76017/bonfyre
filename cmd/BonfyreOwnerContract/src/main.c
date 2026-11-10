#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include "catalog.h"

static const char *classes[]={
"IDENTITY","SEMANTIC","AUTHORITY","EVIDENCE","CAUSAL","DEPENDENCY","CAPABILITY","PROVIDER",
"PLACEMENT","TEMPORAL","ECONOMIC","INSTITUTIONAL","PROVENANCE","PROJECTION","EXECUTION","LEARNING"};

static int has(const char*s,const char*n){return s&&n&&strstr(s,n);}
static int eq(const char*a,const char*b){return a&&b&&!strcmp(a,b);}
static const char *reviewed_edge(const char*s,const char*d,const char*r){
 if(eq(r,"rung_promotion"))return"SEMANTIC";
 if(eq(r,"undeclared_reporting"))return"EVIDENCE";
 if(eq(r,"intake_mapping"))return"PROJECTION";
 if(eq(r,"verified_autofill"))return"PROJECTION";
 if(eq(r,"eligibility_prefilter"))return"PROJECTION";
 /* A ranked frontier entering a bounded reducer. The ranker contributes
  * order and the plane contributed membership, so what crosses this edge
  * is a capability question -- what may now be computed -- rather than a
  * projection of one owner onto another. */
 if(eq(r,"ranked_frontier_enters_bounded_reduction"))return"CAPABILITY";
 /* The composition engine handing its own search to a reducer that can
  * check it. What crosses is a question about what may be computed, not a
  * projection of one owner onto another. */
 if(eq(r,"composition_question_stated_for_bounded_reduction"))return"CAPABILITY";
 /* A bound grammar's coordinates entering AddressPlane, with donor
  * implementation, authority, bodies and effects dropped. Same shape as
  * every other address-plane edge here: a projection onto the plane, not a
  * grant of anything that arrives with it. */
 if(eq(r,"forwards")&&eq(d,"address-plane"))return"PROJECTION";
 if(eq(r,"typed_handoff_consumer_recheck"))return"PROJECTION";
 if(eq(r,"admission_before_dispatch"))return"DEPENDENCY";
 if(eq(r,"event_frontier_coverage"))return"EVIDENCE";
 if(eq(r,"bounded_read_realization"))return"EXECUTION";
 if(eq(r,"self_measurement"))return"EVIDENCE";
 if(eq(r,"claim_emission"))return"EVIDENCE";
 if(eq(r,"mask_compilation")&&eq(s,"authority-graph")&&eq(d,"address-plane"))return"AUTHORITY";
 if(eq(r,"mask_compilation")&&eq(s,"capability-lattice")&&eq(d,"address-plane"))return"CAPABILITY";
 if(eq(r,"ranked-receipted-material-context-compilation"))return"LEARNING";
 if(eq(r,"bounded-exact-owner-query-feedback"))return"EXECUTION";
 if(eq(r,"measurement_input"))return"EVIDENCE";
 if(eq(r,"unreconciled_to_work"))return"DEPENDENCY";
 if(eq(r,"monotonicity_withdrawn"))return"SEMANTIC";
 if(eq(r,"durable_submission"))return"EXECUTION";
 if(eq(r,"post_commit_typed_change_notice"))return"TEMPORAL";
 if(eq(r,"native_command_family_materialization"))return"CAPABILITY";
 /* The estate side of the same materialization. command-law gives the
  * composition lattice its families from declared binaries; this gives it
  * material from retained bodies. Both answer "what can be composed now",
  * which is the capability question, not a projection of one thing onto
  * another. */
 if(eq(r,"retained_representation_becomes_composable_material"))return"CAPABILITY";
 if(eq(r,"bridge_census"))return"EVIDENCE";
 if(eq(r,"separate_decision"))return"DEPENDENCY";
 if(eq(r,"scope-bounded-review-mission"))return"INSTITUTIONAL";
 if(eq(r,"invalidation_precision"))return"TEMPORAL";
 if(eq(r,"public_host_reachability_candidate"))return"PROJECTION";
 if(eq(r,"host-selected-arrival-character"))return"PROJECTION";
 if(eq(r,"ladder_production"))return"SEMANTIC";
 if(eq(r,"failure_must_reach_the_caller"))return"EVIDENCE";
 if(eq(r,"source_bound_household_comparison"))return"ECONOMIC";
 if(eq(r,"ternary_compilation"))return"EVIDENCE";
 if(eq(r,"binding_condition_to_work"))return"DEPENDENCY";
 if(eq(r,"rights_limited_observation_input"))return"PROVENANCE";
 if(eq(r,"namespace_to_grammar"))return"SEMANTIC";
 if(eq(r,"owner_rechecked_terminal_reconciliation"))return"EVIDENCE";
 if(eq(r,"bounded-owner-aperture"))return"PROJECTION";
 if(eq(r,"push_notification_coalescence"))return"TEMPORAL";
 if(eq(r,"retained-foreign-body-intersection"))return"PROJECTION";
 if(eq(r,"resolved_location_candidate"))return"PROJECTION";
 if(eq(r,"factorized_subject_and_owner_topology"))return"INSTITUTIONAL";
 if(eq(r,"config-gated-native-address-cut"))return"PROJECTION";
 if(eq(r,"representation_reference_resolution"))return"IDENTITY";
 if(eq(r,"native-schema-bound-human-workflow"))return"INSTITUTIONAL";
 if(eq(r,"universal_participation"))return"CAPABILITY";
 if(eq(r,"shared_implementation"))return"DEPENDENCY";
 if(eq(r,"verdict_input"))return"EVIDENCE";
 if(eq(r,"admits_through"))return"DEPENDENCY";
 /* Classified by the precedent above rather than by its own author:
  * admission_before_dispatch, admits_through and reachability_gate are all
  * DEPENDENCY, and a bounded transport lease is the same shape -- delivery
  * cannot proceed until MediaLibrary is rechecked. If MediaDelivery meant
  * this edge to carry the right itself rather than the check, it belongs in
  * AUTHORITY and this line is wrong. */
 if(eq(r,"bounded_transport_lease_admission"))return"DEPENDENCY";
 if(eq(r,"reachability_gate"))return"DEPENDENCY";
 if(eq(r,"bounded_divergence_report"))return"EVIDENCE";
 if(eq(r,"residual_to_work"))return"DEPENDENCY";
 if(eq(r,"bounded_capacity_recovery"))return"CAUSAL";
 if(eq(r,"required_postcondition_realization"))return"DEPENDENCY";
 if(eq(r,"conversion_gate"))return"INSTITUTIONAL";
 if(eq(r,"realization_residue"))return"DEPENDENCY";
 if(eq(r,"index_stamping"))return"PROVENANCE";
 if(eq(r,"normalized_web_address_context"))return"PROJECTION";
 return 0;
}
static const char *edge(const char*s,const char*d,const char*r){
 if(!r||!*r)return 0;
 const char*reviewed=reviewed_edge(s,d,r);
 if(reviewed)return reviewed;
 if(has(r,"authority")||has(r,"mutation_gate")||has(r,"gates_disclosure")||has(r,"oauth-session")||has(r,"delegated_")||has(r,"revocable_"))return"AUTHORITY";
 if(has(r,"evidence")||has(r,"verification")||has(r,"falsifi")||has(r,"proof")||has(r,"attestation")||has(r,"content_integrity"))return"EVIDENCE";
 if(has(r,"economic")||has(r,"spend")||has(r,"quoted_capability")||has(r,"valuation")||has(r,"bid_supply")||has(r,"commitment")||has(r,"settle"))return"ECONOMIC";
 if(has(r,"learned")||has(r,"ranking")||has(r,"reorders")||has(r,"prediction_scoring")||has(r,"clustering"))return"LEARNING";
 if(has(r,"incremental_maintenance")||has(r,"change_notification")||has(r,"resume_typestate")||has(r,"effect_time")||has(r,"bitemporal")||has(r,"supersession")||has(r,"expiry"))return"TEMPORAL";
 if(has(r,"identifier_lineage")||has(r,"canonical_match")||has(r,"identity")||has(r,"equivalence"))return"IDENTITY";
 if(has(r,"placement")||has(r,"residency")||has(r,"width_admission"))return"PLACEMENT";
 if(has(r,"provider")||has(r,"realization_routing")||has(r,"service_binding")||has(r,"mechanism_binding")||!strcmp(r,"supplies"))return"PROVIDER";
 if(has(r,"capability")||has(r,"activation_mechanism")||has(r,"realized_participation")||!strcmp(r,"selects"))return"CAPABILITY";
 if(has(r,"blocker_resolution")||has(r,"activation_gate")||has(r,"capacity_unlock")||has(r,"sufficiency_consequence")||has(r,"triggers")||has(r,"removes"))return"CAUSAL";
 if(has(r,"dependency")||has(r,"constraint")||has(r,"requires_")||has(r,"binding_source")||has(r,"residual_to_target")||has(r,"preservation_loss_to_work")||!strcmp(r,"reads")||!strcmp(r,"demands"))return"DEPENDENCY";
 if(has(r,"provenance")||has(r,"receipt_projection")||has(r,"replay")||has(r,"derived_from")||has(r,"source_tree")||has(r,"observation_envelope")||has(r,"owner_reference"))return"PROVENANCE";
 if(has(r,"execution")||has(r,"carrier")||has(r,"invocation")||has(r,"route_to_transformation")||has(r,"durable_realization")||has(r,"delivered_event")||has(r,"effect_")||has(r,"realized_measurement"))return"EXECUTION";
 if(has(r,"project")||has(r,"projection")||has(r,"lowering")||has(r,"renders")||has(r,"publication")||has(r,"surface_")||has(r,"candidate_")||has(r,"frontend")||has(r,"context_delivery"))return"PROJECTION";
 if(has(r,"institution")||has(r,"workroom")||has(r,"partner_review")||has(r,"role_scope")||has(r,"human_workflow"))return"INSTITUTIONAL";
 if(has(r,"semantic")||has(r,"typing")||has(r,"generalisation")||has(r,"specializes")||has(r,"refines")||has(r,"predicate")||has(r,"classification")||has(r,"law_realization")||has(r,"compile")||has(r,"deriv")||has(r,"narrows")||has(r,"reduction"))return"SEMANTIC";
 if(!strcmp(r,"projects")||!strcmp(r,"renders"))return"PROJECTION";
 if(!strcmp(r,"carrier"))return"EXECUTION";
 if(!strcmp(r,"measures"))return"EVIDENCE";
 if(!strcmp(r,"composition"))return"SEMANTIC";
 return 0;
}
static void val(const char*l,char*out,size_t n){
 const char*p=strchr(l,':'); if(!p){*out=0;return;} p++;
 while(*p==' '||*p=='"')p++; size_t i=0;
 while(*p&&*p!='"'&&*p!='\n'&&i+1<n)out[i++]=*p++; out[i]=0;
}

typedef struct { int total, unresolved, classes[16]; } OwnerClassification;

static int classify_summary(const char *atlas, OwnerClassification *out){
 FILE*f;char l[32768],rel[256]="",src[256]="",dst[256]="";int in=0;
 if(!atlas||!out)return 0;memset(out,0,sizeof(*out));
 if(!(f=fopen(atlas,"r")))return 0;
 while(fgets(l,sizeof l,f)){
  if(!in){if(strstr(l,"\"interactions\": {"))in=1;continue;}
  if(strstr(l,"\"phenotypes\": {"))break;
  if(strstr(l,"\"destination\":"))val(l,dst,sizeof dst);
  else if(strstr(l,"\"relation\":"))val(l,rel,sizeof rel);
  else if(strstr(l,"\"source\":")){
   val(l,src,sizeof src);
   if(*rel){const char*k;out->total++;k=edge(src,dst,rel);if(!k)out->unresolved++;else for(int i=0;i<16;i++)if(!strcmp(k,classes[i]))out->classes[i]++;*rel=*src=*dst=0;}
  }
 }
 fclose(f);return 1;
}

static void json_string(FILE *f,const char *s){
 fputc('"',f);for(;s&&*s;s++){unsigned char c=(unsigned char)*s;switch(c){case '\\':fputs("\\\\",f);break;case '"':fputs("\\\"",f);break;case '\n':fputs("\\n",f);break;case '\r':fputs("\\r",f);break;case '\t':fputs("\\t",f);break;default:if(c>=0x20)fputc(c,f);else fprintf(f,"\\u%04x",c);}}fputc('"',f);
}

/* A sealed, disposable reference image.  Atlas and Capability remain owners;
 * consumers use this only to reject a stale/unreviewed composition input. */
static int write_image(const char *atlas,const char *capbin,const char *output){
 OwnerClassification classification;BfOwnerCatalogSummary catalog;char tmp[4096];int fd;FILE*f;
 if(!atlas||!capbin||!output||!classify_summary(atlas,&classification)||!catalog_summary(atlas,capbin,&catalog)||classification.unresolved||catalog.realized_unmapped)return 0;
 if(snprintf(tmp,sizeof(tmp),"%s.tmp.XXXXXX",output)>=(int)sizeof(tmp)||(fd=mkstemp(tmp))<0)return 0;
 if(!(f=fdopen(fd,"wb"))){close(fd);unlink(tmp);return 0;}
 fputs("{\"schema\":\"bonfyre-owner-contract-image.v1\",\"state\":\"compiled_reference_image\",\"atlas\":{\"path\":",f);json_string(f,atlas);
 fprintf(f,",\"interactions\":%d,\"unresolved\":0,\"classes\":{",classification.total);
 for(int i=0;i<16;i++)fprintf(f,"%s\"%s\":%d",i?",":"",classes[i],classification.classes[i]);
 fputs("}},\"capability_registry\":{\"path\":",f);json_string(f,capbin);
 fprintf(f,",\"rows\":%d,\"atlas_architectures\":%d,\"canonical_realizations\":%d,\"operation_aliases\":%d,\"source_linked_realizations\":%d,\"multi_owner_realizations\":%d,\"external_realizations\":%d,\"archives\":%d,\"declared_unrealized\":%d,\"realized_unmapped\":0},",catalog.capability_rows,catalog.atlas_architectures,catalog.canonical_realizations,catalog.operation_aliases,catalog.source_linked_realizations,catalog.multi_owner_realizations,catalog.external_realizations,catalog.archives,catalog.declared_unrealized);
 fputs("\"references\":{\"representation_passports\":\"AddressPlane:CandidateAdvertisement->RepresentationPassport\",\"exact_gates\":[\"AuthorityGraph:exact\",\"InformationPolicy:exact\",\"ResourceAdmission:exact\",\"EffectKernel:exact\"],\"consumers\":[\"DisCIPL\",\"Runtime\",\"AddressPlane\",\"LowererWorkers\"]},\"cannot_infer\":[\"compiled image -> capability invocation admitted\",\"physical realization -> authority or effect\",\"representation passport -> selected execution plan\"]}\n",f);
 if(fflush(f)||fsync(fileno(f))||fclose(f)){unlink(tmp);return 0;}
 if(rename(tmp,output)){unlink(tmp);return 0;}
 printf("{\"owner_contract_image\":");json_string(stdout,output);printf(",\"interactions\":%d,\"unresolved\":0,\"declared_unrealized\":%d,\"truth\":\"reference_image_not_invocation_authority\"}\n",classification.total,catalog.declared_unrealized);
 return 1;
}

static int verify_image(const char *path){
 FILE*f;char body[8192];size_t n;
 if(!path||!(f=fopen(path,"rb")))return 0;n=fread(body,1,sizeof(body)-1,f);fclose(f);body[n]='\0';
 if(!strstr(body,"\"schema\":\"bonfyre-owner-contract-image.v1\"")||!strstr(body,"\"unresolved\":0")||!strstr(body,"\"realized_unmapped\":0"))return 0;
 printf("{\"owner_contract_image\":");json_string(stdout,path);puts(",\"state\":\"accepted_reference_image\",\"truth\":\"exact_gates_still_required\"}");return 1;
}

int main(int ac,char**av){
 if(ac>1&&!strcmp(av[1],"image")){const char*out=NULL;for(int i=4;i+1<ac;i++)if(!strcmp(av[i],"--out"))out=av[i+1];if(ac<5||!out){fprintf(stderr,"usage: bonfyre-owner-contract image ATLAS CAPABILITY_BIN --out FILE\n");return 2;}return write_image(av[2],av[3],out)?0:1;}
 if(ac>1&&!strcmp(av[1],"verify-image")){if(ac!=3){fprintf(stderr,"usage: bonfyre-owner-contract verify-image IMAGE\n");return 2;}return verify_image(av[2])?0:1;}
 if(ac>1&&!strcmp(av[1],"audit-catalog"))return catalog_audit(ac,av);
 if(ac<3||strcmp(av[1],"classify")){fprintf(stderr,"usage: bonfyre-owner-contract classify ATLAS [--summary]\n");return 2;}
 int summary=ac>3&&!strcmp(av[3],"--summary"),in=0,total=0,unknown=0,cnt[16]={0};
 FILE*f=fopen(av[2],"r"); if(!f){perror(av[2]);return 2;}
 char l[32768],rel[256]="",src[256]="",dst[256]="";
 while(fgets(l,sizeof l,f)){
  if(!in){if(strstr(l,"\"interactions\": {"))in=1;continue;}
  if(strstr(l,"\"phenotypes\": {"))break;
  if(strstr(l,"\"destination\":"))val(l,dst,sizeof dst);
  else if(strstr(l,"\"relation\":"))val(l,rel,sizeof rel);
  else if(strstr(l,"\"source\":")){
   val(l,src,sizeof src);
   if(*rel){
    total++; const char*k=edge(src,dst,rel);
    if(!k){unknown++; if(!summary)printf("UNRESOLVED\t%s\t%s\t%s\n",src,dst,rel);}
    else {for(int i=0;i<16;i++)if(!strcmp(k,classes[i]))cnt[i]++; if(!summary)printf("%s\t%s\t%s\t%s\n",k,src,dst,rel);}
    *rel=*src=*dst=0;
   }
  }
 }
 fclose(f);
 printf("{\"schema\":\"bonfyre-owner-contract-classification.v1\",\"total\":%d,\"unresolved\":%d,\"classes\":{",total,unknown);
 for(int i=0;i<16;i++)printf("%s\"%s\":%d",i?",":"",classes[i],cnt[i]);
 printf("}}\n");
 return unknown?1:0;
}
