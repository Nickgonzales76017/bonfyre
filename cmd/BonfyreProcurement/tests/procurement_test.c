#include "procurement.h"
#include "bf_jv.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *H="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

static bf_jv *purchase(int count,int quantity) {
    bf_jv *p=bf_jv_obj(),*source=bf_jv_obj(),*lines=bf_jv_arr(); long long total=0;
    bf_jv_set(p,"schema",bf_jv_str("service-site.room-purchase-list.v1"));bf_jv_set(p,"id",bf_jv_str("purchase-12345678"));
    bf_jv_set(p,"state",bf_jv_str("draft"));bf_jv_set(p,"currency",bf_jv_str("USD"));bf_jv_set(p,"sha256",bf_jv_str(H));bf_jv_set(p,"commitment",bf_jv_null());
    bf_jv_set(source,"key",bf_jv_str("fixture-plan:room"));bf_jv_set(source,"revision",bf_jv_num(7));bf_jv_set(source,"sha256",bf_jv_str(H));bf_jv_set(p,"source",source);
    for(int i=0;i<count;i++){
        char product[40],variant[40],url[128],label[80];snprintf(product,sizeof product,"product-%d",i);snprintf(variant,sizeof variant,"variant-%d",i);
        snprintf(url,sizeof url,"https://merchant.example/items/%d",i);snprintf(label,sizeof label,"Chair %d — café",i);
        bf_jv *l=bf_jv_obj();bf_jv_set(l,"product_id",bf_jv_str(product));bf_jv_set(l,"variant_id",bf_jv_str(variant));bf_jv_set(l,"label",bf_jv_str(label));
        bf_jv_set(l,"option",bf_jv_str("Walnut"));bf_jv_set(l,"source_url",bf_jv_str(url));bf_jv_set(l,"source_sha256",bf_jv_str(H));
        bf_jv_set(l,"observed_at",bf_jv_str("2026-10-23T12:00:00Z"));bf_jv_set(l,"unit_cents",bf_jv_num(1000+i));bf_jv_set(l,"quantity",bf_jv_num(quantity));
        bf_jv_set(l,"subtotal_cents",bf_jv_num((1000+i)*quantity));bf_jv_push(lines,l);total+=(1000+i)*quantity;
    }
    bf_jv_set(p,"lines",lines);bf_jv_set(p,"subtotal_cents",bf_jv_num((double)total));return p;
}

static char *rfq_input(const bf_jv *p,const bf_jv *selected,const bf_jv *supplier,const char *at) {
    bf_jv *r=bf_jv_obj();bf_jv_set(r,"purchase_list",bf_jv_clone(p));bf_jv_set(r,"purchase_list_revision",bf_jv_num(3));bf_jv_set(r,"id",bf_jv_str("rfq-12345678"));
    bf_jv_set(r,"requested_at",bf_jv_str(at?at:"2026-10-23T13:00:00Z"));bf_jv_set(r,"supplier",supplier?bf_jv_clone(supplier):bf_jv_null());bf_jv_set(r,"selected",selected?bf_jv_clone(selected):bf_jv_null());
    char *s=bf_jv_dump(r,0,BF_JV_SORT_NONE,NULL);bf_jv_free(r);return s;
}

static bf_jv *run_rfq(const bf_jv *p) {
    char err[256],*in=rfq_input(p,NULL,NULL,NULL),*out=NULL;assert(bf_procurement_rfq(in,strlen(in),&out,err,sizeof err));
    bf_jv *v=bf_jv_parse(out,strlen(out),err,sizeof err);free(in);free(out);assert(v);return v;
}

static char *quote_input(const bf_jv *rfq,const char *currency,int partial,int total_only,int substitution,const bf_jv *supplier,const char *valid_until) {
    const bf_jv *rlines=bf_jv_get(rfq,"lines");bf_jv *root=bf_jv_obj(),*o=bf_jv_obj(),*lines=bf_jv_arr();
    bf_jv_set(root,"rfq",bf_jv_clone(rfq));bf_jv_set(o,"id",bf_jv_str(currency[0]=='E'?"quote-eur-1234":"quote-usd-1234"));bf_jv_set(o,"supplier",supplier?bf_jv_clone(supplier):bf_jv_null());
    bf_jv_set(o,"supplier_quote_id",bf_jv_null());bf_jv_set(o,"observed_at",bf_jv_str("2026-10-23T14:00:00Z"));bf_jv_set(o,"quoted_at",bf_jv_null());
    bf_jv_set(o,"valid_until",valid_until?bf_jv_str(valid_until):bf_jv_str("2026-10-25T00:00:00Z"));bf_jv_set(o,"currency",bf_jv_str(currency));bf_jv_set(o,"source_ref",bf_jv_str("file:quote.pdf"));
    bf_jv_set(o,"freight_cents",bf_jv_null());bf_jv_set(o,"tax_cents",bf_jv_null());bf_jv_set(o,"other_fees_cents",bf_jv_num(0));
    size_t take=partial?1:rlines->len;
    for(size_t i=0;i<take;i++){
        const bf_jv *r=bf_jv_at(rlines,i);bf_jv *l=bf_jv_obj();char id[32];snprintf(id,sizeof id,"quote-line-%zu",i);
        bf_jv_set(l,"id",bf_jv_str(id));bf_jv_set(l,"rfq_line_id",bf_jv_clone(bf_jv_get(r,"line_id")));bf_jv_set(l,"quantity",bf_jv_clone(bf_jv_get(r,"quantity_requested")));
        if(total_only){bf_jv_set(l,"unit_cents",bf_jv_null());bf_jv_set(l,"line_total_cents",bf_jv_num(5000));}
        else{bf_jv_set(l,"unit_cents",bf_jv_num(900));bf_jv_set(l,"line_total_cents",bf_jv_null());}
        if(substitution&&i==0){bf_jv *s=bf_jv_obj();bf_jv_set(s,"source_url",bf_jv_str("https://merchant.example/sub"));bf_jv_set(s,"product_id",bf_jv_str("sub-product"));bf_jv_set(s,"variant_id",bf_jv_str("sub-variant"));bf_jv_set(s,"source_sha256",bf_jv_str(H));bf_jv_set(l,"substitution",s);}else bf_jv_set(l,"substitution",bf_jv_null());
        bf_jv_set(l,"availability",bf_jv_null());bf_jv_set(l,"lead_time",bf_jv_null());bf_jv_push(lines,l);
    }
    bf_jv_set(o,"lines",lines);bf_jv_set(root,"observation",o);char *s=bf_jv_dump(root,0,BF_JV_SORT_NONE,NULL);bf_jv_free(root);return s;
}

static bf_jv *run_quote(const bf_jv *rfq,const char *currency,int partial,int total_only,int substitution) {
    char err[256],*in=quote_input(rfq,currency,partial,total_only,substitution,NULL,NULL),*out=NULL;assert(bf_procurement_quote(in,strlen(in),&out,err,sizeof err));
    bf_jv *v=bf_jv_parse(out,strlen(out),err,sizeof err);free(in);free(out);assert(v);return v;
}

static char *compare_input(const bf_jv *rfq,const bf_jv *a,const bf_jv *b) {
    bf_jv *root=bf_jv_obj(),*quotes=bf_jv_arr();bf_jv_set(root,"rfq",bf_jv_clone(rfq));bf_jv_push(quotes,bf_jv_clone(a));if(b)bf_jv_push(quotes,bf_jv_clone(b));bf_jv_set(root,"quotes",quotes);
    char *in=bf_jv_dump(root,0,BF_JV_SORT_NONE,NULL);bf_jv_free(root);return in;
}

static bf_jv *run_compare(const bf_jv *rfq,const bf_jv *a,const bf_jv *b,const char *asof) {
    char *in=compare_input(rfq,a,b),*out=NULL,err[256];assert(bf_procurement_compare(in,strlen(in),asof,&out,err,sizeof err));
    bf_jv *v=bf_jv_parse(out,strlen(out),err,sizeof err);free(in);free(out);assert(v);return v;
}

static void expect_bad_rfq(const bf_jv *p) {
    char err[256],*in=rfq_input(p,NULL,NULL,NULL),*out=NULL;assert(!bf_procurement_rfq(in,strlen(in),&out,err,sizeof err));free(in);free(out);
}

static void reseal(bf_jv *v) {
    char digest[65];bf_jv_del(v,"digest");bf_jv_digest(v,digest);bf_jv_set(v,"digest",bf_jv_str(digest));
}

static bf_jv *quote_variant(const bf_jv *base,const char *id,const char *supplier_id,const char *supplier_name,long long quantity,long long unit,const char *valid_until) {
    bf_jv *q=bf_jv_clone(base),*supplier=bf_jv_obj(),*line=bf_jv_at(bf_jv_get(q,"lines"),0);char line_id[128];
    bf_jv_set(q,"id",bf_jv_str(id));bf_jv_set(supplier,"id",bf_jv_str(supplier_id));bf_jv_set(supplier,"name",bf_jv_str(supplier_name));bf_jv_set(q,"supplier",supplier);
    bf_jv_set(q,"source_ref",bf_jv_str(id));bf_jv_set(q,"valid_until",bf_jv_str(valid_until));bf_jv_set(q,"freight_cents",bf_jv_num(0));bf_jv_set(q,"tax_cents",bf_jv_num(0));bf_jv_set(q,"other_fees_cents",bf_jv_num(0));
    snprintf(line_id,sizeof line_id,"%s-line",id);bf_jv_set(line,"id",bf_jv_str(line_id));bf_jv_set(line,"quantity",bf_jv_num((double)quantity));bf_jv_set(line,"unit_cents",bf_jv_num((double)unit));bf_jv_set(line,"line_total_cents",bf_jv_null());bf_jv_set(line,"known_extended_cents",bf_jv_num((double)(quantity*unit)));
    reseal(q);return q;
}

static char *allocation_input(const bf_jv *rfq,const bf_jv *a,const bf_jv *b,const bf_jv *comparison,const char *at) {
    bf_jv *root=bf_jv_obj(),*quotes=bf_jv_arr();bf_jv_set(root,"rfq",bf_jv_clone(rfq));bf_jv_push(quotes,bf_jv_clone(a));if(b)bf_jv_push(quotes,bf_jv_clone(b));bf_jv_set(root,"quotes",quotes);
    bf_jv_set(root,"comparison",bf_jv_clone(comparison));bf_jv_set(root,"id",bf_jv_str("allocation-12345678"));bf_jv_set(root,"prepared_at",bf_jv_str(at));bf_jv_set(root,"policy",bf_jv_str("lowest_known_merchandise"));
    char *out=bf_jv_dump(root,0,BF_JV_SORT_NONE,NULL);bf_jv_free(root);return out;
}

static bf_jv *run_allocate(const bf_jv *rfq,const bf_jv *a,const bf_jv *b,const bf_jv *comparison,const char *at) {
    char err[256],*in=allocation_input(rfq,a,b,comparison,at),*out=NULL;assert(bf_procurement_allocate(in,strlen(in),&out,err,sizeof err));
    bf_jv *v=bf_jv_parse(out,strlen(out),err,sizeof err);free(in);free(out);assert(v);return v;
}

static char *approval_input(const bf_jv *allocation,long long revision,const char *id) {
    bf_jv *root=bf_jv_obj();bf_jv_set(root,"allocation",bf_jv_clone(allocation));bf_jv_set(root,"allocation_revision",bf_jv_num((double)revision));bf_jv_set(root,"id",bf_jv_str(id));
    bf_jv_set(root,"requested_at",bf_jv_str("2026-10-24T00:01:00Z"));bf_jv_set(root,"requester",bf_jv_str("user:19"));bf_jv_set(root,"reason",bf_jv_str("Review exact allocation"));
    char *out=bf_jv_dump(root,0,BF_JV_SORT_NONE,NULL);bf_jv_free(root);return out;
}

int main(void) {
    char err[256],*out1=NULL,*out2=NULL;
    bf_jv *p=purchase(2,2);char *in=rfq_input(p,NULL,NULL,NULL);
    assert(bf_procurement_rfq(in,strlen(in),&out1,err,sizeof err));assert(bf_procurement_rfq(in,strlen(in),&out2,err,sizeof err));assert(!strcmp(out1,out2));
    bf_jv *reordered=bf_jv_parse(in,strlen(in),err,sizeof err);bf_jv_sort_keys(reordered,BF_JV_SORT_BYTES);char *reordered_in=bf_jv_dump(reordered,0,BF_JV_SORT_NONE,NULL),*out3=NULL;
    assert(bf_procurement_rfq(reordered_in,strlen(reordered_in),&out3,err,sizeof err));assert(!strcmp(out1,out3));bf_jv_free(reordered);free(reordered_in);free(out3);
    free(in);free(out1);free(out2);

    bf_jv *rfq=run_rfq(p);assert(!strcmp(bf_jv_get(rfq,"status")->s,"prepared"));assert(bf_jv_get(rfq,"lines")->len==2);
    assert(!bf_jv_get(rfq,"supplier_contacted")->b&&!bf_jv_get(rfq,"purchase_approved")->b&&!bf_jv_get(rfq,"order_committed")->b);

    bf_jv *sel=bf_jv_arr();bf_jv_push(sel,bf_jv_clone(bf_jv_get(bf_jv_at(bf_jv_get(rfq,"lines"),0),"line_id")));in=rfq_input(p,sel,NULL,NULL);
    assert(bf_procurement_rfq(in,strlen(in),&out1,err,sizeof err));bf_jv *one=bf_jv_parse(out1,strlen(out1),err,sizeof err);assert(bf_jv_get(one,"lines")->len==1);free(in);free(out1);bf_jv_free(one);bf_jv_free(sel);

    bf_jv *q=run_quote(rfq,"USD",0,1,0);assert(bf_jv_is(bf_jv_get(q,"freight_cents"),BF_JV_NULL));assert(bf_jv_is(bf_jv_get(q,"tax_cents"),BF_JV_NULL));
    assert(!bf_jv_get(q,"quote_accepted")->b&&!bf_jv_get(q,"reserved")->b&&!bf_jv_get(q,"order_authorized")->b);

    bf_jv *cmp=run_compare(rfq,q,NULL,"2026-10-26T00:00:00Z");assert(!strcmp(bf_jv_get(cmp,"status")->s,"partial"));
    assert(bf_jv_get(bf_jv_at(bf_jv_get(cmp,"quotes"),0),"expired")->b);assert((int)bf_jv_n(bf_jv_get(cmp,"eligible_quote_count"),-1)==0);assert((int)bf_jv_n(bf_jv_get(cmp,"expired_quote_count"),-1)==1);
    assert(bf_jv_n(bf_jv_get(bf_jv_at(bf_jv_get(cmp,"coverage"),0),"uncovered_quantity"),-1)>0);assert(!bf_jv_get(cmp,"approved")->b&&!bf_jv_get(cmp,"committed")->b&&!bf_jv_get(cmp,"ordered")->b);bf_jv_free(cmp);

    bf_jv *partial=run_quote(rfq,"USD",1,0,0);cmp=run_compare(rfq,partial,NULL,NULL);assert(!strcmp(bf_jv_get(cmp,"status")->s,"partial"));bf_jv_free(cmp);
    bf_jv *eur=run_quote(rfq,"EUR",0,0,0);cmp=run_compare(rfq,q,eur,NULL);assert(!strcmp(bf_jv_get(cmp,"status")->s,"mixed_currency"));assert(bf_jv_get(cmp,"mixed_currency")->b);bf_jv_free(cmp);
    bf_jv *sub=run_quote(rfq,"USD",0,0,1);cmp=run_compare(rfq,sub,NULL,NULL);assert(bf_jv_n(bf_jv_get(bf_jv_at(bf_jv_get(cmp,"coverage"),0),"uncovered_quantity"),-1)>0);assert(bf_jv_get(bf_jv_at(bf_jv_get(cmp,"coverage"),0),"substitutions")->len==1);bf_jv_free(cmp);

    char *cin=compare_input(rfq,q,NULL);bf_jv *croot=bf_jv_parse(cin,strlen(cin),err,sizeof err);bf_jv_set(croot,"expected_rfq_digest",bf_jv_str(H));free(cin);cin=bf_jv_dump(croot,0,BF_JV_SORT_NONE,NULL);bf_jv_free(croot);
    assert(bf_procurement_compare(cin,strlen(cin),NULL,&out1,err,sizeof err));cmp=bf_jv_parse(out1,strlen(out1),err,sizeof err);assert(!strcmp(bf_jv_get(cmp,"status")->s,"stale"));bf_jv_free(cmp);free(out1);free(cin);
    cin=compare_input(rfq,q,NULL);assert(!bf_procurement_compare(cin,strlen(cin),"bad",&out1,err,sizeof err));free(out1);free(cin);

    bf_jv *palloc=purchase(1,2),*rfqalloc=run_rfq(palloc),*basealloc=run_quote(rfqalloc,"USD",0,0,0);
    bf_jv *qa=quote_variant(basealloc,"quote-a","supplier-a","Supplier A",1,800,"2026-10-30T00:00:00Z");
    bf_jv *qb=quote_variant(basealloc,"quote-b","supplier-b","Supplier B",1,900,"2026-10-30T00:00:00Z");
    bf_jv *alloccmp=run_compare(rfqalloc,qa,qb,"2026-10-24T00:00:00Z");assert(!strcmp(bf_jv_get(alloccmp,"status")->s,"current"));
    char *ain=allocation_input(rfqalloc,qa,qb,alloccmp,"2026-10-24T00:00:00Z"),*aout1=NULL,*aout2=NULL;
    assert(bf_procurement_allocate(ain,strlen(ain),&aout1,err,sizeof err));assert(bf_procurement_allocate(ain,strlen(ain),&aout2,err,sizeof err));assert(!strcmp(aout1,aout2));
    bf_jv *allocation=bf_jv_parse(aout1,strlen(aout1),err,sizeof err);assert(allocation);assert(!strcmp(bf_jv_get(allocation,"status")->s,"prepared"));
    assert(bf_jv_get(allocation,"allocations")->len==2);assert((long long)bf_jv_n(bf_jv_get(allocation,"known_merchandise_cents"),-1)==1700);assert((long long)bf_jv_n(bf_jv_get(allocation,"known_total_cents"),-1)==1700);
    assert(!strcmp(bf_jv_get(allocation,"fees_attribution")->s,"full_quote"));assert(bf_jv_get(allocation,"order_lots")->len==2);assert(bf_jv_get(bf_jv_at(bf_jv_get(allocation,"selected_quotes"),0),"fees_attributed")->b);assert(bf_jv_get(allocation,"approval_required")->b&&!bf_jv_get(allocation,"approved")->b&&!bf_jv_get(allocation,"cart_created")->b&&!bf_jv_get(allocation,"commitment_opened")->b&&!bf_jv_get(allocation,"order_created")->b);
    const bf_jv *chain=bf_jv_get(allocation,"commerce_chain");assert(!strcmp(bf_jv_get(chain,"owner")->s,"CommerceChain"));assert(!strcmp(bf_jv_get(chain,"status")->s,"unmaterialized"));assert(!strcmp(bf_jv_get(chain,"next_required_stage")->s,"product"));
    char *apin=approval_input(allocation,4,"approval-12345678"),*apout1=NULL,*apout2=NULL;assert(bf_procurement_approval_request(apin,strlen(apin),&apout1,err,sizeof err));assert(bf_procurement_approval_request(apin,strlen(apin),&apout2,err,sizeof err));assert(!strcmp(apout1,apout2));
    bf_jv *approval=bf_jv_parse(apout1,strlen(apout1),err,sizeof err);assert(approval);assert(!strcmp(bf_jv_get(approval,"status")->s,"pending"));assert((long long)bf_jv_n(bf_jv_get(approval,"known_total_cents"),-1)==1700);assert(!strcmp(bf_jv_get(approval,"fees_attribution")->s,"full_quote"));const bf_jv *authority=bf_jv_get(approval,"authority");assert(!strcmp(bf_jv_get(authority,"owner")->s,"AuthorityGraph"));assert(!strcmp(bf_jv_get(authority,"decision")->s,"pending"));assert(bf_jv_get(authority,"required_permissions")->len==2);assert(!bf_jv_get(approval,"approved")->b&&!bf_jv_get(approval,"spend_authorized")->b&&!bf_jv_get(approval,"quote_accepted")->b&&!bf_jv_get(approval,"commitment_opened")->b&&!bf_jv_get(approval,"order_created")->b&&!bf_jv_get(approval,"payment_presented")->b);

    bf_jv *authority_status=bf_jv_obj();
    bf_jv_set(authority_status,"state",bf_jv_str("eligible"));
    bf_jv_set(authority_status,"record_key",bf_jv_str("procurement-approval:approval-12345678"));
    bf_jv_set(authority_status,"revision",bf_jv_num(5));bf_jv_set(authority_status,"body_sha256",bf_jv_str(H));
    bf_jv_set(authority_status,"authority_subject",bf_jv_clone(bf_jv_get(authority,"subject")));
    bf_jv_set(authority_status,"decision_id",bf_jv_str("decision-12345678"));bf_jv_set(authority_status,"outcome",bf_jv_str("approved"));
    bf_jv_set(authority_status,"decided_by",bf_jv_str("user:19"));bf_jv_set(authority_status,"decided_at",bf_jv_str("2026-10-24T01:00:00Z"));
    bf_jv_set(authority_status,"assent_present",bf_jv_bool(1));bf_jv_set(authority_status,"approve_authority",bf_jv_bool(1));bf_jv_set(authority_status,"spend_authority",bf_jv_bool(1));
    bf_jv_set(authority_status,"eligible_for_order_intent",bf_jv_bool(1));bf_jv_set(authority_status,"decision_receipt",bf_jv_str("fixture:decision-receipt"));
    bf_jv *oiroot=bf_jv_obj();bf_jv_set(oiroot,"allocation",bf_jv_clone(allocation));bf_jv_set(oiroot,"allocation_revision",bf_jv_num(4));
    bf_jv_set(oiroot,"approval",bf_jv_clone(approval));bf_jv_set(oiroot,"approval_revision",bf_jv_num(5));bf_jv_set(oiroot,"authority_status",bf_jv_clone(authority_status));
    bf_jv_set(oiroot,"id",bf_jv_str("order-intent-12345678"));bf_jv_set(oiroot,"prepared_at",bf_jv_str("2026-10-24T01:01:00Z"));
    char *oiin=bf_jv_dump(oiroot,0,BF_JV_SORT_NONE,NULL),*oiout1=NULL,*oiout2=NULL;
    assert(bf_procurement_order_intent(oiin,strlen(oiin),&oiout1,err,sizeof err));assert(bf_procurement_order_intent(oiin,strlen(oiin),&oiout2,err,sizeof err));assert(!strcmp(oiout1,oiout2));
    bf_jv *order_intent=bf_jv_parse(oiout1,strlen(oiout1),err,sizeof err);assert(order_intent);
    assert(!strcmp(bf_jv_get(order_intent,"status")->s,"prepared")&&bf_jv_get(order_intent,"authority_eligible")->b);assert((long long)bf_jv_n(bf_jv_get(order_intent,"known_total_cents"),-1)==1700);assert(!strcmp(bf_jv_get(order_intent,"fees_attribution")->s,"full_quote"));
    assert(!bf_jv_get(order_intent,"quote_accepted")->b&&!bf_jv_get(order_intent,"commitment_opened")->b&&!bf_jv_get(order_intent,"order_created")->b&&!bf_jv_get(order_intent,"payment_presented")->b&&!bf_jv_get(order_intent,"settled")->b&&!bf_jv_get(order_intent,"shipped")->b);
    const bf_jv *handoff=bf_jv_get(order_intent,"commitment_handoff");assert(!strcmp(bf_jv_get(handoff,"owner")->s,"CommitmentGraph"));assert(!strcmp(bf_jv_get(handoff,"status")->s,"unmaterialized"));
    bf_jv *pfroot=bf_jv_obj(),*actor_rows=bf_jv_arr();bf_jv_set(pfroot,"order_intent",bf_jv_clone(order_intent));bf_jv_set(pfroot,"id",bf_jv_str("protocol-preflight-12345678"));bf_jv_set(pfroot,"prepared_at",bf_jv_str("2026-10-24T01:02:00Z"));bf_jv_set(pfroot,"buyer_actor",bf_jv_str("buyer:acme"));
    const bf_jv *intent_lots=bf_jv_get(order_intent,"order_lots");for(size_t li=0;li<intent_lots->len;li++){const bf_jv *lot=bf_jv_at(intent_lots,li);bf_jv *row=bf_jv_obj();bf_jv_set(row,"quote_id",bf_jv_clone(bf_jv_get(lot,"quote_id")));char actor[64],capability[128];snprintf(actor,sizeof actor,"seller:%zu",li+1);snprintf(capability,sizeof capability,"procurement-capability:%zu",li+1);bf_jv_set(row,"seller_actor",bf_jv_str(actor));bf_jv_set(row,"settlement_actor",bf_jv_str("settlement:buyer"));bf_jv_set(row,"deliverer_actor",bf_jv_str(actor));bf_jv_set(row,"capability_ref",bf_jv_str(capability));bf_jv_push(actor_rows,row);}bf_jv_set(pfroot,"lot_actors",actor_rows);
    char *pfin=bf_jv_dump(pfroot,0,BF_JV_SORT_NONE,NULL),*pfout1=NULL,*pfout2=NULL;assert(bf_procurement_contract_preflight(pfin,strlen(pfin),&pfout1,err,sizeof err));assert(bf_procurement_contract_preflight(pfin,strlen(pfin),&pfout2,err,sizeof err));assert(!strcmp(pfout1,pfout2));
    bf_jv *preflight=bf_jv_parse(pfout1,strlen(pfout1),err,sizeof err);assert(preflight);assert(!strcmp(bf_jv_get(preflight,"schema")->s,"bonfyre.procurement.protocol-preflight.v1"));assert(bf_jv_get(preflight,"effect_free")->b);const bf_jv *contracts=bf_jv_get(preflight,"contracts");assert(contracts->len==2);for(size_t ci=0;ci<contracts->len;ci++){const bf_jv *contract=bf_jv_at(contracts,ci);assert(!strcmp(bf_jv_get(contract,"contract_name")->s,"quote-accept"));assert(!strcmp(bf_jv_get(contract,"status")->s,"authority_preflight_required"));assert(bf_jv_get(contract,"authority_requirements")->len==4);assert(bf_jv_get(contract,"proposed_commitments")->len==2);assert(!bf_jv_get(contract,"supplier_actor_binding_verified")->b&&!bf_jv_get(contract,"authority_verified")->b&&!bf_jv_get(contract,"supplier_acceptance_present")->b&&!bf_jv_get(contract,"protocol_enacted")->b&&!bf_jv_get(contract,"commitment_opened")->b);}
    bf_jv *badpf=bf_jv_clone(pfroot);bf_jv *badactors=bf_jv_get(badpf,"lot_actors");bf_jv_push(badactors,bf_jv_clone(bf_jv_at(badactors,0)));char *badpfin=bf_jv_dump(badpf,0,BF_JV_SORT_NONE,NULL);assert(!bf_procurement_contract_preflight(badpfin,strlen(badpfin),&out1,err,sizeof err));free(out1);out1=NULL;free(badpfin);bf_jv_free(badpf);bf_jv_free(preflight);free(pfin);free(pfout1);free(pfout2);bf_jv_free(pfroot);

    bf_jv_set(authority_status,"spend_authority",bf_jv_bool(0));bf_jv_set(oiroot,"authority_status",bf_jv_clone(authority_status));char *bad_oi=bf_jv_dump(oiroot,0,BF_JV_SORT_NONE,NULL);
    assert(!bf_procurement_order_intent(bad_oi,strlen(bad_oi),&out1,err,sizeof err));free(out1);out1=NULL;free(bad_oi);
    bf_jv_set(authority_status,"spend_authority",bf_jv_bool(1));bf_jv_set(authority_status,"outcome",bf_jv_str("rejected"));bf_jv_set(oiroot,"authority_status",bf_jv_clone(authority_status));bad_oi=bf_jv_dump(oiroot,0,BF_JV_SORT_NONE,NULL);
    assert(!bf_procurement_order_intent(bad_oi,strlen(bad_oi),&out1,err,sizeof err));free(out1);out1=NULL;free(bad_oi);
    free(oiin);free(oiout1);free(oiout2);bf_jv_free(order_intent);bf_jv_free(oiroot);bf_jv_free(authority_status);

    bf_jv *effectful=bf_jv_clone(allocation);bf_jv_set(effectful,"approved",bf_jv_bool(1));reseal(effectful);char *badap=approval_input(effectful,4,"approval-bad");assert(!bf_procurement_approval_request(badap,strlen(badap),&out1,err,sizeof err));free(out1);out1=NULL;free(badap);bf_jv_free(effectful);
    free(apin);free(apout1);free(apout2);bf_jv_free(approval);free(ain);free(aout1);free(aout2);bf_jv_free(allocation);

    bf_jv *expired=quote_variant(basealloc,"quote-expired","supplier-cheap","Supplier Cheap",2,100,"2026-10-23T23:00:00Z");
    bf_jv *valid=quote_variant(basealloc,"quote-valid","supplier-valid","Supplier Valid",2,950,"2026-10-30T00:00:00Z");
    bf_jv *expirycmp=run_compare(rfqalloc,expired,valid,"2026-10-24T00:00:00Z");assert(!strcmp(bf_jv_get(expirycmp,"status")->s,"current"));assert((int)bf_jv_n(bf_jv_get(expirycmp,"expired_quote_count"),-1)==1);
    allocation=run_allocate(rfqalloc,expired,valid,expirycmp,"2026-10-24T00:00:00Z");assert(bf_jv_get(allocation,"allocations")->len==1);assert(!strcmp(bf_jv_get(bf_jv_at(bf_jv_get(allocation,"allocations"),0),"quote_id")->s,"quote-valid"));assert((long long)bf_jv_n(bf_jv_get(allocation,"known_merchandise_cents"),-1)==1900);bf_jv_free(allocation);

    bf_jv *excess=quote_variant(basealloc,"quote-excess","supplier-excess","Supplier Excess",3,650,"2026-10-30T00:00:00Z");
    bf_jv *excesscmp=run_compare(rfqalloc,excess,NULL,"2026-10-24T00:00:00Z");assert(!strcmp(bf_jv_get(excesscmp,"status")->s,"current"));
    bf_jv *excessalloc=run_allocate(rfqalloc,excess,NULL,excesscmp,"2026-10-24T00:00:00Z");assert(bf_jv_is(bf_jv_get(excessalloc,"known_total_cents"),BF_JV_NULL));assert(!strcmp(bf_jv_get(excessalloc,"fees_attribution")->s,"unallocated"));assert(!bf_jv_get(bf_jv_at(bf_jv_get(excessalloc,"order_lots"),0),"full_quote_consumed")->b);
    char *excessapin=approval_input(excessalloc,1,"approval-excess"),*excessapout=NULL;assert(bf_procurement_approval_request(excessapin,strlen(excessapin),&excessapout,err,sizeof err));bf_jv *excessapproval=bf_jv_parse(excessapout,strlen(excessapout),err,sizeof err);
    bf_jv *excessstatus=bf_jv_obj();bf_jv_set(excessstatus,"state",bf_jv_str("eligible"));bf_jv_set(excessstatus,"record_key",bf_jv_str("procurement-approval:approval-excess"));bf_jv_set(excessstatus,"revision",bf_jv_num(1));bf_jv_set(excessstatus,"body_sha256",bf_jv_str(H));bf_jv_set(excessstatus,"authority_subject",bf_jv_clone(bf_jv_get(bf_jv_get(excessapproval,"authority"),"subject")));bf_jv_set(excessstatus,"decision_id",bf_jv_str("decision-excess"));bf_jv_set(excessstatus,"outcome",bf_jv_str("approved"));bf_jv_set(excessstatus,"decided_by",bf_jv_str("user:19"));bf_jv_set(excessstatus,"decided_at",bf_jv_str("2026-10-24T01:00:00Z"));bf_jv_set(excessstatus,"assent_present",bf_jv_bool(1));bf_jv_set(excessstatus,"approve_authority",bf_jv_bool(1));bf_jv_set(excessstatus,"spend_authority",bf_jv_bool(1));bf_jv_set(excessstatus,"eligible_for_order_intent",bf_jv_bool(1));bf_jv_set(excessstatus,"decision_receipt",bf_jv_str("fixture:decision-excess"));
    bf_jv *excessroot=bf_jv_obj();bf_jv_set(excessroot,"allocation",bf_jv_clone(excessalloc));bf_jv_set(excessroot,"allocation_revision",bf_jv_num(1));bf_jv_set(excessroot,"approval",bf_jv_clone(excessapproval));bf_jv_set(excessroot,"approval_revision",bf_jv_num(1));bf_jv_set(excessroot,"authority_status",excessstatus);bf_jv_set(excessroot,"id",bf_jv_str("order-intent-excess"));bf_jv_set(excessroot,"prepared_at",bf_jv_str("2026-10-24T01:02:00Z"));
    char *excessoi=bf_jv_dump(excessroot,0,BF_JV_SORT_NONE,NULL);assert(!bf_procurement_order_intent(excessoi,strlen(excessoi),&out1,err,sizeof err));assert(strstr(err,"exact fully attributed spend total"));free(out1);out1=NULL;free(excessoi);bf_jv_free(excessroot);bf_jv_free(excessapproval);free(excessapout);free(excessapin);bf_jv_free(excessalloc);bf_jv_free(excesscmp);bf_jv_free(excess);

    bf_jv *oneunit=quote_variant(basealloc,"quote-one","supplier-one","Supplier One",1,700,"2026-10-30T00:00:00Z");
    bf_jv *partialcmp=run_compare(rfqalloc,oneunit,NULL,"2026-10-24T00:00:00Z");assert(!strcmp(bf_jv_get(partialcmp,"status")->s,"partial"));
    ain=allocation_input(rfqalloc,oneunit,NULL,partialcmp,"2026-10-24T00:00:00Z");assert(!bf_procurement_allocate(ain,strlen(ain),&aout1,err,sizeof err));free(ain);free(aout1);aout1=NULL;

    bf_jv *malformed=bf_jv_clone(valid),*malformed_line=bf_jv_at(bf_jv_get(malformed,"lines"),0);bf_jv_set(malformed_line,"known_extended_cents",bf_jv_num(1));reseal(malformed);
    char *badcmpin=compare_input(rfqalloc,malformed,NULL);assert(!bf_procurement_compare(badcmpin,strlen(badcmpin),"2026-10-24T00:00:00Z",&aout1,err,sizeof err));free(badcmpin);free(aout1);aout1=NULL;
    bf_jv_free(malformed);bf_jv_free(partialcmp);bf_jv_free(oneunit);bf_jv_free(expirycmp);bf_jv_free(valid);bf_jv_free(expired);bf_jv_free(alloccmp);bf_jv_free(qb);bf_jv_free(qa);bf_jv_free(basealloc);bf_jv_free(rfqalloc);bf_jv_free(palloc);

    bf_jv *forty=purchase(40,1),*rfq40=run_rfq(forty);assert(bf_jv_get(rfq40,"lines")->len==40);bf_jv_free(rfq40);bf_jv_free(forty);
    bf_jv *sixtyfour=purchase(1,64),*rfq64=run_rfq(sixtyfour);assert((int)bf_jv_n(bf_jv_get(bf_jv_at(bf_jv_get(rfq64,"lines"),0),"quantity_requested"),0)==64);bf_jv_free(rfq64);bf_jv_free(sixtyfour);

    bf_jv *bad=bf_jv_clone(p);bf_jv_set(bf_jv_at(bf_jv_get(bad,"lines"),1),"product_id",bf_jv_clone(bf_jv_get(bf_jv_at(bf_jv_get(bad,"lines"),0),"product_id")));bf_jv_set(bf_jv_at(bf_jv_get(bad,"lines"),1),"variant_id",bf_jv_clone(bf_jv_get(bf_jv_at(bf_jv_get(bad,"lines"),0),"variant_id")));bf_jv_set(bf_jv_at(bf_jv_get(bad,"lines"),1),"source_url",bf_jv_clone(bf_jv_get(bf_jv_at(bf_jv_get(bad,"lines"),0),"source_url")));expect_bad_rfq(bad);bf_jv_free(bad);
    bad=bf_jv_clone(p);bf_jv_set(bf_jv_at(bf_jv_get(bad,"lines"),0),"source_url",bf_jv_str("http://insecure.example/item"));expect_bad_rfq(bad);bf_jv_free(bad);
    bad=bf_jv_clone(p);bf_jv_set(bf_jv_at(bf_jv_get(bad,"lines"),0),"label",bf_jv_str("bad\nlabel"));expect_bad_rfq(bad);bf_jv_free(bad);
    in=rfq_input(p,NULL,NULL,"not-a-time");assert(!bf_procurement_rfq(in,strlen(in),&out1,err,sizeof err));free(out1);free(in);
    assert(!bf_procurement_rfq("{}",1024u*1024u+1u,&out1,err,sizeof err));free(out1);

    char *qin=quote_input(rfq,"USD",0,0,0,NULL,NULL);bf_jv *qr=bf_jv_parse(qin,strlen(qin),err,sizeof err);
    bf_jv_set(bf_jv_at(bf_jv_get(bf_jv_get(qr,"observation"),"lines"),0),"quantity",bf_jv_num(0));char *badq=bf_jv_dump(qr,0,BF_JV_SORT_NONE,NULL);
    assert(!bf_procurement_quote(badq,strlen(badq),&out1,err,sizeof err));free(out1);free(badq);bf_jv_free(qr);free(qin);

    qin=quote_input(rfq,"USD",0,0,0,NULL,NULL);qr=bf_jv_parse(qin,strlen(qin),err,sizeof err);
    bf_jv_set(bf_jv_at(bf_jv_get(bf_jv_get(qr,"observation"),"lines"),0),"unit_cents",bf_jv_num(-1));badq=bf_jv_dump(qr,0,BF_JV_SORT_NONE,NULL);
    assert(!bf_procurement_quote(badq,strlen(badq),&out1,err,sizeof err));free(out1);free(badq);bf_jv_free(qr);free(qin);

    qin=quote_input(rfq,"USD",0,0,0,NULL,NULL);qr=bf_jv_parse(qin,strlen(qin),err,sizeof err);
    bf_jv_set(bf_jv_get(qr,"observation"),"tax_cents",bf_jv_num(-1));badq=bf_jv_dump(qr,0,BF_JV_SORT_NONE,NULL);
    assert(!bf_procurement_quote(badq,strlen(badq),&out1,err,sizeof err));free(out1);free(badq);bf_jv_free(qr);free(qin);

    qin=quote_input(rfq,"USD",0,0,0,NULL,NULL);qr=bf_jv_parse(qin,strlen(qin),err,sizeof err);
    bf_jv_set(bf_jv_at(bf_jv_get(bf_jv_get(qr,"observation"),"lines"),0),"rfq_line_id",bf_jv_str(H));badq=bf_jv_dump(qr,0,BF_JV_SORT_NONE,NULL);
    assert(!bf_procurement_quote(badq,strlen(badq),&out1,err,sizeof err));free(out1);free(badq);bf_jv_free(qr);free(qin);

    qin=quote_input(rfq,"USD",0,0,0,NULL,NULL);qr=bf_jv_parse(qin,strlen(qin),err,sizeof err);
    bf_jv *qlines=bf_jv_get(bf_jv_get(qr,"observation"),"lines");bf_jv_push(qlines,bf_jv_clone(bf_jv_at(qlines,0)));badq=bf_jv_dump(qr,0,BF_JV_SORT_NONE,NULL);
    assert(!bf_procurement_quote(badq,strlen(badq),&out1,err,sizeof err));free(out1);free(badq);bf_jv_free(qr);free(qin);

    bf_jv *supplier=bf_jv_obj();bf_jv_set(supplier,"id",bf_jv_str("supplier-a"));bf_jv_set(supplier,"name",bf_jv_str("Supplier A"));
    in=rfq_input(p,NULL,supplier,NULL);assert(bf_procurement_rfq(in,strlen(in),&out1,err,sizeof err));bf_jv *sr=bf_jv_parse(out1,strlen(out1),err,sizeof err);free(in);free(out1);
    bf_jv *other=bf_jv_obj();bf_jv_set(other,"id",bf_jv_str("supplier-b"));bf_jv_set(other,"name",bf_jv_str("Supplier B"));
    qin=quote_input(sr,"USD",0,0,0,other,NULL);assert(!bf_procurement_quote(qin,strlen(qin),&out1,err,sizeof err));free(out1);free(qin);bf_jv_free(other);bf_jv_free(supplier);bf_jv_free(sr);

    cin=compare_input(rfq,q,q);assert(!bf_procurement_compare(cin,strlen(cin),NULL,&out1,err,sizeof err));free(out1);free(cin);

    bf_jv *changed=bf_jv_clone(q);bf_jv_set(changed,"source_ref",bf_jv_str("file:revised.pdf"));bf_jv_del(changed,"digest");char dig[65];bf_jv_digest(changed,dig);bf_jv_set(changed,"digest",bf_jv_str(dig));assert(strcmp(bf_jv_get(changed,"digest")->s,bf_jv_get(q,"digest")->s));bf_jv_free(changed);

    bf_jv_free(q);bf_jv_free(partial);bf_jv_free(eur);bf_jv_free(sub);bf_jv_free(rfq);bf_jv_free(p);
    puts("procurement native core: expanded scenarios ok");
    return 0;
}
