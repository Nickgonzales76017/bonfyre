#define _POSIX_C_SOURCE 200809L
#include "procurement.h"
#include "bf_jv.h"

#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SAFE_INT 9007199254740991LL
#define MAX_LINES 256u
#define MAX_QUOTES 32u

static int fail(char *err, size_t n, const char *message) {
    if (err && n) snprintf(err, n, "%s", message);
    return 0;
}

static const char *sval(const bf_jv *v) {
    return bf_jv_is(v, BF_JV_STR) ? v->s : NULL;
}

static int text_ok(const bf_jv *v, size_t max) {
    if (!bf_jv_is(v, BF_JV_STR) || !v->len || v->len > max) return 0;
    for (size_t i = 0; i < v->len; i++) {
        unsigned char c = (unsigned char)v->s[i];
        if (c < 0x20 || c == 0x7f) return 0;
    }
    return 1;
}

static int hex64(const bf_jv *v) {
    if (!bf_jv_is(v, BF_JV_STR) || v->len != 64) return 0;
    for (size_t i = 0; i < 64; i++)
        if (!((v->s[i] >= '0' && v->s[i] <= '9') || (v->s[i] >= 'a' && v->s[i] <= 'f'))) return 0;
    return 1;
}

static int safe_int(const bf_jv *v, long long *out, int positive) {
    if (!bf_jv_is(v, BF_JV_NUM) || !isfinite(v->n) || floor(v->n) != v->n ||
        v->n < (positive ? 1.0 : 0.0) || v->n > (double)SAFE_INT) return 0;
    *out = (long long)v->n;
    return 1;
}

static int nullable_money(const bf_jv *v, long long *out, int *known) {
    if (!v || bf_jv_is(v, BF_JV_NULL)) { *known = 0; *out = 0; return 1; }
    if (!safe_int(v, out, 0)) return 0;
    *known = 1;
    return 1;
}

static int timestamp_ok(const bf_jv *v) {
    if (!bf_jv_is(v, BF_JV_STR) || v->len != 20) return 0;
    const char *s = v->s;
    for (int i = 0; i < 20; i++) {
        if (i == 4 || i == 7) { if (s[i] != '-') return 0; }
        else if (i == 10) { if (s[i] != 'T') return 0; }
        else if (i == 13 || i == 16) { if (s[i] != ':') return 0; }
        else if (i == 19) { if (s[i] != 'Z') return 0; }
        else if (!isdigit((unsigned char)s[i])) return 0;
    }
    int month=(s[5]-'0')*10+s[6]-'0', day=(s[8]-'0')*10+s[9]-'0';
    int hour=(s[11]-'0')*10+s[12]-'0', minute=(s[14]-'0')*10+s[15]-'0', second=(s[17]-'0')*10+s[18]-'0';
    return month >= 1 && month <= 12 && day >= 1 && day <= 31 && hour <= 23 && minute <= 59 && second <= 60;
}

static int timestamp_nullable(const bf_jv *v) {
    return !v || bf_jv_is(v, BF_JV_NULL) || timestamp_ok(v);
}

static int currency_ok(const bf_jv *v) {
    if (!bf_jv_is(v, BF_JV_STR) || v->len != 3) return 0;
    return v->s[0] >= 'A' && v->s[0] <= 'Z' &&
           v->s[1] >= 'A' && v->s[1] <= 'Z' &&
           v->s[2] >= 'A' && v->s[2] <= 'Z';
}

static int https_ok(const bf_jv *v) {
    if (!text_ok(v, 4000) || strncmp(v->s, "https://", 8)) return 0;
    const char *host = v->s + 8, *end = strpbrk(host, "/?#");
    size_t n = end ? (size_t)(end-host) : strlen(host);
    if (!n) return 0;
    for (size_t i = 0; i < n; i++) if (host[i] == '@' || isspace((unsigned char)host[i])) return 0;
    return 1;
}

static int bool_is(const bf_jv *v, int expected) {
    return bf_jv_is(v, BF_JV_BOOL) && v->b == expected;
}

static bf_jv *parse_root(const char *json, size_t len, char *err, size_t n) {
    if (!json || !len || len > 1024u*1024u) { fail(err,n,"input must be 1..1048576 bytes"); return NULL; }
    char why[160] = {0};
    bf_jv *root = bf_jv_parse(json, len, why, sizeof why);
    if (!root || !bf_jv_is(root, BF_JV_OBJ)) {
        if (root) bf_jv_free(root);
        fail(err,n,why[0] ? why : "input must be a JSON object");
        return NULL;
    }
    return root;
}

static int verify_digest(const bf_jv *record, char *err, size_t n) {
    const bf_jv *digest = bf_jv_get(record,"digest");
    if (!bf_jv_is(record,BF_JV_OBJ) || !hex64(digest)) return fail(err,n,"record is missing a canonical digest");
    bf_jv *copy=bf_jv_clone(record); char got[65];
    bf_jv_del(copy,"digest"); bf_jv_digest(copy,got); bf_jv_free(copy);
    return !strcmp(got,digest->s) || fail(err,n,"record digest does not match its body");
}

static void seal(bf_jv *record) {
    char digest[65]; bf_jv_digest(record,digest); bf_jv_set(record,"digest",bf_jv_str(digest));
}

static void line_identity(const bf_jv *line, char out[65]) {
    const char *keys[]={"product_id","source_url","variant_id"};
    const bf_jv *values[]={bf_jv_get(line,"product_id"),bf_jv_get(line,"source_url"),bf_jv_get(line,"variant_id")};
    bf_jv_digest_members(BF_JV_SORT_BYTES,3,keys,values,out);
}

static int ed25519_actor_string(const char *s) {
    if (!s || strncmp(s,"ed25519:",8) || strlen(s)!=72) return 0;
    for (size_t i=8;i<72;i++) if (!((s[i]>='0'&&s[i]<='9')||(s[i]>='a'&&s[i]<='f'))) return 0;
    return 1;
}

static int supplier_ok(const bf_jv *v) {
    if (!v || bf_jv_is(v,BF_JV_NULL)) return 1;
    if (!bf_jv_is(v,BF_JV_OBJ)) return 0;
    const bf_jv *id=bf_jv_get(v,"id"), *name=bf_jv_get(v,"name");
    if ((!id || bf_jv_is(id,BF_JV_NULL)) && (!name || bf_jv_is(name,BF_JV_NULL))) return 0;
    if (id && !bf_jv_is(id,BF_JV_NULL) && !text_ok(id,256)) return 0;
    if (name && !bf_jv_is(name,BF_JV_NULL) && !text_ok(name,512)) return 0;
    return 1;
}

static int supplier_same(const bf_jv *a, const bf_jv *b) {
    if (!a || bf_jv_is(a,BF_JV_NULL) || !b || bf_jv_is(b,BF_JV_NULL)) return 1;
    const bf_jv *ai=bf_jv_get(a,"id"), *bi=bf_jv_get(b,"id");
    if (text_ok(ai,256) && text_ok(bi,256)) return !strcmp(ai->s,bi->s);
    return bf_jv_equal(a,b);
}

static int purchase_list_ok(const bf_jv *p, char *err, size_t n) {
    const char *schema=sval(bf_jv_get(p,"schema")), *state=sval(bf_jv_get(p,"state")), *currency=sval(bf_jv_get(p,"currency"));
    if (!bf_jv_is(p,BF_JV_OBJ) || !schema || strcmp(schema,"service-site.room-purchase-list.v1") ||
        !state || strcmp(state,"draft") || !currency || strcmp(currency,"USD") ||
        !text_ok(bf_jv_get(p,"id"),128) || !hex64(bf_jv_get(p,"sha256")) ||
        !bf_jv_is(bf_jv_get(p,"commitment"),BF_JV_NULL))
        return fail(err,n,"invalid retained purchase list identity or state");
    const bf_jv *source=bf_jv_get(p,"source"), *lines=bf_jv_get(p,"lines"); long long revision=0,total=0;
    if (!bf_jv_is(source,BF_JV_OBJ) || !text_ok(bf_jv_get(source,"key"),511) ||
        !safe_int(bf_jv_get(source,"revision"),&revision,1) || !hex64(bf_jv_get(source,"sha256")))
        return fail(err,n,"purchase list source lineage is invalid");
    if (!bf_jv_is(lines,BF_JV_ARR) || !lines->len || lines->len>MAX_LINES)
        return fail(err,n,"purchase list must contain 1..256 lines");
    char ids[MAX_LINES][65];
    for(size_t i=0;i<lines->len;i++){
        const bf_jv *line=bf_jv_at(lines,i); long long unit=0,qty=0,subtotal=0;
        if(!bf_jv_is(line,BF_JV_OBJ)||!text_ok(bf_jv_get(line,"product_id"),128)||!text_ok(bf_jv_get(line,"variant_id"),128)||
           !text_ok(bf_jv_get(line,"label"),1000)||!text_ok(bf_jv_get(line,"option"),1000)||!https_ok(bf_jv_get(line,"source_url"))||
           !hex64(bf_jv_get(line,"source_sha256"))||!timestamp_ok(bf_jv_get(line,"observed_at"))||
           !safe_int(bf_jv_get(line,"unit_cents"),&unit,0)||!safe_int(bf_jv_get(line,"quantity"),&qty,1)||
           !safe_int(bf_jv_get(line,"subtotal_cents"),&subtotal,0)) return fail(err,n,"purchase list line is malformed");
        if(unit && qty>SAFE_INT/unit) return fail(err,n,"purchase list line total exceeds safe integer precision");
        if(unit*qty!=subtotal) return fail(err,n,"purchase list line subtotal conflicts with unit price and quantity");
        line_identity(line,ids[i]); for(size_t k=0;k<i;k++) if(!strcmp(ids[i],ids[k])) return fail(err,n,"purchase list contains duplicate product identity");
        if(total>SAFE_INT-subtotal) return fail(err,n,"purchase list total exceeds safe integer precision"); total+=subtotal;
    }
    long long declared=0;
    if(!safe_int(bf_jv_get(p,"subtotal_cents"),&declared,0)||declared!=total)return fail(err,n,"purchase list subtotal does not match its lines");
    return 1;
}

static bf_jv *rfq_line(const bf_jv *line) {
    char id[65]; line_identity(line,id); bf_jv *o=bf_jv_obj();
    bf_jv_set(o,"line_id",bf_jv_str(id));
    const char *fields[]={"product_id","variant_id","label","option","source_url","source_sha256","observed_at",NULL};
    for(const char **f=fields;*f;f++)bf_jv_set(o,*f,bf_jv_clone(bf_jv_get(line,*f)));
    bf_jv_set(o,"quantity_requested",bf_jv_clone(bf_jv_get(line,"quantity")));
    bf_jv_set(o,"listed_unit_cents",bf_jv_clone(bf_jv_get(line,"unit_cents")));
    return o;
}

static int selected_has(const bf_jv *selected,const char *id,int *bad) {
    if(!selected||bf_jv_is(selected,BF_JV_NULL))return 1;
    if(!bf_jv_is(selected,BF_JV_ARR)||!selected->len||selected->len>MAX_LINES){*bad=1;return 0;}
    int found=0;
    for(size_t i=0;i<selected->len;i++){
        const bf_jv *v=bf_jv_at(selected,i); if(!hex64(v)){*bad=1;return 0;}
        for(size_t k=0;k<i;k++)if(!strcmp(v->s,sval(bf_jv_at(selected,k)))){*bad=1;return 0;}
        if(!strcmp(v->s,id))found=1;
    }
    return found;
}

int bf_procurement_rfq(const char *json,size_t len,char **out,char *err,size_t n) {
    *out=NULL; bf_jv *root=parse_root(json,len,err,n); if(!root)return 0;
    const bf_jv *p=bf_jv_get(root,"purchase_list"),*rev=bf_jv_get(root,"purchase_list_revision"),*id=bf_jv_get(root,"id");
    const bf_jv *at=bf_jv_get(root,"requested_at"),*supplier=bf_jv_get(root,"supplier"),*selected=bf_jv_get(root,"selected"); long long revision=0;
    if(!purchase_list_ok(p,err,n)||!safe_int(rev,&revision,1)||!text_ok(id,128)||!timestamp_ok(at)||!supplier_ok(supplier)){bf_jv_free(root);return 0;}
    bf_jv *record=bf_jv_obj(),*source=bf_jv_obj(),*lines=bf_jv_arr();
    bf_jv_set(record,"schema",bf_jv_str("bonfyre.procurement.supplier-request.v1"));bf_jv_set(record,"id",bf_jv_clone(id));
    bf_jv_set(record,"status",bf_jv_str("prepared"));bf_jv_set(record,"requested_at",bf_jv_clone(at));bf_jv_set(record,"supplier",supplier?bf_jv_clone(supplier):bf_jv_null());
    bf_jv_set(source,"purchase_list_id",bf_jv_clone(bf_jv_get(p,"id")));bf_jv_set(source,"revision",bf_jv_num((double)revision));bf_jv_set(source,"sha256",bf_jv_clone(bf_jv_get(p,"sha256")));bf_jv_set(record,"source",source);
    const bf_jv *plines=bf_jv_get(p,"lines"); size_t kept=0; int bad=0;
    for(size_t i=0;i<plines->len;i++){char lid[65];line_identity(bf_jv_at(plines,i),lid);if(selected_has(selected,lid,&bad)){bf_jv_push(lines,rfq_line(bf_jv_at(plines,i)));kept++;}}
    if(bad||!kept||(selected&&!bf_jv_is(selected,BF_JV_NULL)&&kept!=selected->len)){bf_jv_free(lines);bf_jv_free(record);bf_jv_free(root);return fail(err,n,bad?"selected line identities are invalid or duplicated":"selected lines do not belong to the purchase list");}
    bf_jv_set(record,"lines",lines);bf_jv_set(record,"supplier_contacted",bf_jv_bool(0));bf_jv_set(record,"quote_received",bf_jv_bool(0));
    bf_jv_set(record,"purchase_approved",bf_jv_bool(0));bf_jv_set(record,"order_committed",bf_jv_bool(0));
    bf_jv_set(record,"does_not_establish",bf_jv_str("supplier contact, quotation, reservation, purchase approval, order, payment, settlement or shipment"));
    seal(record);*out=bf_jv_dump(record,0,BF_JV_SORT_NONE,NULL);bf_jv_free(record);bf_jv_free(root);return *out!=NULL;
}

static int rfq_ok(const bf_jv *r,char *err,size_t n) {
    const char *schema=sval(bf_jv_get(r,"schema")),*status=sval(bf_jv_get(r,"status"));
    if(!bf_jv_is(r,BF_JV_OBJ)||!schema||strcmp(schema,"bonfyre.procurement.supplier-request.v1")||!status||strcmp(status,"prepared")||
       !text_ok(bf_jv_get(r,"id"),128)||!timestamp_ok(bf_jv_get(r,"requested_at"))||!supplier_ok(bf_jv_get(r,"supplier"))||
       !bool_is(bf_jv_get(r,"supplier_contacted"),0)||!bool_is(bf_jv_get(r,"purchase_approved"),0)||!bool_is(bf_jv_get(r,"order_committed"),0)||
       !verify_digest(r,err,n))return fail(err,n,"invalid supplier request draft");
    const bf_jv *source=bf_jv_get(r,"source"),*lines=bf_jv_get(r,"lines");long long rev=0;
    if(!bf_jv_is(source,BF_JV_OBJ)||!text_ok(bf_jv_get(source,"purchase_list_id"),128)||!safe_int(bf_jv_get(source,"revision"),&rev,1)||
       !hex64(bf_jv_get(source,"sha256"))||!bf_jv_is(lines,BF_JV_ARR)||!lines->len||lines->len>MAX_LINES)return fail(err,n,"supplier request lineage or lines are invalid");
    for(size_t i=0;i<lines->len;i++){
        const bf_jv *l=bf_jv_at(lines,i);long long q=0,u=0;
        if(!bf_jv_is(l,BF_JV_OBJ)||!hex64(bf_jv_get(l,"line_id"))||!safe_int(bf_jv_get(l,"quantity_requested"),&q,1)||
           !safe_int(bf_jv_get(l,"listed_unit_cents"),&u,0)||!https_ok(bf_jv_get(l,"source_url"))||
           !text_ok(bf_jv_get(l,"product_id"),128)||!text_ok(bf_jv_get(l,"variant_id"),128)||!hex64(bf_jv_get(l,"source_sha256")))return fail(err,n,"supplier request line is invalid");
        for(size_t k=0;k<i;k++)if(!strcmp(sval(bf_jv_get(l,"line_id")),sval(bf_jv_get(bf_jv_at(lines,k),"line_id"))))return fail(err,n,"supplier request has duplicate line identity");
    }
    return 1;
}

static const bf_jv *rfq_find_line(const bf_jv *rfq,const char *id) {
    const bf_jv *lines=bf_jv_get(rfq,"lines");
    for(size_t i=0;i<lines->len;i++){const bf_jv *line=bf_jv_at(lines,i);if(!strcmp(sval(bf_jv_get(line,"line_id")),id))return line;}
    return NULL;
}

static int substitution_ok(const bf_jv *s) {
    if(!s||bf_jv_is(s,BF_JV_NULL))return 1;
    return bf_jv_is(s,BF_JV_OBJ)&&https_ok(bf_jv_get(s,"source_url"))&&text_ok(bf_jv_get(s,"product_id"),128)&&
           text_ok(bf_jv_get(s,"variant_id"),128)&&hex64(bf_jv_get(s,"source_sha256"));
}

int bf_procurement_quote(const char *json,size_t len,char **out,char *err,size_t n) {
    *out=NULL;bf_jv *root=parse_root(json,len,err,n);if(!root)return 0;
    const bf_jv *rfq=bf_jv_get(root,"rfq"),*o=bf_jv_get(root,"observation");
    if(!rfq_ok(rfq,err,n)||!bf_jv_is(o,BF_JV_OBJ)){bf_jv_free(root);return fail(err,n,err[0]?err:"quote observation is missing");}
    const bf_jv *id=bf_jv_get(o,"id"),*supplier=bf_jv_get(o,"supplier"),*sid=bf_jv_get(o,"supplier_quote_id");
    const bf_jv *observed=bf_jv_get(o,"observed_at"),*quoted=bf_jv_get(o,"quoted_at"),*valid=bf_jv_get(o,"valid_until");
    const bf_jv *currency=bf_jv_get(o,"currency"),*source_ref=bf_jv_get(o,"source_ref"),*qlines=bf_jv_get(o,"lines");
    long long amount=0;int known=0;
    if(!text_ok(id,128)||!supplier_ok(supplier)||!supplier_same(bf_jv_get(rfq,"supplier"),supplier)||
       (sid&&!bf_jv_is(sid,BF_JV_NULL)&&!text_ok(sid,256))||!timestamp_ok(observed)||!timestamp_nullable(quoted)||!timestamp_nullable(valid)||
       !currency_ok(currency)||!text_ok(source_ref,4000)||!nullable_money(bf_jv_get(o,"freight_cents"),&amount,&known)||
       !nullable_money(bf_jv_get(o,"tax_cents"),&amount,&known)||!nullable_money(bf_jv_get(o,"other_fees_cents"),&amount,&known)||
       !bf_jv_is(qlines,BF_JV_ARR)||!qlines->len||qlines->len>MAX_LINES){bf_jv_free(root);return fail(err,n,"quote observation metadata is invalid");}
    if(quoted&&!bf_jv_is(quoted,BF_JV_NULL)&&valid&&!bf_jv_is(valid,BF_JV_NULL)&&strcmp(valid->s,quoted->s)<0){bf_jv_free(root);return fail(err,n,"quote expires before its quoted timestamp");}
    bf_jv *record=bf_jv_obj(),*src=bf_jv_obj(),*lines=bf_jv_arr();
    bf_jv_set(record,"schema",bf_jv_str("bonfyre.procurement.supplier-quote-observation.v1"));bf_jv_set(record,"id",bf_jv_clone(id));bf_jv_set(record,"status",bf_jv_str("observed"));
    bf_jv_set(src,"rfq_id",bf_jv_clone(bf_jv_get(rfq,"id")));bf_jv_set(src,"rfq_digest",bf_jv_clone(bf_jv_get(rfq,"digest")));bf_jv_set(record,"source",src);
    bf_jv_set(record,"supplier",supplier?bf_jv_clone(supplier):bf_jv_null());bf_jv_set(record,"supplier_quote_id",sid?bf_jv_clone(sid):bf_jv_null());
    bf_jv_set(record,"observed_at",bf_jv_clone(observed));bf_jv_set(record,"quoted_at",quoted?bf_jv_clone(quoted):bf_jv_null());bf_jv_set(record,"valid_until",valid?bf_jv_clone(valid):bf_jv_null());
    bf_jv_set(record,"currency",bf_jv_clone(currency));bf_jv_set(record,"source_ref",bf_jv_clone(source_ref));
    const char *fees[]={"freight_cents","tax_cents","other_fees_cents",NULL};
    for(const char **f=fees;*f;f++)bf_jv_set(record,*f,bf_jv_get(o,*f)?bf_jv_clone(bf_jv_get(o,*f)):bf_jv_null());
    for(size_t i=0;i<qlines->len;i++){
        const bf_jv *q=bf_jv_at(qlines,i),*qid=bf_jv_get(q,"id"),*rid=bf_jv_get(q,"rfq_line_id"),*sub=bf_jv_get(q,"substitution");
        long long qty=0,unit=0,total=0;int uk=0,tk=0;
        if(!bf_jv_is(q,BF_JV_OBJ)||!text_ok(qid,128)||!hex64(rid)||!rfq_find_line(rfq,rid->s)||!safe_int(bf_jv_get(q,"quantity"),&qty,1)||
           !nullable_money(bf_jv_get(q,"unit_cents"),&unit,&uk)||!nullable_money(bf_jv_get(q,"line_total_cents"),&total,&tk)||!substitution_ok(sub)||
           (bf_jv_get(q,"availability")&&!bf_jv_is(bf_jv_get(q,"availability"),BF_JV_NULL)&&!text_ok(bf_jv_get(q,"availability"),512))||
           (bf_jv_get(q,"lead_time")&&!bf_jv_is(bf_jv_get(q,"lead_time"),BF_JV_NULL)&&!text_ok(bf_jv_get(q,"lead_time"),512))){
            bf_jv_free(lines);bf_jv_free(record);bf_jv_free(root);return fail(err,n,"quote line is invalid or references an unknown RFQ line");
        }
        for(size_t k=0;k<i;k++)if(!strcmp(qid->s,sval(bf_jv_get(bf_jv_at(qlines,k),"id")))){bf_jv_free(lines);bf_jv_free(record);bf_jv_free(root);return fail(err,n,"quote line id is duplicated");}
        if(uk&&qty>SAFE_INT/unit){bf_jv_free(lines);bf_jv_free(record);bf_jv_free(root);return fail(err,n,"quote line extended price exceeds safe integer precision");}
        long long extended=uk?unit*qty:total;int ek=uk||tk;
        if(uk&&tk&&extended!=total){bf_jv_free(lines);bf_jv_free(record);bf_jv_free(root);return fail(err,n,"quote line total conflicts with unit price and quantity");}
        bf_jv *line=bf_jv_obj();bf_jv_set(line,"id",bf_jv_clone(qid));bf_jv_set(line,"rfq_line_id",bf_jv_clone(rid));bf_jv_set(line,"quantity",bf_jv_num((double)qty));
        bf_jv_set(line,"unit_cents",bf_jv_get(q,"unit_cents")?bf_jv_clone(bf_jv_get(q,"unit_cents")):bf_jv_null());bf_jv_set(line,"line_total_cents",bf_jv_get(q,"line_total_cents")?bf_jv_clone(bf_jv_get(q,"line_total_cents")):bf_jv_null());
        bf_jv_set(line,"known_extended_cents",ek?bf_jv_num((double)extended):bf_jv_null());bf_jv_set(line,"substitution",sub?bf_jv_clone(sub):bf_jv_null());
        bf_jv_set(line,"availability",bf_jv_get(q,"availability")?bf_jv_clone(bf_jv_get(q,"availability")):bf_jv_null());bf_jv_set(line,"lead_time",bf_jv_get(q,"lead_time")?bf_jv_clone(bf_jv_get(q,"lead_time")):bf_jv_null());bf_jv_push(lines,line);
    }
    bf_jv_set(record,"lines",lines);bf_jv_set(record,"quote_accepted",bf_jv_bool(0));bf_jv_set(record,"reserved",bf_jv_bool(0));bf_jv_set(record,"order_authorized",bf_jv_bool(0));
    bf_jv_set(record,"does_not_establish",bf_jv_str("supplier acceptance, reservation, purchase approval, order, payment, settlement, fulfillment or shipment"));
    seal(record);*out=bf_jv_dump(record,0,BF_JV_SORT_NONE,NULL);bf_jv_free(record);bf_jv_free(root);return *out!=NULL;
}

static int quote_ok(const bf_jv *q,const bf_jv *rfq,char *err,size_t n) {
    const char *schema=sval(bf_jv_get(q,"schema")),*status=sval(bf_jv_get(q,"status"));
    const bf_jv *supplier=bf_jv_get(q,"supplier"),*currency=bf_jv_get(q,"currency"),*lines=bf_jv_get(q,"lines");
    long long money=0;int known=0;
    if(!bf_jv_is(q,BF_JV_OBJ)||!schema||strcmp(schema,"bonfyre.procurement.supplier-quote-observation.v1")||!status||strcmp(status,"observed")||
       !text_ok(bf_jv_get(q,"id"),128)||!supplier_ok(supplier)||!timestamp_ok(bf_jv_get(q,"observed_at"))||
       !timestamp_nullable(bf_jv_get(q,"quoted_at"))||!timestamp_nullable(bf_jv_get(q,"valid_until"))||!currency_ok(currency)||
       !text_ok(bf_jv_get(q,"source_ref"),4000)||!nullable_money(bf_jv_get(q,"freight_cents"),&money,&known)||
       !nullable_money(bf_jv_get(q,"tax_cents"),&money,&known)||!nullable_money(bf_jv_get(q,"other_fees_cents"),&money,&known)||
       !bf_jv_is(lines,BF_JV_ARR)||!lines->len||lines->len>MAX_LINES||
       !bool_is(bf_jv_get(q,"quote_accepted"),0)||!bool_is(bf_jv_get(q,"reserved"),0)||!bool_is(bf_jv_get(q,"order_authorized"),0)||
       !verify_digest(q,err,n))return fail(err,n,"invalid supplier quote observation");
    const bf_jv *s=bf_jv_get(q,"source"),*srid=bf_jv_get(s,"rfq_id"),*srd=bf_jv_get(s,"rfq_digest");
    if(!bf_jv_is(s,BF_JV_OBJ)||!text_ok(srid,128)||!hex64(srd)||strcmp(srid->s,sval(bf_jv_get(rfq,"id")))||strcmp(srd->s,sval(bf_jv_get(rfq,"digest"))))
       return fail(err,n,"quote observation is for another RFQ revision");
    for(size_t i=0;i<lines->len;i++){
        const bf_jv *line=bf_jv_at(lines,i),*rid=bf_jv_get(line,"rfq_line_id"),*sub=bf_jv_get(line,"substitution");
        long long qty=0,unit=0,total=0,extended=0;int uk=0,tk=0,ek=0;
        if(!bf_jv_is(line,BF_JV_OBJ)||!text_ok(bf_jv_get(line,"id"),128)||!hex64(rid)||!rfq_find_line(rfq,rid->s)||
           !safe_int(bf_jv_get(line,"quantity"),&qty,1)||!nullable_money(bf_jv_get(line,"unit_cents"),&unit,&uk)||
           !nullable_money(bf_jv_get(line,"line_total_cents"),&total,&tk)||!nullable_money(bf_jv_get(line,"known_extended_cents"),&extended,&ek)||
           !substitution_ok(sub)||(bf_jv_get(line,"availability")&&!bf_jv_is(bf_jv_get(line,"availability"),BF_JV_NULL)&&!text_ok(bf_jv_get(line,"availability"),512))||
           (bf_jv_get(line,"lead_time")&&!bf_jv_is(bf_jv_get(line,"lead_time"),BF_JV_NULL)&&!text_ok(bf_jv_get(line,"lead_time"),512)))
            return fail(err,n,"retained quote line is malformed");
        if(uk&&qty>SAFE_INT/unit)return fail(err,n,"retained quote line exceeds safe integer precision");
        long long calculated=uk?unit*qty:total;int calculated_known=uk||tk;
        if(uk&&tk&&calculated!=total)return fail(err,n,"retained quote line total conflicts with unit price and quantity");
        if(ek!=calculated_known||(ek&&extended!=calculated))return fail(err,n,"retained quote line known total conflicts with quoted money");
        for(size_t k=0;k<i;k++)if(!strcmp(sval(bf_jv_get(line,"id")),sval(bf_jv_get(bf_jv_at(lines,k),"id"))))return fail(err,n,"retained quote line id is duplicated");
    }
    return 1;
}

static int expected_stale(const bf_jv *root,const bf_jv *rfq,const bf_jv *quotes) {
    const bf_jv *erd=bf_jv_get(root,"expected_rfq_digest");
    if(erd&&!bf_jv_is(erd,BF_JV_NULL)&&(!hex64(erd)||strcmp(erd->s,sval(bf_jv_get(rfq,"digest")))))return 1;
    const bf_jv *expected=bf_jv_get(root,"expected_quotes"); if(!expected||bf_jv_is(expected,BF_JV_NULL))return 0;
    if(!bf_jv_is(expected,BF_JV_ARR)||expected->len>MAX_QUOTES)return 1;
    for(size_t i=0;i<expected->len;i++){
        const bf_jv *e=bf_jv_at(expected,i);if(!bf_jv_is(e,BF_JV_OBJ)||!text_ok(bf_jv_get(e,"id"),128)||!hex64(bf_jv_get(e,"digest")))return 1;
        int found=0;for(size_t k=0;k<quotes->len;k++){const bf_jv *q=bf_jv_at(quotes,k);if(!strcmp(sval(bf_jv_get(q,"id")),sval(bf_jv_get(e,"id")))){found=1;if(strcmp(sval(bf_jv_get(q,"digest")),sval(bf_jv_get(e,"digest"))))return 1;}}
        if(!found)return 1;
    }
    return 0;
}

int bf_procurement_compare(const char *json,size_t len,const char *as_of,char **out,char *err,size_t n) {
    *out=NULL;bf_jv *root=parse_root(json,len,err,n);if(!root)return 0;
    const bf_jv *rfq=bf_jv_get(root,"rfq"),*quotes=bf_jv_get(root,"quotes");
    bf_jv asv={0};if(as_of){asv.type=BF_JV_STR;asv.s=(char*)as_of;asv.len=strlen(as_of);if(!timestamp_ok(&asv)){bf_jv_free(root);return fail(err,n,"--as-of must be UTC RFC3339 seconds");}}
    if(!rfq_ok(rfq,err,n)||!bf_jv_is(quotes,BF_JV_ARR)||!quotes->len||quotes->len>MAX_QUOTES){bf_jv_free(root);return fail(err,n,err[0]?err:"comparison requires 1..32 quotes");}
    for(size_t i=0;i<quotes->len;i++){if(!quote_ok(bf_jv_at(quotes,i),rfq,err,n)){bf_jv_free(root);return 0;}for(size_t k=0;k<i;k++)if(!strcmp(sval(bf_jv_get(bf_jv_at(quotes,i),"id")),sval(bf_jv_get(bf_jv_at(quotes,k),"id")))){bf_jv_free(root);return fail(err,n,"comparison contains duplicate quote observation id");}}
    bf_jv *record=bf_jv_obj(),*src=bf_jv_obj(),*summaries=bf_jv_arr(),*coverage=bf_jv_arr(),*currencies=bf_jv_arr();
    bf_jv_set(record,"schema",bf_jv_str("bonfyre.procurement.quote-comparison.v1"));bf_jv_set(src,"rfq_id",bf_jv_clone(bf_jv_get(rfq,"id")));bf_jv_set(src,"rfq_digest",bf_jv_clone(bf_jv_get(rfq,"digest")));bf_jv_set(record,"source",src);bf_jv_set(record,"as_of",as_of?bf_jv_str(as_of):bf_jv_null());
    int any_unpriced=0,stale=expected_stale(root,rfq,quotes),partial=0,eligible_count=0;
    int eligible[MAX_QUOTES]={0};
    for(size_t i=0;i<quotes->len;i++){
        const bf_jv *q=bf_jv_at(quotes,i),*cur=bf_jv_get(q,"currency"),*lines=bf_jv_get(q,"lines"),*valid=bf_jv_get(q,"valid_until");int seen=0,expired=0;
        if(as_of&&bf_jv_is(valid,BF_JV_STR))expired=strcmp(valid->s,as_of)<0;
        eligible[i]=!expired;if(eligible[i])eligible_count++;
        if(eligible[i]){for(size_t c=0;c<currencies->len;c++)if(!strcmp(cur->s,sval(bf_jv_at(currencies,c))))seen=1;if(!seen)bf_jv_push(currencies,bf_jv_clone(cur));}
        long long total=0;int fully=1;
        for(size_t j=0;j<lines->len;j++){const bf_jv *e=bf_jv_get(bf_jv_at(lines,j),"known_extended_cents");long long v=0;if(!e||bf_jv_is(e,BF_JV_NULL)){fully=0;continue;}if(!safe_int(e,&v,0)||total>SAFE_INT-v){fully=0;continue;}total+=v;}
        const char *fees[]={"freight_cents","tax_cents","other_fees_cents",NULL};for(const char **f=fees;*f;f++){const bf_jv *v=bf_jv_get(q,*f);long long x=0;if(!v||bf_jv_is(v,BF_JV_NULL)){fully=0;continue;}if(!safe_int(v,&x,0)||total>SAFE_INT-x){fully=0;continue;}total+=x;}
        if(eligible[i]&&!fully)any_unpriced=1;bf_jv *s=bf_jv_obj();bf_jv_set(s,"quote_id",bf_jv_clone(bf_jv_get(q,"id")));bf_jv_set(s,"digest",bf_jv_clone(bf_jv_get(q,"digest")));bf_jv_set(s,"supplier",bf_jv_clone(bf_jv_get(q,"supplier")));bf_jv_set(s,"currency",bf_jv_clone(cur));bf_jv_set(s,"known_total_cents",fully?bf_jv_num((double)total):bf_jv_null());
        if(as_of&&bf_jv_is(valid,BF_JV_STR))bf_jv_set(s,"expired",bf_jv_bool(expired));else bf_jv_set(s,"expired",bf_jv_null());
        bf_jv_set(s,"source_ref",bf_jv_clone(bf_jv_get(q,"source_ref")));bf_jv_push(summaries,s);
    }
    int mixed=currencies->len>1;const bf_jv *rlines=bf_jv_get(rfq,"lines");
    for(size_t i=0;i<rlines->len;i++){
        const bf_jv *rline=bf_jv_at(rlines,i);long long requested=0,totalq=0,subs=0;safe_int(bf_jv_get(rline,"quantity_requested"),&requested,1);bf_jv *sublist=bf_jv_arr();
        for(size_t qi=0;qi<quotes->len;qi++){if(!eligible[qi])continue;const bf_jv *ql=bf_jv_get(bf_jv_at(quotes,qi),"lines");for(size_t j=0;j<ql->len;j++){const bf_jv *line=bf_jv_at(ql,j);if(strcmp(sval(bf_jv_get(line,"rfq_line_id")),sval(bf_jv_get(rline,"line_id"))))continue;long long qty=0;safe_int(bf_jv_get(line,"quantity"),&qty,1);const bf_jv *sub=bf_jv_get(line,"substitution");if(sub&&!bf_jv_is(sub,BF_JV_NULL)){subs+=qty;bf_jv *x=bf_jv_obj();bf_jv_set(x,"quote_id",bf_jv_clone(bf_jv_get(bf_jv_at(quotes,qi),"id")));bf_jv_set(x,"quantity",bf_jv_num((double)qty));bf_jv_set(x,"product",bf_jv_clone(sub));bf_jv_push(sublist,x);}else if(totalq<=SAFE_INT-qty)totalq+=qty;}}
        long long covered=totalq>requested?requested:totalq,uncovered=requested-covered;if(uncovered>0)partial=1;bf_jv *c=bf_jv_obj();
        bf_jv_set(c,"rfq_line_id",bf_jv_clone(bf_jv_get(rline,"line_id")));bf_jv_set(c,"quantity_requested",bf_jv_num((double)requested));bf_jv_set(c,"exact_quantity_quoted",bf_jv_num((double)totalq));bf_jv_set(c,"potential_exact_covered",bf_jv_num((double)covered));bf_jv_set(c,"uncovered_quantity",bf_jv_num((double)uncovered));bf_jv_set(c,"substitution_quantity",bf_jv_num((double)subs));bf_jv_set(c,"substitutions",sublist);bf_jv_push(coverage,c);
    }
    bf_jv_set(record,"quotes",summaries);bf_jv_set(record,"coverage",coverage);bf_jv_set(record,"currencies",currencies);bf_jv_set(record,"eligible_quote_count",bf_jv_num((double)eligible_count));bf_jv_set(record,"expired_quote_count",bf_jv_num((double)(quotes->len-(size_t)eligible_count)));bf_jv_set(record,"mixed_currency",bf_jv_bool(mixed));bf_jv_set(record,"stale",bf_jv_bool(stale));
    const char *status=stale?"stale":mixed?"mixed_currency":partial?"partial":any_unpriced?"unpriced":"current";bf_jv_set(record,"status",bf_jv_str(status));
    bf_jv_set(record,"approved",bf_jv_bool(0));bf_jv_set(record,"committed",bf_jv_bool(0));bf_jv_set(record,"ordered",bf_jv_bool(0));bf_jv_set(record,"does_not_establish",bf_jv_str("quote acceptance, purchase approval, allocation, reservation, commitment, order, payment, settlement, fulfillment or shipment"));
    seal(record);*out=bf_jv_dump(record,0,BF_JV_SORT_NONE,NULL);bf_jv_free(record);bf_jv_free(root);return *out!=NULL;
}


static int comparison_ok(const bf_jv *comparison,const bf_jv *rfq,const bf_jv *quotes,char *err,size_t n) {
    const char *schema=sval(bf_jv_get(comparison,"schema")),*status=sval(bf_jv_get(comparison,"status"));
    if(!bf_jv_is(comparison,BF_JV_OBJ)||!schema||strcmp(schema,"bonfyre.procurement.quote-comparison.v1")||!status||
       (strcmp(status,"current")&&strcmp(status,"partial")&&strcmp(status,"unpriced")&&strcmp(status,"mixed_currency")&&strcmp(status,"stale"))||
       !verify_digest(comparison,err,n)||!bool_is(bf_jv_get(comparison,"approved"),0)||!bool_is(bf_jv_get(comparison,"committed"),0)||!bool_is(bf_jv_get(comparison,"ordered"),0))
        return fail(err,n,"invalid retained quote comparison");
    const bf_jv *source=bf_jv_get(comparison,"source"),*summaries=bf_jv_get(comparison,"quotes"),*srid=bf_jv_get(source,"rfq_id"),*srd=bf_jv_get(source,"rfq_digest");
    if(!bf_jv_is(source,BF_JV_OBJ)||!text_ok(srid,128)||!hex64(srd)||strcmp(srid->s,sval(bf_jv_get(rfq,"id")))||
       strcmp(srd->s,sval(bf_jv_get(rfq,"digest")))||!bf_jv_is(summaries,BF_JV_ARR)||summaries->len!=quotes->len)
        return fail(err,n,"comparison lineage no longer matches the RFQ and quote set");
    for(size_t i=0;i<summaries->len;i++){
        const bf_jv *summary=bf_jv_at(summaries,i),*sid=bf_jv_get(summary,"quote_id"),*sd=bf_jv_get(summary,"digest");int found=0;
        if(!text_ok(sid,128)||!hex64(sd))return fail(err,n,"comparison quote lineage is invalid");
        for(size_t q=0;q<quotes->len;q++){const bf_jv *quote=bf_jv_at(quotes,q);if(!strcmp(sid->s,sval(bf_jv_get(quote,"id")))){found=1;if(strcmp(sd->s,sval(bf_jv_get(quote,"digest"))))return fail(err,n,"comparison quote digest changed");}}
        if(!found)return fail(err,n,"comparison references a missing quote");
    }
    return 1;
}

static int quote_expired_at(const bf_jv *quote,const char *at) {
    const bf_jv *valid=bf_jv_get(quote,"valid_until");
    return at&&bf_jv_is(valid,BF_JV_STR)&&strcmp(valid->s,at)<0;
}

static const bf_jv *quote_line_at(const bf_jv *quote,size_t index) {
    const bf_jv *lines=bf_jv_get(quote,"lines");
    return bf_jv_is(lines,BF_JV_ARR)&&index<lines->len?bf_jv_at(lines,index):NULL;
}

int bf_procurement_allocate(const char *json,size_t len,char **out,char *err,size_t n) {
    *out=NULL;bf_jv *root=parse_root(json,len,err,n);if(!root)return 0;
    const bf_jv *rfq=bf_jv_get(root,"rfq"),*quotes=bf_jv_get(root,"quotes"),*comparison=bf_jv_get(root,"comparison");
    const bf_jv *id=bf_jv_get(root,"id"),*prepared=bf_jv_get(root,"prepared_at"),*policy=bf_jv_get(root,"policy");
    if(!rfq_ok(rfq,err,n)||!bf_jv_is(quotes,BF_JV_ARR)||!quotes->len||quotes->len>MAX_QUOTES||!text_ok(id,128)||!timestamp_ok(prepared)||
       !bf_jv_is(policy,BF_JV_STR)||strcmp(policy->s,"lowest_known_merchandise")){bf_jv_free(root);return fail(err,n,err[0]?err:"allocation input is invalid");}
    for(size_t i=0;i<quotes->len;i++)if(!quote_ok(bf_jv_at(quotes,i),rfq,err,n)){bf_jv_free(root);return 0;}
    if(!comparison_ok(comparison,rfq,quotes,err,n)){bf_jv_free(root);return 0;}
    char *recheck_input=bf_jv_dump(root,0,BF_JV_SORT_NONE,NULL),*recheck_json=NULL;
    if(!recheck_input||!bf_procurement_compare(recheck_input,strlen(recheck_input),prepared->s,&recheck_json,err,n)){free(recheck_input);free(recheck_json);bf_jv_free(root);return fail(err,n,err[0]?err:"quote comparison could not be rechecked");}
    free(recheck_input);
    bf_jv *recheck=parse_root(recheck_json,strlen(recheck_json),err,n);free(recheck_json);
    if(!recheck){bf_jv_free(root);return 0;}
    if(strcmp(sval(bf_jv_get(recheck,"status")),"current")){bf_jv_free(recheck);bf_jv_free(root);return fail(err,n,"quote set is no longer current enough to allocate");}
    long long *used=calloc(MAX_QUOTES*MAX_LINES,sizeof(*used));if(!used){bf_jv_free(recheck);bf_jv_free(root);return fail(err,n,"allocation memory unavailable");}
    bf_jv *record=bf_jv_obj(),*source=bf_jv_obj(),*allocations=bf_jv_arr(),*selected=bf_jv_arr(),*order_lots=bf_jv_arr(),*commerce=bf_jv_obj();
    long long merchandise=0;char currency[4]={0};const bf_jv *rlines=bf_jv_get(rfq,"lines");
    for(size_t ri=0;ri<rlines->len;ri++){
        const bf_jv *rline=bf_jv_at(rlines,ri),*rid=bf_jv_get(rline,"line_id");long long requested=0,remaining=0;
        safe_int(bf_jv_get(rline,"quantity_requested"),&requested,1);remaining=requested;
        while(remaining>0){
            size_t best_q=MAX_QUOTES,best_l=MAX_LINES;long long best_unit=0,best_available=0;
            for(size_t qi=0;qi<quotes->len;qi++){
                const bf_jv *quote=bf_jv_at(quotes,qi);if(quote_expired_at(quote,prepared->s))continue;
                const bf_jv *qlines=bf_jv_get(quote,"lines");
                for(size_t li=0;li<qlines->len;li++){
                    const bf_jv *line=bf_jv_at(qlines,li),*lrid=bf_jv_get(line,"rfq_line_id"),*sub=bf_jv_get(line,"substitution"),*unitv=bf_jv_get(line,"unit_cents");
                    long long quantity=0,unit=0,available=0;if(strcmp(sval(lrid),rid->s)||!safe_int(bf_jv_get(line,"quantity"),&quantity,1)||!safe_int(unitv,&unit,0)||!(!sub||bf_jv_is(sub,BF_JV_NULL)))continue;
                    available=quantity-used[qi*MAX_LINES+li];if(available<=0)continue;
                    int better=best_q==MAX_QUOTES||unit<best_unit;
                    if(!better&&unit==best_unit){const char *qid=sval(bf_jv_get(quote,"id")),*bqid=sval(bf_jv_get(bf_jv_at(quotes,best_q),"id"));int cmp=strcmp(qid,bqid);if(cmp<0)better=1;else if(!cmp&&strcmp(sval(bf_jv_get(line,"id")),sval(bf_jv_get(quote_line_at(bf_jv_at(quotes,best_q),best_l),"id")))<0)better=1;}
                    if(better){best_q=qi;best_l=li;best_unit=unit;best_available=available;}
                }
            }
            if(best_q==MAX_QUOTES){free(used);bf_jv_free(commerce);bf_jv_free(selected);bf_jv_free(allocations);bf_jv_free(source);bf_jv_free(record);bf_jv_free(recheck);bf_jv_free(root);return fail(err,n,"no still-valid exact quote line with a known unit price can cover the requested quantity");}
            const bf_jv *quote=bf_jv_at(quotes,best_q),*line=quote_line_at(quote,best_l),*cur=bf_jv_get(quote,"currency");long long take=remaining<best_available?remaining:best_available;
            if((best_unit&&take>SAFE_INT/best_unit)||merchandise>SAFE_INT-(take*best_unit)){free(used);bf_jv_free(commerce);bf_jv_free(selected);bf_jv_free(allocations);bf_jv_free(source);bf_jv_free(record);bf_jv_free(recheck);bf_jv_free(root);return fail(err,n,"allocation merchandise total exceeds safe integer precision");}
            if(!currency[0])snprintf(currency,sizeof currency,"%s",cur->s);else if(strcmp(currency,cur->s)){free(used);bf_jv_free(commerce);bf_jv_free(selected);bf_jv_free(allocations);bf_jv_free(source);bf_jv_free(record);bf_jv_free(recheck);bf_jv_free(root);return fail(err,n,"allocation cannot mix currencies");}
            bf_jv *a=bf_jv_obj();bf_jv_set(a,"rfq_line_id",bf_jv_clone(rid));bf_jv_set(a,"quote_id",bf_jv_clone(bf_jv_get(quote,"id")));bf_jv_set(a,"quote_digest",bf_jv_clone(bf_jv_get(quote,"digest")));bf_jv_set(a,"quote_line_id",bf_jv_clone(bf_jv_get(line,"id")));bf_jv_set(a,"supplier",bf_jv_clone(bf_jv_get(quote,"supplier")));bf_jv_set(a,"quantity",bf_jv_num((double)take));bf_jv_set(a,"unit_cents",bf_jv_num((double)best_unit));bf_jv_set(a,"merchandise_cents",bf_jv_num((double)(take*best_unit)));bf_jv_push(allocations,a);
            int seen=0;for(size_t si=0;si<selected->len;si++)if(!strcmp(sval(bf_jv_get(bf_jv_at(selected,si),"quote_id")),sval(bf_jv_get(quote,"id"))))seen=1;
            if(!seen){bf_jv *s=bf_jv_obj();bf_jv_set(s,"quote_id",bf_jv_clone(bf_jv_get(quote,"id")));bf_jv_set(s,"digest",bf_jv_clone(bf_jv_get(quote,"digest")));bf_jv_set(s,"supplier",bf_jv_clone(bf_jv_get(quote,"supplier")));bf_jv_set(s,"source_ref",bf_jv_clone(bf_jv_get(quote,"source_ref")));bf_jv_set(s,"valid_until",bf_jv_clone(bf_jv_get(quote,"valid_until")));bf_jv_set(s,"freight_cents",bf_jv_clone(bf_jv_get(quote,"freight_cents")));bf_jv_set(s,"tax_cents",bf_jv_clone(bf_jv_get(quote,"tax_cents")));bf_jv_set(s,"other_fees_cents",bf_jv_clone(bf_jv_get(quote,"other_fees_cents")));bf_jv_set(s,"fees_attributed",bf_jv_bool(0));bf_jv_push(selected,s);}
            used[best_q*MAX_LINES+best_l]+=take;merchandise+=take*best_unit;remaining-=take;
        }
    }
    long long known_total=merchandise;int all_lots_priced=1;
    for(size_t si=0;si<selected->len;si++){
        bf_jv *sel=bf_jv_at(selected,si);const char *qid=sval(bf_jv_get(sel,"quote_id"));size_t qi=MAX_QUOTES;
        for(size_t q=0;q<quotes->len;q++)if(!strcmp(qid,sval(bf_jv_get(bf_jv_at(quotes,q),"id")))){qi=q;break;}
        int full=qi<quotes->len,fees_known=full;long long lot_merch=0,fees=0;
        const bf_jv *quote=full?bf_jv_at(quotes,qi):NULL,*qlines=quote?bf_jv_get(quote,"lines"):NULL;
        if(!bf_jv_is(qlines,BF_JV_ARR))full=0;
        for(size_t li=0;full&&li<qlines->len;li++){
            const bf_jv *line=bf_jv_at(qlines,li),*sub=bf_jv_get(line,"substitution");long long qty=0,unit=0;
            if((sub&&!bf_jv_is(sub,BF_JV_NULL))||!safe_int(bf_jv_get(line,"quantity"),&qty,1)||!safe_int(bf_jv_get(line,"unit_cents"),&unit,0)||
               used[qi*MAX_LINES+li]!=qty||(unit&&qty>SAFE_INT/unit)||lot_merch>SAFE_INT-(qty*unit)){full=0;break;}
            lot_merch+=qty*unit;
        }
        const char *fee_names[]={"freight_cents","tax_cents","other_fees_cents",NULL};
        for(const char **f=fee_names;fees_known&&*f;f++){long long amount=0;const bf_jv *v=quote?bf_jv_get(quote,*f):NULL;if(!v||bf_jv_is(v,BF_JV_NULL)||!safe_int(v,&amount,0)||fees>SAFE_INT-amount)fees_known=0;else fees+=amount;}
        int priced=full&&fees_known&&lot_merch<=SAFE_INT-fees;
        if(!priced)all_lots_priced=0;else if(known_total>SAFE_INT-fees)all_lots_priced=0;else known_total+=fees;
        bf_jv_set(sel,"fees_attributed",bf_jv_bool(priced));
        bf_jv *lot=bf_jv_obj();bf_jv_set(lot,"quote_id",bf_jv_clone(bf_jv_get(sel,"quote_id")));bf_jv_set(lot,"digest",bf_jv_clone(bf_jv_get(sel,"digest")));
        bf_jv_set(lot,"supplier",bf_jv_clone(bf_jv_get(sel,"supplier")));bf_jv_set(lot,"source_ref",bf_jv_clone(bf_jv_get(sel,"source_ref")));bf_jv_set(lot,"valid_until",bf_jv_clone(bf_jv_get(sel,"valid_until")));
        bf_jv_set(lot,"merchandise_cents",bf_jv_num((double)lot_merch));bf_jv_set(lot,"full_quote_consumed",bf_jv_bool(full));
        bf_jv_set(lot,"freight_cents",bf_jv_clone(bf_jv_get(sel,"freight_cents")));bf_jv_set(lot,"tax_cents",bf_jv_clone(bf_jv_get(sel,"tax_cents")));bf_jv_set(lot,"other_fees_cents",bf_jv_clone(bf_jv_get(sel,"other_fees_cents")));
        bf_jv_set(lot,"known_total_cents",priced?bf_jv_num((double)(lot_merch+fees)):bf_jv_null());bf_jv_push(order_lots,lot);
    }
    free(used);
    bf_jv_set(record,"schema",bf_jv_str("bonfyre.procurement.allocation-plan.v1"));bf_jv_set(record,"id",bf_jv_clone(id));bf_jv_set(record,"status",bf_jv_str("prepared"));bf_jv_set(record,"prepared_at",bf_jv_clone(prepared));bf_jv_set(record,"policy",bf_jv_clone(policy));
    bf_jv_set(source,"comparison_digest",bf_jv_clone(bf_jv_get(comparison,"digest")));bf_jv_set(source,"rfq_id",bf_jv_clone(bf_jv_get(rfq,"id")));bf_jv_set(source,"rfq_digest",bf_jv_clone(bf_jv_get(rfq,"digest")));bf_jv_set(source,"recheck_digest",bf_jv_clone(bf_jv_get(recheck,"digest")));bf_jv_set(record,"source",source);
    bf_jv_set(record,"currency",bf_jv_str(currency));bf_jv_set(record,"allocations",allocations);bf_jv_set(record,"selected_quotes",selected);bf_jv_set(record,"order_lots",order_lots);bf_jv_set(record,"known_merchandise_cents",bf_jv_num((double)merchandise));bf_jv_set(record,"known_total_cents",all_lots_priced?bf_jv_num((double)known_total):bf_jv_null());bf_jv_set(record,"fees_attribution",bf_jv_str(all_lots_priced?"full_quote":"unallocated"));
    bf_jv_set(record,"approval_required",bf_jv_bool(1));bf_jv_set(record,"approved",bf_jv_bool(0));bf_jv_set(record,"cart_created",bf_jv_bool(0));bf_jv_set(record,"commitment_opened",bf_jv_bool(0));bf_jv_set(record,"order_created",bf_jv_bool(0));
    char exchange[160];snprintf(exchange,sizeof exchange,"procurement:%s",id->s);bf_jv_set(commerce,"owner",bf_jv_str("CommerceChain"));bf_jv_set(commerce,"exchange_candidate",bf_jv_str(exchange));bf_jv_set(commerce,"status",bf_jv_str("unmaterialized"));bf_jv_set(commerce,"next_required_stage",bf_jv_str("product"));bf_jv_set(record,"commerce_chain",commerce);
    bf_jv_set(record,"optimization_scope",bf_jv_str("lowest known exact-line merchandise only; observed freight, tax and other quote-level fees remain unattributed and do not affect selection"));
    bf_jv_set(record,"does_not_establish",bf_jv_str("CommerceChain product/listing/inventory/availability/offer/quote/cart stages, quote acceptance, purchase approval, reservation, commitment, purchase order, payment, settlement, fulfillment, delivery, satisfaction or ownership"));
    seal(record);*out=bf_jv_dump(record,0,BF_JV_SORT_NONE,NULL);bf_jv_free(record);bf_jv_free(recheck);bf_jv_free(root);return *out!=NULL;
}


static int allocation_ok(const bf_jv *allocation,char *err,size_t n) {
    const char *schema=sval(bf_jv_get(allocation,"schema")),*status=sval(bf_jv_get(allocation,"status")),*policy=sval(bf_jv_get(allocation,"policy"));
    long long merchandise=0,known_total=0;const bf_jv *known_total_v=bf_jv_get(allocation,"known_total_cents");const char *fees=sval(bf_jv_get(allocation,"fees_attribution"));int total_known=safe_int(known_total_v,&known_total,0);
    if(!bf_jv_is(allocation,BF_JV_OBJ)||!schema||strcmp(schema,"bonfyre.procurement.allocation-plan.v1")||!status||strcmp(status,"prepared")||
       !text_ok(bf_jv_get(allocation,"id"),128)||!timestamp_ok(bf_jv_get(allocation,"prepared_at"))||!policy||strcmp(policy,"lowest_known_merchandise")||
       !currency_ok(bf_jv_get(allocation,"currency"))||!safe_int(bf_jv_get(allocation,"known_merchandise_cents"),&merchandise,0)||
       (!total_known&&!bf_jv_is(known_total_v,BF_JV_NULL))||!fees||((total_known&&strcmp(fees,"full_quote"))||(!total_known&&strcmp(fees,"unallocated")))||(total_known&&known_total<merchandise)||
       !bool_is(bf_jv_get(allocation,"approval_required"),1)||!bool_is(bf_jv_get(allocation,"approved"),0)||
       !bool_is(bf_jv_get(allocation,"cart_created"),0)||!bool_is(bf_jv_get(allocation,"commitment_opened"),0)||!bool_is(bf_jv_get(allocation,"order_created"),0)||
       !verify_digest(allocation,err,n)) return fail(err,n,"invalid retained allocation plan");
    const bf_jv *source=bf_jv_get(allocation,"source"),*allocations=bf_jv_get(allocation,"allocations"),*selected=bf_jv_get(allocation,"selected_quotes"),*lots=bf_jv_get(allocation,"order_lots"),*commerce=bf_jv_get(allocation,"commerce_chain");
    if(!bf_jv_is(source,BF_JV_OBJ)||!hex64(bf_jv_get(source,"comparison_digest"))||!text_ok(bf_jv_get(source,"rfq_id"),128)||!hex64(bf_jv_get(source,"rfq_digest"))||!hex64(bf_jv_get(source,"recheck_digest"))||
       !bf_jv_is(allocations,BF_JV_ARR)||!allocations->len||allocations->len>MAX_LINES||
       !bf_jv_is(selected,BF_JV_ARR)||!selected->len||selected->len>MAX_QUOTES||!bf_jv_is(lots,BF_JV_ARR)||lots->len!=selected->len||
       !bf_jv_is(commerce,BF_JV_OBJ)||strcmp(sval(bf_jv_get(commerce,"owner"))?sval(bf_jv_get(commerce,"owner")):"","CommerceChain")||
       strcmp(sval(bf_jv_get(commerce,"status"))?sval(bf_jv_get(commerce,"status")):"","unmaterialized")||
       strcmp(sval(bf_jv_get(commerce,"next_required_stage"))?sval(bf_jv_get(commerce,"next_required_stage")):"","product"))
        return fail(err,n,"allocation lineage or commerce boundary is invalid");
    for(size_t i=0;i<allocations->len;i++){
        const bf_jv *row=bf_jv_at(allocations,i);long long quantity=0,unit=0,total=0;
        if(!bf_jv_is(row,BF_JV_OBJ)||!hex64(bf_jv_get(row,"rfq_line_id"))||!text_ok(bf_jv_get(row,"quote_id"),128)||!hex64(bf_jv_get(row,"quote_digest"))||
           !text_ok(bf_jv_get(row,"quote_line_id"),128)||!supplier_ok(bf_jv_get(row,"supplier"))||
           !safe_int(bf_jv_get(row,"quantity"),&quantity,1)||!safe_int(bf_jv_get(row,"unit_cents"),&unit,0)||!safe_int(bf_jv_get(row,"merchandise_cents"),&total,0)||
           (unit&&quantity>SAFE_INT/unit)||unit*quantity!=total) return fail(err,n,"allocation row is malformed");
    }
    for(size_t i=0;i<selected->len;i++){
        const bf_jv *row=bf_jv_at(selected,i);
        if(!bf_jv_is(row,BF_JV_OBJ)||!text_ok(bf_jv_get(row,"quote_id"),128)||!hex64(bf_jv_get(row,"digest"))||!supplier_ok(bf_jv_get(row,"supplier"))||
           !text_ok(bf_jv_get(row,"source_ref"),4000)||!timestamp_nullable(bf_jv_get(row,"valid_until"))||!bf_jv_is(bf_jv_get(row,"fees_attributed"),BF_JV_BOOL))
            return fail(err,n,"selected quote lineage is malformed");
    }
    for(size_t i=0;i<lots->len;i++){
        const bf_jv *lot=bf_jv_at(lots,i),*lt=bf_jv_get(lot,"known_total_cents");long long lm=0,lv=0;int lk=safe_int(lt,&lv,0);
        if(!bf_jv_is(lot,BF_JV_OBJ)||!text_ok(bf_jv_get(lot,"quote_id"),128)||!hex64(bf_jv_get(lot,"digest"))||!supplier_ok(bf_jv_get(lot,"supplier"))||
           !text_ok(bf_jv_get(lot,"source_ref"),4000)||!timestamp_nullable(bf_jv_get(lot,"valid_until"))||!safe_int(bf_jv_get(lot,"merchandise_cents"),&lm,0)||
           !bf_jv_is(bf_jv_get(lot,"full_quote_consumed"),BF_JV_BOOL)||(!lk&&!bf_jv_is(lt,BF_JV_NULL))||(lk&&lv<lm))return fail(err,n,"order lot is malformed");
    }
    return 1;
}

int bf_procurement_approval_request(const char *json,size_t len,char **out,char *err,size_t n) {
    *out=NULL;bf_jv *root=parse_root(json,len,err,n);if(!root)return 0;
    const bf_jv *allocation=bf_jv_get(root,"allocation"),*revisionv=bf_jv_get(root,"allocation_revision"),*id=bf_jv_get(root,"id"),*requested=bf_jv_get(root,"requested_at"),*requester=bf_jv_get(root,"requester"),*reason=bf_jv_get(root,"reason");
    long long revision=0;
    if(!allocation_ok(allocation,err,n)||!safe_int(revisionv,&revision,1)||!text_ok(id,128)||!timestamp_ok(requested)||!text_ok(requester,256)||
       (reason&&!bf_jv_is(reason,BF_JV_NULL)&&!text_ok(reason,2000))){bf_jv_free(root);return fail(err,n,err[0]?err:"approval request input is invalid");}
    bf_jv *record=bf_jv_obj(),*source=bf_jv_obj(),*authority=bf_jv_obj(),*permissions=bf_jv_arr();
    bf_jv_set(record,"schema",bf_jv_str("bonfyre.procurement.approval-request.v1"));bf_jv_set(record,"id",bf_jv_clone(id));bf_jv_set(record,"status",bf_jv_str("pending"));bf_jv_set(record,"requested_at",bf_jv_clone(requested));bf_jv_set(record,"requester",bf_jv_clone(requester));bf_jv_set(record,"reason",reason?bf_jv_clone(reason):bf_jv_null());
    bf_jv_set(source,"allocation_id",bf_jv_clone(bf_jv_get(allocation,"id")));bf_jv_set(source,"revision",bf_jv_num((double)revision));bf_jv_set(source,"digest",bf_jv_clone(bf_jv_get(allocation,"digest")));bf_jv_set(record,"source",source);
    bf_jv_set(record,"currency",bf_jv_clone(bf_jv_get(allocation,"currency")));bf_jv_set(record,"known_merchandise_cents",bf_jv_clone(bf_jv_get(allocation,"known_merchandise_cents")));bf_jv_set(record,"known_total_cents",bf_jv_clone(bf_jv_get(allocation,"known_total_cents")));bf_jv_set(record,"fees_attribution",bf_jv_clone(bf_jv_get(allocation,"fees_attribution")));bf_jv_set(record,"selected_quotes",bf_jv_clone(bf_jv_get(allocation,"selected_quotes")));bf_jv_set(record,"order_lots",bf_jv_clone(bf_jv_get(allocation,"order_lots")));
    bf_jv_set(authority,"owner",bf_jv_str("AuthorityGraph"));char subject[192];snprintf(subject,sizeof subject,"procurement-allocation:%s",sval(bf_jv_get(allocation,"id")));bf_jv_set(authority,"subject",bf_jv_str(subject));bf_jv_push(permissions,bf_jv_str("approve"));bf_jv_push(permissions,bf_jv_str("spend"));bf_jv_set(authority,"required_permissions",permissions);bf_jv_set(authority,"decision",bf_jv_str("pending"));bf_jv_set(authority,"decided_by",bf_jv_null());bf_jv_set(authority,"decision_ref",bf_jv_null());bf_jv_set(record,"authority",authority);
    bf_jv_set(record,"approved",bf_jv_bool(0));bf_jv_set(record,"spend_authorized",bf_jv_bool(0));bf_jv_set(record,"quote_accepted",bf_jv_bool(0));bf_jv_set(record,"commitment_opened",bf_jv_bool(0));bf_jv_set(record,"order_created",bf_jv_bool(0));bf_jv_set(record,"payment_presented",bf_jv_bool(0));
    bf_jv_set(record,"does_not_establish",bf_jv_str("AuthorityGraph approval or spend authority, quote acceptance, reservation, cart, commitment, purchase order, payment, settlement, fulfillment, delivery, satisfaction or ownership"));
    seal(record);*out=bf_jv_dump(record,0,BF_JV_SORT_NONE,NULL);bf_jv_free(record);bf_jv_free(root);return *out!=NULL;
}
static int approval_request_ok(const bf_jv *approval,char *err,size_t n) {
    const char *schema=sval(bf_jv_get(approval,"schema")),*status=sval(bf_jv_get(approval,"status")),*fees=sval(bf_jv_get(approval,"fees_attribution"));
    long long merchandise=0,total=0,revision=0;const bf_jv *totalv=bf_jv_get(approval,"known_total_cents");int total_known=safe_int(totalv,&total,0);
    if(!bf_jv_is(approval,BF_JV_OBJ)||!schema||strcmp(schema,"bonfyre.procurement.approval-request.v1")||!status||strcmp(status,"pending")||
       !text_ok(bf_jv_get(approval,"id"),128)||!timestamp_ok(bf_jv_get(approval,"requested_at"))||!text_ok(bf_jv_get(approval,"requester"),256)||
       !currency_ok(bf_jv_get(approval,"currency"))||!safe_int(bf_jv_get(approval,"known_merchandise_cents"),&merchandise,0)||
       (!total_known&&!bf_jv_is(totalv,BF_JV_NULL))||!fees||((total_known&&strcmp(fees,"full_quote"))||(!total_known&&strcmp(fees,"unallocated")))||(total_known&&total<merchandise)||
       !bool_is(bf_jv_get(approval,"approved"),0)||!bool_is(bf_jv_get(approval,"spend_authorized"),0)||
       !bool_is(bf_jv_get(approval,"quote_accepted"),0)||!bool_is(bf_jv_get(approval,"commitment_opened"),0)||
       !bool_is(bf_jv_get(approval,"order_created"),0)||!bool_is(bf_jv_get(approval,"payment_presented"),0)||
       !verify_digest(approval,err,n)) return fail(err,n,"invalid retained approval request");
    const bf_jv *source=bf_jv_get(approval,"source"),*authority=bf_jv_get(approval,"authority"),*selected=bf_jv_get(approval,"selected_quotes"),*lots=bf_jv_get(approval,"order_lots");
    if(!bf_jv_is(source,BF_JV_OBJ)||!text_ok(bf_jv_get(source,"allocation_id"),128)||!safe_int(bf_jv_get(source,"revision"),&revision,1)||
       !hex64(bf_jv_get(source,"digest"))||!bf_jv_is(selected,BF_JV_ARR)||!selected->len||selected->len>MAX_QUOTES||!bf_jv_is(lots,BF_JV_ARR)||lots->len!=selected->len||
       !bf_jv_is(authority,BF_JV_OBJ)||strcmp(sval(bf_jv_get(authority,"owner"))?sval(bf_jv_get(authority,"owner")):"","AuthorityGraph")||
       !text_ok(bf_jv_get(authority,"subject"),256)||strcmp(sval(bf_jv_get(authority,"decision"))?sval(bf_jv_get(authority,"decision")):"","pending")||
       !bf_jv_is(bf_jv_get(authority,"decided_by"),BF_JV_NULL)||!bf_jv_is(bf_jv_get(authority,"decision_ref"),BF_JV_NULL))
        return fail(err,n,"approval lineage or authority boundary is invalid");
    const bf_jv *permissions=bf_jv_get(authority,"required_permissions");
    if(!bf_jv_is(permissions,BF_JV_ARR)||permissions->len!=2||
       strcmp(sval(bf_jv_at(permissions,0))?sval(bf_jv_at(permissions,0)):"","approve")||
       strcmp(sval(bf_jv_at(permissions,1))?sval(bf_jv_at(permissions,1)):"","spend"))
        return fail(err,n,"approval permissions are invalid");
    return 1;
}

static int authority_status_ok(const bf_jv *status,const bf_jv *approval,long long approval_revision,char *err,size_t n) {
    long long revision=0;char expected[512];
    const bf_jv *authority=bf_jv_get(approval,"authority");
    if(!bf_jv_is(status,BF_JV_OBJ)||strcmp(sval(bf_jv_get(status,"state"))?sval(bf_jv_get(status,"state")):"","eligible")||
       !text_ok(bf_jv_get(status,"record_key"),511)||!safe_int(bf_jv_get(status,"revision"),&revision,1)||revision!=approval_revision||
       !hex64(bf_jv_get(status,"body_sha256"))||!text_ok(bf_jv_get(status,"authority_subject"),256)||
       strcmp(sval(bf_jv_get(status,"authority_subject")),sval(bf_jv_get(authority,"subject")))||
       !text_ok(bf_jv_get(status,"decision_id"),159)||strcmp(sval(bf_jv_get(status,"outcome"))?sval(bf_jv_get(status,"outcome")):"","approved")||
       !text_ok(bf_jv_get(status,"decided_by"),256)||!timestamp_ok(bf_jv_get(status,"decided_at"))||
       !bool_is(bf_jv_get(status,"assent_present"),1)||!bool_is(bf_jv_get(status,"approve_authority"),1)||
       !bool_is(bf_jv_get(status,"spend_authority"),1)||!bool_is(bf_jv_get(status,"eligible_for_order_intent"),1))
        return fail(err,n,"authority reconciliation is not eligible for an order intent");
    if(snprintf(expected,sizeof expected,"%s%s",BF_PROCUREMENT_APPROVAL_PREFIX,sval(bf_jv_get(approval,"id"))) >= (int)sizeof expected ||
       strcmp(expected,sval(bf_jv_get(status,"record_key"))))
        return fail(err,n,"authority reconciliation belongs to another approval request");
    const bf_jv *receipt=bf_jv_get(status,"decision_receipt");
    if(receipt&&(!bf_jv_is(receipt,BF_JV_STR)||receipt->len>511))return fail(err,n,"authority decision receipt is invalid");
    return 1;
}

int bf_procurement_order_intent(const char *json,size_t len,char **out,char *err,size_t n) {
    *out=NULL;bf_jv *root=parse_root(json,len,err,n);if(!root)return 0;
    const bf_jv *allocation=bf_jv_get(root,"allocation"),*approval=bf_jv_get(root,"approval"),*authority_status=bf_jv_get(root,"authority_status");
    const bf_jv *allocation_revision_v=bf_jv_get(root,"allocation_revision"),*approval_revision_v=bf_jv_get(root,"approval_revision");
    const bf_jv *id=bf_jv_get(root,"id"),*prepared=bf_jv_get(root,"prepared_at");long long allocation_revision=0,approval_revision=0,source_revision=0;
    if(!allocation_ok(allocation,err,n)||!approval_request_ok(approval,err,n)||
       !safe_int(allocation_revision_v,&allocation_revision,1)||!safe_int(approval_revision_v,&approval_revision,1)||
       !text_ok(id,128)||!timestamp_ok(prepared)||!authority_status_ok(authority_status,approval,approval_revision,err,n)){
        bf_jv_free(root);return fail(err,n,err[0]?err:"order intent input is invalid");
    }
    long long approved_total=0;const bf_jv *approved_total_v=bf_jv_get(approval,"known_total_cents");
    if(!safe_int(approved_total_v,&approved_total,0)||strcmp(sval(bf_jv_get(approval,"fees_attribution"))?sval(bf_jv_get(approval,"fees_attribution")):"","full_quote")){
        bf_jv_free(root);return fail(err,n,"approval does not retain an exact fully attributed spend total");
    }
    const bf_jv *approval_source=bf_jv_get(approval,"source");
    if(strcmp(sval(bf_jv_get(approval_source,"allocation_id")),sval(bf_jv_get(allocation,"id")))||
       !safe_int(bf_jv_get(approval_source,"revision"),&source_revision,1)||source_revision!=allocation_revision||
       strcmp(sval(bf_jv_get(approval_source,"digest")),sval(bf_jv_get(allocation,"digest")))){
        bf_jv_free(root);return fail(err,n,"approval request does not bind this exact allocation version");
    }

    bf_jv *record=bf_jv_obj(),*source=bf_jv_obj(),*authority=bf_jv_obj(),*commitment=bf_jv_obj();
    bf_jv_set(record,"schema",bf_jv_str("bonfyre.procurement.order-intent.v1"));
    bf_jv_set(record,"id",bf_jv_clone(id));bf_jv_set(record,"status",bf_jv_str("prepared"));bf_jv_set(record,"prepared_at",bf_jv_clone(prepared));
    bf_jv_set(source,"allocation_id",bf_jv_clone(bf_jv_get(allocation,"id")));bf_jv_set(source,"allocation_revision",bf_jv_num((double)allocation_revision));bf_jv_set(source,"allocation_digest",bf_jv_clone(bf_jv_get(allocation,"digest")));
    bf_jv_set(source,"approval_id",bf_jv_clone(bf_jv_get(approval,"id")));bf_jv_set(source,"approval_revision",bf_jv_num((double)approval_revision));bf_jv_set(source,"approval_digest",bf_jv_clone(bf_jv_get(approval,"digest")));
    bf_jv_set(source,"approval_record_sha256",bf_jv_clone(bf_jv_get(authority_status,"body_sha256")));bf_jv_set(source,"decision_id",bf_jv_clone(bf_jv_get(authority_status,"decision_id")));bf_jv_set(record,"source",source);

    bf_jv_set(authority,"owner",bf_jv_str("AuthorityGraph"));bf_jv_set(authority,"subject",bf_jv_clone(bf_jv_get(authority_status,"authority_subject")));
    bf_jv_set(authority,"outcome",bf_jv_str("approved"));bf_jv_set(authority,"decided_by",bf_jv_clone(bf_jv_get(authority_status,"decided_by")));bf_jv_set(authority,"decided_at",bf_jv_clone(bf_jv_get(authority_status,"decided_at")));
    const bf_jv *receipt=bf_jv_get(authority_status,"decision_receipt");bf_jv_set(authority,"decision_ref",receipt&&receipt->len?bf_jv_clone(receipt):bf_jv_null());
    bf_jv_set(authority,"assent_verified",bf_jv_bool(1));bf_jv_set(authority,"approve_authority_verified",bf_jv_bool(1));bf_jv_set(authority,"spend_authority_verified",bf_jv_bool(1));bf_jv_set(record,"authority_reconciliation",authority);

    bf_jv_set(record,"currency",bf_jv_clone(bf_jv_get(allocation,"currency")));bf_jv_set(record,"allocations",bf_jv_clone(bf_jv_get(allocation,"allocations")));bf_jv_set(record,"selected_quotes",bf_jv_clone(bf_jv_get(allocation,"selected_quotes")));
    bf_jv_set(record,"known_merchandise_cents",bf_jv_clone(bf_jv_get(allocation,"known_merchandise_cents")));bf_jv_set(record,"known_total_cents",bf_jv_num((double)approved_total));bf_jv_set(record,"fees_attribution",bf_jv_str("full_quote"));bf_jv_set(record,"order_lots",bf_jv_clone(bf_jv_get(allocation,"order_lots")));

    char commitment_id[192];snprintf(commitment_id,sizeof commitment_id,"procurement-order-intent:%s",id->s);
    bf_jv_set(commitment,"owner",bf_jv_str("CommitmentGraph"));bf_jv_set(commitment,"status",bf_jv_str("unmaterialized"));bf_jv_set(commitment,"proposed_resource",bf_jv_str(commitment_id));
    bf_jv_set(commitment,"requires_quote_acceptance",bf_jv_bool(1));bf_jv_set(commitment,"requires_supplier_counterparty_terms",bf_jv_bool(1));bf_jv_set(record,"commitment_handoff",commitment);
    bf_jv_set(record,"commerce_chain",bf_jv_clone(bf_jv_get(allocation,"commerce_chain")));

    bf_jv_set(record,"authority_eligible",bf_jv_bool(1));bf_jv_set(record,"quote_accepted",bf_jv_bool(0));bf_jv_set(record,"reservation_created",bf_jv_bool(0));bf_jv_set(record,"cart_created",bf_jv_bool(0));
    bf_jv_set(record,"commitment_opened",bf_jv_bool(0));bf_jv_set(record,"order_created",bf_jv_bool(0));bf_jv_set(record,"purchase_order_issued",bf_jv_bool(0));bf_jv_set(record,"supplier_accepted",bf_jv_bool(0));
    bf_jv_set(record,"payable_created",bf_jv_bool(0));bf_jv_set(record,"payment_presented",bf_jv_bool(0));bf_jv_set(record,"settled",bf_jv_bool(0));bf_jv_set(record,"fulfilled",bf_jv_bool(0));bf_jv_set(record,"shipped",bf_jv_bool(0));bf_jv_set(record,"received",bf_jv_bool(0));
    bf_jv_set(record,"does_not_establish",bf_jv_str("quote acceptance, supplier acceptance, reservation, cart, CommitmentGraph opening, purchase order issuance, payable, payment, settlement, fulfillment, shipment, receipt, satisfaction or ownership"));
    seal(record);*out=bf_jv_dump(record,0,BF_JV_SORT_NONE,NULL);bf_jv_free(record);bf_jv_free(root);return *out!=NULL;
}

static int contract_actor_row_ok(const bf_jv *row,const char *quote_id) {
    return bf_jv_is(row,BF_JV_OBJ)&&!strcmp(sval(bf_jv_get(row,"quote_id"))?sval(bf_jv_get(row,"quote_id")):"",quote_id)&&
           text_ok(bf_jv_get(row,"seller_actor"),256)&&text_ok(bf_jv_get(row,"settlement_actor"),256)&&
           text_ok(bf_jv_get(row,"deliverer_actor"),256)&&text_ok(bf_jv_get(row,"capability_ref"),512);
}

int bf_procurement_contract_preflight(const char *json,size_t len,char **out,char *err,size_t n) {
    *out=NULL;bf_jv *root=parse_root(json,len,err,n);if(!root)return 0;
    const bf_jv *intent=bf_jv_get(root,"order_intent"),*id=bf_jv_get(root,"id"),*prepared=bf_jv_get(root,"prepared_at"),*buyer=bf_jv_get(root,"buyer_actor"),*actors=bf_jv_get(root,"lot_actors");
    long long order_total=0;
    if(!bf_jv_is(intent,BF_JV_OBJ)||strcmp(sval(bf_jv_get(intent,"schema"))?sval(bf_jv_get(intent,"schema")):"","bonfyre.procurement.order-intent.v1")||
       strcmp(sval(bf_jv_get(intent,"status"))?sval(bf_jv_get(intent,"status")):"","prepared")||!verify_digest(intent,err,n)||
       !bool_is(bf_jv_get(intent,"authority_eligible"),1)||!bool_is(bf_jv_get(intent,"quote_accepted"),0)||!bool_is(bf_jv_get(intent,"commitment_opened"),0)||!bool_is(bf_jv_get(intent,"order_created"),0)||
       !safe_int(bf_jv_get(intent,"known_total_cents"),&order_total,0)||strcmp(sval(bf_jv_get(intent,"fees_attribution"))?sval(bf_jv_get(intent,"fees_attribution")):"","full_quote")||
       !text_ok(id,128)||!timestamp_ok(prepared)||!text_ok(buyer,256)||!bf_jv_is(actors,BF_JV_ARR)){
        bf_jv_free(root);return fail(err,n,err[0]?err:"order intent is not ready for protocol preflight");
    }
    const bf_jv *lots=bf_jv_get(intent,"order_lots"),*currency=bf_jv_get(intent,"currency"),*intent_digest=bf_jv_get(intent,"digest");
    if(!bf_jv_is(lots,BF_JV_ARR)||!lots->len||lots->len>MAX_QUOTES||actors->len!=lots->len||!currency_ok(currency)||!hex64(intent_digest)){
        bf_jv_free(root);return fail(err,n,"protocol preflight requires one explicit actor set per exact supplier lot");
    }
    bf_jv *record=bf_jv_obj(),*source=bf_jv_obj(),*contracts=bf_jv_arr();long long summed=0;
    for(size_t li=0;li<lots->len;li++){
        const bf_jv *lot=bf_jv_at(lots,li),*quote_id=bf_jv_get(lot,"quote_id"),*quote_digest=bf_jv_get(lot,"digest"),*supplier=bf_jv_get(lot,"supplier"),*lot_total_v=bf_jv_get(lot,"known_total_cents");
        long long lot_total=0;const char *qid=sval(quote_id);
        if(!bf_jv_is(lot,BF_JV_OBJ)||!text_ok(quote_id,128)||!hex64(quote_digest)||!supplier_ok(supplier)||!bool_is(bf_jv_get(lot,"full_quote_consumed"),1)||
           !safe_int(lot_total_v,&lot_total,0)||summed>SAFE_INT-lot_total){bf_jv_free(record);bf_jv_free(root);return fail(err,n,"protocol preflight requires fully priced exact supplier lots");}
        summed+=lot_total;
        const bf_jv *binding=NULL;size_t hits=0;
        for(size_t ai=0;ai<actors->len;ai++){const bf_jv *row=bf_jv_at(actors,ai);if(bf_jv_is(row,BF_JV_OBJ)&&!strcmp(sval(bf_jv_get(row,"quote_id"))?sval(bf_jv_get(row,"quote_id")):"",qid)){binding=row;hits++;}}
        if(hits!=1||!contract_actor_row_ok(binding,qid)){bf_jv_free(record);bf_jv_free(root);return fail(err,n,"each supplier lot requires one explicit buyer-side contract actor proposal");}
        const bf_jv *seller=bf_jv_get(binding,"seller_actor"),*settlement=bf_jv_get(binding,"settlement_actor"),*deliverer=bf_jv_get(binding,"deliverer_actor"),*capability=bf_jv_get(binding,"capability_ref");
        const char *keys[]={"order_intent","quote","capability","buyer","seller","settlement","deliverer"};
        const bf_jv *values[]={intent_digest,quote_digest,capability,buyer,seller,settlement,deliverer};char basis[65],exchange[96];
        bf_jv_digest_members(BF_JV_SORT_BYTES,7,keys,values,basis);snprintf(exchange,sizeof exchange,"procurement:%.*s",48,basis);
        bf_jv *contract=bf_jv_obj(),*actor_obj=bf_jv_obj(),*requirements=bf_jv_arr(),*commitments=bf_jv_arr(),*bindings=bf_jv_obj();
        bf_jv_set(contract,"schema",bf_jv_str("bonfyre.protocol-contract.preflight.v1"));bf_jv_set(contract,"contract_name",bf_jv_str("quote-accept"));bf_jv_set(contract,"status",bf_jv_str("authority_preflight_required"));bf_jv_set(contract,"exchange_id",bf_jv_str(exchange));
        bf_jv_set(contract,"quote_id",bf_jv_clone(quote_id));bf_jv_set(contract,"quote_digest",bf_jv_clone(quote_digest));bf_jv_set(contract,"supplier_observation",bf_jv_clone(supplier));
        bf_jv_set(contract,"capability_ref",bf_jv_clone(capability));bf_jv_set(contract,"quoted_amount_cents",bf_jv_num((double)lot_total));bf_jv_set(contract,"currency",bf_jv_clone(currency));
        bf_jv_set(actor_obj,"buyer",bf_jv_clone(buyer));bf_jv_set(actor_obj,"seller",bf_jv_clone(seller));bf_jv_set(actor_obj,"settlement",bf_jv_clone(settlement));bf_jv_set(actor_obj,"deliverer",bf_jv_clone(deliverer));bf_jv_set(contract,"proposed_actors",actor_obj);
        const char *roles[]={"buyer","seller","settlement","deliverer"},*perms[]={"request:capability","quote:capability","accept:quote","deliver:capability"};const bf_jv *actor_values[]={buyer,seller,settlement,deliverer};
        for(size_t ri=0;ri<4;ri++){bf_jv *req=bf_jv_obj();bf_jv_set(req,"role",bf_jv_str(roles[ri]));bf_jv_set(req,"actor",bf_jv_clone(actor_values[ri]));bf_jv_set(req,"permission",bf_jv_str(perms[ri]));bf_jv_push(requirements,req);}
        bf_jv_set(contract,"authority_requirements",requirements);
        bf_jv *delivery=bf_jv_obj();bf_jv_set(delivery,"key",bf_jv_str("delivery"));bf_jv_set(delivery,"committer_role",bf_jv_str("deliverer"));bf_jv_set(delivery,"beneficiary_role",bf_jv_str("buyer"));bf_jv_set(delivery,"resource_ref",bf_jv_clone(capability));bf_jv_set(delivery,"discharged_by",bf_jv_str("delivery"));bf_jv_push(commitments,delivery);
        bf_jv *payment=bf_jv_obj();bf_jv_set(payment,"key",bf_jv_str("payment"));bf_jv_set(payment,"committer_role",bf_jv_str("buyer"));bf_jv_set(payment,"beneficiary_role",bf_jv_str("seller"));bf_jv_set(payment,"amount_cents",bf_jv_num((double)lot_total));bf_jv_set(payment,"currency",bf_jv_clone(currency));bf_jv_set(payment,"discharged_by",bf_jv_str("acceptance"));bf_jv_push(commitments,payment);bf_jv_set(contract,"proposed_commitments",commitments);
        bf_jv_set(bindings,"capability",bf_jv_clone(capability));bf_jv_set(bindings,"buyer_identity",bf_jv_clone(buyer));bf_jv_set(bindings,"exchange_id",bf_jv_str(exchange));bf_jv_set(contract,"parameter_bindings",bindings);
        const char *supplier_id=sval(bf_jv_get(supplier,"id")),*seller_actor=sval(seller);
        int supplier_bound=supplier_id&&seller_actor&&ed25519_actor_string(seller_actor)&&!strcmp(supplier_id,seller_actor);
        bf_jv_set(contract,"structurally_complete",bf_jv_bool(1));bf_jv_set(contract,"supplier_actor_binding_verified",bf_jv_bool(supplier_bound));bf_jv_set(contract,"authority_verified",bf_jv_bool(0));bf_jv_set(contract,"supplier_acceptance_present",bf_jv_bool(0));bf_jv_set(contract,"protocol_enacted",bf_jv_bool(0));bf_jv_set(contract,"commitment_opened",bf_jv_bool(0));
        bf_jv_set(contract,"does_not_establish",bf_jv_str("supplier identity binding, role authority, supplier acceptance, protocol enactment, CommitmentGraph obligation, purchase order, payable, payment, shipment or receipt"));
        seal(contract);bf_jv_push(contracts,contract);
    }
    if(summed!=order_total){bf_jv_free(record);bf_jv_free(root);return fail(err,n,"supplier lot totals do not equal the exact approved order total");}
    bf_jv_set(record,"schema",bf_jv_str("bonfyre.procurement.protocol-preflight.v1"));bf_jv_set(record,"id",bf_jv_clone(id));bf_jv_set(record,"status",bf_jv_str("prepared"));bf_jv_set(record,"prepared_at",bf_jv_clone(prepared));
    bf_jv_set(source,"order_intent_id",bf_jv_clone(bf_jv_get(intent,"id")));bf_jv_set(source,"order_intent_digest",bf_jv_clone(intent_digest));bf_jv_set(record,"source",source);bf_jv_set(record,"buyer_actor",bf_jv_clone(buyer));bf_jv_set(record,"contracts",contracts);
    bf_jv_set(record,"protocol_owner",bf_jv_str("ProtocolContract"));bf_jv_set(record,"commitment_owner",bf_jv_str("CommitmentGraph"));bf_jv_set(record,"effect_free",bf_jv_bool(1));
    bf_jv_set(record,"does_not_establish",bf_jv_str("supplier identity binding, AuthorityGraph grants, supplier acceptance, protocol enactment, CommitmentGraph opening, order, payable, payment, settlement, fulfillment, shipment or receipt"));
    seal(record);*out=bf_jv_dump(record,0,BF_JV_SORT_NONE,NULL);bf_jv_free(record);bf_jv_free(root);return *out!=NULL;
}
