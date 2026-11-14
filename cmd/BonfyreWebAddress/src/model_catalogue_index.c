#define _POSIX_C_SOURCE 200809L
#include "model_catalogue_index.h"
#include "glyph_set.h"
#include "bf_sqlite_path.h"
#include <sqlite3.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>

static int append(char *buffer,size_t cap,size_t *len,const char *fmt,...) {
    if (*len >= cap) return 0;
    va_list ap; va_start(ap,fmt);
    int n=vsnprintf(buffer+*len,cap-*len,fmt,ap);
    va_end(ap);
    if (n<0 || (size_t)n>=cap-*len) return 0;
    *len+=(size_t)n;
    return 1;
}
static void escaped(const char *value,char *out,size_t size) {
    const unsigned char *p=(const unsigned char *)(value?value:"");
    size_t n=0;
    if (!size) return;
    while (*p && n+7<size) {
        const char *s=NULL;
        switch (*p) {
        case '<':s="&lt;";break;
        case '>':s="&gt;";break;
        case '&':s="&amp;";break;
        case '"':s="&quot;";break;
        default:break;
        }
        if(s){size_t k=strlen(s);memcpy(out+n,s,k);n+=k;}
        else out[n++]=(char)*p;
        ++p;
    }
    out[n]='\0';
}
/* Size as a person reads it. Zero is an unrecorded size, never a tiny model. */
static void size_words(double mb,char *out,size_t cap) {
    if(mb<=0.0)snprintf(out,cap,"size not recorded");
    else if(mb>=1000.0)snprintf(out,cap,"%.1f GB",mb/1000.0);
    else if(mb>=1.0)snprintf(out,cap,"%.0f MB",mb);
    else snprintf(out,cap,"under 1 MB");
}
int bf_model_catalogue_index(const char *path,char *facts,size_t cap,size_t *used) {
    return bf_model_catalogue_find(path,NULL,facts,cap,used);
}
/* The catalogue as the screen's tool list: a filter, then one row per model
 * naming what it is, what it says it does and where it is used. */
int bf_model_catalogue_find(const char *path,const char *want,char *facts,size_t cap,size_t *used) {
    char uri[PATH_MAX];sqlite3 *db=NULL;sqlite3_stmt *st=NULL;
    long models=0,caps=0,edges=0,dangling=0,shown=0;
    size_t fw=0;
    int state=-1;
    char ewant[320];
    if(used)*used=0;
    if(!facts||!cap||!used)return -1;
    facts[0]='\0';
    if(want&&!*want)want=NULL;
    escaped(want,ewant,sizeof(ewant));
    if(!path||!*path||bf_sqlite_readonly_uri(uri,sizeof(uri),path)>=(int)sizeof(uri)||
       sqlite3_open_v2(uri,&db,SQLITE_OPEN_READONLY|SQLITE_OPEN_URI,NULL)!=SQLITE_OK)goto done;
    if(sqlite3_prepare_v2(db,
       "SELECT (SELECT COUNT(*) FROM catalog_nodes WHERE kind='model'),"
       " (SELECT COUNT(*) FROM catalog_nodes WHERE kind='capability'),"
       " (SELECT COUNT(*) FROM catalog_edges e"
       "   WHERE EXISTS (SELECT 1 FROM catalog_nodes n WHERE n.node_id=e.src_node_id)"
       "     AND EXISTS (SELECT 1 FROM catalog_nodes n WHERE n.node_id=e.dst_node_id)),"
       " (SELECT COUNT(*) FROM catalog_edges e"
       "   WHERE NOT EXISTS (SELECT 1 FROM catalog_nodes n WHERE n.node_id=e.src_node_id)"
       "      OR NOT EXISTS (SELECT 1 FROM catalog_nodes n WHERE n.node_id=e.dst_node_id))",
       -1,&st,NULL)!=SQLITE_OK || sqlite3_step(st)!=SQLITE_ROW) goto done;
    models=(long)sqlite3_column_int64(st,0);
    caps=(long)sqlite3_column_int64(st,1);
    edges=(long)sqlite3_column_int64(st,2);
    dangling=(long)sqlite3_column_int64(st,3);
    sqlite3_finalize(st);st=NULL;
    if(!models){state=0;goto done;}
    if(!append(facts,cap,&fw,
       "<dl class=\"ak-mc-stats\">"
       "<div><dt>Models</dt><dd>%ld</dd></div>"
       "<div><dt>Things they do</dt><dd>%ld</dd></div>"
       "<div><dt>Connections</dt><dd>%ld</dd></div>"
       "<div><dt>Broken links</dt><dd>%ld</dd></div></dl>"
       "<form class=\"ak-mc-find\" role=\"search\" action=\"/s/models/all\" method=\"get\">"
       "<span class=\"ak-mc-find-icon\">" BF_ICON_SEARCH "</span>"
       "<input type=\"search\" name=\"q\" value=\"%s\" placeholder=\"Find a model by name or what it does\" aria-label=\"Find a model\">"
       "<button type=\"submit\">Find</button></form>"
       "<div class=\"ak-mc-list\" data-bf-list data-bf-row-height=\"128\">",
       models,caps,edges,dangling,ewant))goto done;
    if(sqlite3_prepare_v2(db,
       "SELECT n.external_id, n.name, COALESCE(json_extract(n.json_data,'$.format'),''),"
       " COALESCE(json_extract(n.json_data,'$.size_mb'),0), COALESCE(n.summary,''),"
       " (SELECT COUNT(*) FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.dst_node_id"
       "   WHERE e.src_node_id=n.node_id"
       "    AND e.rel IN ('powers_capability','implements_capability','supports_capability')),"
       " (SELECT COUNT(*) FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.src_node_id"
       "   WHERE e.dst_node_id=n.node_id AND o.kind<>'model_source'"
       "    AND e.rel IN ('uses_model','sources_model','has_model')),"
       " (SELECT COUNT(*) FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.src_node_id"
       "   WHERE e.dst_node_id=n.node_id AND e.rel='derived_from_model'),"
       " (SELECT COUNT(*) FROM catalog_edges e WHERE"
       "   (e.src_node_id=n.node_id AND NOT EXISTS"
       "      (SELECT 1 FROM catalog_nodes o WHERE o.node_id=e.dst_node_id))"
       "   OR (e.dst_node_id=n.node_id AND NOT EXISTS"
       "      (SELECT 1 FROM catalog_nodes o WHERE o.node_id=e.src_node_id))),"
       " (SELECT o.name FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.src_node_id"
       "   WHERE e.dst_node_id=n.node_id AND o.kind<>'model_source' AND e.rel IN ('uses_model','sources_model','has_model')"
       "   ORDER BY o.kind='capability', o.name LIMIT 1),"
       " COALESCE(json_extract(n.json_data,'$.mean_f1'),0)"
       " FROM catalog_nodes n WHERE n.kind='model'"
       "  AND (?1 IS NULL OR instr(lower(n.name),lower(?1)) OR instr(lower(n.external_id),lower(?1))"
       "   OR instr(lower(COALESCE(n.summary,'')),lower(?1))"
       "   OR EXISTS (SELECT 1 FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.src_node_id"
       "     WHERE e.dst_node_id=n.node_id AND o.kind<>'model_source' AND instr(lower(o.name),lower(?1))))"
       " ORDER BY json_extract(n.json_data,'$.size_mb') DESC, n.name",
       -1,&st,NULL)!=SQLITE_OK)goto done;
    if(want)sqlite3_bind_text(st,1,want,-1,SQLITE_STATIC);
    int rc;
    while((rc=sqlite3_step(st))==SQLITE_ROW){
        if(fw>=cap-((cap>2048)?2048:cap))break;
        char eid[192],name[320],format[64],summary[1200],first[400],size[48];
        long powers=(long)sqlite3_column_int64(st,5);
        long taken=(long)sqlite3_column_int64(st,6);
        long derived=(long)sqlite3_column_int64(st,7);
        long unresolved=(long)sqlite3_column_int64(st,8);
        double f1=sqlite3_column_double(st,10);
        escaped((const char *)sqlite3_column_text(st,0),eid,sizeof(eid));
        escaped((const char *)sqlite3_column_text(st,1),name,sizeof(name));
        escaped((const char *)sqlite3_column_text(st,2),format,sizeof(format));
        escaped((const char *)sqlite3_column_text(st,4),summary,sizeof(summary));
        escaped((const char *)sqlite3_column_text(st,9),first,sizeof(first));
        size_words(sqlite3_column_double(st,3),size,sizeof(size));
        shown++;
        if(!append(facts,cap,&fw,
          "<a class=\"ak-mc-model\" href=\"/s/model/%s\">"
          "<span class=\"ak-mc-mark\">" BF_ICON_ROBOT "</span>"
          "<span class=\"ak-mc-name\">%s</span>"
          "<span class=\"ak-mc-what\">%s</span>"
          "<span class=\"ak-mc-meta\">%s &middot; %s%s</span>"
          "<span class=\"ak-mc-uses\">",
          eid,name,summary[0]?summary:"No description recorded yet.",
          format[0]?format:"format not recorded",size,
          f1>0.0?" &middot; tested":""))goto done;
        if(taken&&first[0]){
            if(!append(facts,cap,&fw,"<b>Used in %s",first))goto done;
            if(taken>1&&!append(facts,cap,&fw," and %ld more",taken-1))goto done;
            if(!append(facts,cap,&fw,"</b>"))goto done;
        }else if(!append(facts,cap,&fw,"<b>Not used anywhere yet</b>"))goto done;
        if(!append(facts,cap,&fw,
          "<i>does %ld &middot; used by %ld &middot; %ld made from it%s</i></span></a>",
          powers,taken,derived,unresolved?" &middot; some links broken":""))goto done;
    }
    if(rc!=SQLITE_DONE)goto done;
    if(!shown&&!append(facts,cap,&fw,
       "<p class=\"ak-mc-none\">No model matches &ldquo;%s&rdquo;. <a href=\"/s/models/all\">Show every model</a></p>",ewant))goto done;
    if(!append(facts,cap,&fw,"</div>"
       "<p class=\"ak-mc-note\">%ld links name something that was never added to the catalogue. "
       "They are left out of these counts, and each model&rsquo;s page lists its own.</p>",dangling))goto done;
    *used=fw;state=1;
done:
    sqlite3_finalize(st);
    if(db)sqlite3_close(db);
    if(state<0){facts[0]='\0';*used=0;}
    return state;
}

/* A passport's admission word as a person reads it. */
static const char *admission_words(const char *code) {
    if(!code||!*code)return "no decision recorded";
    if(!strcasecmp(code,"ADMIT"))return "approved";
    if(!strcasecmp(code,"DEFER"))return "on hold";
    if(!strcasecmp(code,"REJECT"))return "turned down";
    return code;
}

/* One model, laid out as the sandbox runner screen: where you are, the model's
 * name and what it says about itself, then (from the caller) the try-it
 * workbench, then what is known about it, then where it is used and how it is
 * made. Two markers split the output so the caller can place the workbench and
 * the price it reads from its own store:
 *   BF_MODEL_TRY_MARK   -- the workbench goes here
 *   BF_MODEL_PRICE_MARK -- one price tile goes here, inside the facts grid
 * Route, prices and authorizations stay upstream. */
int bf_model_catalogue_detail(const char *path,const char *ident,
     char *name,size_t name_cap,char *facts,size_t cap,size_t *used) {
    if(used)*used=0;
    if(!ident||!*ident||!name||!name_cap||!facts||cap<262144||!used)return -1;
    name[0]='\0';facts[0]='\0';
    sqlite3 *db=NULL;sqlite3_stmt *st=NULL;char uri[PATH_MAX];
    size_t fw=0;
    char summary[2048]={0},format[64]={0},geometry[96]={0},family[96]={0};
    double size_mb=0,mean_f1=0;
    /* A catalogue node is keyed by text ("model:llama-3-8b-instruct"), not by
     * a row number. Reading it as an integer gave 0, bound 0, matched
     * nothing, and every relation band silently disappeared while the index
     * page counting the same edges showed nine dependents. */
    char node_id[192]={0};

    if(!path||!*path||
       bf_sqlite_readonly_uri(uri,sizeof(uri),path)>=(int)sizeof(uri)||
       sqlite3_open_v2(uri,&db,SQLITE_OPEN_READONLY|SQLITE_OPEN_URI,NULL)!=SQLITE_OK){
        if(db)sqlite3_close(db);
        return -1;
    }
    if(sqlite3_prepare_v2(db,
          "SELECT node_id, name, COALESCE(summary,''),"
          " COALESCE(json_extract(json_data,'$.format'),''),"
          " COALESCE(json_extract(json_data,'$.size_mb'),0),"
          " COALESCE(json_extract(json_data,'$.geometry'),''),"
          " COALESCE(json_extract(json_data,'$.transform_family'),''),"
          " COALESCE(json_extract(json_data,'$.mean_f1'),0)"
          " FROM catalog_nodes WHERE kind='model' AND external_id=?1",-1,&st,NULL)!=SQLITE_OK||
       (sqlite3_bind_text(st,1,ident,-1,SQLITE_STATIC),sqlite3_step(st))!=SQLITE_ROW){
        if(st)sqlite3_finalize(st);
        sqlite3_close(db);
        return 0;
    }
    snprintf(node_id,sizeof(node_id),"%s",(const char *)sqlite3_column_text(st,0));
    snprintf(name,name_cap,"%s",(const char *)sqlite3_column_text(st,1));
    snprintf(summary,sizeof(summary),"%s",(const char *)sqlite3_column_text(st,2));
    snprintf(format,sizeof(format),"%s",(const char *)sqlite3_column_text(st,3));
    size_mb=sqlite3_column_double(st,4);
    snprintf(geometry,sizeof(geometry),"%s",(const char *)sqlite3_column_text(st,5));
    snprintf(family,sizeof(family),"%s",(const char *)sqlite3_column_text(st,6));
    mean_f1=sqlite3_column_double(st,7);
    sqlite3_finalize(st);st=NULL;

    {
        char es[4096],eg[192],efam[192],eformat[128],ename[640],size[48];
        escaped(summary,es,sizeof(es));
        escaped(format,eformat,sizeof(eformat));
        escaped(geometry,eg,sizeof(eg));
        escaped(family,efam,sizeof(efam));
        escaped(name,ename,sizeof(ename));
        size_words(size_mb,size,sizeof(size));
        fw+=(size_t)snprintf(facts+fw,cap-fw,
          "<nav class=\"ak-mc-strip\" aria-label=\"Where you are\">"
          "<a href=\"/s/models/all\">Model Commons</a><span aria-hidden=\"true\">/</span>"
          "<span>%s</span><span class=\"ak-mc-strip-r\">%s &middot; %s</span></nav>"
          "<header class=\"ak-mc-hero\"><p class=\"ak-mc-kicker\">Try a model</p>"
          "<h1>%s</h1><p class=\"ak-mc-lede\">%s</p></header>"
          BF_MODEL_TRY_MARK
          "<section class=\"ak-mc-band\" aria-labelledby=\"ak-mc-about\">"
          "<h2 class=\"ak-mc-h\" id=\"ak-mc-about\">About this model</h2>"
          "<dl class=\"ak-mc-tiles\">"
          "<div><dt>Format</dt><dd>%s</dd></div>"
          "<div><dt>Size</dt><dd>%s</dd></div>"
          "<div><dt>Shape</dt><dd>%s</dd></div>"
          "<div><dt>Family</dt><dd>%s</dd></div>",
          ename,eformat[0]?eformat:"format not recorded",size,
          ename,es[0]?es:"No description recorded yet.",
          eformat[0]?eformat:"not recorded",size,
          (eg[0]&&strcmp(eg,"-"))?eg:"not recorded",
          (efam[0]&&strcmp(efam,"-"))?efam:"none");
        /* Representation passports the catalogue carries for this model.
         * Without this the page said nothing had been measured about a
         * model whose registry held a measured passport. Each is shown with
         * its decision and its own envelope, never as a score. */
        int passports=0;
        if(sqlite3_prepare_v2(db,
              "SELECT o.name, COALESCE(o.category,''),"
              " COALESCE(json_extract(o.json_data,'$.admission_scope'),''),"
              " COALESCE(json_extract(o.json_data,'$.measurement_state'),''),"
              " COALESCE(o.summary,''), COALESCE(o.source_hash,'')"
              " FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.dst_node_id"
              " WHERE e.src_node_id=?1 AND e.rel='has_representation' AND o.kind='representation'"
              " ORDER BY o.name LIMIT 12",-1,&st,NULL)==SQLITE_OK){
            sqlite3_bind_text(st,1,node_id,-1,SQLITE_STATIC);
            while(sqlite3_step(st)==SQLITE_ROW&&fw<cap-6144){
                char rn[400],ra[64],rs[64],rm[64],rq[4096],rd[80];
                escaped((const char *)sqlite3_column_text(st,0),rn,sizeof(rn));
                escaped(admission_words((const char *)sqlite3_column_text(st,1)),ra,sizeof(ra));
                escaped((const char *)sqlite3_column_text(st,2),rs,sizeof(rs));
                escaped((const char *)sqlite3_column_text(st,3),rm,sizeof(rm));
                escaped((const char *)sqlite3_column_text(st,4),rq,sizeof(rq));
                escaped((const char *)sqlite3_column_text(st,5),rd,sizeof(rd));
                if(!strcmp(rm,"measured"))passports++;
                fw+=(size_t)snprintf(facts+fw,cap-fw,
                  "<div class=\"is-wide\" data-representation=\"%s\"><dt>Tested for %s</dt>"
                  "<dd>%s, %s</dd><p>%s. %s Check code %s.</p></div>",
                  rn,rs[0]?rs:"one use",rm[0]?rm:"not tested",ra,rn,rq,rd[0]?rd:"none");
            }
            sqlite3_finalize(st);st=NULL;
        }
        /* A measured figure is printed only where one exists. A zero here is
         * an absence of measurement, not a score of nothing. */
        if(passports>0&&mean_f1<=0.0)
            ;
        else if(mean_f1>0.0)
            fw+=(size_t)snprintf(facts+fw,cap-fw,
              "<div><dt>Test score</dt><dd>%.3f</dd>"
              "<p>F1 on this estate&rsquo;s own test. It says nothing about other tasks.</p></div>",mean_f1);
        else
            fw+=(size_t)snprintf(facts+fw,cap-fw,
              "<div data-unmeasured><dt>How well it works</dt><dd>Not tested yet</dd>"
              "<p>Size and format don&rsquo;t say how well a model works.</p></div>");
        fw+=(size_t)snprintf(facts+fw,cap-fw,BF_MODEL_PRICE_MARK "</dl></section>");
    }

    /* Each band is one relation in the graph, in the words a person uses. */
    {
        struct { const char *rel; const char *heading; const char *note; int outgoing; } bands[]={
          {"uses_model","Used in",
           "Recorded as using this model. That doesn&rsquo;t mean it is running now.",0},
          {"powers_capability","What it does here",
           "Recorded as powering this. Using it still needs your own access.",1},
          {"derived_from_model","Made from",
           "How it was made, as recorded here. A licence doesn&rsquo;t travel with it.",1},
          {"has_source","Where to get it",
           "An address recorded here, not a promise that it answers.",1},
          {"has_layer_artifact","Its parts",
           "Files that make up the model. None of them runs on its own.",1},
        };
        size_t b;
        for(b=0;b<sizeof(bands)/sizeof(bands[0])&&fw<cap-8192;b++){
            int count=0;
            char sql[512];
            snprintf(sql,sizeof(sql),
              "SELECT o.kind, o.name, COALESCE(o.external_id,'') FROM catalog_edges e"
              " JOIN catalog_nodes o ON o.node_id = e.%s"
              " WHERE e.%s = ?1 AND e.rel = ?2 ORDER BY o.name LIMIT 40",
              bands[b].outgoing?"dst_node_id":"src_node_id",
              bands[b].outgoing?"src_node_id":"dst_node_id");
            if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)continue;
            sqlite3_bind_text(st,1,node_id,-1,SQLITE_STATIC);
            sqlite3_bind_text(st,2,bands[b].rel,-1,SQLITE_STATIC);
            while(sqlite3_step(st)==SQLITE_ROW&&fw<cap-2048){
                char en[400],eo[192];
                const char *kind=(const char *)sqlite3_column_text(st,0);
                const char *oname=(const char *)sqlite3_column_text(st,1);
                int local_source=kind&&!strcmp(kind,"model_source")&&oname&&
                    (!strncasecmp(oname,"file:",5)||oname[0]=='/'||
                     (oname[0]=='~'&&oname[1]=='/'));
                escaped(local_source?"Local file":oname,en,sizeof(en));
                escaped((const char *)sqlite3_column_text(st,2),eo,sizeof(eo));
                if(!count++)
                    fw+=(size_t)snprintf(facts+fw,cap-fw,
                      "<section class=\"ak-mc-band\"><h2 class=\"ak-mc-h\">%s</h2>"
                      "<p class=\"ak-mc-note\">%s</p><ul class=\"ak-mc-links\">",
                      bands[b].heading,bands[b].note);
                if(kind&&!strcmp(kind,"model")&&eo[0])
                    fw+=(size_t)snprintf(facts+fw,cap-fw,
                      "<li><a href=\"/s/model/%s\">%s</a></li>",eo,en);
                else
                    fw+=(size_t)snprintf(facts+fw,cap-fw,"<li>%s</li>",en);
            }
            if(count)fw+=(size_t)snprintf(facts+fw,cap-fw,"</ul></section>");
            sqlite3_finalize(st);st=NULL;
        }
    }

    /* References this model carries to nodes the catalogue never registered.
     * A recipe that uses a model is real information even when the recipe was
     * never inserted, so it is named as a broken link rather than dropped by
     * the join that could not follow it. */
    if(sqlite3_prepare_v2(db,
          "SELECT e.rel, CASE WHEN e.src_node_id=?1 THEN e.dst_node_id ELSE e.src_node_id END"
          " FROM catalog_edges e WHERE"
          "  (e.src_node_id=?1 AND NOT EXISTS"
          "     (SELECT 1 FROM catalog_nodes o WHERE o.node_id=e.dst_node_id))"
          "  OR (e.dst_node_id=?1 AND NOT EXISTS"
          "     (SELECT 1 FROM catalog_nodes o WHERE o.node_id=e.src_node_id))"
          " ORDER BY 2 LIMIT 40",-1,&st,NULL)==SQLITE_OK){
        int count=0;
        sqlite3_bind_text(st,1,node_id,-1,SQLITE_STATIC);
        while(sqlite3_step(st)==SQLITE_ROW&&fw<cap-2048){
            char eref[200];
            escaped((const char *)sqlite3_column_text(st,1),eref,sizeof(eref));
            if(!count++)
                fw+=(size_t)snprintf(facts+fw,cap-fw,
                  "<section class=\"ak-mc-band\" data-broken-links><h2 class=\"ak-mc-h\">Links we can&rsquo;t follow</h2>"
                  "<p class=\"ak-mc-note\">Each of these names something that was never added to the catalogue, "
                  "so this page can&rsquo;t say what it is.</p><ul class=\"ak-mc-links is-broken\">");
            fw+=(size_t)snprintf(facts+fw,cap-fw,"<li><code>%s</code></li>",eref);
        }
        if(count)fw+=(size_t)snprintf(facts+fw,cap-fw,"</ul></section>");
        sqlite3_finalize(st);st=NULL;
    }
    sqlite3_close(db);
    if(fw>=cap){facts[0]='\0';return -1;}
    *used=fw;
    return 1;
}
