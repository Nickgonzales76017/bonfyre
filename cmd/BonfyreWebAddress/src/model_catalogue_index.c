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
int bf_model_catalogue_index(const char *path,char *facts,size_t cap,size_t *used) {
    char uri[PATH_MAX];sqlite3 *db=NULL;sqlite3_stmt *st=NULL;
    long models=0,layers=0,caps=0,edges=0,dangling=0;
    size_t fw=0;
    int state=-1;
    if(used)*used=0;
    if(!facts||!cap||!used)return -1;
    facts[0]='\0';
    if(!path||!*path||bf_sqlite_readonly_uri(uri,sizeof(uri),path)>=(int)sizeof(uri)||
       sqlite3_open_v2(uri,&db,SQLITE_OPEN_READONLY|SQLITE_OPEN_URI,NULL)!=SQLITE_OK)goto done;
    if(sqlite3_prepare_v2(db,
       "SELECT (SELECT COUNT(*) FROM catalog_nodes WHERE kind='model'),"
       " (SELECT COUNT(*) FROM catalog_nodes WHERE kind='layer'),"
       " (SELECT COUNT(*) FROM catalog_nodes WHERE kind='capability'),"
       " (SELECT COUNT(*) FROM catalog_edges e"
       "   WHERE EXISTS (SELECT 1 FROM catalog_nodes n WHERE n.node_id=e.src_node_id)"
       "     AND EXISTS (SELECT 1 FROM catalog_nodes n WHERE n.node_id=e.dst_node_id)),"
       " (SELECT COUNT(*) FROM catalog_edges e"
       "   WHERE NOT EXISTS (SELECT 1 FROM catalog_nodes n WHERE n.node_id=e.src_node_id)"
       "      OR NOT EXISTS (SELECT 1 FROM catalog_nodes n WHERE n.node_id=e.dst_node_id))",
       -1,&st,NULL)!=SQLITE_OK || sqlite3_step(st)!=SQLITE_ROW) goto done;
    models=(long)sqlite3_column_int64(st,0);
    layers=(long)sqlite3_column_int64(st,1);
    caps=(long)sqlite3_column_int64(st,2);
    edges=(long)sqlite3_column_int64(st,3);
    dangling=(long)sqlite3_column_int64(st,4);
    sqlite3_finalize(st);st=NULL;
    if(!models){state=0;goto done;}
    if(!append(facts,cap,&fw,
       "<h3 class=\"room-h\">Registered models</h3>"
       "<p class=\"subj-note\">Explore a model to see its declared uses, variants, "
       "measured evidence and availability. Catalogue registration does not mean "
       "the model is installed or runnable.</p>"
       "<details class=\"model-coverage\" data-model-coverage>"
       "<summary>Catalogue coverage and unresolved references</summary>"
       "<dl class=\"flow-facts\">"
       "<div><dt>Models</dt><dd>%ld</dd></div>"
       "<div><dt>Layer artifacts</dt><dd>%ld</dd></div>"
       "<div><dt>Capabilities</dt><dd>%ld</dd></div>"
       "<div><dt>Relations</dt><dd>%ld</dd></div>"
       "<div><dt>Unresolved</dt><dd>%ld</dd></div></dl>"
       "<p class=\"subj-note\">Models, layer artifacts and capabilities count registered entries. "
       "Relations count only links with registered source and destination entries.</p>"
       "<p class=\"subj-note\">%ld references point to catalogue entries that do not exist, "
       "mostly recipes and layer artifacts. They are excluded from relation counts, "
       "and cannot establish a dependency. Each model page lists its unresolved references.</p>"
       "</details><style>.model-list{display:grid;gap:10px;margin-top:22px}.model-list .model-card{position:relative;padding:16px 16px 16px 62px;border:1px solid rgba(16,32,26,.16);border-radius:16px;background:#fff;min-width:0}.model-list .model-card:first-of-type{border-top:1px solid rgba(16,32,26,.16)}.model-card:focus-visible{outline:3px solid #2f6145;outline-offset:3px}.model-mark{position:absolute;left:15px;top:15px;display:grid;place-items:center;width:34px;height:34px;border-radius:10px;background:#e5efde;color:#2b5138}.model-mark .ak-icon{width:20px;height:20px}.model-list .desk-l,.model-list .desk-s{overflow-wrap:anywhere}.model-list .desk-a{white-space:normal;text-align:right}@media(max-width:560px){.model-list .model-card{grid-template-columns:minmax(0,1fr)}.model-list .desk-a{grid-row:auto;grid-column:1;text-align:left}}</style><div class=\"model-list\" data-bf-list data-bf-row-height=\"104\">",
       models,layers,caps,edges,dangling,dangling))goto done;
    if(sqlite3_prepare_v2(db,
       "SELECT n.external_id, n.name, COALESCE(json_extract(n.json_data,'$.format'),''),"
       " COALESCE(json_extract(n.json_data,'$.size_mb'),0),"
       " (SELECT COUNT(*) FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.dst_node_id"
       "   WHERE e.src_node_id=n.node_id"
       "    AND e.rel IN ('powers_capability','implements_capability','supports_capability')),"
       " (SELECT COUNT(*) FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.src_node_id"
       "   WHERE e.dst_node_id=n.node_id"
       "    AND e.rel IN ('uses_model','sources_model','has_model')),"
       " (SELECT COUNT(*) FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.src_node_id"
       "   WHERE e.dst_node_id=n.node_id AND e.rel='derived_from_model'),"
       " (SELECT COUNT(*) FROM catalog_edges e WHERE"
       "   (e.src_node_id=n.node_id AND NOT EXISTS"
       "      (SELECT 1 FROM catalog_nodes o WHERE o.node_id=e.dst_node_id))"
       "   OR (e.dst_node_id=n.node_id AND NOT EXISTS"
       "      (SELECT 1 FROM catalog_nodes o WHERE o.node_id=e.src_node_id)))"
       " FROM catalog_nodes n WHERE n.kind='model'"
       " ORDER BY json_extract(n.json_data,'$.size_mb') DESC, n.name",
       -1,&st,NULL)!=SQLITE_OK)goto done;
    int rc;
    while((rc=sqlite3_step(st))==SQLITE_ROW){
        if(fw>=cap-((cap>2048)?2048:cap))break;
        char eid[192],name[320],format[64];
        const double size_mb=sqlite3_column_double(st,3);
        long powers=(long)sqlite3_column_int64(st,4);
        long taken=(long)sqlite3_column_int64(st,5);
        long derived=(long)sqlite3_column_int64(st,6);
        long unresolved=(long)sqlite3_column_int64(st,7);
        escaped((const char *)sqlite3_column_text(st,0),eid,sizeof(eid));
        escaped((const char *)sqlite3_column_text(st,1),name,sizeof(name));
        escaped((const char *)sqlite3_column_text(st,2),format,sizeof(format));
        if(!append(facts,cap,&fw,
          "<a class=\"desk-row model-card\" href=\"/s/model/%s\"><span class=\"desk-l\">%s</span>"
          "<span class=\"desk-a\">%s, %.0f MB</span>"
          "<span class=\"desk-s\">powers %ld &middot; used by %ld &middot; %ld derived from it%s</span>"
          "<span class=\"model-mark\">" BF_ICON_ROBOT "</span></a>",
          eid,name,format[0]?format:"unstated",size_mb,powers,taken,derived,
          unresolved?" &middot; some references unresolved":""))goto done;
    }
    if(rc!=SQLITE_DONE)goto done;
    if(!append(facts,cap,&fw,"</div>"))goto done;
    *used=fw;state=1;
done:
    sqlite3_finalize(st);
    if(db)sqlite3_close(db);
    if(state<0){facts[0]='\0';*used=0;}
    return state;
}

/* Exact catalogue details; route, prices, authorizations remain upstream. */
int bf_model_catalogue_detail(const char *path,const char *ident,
     char *name,size_t name_cap,char *facts,size_t cap,size_t *used) {
    if(used)*used=0;
    if(!ident||!*ident||!name||!name_cap||!facts||cap<262144||!used)return -1;
    name[0]='\0';facts[0]='\0';
        sqlite3 *db = NULL; sqlite3_stmt *st = NULL; char uri[PATH_MAX];
        size_t fw = 0;
        char summary[2048] = {0}, format[64] = {0}, geometry[96] = {0};
        char family[96] = {0};
        double size_mb = 0, mean_f1 = 0;
        /* A catalogue node is keyed by text ("model:llama-3-8b-instruct"), not by
         * a row number. Reading it as an integer gave 0, bound 0, matched
         * nothing, and every relation band silently disappeared while the index
         * page counting the same edges showed nine dependents. */
        char node_id[192] = {0};

        if (!path || !*path ||
            bf_sqlite_readonly_uri(uri, sizeof(uri), path) >= (int)sizeof(uri) ||
            sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
            if (db) sqlite3_close(db);
            return -1;
        }
        if (sqlite3_prepare_v2(db,
              "SELECT node_id, name, COALESCE(summary,''),"
              " COALESCE(json_extract(json_data,'$.format'),''),"
              " COALESCE(json_extract(json_data,'$.size_mb'),0),"
              " COALESCE(json_extract(json_data,'$.geometry'),''),"
              " COALESCE(json_extract(json_data,'$.transform_family'),''),"
              " COALESCE(json_extract(json_data,'$.mean_f1'),0)"
              " FROM catalog_nodes WHERE kind='model' AND external_id=?1", -1, &st, NULL) != SQLITE_OK ||
            (sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC), sqlite3_step(st)) != SQLITE_ROW) {
            if (st) sqlite3_finalize(st);
            sqlite3_close(db);
            return 0;
        }
        snprintf(node_id, sizeof(node_id), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(name, name_cap, "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(summary, sizeof(summary), "%s", (const char *)sqlite3_column_text(st, 2));
        snprintf(format, sizeof(format), "%s", (const char *)sqlite3_column_text(st, 3));
        size_mb = sqlite3_column_double(st, 4);
        snprintf(geometry, sizeof(geometry), "%s", (const char *)sqlite3_column_text(st, 5));
        snprintf(family, sizeof(family), "%s", (const char *)sqlite3_column_text(st, 6));
        mean_f1 = sqlite3_column_double(st, 7);
        sqlite3_finalize(st); st = NULL;

        {
            char es[4096], eg[192], efam[192], eformat[128];
            escaped(summary, es, sizeof(es));
            escaped(format, eformat, sizeof(eformat));
            escaped(geometry, eg, sizeof(eg));
            escaped(family, efam, sizeof(efam));
            fw += (size_t)snprintf(facts + fw, cap - fw,
              "<dl class=\"flow-facts\">"
              "<div><dt>Format</dt><dd>%s</dd></div>"
              "<div><dt>Size</dt><dd>%.0f MB</dd></div>"
              "<div><dt>Geometry</dt><dd>%s</dd></div>"
              "<div><dt>Transform</dt><dd>%s</dd></div></dl>"
              "<p class=\"subj-note\">%s</p>",
              eformat[0] ? eformat : "unstated", size_mb,
              (eg[0] && strcmp(eg, "-")) ? eg : "not stated",
              (efam[0] && strcmp(efam, "-")) ? efam : "none",
              es);
            /* Representation passports the catalogue carries for this model.
             * Without this the page said nothing had been measured about a
             * model whose registry held a measured passport. Each is shown with
             * its admission state and its own envelope, never as a score. */
            int passports = 0;
            if (sqlite3_prepare_v2(db,
                  "SELECT o.name, COALESCE(o.category,''),"
                  " COALESCE(json_extract(o.json_data,'$.admission_scope'),''),"
                  " COALESCE(json_extract(o.json_data,'$.measurement_state'),''),"
                  " COALESCE(o.summary,''), COALESCE(o.source_hash,'')"
                  " FROM catalog_edges e JOIN catalog_nodes o ON o.node_id=e.dst_node_id"
                  " WHERE e.src_node_id=?1 AND e.rel='has_representation' AND o.kind='representation'"
                  " ORDER BY o.name LIMIT 12", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, node_id, -1, SQLITE_STATIC);
                while (sqlite3_step(st) == SQLITE_ROW && fw < cap - 6144) {
                    char rn[400], ra[64], rs[64], rm[64], rq[4096], rd[80];
                    escaped((const char *)sqlite3_column_text(st, 0), rn, sizeof(rn));
                    escaped((const char *)sqlite3_column_text(st, 1), ra, sizeof(ra));
                    escaped((const char *)sqlite3_column_text(st, 2), rs, sizeof(rs));
                    escaped((const char *)sqlite3_column_text(st, 3), rm, sizeof(rm));
                    escaped((const char *)sqlite3_column_text(st, 4), rq, sizeof(rq));
                    escaped((const char *)sqlite3_column_text(st, 5), rd, sizeof(rd));
                    if (!strcmp(rm, "measured")) passports++;
                    fw += (size_t)snprintf(facts + fw, cap - fw,
                      "<div class=\"desk-row\" data-representation=\"%s\"><span class=\"desk-l\">Representation passport</span>"
                      "<span class=\"desk-a\">%s %s, %s</span>"
                      "<span class=\"desk-s\">%s. %s Receipt %s.</span></div>",
                      rn, rm, rs, ra, rn, rq, rd);
                }
                sqlite3_finalize(st); st = NULL;
            }
            /* A measured figure is printed only where one exists. A zero here is
             * an absence of measurement, not a score of nothing. */
            if (passports > 0 && mean_f1 <= 0.0)
                ;
            else if (mean_f1 > 0.0)
                fw += (size_t)snprintf(facts + fw, cap - fw,
                  "<div class=\"desk-row\"><span class=\"desk-l\">Mean F1</span>"
                  "<span class=\"desk-a\">%.3f</span>"
                  "<span class=\"desk-s\">measured on this estate's own evaluation, which is not a "
                  "claim about any other task</span></div>", mean_f1);
            else
                fw += (size_t)snprintf(facts + fw, cap - fw,
                  "<div class=\"desk-row\"><span class=\"desk-l\">Measured quality</span>"
                  "<span class=\"desk-a\">none</span>"
                  "<span class=\"desk-s\">nothing has been measured about this model here. Its size "
                  "and format are registration facts; they say nothing about how well it works</span></div>");
        }

        /* Four bands, each one a relation in the graph. */
        {
            struct { const char *rel; const char *heading; const char *note; int outgoing; } bands[] = {
              {"derived_from_model", "What it came from",
               "a derivation is what this estate recorded about how the artifact was made, never a licence that travels with it", 1},
              {"has_layer_artifact", "Its layer artifacts",
               "a layer artifact exists as a file; it is not a loadable model on its own", 1},
              {"powers_capability", "What it powers",
               "powering a capability is not authority to invoke it, and not a claim it is the only model that could", 1},
              {"has_source", "Where it can be pulled from",
               "a pull source is an address this estate recorded, not a permission or a guarantee it answers", 1},
              {"uses_model", "What uses it",
               "something using a model is a dependency this estate recorded; it does not mean that thing currently runs", 0},
            };
            size_t b;
            for (b = 0; b < sizeof(bands) / sizeof(bands[0]) && fw < cap - 8192; b++) {
                int shown = 0;
                char sql[512];
                snprintf(sql, sizeof(sql),
                  "SELECT o.kind, o.name, COALESCE(o.external_id,'') FROM catalog_edges e"
                  " JOIN catalog_nodes o ON o.node_id = e.%s"
                  " WHERE e.%s = ?1 AND e.rel = ?2 ORDER BY o.name LIMIT 40",
                  bands[b].outgoing ? "dst_node_id" : "src_node_id",
                  bands[b].outgoing ? "src_node_id" : "dst_node_id");
                if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) continue;
                sqlite3_bind_text(st, 1, node_id, -1, SQLITE_STATIC);
                sqlite3_bind_text(st, 2, bands[b].rel, -1, SQLITE_STATIC);
                while (sqlite3_step(st) == SQLITE_ROW && fw < cap - 2048) {
                    char ek[64], en[400], eo[192];
                    const char *kind = (const char *)sqlite3_column_text(st, 0);
                    const char *name = (const char *)sqlite3_column_text(st, 1);
                    int local_source = kind && !strcmp(kind, "model_source") && name &&
                        (!strncasecmp(name, "file:", 5) || name[0] == '/' ||
                         (name[0] == '~' && name[1] == '/'));
                    escaped((const char *)sqlite3_column_text(st, 0), ek, sizeof(ek));
                    escaped(local_source ? "Local file" : name, en, sizeof(en));
                    escaped((const char *)sqlite3_column_text(st, 2), eo, sizeof(eo));
                    if (!shown) {
                        fw += (size_t)snprintf(facts + fw, cap - fw,
                          "<h3 class=\"room-h\">%s</h3>", bands[b].heading);
                        shown = 1;
                    }
                    if (!strcmp(ek, "model") && eo[0])
                        fw += (size_t)snprintf(facts + fw, cap - fw,
                          "<a class=\"desk-row\" href=\"/s/model/%s\"><span class=\"desk-l\">%s</span>"
                          "<span class=\"desk-a\">%s</span><span class=\"desk-s\">%s</span></a>",
                          eo, en, ek, bands[b].note);
                    else
                        fw += (size_t)snprintf(facts + fw, cap - fw,
                          "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                          "<span class=\"desk-a\">%s</span><span class=\"desk-s\">%s</span></div>",
                          en, ek, bands[b].note);
                }
                sqlite3_finalize(st); st = NULL;
            }
        }

        /* References this model carries to nodes the catalogue never registered.
         * A recipe that uses a model is real information even when the recipe was
         * never inserted, so it is named as an unresolved reference rather than
         * dropped by the join that could not follow it. */
        if (sqlite3_prepare_v2(db,
              "SELECT e.rel, CASE WHEN e.src_node_id=?1 THEN e.dst_node_id ELSE e.src_node_id END,"
              " CASE WHEN e.src_node_id=?1 THEN 'names' ELSE 'named by' END"
              " FROM catalog_edges e WHERE"
              "  (e.src_node_id=?1 AND NOT EXISTS"
              "     (SELECT 1 FROM catalog_nodes o WHERE o.node_id=e.dst_node_id))"
              "  OR (e.dst_node_id=?1 AND NOT EXISTS"
              "     (SELECT 1 FROM catalog_nodes o WHERE o.node_id=e.src_node_id))"
              " ORDER BY 2 LIMIT 40", -1, &st, NULL) == SQLITE_OK) {
            int shown = 0;
            sqlite3_bind_text(st, 1, node_id, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < cap - 2048) {
                char erel[64], eref[200], edir[32];
                escaped((const char *)sqlite3_column_text(st, 0), erel, sizeof(erel));
                escaped((const char *)sqlite3_column_text(st, 1), eref, sizeof(eref));
                escaped((const char *)sqlite3_column_text(st, 2), edir, sizeof(edir));
                if (!shown) {
                    fw += (size_t)snprintf(facts + fw, cap - fw,
                      "<h3 class=\"room-h\">References the catalogue cannot follow</h3>");
                    shown = 1;
                }
                fw += (size_t)snprintf(facts + fw, cap - fw,
                  "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                  "<span class=\"desk-a\">%s %s</span>"
                  "<span class=\"desk-s\">this identifier was never registered as a node, so nothing "
                  "here can say what it is. The relation was recorded and the thing at the other end "
                  "was not</span></div>", eref, edir, erel);
            }
            sqlite3_finalize(st); st = NULL;
        }
        sqlite3_close(db);
    *used=fw;
    return 1;
}
