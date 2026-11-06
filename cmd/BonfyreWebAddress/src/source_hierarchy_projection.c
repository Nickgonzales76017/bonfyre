#define _POSIX_C_SOURCE 200809L
#include "source_hierarchy_projection.h"

#include <sqlite3.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "bf_sqlite_path.h"
#include "estate_reference.h"

static void h(FILE *out, const char *s) {
    const unsigned char *p = (const unsigned char *)(s ? s : "");
    for (; *p; p++) switch (*p) {
        case '&': fputs("&amp;", out); break;
        case '<': fputs("&lt;", out); break;
        case '>': fputs("&gt;", out); break;
        case '"': fputs("&quot;", out); break;
        case 39: fputs("&#39;", out); break;
        default: fputc(*p, out);
    }
}

static void url_component(FILE *out, const char *s) {
    static const char hex[] = "0123456789ABCDEF";
    const unsigned char *p = (const unsigned char *)(s ? s : "");
    for (; *p; p++) {
        if (isalnum(*p) || *p == '-' || *p == '_' || *p == '.' || *p == '~') fputc(*p, out);
        else { fputc('%', out); fputc(hex[*p >> 4], out); fputc(hex[*p & 15], out); }
    }
}

static const char *protocol_words(const char *p) {
    if (!p) return "Other";
    if (!strcasecmp(p, "unknown")) return "Other";
    if (!strcasecmp(p, "rss")) return "Feeds";
    if (!strcasecmp(p, "atom")) return "Atom feeds";
    if (!strcasecmp(p, "json_feed")) return "JSON feeds";
    if (!strcasecmp(p, "ics")) return "Calendars";
    if (!strcasecmp(p, "html")) return "Web pages";
    if (!strcasecmp(p, "openapi")) return "Documented APIs";
    if (!strcasecmp(p, "json_api")) return "APIs";
    if (!strcasecmp(p, "graphql")) return "Graph APIs";
    if (!strcasecmp(p, "socrata")) return "Open data";
    if (!strcasecmp(p, "arcgis")) return "Map data";
    if (!strcasecmp(p, "opml")) return "Feed collections";
    if (!strcasecmp(p, "activitypub")) return "Federated channels";
    if (!strcasecmp(p, "websub")) return "Live updates";
    if (!strcasecmp(p, "mcp")) return "Tools";
    return p;
}

static int table_exists(sqlite3 *db, const char *name) {
    sqlite3_stmt *st = NULL; int yes = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, name, -1, SQLITE_TRANSIENT);
        yes = sqlite3_step(st) == SQLITE_ROW;
    }
    sqlite3_finalize(st);
    return yes;
}

static int binary_channel_columns(sqlite3 *db) {
    const char *columns[] = {"family_id", "state", "first_seen"};
    size_t i;
    for (i = 0; i < sizeof(columns)/sizeof(columns[0]); i++) {
        const char *collation = NULL;
        if (sqlite3_table_column_metadata(db, "main", "channel", columns[i],
                NULL, &collation, NULL, NULL, NULL) != SQLITE_OK ||
            !collation || strcasecmp(collation, "BINARY")) return 0;
    }
    return 1;
}

static int channel_family_indexed(sqlite3 *db) {
    sqlite3_stmt *st = NULL; int yes = 0;
    if (sqlite3_prepare_v2(db,
            "SELECT 1 FROM pragma_index_list('channel') l JOIN pragma_index_info(l.name) i "
            "WHERE l.partial=0 AND i.seqno=0 AND i.name='family_id' LIMIT 1",
            -1, &st, NULL) == SQLITE_OK) yes = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return yes;
}

static int reader_selected(const char *const *readers,size_t count,const char *value){
    size_t i;if(!value||!*value)return 0;
    for(i=0;i<count;i++)if(readers[i]&&!strcmp(readers[i],value))return 1;
    return 0;
}

static int source_edge_readers(FILE *out,sqlite3_stmt *census,const char *channel_id,
                               const char *const *readers,size_t reader_count){
    int matched=0;
    sqlite3_reset(census);sqlite3_clear_bindings(census);
    sqlite3_bind_text(census,1,channel_id,-1,SQLITE_TRANSIENT);
    while(sqlite3_step(census)==SQLITE_ROW){
        const char *source=(const char *)sqlite3_column_text(census,0);
        const char *material=(const char *)sqlite3_column_text(census,1);
        if(!reader_selected(readers,reader_count,source))continue;
        if(!matched)fputs("<span class='source-edge-readers'><span class='source-edge-label'>Used here by</span> ",out);
        else fputs(", ",out);
        fputs("<a data-source-reader href='/sources?q=",out);url_component(out,source);fputs("'>",out);h(out,source);fputs("</a>",out);
        if(material&&*material){fputs(" <small>",out);h(out,material);fputs("</small>",out);}
        matched++;
    }
    if(matched)fputs("</span>",out);
    return matched;
}

int bf_source_hierarchy_emit(FILE *out, const char *db_path,
                             const char *const *readers, size_t reader_count,
                             const char *route, unsigned root_limit,
                             unsigned channels_per_family) {
    sqlite3 *db = NULL;
    sqlite3_stmt *root = NULL, *family = NULL, *channel = NULL, *census = NULL;
    char **roots = NULL, predicate[4096] = {0}, channel_sql[8192], temp_sql[4096], family_sql[1024];
    size_t roots_n = 0, i, pw = 0;
    int rendered = 0, have_temp = 0;

    if (!out || !db_path || !*db_path || !readers || !reader_count) return 0;
    if (reader_count > 32) reader_count = 32;
    if (root_limit < 1) root_limit = 8;
    if (channels_per_family < 1) channels_per_family = 8;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) goto done;
    if (!table_exists(db, "channel") || !table_exists(db, "channel_census") ||
        !table_exists(db, "root") || !table_exists(db, "family")) goto done;

    roots = calloc(root_limit, sizeof(*roots));
    if (!roots) goto done;
    for (i = 0; i < reader_count && roots_n < root_limit; i++) {
        sqlite3_stmt *st = NULL;
        if (!readers[i] || !*readers[i]) continue;
        if (sqlite3_prepare_v2(db,
            "SELECT DISTINCT c.root_id FROM channel_census cc JOIN channel c ON c.channel_id=cc.channel_id "
            "WHERE cc.census_source=?1 ORDER BY c.contributions DESC,c.last_contributed DESC", -1, &st, NULL) != SQLITE_OK) continue;
        sqlite3_bind_text(st, 1, readers[i], -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW && roots_n < root_limit) {
            const char *rid = (const char *)sqlite3_column_text(st, 0);
            int seen = 0; size_t j;
            for (j = 0; j < roots_n; j++) if (!strcmp(roots[j], rid ? rid : "")) { seen = 1; break; }
            if (!seen && rid && *rid) roots[roots_n++] = strdup(rid);
        }
        sqlite3_finalize(st);
    }
    if (!roots_n) goto done;

    /* Build a private, indexed working set. A channel's root_id need not agree
       with its family's root_id, so select by family rather than channel root. */
    {
        sqlite3_stmt *st = NULL;
        size_t n = (size_t)snprintf(temp_sql, sizeof(temp_sql),
            "CREATE TEMP TABLE hierarchy_channel AS SELECT channel_id,family_id,address,title,state,virtual,material,contributions,first_seen FROM channel WHERE family_id IN (SELECT family_id FROM family WHERE root_id IN (");
        for (i = 0; i < roots_n && n < sizeof(temp_sql); i++) {
            int written = snprintf(temp_sql+n, sizeof(temp_sql)-n, "%s?%zu", i ? "," : "", i+1);
            if (written < 0 || (size_t)written >= sizeof(temp_sql)-n) { n = sizeof(temp_sql); break; }
            n += (size_t)written;
        }
        if (!channel_family_indexed(db) && binary_channel_columns(db) && n < sizeof(temp_sql) && snprintf(temp_sql+n, sizeof(temp_sql)-n, "))") < (int)(sizeof(temp_sql)-n) &&
            sqlite3_prepare_v2(db, temp_sql, -1, &st, NULL) == SQLITE_OK) {
            for (i = 0; i < roots_n; i++) sqlite3_bind_text(st, (int)i+1, roots[i], -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_DONE) have_temp = 1;
        }
        sqlite3_finalize(st);
        if (have_temp && sqlite3_exec(db, "CREATE INDEX temp.hierarchy_channel_family ON hierarchy_channel(family_id)", NULL, NULL, NULL) != SQLITE_OK) have_temp = 0;
    }

    for(i=0;i<reader_count;i++){
        int n=snprintf(predicate+pw,sizeof(predicate)-pw,"%scc.census_source=?%zu",pw?" OR ":"",i+2);
        if(n<0||(size_t)n>=sizeof(predicate)-pw)goto done;pw+=(size_t)n;
    }
    if(snprintf(channel_sql,sizeof(channel_sql),
        "SELECT COALESCE(NULLIF(c.title,''),c.address),c.address,c.state,c.virtual,"
        "COALESCE(c.material,''),c.contributions,c.channel_id,"
        "EXISTS(SELECT 1 FROM channel_census cc WHERE cc.channel_id=c.channel_id AND (%s)) AS used_here "
        "FROM %s c WHERE c.family_id=?1 "
        "ORDER BY used_here DESC,c.state='active' DESC,c.state='admitted' DESC,c.contributions DESC,c.virtual ASC,c.first_seen ASC LIMIT ?%zu",
        predicate,have_temp ? "temp.hierarchy_channel" : "main.channel",reader_count+2) >= (int)sizeof(channel_sql)) goto done;

    fprintf(out,
        "<section class='source-depth' aria-labelledby='source-depth-title'>"
        "<header><h2 id='source-depth-title'>Where this comes from</h2>"
        "<p>Solid branches are exact SourceFrontier census edges used by this page. Folded branches share the publisher but did not supply these items.</p></header>");

    if (sqlite3_prepare_v2(db,
        "SELECT host,COALESCE(NULLIF(name,''),(SELECT title FROM channel WHERE root_id=r.root_id AND COALESCE(title,'')<>'' "
        "ORDER BY state IN ('admitted','active') DESC LIMIT 1),host),COALESCE(identity,''),"
        "(SELECT COUNT(*) FROM channel WHERE root_id=r.root_id),"
        "(SELECT COUNT(*) FROM channel WHERE root_id=r.root_id AND state IN ('admitted','active')) "
        "FROM root r WHERE root_id=?1", -1, &root, NULL) != SQLITE_OK) goto close_section;
    snprintf(family_sql, sizeof(family_sql),
        "SELECT f.family_id,f.protocol,COUNT(c.channel_id),SUM(c.state IN ('admitted','active')) "
        "FROM family f JOIN %s c ON c.family_id=f.family_id WHERE f.root_id=?1 "
        "GROUP BY f.family_id ORDER BY SUM(c.state IN ('admitted','active')) DESC,COUNT(c.channel_id) DESC,f.protocol",
        have_temp ? "temp.hierarchy_channel" : "main.channel");
    if (sqlite3_prepare_v2(db, family_sql, -1, &family, NULL) != SQLITE_OK) goto close_section;
    if (sqlite3_prepare_v2(db, channel_sql, -1, &channel, NULL) != SQLITE_OK) goto close_section;
    if (sqlite3_prepare_v2(db,
        "SELECT census_source,COALESCE(material,'') FROM channel_census WHERE channel_id=?1 ORDER BY census_source",
        -1,&census,NULL) != SQLITE_OK) goto close_section;

    fputs("<div class='source-depth-roots'>", out);
    for (i = 0; i < roots_n; i++) {
        const char *name, *host, *identity;
        int chans, readable;
        sqlite3_reset(root); sqlite3_clear_bindings(root);
        sqlite3_bind_text(root, 1, roots[i], -1, SQLITE_TRANSIENT);
        if (sqlite3_step(root) != SQLITE_ROW) continue;
        host = (const char *)sqlite3_column_text(root, 0);
        name = (const char *)sqlite3_column_text(root, 1);
        identity = (const char *)sqlite3_column_text(root, 2);
        chans = sqlite3_column_int(root, 3);
        readable = sqlite3_column_int(root, 4);

        fputs("<article class='source-root'><h3><a href='", out);
        h(out, route && *route ? route : "/channels");
        fputs("?s=", out); url_component(out, name); fputs("'>", out);
        h(out, name); fputs("</a></h3><p class='source-root-meta'>", out);
        fprintf(out, "%d feed%s", chans, chans == 1 ? "" : "s");
        if (readable) fprintf(out, " &middot; %d available", readable);
        if (host && *host && strcmp(host, name)) { fputs(" &middot; ", out); h(out, host); }
        {
            char own[600];
            if (estate_publisher_link(NULL, host, identity, own, sizeof(own))) {
                fputs(" &middot; <a href='", out); h(out, own); fputs("'>Its page</a>", out);
            }
        }
        fputs("</p><div class='source-families'>", out);

        sqlite3_reset(family); sqlite3_clear_bindings(family);
        sqlite3_bind_text(family, 1, roots[i], -1, SQLITE_TRANSIENT);
        while (sqlite3_step(family) == SQLITE_ROW) {
            const char *fid = (const char *)sqlite3_column_text(family, 0);
            const char *proto = (const char *)sqlite3_column_text(family, 1);
            int count = sqlite3_column_int(family, 2), admitted = sqlite3_column_int(family, 3);
            int shown=0,used=0,related_open=0;
            fputs("<details class='source-family' open><summary><b>", out); h(out, protocol_words(proto));
            fprintf(out, "</b><span>%d channel%s", count, count==1?"":"s");
            if (admitted) fprintf(out, " &middot; %d available", admitted);
            fputs("</span></summary><ol class='source-used' aria-label='Channels used on this page'>", out);

            sqlite3_reset(channel); sqlite3_clear_bindings(channel);
            sqlite3_bind_text(channel,1,fid,-1,SQLITE_TRANSIENT);
            for(size_t rr=0;rr<reader_count;rr++)sqlite3_bind_text(channel,(int)rr+2,readers[rr],-1,SQLITE_TRANSIENT);
            sqlite3_bind_int(channel,(int)reader_count+2,(int)channels_per_family);
            while(sqlite3_step(channel)==SQLITE_ROW){
                const char *title=(const char *)sqlite3_column_text(channel,0);
                const char *address=(const char *)sqlite3_column_text(channel,1);
                const char *state=(const char *)sqlite3_column_text(channel,2);
                int virtual=sqlite3_column_int(channel,3);
                const char *material=(const char *)sqlite3_column_text(channel,4);
                int contributions=sqlite3_column_int(channel,5);
                const char *channel_id=(const char *)sqlite3_column_text(channel,6);
                int used_here=sqlite3_column_int(channel,7);
                if(!used_here&&!related_open){
                    if(!used)fputs("<li class='source-none'>No channel in this family supplied the current page.</li>",out);
                    fputs("</ol><details class='source-related'><summary>Other related channels from this publisher — not used on this page</summary><ol>",out);
                    related_open=1;
                }
                if(used_here){
                    fputs("<li class='source-edge' data-source-edge><div class='source-edge-path'>",out);
                    source_edge_readers(out,census,channel_id,readers,reader_count);
                    fputs(" <span class='source-edge-arrow' aria-hidden='true'>&rarr;</span> <a rel='nofollow' href='",out);h(out,address);fputs("'>",out);h(out,title);fputs("</a></div>",out);
                    fputs("<span class='channel-state'>used on this page</span>",out);
                    if(material&&*material){fputs("<small>",out);h(out,material);if(contributions)fprintf(out," &middot; %d shown",contributions);fputs("</small>",out);}
                    fputs("</li>",out);used++;
                }else{
                    fputs("<li class='source-related-channel'><div><a rel='nofollow' href='",out);h(out,address);fputs("'>",out);h(out,title);fputs("</a>",out);
                    fputs("<span class='channel-state'>related</span><small>same publisher &middot; not used on this page",out);
                    if(state&&(!strcmp(state,"active")||!strcmp(state,"admitted")))fputs(" &middot; available",out);else if(virtual)fputs(" &middot; listed",out);
                    fputs("</small></div></li>",out);
                }
                shown++;
            }
            if(related_open){
                if(count>shown)fprintf(out,"<li class='source-tail'>and %d more channels at this publisher</li>",count-shown);
                fputs("</ol></details>",out);
            }else{
                fputs("</ol>",out);
                if(count>shown)fprintf(out,"<details class='source-related'><summary>Other related channels from this publisher — not used on this page</summary><ol><li class='source-tail'>and %d more channels at this publisher</li></ol></details>",count-shown);
            }
            fputs("</details>",out);
        }
        fputs("</div></article>", out);
        rendered++;
    }
    fputs("</div>", out);

close_section:
    fputs("</section>", out);
done:
    sqlite3_finalize(root);
    sqlite3_finalize(family);
    sqlite3_finalize(channel);
    sqlite3_finalize(census);
    if (roots) {
        for (i = 0; i < roots_n; i++) free(roots[i]);
        free(roots);
    }
    if (db) sqlite3_close(db);
    return rendered;
}
