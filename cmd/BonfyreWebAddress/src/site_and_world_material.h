#ifndef BONFYRE_SITE_AND_WORLD_MATERIAL_H
#define BONFYRE_SITE_AND_WORLD_MATERIAL_H
/* Extracted verbatim from main.c lines 5111-6404 (mechanical split, no logic changes). */
static int site_program_publications(sqlite3 *db, const char *collection,
                                     NativePublication *out, size_t cap) {
    sqlite3_stmt *st = NULL; int count = 0;
    if (!db || !collection || !*collection || !out || !cap ||
        !cms_type_registered(db, "publication_occurrence") ||
        !cms_type_registered(db, "source_collection")) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT p.title,COALESCE(p.canonical_href,''),COALESCE(p.published_at,p.observed_at,''),"
        "COALESCE(i.says,p.source_ref) FROM publication_occurrence p "
        "LEFT JOIN internet_source i ON i.source=p.source_ref AND i.status='published' "
        "WHERE p.status='published' AND p.visibility='public' AND p.assertion_state='OBSERVED' "
        "AND EXISTS (SELECT 1 FROM source_collection sc WHERE sc.collection_id=?1 "
        "AND sc.status='published' AND EXISTS (SELECT 1 FROM json_each(sc.material_kind) kind "
        "WHERE kind.value IN ('PublicationOccurrence','publication_occurrence')) AND EXISTS (SELECT 1 FROM json_each(sc.source_selector,'$.source_refs') ref "
        "WHERE ref.value=p.source_ref)) ORDER BY COALESCE(p.published_at,p.observed_at) DESC,p.id DESC LIMIT ?2",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, collection, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, (int)cap);
    while (sqlite3_step(st) == SQLITE_ROW && (size_t)count < cap) {
        NativePublication *p = &out[count++];
#define SITE_PUB_COPY(field,col) snprintf(p->field, sizeof(p->field), "%s", sqlite3_column_text(st,col) ? (const char *)sqlite3_column_text(st,col) : "")
        SITE_PUB_COPY(title,0); SITE_PUB_COPY(href,1); SITE_PUB_COPY(when,2); SITE_PUB_COPY(attribution,3);
#undef SITE_PUB_COPY
    }
    sqlite3_finalize(st); return count;
}

static const BfSiteApplicationLowerer *site_application_lowerer(const char *kind) {
    size_t i;
    for (i = 0; i < BF_SITE_APPLICATION_LOWERER_COUNT; i++)
        if (kind && !strcmp(bf_site_application_lowerers[i].application_kind, kind))
            return &bf_site_application_lowerers[i];
    return NULL;
}

/* A selected profile is CMS presentation data, not a route switch in C.  The
 * visual token boundary is intentionally closed: values may tune the named
 * CSS variables, but cannot inject a selector, declaration, URL or script.
 * Reference-candidate Ghost rows are excluded by the published-state query;
 * a marketplace row becomes live art direction only after its own study has
 * promoted it to a real PresentationTemplate. */
/* A site outside the estate's territories is its operator's own: it is named
 * by its own template, not by the estate.  Returns 1 and fills name when the
 * profile's template states one. */
static int site_program_own_name(sqlite3 *db, const char *profile, const char *territory, char *name, size_t cap) {
    sqlite3_stmt *st = NULL;
    const char *bare = profile ? strchr(profile, ':') : NULL;
    int found = 0;
    if (!db || !profile || !territory || strcmp(territory, "independent") ||
        !cms_type_registered(db, "presentation_template")) return 0;
    if (sqlite3_prepare_v2(db, "SELECT reference_name FROM presentation_template WHERE (template=?1 OR template=?2) "
                               "AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, profile, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, bare ? bare + 1 : profile, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0) && *sqlite3_column_text(st, 0)) {
        snprintf(name, cap, "%s", (const char *)sqlite3_column_text(st, 0));
        found = 1;
    }
    sqlite3_finalize(st);
    return found;
}

static void site_program_emit_profile_tokens(sqlite3 *db, const char *profile) {
    bf_site_presentation_emit(stdout, db, profile);
}

/* Navigation is authored as grammar names, then resolved through the same
 * generated SurfaceProgram registry that owns public routes.  The CMS cannot
 * smuggle an arbitrary URL or manufacture a navigation destination by typing
 * a label; an unpublished/unknown grammar simply does not lower. */
static void site_program_emit_navigation(sqlite3 *db, const char *site) {
    sqlite3_stmt *st = NULL;
    int count = 0;
    if (!db || !site || !*site ||
        sqlite3_prepare_v2(db,
          "SELECT n.value FROM site_presentation p, json_each(p.navigation) n "
          "WHERE p.site_id=?1 AND p.status='published' AND json_type(p.navigation)='array' "
          "ORDER BY n.key", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, site, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *grammar = (const char *)sqlite3_column_text(st, 0);
        const BfNativeSurfaceProgram *program = native_surface_program(grammar);
        if (!program) continue;
        if (!count++) fputs("<nav class=\"site-program-nav\" aria-label=\"Site navigation\">", stdout);
        fputs("<a href=\"", stdout); html(program->route); fputs("\">", stdout);
        html(grammar); fputs("</a>", stdout);
    }
    if (count) fputs("</nav>", stdout);
    sqlite3_finalize(st);
}

static int site_program_uses_application(sqlite3 *db, const char *site, const char *kind) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (!db || !site || !kind || !cms_type_registered(db, "application_binding") ||
        sqlite3_prepare_v2(db,
          "SELECT 1 FROM application_binding WHERE site_id=?1 AND application_kind=?2 "
          "AND status='published' LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, site, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, kind, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return found;
}

/* Owner readers normalize their lawful projections into this narrow material
 * stream.  Everything after this boundary is a generic SurfaceIR renderer;
 * it never branches on an application name, source, protocol or World. */
#define SITE_MATERIAL_MAX 50
typedef struct {
    char title[512], byline[512], when[256], href[1024], body[2048];
    char asset[1024], poster[1024], transcript[1024];
    long duration_ms;
    int reaction_count;
} SiteMaterialItem;
typedef struct {
    char title[512], description[1024], metric[128], boundary[1024];
    int count;
    SiteMaterialItem item[SITE_MATERIAL_MAX];
} SiteMaterialStream;

static void site_material_copy(char *out, size_t cap, const unsigned char *in) {
    snprintf(out, cap, "%s", in ? (const char *)in : "");
}

/* A social relation may remain retained after it stops being current.  These
 * readers are intentionally shared by the full Community application and the
 * small site-program projection: an expired member, a withdrawn reaction, or
 * a stale presence cannot become socially current merely because a second
 * renderer used a cheaper query. */
static int community_active_member_count(sqlite3 *db, const char *community) {
    sqlite3_stmt *st = NULL; int count = 0;
    if (!db || !community || !cms_type_registered(db, "community_membership") ||
        sqlite3_prepare_v2(db,
          "SELECT COUNT(*) FROM community_membership WHERE community=?1 AND status='published' AND state='active' "
          "AND (COALESCE(expires_at,'')='' OR julianday(expires_at)>julianday('now'))",
          -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, community, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st); return count;
}

static int community_public_presence_count(sqlite3 *db, const char *community) {
    sqlite3_stmt *st = NULL; int count = 0;
    if (!db || !community || !cms_type_registered(db, "community_presence") ||
        sqlite3_prepare_v2(db,
          "SELECT COUNT(*) FROM community_presence p WHERE p.community=?1 AND p.surface='community' "
          "AND p.status='published' AND p.disclosure='public' "
          "AND julianday(p.observed_at)<=julianday('now') AND julianday(p.expires_at)>julianday('now') "
          "AND EXISTS (SELECT 1 FROM community_membership m WHERE m.community=p.community AND m.member_subject=p.actor_subject "
          "AND m.status='published' AND m.state='active' AND (COALESCE(m.expires_at,'')='' OR julianday(m.expires_at)>julianday('now')))",
          -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, community, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st); return count;
}

static int community_post_reaction_count(sqlite3 *db, const char *post) {
    sqlite3_stmt *st = NULL; int count = 0;
    if (!db || !post || !cms_type_registered(db, "community_reaction") ||
        sqlite3_prepare_v2(db,
          "SELECT COUNT(*) FROM community_reaction WHERE post=?1 AND status='published' AND state='active'",
          -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, post, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st); return count;
}

/* What a named collection of sources is, in the words a reader recognises.
 * The grouping and its value are published facts on the collection itself, so
 * this states them rather than deriving a name from the identifier.  The
 * description is the boundary: a collection standing on a subject's page is
 * this estate's own reading, never something that subject chose. */
static void site_collection_character(sqlite3 *db, const char *collection,
                                      char *title, size_t title_sz,
                                      char *description, size_t description_sz) {
    sqlite3_stmt *st = NULL;
    char grouping[64] = {0}, value[128] = {0};
    int sources = 0;
    if (!db || !collection || !*collection || !cms_type_registered(db, "source_collection") ||
        sqlite3_prepare_v2(db,
          "SELECT COALESCE(json_extract(source_selector,'$.grouping'),''),"
          " COALESCE(json_extract(source_selector,'$.value'),''),"
          " COALESCE(json_array_length(source_selector,'$.source_refs'),0)"
          " FROM source_collection WHERE collection_id=?1 AND status='published' ORDER BY id DESC LIMIT 1",
          -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, collection, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        snprintf(grouping, sizeof(grouping), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(value, sizeof(value), "%s", (const char *)sqlite3_column_text(st, 1));
        sources = sqlite3_column_int(st, 2);
    }
    sqlite3_finalize(st);
    if (!value[0]) return;
    if (!strcmp(grouping, "carries"))
        snprintf(title, title_sz, "What this estate reads that carries %s", value);
    else
        snprintf(title, title_sz, "%s, as this estate reads it", value);
    snprintf(description, description_sz,
             "%d sources this estate observed. The collection is this estate's own arrangement "
             "of its reading; nothing here was chosen, contributed or endorsed by whoever stands "
             "on this address.", sources);
}

static int site_material_publications(sqlite3 *db, const char *collection, int max_items,
                                      SiteMaterialStream *stream) {
    NativePublication rows[18]; int i;
    if (max_items < 1) { max_items = 1; } if (max_items > 18) max_items = 18;
    stream->count = site_program_publications(db, collection, rows, (size_t)max_items);
    /* A reading surface says which named collection it is reading, and that
     * the collection is this estate's own arrangement of what it observed.
     * Without this the section headed itself with its first article's title,
     * which reads as though the page were claiming that article. */
    site_collection_character(db, collection, stream->title, sizeof(stream->title),
                              stream->description, sizeof(stream->description));
    if (stream->count)
        snprintf(stream->metric, sizeof(stream->metric), "%d observed here, newest first", stream->count);
    for (i = 0; i < stream->count; i++) {
        /* Bounded copies: a publication row is wider than the stream item. */
        snprintf(stream->item[i].title, sizeof(stream->item[i].title), "%.*s", (int)sizeof(stream->item[i].title) - 1, rows[i].title);
        snprintf(stream->item[i].byline, sizeof(stream->item[i].byline), "%.*s", (int)sizeof(stream->item[i].byline) - 1, rows[i].attribution);
        snprintf(stream->item[i].when, sizeof(stream->item[i].when), "%.*s", (int)sizeof(stream->item[i].when) - 1, rows[i].when);
        snprintf(stream->item[i].href, sizeof(stream->item[i].href), "%.*s", (int)sizeof(stream->item[i].href) - 1, rows[i].href);
    }
    return stream->count;
}

static int site_material_community(sqlite3 *db, const char *community, int max_items,
                                   SiteMaterialStream *stream) {
    sqlite3_stmt *st = NULL; int members = 0, present = 0;
    if (!db || !community || !*community || !cms_type_registered(db, "community_body") ||
        !cms_type_registered(db, "community_post") ||
        sqlite3_prepare_v2(db, "SELECT title,COALESCE(description,'') FROM community_body "
          "WHERE community=?1 AND status='published' AND visibility='public' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, community, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return 0; }
    site_material_copy(stream->title, sizeof(stream->title), sqlite3_column_text(st, 0));
    site_material_copy(stream->description, sizeof(stream->description), sqlite3_column_text(st, 1));
    sqlite3_finalize(st); st = NULL;
    members = community_active_member_count(db, community);
    present = community_public_presence_count(db, community);
    /* Membership is a community's measure; a site of posts has readers, and
     * says nothing rather than "0 active members". */
    if (site_program_own_render) stream->metric[0] = '\0';
    else snprintf(stream->metric, sizeof(stream->metric), "%d active members · %d here now", members, present);
    if (max_items < 1) { max_items = 1; } if (max_items > SITE_MATERIAL_MAX) max_items = SITE_MATERIAL_MAX;
    if (sqlite3_prepare_v2(db, "SELECT post,author_subject,COALESCE(title,''),body,published_at FROM community_post "
        "WHERE community=?1 AND status='published' AND visibility='public' "
        "AND moderation_state IN ('accepted','visible','published') ORDER BY published_at DESC,id DESC LIMIT ?2",
        -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, community, -1, SQLITE_TRANSIENT); sqlite3_bind_int(st, 2, max_items);
    while (stream->count < max_items && sqlite3_step(st) == SQLITE_ROW) {
        SiteMaterialItem *item = &stream->item[stream->count++];
        const char *post = (const char *)sqlite3_column_text(st, 0);
        site_material_copy(item->byline, sizeof(item->byline), sqlite3_column_text(st, 1));
        site_material_copy(item->title, sizeof(item->title), sqlite3_column_text(st, 2));
        site_material_copy(item->body, sizeof(item->body), sqlite3_column_text(st, 3));
        site_material_copy(item->when, sizeof(item->when), sqlite3_column_text(st, 4));
        item->reaction_count = community_post_reaction_count(db, post);
    }
    sqlite3_finalize(st); return 1;
}

static int site_material_recording(sqlite3 *db, const char *recording, SiteMaterialStream *stream) {
    sqlite3_stmt *st = NULL; char subject[256]; SiteMaterialItem *item = &stream->item[0];
    if (!db || !recording || !studio_safe_slug_arg(recording) || !cms_type_registered(db, "music_recording") ||
        sqlite3_prepare_v2(db, "SELECT title,primary_artist_ref,COALESCE(duration_ms,0) FROM music_recording WHERE recording_id=?1 AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, recording, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return 0; }
    site_material_copy(item->title, sizeof(item->title), sqlite3_column_text(st, 0));
    site_material_copy(item->byline, sizeof(item->byline), sqlite3_column_text(st, 1));
    item->duration_ms = (long)sqlite3_column_int64(st, 2);
    snprintf(subject, sizeof(subject), "recording:%s", recording); sqlite3_finalize(st); st = NULL;
    if (cms_type_registered(db, "media_program") && sqlite3_prepare_v2(db,
        "SELECT asset_ref,COALESCE(transcript_ref,'') FROM media_program WHERE subject_ref=?1 "
        "AND media_kind='audio' AND publication_state='published' AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) { site_material_copy(item->asset, sizeof(item->asset), sqlite3_column_text(st, 0)); site_material_copy(item->transcript, sizeof(item->transcript), sqlite3_column_text(st, 1)); }
        sqlite3_finalize(st);
    }
    stream->count = 1; return 1;
}

static int site_material_media_program(sqlite3 *db, const char *program, SiteMaterialStream *stream) {
    sqlite3_stmt *st = NULL; SiteMaterialItem *item = &stream->item[0];
    if (!db || !program || !*program || strlen(program) >= 192 || strchr(program, '/') || strstr(program, "..") ||
        !cms_type_registered(db, "media_program") || sqlite3_prepare_v2(db,
        "SELECT subject_ref,asset_ref,COALESCE(poster_asset_ref,''),COALESCE(transcript_ref,''),COALESCE(duration_ms,0) FROM media_program "
        "WHERE program_id=?1 AND media_kind='video' AND publication_state='published' AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, program, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return 0; }
    site_material_copy(item->title, sizeof(item->title), sqlite3_column_text(st, 0));
    site_material_copy(item->asset, sizeof(item->asset), sqlite3_column_text(st, 1));
    site_material_copy(item->poster, sizeof(item->poster), sqlite3_column_text(st, 2));
    site_material_copy(item->transcript, sizeof(item->transcript), sqlite3_column_text(st, 3));
    item->duration_ms = (long)sqlite3_column_int64(st, 4);
    sqlite3_finalize(st); stream->count = 1; return 1;
}

/* WorldFragment is the canonical Board-pin relation.  This reader exposes
 * only its published presentation coordinate and the published nested World
 * address; it never mistakes a pin for the subject, source material or an
 * action aperture behind it. */
static int site_material_world_fragments(sqlite3 *db, const char *world, int max_items,
                                         SiteMaterialStream *stream, const char *klass) {
    sqlite3_stmt *st = NULL;
    char for_this_reader[192];
    /* A surface is the same surface to everyone; what can be done on it is
     * not. The presentation cells already say that -- an anonymous visitor is
     * asked a different question of the same grammar than the person whose
     * address it is -- so a World published for this reader's class answers
     * before the public one. A class with nothing published for it falls back
     * to the public World rather than to an empty page. */
    if (klass && *klass && strcmp(klass, "anonymous") && studio_safe_slug_arg(klass) &&
        snprintf(for_this_reader, sizeof(for_this_reader), "%s-%s", world, klass) < (int)sizeof(for_this_reader) &&
        studio_safe_slug_arg(for_this_reader) && cms_type_registered(db, "world_fragment")) {
        sqlite3_stmt *mine = NULL;
        if (sqlite3_prepare_v2(db, "SELECT 1 FROM world_fragment WHERE world=?1 AND status='published' LIMIT 1",
                               -1, &mine, NULL) == SQLITE_OK) {
            sqlite3_bind_text(mine, 1, for_this_reader, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(mine) == SQLITE_ROW) world = for_this_reader;
            sqlite3_finalize(mine);
        }
    }
    if (!db || !world || !studio_safe_slug_arg(world) || !cms_type_registered(db, "world_fragment") ||
        sqlite3_prepare_v2(db,
          "SELECT subject_address,surface_grammar,COALESCE(provenance,''),COALESCE(visible_sections,'') "
          "FROM world_fragment WHERE world=?1 AND status='published' ORDER BY ordinal,id LIMIT ?2",
          -1, &st, NULL) != SQLITE_OK) return 0;
    if (max_items < 1) max_items = 1;
    if (max_items > SITE_MATERIAL_MAX) max_items = SITE_MATERIAL_MAX;
    /* A World says what it is; the identifier is only how it is addressed.
     * Heading a person's own surfaces with their world's slug made the one
     * section that is genuinely theirs read like a database row. */
    snprintf(stream->title, sizeof(stream->title), "%s", world);
    snprintf(stream->description, sizeof(stream->description), "%s", "Published arrangements in this World.");
    if (cms_type_registered(db, "world")) {
        sqlite3_stmt *named = NULL;
        if (sqlite3_prepare_v2(db, "SELECT COALESCE(NULLIF(title,''),?1),COALESCE(NULLIF(lede,''),'') "
              "FROM world WHERE world=?1 AND status='published' ORDER BY id DESC LIMIT 1",
              -1, &named, NULL) == SQLITE_OK) {
            sqlite3_bind_text(named, 1, world, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(named) == SQLITE_ROW) {
                site_material_copy(stream->title, sizeof(stream->title), sqlite3_column_text(named, 0));
                if (sqlite3_column_text(named, 1) && *sqlite3_column_text(named, 1))
                    site_material_copy(stream->description, sizeof(stream->description), sqlite3_column_text(named, 1));
            }
            sqlite3_finalize(named);
        }
    }
    sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, max_items);
    while (stream->count < max_items && sqlite3_step(st) == SQLITE_ROW) {
        const char *grammar = (const char *)sqlite3_column_text(st, 1);
        SiteMaterialItem *item = &stream->item[stream->count++];
        site_material_copy(item->title, sizeof(item->title), sqlite3_column_text(st, 0));
        site_material_copy(item->byline, sizeof(item->byline), sqlite3_column_text(st, 1));
        site_material_copy(item->body, sizeof(item->body), sqlite3_column_text(st, 2));
        if (!item->body[0]) site_material_copy(item->body, sizeof(item->body), sqlite3_column_text(st, 3));
        if (grammar && studio_safe_slug_arg(grammar) && world_fragment_route_published(world, grammar))
            snprintf(item->href, sizeof(item->href), "/w/%s/%s", world, grammar);
        /* A reader is shown what the page is called, not the route it is
         * addressed by or the question its arrangement was designed against. */
        {
            static const char *const plain[][2] = {
                {"board", "Board"}, {"channels", "Channels"}, {"community", "Community"},
                {"console", "Status"}, {"discover", "Discover"}, {"inbox", "Inbox"},
                {"link", "Links"}, {"listings", "Listings"}, {"observations", "Observations"},
                {"packs", "Packs"}, {"spatial", "Map"}, {"stage", "Stage"}, {"studio", "Studio"},
                {"territory", "About this address"}, {"worlds", "Pages"},
            };
            size_t k;
            for (k = 0; grammar && k < sizeof(plain) / sizeof(plain[0]); k++)
                if (!strcmp(grammar, plain[k][0])) {
                    snprintf(item->title, sizeof(item->title), "%s", plain[k][1]);
                    item->byline[0] = '\0'; item->body[0] = '\0';
                    break;
                }
        }
    }
    sqlite3_finalize(st);
    snprintf(stream->metric, sizeof(stream->metric), "%d page%s", stream->count, stream->count == 1 ? "" : "s");
    return stream->count;
}

static int site_material_resolve(sqlite3 *db, const BfSiteApplicationLowerer *lowerer,
                                 const char *selector, int max_items, SiteMaterialStream *stream,
                                 const char *klass) {
    memset(stream, 0, sizeof(*stream));
    if (!lowerer) return 0;
    switch (lowerer->reader) {
    case BF_SITE_MATERIAL_READER_PUBLICATION:
        return site_material_publications(db, selector, max_items, stream);
    case BF_SITE_MATERIAL_READER_COMMUNITY:
        return site_material_community(db, selector, max_items, stream);
    case BF_SITE_MATERIAL_READER_WORLD_FRAGMENT:
        return site_material_world_fragments(db, selector, max_items, stream, klass);
    case BF_SITE_MATERIAL_READER_RECORDING:
        return site_material_recording(db, selector, stream);
    case BF_SITE_MATERIAL_READER_MEDIA_PROGRAM:
        return site_material_media_program(db, selector, stream);
    case BF_SITE_MATERIAL_READER_NONE:
        return 0; /* An owner with no implemented reader remains typed-empty. */
    }
    return 0;
}

static void site_surface_link(const char *href, const char *label) {
    if (href && *href && public_http_url(href)) { fputs("<a href=\"", stdout); html(href); fputs("\" rel=\"noopener\" target=\"_blank\">", stdout); html(label); fputs("</a>", stdout); }
    else html(label);
}

static void site_surface_emit_header(const SiteMaterialStream *stream) {
    const SiteMaterialItem *item = &stream->item[0];
    fputs("<header>", stdout);
    if (stream->title[0]) { fputs("<h2>", stdout); html(stream->title); fputs("</h2>", stdout); }
    else if (item->title[0]) { fputs("<h2>", stdout); html(item->title); fputs("</h2>", stdout); }
    if (stream->description[0]) { fputs("<p>", stdout); html(stream->description); fputs("</p>", stdout); }
    /* On an operator's own site the header is the site speaking; the first
     * post's author and instant are that post's, and stay with it. */
    else if (!site_program_own_render && item->byline[0]) { fputs("<p>", stdout); html(item->byline); fputs("</p>", stdout); }
    if (stream->metric[0]) { fputs("<small>", stdout); html(stream->metric); fputs("</small>", stdout); }
    else if (!site_program_own_render && item->when[0]) { fputs("<time>", stdout); html(item->when); fputs("</time>", stdout); }
    fputs("</header>", stdout);
}

static void site_surface_emit_lead(const SiteMaterialStream *stream) {
    const SiteMaterialItem *item = &stream->item[0];
    fputs("<div class=\"application-lead\"><h2>", stdout);
    site_surface_link(item->href, item->title);
    fputs("</h2></div>", stdout);
}

static void site_surface_emit_stream(const SiteMaterialStream *stream) {
    int i;
    if (stream->count < 2) return;
    fputs("<ol class=\"application-stream\">", stdout);
    for (i = 1; i < stream->count; i++) {
        fputs("<li>", stdout); site_surface_link(stream->item[i].href, stream->item[i].title);
        fputs("<small>", stdout); html(stream->item[i].byline);
        if (stream->item[i].when[0]) { fputs(" · ", stdout); html(stream->item[i].when); }
        fputs("</small></li>", stdout);
    }
    fputs("</ol>", stdout);
}

static void site_surface_emit_thread(const SiteMaterialStream *stream) {
    int i;
    fputs("<div class=\"application-thread\">", stdout);
    for (i = 0; i < stream->count; i++) {
        fputs("<article><header><span>", stdout); html(stream->item[i].byline);
        if (site_program_own_render && strlen(stream->item[i].when) >= 10) {
            /* A reader wants the day; the machine keeps the instant. */
            char day[11];
            snprintf(day, sizeof(day), "%.10s", stream->item[i].when);
            fputs("</span><time datetime=\"", stdout); html(stream->item[i].when); fputs("\">", stdout); html(day);
            fputs("</time></header>", stdout);
        } else {
        fputs("</span><time>", stdout); html(stream->item[i].when); fputs("</time></header>", stdout);
        }
        if (stream->item[i].title[0]) { fputs("<h3>", stdout); html(stream->item[i].title); fputs("</h3>", stdout); }
        fputs("<p>", stdout); html(stream->item[i].body); fputs("</p></article>", stdout);
        if (stream->item[i].reaction_count) { fputs("<small class=\"community-reactions\">", stdout); printf("%d accepted reactions", stream->item[i].reaction_count); fputs("</small>", stdout); }
    }
    fputs("</div>", stdout);
}

static void site_surface_emit_board(const SiteMaterialStream *stream) {
    int i;
    fputs("<ol class=\"application-board\">", stdout);
    for (i = 0; i < stream->count; i++) {
        int linked = stream->item[i].href[0] == '/';
        fputs("<li>", stdout);
        if (linked) { fputs("<a href=\"", stdout); html(stream->item[i].href); fputs("\">", stdout); }
        else fputs("<div>", stdout);
        fputs("<small>", stdout);
        html(stream->item[i].byline); fputs("</small><strong>", stdout); html(stream->item[i].title);
        fputs("</strong>", stdout);
        if (stream->item[i].body[0]) { fputs("<span>", stdout); html(stream->item[i].body); fputs("</span>", stdout); }
        fputs(linked ? "</a></li>" : "</div></li>", stdout);
    }
    fputs("</ol>", stdout);
}

static void site_surface_emit_media(const BfSiteApplicationLowerer *lowerer, const SiteMaterialStream *stream) {
    const SiteMaterialItem *item = &stream->item[0];
    if (!listen_safe_asset(item->asset)) { fputs("<p class=\"site-program-empty\">No safe public media representation is published for this material.</p>", stdout); return; }
    if (lowerer->layout == BF_SITE_LAYOUT_AUDIO) {
        fputs("<audio controls preload=\"metadata\" src=\"", stdout); html(item->asset); fputs("\"></audio>", stdout);
    } else {
        fputs("<video controls preload=\"metadata\" src=\"", stdout); html(item->asset); fputs("\"", stdout);
        if (listen_safe_asset(item->poster)) { fputs(" poster=\"", stdout); html(item->poster); fputs("\"", stdout); }
        fputs("></video>", stdout);
    }
}

static void site_surface_emit_transcript(const SiteMaterialStream *stream) {
    if (!listen_safe_asset(stream->item[0].transcript)) return;
    fputs("<p><a href=\"", stdout); html(stream->item[0].transcript); fputs("\">Transcript</a></p>", stdout);
}

static void site_surface_render_stream(const BfSiteApplicationLowerer *lowerer, const SiteMaterialStream *stream) {
    size_t i;
    /* A part with nothing in it still says so in the document; the sheet folds
     * the repeats away so a reader meets the statement once, not per part. */
    if (!lowerer || !stream || !stream->count) { fputs("<p class=\"site-program-empty\">", stdout); html(lowerer ? lowerer->empty_character : "No native lowerer has been published for this application."); fputs("</p>", stdout); return; }
    fputs("<article class=\"application-data\">", stdout);
    for (i = 0; i < lowerer->op_count; i++) switch (lowerer->ops[i]) {
    case BF_SITE_OP_HEADER: site_surface_emit_header(stream); break;
    case BF_SITE_OP_LEAD: site_surface_emit_lead(stream); break;
    case BF_SITE_OP_STREAM: site_surface_emit_stream(stream); break;
    case BF_SITE_OP_THREAD: site_surface_emit_thread(stream); break;
    case BF_SITE_OP_BOARD: site_surface_emit_board(stream); break;
    case BF_SITE_OP_MEDIA: site_surface_emit_media(lowerer, stream); break;
    case BF_SITE_OP_TRANSCRIPT: site_surface_emit_transcript(stream); break;
    }
    fputs("</article>", stdout);
}

static int site_material_binding_selector(const BfSiteApplicationLowerer *lowerer,
                                          const char *source_collection, const char *material_query,
                                          char *out, size_t out_sz) {
    if (!lowerer || !out || out_sz < 2) return 0;
    out[0] = '\0';
    if (!strcmp(lowerer->query_key, "none")) return 1;
    if (!strcmp(lowerer->query_key, "source_collection")) {
        snprintf(out, out_sz, "%s", source_collection ? source_collection : "");
        return out[0] != '\0';
    }
    return material_query && studio_json_field(material_query, lowerer->query_key, out, out_sz) && out[0];
}

/* The CMS application kind is an owner coordinate, not presentation chrome.
 * CUE compiles a stable presentation hook with the structural lowerer, so a
 * second river-shaped application need not add its internal name to CSS. */
static const char *site_application_css_class(const BfSiteApplicationLowerer *lowerer) {
    return lowerer && lowerer->presentation_class && *lowerer->presentation_class
        ? lowerer->presentation_class : "state";
}

static void site_program_emit_binding(sqlite3 *db, const char *kind, const char *source_collection,
                                      const char *material_query, int max_items, const char *boundary,
                                      const char *klass) {
    const BfSiteApplicationLowerer *lowerer = site_application_lowerer(kind);
    SiteMaterialStream stream;
    char selector[256];
    if (boundary && *boundary) { fputs("<div id=\"", stdout); html(boundary); fputs("\"", stdout); } else fputs("<div", stdout);
    fputs(" class=\"site-program-application application-", stdout);
    fputs(site_application_css_class(lowerer), stdout);
    fputs("\" data-application-kind=\"", stdout); html(kind); fputs("\">", stdout);
    if (!site_material_binding_selector(lowerer, source_collection, material_query, selector, sizeof(selector)))
        memset(&stream, 0, sizeof(stream));
    else
        site_material_resolve(db, lowerer, selector, max_items, &stream, klass);
    site_surface_render_stream(lowerer, &stream);
    fputs("</div>", stdout);
}

/* SiteProgram is the executable composition join. JSON remains the bounded
 * machine projection. HTML is server-lowered from published slots/bindings;
 * the browser never receives a site plan and decides what an application is. */
static int serve_site_program_uncached(const char *method, const char *target, const char *query,
                                       const char *host, const char *accept, const char *bearer, const char *auth_db) {
    char site[128]; sqlite3 *db = NULL; sqlite3_stmt *st = NULL; const char *klass;
    int apex = target && !strcmp(target, "/");
    if (strcmp(target, "/_surface/site-program") && !apex) return 0;
    if (strcmp(method, "GET") || presentation_has_forbidden_coordinate(query)) {
        protocol_reply(400, "application/json", "{\"error\":\"site_program_query_invalid\"}\n"); return 1;
    }
    if (!(db = cms_open_ro()) || !cms_type_registered(db, "site_program") ||
        !cms_type_registered(db, "site_presentation")) {
        if (db) sqlite3_close(db);
        /* A site apex is an optional CMS projection. Its absence may not
         * consume the AddressPlane's ordinary territory home. The explicit
         * machine endpoint, by contrast, reports the unavailable owner. */
        if (apex) return 0;
        protocol_reply(503,"application/json","{\"error\":\"site_program_store_unavailable\"}\n"); return 1;
    }
    if (!apex && (!query_value(query, "site", site, sizeof(site)) || !studio_safe_slug_arg(site))) {
        sqlite3_close(db); protocol_reply(400, "application/json", "{\"error\":\"site_program_query_invalid\"}\n"); return 1;
    }
    if (apex && (!host || !*host)) { sqlite3_close(db); return 0; }
    /* A browser on a local port sends "host:port"; a presentation names the
     * host alone.  The port is how the request arrived, not which site it is. */
    char apex_host[256];
    snprintf(apex_host, sizeof(apex_host), "%s", host ? host : "");
    { char *colon = strrchr(apex_host, ':'); if (colon && !strchr(colon, ']')) *colon = '\0'; }
    host = apex_host;
    if (sqlite3_prepare_v2(db,
        "SELECT p.program_id,p.site_id,s.subject_address,s.territory,s.profile,p.experience_programs,"
        "p.application_bindings,p.renderer_preferences,p.source_collections,p.capability_programs,p.actor_presentations,"
        "COALESCE(s.host,'') "
        "FROM site_program p JOIN site_presentation s ON s.site_id=p.site_id "
        "WHERE ((?2=0 AND p.site_id=?1) OR (?2=1 AND s.host=?3)) AND p.status='published' AND s.status='published' ORDER BY p.id DESC LIMIT 2",
        -1,&st,NULL)!=SQLITE_OK) { sqlite3_close(db); protocol_reply(500,"application/json","{\"error\":\"site_program_query_failed\"}\n"); return 1; }
    sqlite3_bind_text(st,1,site,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(st,2,apex);
    sqlite3_bind_text(st,3,host ? host : "",-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)!=SQLITE_ROW) { sqlite3_finalize(st); sqlite3_close(db); if (apex) return 0; protocol_reply(404,"application/json","{\"error\":\"site_program_not_found\"}\n"); return 1; }
    if (apex) snprintf(site,sizeof(site),"%s", (const char *)sqlite3_column_text(st,1));
    klass=native_presentation_class(bearer,auth_db);
    if (wants_html(accept)) {
        const char *subject = (const char *)sqlite3_column_text(st,2);
        const char *territory = (const char *)sqlite3_column_text(st,3);
        const char *profile = (const char *)sqlite3_column_text(st,4);
        /* A subject's page is named by the address people were given, not by
         * the store's identifier for it: "person-nick" is how this estate
         * files the site; nick.aurekai.life is where someone is standing.
         * The identifier stays available as data on the composition. */
        const char *published_host = (const char *)sqlite3_column_text(st, 11);
        const char *cache_control = (!bearer || !*bearer) && !strcmp(klass, "anonymous")
            ? "public, max-age=60, stale-while-revalidate=300"
            : "private, no-store";
        sqlite3_stmt *slots = NULL;
        size_t lowerer_index, prior_lowerer;
        if (!cms_type_registered(db, "semantic_slot") || !cms_type_registered(db, "application_binding") ||
            sqlite3_prepare_v2(db,
              "SELECT s.slot_name,s.extent,s.max_items,COALESCE(s.htmx_boundary,''),b.application_kind,COALESCE(b.source_collection,''),b.material_query "
              "FROM semantic_slot s JOIN application_binding b ON b.slot_id=s.slot_id "
              "WHERE s.site_id=?1 AND s.status='published' AND b.site_id=?1 AND b.status='published' "
              "ORDER BY s.ordinal,b.id", -1, &slots, NULL) != SQLITE_OK) {
            sqlite3_finalize(st); sqlite3_close(db); protocol_reply(503,"text/html; charset=utf-8","<!doctype html><title>Site program unavailable</title><p>The authored composition store is unavailable.</p>"); return 1;
        }
        sqlite3_bind_text(slots,1,site,-1,SQLITE_TRANSIENT);
        printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: %s\r\nVary: Host, Accept, Cookie, Authorization\r\nX-Bonfyre-Site-Program: native\r\nConnection: close\r\n\r\n<!doctype html><html lang=\"en\" class=\"site-program profile-", cache_control); html(profile); fputs(" territory-",stdout); html(territory); fputs("\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>",stdout);
        char own_name[200];
        int own = site_program_own_name(db, profile, territory, own_name, sizeof(own_name));
        if (own) { html(own_name); fputs("</title>",stdout); }
        else { html(published_host && *published_host ? published_host : site); fputs(" · Aurekai</title>",stdout); }
        /* Independent customer pages keep their own visual identity. */
        if (!own) fputs(BF_AUREKAI_FAVICON_LINK, stdout);
        /* A subject's own page wears the estate's dress before any
         * application's.  Without these a member's address arrived unstyled
         * next to every other page in the same territory, which reads as a
         * different system rather than the same one showing a different
         * subject.  Application sheets still follow and win where they
         * disagree, because each one styles material only it knows. */
        /* An operator's own site wears one sheet drawn from its own tokens
         * and nothing of the estate's: the estate's dress under a site's
         * palette gave a dark page with a title in dark ink. */
        site_program_own_render = own;
        if (own) fputs("<link rel=\"stylesheet\" href=\"/assets/own-site.css?v=6\"><link rel=\"mcp\" href=\"/.well-known/mcp/server-card.json\">"
                       "<script src=\"/assets/own-site-agent.js\" defer></script>", stdout);
        else {
        fputs("<link rel=\"stylesheet\" href=\"/assets/tailwind.css\">"
              "<link rel=\"stylesheet\" href=\"/assets/world.css\">", stdout);
        for (lowerer_index = 0; lowerer_index < BF_SITE_APPLICATION_LOWERER_COUNT; lowerer_index++) {
            int seen_asset = 0;
            if (!site_program_uses_application(db, site,
                                               bf_site_application_lowerers[lowerer_index].application_kind))
                continue;
            for (prior_lowerer = 0; prior_lowerer < lowerer_index; prior_lowerer++)
                if (bf_site_application_lowerers[lowerer_index].style_asset && bf_site_application_lowerers[prior_lowerer].style_asset && !strcmp(bf_site_application_lowerers[lowerer_index].style_asset, bf_site_application_lowerers[prior_lowerer].style_asset)) seen_asset = 1;
            if (bf_site_application_lowerers[lowerer_index].style_asset && !seen_asset) {
                fputs("<link rel=\"stylesheet\" href=\"", stdout);
                html(bf_site_application_lowerers[lowerer_index].style_asset);
                fputs("\">", stdout);
            }
        }
        fputs("<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&amp;family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&amp;family=Instrument+Serif:ital@0;1&amp;display=swap\">"
              "<link rel=\"stylesheet\" href=\"/assets/aurekai-site-program.css?v=2\">", stdout);
        /* A member's world gets the same organs as everything else. */
        fputs("<script src=\"/assets/client-organs.js\" defer></script>", stdout);
        }
        site_program_emit_profile_tokens(db, profile);
        {
            const char *masthead = published_host && *published_host ? published_host : site;
            fputs("</head><body class=\"site-program-body\"><main class=\"site-composition\" data-site=\"",stdout); html(site); fputs("\" data-subject=\"",stdout); html(subject); fputs("\" data-presentation=\"",stdout); html(klass); fputs("\" data-bf-profile=\"",stdout); html(profile); fputs("\"",stdout); bf_site_presentation_attributes(stdout,db,profile); if (own) { fputs(">",stdout); bf_site_bar(own_name); fputs("<header class=\"site-program-masthead\"><p>",stdout); html(masthead); fputs("</p><h1>",stdout); html(own_name); fputs("</h1></header>",stdout); }
            else { fputs("><header class=\"site-program-masthead\"><p>AUREKAI / ",stdout); html(territory); fputs("</p><h1>",stdout); html(masthead); fputs("</h1><span class=\"site-program-profile\">",stdout); html(profile); fputs("</span></header>",stdout); }
        }
        site_program_emit_navigation(db, site);
        if (own) bf_site_pages_nav(db, site);
        int reading_seen=0,more_reading_open=0;
        while (sqlite3_step(slots) == SQLITE_ROW) {
            const char *slot = (const char *)sqlite3_column_text(slots,0);
            int reading = slot && !strncmp(slot,"reading-",8);
            if (more_reading_open && !reading) { fputs("</div></details>",stdout);more_reading_open=0; }
            if (reading && ++reading_seen > 10 && !more_reading_open) {
                fputs("<details class=\"site-program-extra-reading\"><summary>",stdout);
                page_glyph("discover");fputs("<span>Explore more source families</span></summary><div class=\"site-program-extra-grid\">",stdout);
                more_reading_open=1;
            }
            const char *extent = (const char *)sqlite3_column_text(slots,1);
            int max_items = sqlite3_column_int(slots,2);
            const char *boundary = (const char *)sqlite3_column_text(slots,3);
            const char *kind = (const char *)sqlite3_column_text(slots,4);
            const char *collection = (const char *)sqlite3_column_text(slots,5);
            const char *material_query = (const char *)sqlite3_column_text(slots,6);
            /* The list of a member's page layouts is theirs to work from.  A
             * visitor is shown what has been posted; a list of arrangements
             * with nothing in them is not material, so it is not sent. */
            if (slot && !strcmp(slot, "surfaces") && klass && !strcmp(klass, "anonymous")) continue;
            fputs("<section class=\"site-program-slot slot-",stdout); html(slot); fputs(" extent-",stdout); html(extent); fputs("\" data-application=\"",stdout); html(kind); fputs("\" data-bf-source=\"application_binding\" data-bf-slot=\"",stdout); html(slot); fputs("\">",stdout);
            site_program_emit_binding(db, kind, collection, material_query, max_items, boundary, klass);
            fputs("</section>",stdout);
        }
        if (more_reading_open) fputs("</div></details>",stdout);
        sqlite3_finalize(slots);
        /* What a subject's world is, and what standing on it does not mean. The
         * page carried this in its machine projection and never said it to a
         * reader; a boundary only the machine can see is not a boundary. */
        /* That boundary is the estate speaking about someone else's address.
         * An operator's own site is published by whoever stands at it. */
        if (!own)
        fputs("<footer class=\"region\"><p class=\"src\">Aurekai put this page together. "
              "Nothing on it is published, chosen or endorsed by whoever this address belongs to, "
              "and an empty part means nothing has been added yet, not that nothing exists.</p></footer>",
              stdout);
        if (own) bf_site_home_extras(db, site, own_name);
        site_program_own_render = 0;
        fputs("</main></body></html>",stdout); sqlite3_finalize(st); sqlite3_close(db); return 1;
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{\"schema_version\":\"bonfyre.site_program.v1\",\"site_id\":"); json(site);
    fputs(",\"program_id\":",stdout); json((const char*)sqlite3_column_text(st,0));
    fputs(",\"subject_address\":",stdout); json((const char*)sqlite3_column_text(st,2));
    fputs(",\"territory\":",stdout); json((const char*)sqlite3_column_text(st,3));
    fputs(",\"ghost_profile\":",stdout); json((const char*)sqlite3_column_text(st,4));
    fputs(",\"experience_programs\":",stdout); fputs((const char*)sqlite3_column_text(st,5),stdout);
    fputs(",\"application_bindings\":",stdout); fputs((const char*)sqlite3_column_text(st,6),stdout);
    fputs(",\"renderer_preferences\":",stdout); fputs((const char*)sqlite3_column_text(st,7),stdout);
    fputs(",\"source_collections\":",stdout); fputs((const char*)sqlite3_column_text(st,8),stdout);
    fputs(",\"capability_programs\":",stdout); fputs((const char*)sqlite3_column_text(st,9),stdout);
    fputs(",\"actor_presentations\":",stdout); fputs((const char*)sqlite3_column_text(st,10),stdout);
    fputs(",\"presentation_class\":",stdout); json(klass);
    fputs(",\"cannot_infer\":[\"program -> material existence\",\"renderer preference -> authority or effect\",\"carrier candidate -> provider admission or payment\"]}\n",stdout);
    sqlite3_finalize(st); sqlite3_close(db); return 1;
}

typedef struct {
    const char *method, *target, *query, *host, *accept, *bearer, *auth_db;
} BfSiteProgramEmit;
static int emit_site_program_cached_body(void *opaque) {
    BfSiteProgramEmit *request = opaque;
    return serve_site_program_uncached(request->method, request->target, request->query,
                                       request->host, request->accept, request->bearer,
                                       request->auth_db);
}

/* The SiteProgram cache carries the same owner coordinates emitted into the
 * AddressPlane. It does not copy those records: it merely lets the declared
 * SurfaceIR lowerer retire this one derived response when a matching FactDelta
 * reaches its handoff inbox. */
static size_t site_program_cache_dependencies(const char *site, char storage[][640],
                                              const char **out, size_t cap) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    size_t count = 0;
    const char *sql =
        "SELECT 'cms:site_program:'||p.program_id FROM site_program p "
        "WHERE p.site_id=?1 AND p.status='published' "
        "UNION SELECT 'cms:site_presentation:'||s.site_id FROM site_presentation s "
        "WHERE s.site_id=?1 AND s.status='published' "
        "UNION SELECT 'cms:semantic_slot:'||slot_id FROM semantic_slot "
        "WHERE site_id=?1 AND status='published' "
        "UNION SELECT 'cms:application_binding:'||binding_id FROM application_binding "
        "WHERE site_id=?1 AND status='published' "
        "UNION SELECT 'cms:source_collection:'||source_collection FROM application_binding "
        "WHERE site_id=?1 AND status='published' AND source_collection IS NOT NULL AND source_collection!='' "
        "UNION SELECT 'cms:presentation_template:'||REPLACE(profile,'ghost:','') FROM site_presentation "
        "WHERE site_id=?1 AND status='published'";
    if (!site || !*site || !out || !cap) return 0;
    snprintf(storage[count], sizeof(storage[count]), "%s", "surface:site-program");
    out[count] = storage[count]; count++;
    snprintf(storage[count], sizeof(storage[count]), "%s", "cms:published-material");
    out[count] = storage[count]; count++;
    if (!(db = cms_open_ro()) || sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        if (st) sqlite3_finalize(st);
        if (db) sqlite3_close(db);
        return count;
    }
    sqlite3_bind_text(st, 1, site, -1, SQLITE_TRANSIENT);
    while (count < cap && sqlite3_step(st) == SQLITE_ROW) {
        const char *dependency = (const char *)sqlite3_column_text(st, 0);
        if (!dependency || !*dependency || strlen(dependency) >= sizeof(storage[count])) continue;
        snprintf(storage[count], sizeof(storage[count]), "%s", dependency);
        out[count] = storage[count];
        count++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return count;
}
static int site_program_cache_site_for_host(const char *host, char *site, size_t site_sz) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (!host || !*host || !site || !site_sz || !(db = cms_open_ro()) ||
        sqlite3_prepare_v2(db,
          "SELECT site_id FROM site_presentation WHERE host=?1 AND status='published' ORDER BY id DESC LIMIT 1",
          -1, &st, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(st, 1, host, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) {
        snprintf(site, site_sz, "%s", (const char *)sqlite3_column_text(st, 0));
        found = studio_safe_slug_arg(site);
    }
out:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    return found;
}

/* An anonymous SiteProgram is a public closure projection. It can reuse its
 * compiled response only while its source frontier still matches. A bearer,
 * even one which currently resolves to the anonymous presentation cell, never
 * enters this cache: we do not turn an authentication coordinate into a public
 * cache key. */
static int serve_site_program(const char *method, const char *target, const char *query,
                              const char *host, const char *accept, const char *bearer, const char *auth_db) {
    BfSiteProgramEmit request = {method, target, query, host, accept, bearer, auth_db};
    char key[768], fingerprint[65], site[128] = {0};
    char dependency_storage[32][640];
    const char *dependencies[32];
    size_t dependency_count;
    if (strcmp(method, "GET") || !wants_html(accept) || (bearer && *bearer) ||
        !public_projection_cache_db())
        return emit_site_program_cached_body(&request);
    if (target && !strcmp(target, "/_surface/site-program")) {
        if (!query_value(query, "site", site, sizeof(site)) || !studio_safe_slug_arg(site))
            return emit_site_program_cached_body(&request);
    } else if (target && !strcmp(target, "/") && host && *host) {
        if (!site_program_cache_site_for_host(host, site, sizeof(site)))
            return emit_site_program_cached_body(&request);
    } else return emit_site_program_cached_body(&request);
    if (snprintf(key, sizeof(key), "public-site-program|%s|%s|%s", site,
                 target ? target : "", query ? query : "") >= (int)sizeof(key))
        return emit_site_program_cached_body(&request);
    public_projection_fingerprint(fingerprint, host, target, query);
    dependency_count = site_program_cache_dependencies(site, dependency_storage, dependencies,
                                                        sizeof(dependencies) / sizeof(dependencies[0]));
    if (dependency_count < sizeof(dependencies) / sizeof(dependencies[0])) {
        snprintf(dependency_storage[dependency_count], sizeof(dependency_storage[dependency_count]), "%s",
                 target && !strcmp(target, "/") ? "address:apex" : "address:site-program");
        dependencies[dependency_count] = dependency_storage[dependency_count];
        dependency_count++;
    }
    return public_projection_emit(emit_site_program_cached_body, &request, key, fingerprint,
                                  dependencies, dependency_count);
}

static int serve_world_fragments(const char *method, const char *target, const char *query,
                                 const char *bearer, const char *auth_db) {
    char world[128];
    WorldFragmentPlan fragments[WORLD_FRAGMENT_PLAN_MAX];
    sqlite3 *db = NULL;
    const char *klass;
    int count, i;
    if (strcmp(target, "/_surface/world-fragments")) return 0;
    if (strcmp(method, "GET")) {
        protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n");
        return 1;
    }
    if (presentation_has_forbidden_coordinate(query)) {
        protocol_reply(400, "application/json", "{\"error\":\"forbidden_presentation_coordinate\",\"limit\":\"World fragments resolve presentation class from native closure; actor, audience, institution, role and authority are not URL coordinates\"}\n");
        return 1;
    }
    if (!query_value(query, "world", world, sizeof(world)) || !studio_safe_slug_arg(world)) {
        protocol_reply(400, "application/json", "{\"error\":\"world_required\"}\n");
        return 1;
    }
    db = cms_open_ro();
    if (!db) {
        protocol_reply(503, "application/json", "{\"error\":\"world_store_unavailable\"}\n");
        return 1;
    }
    count = load_world_fragment_plans(db, world, fragments, WORLD_FRAGMENT_PLAN_MAX);
    sqlite3_close(db);
    if (count < 0) {
        protocol_reply(500, "application/json", "{\"error\":\"invalid_world_fragment\"}\n");
        return 1;
    }
    if (count == 0) {
        protocol_reply(404, "application/json", "{\"error\":\"world_fragments_not_authored\"}\n");
        return 1;
    }
    klass = native_presentation_class(bearer, auth_db);
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n");
    fputs("{\"schema_version\":\"bonfyre.world_fragments.v1\",\"world\":", stdout); json(world);
    fputs(",\"presentation_class\":", stdout); json(klass);
    fputs(",\"interaction_density\":", stdout); json(presentation_density(klass));
    fputs(",\"ordered_fragments\":[", stdout);
    for (i = 0; i < count; i++) {
        WorldFragmentPlan *f = &fragments[i];
        if (i) putchar(',');
        fputs("{\"fragment_id\":", stdout); json(f->fragment_id);
        fputs(",\"subject_address\":", stdout); json(f->subject_address);
        fputs(",\"surface_grammar\":", stdout); json(f->surface_grammar);
        fputs(",\"renderer_family\":", stdout); json(f->renderer_family);
        fputs(",\"renderer_variant\":", stdout); json(f->renderer_variant);
        fputs(",\"extent\":", stdout); json(f->extent);
        fputs(",\"template\":", stdout); json(f->template_name);
        fputs(",\"source_projection\":", stdout); json(f->source_projection);
        fputs(",\"media_posture\":", stdout); json(f->media_posture);
        fputs(",\"temporal_posture\":", stdout); json(f->temporal_posture);
        fputs(",\"continuity_group\":", stdout); json(f->continuity_group);
        fputs(",\"visible_sections\":", stdout); json(f->visible_sections);
        fputs(",\"action_aperture\":", stdout); json(f->action_aperture);
        fputs(",\"provenance\":", stdout); json(f->provenance);
        fputs(",\"cannot_infer\":", stdout); json(f->cannot_infer);
        fputs(",\"interaction_density\":", stdout); json(presentation_density(klass));
        fputs(",\"truth_level\":\"presentation_plan\"}", stdout);
    }
    printf("],\"fragment_count\":%d,", count);
    fputs("\"cannot_infer\":[\"fragment order -> subject identity\",\"presentation class -> authority\",\"source projection -> permission or admitted effect\"]}\n", stdout);
    return 1;
}

/* The prior World reader parsed retained bodies in a browser script.  That
 * duplicated the census contract at every page load and made the native
 * World-fragment plan decorative.  This is deliberately small: it interprets
 * JSON bodies through the census's authored dot paths and emits a bounded,
 * escaped HTML fragment.  XML/RSS stays with its existing reader until it has
 * an equally precise native parser; a source is never approximated as JSON. */
static int world_body_for_twin(const char *twin, char *body, size_t cap) {
    const char *root = getenv("BONFYRE_FOREIGN_ROOT");
    char receipt_path[PATH_MAX], bodies[PATH_MAX], body_path[PATH_MAX] = {0};
    char receipt[65536]; FILE *f; long size; size_t n;
    if (!root || !*root) root = "/var/lib/bonfyre/foreign";
    if (!twin || !foreign_safe_file(twin) ||
        snprintf(receipt_path, sizeof(receipt_path), "%s/receipts/%s.json", root, twin) >= (int)sizeof(receipt_path) ||
        snprintf(bodies, sizeof(bodies), "%s/bodies/", root) >= (int)sizeof(bodies)) return 0;
    if (!(f = bf_receipt_fopen(receipt_path, "rb"))) return 0;
    if (fseek(f, 0, SEEK_END) != 0 || (size = ftell(f)) < 0 ||
        fseek(f, size > (long)sizeof(receipt) - 1 ? size - ((long)sizeof(receipt) - 1) : 0, SEEK_SET) != 0) {
        fclose(f); return 0;
    }
    n = fread(receipt, 1, sizeof(receipt) - 1, f); fclose(f); receipt[n] = '\0';
    { char *last = receipt, *p = receipt; for (;;) { char *nl = strchr(p, '\n'); if (!nl) break; *nl = '\0'; if (*p) last = p; p = nl + 1; } if (*p) last = p; if (last != receipt) memmove(receipt, last, strlen(last) + 1); }
    if (!studio_json_field(receipt, "body_path", body_path, sizeof(body_path)) ||
        strstr(body_path, "..")) return 0;
    {
        /* The receipt records where the body was written, which is where that
         * reading happened -- not where this estate keeps its bodies now.  An
         * estate that can only read its own evidence from the absolute path it
         * was first written to is an estate that cannot be moved, restored or
         * carried onto another machine, which is exactly what this one was
         * forced to do.  The body belongs to the twin: it is looked for under
         * the foreign root this estate was told to use, by the name the
         * receipt gave it, and never outside that directory. */
        const char *slash = strrchr(body_path, '/');
        const char *name = slash ? slash + 1 : body_path;
        char mine[PATH_MAX];
        if (!*name || !foreign_safe_file(name) ||
            snprintf(mine, sizeof(mine), "%s%s", bodies, name) >= (int)sizeof(mine)) return 0;
        snprintf(body_path, sizeof(body_path), "%s", mine);
    }
    /* A retained body may have been compressed where it lies: the estate keeps
     * every body it observed, and keeping them costs less as gzip.  The reader
     * takes whichever exists, so compressing evidence never loses it. */
    if ((f = fopen(body_path, "rb"))) {
        n = fread(body, 1, cap - 1, f); fclose(f); body[n] = '\0'; return n > 0;
    }
    {
        char packed[PATH_MAX]; gzFile z;
        int read;
        if (snprintf(packed, sizeof(packed), "%s.gz", body_path) >= (int)sizeof(packed)) return 0;
        if (!(z = gzopen(packed, "rb"))) return 0;
        read = gzread(z, body, (unsigned)(cap - 1));
        gzclose(z);
        if (read <= 0) return 0;
        body[read] = '\0';
        return 1;
    }
}

static int world_value(const bf_json_doc_t *doc, const bf_json_node_t *row,
                       const char *path, char *out, size_t cap) {
    const bf_json_node_t *n; const char *s; int len;
    if (!doc || !row || !path || !*path || !out || cap < 2) return 0;
    n = bf_json_dotpath(doc, path);
    /* dotpath starts from document root. Fields in the census contract are
     * row-relative, so temporarily support the ordinary first segment here. */
    if (!n || n->parent != row->parent) n = bf_json_obj_get(doc, row, path);
    if (!n) return 0;
    s = bf_json_get_str(n, &len);
    if (!s || len < 1) return 0;
    if ((size_t)len >= cap) len = (int)cap - 1;
    memcpy(out, s, (size_t)len); out[len] = '\0'; return 1;
}

/* A World character is a visual grammar, not a generic-card skin.  Both JSON
 * and RSS material enter this one native emitter so an item changes shape with
 * the World it joins.  `source` is provenance for the reader, never a source
 * catalogue label or an identity claim about the estate. */
static const BfWorldMaterialRenderer *world_material_renderer(const char *character) {
    size_t i;
    for (i = 0; i < BF_WORLD_MATERIAL_RENDERER_COUNT; i++)
        if (character && !strcmp(character, bf_world_material_renderers[i].character))
            return &bf_world_material_renderers[i];
    return &bf_world_material_renderers[BF_WORLD_MATERIAL_RENDERER_COUNT - 1];
}

static void world_material_open(const BfWorldMaterialRenderer *renderer, const char *says, const char *source) {
    fputs("<section class=\"world-material\"><h2>", stdout);
    html(says && *says ? says : source); fputs("</h2>", stdout);
    if (renderer->item_variant == BF_WORLD_MATERIAL_FLYER) fputs("<div class=\"rule\"></div>", stdout);
    fputs("<div class=\"", stdout); html(renderer->container_class); fputs("\">", stdout);
}

static void world_material_item(const BfWorldMaterialRenderer *renderer, const char *title, const char *meta,
                                const char *href, const char *source, int ordinal) {
    int linked = public_http_url(href);
    const char *when = meta && *meta ? meta : "observed";
    if (!title || !*title) return;
    switch (renderer->item_variant) {
    case BF_WORLD_MATERIAL_FLYER:
        fputs("<a class=\"fl noimg\"", stdout); if (linked) { fputs(" rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(href); fputs("\"", stdout); } fputs("><span class=\"stamp\">", stdout); html(when); fputs("</span><div class=\"t\"><b>", stdout); html(title); fputs("</b><span>", stdout); html(source); fputs("</span></div></a>", stdout);
        break;
    case BF_WORLD_MATERIAL_PIN:
        fputs("<a class=\"pin noimg\"", stdout); if (linked) { fputs(" rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(href); fputs("\"", stdout); } fputs("><div class=\"cap\"><b>", stdout); html(title); fputs("</b><span>", stdout); html(source); if (meta && *meta) { fputs(" &middot; ", stdout); html(meta); } fputs("</span></div></a>", stdout);
        break;
    case BF_WORLD_MATERIAL_CASE:
        fputs("<a class=\"case\"", stdout); if (linked) { fputs(" rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(href); fputs("\"", stdout); } fputs("><div class=\"n\">", stdout); printf("%03d", ordinal); fputs("</div><div class=\"t\"><b>", stdout); html(title); fputs("</b><i>", stdout); html(source); fputs("</i></div><div class=\"w\">", stdout); html(when); fputs("</div></a>", stdout);
        break;
    case BF_WORLD_MATERIAL_TICK:
        fputs("<a class=\"t\"", stdout); if (linked) { fputs(" rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(href); fputs("\"", stdout); } fputs("><div class=\"nm\">", stdout); html(title); fputs("<em>", stdout); html(source); fputs("</em></div><div class=\"v\">", stdout); html(when); fputs("</div><div class=\"c\"></div></a>", stdout);
        break;
    case BF_WORLD_MATERIAL_READING:
        fputs("<a class=\"r\"", stdout); if (linked) { fputs(" rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(href); fputs("\"", stdout); } fputs("><b>", stdout); html(title); fputs("</b><span>", stdout); html(when); fputs("</span></a>", stdout);
        break;
    case BF_WORLD_MATERIAL_ARTICLE:
        fputs("<a class=\"a\"", stdout); if (linked) { fputs(" rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(href); fputs("\"", stdout); } fputs("><b>", stdout); html(title); fputs("</b><span class=\"au\">", stdout); html(source); fputs("</span>", stdout); if (meta && *meta) { fputs("<span class=\"id\">", stdout); html(meta); fputs("</span>", stdout); } fputs("</a>", stdout);
        break;
    case BF_WORLD_MATERIAL_ROW:
        fputs("<a class=\"q\"", stdout); if (linked) { fputs(" rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(href); fputs("\"", stdout); } fputs("><div class=\"mag\">&bull;</div><div class=\"place\">", stdout); html(title); fputs("</div><div class=\"when\">", stdout); html(when); fputs("</div></a>", stdout);
        break;
    }
}

static void world_material_close(void) { fputs("</div></section>", stdout); }

static int world_material_rows(const char *family, const char *source, const char *says,
                               const char *body, const char *extract, const char *title_field,
                               const char *meta_field, const char *link_field) {
    bf_json_doc_t *doc; const bf_json_node_t *items, *item; char err[160]; int shown = 0;
    const BfWorldMaterialRenderer *renderer = world_material_renderer(family);
    if (!body || !extract || !*extract || !strncmp(extract, "rss:", 4)) return 0;
    if (!(doc = bf_json_parse_str(body, err, sizeof(err)))) return 0;
    items = bf_json_dotpath(doc, extract);
    if (!items || items->type != BF_JSON_ARRAY) { bf_json_free(doc); return 0; }
    world_material_open(renderer, says, source);
    for (item = bf_json_child_first(doc, items); item && shown < 18; item = bf_json_child_next(doc, item)) {
        char title[768] = {0}, meta[256] = {0}, href[1024] = {0};
        if (item->type != BF_JSON_OBJECT || !world_value(doc, item, title_field, title, sizeof(title))) continue;
        world_value(doc, item, meta_field, meta, sizeof(meta)); world_value(doc, item, link_field, href, sizeof(href));
        world_material_item(renderer, title, meta, href, source, shown + 1); shown++;
    }
    world_material_close(); bf_json_free(doc); return shown;
}

/* RSS is read as an observed publication carrier, never guessed as JSON.
 * This deliberately accepts only ordinary item/title/link/pubDate fields. A
 * missing author, image, licence, or semantic category remains missing rather
 * than being manufactured from a feed name or its surrounding markup. */
static const char *rss_item_open(const char *at) {
    const char *p = at;
    while ((p = strstr(p, "<item")) != NULL) {
        char next = p[5];
        if (next == '>' || isspace((unsigned char)next)) {
            p = strchr(p, '>'); return p ? p + 1 : NULL;
        }
        p += 5;
    }
    return NULL;
}

static int rss_tag_text(const char *start, const char *end, const char *tag, char *out, size_t cap) {
    char open[96], close[96]; const char *p, *q; size_t n = 0; int in_tag = 0, space = 1;
    if (!start || !end || !tag || !out || cap < 2) return 0;
    snprintf(open, sizeof(open), "<%s", tag); snprintf(close, sizeof(close), "</%s>", tag);
    for (p = strstr(start, open); p && p < end; p = strstr(p + 1, open)) {
        char after;
        after = p[strlen(open)];
        if (after == '>' || isspace((unsigned char)after)) break;
    }
    if (!p || p >= end || !(p = strchr(p, '>')) || p >= end) return 0;
    p++;
    q = strstr(p, close); if (!q || q > end) return 0;
    if ((size_t)(q - p) >= 12 && !strncmp(p, "<![CDATA[", 9) && !strncmp(q - 3, "]]>", 3)) { p += 9; q -= 3; }
    for (; p < q && n + 1 < cap; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '<') { in_tag = 1; continue; }
        if (c == '>' && in_tag) { in_tag = 0; continue; }
        if (in_tag) continue;
        if (isspace(c)) { if (!space) out[n++] = ' '; space = 1; }
        else { out[n++] = (char)c; space = 0; }
    }
    while (n && isspace((unsigned char)out[n - 1])) n--;
    out[n] = '\0'; return n > 0;
}

static int world_material_rss(const char *family, const char *source, const char *says, const char *body) {
    const BfWorldMaterialRenderer *renderer = world_material_renderer(family);
    const char *cursor = body, *item_end; int shown = 0;
    if (!body || !strstr(body, "<item")) return 0;
    world_material_open(renderer, says, source);
    while (shown < 18 && (cursor = rss_item_open(cursor)) != NULL && (item_end = strstr(cursor, "</item>")) != NULL) {
        char title[768] = {0}, href[1024] = {0}, when[256] = {0};
        if (rss_tag_text(cursor, item_end, "title", title, sizeof(title))) {
            rss_tag_text(cursor, item_end, "link", href, sizeof(href));
            if (!rss_tag_text(cursor, item_end, "pubDate", when, sizeof(when)))
                rss_tag_text(cursor, item_end, "updated", when, sizeof(when));
            world_material_item(renderer, title, when, href, source, shown + 1); shown++;
        }
        cursor = item_end + strlen("</item>");
    }
    world_material_close(); return shown;
}

/* A retained body is a compatibility input, never a second semantic owner.
 * Once a source has admitted PublicationOccurrence material, that canonical
 * material is the only material it contributes to a World. */
/* publication_occurrence has no index on source_ref, and a front asks this
 * once per source: each answer was a scan of every occurrence (65k rows), and
 * a cold LIFE home spent a quarter of its time here. The set of sources with a
 * public observed occurrence is read once per worker and refreshed after a
 * minute, which is the same bound the public projection cache already keeps. */
static int occurrence_source_cmp(const void *a, const void *b) { return strcmp(*(const char *const *)a, *(const char *const *)b); }
static int source_has_publication_occurrences(sqlite3 *db, const char *source) {
    static char **sources = NULL; static size_t count = 0; static time_t loaded = 0;
    time_t now = time(NULL);
    if (!db || !source || !*source || !cms_type_registered(db, "publication_occurrence")) return 0;
    if (!sources || now - loaded > 60) {
        sqlite3_stmt *st = NULL; char **next = NULL; size_t n = 0, cap = 0, i;
        if (sqlite3_prepare_v2(db,
            "SELECT DISTINCT source_ref FROM publication_occurrence WHERE status='published' "
            "AND visibility='public' AND assertion_state='OBSERVED' AND COALESCE(source_ref,'')<>''",
            -1, &st, NULL) != SQLITE_OK) return 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *v = (const char *)sqlite3_column_text(st, 0); char *copy;
            if (!v) continue;
            if (n == cap) { char **grown = realloc(next, (cap = cap ? cap * 2 : 256) * sizeof(*next)); if (!grown) break; next = grown; }
            if (!(copy = strdup(v))) break;
            next[n++] = copy;
        }
        sqlite3_finalize(st);
        qsort(next, n, sizeof(*next), occurrence_source_cmp);
        for (i = 0; i < count; i++) free(sources[i]);
        free(sources); sources = next; count = n; loaded = now;
    }
    return count && bsearch(&source, sources, count, sizeof(*sources), occurrence_source_cmp) != NULL;
}

static int world_resolve_sources(sqlite3 *db, const char *declared, char *out, size_t out_sz) {
    char copy[4096], *cursor, *token;
    size_t used = 0;
    if (!db || !declared || !out || out_sz < 2 || strlen(declared) >= sizeof(copy)) return 0;
    out[0] = '\0';
    snprintf(copy, sizeof(copy), "%s", declared);
    for (cursor = copy; cursor && *cursor;) {
        sqlite3_stmt *st = NULL;
        char *comma = strchr(cursor, ',');
        token = cursor;
        if (comma) { *comma = '\0'; cursor = comma + 1; }
        else cursor = NULL;
        while (isspace((unsigned char)*token)) token++;
        if (!strncmp(token, "source_collection:", 18) && token[18]) {
            if (sqlite3_prepare_v2(db,
                    "SELECT value FROM source_collection,json_each(source_selector,'$.source_refs') "
                    "WHERE collection_id=?1 AND source_collection.status='published' "
                    "AND EXISTS (SELECT 1 FROM json_each(source_collection.material_kind) kind "
                    "WHERE kind.value IN ('PublicationOccurrence','publication_occurrence')) ORDER BY value", -1, &st, NULL) != SQLITE_OK)
                return 0;
            sqlite3_bind_text(st, 1, token + 18, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *source = (const char *)sqlite3_column_text(st, 0);
                if (!source || !*source || used + strlen(source) + 2 >= out_sz) { sqlite3_finalize(st); return 0; }
                if (used) out[used++] = ',';
                used += (size_t)snprintf(out + used, out_sz - used, "%s", source);
            }
            sqlite3_finalize(st);
        } else if (*token) {
            if (used + strlen(token) + 2 >= out_sz) return 0;
            if (used) out[used++] = ',';
            used += (size_t)snprintf(out + used, out_sz - used, "%s", token);
        }
    }
    /* An empty selection still resolves: keep the World's native empty state. */
    return 1;
}

static int emit_world_material_from_sources(sqlite3 *db, const char *family, char *sources) {
    sqlite3_stmt *st = NULL; char resolved[4096], *p; int shown = 0;
    if (!db || !family || !sources) return 0;
    if (!world_resolve_sources(db, sources, resolved, sizeof(resolved))) return 0;
    snprintf(sources, 4096, "%s", resolved);
    for (p = sources; *p;) {
        char *start = p, source[256], says[512], twin[256], extract[256], title[256], meta[256], link[256], body[524288];
        while (*p && *p != ',') p++;
        if (*p) *p++ = '\0';
        while (*start && isspace((unsigned char)*start)) start++;
        if (!*start) continue;
        if (source_has_publication_occurrences(db, start)) continue;
        if (sqlite3_prepare_v2(db, "SELECT source,COALESCE(says,''),twin,extract,title_field,COALESCE(meta_field,''),COALESCE(link_field,'') FROM internet_source WHERE source=?1 AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) continue;
        sqlite3_bind_text(st, 1, start, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
#define WCOPY(dst,col) snprintf(dst,sizeof(dst),"%s",sqlite3_column_text(st,col)?(const char *)sqlite3_column_text(st,col):"")
            WCOPY(source,0); WCOPY(says,1); WCOPY(twin,2); WCOPY(extract,3); WCOPY(title,4); WCOPY(meta,5); WCOPY(link,6);
#undef WCOPY
            if (world_body_for_twin(twin, body, sizeof(body))) {
                if (!strncmp(extract, "rss:", 4)) shown += world_material_rss(family, source, says, body);
                else shown += world_material_rows(family, source, says, body, extract, title, meta, link);
            }
        }
        sqlite3_finalize(st); st = NULL;
    }
    return shown;
}

static int serve_world_material(const char *method, const char *target, const char *query,
                                const char *accept) {
    char world[128], fragment[128], family[64] = {0}, sources[2048] = {0};
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    if (strcmp(target, "/_surface/world-material")) return 0;
    if (strcmp(method, "GET") || !query_value(query, "world", world, sizeof(world)) ||
        !query_value(query, "fragment", fragment, sizeof(fragment)) || !studio_safe_slug_arg(world) || !studio_safe_slug_arg(fragment)) {
        protocol_reply(400, "application/json", "{\"error\":\"world_and_fragment_required\"}\n"); return 1;
    }
    if (!(db = cms_open_ro()) || !cms_type_registered(db, "world_fragment")) { if (db) sqlite3_close(db); protocol_reply(503, "application/json", "{\"error\":\"world_store_unavailable\"}\n"); return 1; }
    if (sqlite3_prepare_v2(db, "SELECT f.renderer_family,w.sources FROM world_fragment f JOIN world w ON w.world=f.world AND w.namespace=f.namespace WHERE f.world=?1 AND f.fragment_id=?2 AND f.status='published' AND w.status='published'", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); protocol_reply(500, "application/json", "{\"error\":\"world_material_query_failed\"}\n"); return 1; }
    { char fragment_id[300]; snprintf(fragment_id, sizeof(fragment_id), "%s:%s", world, fragment); sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, fragment_id, -1, SQLITE_TRANSIENT); }
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); sqlite3_close(db); protocol_reply(404, "application/json", "{\"error\":\"world_fragment_not_found\"}\n"); return 1; }
    snprintf(family, sizeof(family), "%s", sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "");
    snprintf(sources, sizeof(sources), "%s", sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : ""); sqlite3_finalize(st);
    if (!accept || !strstr(accept, "text/html")) { sqlite3_close(db); protocol_reply(406, "application/json", "{\"error\":\"html_projection_required\"}\n"); return 1; }
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n");
    emit_world_material_from_sources(db, family, sources);
    sqlite3_close(db); return 1;
}

static const char *world_grammar_route(const char *grammar) {
    size_t i;
    for (i = 0; i < BF_NATIVE_PRESENTATION_CELL_COUNT; i++)
        if (!strcmp(grammar, bf_native_presentation_cells[i].grammar))
            return bf_native_presentation_cells[i].route;
    return NULL;
}

/* Only grammars whose donor document is already lowered natively can retain
 * the World coordinate in a nested public URL.  Other published grammars keep
 * their existing owner route until their own lowerer is ready; a pretty link
 * to a generic/static page is worse than an explicit boundary. */
static const char *world_nested_grammar_route(const char *world, const char *grammar,
                                              char *out, size_t cap) {
    if (!world || !grammar || !out || !cap) return NULL;
    if (world_fragment_route_published(world, grammar)) {
        if (snprintf(out, cap, "/w/%s/%s", world, grammar) >= (int)cap) return NULL;
        return out;
    }
    return world_grammar_route(grammar);
}

/* ------------------------------------------------------------ composed page
 *
 * A subject's page is several reading rhythms composed from the material it
 * actually has (schemas/control/page-composition.cue).  Canonical owner
 * readers normalize material into one stream per kind; the declared regions
 * consume those streams in order.  The executor branches on a region's
 * topology, never on its role, a grammar, a source or a World, and nothing it
 * writes to the page names the machinery that assembled it. */
/* How much of one kind of material a page may hold at once.
 *
 * At 192 a page ran out after two regions no matter how much the estate held:
 * a route naming nine rhythms rendered two of them and stopped, because the
 * stream behind them was empty.
 *
 * It is not free: every stream is held at this size while a page composes, so
 * the number is what one composition costs in memory, multiplied by however
 * many are in flight.  The rhythms that draw on a single stream ask for about
 * a hundred and forty items between them, so this is generous without being
 * the six hundred that left a laptop with nothing to run anything else on. */
#define PAGE_STREAM_MAX 320
typedef struct {
    char title[768], by[256], href[1024], body[2048], group[512], note[128];
    char asset[1024], poster[1024], image[1024];
    char rights[128], rights_href[512];   /* the terms its source states, and where they are written */
    char origin[64], origin_href[768];    /* the commons keeping its picture or file, and its page there */
    char ident[48];                       /* the identity it shares with the same thing elsewhere */
    char about[48];                       /* the identity it depicts or is about, which is not its own */
    char people[256], where[128];         /* who an episode names, and the place it names */
    char transcript_href[512], chapters_href[512], talk_href[512];
    char source[96], key[25];   /* where the item came from, and its stable page key */
    int kind;                   /* the BfPageMaterial stream it belongs to */
    unsigned order;             /* when it was read, so equal times keep their sources' order */
    time_t at; long duration_ms;
    double lon, lat; int placed;
    int archived;               /* read from the durable record, not from what the source publishes now */
} PageItem;
typedef struct { int count, cursor; PageItem item[PAGE_STREAM_MAX]; } PageStream;
typedef struct { PageStream stream[BF_PAGE_MATERIAL_COUNT]; } PageMaterial;

static const char *const page_month_names[] = {
    "Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"
};

#endif /* BONFYRE_SITE_AND_WORLD_MATERIAL_H */
