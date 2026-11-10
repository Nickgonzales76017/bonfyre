/* Read-only account Compute view. Meter owns token evidence; Queue never estimates
 * a bill or invents a rate. The current Auth/Queue scope owns the account key. */
#ifndef BONFYRE_ACCOUNT_COMPUTE_H
#define BONFYRE_ACCOUNT_COMPUTE_H

static int awk_compute_read(const char *meter_path,const char *subject,const char *scope,FILE *out) {
    sqlite3 *db=NULL; sqlite3_stmt *q=NULL; bf_mp_writer_t w;
    int ok=0, rows=0, n=0, rc; sqlite3_int64 total=0,snapshots=0,sessions=0,cache=0;
    struct ComputeProvider {
        char id[16]; sqlite3_int64 records,sessions,additive,snapshot,cached,uncached,cache_write,output;
    } providers[2]={{0}};
    if (!meter_path || meter_path[0]!='/' || !qact_scope_valid(subject,scope) ||
        strcmp(scope,"self") || strncmp(subject,"user:",5)) return 2;
    if (sqlite3_open_v2(meter_path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK) goto done;
    sqlite3_busy_timeout(db,1000);
    if (sqlite3_exec(db,"BEGIN",NULL,NULL,NULL)!=SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db,
        "SELECT provider,COUNT(*),COUNT(DISTINCT session_ref),"
        "COALESCE(SUM(CASE WHEN counter_basis<>'inherited_cumulative_snapshot' THEN "
        "COALESCE(uncached_input_tokens,0)+COALESCE(cached_input_tokens,0)+"
        "COALESCE(cache_write_tokens,0)+COALESCE(output_tokens,0) ELSE 0 END),0),"
        "COALESCE(SUM(CASE WHEN counter_basis='inherited_cumulative_snapshot' THEN "
        "COALESCE(uncached_input_tokens,0)+COALESCE(cached_input_tokens,0)+"
        "COALESCE(cache_write_tokens,0)+COALESCE(output_tokens,0) ELSE 0 END),0),"
        "COALESCE(SUM(CASE WHEN counter_basis<>'inherited_cumulative_snapshot' THEN "
        "COALESCE(cached_input_tokens,0) ELSE 0 END),0),"
        "COALESCE(SUM(CASE WHEN counter_basis<>'inherited_cumulative_snapshot' THEN "
        "COALESCE(uncached_input_tokens,0) ELSE 0 END),0),"
        "COALESCE(SUM(CASE WHEN counter_basis<>'inherited_cumulative_snapshot' THEN "
        "COALESCE(cache_write_tokens,0) ELSE 0 END),0),"
        "COALESCE(SUM(CASE WHEN counter_basis<>'inherited_cumulative_snapshot' THEN "
        "COALESCE(output_tokens,0) ELSE 0 END),0) "
        "FROM events WHERE key_id=?1 AND op='model.tokens' AND provider IN ('claude','codex')"
        " GROUP BY provider ORDER BY provider", -1,&q,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(q,1,subject,-1,SQLITE_TRANSIENT);
    while ((rc=sqlite3_step(q))==SQLITE_ROW) {
        const char *provider=(const char *)sqlite3_column_text(q,0);
        struct ComputeProvider *p;
        if (n>=2 || !provider || (strcmp(provider,"claude")&&strcmp(provider,"codex"))) goto done;
        p=&providers[n++];
        snprintf(p->id,sizeof(p->id),"%s",provider);
        p->records=sqlite3_column_int64(q,1);p->sessions=sqlite3_column_int64(q,2);
        p->additive=sqlite3_column_int64(q,3);p->snapshot=sqlite3_column_int64(q,4);
        p->cached=sqlite3_column_int64(q,5);p->uncached=sqlite3_column_int64(q,6);
        p->cache_write=sqlite3_column_int64(q,7);p->output=sqlite3_column_int64(q,8);
        if (p->records<0 || p->sessions<0 || p->additive<0 || p->snapshot<0 ||
            p->cached<0 || p->uncached<0 || p->cache_write<0 || p->output<0 ||
            total>INT64_MAX-p->additive || snapshots>INT64_MAX-p->snapshot ||
            rows>INT_MAX-p->records || sessions>INT64_MAX-p->sessions ||
            cache>INT64_MAX-p->cached) goto done;
        total+=p->additive;snapshots+=p->snapshot;rows+=(int)p->records;
        sessions+=p->sessions;cache+=p->cached;
    }
    if (rc!=SQLITE_DONE) goto done;
    sqlite3_finalize(q);q=NULL;
    bf_mp_writer_init(&w,2048);
    if (bf_mp_pack_array(&w,3)||bf_mp_pack_int(&w,1)||bf_mp_pack_bool(&w,1)||
        bf_mp_pack_map(&w,7)||
        bf_mp_pack_kv_str(&w,"status",rows?"measured":"no_evidence")||
        bf_mp_pack_kv_int(&w,"records",rows)||
        bf_mp_pack_kv_int(&w,"providerSessions",sessions)||
        bf_mp_pack_kv_int(&w,"additiveTokens",total)||
        bf_mp_pack_kv_int(&w,"inheritedSnapshotTokens",snapshots)||
        bf_mp_pack_kv_int(&w,"cachedInputTokens",cache)||
        bf_mp_pack_str_cstr(&w,"providers")||bf_mp_pack_array(&w,(uint32_t)n)) goto done_writer;
    for(int i=0;i<n;i++){
        struct ComputeProvider *p=&providers[i];
        if(bf_mp_pack_map(&w,9)||bf_mp_pack_kv_str(&w,"provider",p->id)||
            bf_mp_pack_kv_int(&w,"records",p->records)||
            bf_mp_pack_kv_int(&w,"sessions",p->sessions)||
            bf_mp_pack_kv_int(&w,"additiveTokens",p->additive)||
            bf_mp_pack_kv_int(&w,"inheritedSnapshotTokens",p->snapshot)||
            bf_mp_pack_kv_int(&w,"cachedInputTokens",p->cached)||
            bf_mp_pack_kv_int(&w,"uncachedInputTokens",p->uncached)||
            bf_mp_pack_kv_int(&w,"cacheWriteTokens",p->cache_write)||
            bf_mp_pack_kv_int(&w,"outputTokens",p->output)) goto done_writer;
    }
    if(bf_mp_writer_size(&w)>QACT_PACKET_LIMIT)goto done_writer;
    ok=fwrite(bf_mp_writer_data(&w),1,bf_mp_writer_size(&w),out)==bf_mp_writer_size(&w)&&!ferror(out);
done_writer:
    bf_mp_writer_free(&w);
done:
    sqlite3_finalize(q);
    if(db){sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);sqlite3_close(db);}
    return ok?0:1;
}
#endif
