/* Read-only, account-scoped narration intake over native CMS content.
 * No new task database: the canonical site/post identity and MediaProgram
 * publication state tell every production surface what remains to do. */
#ifndef BF_BROADCAST_NARRATION_WORKLIST_H
#define BF_BROADCAST_NARRATION_WORKLIST_H
static int cmd_intake(int argc,char **argv){
    const char *cms=opt(argc,argv,"--cms",getenv("BONFYRE_CMS_DB"));
    const char *ns=opt(argc,argv,"--namespace",NULL);
    const char *subject=opt(argc,argv,"--subject",NULL);
    const char *out=opt(argc,argv,"--out",NULL);
    const char *only=opt(argc,argv,"--only",NULL);
    const char *limit_arg=opt(argc,argv,"--limit","200");
    const char *after_ns=opt(argc,argv,"--after-namespace",NULL);
    const char *after_ref=opt(argc,argv,"--after-subject",NULL);
    if(!cms||!*cms)return fail("intake requires the canonical --cms DB");
    if(!limit_arg||!*limit_arg||strspn(limit_arg,"0123456789")!=strlen(limit_arg))
        return fail("intake --limit is an integer from 1 to 5000");
    long limit=strtol(limit_arg,NULL,10);
    if(limit<1||limit>5000)return fail("intake --limit is 1..5000");
    if((out&&!subject)||(subject&&(!ns||!bf_narration_ref(subject)))||
       (ns&&(!*ns||strlen(ns)>96||strspn(ns,"abcdefghijklmnopqrstuvwxyz0123456789-_.")!=strlen(ns))))
        return fail("intake requires exact --namespace and --subject for a script");
    if((after_ns&&!after_ref)||(!after_ns&&after_ref)||(subject&&after_ns)||
       (after_ns&&(!*after_ns||strlen(after_ns)>96||
        strspn(after_ns,"abcdefghijklmnopqrstuvwxyz0123456789-_.")!=strlen(after_ns)))||
       (after_ref&&!bf_narration_ref(after_ref)))
        return fail("intake --after-namespace and --after-subject require one valid keyset cursor");
    sqlite3 *db=NULL;
    if(sqlite3_open_v2(cms,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){
        if(db)sqlite3_close(db);return fail("intake cannot read CMS");
    }
    int with_sha=bf_narration_digest_column(db),count=0;
    if(subject){
        BfNarrationSource source;
        if(!bf_narration_source(db,ns,subject,&source)){
            sqlite3_close(db);return fail("subject is not one published, eligible native article or post");
        }
        const char *state=bf_narration_state(db,ns,subject,source.sha,with_sha);
        if(only&&strcmp(only,state)){sqlite3_close(db);return 0;}
        if(out){
            int fd=open(out,O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW,0600);
            if(fd<0){sqlite3_close(db);return fail("script already exists or cannot be created");}
            ssize_t wrote=write(fd,source.text,source.length);
            int valid=wrote==(ssize_t)source.length&&!fsync(fd);
            if(close(fd))valid=0;
            if(!valid){unlink(out);sqlite3_close(db);return fail("could not retain the complete source script");}
        }
        printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
               source.kind,ns,subject,source.sha,state,source.route,source.title);
        sqlite3_close(db);return 0;
    }
    sqlite3_stmt *stmt=NULL;
    const char *query=
       "SELECT 'article',namespace,COALESCE(media_subject_ref,''),"
       "COALESCE(heading,title),route FROM site_page "
       "WHERE status='published' AND editorial_kind IN "
       "('article','essay','report','feature','longform') AND (?2='' OR namespace=?2) "
       "AND (?3='' OR namespace>?3 OR (namespace=?3 AND COALESCE(media_subject_ref,'')>?4)) "
       "UNION ALL SELECT 'post',namespace,'post:'||post,"
       "COALESCE(NULLIF(title,''),'Community post'),'' FROM community_post "
       "WHERE status='published' AND visibility='public' "
       "AND moderation_state IN ('accepted','visible','published') "
       "AND (?2='' OR namespace=?2) "
       "AND (?3='' OR namespace>?3 OR (namespace=?3 AND ('post:'||post)>?4)) "
       "ORDER BY 2,3 LIMIT ?1";
    if(sqlite3_prepare_v2(db,query,-1,&stmt,NULL)!=SQLITE_OK){
        sqlite3_close(db);return fail("CMS lacks the native authoring content types");
    }
    sqlite3_bind_int(stmt,1,(int)limit);
    sqlite3_bind_text(stmt,2,ns?ns:"",-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt,3,after_ns?after_ns:"",-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt,4,after_ref?after_ref:"",-1,SQLITE_TRANSIENT);
    puts("kind\tnamespace\tsubject\tsource_sha256\tstate\troute\ttitle");
    while(sqlite3_step(stmt)==SQLITE_ROW){
        const char *kind=(const char*)sqlite3_column_text(stmt,0),
                   *row_ns=(const char*)sqlite3_column_text(stmt,1),
                   *ref=(const char*)sqlite3_column_text(stmt,2);
        if(!row_ns||!ref||(ns&&strcmp(ns,row_ns)))continue;
        const char *state="needs-identity",*digest="",*route="";
        BfNarrationSource source;
        if(bf_narration_ref(ref)&&bf_narration_source(db,row_ns,ref,&source)){
            digest=source.sha;route=source.route;
            state=bf_narration_state(db,row_ns,ref,digest,with_sha);
            if(!strcmp(kind,"post")&&!strcmp(state,"needs-narration"))
                state="requires-author-review";
        }else if(*ref)state="needs-source-review";
        if(only&&strcmp(only,state))continue;
        /* A tab-delimited human view: unexpected content text is truncated
         * to its safe display value, not treated as a control record. */
        const char *title=(const char*)sqlite3_column_text(stmt,3);
        char safe_title[256];size_t n=0;
        for(const char *p=title?title:"";*p&&n<sizeof(safe_title)-1;p++)
            safe_title[n++]=(*p=='\t'||*p=='\r'||*p=='\n')?' ':*p;
        safe_title[n]=0;
        printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
               kind?kind:"",row_ns,ref,digest,state,route,safe_title);
        count++;
    }
    sqlite3_finalize(stmt);sqlite3_close(db);
    if(!count)fprintf(stderr,"bonfyre-broadcast: no matching narrated-content candidates\n");
    return 0;
}
#endif
