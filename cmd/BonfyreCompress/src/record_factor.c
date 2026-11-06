#define _POSIX_C_SOURCE 200809L
#include "record_factor.h"
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#ifdef BONFYRE_HAVE_ZSTD
#include <zstd.h>
#include <zdict.h>
#endif
void bf_sha256_hex(const uint8_t *, size_t, char *);
#define RF_MAX (1024u*1024u)
#define RF_APP 1112688198
/* The largest dictionary a pack may carry, and the largest this compiler learns. */
#define RF_DICT (1024u*1024u)
#define RF_DICT_TRAIN (512u*1024u)
typedef struct {
    const void *bytes;size_t size;int owned;
#ifdef BONFYRE_HAVE_ZSTD
    ZSTD_CCtx *c;ZSTD_CDict *cd;ZSTD_DCtx *d;ZSTD_DDict *dd;
#endif
} RFCodec;
static void codec_close(RFCodec *c) {
#ifdef BONFYRE_HAVE_ZSTD
    ZSTD_freeCCtx(c->c);ZSTD_freeCDict(c->cd);ZSTD_freeDCtx(c->d);ZSTD_freeDDict(c->dd);
#endif
    if(c->owned)free((void*)c->bytes);
}
static int sql(sqlite3 *d,const char *s) { return sqlite3_exec(d,s,NULL,NULL,NULL)==SQLITE_OK?0:1; }
static int prep(sqlite3 *d,sqlite3_stmt **s,const char *q) { return sqlite3_prepare_v2(d,q,-1,s,NULL)!=SQLITE_OK; }
static int line_read(FILE *f,char *p,size_t *n) {
    size_t i=0; int c;
    while((c=fgetc(f))!=EOF) { if(i==RF_MAX||c==0)return -1; p[i++]=(char)c; if(c=='\n')break; }
    if(ferror(f))return -1;
    p[i]=0; *n=i; return i?1:0;
}
/* The dictionary is learned from the pieces that will actually be compressed
 * with it: root-level fragments, taken from records spread across the whole
 * source. It used to be learned from the first 4,096 whole records, so on a
 * large source it knew one corner of the material and none of the fragment
 * shapes; a 40,000-recipe pack came out at 70 percent of its source. */
static int train(FILE *f,char *line,RFCodec *codec) {
#ifdef BONFYRE_HAVE_ZSTD
    const size_t cap=48u*1024u*1024u,most=400000;size_t used=0,n=0,*sizes=malloc(most*sizeof(size_t));unsigned count=0;
    char *samples=malloc(cap),*dict=malloc(RF_DICT_TRAIN);struct stat st;long long seen=0,step=1;
    if(!samples||!dict||!sizes||fstat(fileno(f),&st)){free(samples);free(dict);free(sizes);return 1;}
    if((size_t)st.st_size>cap/2)step=(long long)((size_t)st.st_size/(cap/2))+1;
    int read_status=0;
    while((read_status=line_read(f,line,&n))==1){
        if(seen++%step)continue;
        int quoted=0,escaped=0,depth=0;size_t start=0,body=n&&line[n-1]=='\n'?n-1:n;
        for(size_t i=0;i<=body;i++){
            int cut=i==body;
            if(!cut){char c=line[i];if(quoted){if(escaped)escaped=0;else if(c=='\\')escaped=1;else if(c=='"')quoted=0;continue;}
                if(c=='"')quoted=1;else if(c=='{'||c=='[')depth++;else if(c=='}'||c==']')depth--;else if(c==','&&depth==1)cut=1;}
            if(!cut)continue;
            size_t len=(i<body?i+1:n)-start;
            if(len>96&&count<most&&len<=cap-used){memcpy(samples+used,line+start,len);sizes[count++]=len;used+=len;}
            start=i+1;
        }
    }
    if(read_status<0||fseek(f,0,SEEK_SET)){free(samples);free(dict);free(sizes);return 1;}
    if(count>=64&&used>131072u){size_t want=used/100<RF_DICT_TRAIN?(used/100<65536u?65536u:used/100):RF_DICT_TRAIN;
        size_t got=ZDICT_trainFromBuffer(dict,want,samples,sizes,count);if(!ZDICT_isError(got)){codec->bytes=dict;codec->size=got;codec->owned=1;}}
    if(!codec->owned)free(dict);
    free(samples);free(sizes);codec->c=ZSTD_createCCtx();if(!codec->c)return 1;
    if(codec->size){codec->cd=ZSTD_createCDict(codec->bytes,codec->size,19);if(!codec->cd)return 1;}
#else
    (void)f;(void)line;(void)codec;
#endif
    return 0;
}
static int dictionary(sqlite3 *db,sqlite3_stmt **held,RFCodec *codec) {
    sqlite3_stmt *v=NULL;int version=0;char sha[65];
    if(prep(db,&v,"PRAGMA user_version"))return 1;
    if(sqlite3_step(v)==SQLITE_ROW)version=sqlite3_column_int(v,0);
    sqlite3_finalize(v);if(version==1)return 0;
    if(version!=2||prep(db,held,"SELECT payload,digest FROM record_dictionary WHERE id=1"))return 1;
    if(sqlite3_step(*held)!=SQLITE_ROW)return 1;
    int n=sqlite3_column_bytes(*held,0);const char *expected=(const char*)sqlite3_column_text(*held,1);
    if(n<0||(size_t)n>RF_DICT||!expected||strlen(expected)!=64)return 1;
    codec->bytes=sqlite3_column_blob(*held,0);codec->size=(size_t)n;
    bf_sha256_hex(codec->size?codec->bytes:(const void*)"",codec->size,sha);if(strcmp(sha,expected))return 1;
#ifdef BONFYRE_HAVE_ZSTD
    if(n){codec->d=ZSTD_createDCtx();codec->dd=ZSTD_createDDict(codec->bytes,codec->size);if(!codec->d||!codec->dd)return 1;}
#endif
    return 0;
}
int bf_record_factor_open(const char *path,sqlite3 **db) {
    sqlite3_stmt *s=NULL; *db=NULL;
    if(sqlite3_open_v2(path,db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto bad;
    if(prep(*db,&s,"PRAGMA application_id"))goto bad;
    if(sqlite3_step(s)!=SQLITE_ROW||sqlite3_column_int(s,0)!=RF_APP)goto bad;
    sqlite3_finalize(s); s=NULL;
    if(prep(*db,&s,"PRAGMA user_version"))goto bad;
    if(sqlite3_step(s)!=SQLITE_ROW||(sqlite3_column_int(s,0)!=1&&sqlite3_column_int(s,0)!=2))goto bad;
    sqlite3_finalize(s); return 0;
bad: sqlite3_finalize(s); if(*db)sqlite3_close(*db); *db=NULL; return 1;
}
static int unpack(sqlite3_stmt *s,int col,char **out,size_t *len,RFCodec *codec) {
#ifndef BONFYRE_HAVE_ZSTD
    (void)codec;
#endif
    sqlite3_int64 size=sqlite3_column_int64(s,col); int encoding=sqlite3_column_int(s,col+1);
    int n=sqlite3_column_bytes(s,col+2); const void *z=sqlite3_column_blob(s,col+2);
    const char *expected=(const char*)sqlite3_column_text(s,col+3); char sha[65];
    if(size<0||size>RF_MAX||!expected||strlen(expected)!=64)return 1;
    char *p=malloc((size_t)size+1); if(!p)return 1;
    if(encoding==0) { if(n!=size){free(p);return 1;} if(n)memcpy(p,z,(size_t)n); }
#ifdef BONFYRE_HAVE_ZSTD
    else if(encoding==1) { size_t got=ZSTD_decompress(p,(size_t)size,z,(size_t)n); if(ZSTD_isError(got)||got!=(size_t)size){free(p);return 1;} }
    else if(encoding==2&&codec->d&&codec->dd) {size_t got=ZSTD_decompress_usingDDict(codec->d,p,(size_t)size,z,(size_t)n,codec->dd);if(ZSTD_isError(got)||got!=(size_t)size){free(p);return 1;}}
#endif
    else {free(p);return 1;}
    bf_sha256_hex((const uint8_t*)p,(size_t)size,sha);
    if(strcmp(sha,expected)){free(p);return 1;}
    p[size]=0; *out=p; *len=(size_t)size; return 0;
}
int bf_record_factor_read(sqlite3 *db,sqlite3_int64 row,char **body,size_t *length) {
    sqlite3_stmt *s=NULL,*p=NULL,*check=NULL,*dict=NULL;RFCodec codec={0}; char digest[65],actual[65],*out=NULL; size_t used=0;
    sqlite3_int64 bytes=0; int parts=0,seen=0,rc=1,step;
    *body=NULL; *length=0;
    if(dictionary(db,&dict,&codec))goto done;
    if(prep(db,&s,"SELECT raw_bytes,digest,parts FROM record WHERE seq=?"))goto done;
    sqlite3_bind_int64(s,1,row); if(sqlite3_step(s)!=SQLITE_ROW)goto done;
    bytes=sqlite3_column_int64(s,0); parts=sqlite3_column_int(s,2);
    const unsigned char *d=sqlite3_column_text(s,1);
    if(bytes<0||bytes>RF_MAX||!d||strlen((const char*)d)!=64||parts<1||parts>4096)goto done;
    memcpy(digest,d,65); out=malloc((size_t)bytes+1); if(!out)goto done;
    if(prep(db,&p,"SELECT p.ordinal,a.raw_bytes,a.codec,a.payload,a.digest FROM record_part p JOIN record_atom a ON a.id=p.atom WHERE p.record=? ORDER BY p.ordinal"))goto done;
    sqlite3_bind_int64(p,1,row);
    while((step=sqlite3_step(p))==SQLITE_ROW) {
        char *chunk=NULL; size_t n=0;
        if(sqlite3_column_int(p,0)!=seen||unpack(p,1,&chunk,&n,&codec))goto done;
        if(n>(size_t)bytes-used){free(chunk);goto done;}
        memcpy(out+used,chunk,n);used+=n;seen++;free(chunk);
    }
    if(step!=SQLITE_DONE||seen!=parts||used!=(size_t)bytes)goto done;
    bf_sha256_hex((const uint8_t*)out,used,actual);if(strcmp(digest,actual))goto done;
    out[used]=0;
    if(prep(db,&check,"SELECT record_key=coalesce(json_extract(?1,'$.recipe_id'),json_extract(?1,'$.id')) AND title=coalesce(json_extract(?1,'$.title'),json_extract(?1,'$.name')) AND kind=coalesce(json_extract(?1,'$.schema'),'source.record') AND source_url=coalesce(json_extract(?1,'$.source.archive_url'),'') FROM record WHERE seq=?2"))goto done;
    sqlite3_bind_text(check,1,out,(int)used,SQLITE_STATIC);sqlite3_bind_int64(check,2,row);
    if(sqlite3_step(check)!=SQLITE_ROW||sqlite3_column_int(check,0)!=1)goto done;
    *body=out;out=NULL;*length=used;rc=0;
done: codec_close(&codec);sqlite3_finalize(dict);sqlite3_finalize(check);free(out);sqlite3_finalize(s);sqlite3_finalize(p);return rc;
}
static int atom(sqlite3 *db,sqlite3_stmt *find,sqlite3_stmt *ins,const char *p,size_t n,sqlite3_int64 *id,RFCodec *ctx) {
    char digest[65];void *compressed=NULL;size_t count=n;int codec=0,step;
    bf_sha256_hex((const uint8_t*)p,n,digest);sqlite3_reset(find);sqlite3_clear_bindings(find);
    sqlite3_bind_text(find,1,digest,-1,SQLITE_TRANSIENT);step=sqlite3_step(find);
    if(step==SQLITE_ROW){*id=sqlite3_column_int64(find,0);return 0;}
    if(step!=SQLITE_DONE)return 1;
#ifdef BONFYRE_HAVE_ZSTD
    if(n>96) {
        size_t bound=ZSTD_compressBound(n);compressed=malloc(bound);if(!compressed)return 1;
        size_t got=ctx->cd?ZSTD_compress_usingCDict(ctx->c,compressed,bound,p,n,ctx->cd):ZSTD_compressCCtx(ctx->c,compressed,bound,p,n,19);
        if(ZSTD_isError(got)){free(compressed);return 1;}
        if(got<n){count=got;codec=ctx->cd?2:1;}
    }
#else
    (void)ctx;
#endif
    sqlite3_reset(ins);sqlite3_clear_bindings(ins);
    sqlite3_bind_text(ins,1,digest,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(ins,2,(sqlite3_int64)n);
    sqlite3_bind_int(ins,3,codec);sqlite3_bind_blob(ins,4,codec?compressed:p,(int)count,SQLITE_TRANSIENT);
    step=sqlite3_step(ins);free(compressed);if(step!=SQLITE_DONE)return 1;
    *id=sqlite3_last_insert_rowid(db);return 0;
}
static int part(sqlite3 *db,sqlite3_stmt *find,sqlite3_stmt *ins,sqlite3_stmt *put,
                sqlite3_int64 seq,int ordinal,const char *p,size_t n,RFCodec *ctx) {
    sqlite3_int64 id;if(atom(db,find,ins,p,n,&id,ctx))return 1;
    sqlite3_reset(put);sqlite3_clear_bindings(put);sqlite3_bind_int64(put,1,seq);
    sqlite3_bind_int(put,2,ordinal);sqlite3_bind_int64(put,3,id);return sqlite3_step(put)!=SQLITE_DONE;
}
int bf_record_factor_build(const char *source,const char *target,FILE *report) {
    FILE *f=NULL;sqlite3 *db=NULL;RFCodec codec={0};sqlite3_stmt *meta=NULL,*ins=NULL,*find=NULL,*put=NULL,*row=NULL,*setparts=NULL,*stats=NULL;
    char *line=NULL,*tmp=NULL;struct stat before,after,exists;size_t n=0,total=0;sqlite3_int64 seq=0;int fd=-1,rc=1,read_status;
    if(lstat(target,&exists)==0||errno!=ENOENT){fprintf(stderr,"refusing existing output: %s\n",target);return 1;}
    f=fopen(source,"rb");if(!f||fstat(fileno(f),&before)||!S_ISREG(before.st_mode))goto done;
    line=malloc(RF_MAX+1);tmp=calloc(strlen(target)+32,1);if(!line||!tmp)goto done;
    if(train(f,line,&codec))goto done;
    sprintf(tmp,"%s.build-XXXXXX",target);fd=mkstemp(tmp);if(fd<0)goto done;close(fd);fd=-1;
    if(sqlite3_open(tmp,&db)!=SQLITE_OK)goto done;
    if(sql(db,"PRAGMA application_id=1112688198;PRAGMA user_version=2;PRAGMA foreign_keys=ON;PRAGMA journal_mode=DELETE;"
       "CREATE TABLE record_dictionary(id INTEGER PRIMARY KEY,payload BLOB NOT NULL,digest TEXT NOT NULL);"
       "CREATE TABLE record_atom(id INTEGER PRIMARY KEY,digest TEXT NOT NULL,raw_bytes INTEGER NOT NULL,codec INTEGER NOT NULL,payload BLOB NOT NULL);"
       "CREATE UNIQUE INDEX build_atom_digest ON record_atom(digest);"
       "CREATE TABLE record(seq INTEGER PRIMARY KEY,record_key TEXT NOT NULL UNIQUE,title TEXT NOT NULL,kind TEXT NOT NULL,source_url TEXT NOT NULL,keywords TEXT NOT NULL,digest TEXT NOT NULL,raw_bytes INTEGER NOT NULL,parts INTEGER NOT NULL);"
       "CREATE TABLE record_part(record INTEGER NOT NULL REFERENCES record(seq),ordinal INTEGER NOT NULL,atom INTEGER NOT NULL REFERENCES record_atom(id),PRIMARY KEY(record,ordinal)) WITHOUT ROWID;"
       "CREATE VIRTUAL TABLE record_search USING fts5(title,keywords,content='record',content_rowid='seq');BEGIN IMMEDIATE;"))goto done;
    char dict_sha[65];bf_sha256_hex(codec.size?codec.bytes:(const void*)"",codec.size,dict_sha);
    if(prep(db,&meta,"INSERT INTO record_dictionary VALUES(1,?,?)"))goto done;
    sqlite3_bind_blob(meta,1,codec.size?codec.bytes:"",(int)codec.size,SQLITE_STATIC);sqlite3_bind_text(meta,2,dict_sha,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(meta)!=SQLITE_DONE)goto done;
    sqlite3_finalize(meta);meta=NULL;
    if(prep(db,&meta,"SELECT CASE WHEN json_valid(?1) THEN json_type(?1) END,"
       "CASE WHEN json_valid(?1) THEN coalesce(json_extract(?1,'$.recipe_id'),json_extract(?1,'$.id')) END,"
       "CASE WHEN json_valid(?1) THEN coalesce(json_extract(?1,'$.title'),json_extract(?1,'$.name')) END,"
       "CASE WHEN json_valid(?1) THEN coalesce(json_extract(?1,'$.schema'),'source.record') END,"
       "CASE WHEN json_valid(?1) THEN coalesce(json_extract(?1,'$.source.archive_url'),'') END,"
       "CASE WHEN json_valid(?1) THEN (SELECT coalesce(group_concat(CASE WHEN type='object' THEN json_extract(value,'$.name') END,' '),'') FROM json_each(?1,'$.ingredients')) END"))goto done;
    if(prep(db,&row,"INSERT INTO record VALUES(?1,?2,?3,?4,?5,?6,?7,?8,0)")||
       prep(db,&find,"SELECT id FROM record_atom WHERE digest=?")||
       prep(db,&ins,"INSERT INTO record_atom(digest,raw_bytes,codec,payload) VALUES(?,?,?,?)")||
       prep(db,&put,"INSERT INTO record_part VALUES(?,?,?)")||
       prep(db,&setparts,"UPDATE record SET parts=? WHERE seq=?"))goto done;
    while((read_status=line_read(f,line,&n))==1) {
        char digest[65];size_t start=0;int depth=0,quoted=0,escaped=0,parts=0;
        sqlite3_reset(meta);sqlite3_clear_bindings(meta);sqlite3_bind_text(meta,1,line,(int)n,SQLITE_TRANSIENT);
        if(sqlite3_step(meta)!=SQLITE_ROW||sqlite3_column_type(meta,0)!=SQLITE_TEXT||strcmp((const char*)sqlite3_column_text(meta,0),"object"))goto done;
        if(sqlite3_column_type(meta,1)!=SQLITE_TEXT||sqlite3_column_type(meta,2)!=SQLITE_TEXT||sqlite3_column_type(meta,3)!=SQLITE_TEXT||sqlite3_column_type(meta,4)!=SQLITE_TEXT)goto done;
        const char *key=(const char*)sqlite3_column_text(meta,1),*title=(const char*)sqlite3_column_text(meta,2);
        if(!*key||strlen(key)>240||strlen(title)>4096)goto done;
        bf_sha256_hex((const uint8_t*)line,n,digest);sqlite3_reset(row);sqlite3_clear_bindings(row);
        sqlite3_bind_int64(row,1,++seq);sqlite3_bind_text(row,2,key,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(row,3,title,-1,SQLITE_TRANSIENT);sqlite3_bind_text(row,4,(const char*)sqlite3_column_text(meta,3),-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(row,5,(const char*)sqlite3_column_text(meta,4),-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(row,6,(const char*)sqlite3_column_text(meta,5),-1,SQLITE_TRANSIENT);sqlite3_bind_text(row,7,digest,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(row,8,(sqlite3_int64)n);
        if(sqlite3_step(row)!=SQLITE_DONE)goto done;
        /* Split exact bytes only at root field boundaries, never inside a string
         * or nested value. No normalization, rounding, or inferred substitution. */
        for(size_t i=0;i<n;i++) {
            char c=line[i];if(quoted){if(escaped)escaped=0;else if(c=='\\')escaped=1;else if(c=='"')quoted=0;continue;}
            if(c=='"')quoted=1;else if(c=='{'||c=='[')depth++;else if(c=='}'||c==']')depth--;
            else if(c==','&&depth==1){if(parts>=4095||part(db,find,ins,put,seq,parts++,line+start,i+1-start,&codec))goto done;start=i+1;}
        }
        if(depth||quoted||part(db,find,ins,put,seq,parts++,line+start,n-start,&codec))goto done;
        sqlite3_reset(setparts);sqlite3_bind_int(setparts,1,parts);sqlite3_bind_int64(setparts,2,seq);if(sqlite3_step(setparts)!=SQLITE_DONE)goto done;
        char *reconstructed=NULL;size_t rn=0;if(bf_record_factor_read(db,seq,&reconstructed,&rn))goto done;
        int equal=rn==n&&!memcmp(line,reconstructed,n);free(reconstructed);if(!equal)goto done;total+=n;
    }
    if(read_status<0||!seq||fstat(fileno(f),&after)||before.st_size!=after.st_size||before.st_mtime!=after.st_mtime||total!=(size_t)after.st_size)goto done;
    if(sql(db,"INSERT INTO record_search(record_search) VALUES('rebuild');COMMIT;"))goto done;
    if(prep(db,&stats,"SELECT (SELECT count(*) FROM record_atom),(SELECT count(*) FROM record_part),(SELECT sum(raw_bytes) FROM record_atom)"))goto done;
    if(sqlite3_step(stats)!=SQLITE_ROW)goto done;
    sqlite3_int64 atoms=sqlite3_column_int64(stats,0),spans=sqlite3_column_int64(stats,1),distinct=sqlite3_column_int64(stats,2);
    sqlite3_finalize(stats);stats=NULL;sqlite3_finalize(meta);meta=NULL;sqlite3_finalize(row);row=NULL;
    sqlite3_finalize(find);find=NULL;sqlite3_finalize(ins);ins=NULL;sqlite3_finalize(put);put=NULL;sqlite3_finalize(setparts);setparts=NULL;
    /* Digest lookup is a compiler index; immutable reads use atom IDs. Do not
     * ship build-only indexes or their freed pages to every consumer. */
    if(sql(db,"DROP INDEX build_atom_digest;VACUUM;"))goto done;
    if(sqlite3_close(db)!=SQLITE_OK)goto done;
    db=NULL;
    fd=open(tmp,O_RDONLY);if(fd<0||fsync(fd))goto done;close(fd);fd=-1;
    /* link publishes without replacing a file that appeared while compiling. */
    if(bf_record_factor_verify(tmp,source,NULL)||link(tmp,target))goto done;
    if(report)fprintf(report,"records\t%lld\nparts\t%lld\ndistinct_atoms\t%lld\nsource_bytes\t%zu\ndistinct_fragment_bytes\t%lld\nsource_records_verified\t%lld\n",(long long)seq,(long long)spans,(long long)atoms,total,(long long)distinct,(long long)seq);
    rc=0;
done:
    if(rc)fprintf(stderr,"factor failed at record %lld: %s\n",(long long)seq,db?sqlite3_errmsg(db):strerror(errno));
    sqlite3_finalize(meta);sqlite3_finalize(row);sqlite3_finalize(find);sqlite3_finalize(ins);sqlite3_finalize(put);sqlite3_finalize(setparts);sqlite3_finalize(stats);
    if(db)sqlite3_close(db);
    if(fd>=0)close(fd);
    if(f)fclose(f);
    if(tmp&&*tmp)unlink(tmp);
    codec_close(&codec);free(tmp);free(line);return rc;
}
int bf_record_factor_export(const char *path,FILE *out) {
    sqlite3 *d=NULL;sqlite3_stmt *s=NULL;int rc=1,step;if(bf_record_factor_open(path,&d))return 1;
    if(prep(d,&s,"SELECT seq FROM record ORDER BY seq"))goto done;
    while((step=sqlite3_step(s))==SQLITE_ROW){char *p=NULL;size_t n;if(bf_record_factor_read(d,sqlite3_column_int64(s,0),&p,&n))goto done;int bad=fwrite(p,1,n,out)!=n;free(p);if(bad)goto done;}
    if(step==SQLITE_DONE&&!fflush(out))rc=0;
done:sqlite3_finalize(s);sqlite3_close(d);return rc;
}
int bf_record_factor_verify(const char *path,const char *source,FILE *report) {
    sqlite3 *d=NULL;sqlite3_stmt *s=NULL;FILE *f=NULL;char *line=NULL;size_t n;int rc=1,step;sqlite3_int64 count=0;
    if(bf_record_factor_open(path,&d))return 1;
    f=fopen(source,"rb");line=malloc(RF_MAX+1);if(!f||!line||prep(d,&s,"SELECT seq FROM record ORDER BY seq"))goto done;
    while((step=sqlite3_step(s))==SQLITE_ROW){char *p=NULL;size_t pn=0;if(line_read(f,line,&n)!=1||bf_record_factor_read(d,sqlite3_column_int64(s,0),&p,&pn))goto done;int same=pn==n&&!memcmp(p,line,n);free(p);if(!same)goto done;count++;}
    if(step!=SQLITE_DONE||line_read(f,line,&n)!=0)goto done;
    if(report)fprintf(report,"identical_records\t%lld\ncomplete_source\tyes\n",(long long)count);
    rc=0;
done:if(rc)fprintf(stderr,"factor verification failed after %lld records\n",(long long)count);free(line);if(f)fclose(f);sqlite3_finalize(s);sqlite3_close(d);return rc;
}

int bf_record_factor_visit(sqlite3 *db,const char *search,int limit,int offset,
                           BfRecordVisitor visitor,void *context) {
    sqlite3_stmt *s=NULL;int step,n=0;
    if(!db||!visitor||limit<1||limit>100||offset<0)return -1;
    const char *query=search&&*search?
      "SELECT r.seq,r.record_key,r.title,r.kind FROM record_search f JOIN record r ON r.seq=f.rowid WHERE record_search MATCH ?1 ORDER BY rank,r.seq LIMIT ?2 OFFSET ?3":
      "SELECT seq,record_key,title,kind FROM record WHERE ?1 IS NOT NULL ORDER BY seq LIMIT ?2 OFFSET ?3";
    if(sqlite3_prepare_v2(db,query,-1,&s,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(s,1,search?search:"",-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(s,2,limit);sqlite3_bind_int(s,3,offset);
    while((step=sqlite3_step(s))==SQLITE_ROW){
        char *verified=NULL;size_t bytes=0;
        if(bf_record_factor_read(db,sqlite3_column_int64(s,0),&verified,&bytes)){sqlite3_finalize(s);return -1;}
        free(verified);
        if(visitor(context,sqlite3_column_int64(s,0),(const char*)sqlite3_column_text(s,1),
          (const char*)sqlite3_column_text(s,2),(const char*)sqlite3_column_text(s,3))){sqlite3_finalize(s);return -1;}n++;
    }
    sqlite3_finalize(s);return step==SQLITE_DONE?n:-1;
}
