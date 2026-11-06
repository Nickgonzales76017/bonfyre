#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/* Dense physical representation registry.  This is not a truth owner: it
 * compiles passport declarations into handles, support rows and receipts, and
 * returns candidates only.  Exact owner verification/admission is downstream. */
#include "physical.h"
#include <errno.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <bonfyre.h>

static int ex(sqlite3 *d, const char *q) {
    char *e = NULL; int rc = sqlite3_exec(d, q, NULL, NULL, &e);
    if (rc != SQLITE_OK) { fprintf(stderr, "physical sqlite: %s\n", e ? e : "error"); sqlite3_free(e); }
    return rc == SQLITE_OK;
}

static int init(sqlite3 *d) {
    return ex(d,
      "PRAGMA foreign_keys=ON;PRAGMA journal_mode=WAL;"
      "CREATE TABLE IF NOT EXISTS representation_basis(handle INTEGER PRIMARY KEY,identity TEXT NOT NULL UNIQUE) STRICT;"
      "CREATE TABLE IF NOT EXISTS representation_body(handle INTEGER PRIMARY KEY,physical_digest TEXT NOT NULL UNIQUE,canonical_body_ref TEXT NOT NULL,declared_bytes INTEGER NOT NULL CHECK(declared_bytes>=0)) STRICT;"
      "CREATE TABLE IF NOT EXISTS representation_passport("
      "handle INTEGER PRIMARY KEY,identity TEXT NOT NULL UNIQUE,subject_family TEXT NOT NULL,semantic_digest TEXT NOT NULL,"
      "representation_family TEXT NOT NULL,representation_version TEXT NOT NULL,"
      "exactness_class TEXT NOT NULL CHECK(exactness_class IN('exact','latent')),"
      "quality_loss_budget REAL NOT NULL CHECK(quality_loss_budget>=0),"
      "measurement_state TEXT NOT NULL CHECK(measurement_state IN('measured','declared')),"
      "basis_handle INTEGER REFERENCES representation_basis(handle),binding_digest TEXT NOT NULL,exception_digest TEXT NOT NULL,"
      "physical_body_ref TEXT NOT NULL,physical_digest TEXT NOT NULL,lift_ref TEXT NOT NULL,thaw_ref TEXT NOT NULL,"
      "physical_bytes INTEGER NOT NULL CHECK(physical_bytes>=0),encode_ns INTEGER NOT NULL CHECK(encode_ns>=0),"
      "decode_ns INTEGER NOT NULL CHECK(decode_ns>=0),scan_ns INTEGER NOT NULL CHECK(scan_ns>=0),"
      "random_read_ns INTEGER NOT NULL CHECK(random_read_ns>=0),memory_bandwidth INTEGER NOT NULL CHECK(memory_bandwidth>=0),"
      "copy_amplification REAL NOT NULL CHECK(copy_amplification>=0),hydration_amplification REAL NOT NULL CHECK(hydration_amplification>=0),"
      "source_json TEXT NOT NULL) STRICT;"
      "CREATE INDEX IF NOT EXISTS representation_family_ix ON representation_passport(subject_family,exactness_class,measurement_state);"
      "CREATE TABLE IF NOT EXISTS representation_binding(passport_handle INTEGER PRIMARY KEY REFERENCES representation_passport(handle) ON DELETE CASCADE,basis_handle INTEGER NOT NULL REFERENCES representation_basis(handle),subject_family TEXT NOT NULL,binding_digest TEXT NOT NULL) STRICT;"
      "CREATE INDEX IF NOT EXISTS representation_binding_basis_ix ON representation_binding(basis_handle,subject_family);"
      "CREATE TABLE IF NOT EXISTS representation_exception(passport_handle INTEGER PRIMARY KEY REFERENCES representation_passport(handle) ON DELETE CASCADE,exception_digest TEXT NOT NULL) STRICT;"
      "CREATE TABLE IF NOT EXISTS representation_support(passport_handle INTEGER NOT NULL REFERENCES representation_passport(handle) ON DELETE CASCADE,operation TEXT NOT NULL,PRIMARY KEY(passport_handle,operation)) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS representation_support_ix ON representation_support(operation,passport_handle);"
      "CREATE TABLE IF NOT EXISTS physical_generation(name TEXT PRIMARY KEY,source_digest TEXT NOT NULL,compiled_at TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%fZ','now'))) STRICT;"
      "CREATE TABLE IF NOT EXISTS compression_receipt(receipt_id TEXT PRIMARY KEY,passport_handle INTEGER NOT NULL REFERENCES representation_passport(handle),"
      "bytes_read INTEGER NOT NULL,bytes_borrowed INTEGER NOT NULL,bytes_copied INTEGER NOT NULL,bytes_decompressed INTEGER NOT NULL,"
      "bytes_reencoded INTEGER NOT NULL,bytes_hydrated INTEGER NOT NULL,allocations INTEGER NOT NULL,serialize_count INTEGER NOT NULL,"
      "deserialize_count INTEGER NOT NULL,materialization_count INTEGER NOT NULL,borrowed_boundaries INTEGER NOT NULL,fused_boundaries INTEGER NOT NULL,"
      "useful_bytes INTEGER NOT NULL,recorded_at TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%fZ','now'))) STRICT;"
      /* Old generation CopyReceipts have no FK to the mutable, rebuilt
       * passport handles. Preserve every recorded counter and its original
       * subject/representation identity in the same transaction as rebuild. */
      "CREATE TABLE IF NOT EXISTS compression_receipt_history("
      "archive_row INTEGER PRIMARY KEY,"
      "generation_digest TEXT NOT NULL,receipt_id TEXT NOT NULL,passport_identity TEXT NOT NULL,"
      "subject_family TEXT NOT NULL,semantic_digest TEXT NOT NULL,physical_digest TEXT NOT NULL,"
      "exactness_class TEXT NOT NULL,"
      "bytes_read INTEGER NOT NULL,bytes_borrowed INTEGER NOT NULL,bytes_copied INTEGER NOT NULL,"
      "bytes_decompressed INTEGER NOT NULL,bytes_reencoded INTEGER NOT NULL,bytes_hydrated INTEGER NOT NULL,"
      "allocations INTEGER NOT NULL,serialize_count INTEGER NOT NULL,deserialize_count INTEGER NOT NULL,"
      "materialization_count INTEGER NOT NULL,borrowed_boundaries INTEGER NOT NULL,"
      "fused_boundaries INTEGER NOT NULL,useful_bytes INTEGER NOT NULL,recorded_at TEXT NOT NULL,"
      "archived_at TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%fZ','now'))) STRICT;"
    );
}

static const char *arg(int n,char **v,const char *key) { for(int i=0;i+1<n;i++) if(!strcmp(v[i],key)) return v[i+1]; return NULL; }
static int nonempty(const unsigned char *s) { return s && *s; }

static int no_unknowns(sqlite3 *d,const char *line) {
    const char *q="SELECT count(*) FROM json_each(?1) WHERE key NOT IN('identity','subject_family','semantic_digest','representation_family','representation_version','exactness_class','quality_loss_budget','measurement_state','shared_basis','binding_digest','exception_digest','physical_body_ref','physical_digest','supports','lift_ref','thaw_ref','bytes','encode_ns','decode_ns','scan_ns','random_read_ns','memory_bandwidth','copy_amplification','hydration_amplification')";
    sqlite3_stmt *s=NULL;int ok=0;if(sqlite3_prepare_v2(d,q,-1,&s,NULL)!=SQLITE_OK)return 0;sqlite3_bind_text(s,1,line,-1,SQLITE_TRANSIENT);ok=sqlite3_step(s)==SQLITE_ROW&&!sqlite3_column_int(s,0);sqlite3_finalize(s);return ok;
}

static int insert_one(sqlite3 *d,const char *line) {
    const char *fq="SELECT json_extract(?1,'$.identity'),json_extract(?1,'$.subject_family'),json_extract(?1,'$.semantic_digest'),json_extract(?1,'$.representation_family'),json_extract(?1,'$.representation_version'),json_extract(?1,'$.exactness_class'),json_extract(?1,'$.quality_loss_budget'),json_extract(?1,'$.measurement_state'),coalesce(json_extract(?1,'$.shared_basis'),''),coalesce(json_extract(?1,'$.binding_digest'),''),coalesce(json_extract(?1,'$.exception_digest'),''),json_extract(?1,'$.physical_body_ref'),json_extract(?1,'$.physical_digest'),json_extract(?1,'$.lift_ref'),coalesce(json_extract(?1,'$.thaw_ref'),''),coalesce(json_extract(?1,'$.bytes'),0),coalesce(json_extract(?1,'$.encode_ns'),0),coalesce(json_extract(?1,'$.decode_ns'),0),coalesce(json_extract(?1,'$.scan_ns'),0),coalesce(json_extract(?1,'$.random_read_ns'),0),coalesce(json_extract(?1,'$.memory_bandwidth'),0),coalesce(json_extract(?1,'$.copy_amplification'),0),coalesce(json_extract(?1,'$.hydration_amplification'),0) WHERE json_valid(?1) AND json_type(?1,'$.supports')='array'";
    sqlite3_stmt *r=NULL,*b=NULL,*body_stmt=NULL,*p=NULL,*ops=NULL,*bind_stmt=NULL,*exc_stmt=NULL; sqlite3_int64 bh=0,h=0; int ok=0,count=0;
    const unsigned char *identity,*family,*semantic,*rfamily,*version,*exact,*state,*basis,*binding,*exception,*body,*digest,*lift,*thaw;
    if(!no_unknowns(d,line)||sqlite3_prepare_v2(d,fq,-1,&r,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(r,1,line,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(r)!=SQLITE_ROW)goto done;
    identity=sqlite3_column_text(r,0);family=sqlite3_column_text(r,1);semantic=sqlite3_column_text(r,2);rfamily=sqlite3_column_text(r,3);version=sqlite3_column_text(r,4);exact=sqlite3_column_text(r,5);state=sqlite3_column_text(r,7);basis=sqlite3_column_text(r,8);binding=sqlite3_column_text(r,9);exception=sqlite3_column_text(r,10);body=sqlite3_column_text(r,11);digest=sqlite3_column_text(r,12);lift=sqlite3_column_text(r,13);thaw=sqlite3_column_text(r,14);
    if(!nonempty(identity)||!nonempty(family)||!nonempty(semantic)||!nonempty(rfamily)||!nonempty(version)||!nonempty(exact)||!nonempty(state)||!nonempty(body)||!nonempty(digest)||!nonempty(lift)||!basis||!binding||!exception||!thaw)goto done;
    if((strcmp((const char*)exact,"exact")&&strcmp((const char*)exact,"latent"))||(strcmp((const char*)state,"measured")&&strcmp((const char*)state,"declared"))||sqlite3_column_double(r,6)<0)goto done;
    if(!strcmp((const char*)exact,"exact")&&sqlite3_column_double(r,6)!=0)goto done;
    if(!strcmp((const char*)exact,"latent")&&!nonempty(thaw))goto done;
    /* Digest equality deduplicates retained physical bodies only.  Passport
     * identities, semantic digests, bindings and exceptions remain separate;
     * conflicting byte declarations for one claimed body are invalid rather
     * than silently choosing one accounting story. */
    if(sqlite3_prepare_v2(d,"INSERT INTO representation_body(physical_digest,canonical_body_ref,declared_bytes) VALUES(?1,?2,?3) ON CONFLICT(physical_digest) DO NOTHING",-1,&body_stmt,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(body_stmt,1,(const char*)digest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(body_stmt,2,(const char*)body,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(body_stmt,3,sqlite3_column_int64(r,15));
    if(sqlite3_step(body_stmt)!=SQLITE_DONE){sqlite3_finalize(body_stmt);body_stmt=NULL;goto done;}sqlite3_finalize(body_stmt);body_stmt=NULL;
    if(sqlite3_prepare_v2(d,"SELECT declared_bytes FROM representation_body WHERE physical_digest=?1",-1,&body_stmt,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(body_stmt,1,(const char*)digest,-1,SQLITE_TRANSIENT);if(sqlite3_step(body_stmt)!=SQLITE_ROW||sqlite3_column_int64(body_stmt,0)!=sqlite3_column_int64(r,15)){sqlite3_finalize(body_stmt);body_stmt=NULL;goto done;}sqlite3_finalize(body_stmt);body_stmt=NULL;
    if(basis[0]) { if(sqlite3_prepare_v2(d,"INSERT INTO representation_basis(identity) VALUES(?1) ON CONFLICT(identity) DO NOTHING",-1,&b,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(b,1,(const char*)basis,-1,SQLITE_TRANSIENT);if(sqlite3_step(b)!=SQLITE_DONE)goto done;sqlite3_finalize(b);b=NULL;if(sqlite3_prepare_v2(d,"SELECT handle FROM representation_basis WHERE identity=?1",-1,&b,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(b,1,(const char*)basis,-1,SQLITE_TRANSIENT);if(sqlite3_step(b)!=SQLITE_ROW)goto done;bh=sqlite3_column_int64(b,0); }
    if(sqlite3_prepare_v2(d,"INSERT INTO representation_passport(identity,subject_family,semantic_digest,representation_family,representation_version,exactness_class,quality_loss_budget,measurement_state,basis_handle,binding_digest,exception_digest,physical_body_ref,physical_digest,lift_ref,thaw_ref,physical_bytes,encode_ns,decode_ns,scan_ns,random_read_ns,memory_bandwidth,copy_amplification,hydration_amplification,source_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,NULLIF(?9,0),?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24)",-1,&p,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(p,1,(const char*)identity,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,2,(const char*)family,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,3,(const char*)semantic,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,4,(const char*)rfamily,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,5,(const char*)version,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,6,(const char*)exact,-1,SQLITE_TRANSIENT);sqlite3_bind_double(p,7,sqlite3_column_double(r,6));sqlite3_bind_text(p,8,(const char*)state,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(p,9,bh);sqlite3_bind_text(p,10,(const char*)binding,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,11,(const char*)exception,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,12,(const char*)body,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,13,(const char*)digest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,14,(const char*)lift,-1,SQLITE_TRANSIENT);sqlite3_bind_text(p,15,(const char*)thaw,-1,SQLITE_TRANSIENT);for(int i=16;i<=21;i++)sqlite3_bind_int64(p,i,sqlite3_column_int64(r,i-1));sqlite3_bind_double(p,22,sqlite3_column_double(r,21));sqlite3_bind_double(p,23,sqlite3_column_double(r,22));sqlite3_bind_text(p,24,line,-1,SQLITE_TRANSIENT);if(sqlite3_step(p)!=SQLITE_DONE)goto done;h=sqlite3_last_insert_rowid(d);
    if(bh){if(sqlite3_prepare_v2(d,"INSERT INTO representation_binding(passport_handle,basis_handle,subject_family,binding_digest) VALUES(?1,?2,?3,?4)",-1,&bind_stmt,NULL)!=SQLITE_OK)goto done;sqlite3_bind_int64(bind_stmt,1,h);sqlite3_bind_int64(bind_stmt,2,bh);sqlite3_bind_text(bind_stmt,3,(const char*)family,-1,SQLITE_TRANSIENT);sqlite3_bind_text(bind_stmt,4,(const char*)binding,-1,SQLITE_TRANSIENT);if(sqlite3_step(bind_stmt)!=SQLITE_DONE)goto done;}
    if(exception[0]){if(sqlite3_prepare_v2(d,"INSERT INTO representation_exception(passport_handle,exception_digest) VALUES(?1,?2)",-1,&exc_stmt,NULL)!=SQLITE_OK)goto done;sqlite3_bind_int64(exc_stmt,1,h);sqlite3_bind_text(exc_stmt,2,(const char*)exception,-1,SQLITE_TRANSIENT);if(sqlite3_step(exc_stmt)!=SQLITE_DONE)goto done;}
    if(sqlite3_prepare_v2(d,"SELECT value FROM json_each(?1,'$.supports') WHERE typeof(value)='text' AND length(value)>0",-1,&ops,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(ops,1,line,-1,SQLITE_TRANSIENT);while(sqlite3_step(ops)==SQLITE_ROW){sqlite3_stmt*i=NULL;const unsigned char*op=sqlite3_column_text(ops,0);if(!op||sqlite3_prepare_v2(d,"INSERT INTO representation_support(passport_handle,operation) VALUES(?1,?2)",-1,&i,NULL)!=SQLITE_OK)goto done;sqlite3_bind_int64(i,1,h);sqlite3_bind_text(i,2,(const char*)op,-1,SQLITE_TRANSIENT);int rc=sqlite3_step(i);sqlite3_finalize(i);if(rc!=SQLITE_DONE)goto done;count++;}if(!count)goto done;ok=1;
done:sqlite3_finalize(r);sqlite3_finalize(b);sqlite3_finalize(body_stmt);sqlite3_finalize(p);sqlite3_finalize(ops);sqlite3_finalize(bind_stmt);sqlite3_finalize(exc_stmt);return ok;
}

static int import(sqlite3 *d, const char *path, const char *digest, int *out) {
    FILE *f = fopen(path, "r");
    char *line = NULL;
    size_t cap = 0;
    ssize_t n;
    int count = 0, ok = 0;
    sqlite3_stmt *generation = NULL;
    if (!f || !digest) goto done;
    if (!ex(d, "BEGIN IMMEDIATE;")) goto done;
    /* An old receipt must not be orphaned by replacing its passport handle.
     * The LEFT JOIN intentionally fails on a missing passport: NOT NULL on
     * archived owner coordinates prevents quietly discarding such receipts. */
    if (!ex(d,
        "INSERT INTO compression_receipt_history("
        "generation_digest,receipt_id,passport_identity,subject_family,semantic_digest,"
        "physical_digest,exactness_class,bytes_read,bytes_borrowed,bytes_copied,"
        "bytes_decompressed,bytes_reencoded,bytes_hydrated,allocations,serialize_count,"
        "deserialize_count,materialization_count,borrowed_boundaries,fused_boundaries,"
        "useful_bytes,recorded_at) "
        "SELECT coalesce((SELECT source_digest FROM physical_generation "
        "WHERE name='passport'),'unversioned'),r.receipt_id,p.identity,p.subject_family,"
        "p.semantic_digest,p.physical_digest,p.exactness_class,r.bytes_read,"
        "r.bytes_borrowed,r.bytes_copied,r.bytes_decompressed,r.bytes_reencoded,"
        "r.bytes_hydrated,r.allocations,r.serialize_count,r.deserialize_count,"
        "r.materialization_count,r.borrowed_boundaries,r.fused_boundaries,"
        "r.useful_bytes,r.recorded_at FROM compression_receipt r "
        "LEFT JOIN representation_passport p ON p.handle=r.passport_handle;")) goto rollback;
    if (!ex(d, "DELETE FROM compression_receipt;"
               "DELETE FROM representation_passport;"
               "DELETE FROM representation_basis;"
               "DELETE FROM representation_body;")) goto rollback;
    while ((n = getline(&line, &cap, f)) >= 0) {
        while (n && strchr("\r\n \t", line[n-1])) line[--n] = 0;
        if (!n) continue;
        if (!insert_one(d, line)) {
            fprintf(stderr, "physical compiler: rejected declaration %d\n", count + 1);
            goto rollback;
        }
        count++;
    }
    if (ferror(f) || !count) goto rollback;
    /* Passports and the digest naming their generation become visible
     * together. The previous code committed passports before this metadata. */
    if (sqlite3_prepare_v2(d,
        "INSERT INTO physical_generation(name,source_digest) VALUES('passport',?1) "
        "ON CONFLICT(name) DO UPDATE SET source_digest=excluded.source_digest,"
        "compiled_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
        -1, &generation, NULL) != SQLITE_OK) goto rollback;
    sqlite3_bind_text(generation, 1, digest, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(generation) != SQLITE_DONE) goto rollback;
    sqlite3_finalize(generation); generation = NULL;
    if (!ex(d, "COMMIT;")) goto rollback;
    *out = count;
    ok = 1;
    goto done;
rollback:
    sqlite3_finalize(generation); generation = NULL;
    ex(d, "ROLLBACK;");
done:
    sqlite3_finalize(generation);
    free(line);
    if (f) fclose(f);
    return ok;
}

static int compile_passports(const char *dbp, const char *in) {
    sqlite3 *db = NULL;
    sqlite3_stmt *generation = NULL;
    char digest[65];
    int compiled = 0, ok = 0;
    if (bf_sha256_file(in, digest) != 0) return 1;
    if (sqlite3_open(dbp, &db) != SQLITE_OK || !init(db)) goto done;
    /* An unchanged digest is a no-op only when its derived body index exists. */
    if (sqlite3_prepare_v2(db,
        "SELECT source_digest FROM physical_generation WHERE name='passport' "
        "AND (SELECT count(*) FROM representation_body)="
        "(SELECT count(DISTINCT physical_digest) FROM representation_passport)",
        -1, &generation, NULL) == SQLITE_OK &&
        sqlite3_step(generation) == SQLITE_ROW &&
        sqlite3_column_text(generation, 0) &&
        !strcmp((const char *)sqlite3_column_text(generation, 0), digest)) {
        puts("{\"compiled_passports\":0,\"state\":\"unchanged\",\"runtime\":\"dense_handles\",\"truth\":\"representation_contract_only\"}");
        ok = 1;
        goto done;
    }
    sqlite3_finalize(generation); generation = NULL;
    /* No throwaway SQLite import: the real import validates and publishes in
     * one rollback-capable transaction with its generation metadata. */
    if (!import(db, in, digest, &compiled)) goto done;
    printf("{\"compiled_passports\":%d,\"state\":\"published_derived_generation\",\"runtime\":\"dense_handles\",\"truth\":\"representation_contract_only\"}\n", compiled);
    ok = 1;
done:
    sqlite3_finalize(generation);
    if (db) sqlite3_close(db);
    return ok ? 0 : 1;
}

static int plan(const char*dbp,const char*family,const char*op,double loss,int exact) {sqlite3*d=NULL;sqlite3_stmt*s=NULL;int n=0,rc=1;const char*q="SELECT p.handle,p.identity,p.representation_family,p.exactness_class,p.measurement_state,p.quality_loss_budget,p.physical_bytes,p.lift_ref,p.thaw_ref FROM representation_passport p JOIN representation_support x ON x.passport_handle=p.handle WHERE p.subject_family=?1 AND x.operation=?2 AND(p.exactness_class='exact' OR(?3=0 AND p.measurement_state='measured' AND p.quality_loss_budget<=?4)) ORDER BY CASE p.exactness_class WHEN 'exact' THEN 0 ELSE 1 END,p.physical_bytes,p.handle";if(sqlite3_open_v2(dbp,&d,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK||sqlite3_prepare_v2(d,q,-1,&s,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(s,1,family,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,op,-1,SQLITE_TRANSIENT);sqlite3_bind_int(s,3,exact);sqlite3_bind_double(s,4,loss);printf("{\"eligible_for_further_consideration\":[");while(sqlite3_step(s)==SQLITE_ROW){if(n++)putchar(',');printf("{\"handle\":%lld,\"identity\":\"%s\",\"representation_family\":\"%s\",\"exactness\":\"%s\",\"measurement\":\"%s\",\"loss_budget\":%.9g,\"physical_bytes\":%lld,\"lift_ref\":\"%s\",\"thaw_ref\":\"%s\"}",(long long)sqlite3_column_int64(s,0),sqlite3_column_text(s,1),sqlite3_column_text(s,2),sqlite3_column_text(s,3),sqlite3_column_text(s,4),sqlite3_column_double(s,5),(long long)sqlite3_column_int64(s,6),sqlite3_column_text(s,7),sqlite3_column_text(s,8));}printf("],\"selected\":false,\"law\":\"exact_owner_and_resource_admission_required\"}\n");rc=0;done:sqlite3_finalize(s);if(d)sqlite3_close(d);return rc;}

static int num(int n,char**v,const char*k,unsigned long long*out){const char*s=arg(n,v,k),*p=NULL;if(!s)return 0;errno=0;*out=strtoull(s,(char**)&p,10);return !errno&&p&&!*p;}
static int receipt(int n,char**v){const char*dbp=arg(n,v,"--db"),*id=arg(n,v,"--passport"),*rid=arg(n,v,"--receipt");const char*keys[]={"--bytes-read","--bytes-borrowed","--bytes-copied","--bytes-decompressed","--bytes-reencoded","--bytes-hydrated","--allocations","--serialize-count","--deserialize-count","--materialization-count","--borrowed-boundaries","--fused-boundaries","--useful-bytes"};unsigned long long x[13];sqlite3*d=NULL;sqlite3_stmt*s=NULL;int rc=1;if(!dbp||!id||!rid)return 1;for(int i=0;i<13;i++)if(!num(n,v,keys[i],&x[i]))return 1;if(sqlite3_open(dbp,&d)!=SQLITE_OK||!init(d)||sqlite3_prepare_v2(d,"INSERT INTO compression_receipt(receipt_id,passport_handle,bytes_read,bytes_borrowed,bytes_copied,bytes_decompressed,bytes_reencoded,bytes_hydrated,allocations,serialize_count,deserialize_count,materialization_count,borrowed_boundaries,fused_boundaries,useful_bytes)SELECT ?1,handle,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14 FROM representation_passport WHERE identity=?15",-1,&s,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(s,1,rid,-1,SQLITE_TRANSIENT);for(int i=0;i<13;i++)sqlite3_bind_int64(s,i+2,(sqlite3_int64)x[i]);sqlite3_bind_text(s,15,id,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)!=SQLITE_DONE||sqlite3_changes(d)!=1)goto done;puts("{\"receipt\":\"recorded\",\"measurement\":\"executed_runtime_counters\"}");rc=0;done:sqlite3_finalize(s);if(d)sqlite3_close(d);return rc;}

static int ledger(const char*dbp){sqlite3*d=NULL;sqlite3_stmt*s=NULL;int rc=1;const char*q="SELECT count(*),coalesce(sum(physical_bytes),0),(SELECT count(*) FROM compression_receipt),(SELECT coalesce(sum(bytes_read),0) FROM compression_receipt),(SELECT coalesce(sum(bytes_borrowed),0) FROM compression_receipt),(SELECT coalesce(sum(bytes_copied),0) FROM compression_receipt),(SELECT coalesce(sum(bytes_hydrated),0) FROM compression_receipt),(SELECT coalesce(sum(useful_bytes),0) FROM compression_receipt),(SELECT count(*) FROM representation_passport WHERE measurement_state='measured'),(SELECT count(*) FROM representation_basis),(SELECT count(*) FROM representation_binding),(SELECT count(*) FROM representation_exception),(SELECT count(*) FROM representation_body),(SELECT coalesce(sum(declared_bytes),0) FROM representation_body) FROM representation_passport";if(sqlite3_open_v2(dbp,&d,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK||sqlite3_prepare_v2(d,q,-1,&s,NULL)!=SQLITE_OK||sqlite3_step(s)!=SQLITE_ROW)goto done;sqlite3_int64 useful=sqlite3_column_int64(s,7),declared=sqlite3_column_int64(s,1),bodies=sqlite3_column_int64(s,13);printf("{\"passports\":%lld,\"bases\":%lld,\"typed_bindings\":%lld,\"sparse_exceptions\":%lld,\"declared_physical_bytes\":%lld,\"unique_bodies\":%lld,\"unique_body_declared_bytes\":%lld,\"declared_dedup_bytes_avoided\":%lld,\"runtime_receipts\":%lld,\"bytes_read\":%lld,\"bytes_borrowed\":%lld,\"bytes_copied\":%lld,\"bytes_hydrated\":%lld,\"useful_bytes\":%lld,\"measured_passports\":%lld,\"copy_amplification\":%.9g,\"hydration_amplification\":%.9g,\"warning\":\"declared passport costs are not executed measurements\"}\n",(long long)sqlite3_column_int64(s,0),(long long)sqlite3_column_int64(s,9),(long long)sqlite3_column_int64(s,10),(long long)sqlite3_column_int64(s,11),(long long)declared,(long long)sqlite3_column_int64(s,12),(long long)bodies,(long long)(declared-bodies),(long long)sqlite3_column_int64(s,2),(long long)sqlite3_column_int64(s,3),(long long)sqlite3_column_int64(s,4),(long long)sqlite3_column_int64(s,5),(long long)sqlite3_column_int64(s,6),(long long)useful,(long long)sqlite3_column_int64(s,8),useful?(double)sqlite3_column_int64(s,5)/useful:0.0,useful?(double)sqlite3_column_int64(s,6)/useful:0.0);rc=0;done:sqlite3_finalize(s);if(d)sqlite3_close(d);return rc;}

static int history(const char *dbp) {
    sqlite3 *d = NULL;
    sqlite3_stmt *s = NULL;
    int rc = 1;
    const char *q = "SELECT count(*),coalesce(sum(bytes_read),0),"
        "coalesce(sum(bytes_copied),0),coalesce(sum(bytes_hydrated),0),"
        "coalesce(sum(allocations),0),count(DISTINCT generation_digest) "
        "FROM compression_receipt_history";
    if (sqlite3_open_v2(dbp, &d, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(d, q, -1, &s, NULL) != SQLITE_OK ||
        sqlite3_step(s) != SQLITE_ROW) goto done;
    printf("{\"archived_receipts\":%lld,\"bytes_read\":%lld,"
           "\"bytes_copied\":%lld,\"bytes_hydrated\":%lld,"
           "\"allocations\":%lld,\"historical_generation_labels\":%lld,"
           "\"scope\":\"archived CopyReceipts, not active passports or authority\"}\n",
           (long long)sqlite3_column_int64(s, 0),
           (long long)sqlite3_column_int64(s, 1),
           (long long)sqlite3_column_int64(s, 2),
           (long long)sqlite3_column_int64(s, 3),
           (long long)sqlite3_column_int64(s, 4),
           (long long)sqlite3_column_int64(s, 5));
    rc = 0;
done:
    sqlite3_finalize(s);
    if (d) sqlite3_close(d);
    return rc;
}


/* Exactly one CopyReceipt, addressed by generation and ID. Past observations
 * survive across passport rebuilds without posing as current observations. */
static int physical_digest64(const char *v) {
    if (!v || strlen(v) != 64) return 0;
    for (int i=0;i<64;i++) if (!((v[i]>='0' && v[i]<='9') ||
                                  (v[i]>='a' && v[i]<='f'))) return 0;
    return 1;
}
static int physical_read_receipt(const char *dbp,const char *generation,const char *receipt_id) {
    sqlite3 *db=NULL;
    sqlite3_stmt *stmt=NULL;
    char *payload=NULL;
    int rc=1;
    if(!dbp || !physical_digest64(generation) || !receipt_id ||
       !receipt_id[0] || strlen(receipt_id)>512) return 1;
    const char *query=
      "WITH observations AS ("
      "SELECT 'active' origin,g.source_digest generation_digest,r.receipt_id,"
      "p.identity passport_identity,p.subject_family,p.semantic_digest,"
      "p.physical_digest,p.exactness_class,r.recorded_at,"
      "r.bytes_read,r.bytes_borrowed,r.bytes_copied,r.bytes_decompressed,"
      "r.bytes_reencoded,r.bytes_hydrated,r.allocations,r.serialize_count,"
      "r.deserialize_count,r.materialization_count,r.borrowed_boundaries,"
      "r.fused_boundaries,r.useful_bytes FROM compression_receipt r "
      "JOIN representation_passport p ON p.handle=r.passport_handle "
      "JOIN physical_generation g ON g.name='passport' "
      "WHERE g.source_digest=?1 AND r.receipt_id=?2 "
      "UNION ALL "
      "SELECT 'archived',h.generation_digest,h.receipt_id,h.passport_identity,"
      "h.subject_family,h.semantic_digest,h.physical_digest,h.exactness_class,"
      "h.recorded_at,h.bytes_read,h.bytes_borrowed,h.bytes_copied,"
      "h.bytes_decompressed,h.bytes_reencoded,h.bytes_hydrated,h.allocations,"
      "h.serialize_count,h.deserialize_count,h.materialization_count,"
      "h.borrowed_boundaries,h.fused_boundaries,h.useful_bytes "
      "FROM compression_receipt_history h "
      "WHERE h.generation_digest=?1 AND h.receipt_id=?2"
      ") SELECT json_object("
      "'schema','bonfyre.copyreceipt.readback.v1','origin',origin,"
      "'generation_digest',generation_digest,'receipt_id',receipt_id,"
      "'passport_identity',passport_identity,'subject_family',subject_family,"
      "'semantic_digest',semantic_digest,'physical_digest',physical_digest,"
      "'exactness_class',exactness_class,'recorded_at',recorded_at,"
      "'counters',json_object("
      "'bytes_read',bytes_read,'bytes_borrowed',bytes_borrowed,"
      "'bytes_copied',bytes_copied,'bytes_decompressed',bytes_decompressed,"
      "'bytes_reencoded',bytes_reencoded,'bytes_hydrated',bytes_hydrated,"
      "'allocations',allocations,'serialize_count',serialize_count,"
      "'deserialize_count',deserialize_count,'materialization_count',materialization_count,"
      "'borrowed_boundaries',borrowed_boundaries,'fused_boundaries',fused_boundaries,"
      "'useful_bytes',useful_bytes),"
      "'no_effect',json('true'),"
      "'cannot_infer','historical measurement is not current authorization or a universal speedup') "
      "FROM observations LIMIT 2";
    if(sqlite3_open_v2(dbp,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK ||
       sqlite3_prepare_v2(db,query,-1,&stmt,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(stmt,1,generation,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt,2,receipt_id,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(stmt)!=SQLITE_ROW)goto done;
    const unsigned char *value=sqlite3_column_text(stmt,0);
    if(!value)goto done;
    payload=sqlite3_mprintf("%s",value);
    if(!payload || sqlite3_step(stmt)!=SQLITE_DONE)goto done;
    if(printf("%s\n",payload)<0)goto done;
    rc=0;
done:
    sqlite3_free(payload);
    sqlite3_finalize(stmt);
    if(db)sqlite3_close(db);
    return rc;
}

/* What a registry costs to hold and to walk, measured on a registry that
 * exists. This used to insert passports it made up into a database in memory
 * and report their cost; a store nobody wrote says nothing about one somebody
 * did. It says nothing about users, traffic or retrieval quality either. */
static int structural_benchmark(const char *dbp) {
    sqlite3 *d=NULL; sqlite3_stmt *q=NULL; struct timespec a,b;
    long long passports=0,supports=0,bodies=0,physical=0,walked=0,page_count=0,page_size=0; int ok=0;
    if(!dbp||sqlite3_open_v2(dbp,&d,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){fprintf(stderr,"benchmark-structural: cannot read registry %s\n",dbp?dbp:"(none)");goto done;}
    if(sqlite3_prepare_v2(d,"SELECT (SELECT count(*) FROM representation_passport),(SELECT count(*) FROM representation_support),(SELECT count(*) FROM representation_body),(SELECT coalesce(sum(physical_bytes),0) FROM representation_passport)",-1,&q,NULL)!=SQLITE_OK||sqlite3_step(q)!=SQLITE_ROW){fprintf(stderr,"benchmark-structural: %s is not a physical registry\n",dbp);goto done;}
    passports=sqlite3_column_int64(q,0);supports=sqlite3_column_int64(q,1);bodies=sqlite3_column_int64(q,2);physical=sqlite3_column_int64(q,3);sqlite3_finalize(q);q=NULL;
    if(passports==0){fprintf(stderr,"benchmark-structural: %s holds no passports; there is nothing to measure\n",dbp);goto done;}
    if(sqlite3_prepare_v2(d,"SELECT p.rowid,s.operation FROM representation_passport p LEFT JOIN representation_support s ON s.passport_handle=p.rowid",-1,&q,NULL)!=SQLITE_OK)goto done;
    clock_gettime(CLOCK_MONOTONIC,&a);
    while(sqlite3_step(q)==SQLITE_ROW)walked++;
    clock_gettime(CLOCK_MONOTONIC,&b);sqlite3_finalize(q);q=NULL;
    if(sqlite3_prepare_v2(d,"PRAGMA page_count",-1,&q,NULL)!=SQLITE_OK||sqlite3_step(q)!=SQLITE_ROW)goto done;page_count=sqlite3_column_int64(q,0);sqlite3_finalize(q);q=NULL;
    if(sqlite3_prepare_v2(d,"PRAGMA page_size",-1,&q,NULL)!=SQLITE_OK||sqlite3_step(q)!=SQLITE_ROW)goto done;page_size=sqlite3_column_int64(q,0);
    long long elapsed=(long long)(b.tv_sec-a.tv_sec)*1000000000LL+(long long)(b.tv_nsec-a.tv_nsec);
    printf("{\"cohort\":\"recorded_registry\",\"registry\":\"%s\",\"passports\":%lld,\"supports\":%lld,\"bodies\":%lld,\"physical_bytes\":%lld,\"rows_walked\":%lld,\"walk_ns\":%lld,\"sqlite_bytes\":%lld,\"bytes_per_passport\":%.9g,\"scope\":\"this registry only; not traffic or adoption capacity\"}\n",dbp,passports,supports,bodies,physical,walked,elapsed,page_count*page_size,(double)(page_count*page_size)/(double)passports);
    ok=1;
done:sqlite3_finalize(q);if(d)sqlite3_close(d);return ok?0:1;
}
int bf_physical_command(int n,char**v){if(n<2)return -1;if(!strcmp(v[1],"compile-passports")){const char*d=arg(n,v,"--db"),*i=arg(n,v,"--input");return d&&i?compile_passports(d,i):1;}if(!strcmp(v[1],"plan")){const char*d=arg(n,v,"--db"),*f=arg(n,v,"--subject-family"),*o=arg(n,v,"--operation"),*l=arg(n,v,"--loss-budget"),*e=NULL;double x=l?strtod(l,(char**)&e):-1;return d&&f&&o&&l&&e&&!*e&&x>=0?plan(d,f,o,x,arg(n,v,"--requires-exact")!=NULL):1;}if(!strcmp(v[1],"record-receipt"))return receipt(n,v);if(!strcmp(v[1],"ledger")){const char*d=arg(n,v,"--db");return d?ledger(d):1;}if(!strcmp(v[1],"ledger-history")){const char*d=arg(n,v,"--db");return d?history(d):1;}if(!strcmp(v[1],"read-receipt"))return physical_read_receipt(arg(n,v,"--db"),arg(n,v,"--generation"),arg(n,v,"--receipt"));if(!strcmp(v[1],"benchmark-structural")){return structural_benchmark(arg(n,v,"--db"));}return -1;}
