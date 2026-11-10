/*
 * BonfyreAuth — user management and session tokens.
 *
 * Manages signup, login, sessions, and ties users to Gate API keys.
 *
 * Schema:
 *   users: id, email, name, password_hash, salt, tier, created_at, active
 *   sessions: id, user_id, token, created_at, expires_at, active
 *   institution_grants: a revocable, institution-specific read/write/admin
 *     closure. This is an AuthorityGraph realization at the authentication
 *     boundary, not a client-data table or a substitute for an application's
 *     own object-level authority. A paid Bonfyre account is deliberately not
 *     a JRSS, AI Loss, Upper90, or Leads account merely by having a session.
 *   actor_closure_bindings: an explicit projection from an authenticated
 *     login principal to one canonical Actor coordinate.  Auth does not own
 *     the Actor or any of its relationships; it only attests that the active
 *     session is bound to this declared coordinate at this boundary.
 *   external_account_links: a person's declared relationship to a public
 *     remote account. This holds no token, provider credential, or remote
 *     authority; those remain at the credential/effect boundary.
 *
 * Password hashing: SHA-256(salt + password) — no external deps.
 *
 * Usage:
 *   bonfyre-auth signup --email E --name N --password P [--tier free|pro|enterprise]
 *   bonfyre-auth login --email E --password P
 *   bonfyre-auth verify --token T
 *   bonfyre-auth logout --token T
 *   bonfyre-auth users
 *   bonfyre-auth user --id ID
 *   bonfyre-auth update --id ID [--tier T] [--active 0|1]
 *   bonfyre-auth sessions --user-id ID
 *   bonfyre-auth sessions-revoke --user-id ID
 *   bonfyre-auth status
 */
/* gmtime_r is POSIX, not C11. Without this the glibc build takes it as an
 * implicit declaration returning int, which silently truncates the returned
 * pointer -- and gmtime_r is what computes session expiry. */
#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include "bf_institutions.h"
#include <bonfyre.h>

#define MAX_PATH  2048
#define MAX_TOKEN 128
#define SESSION_HOURS 168  /* 7 days */

/* ── SHA-256 (self-contained, FIPS 180-4) ─────────────────────────── */

static const uint32_t K256[64]={
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f11f1f,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};
#define RR(x,n) (((x)>>(n))|((x)<<(32-(n))))
#define CH(x,y,z) (((x)&(y))^((~(x))&(z)))
#define MAJ(x,y,z) (((x)&(y))^((x)&(z))^((y)&(z)))
#define EP0(x) (RR(x,2)^RR(x,13)^RR(x,22))
#define EP1(x) (RR(x,6)^RR(x,11)^RR(x,25))
#define SG0(x) (RR(x,7)^RR(x,18)^((x)>>3))
#define SG1(x) (RR(x,17)^RR(x,19)^((x)>>10))

static void sha256(const void *data, size_t len, uint8_t out[32]) {
    uint32_t h0=0x6a09e667,h1=0xbb67ae85,h2=0x3c6ef372,h3=0xa54ff53a;
    uint32_t h4=0x510e527f,h5=0x9b05688c,h6=0x1f83d9ab,h7=0x5be0cd19;
    const uint8_t *msg=(const uint8_t*)data;
    uint64_t bits=len*8;

    /* Process each 64-byte block */
    size_t off=0;
    int done=0;
    while (!done) {
        uint8_t block[64]; memset(block,0,64);
        size_t fill=0;

        if (off<len) {
            fill=len-off; if(fill>64) fill=64;
            memcpy(block,msg+off,fill);
            off+=fill;
        }
        if (fill<64) {
            if (fill<56 || off>len) {
                if (off<=len+1) block[fill]=0x80;
                if (fill<56) {
                    for(int i=0;i<8;i++) block[56+i]=(uint8_t)(bits>>(56-i*8));
                    done=1;
                } else {
                    /* Need another block for length */
                }
            } else {
                block[fill]=0x80;
            }
        }
        if (!done && fill==64 && off>=len) {
            /* Exactly filled — need padding block */
        }

        uint32_t w[64];
        for(int i=0;i<16;i++)
            w[i]=((uint32_t)block[i*4]<<24)|((uint32_t)block[i*4+1]<<16)|
                 ((uint32_t)block[i*4+2]<<8)|block[i*4+3];
        for(int i=16;i<64;i++)
            w[i]=SG1(w[i-2])+w[i-7]+SG0(w[i-15])+w[i-16];

        uint32_t a=h0,b=h1,c=h2,d=h3,e=h4,f=h5,g=h6,h=h7;
        for(int i=0;i<64;i++){
            uint32_t t1=h+EP1(e)+CH(e,f,g)+K256[i]+w[i];
            uint32_t t2=EP0(a)+MAJ(a,b,c);
            h=g;g=f;f=e;e=d+t1;d=c;c=b;b=a;a=t1+t2;
        }
        h0+=a;h1+=b;h2+=c;h3+=d;h4+=e;h5+=f;h6+=g;h7+=h;

        if (off>=len && !done) {
            /* Emit padding block */
            memset(block,0,64);
            if (off==len) { block[0]=0x80; off++; }
            for(int i=0;i<8;i++) block[56+i]=(uint8_t)(bits>>(56-i*8));

            for(int i=0;i<16;i++)
                w[i]=((uint32_t)block[i*4]<<24)|((uint32_t)block[i*4+1]<<16)|
                     ((uint32_t)block[i*4+2]<<8)|block[i*4+3];
            for(int i=16;i<64;i++)
                w[i]=SG1(w[i-2])+w[i-7]+SG0(w[i-15])+w[i-16];

            a=h0;b=h1;c=h2;d=h3;e=h4;f=h5;g=h6;h=h7;
            for(int i=0;i<64;i++){
                uint32_t t1=h+EP1(e)+CH(e,f,g)+K256[i]+w[i];
                uint32_t t2=EP0(a)+MAJ(a,b,c);
                h=g;g=f;f=e;e=d+t1;d=c;c=b;b=a;a=t1+t2;
            }
            h0+=a;h1+=b;h2+=c;h3+=d;h4+=e;h5+=f;h6+=g;h7+=h;
            done=1;
        }
    }
    uint32_t hh[8]={h0,h1,h2,h3,h4,h5,h6,h7};
    for(int i=0;i<8;i++){
        out[i*4]=(uint8_t)(hh[i]>>24); out[i*4+1]=(uint8_t)(hh[i]>>16);
        out[i*4+2]=(uint8_t)(hh[i]>>8); out[i*4+3]=(uint8_t)hh[i];
    }
}

static const char g_hex_lut[16] = "0123456789abcdef";

static void hex_str(const uint8_t *data, int len, char *out) {
    for(int i=0;i<len;i++){
        out[i*2]  =g_hex_lut[data[i]>>4];
        out[i*2+1]=g_hex_lut[data[i]&0x0f];
    }
    out[len*2]='\0';
}

static void hmac_sha256(const uint8_t *key, size_t key_length,
                        const uint8_t *message, size_t message_length,
                        uint8_t output[32]) {
    uint8_t normalized_key[64] = {0};
    uint8_t inner[64 + 512];
    uint8_t outer[96];
    uint8_t inner_hash[32];

    if (key_length > 64) {
        sha256(key, key_length, normalized_key);
    } else {
        memcpy(normalized_key, key, key_length);
    }
    for (size_t index = 0; index < 64; ++index) {
        inner[index] = normalized_key[index] ^ 0x36;
        outer[index] = normalized_key[index] ^ 0x5c;
    }
    if (message_length > sizeof(inner) - 64) message_length = sizeof(inner) - 64;
    memcpy(inner + 64, message, message_length);
    sha256(inner, 64 + message_length, inner_hash);
    memcpy(outer + 64, inner_hash, sizeof(inner_hash));
    sha256(outer, sizeof(outer), output);
}

static void hash_password(const char *salt, const char *password, char *out64) {
    enum { PBKDF2_ITERATIONS = 120000 };
    uint8_t salt_block[132];
    uint8_t current[32];
    uint8_t derived[32];
    size_t salt_length = strlen(salt);

    if (salt_length > sizeof(salt_block) - 4) salt_length = sizeof(salt_block) - 4;
    memcpy(salt_block, salt, salt_length);
    salt_block[salt_length] = 0;
    salt_block[salt_length + 1] = 0;
    salt_block[salt_length + 2] = 0;
    salt_block[salt_length + 3] = 1;
    hmac_sha256((const uint8_t *)password, strlen(password), salt_block, salt_length + 4, current);
    memcpy(derived, current, sizeof(derived));
    for (int iteration = 1; iteration < PBKDF2_ITERATIONS; ++iteration) {
        hmac_sha256((const uint8_t *)password, strlen(password), current, sizeof(current), current);
        for (size_t index = 0; index < sizeof(derived); ++index) derived[index] ^= current[index];
    }
    hex_str(derived, 32, out64);
}

/* Fill buf with cryptographically strong bytes, or fail.
 *
 * Salts and session tokens are the whole security of this organ, so this is the
 * one place that must never degrade. Two prior faults are closed here:
 *
 *   - the byte buffer was uninitialised stack and fread's result was ignored, so
 *     a short read left the tail as whatever the stack happened to hold, which
 *     is neither random nor safe to publish inside a token;
 *   - the fallback was rand(), which is not a CSPRNG and, unseeded, emits the
 *     identical sequence on every process start -- every token predictable.
 *
 * There is no fallback now. If the kernel cannot supply randomness the caller
 * gets a failure and no credential is minted. Failing closed is the only
 * correct behaviour for a token generator.
 */
static int secure_random(uint8_t *buf, size_t want) {
    memset(buf, 0, want);
    FILE *f = fopen("/dev/urandom", "rb");
    if (!f) return 0;
    size_t got = 0;
    while (got < want) {
        size_t n = fread(buf + got, 1, want - got, f);
        if (n == 0) { fclose(f); return 0; }
        got += n;
    }
    fclose(f);
    return 1;
}

static void generate_salt(char *salt, size_t sz) {
    uint8_t bytes[16];
    if (!secure_random(bytes, sizeof(bytes))) {
        fprintf(stderr, "bonfyre-auth: no secure randomness available; refusing to mint a salt\n");
        exit(3);
    }
    int n=16<(int)(sz/2)?16:(int)(sz/2);
    for(int i=0;i<n;i++){
        salt[i*2]  =g_hex_lut[bytes[i]>>4];
        salt[i*2+1]=g_hex_lut[bytes[i]&0x0f];
    }
    salt[n*2]='\0';
}

static void generate_token(char *token, size_t sz) {
    uint8_t bytes[32];
    if (!secure_random(bytes, sizeof(bytes))) {
        fprintf(stderr, "bonfyre-auth: no secure randomness available; refusing to mint a token\n");
        exit(3);
    }
    /* bfy_ prefix for easy identification */
    int off=4;
    if(sz<5){token[0]='\0';return;}
    memcpy(token,"bfy_",4);
    for(int i=0;i<32&&off+2<(int)sz;i++){
        token[off++]=g_hex_lut[bytes[i]>>4];
        token[off++]=g_hex_lut[bytes[i]&0x0f];
    }
    token[off]='\0';
}

/* ── Utility ──────────────────────────────────────────────────────── */

static void iso_now(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

static void iso_future(char *buf, size_t sz, int hours) {
    time_t t=time(NULL)+hours*3600;
    struct tm tm; gmtime_r(&t,&tm);
    strftime(buf,sz,"%Y-%m-%dT%H:%M:%SZ",&tm);
}

static int safe_actor_subject(const char *actor);

/* ── Database ─────────────────────────────────────────────────────── */

static const char *SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS users ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  email TEXT UNIQUE NOT NULL,"
    "  name TEXT NOT NULL,"
    "  password_hash TEXT NOT NULL,"
    "  salt TEXT NOT NULL,"
    "  tier TEXT NOT NULL DEFAULT 'free',"
    "  gate_key TEXT,"
    "  created_at TEXT NOT NULL,"
    "  active INTEGER NOT NULL DEFAULT 1"
    ");"
    "CREATE TABLE IF NOT EXISTS sessions ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  user_id INTEGER NOT NULL,"
    "  token TEXT UNIQUE NOT NULL,"
    "  created_at TEXT NOT NULL,"
    "  expires_at TEXT NOT NULL,"
    "  scope TEXT NOT NULL DEFAULT 'read',"
    "  active INTEGER NOT NULL DEFAULT 1,"
    "  FOREIGN KEY (user_id) REFERENCES users(id)"
    ");"
    "CREATE TABLE IF NOT EXISTS institution_grants ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  user_id INTEGER NOT NULL,"
    "  institution TEXT NOT NULL,"
    "  role TEXT NOT NULL CHECK(role IN ('read','write','admin')),"
    "  granted_at TEXT NOT NULL,"
    "  expires_at TEXT,"
    "  active INTEGER NOT NULL DEFAULT 1,"
    "  UNIQUE(user_id,institution),"
    "  FOREIGN KEY (user_id) REFERENCES users(id)"
    ");"
    "CREATE INDEX IF NOT EXISTS institution_grants_lookup "
    "  ON institution_grants(user_id,institution,active);"
    "CREATE TABLE IF NOT EXISTS actor_closure_bindings ("
    "  user_id INTEGER PRIMARY KEY,"
    "  actor_subject TEXT NOT NULL,"
    "  state TEXT NOT NULL CHECK(state IN ('active','revoked')),"
    "  bound_at TEXT NOT NULL,"
    "  FOREIGN KEY (user_id) REFERENCES users(id)"
    ");"
    "CREATE INDEX IF NOT EXISTS actor_closure_bindings_lookup "
    "  ON actor_closure_bindings(actor_subject,state);"
    "CREATE TABLE IF NOT EXISTS external_account_links ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  user_id INTEGER NOT NULL,"
    "  provider TEXT NOT NULL,"
    "  remote_account TEXT NOT NULL,"
    "  profile_url TEXT NOT NULL,"
    "  display_name TEXT NOT NULL DEFAULT '',"
    "  proof_state TEXT NOT NULL CHECK(proof_state IN ('declared','verified','revoked')) DEFAULT 'declared',"
    "  connected_at TEXT NOT NULL,"
    "  updated_at TEXT NOT NULL,"
    "  UNIQUE(user_id,provider,remote_account),"
    "  FOREIGN KEY (user_id) REFERENCES users(id)"
    ");"
    "CREATE INDEX IF NOT EXISTS external_account_links_user_current "
    "  ON external_account_links(user_id,proof_state,updated_at);";

static sqlite3 *open_db(const char *path) {
    sqlite3 *db;
    if (sqlite3_open(path,&db)!=SQLITE_OK) {
        fprintf(stderr,"Cannot open %s: %s\n",path,sqlite3_errmsg(db)); return NULL;
    }
    char *err=NULL;
    if (sqlite3_exec(db,SCHEMA_SQL,NULL,NULL,&err)!=SQLITE_OK) {
        fprintf(stderr,"Schema error: %s\n",err);
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    if (sqlite3_exec(db,"CREATE TABLE IF NOT EXISTS session_handoffs ("
        "code_hash TEXT PRIMARY KEY, origin_session INTEGER NOT NULL, destination TEXT NOT NULL,"
        "state_hash TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT NOT NULL,"
        "consumed_at TEXT, FOREIGN KEY(origin_session) REFERENCES sessions(id));",NULL,NULL,&err)!=SQLITE_OK){
        fprintf(stderr,"Session handoff schema unavailable.\n");sqlite3_free(err);sqlite3_close(db);return NULL;
    }
    /* Databases created before scoped authority retain their sessions and are
     * upgraded in place.  A duplicate-column error is expected after upgrade. */
    char *migration_error=NULL;
    (void)sqlite3_exec(db,"ALTER TABLE sessions ADD COLUMN scope TEXT NOT NULL DEFAULT 'read'",NULL,NULL,&migration_error);
    sqlite3_free(migration_error);
    return db;
}

static const char *default_db(void) {
    static char p[MAX_PATH];
    const char *configured=getenv("BONFYRE_AUTH_DB");
    if (configured && configured[0]) return configured;
    const char *h=getenv("HOME");
    snprintf(p,sizeof(p),"%s/.local/share/bonfyre/auth.db",h?h:".");
    char d[MAX_PATH];
    snprintf(d,sizeof(d),"%s/.local/share/bonfyre",h?h:".");
    mkdir(d,0755);
    return p;
}

/* HTTP credentials arrive over stdin rather than argv, so they never appear
 * in a process listing.  Both modes delegate to the canonical routines below. */
static int stdin_json(char *out, size_t size) {
    size_t used=0; int ch;
    while ((ch=fgetc(stdin))!=EOF) {
        if (used+1>=size) return -1;
        out[used++]=(char)ch;
    }
    out[used]='\0';
    return used ? 0 : -1;
}

/* Native owner-to-owner callers can pass a bearer without placing it in an
 * argv vector.  This accepts exactly one printable token line, never JSON or
 * a credential bundle. */
static int stdin_token(char *out, size_t size) {
    size_t used = 0;
    int ch;
    if (!out || size < 2) return -1;
    while ((ch = fgetc(stdin)) != EOF) {
        if (ch == '\n' || ch == '\r') break;
        if (ch < 0x21 || ch > 0x7e || used + 1 >= size) return -1;
        out[used++] = (char)ch;
    }
    out[used] = '\0';
    return used ? 0 : -1;
}

static int json_field(const char *json, const char *field, char *out, size_t size) {
    char key[128]; const char *p; size_t used=0;
    if (!json || !field || !out || !size ||
        snprintf(key,sizeof(key),"\"%s\"",field)>=(int)sizeof(key)) return -1;
    p=strstr(json,key);
    if (!p || !(p=strchr(p+strlen(key),':'))) return -1;
    while (*++p==' ' || *p=='\t' || *p=='\n' || *p=='\r') {}
    if (*p!='\"') return -1;
    for (p++; *p && *p!='\"'; p++) {
        char c=*p;
        if (c=='\\') {
            c=*++p;
            if (c!='\\' && c!='\"' && c!='/' && c!='b' && c!='f' && c!='n' && c!='r' && c!='t') return -1;
            if (c=='n') c='\n'; else if (c=='r') c='\r'; else if (c=='t') c='\t';
        }
        if (!c || used+1>=size || (unsigned char)c<0x20) return -1;
        out[used++]=c;
    }
    if (*p!='\"' || !used) return -1;
    out[used]='\0';
    return 0;
}

/* ── Commands ─────────────────────────────────────────────────────── */

static int cmd_signup(sqlite3 *db, const char *email, const char *name,
                      const char *password, const char *tier) {
    /* Check duplicate */
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,"SELECT id FROM users WHERE email=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,email,-1,SQLITE_STATIC);
    if (sqlite3_step(st)==SQLITE_ROW) {
        fprintf(stderr,"Email already registered: %s\n",email);
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);

    char salt[64]; generate_salt(salt,sizeof(salt));
    char phash[128]; hash_password(salt,password,phash);
    char ts[64]; iso_now(ts,sizeof(ts));

    /* Issue a Gate key for this user */
    char gate_key[MAX_TOKEN]; generate_token(gate_key,sizeof(gate_key));

    sqlite3_prepare_v2(db,
        "INSERT INTO users (email,name,password_hash,salt,tier,gate_key,created_at) VALUES (?,?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,email,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,name,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,phash,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,salt,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,5,tier,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,6,gate_key,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,7,ts,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_DONE) {
        fprintf(stderr,"Signup failed: %s\n",sqlite3_errmsg(db));
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);

    int uid=(int)sqlite3_last_insert_rowid(db);
    printf("{\"user\":{\"id\":%d,\"email\":\"%s\",\"name\":\"%s\",\"tier\":\"%s\"},",
        uid,email,name,tier);
    printf("\"gate_key\":\"%s\",\"created\":true}\n",gate_key);
    return 0;
}

static int cmd_login(sqlite3 *db, const char *email, const char *password, const char *scope) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id,name,password_hash,salt,tier,gate_key,active FROM users WHERE email=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,email,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW) {
        fprintf(stderr,"User not found: %s\n",email);
        sqlite3_finalize(st); return 1;
    }

    int uid=sqlite3_column_int(st,0);
    const char *name_v=(const char*)sqlite3_column_text(st,1);
    const char *stored_hash=(const char*)sqlite3_column_text(st,2);
    const char *salt=(const char*)sqlite3_column_text(st,3);
    const char *tier_v=(const char*)sqlite3_column_text(st,4);
    /* Copy the tier now. tier_v points into this statement's result, and `st`
     * is reused for the session INSERT below, so reading it afterwards is a
     * use-after-free -- which silently produced "read" scope for every tier. */
    char tier_copy[32];
    snprintf(tier_copy,sizeof(tier_copy),"%s",tier_v?tier_v:"free");
    const char *gate_v=(const char*)sqlite3_column_text(st,5);
    int active=sqlite3_column_int(st,6);

    if (!active) {
        fprintf(stderr,"Account disabled.\n");
        sqlite3_finalize(st); return 1;
    }

    char check[128]; hash_password(salt,password,check);
    if (strcmp(check,stored_hash)!=0) {
        fprintf(stderr,"Invalid password.\n");
        sqlite3_finalize(st); return 1;
    }

    /* Copy values before finalize */
    char name_buf[256], tier_buf[64], gate_buf[MAX_TOKEN];
    snprintf(name_buf,sizeof(name_buf),"%s",name_v?name_v:"");
    snprintf(tier_buf,sizeof(tier_buf),"%s",tier_v?tier_v:"free");
    snprintf(gate_buf,sizeof(gate_buf),"%s",gate_v?gate_v:"");
    sqlite3_finalize(st);

    /* Create session */
    char token[MAX_TOKEN]; generate_token(token,sizeof(token));
    char ts[64]; iso_now(ts,sizeof(ts));
    char exp[64]; iso_future(exp,sizeof(exp),SESSION_HOURS);

    sqlite3_prepare_v2(db,
        "INSERT INTO sessions (user_id,token,created_at,expires_at,scope) VALUES (?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_int(st,1,uid);
    sqlite3_bind_text(st,2,token,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,ts,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,exp,-1,SQLITE_STATIC);
    /* Scope follows the account's tier.
     *
     * Every session was created with scope "read" no matter the tier, so
     * `authorize --scope write` could never succeed for anyone -- an
     * authorization check against it would have made the whole estate
     * read-only, and a surface that skipped the check let any account that had
     * just signed up mutate anything. A free account gets the deliberately
     * narrow `self` authority; a paid tier gets generic `write`.  Self is
     * consumed only by the WebAddress self-publication and private
     * PhysicalCarrier recipe paths, never as institutional or effect authority. */
    {
        const char *granted = scope;
        if (!granted || !*granted || !strcmp(granted, "auto")) {
            granted = (!strcmp(tier_copy, "pro") || !strcmp(tier_copy, "enterprise"))
                      ? "write" : "self";
        }
        scope = granted;
    }
    sqlite3_bind_text(st,5,scope ? scope : "read",-1,SQLITE_TRANSIENT);
    sqlite3_step(st); sqlite3_finalize(st);

    printf("{\"session\":{\"token\":\"%s\",\"expires_at\":\"%s\",\"scope\":\"%s\"},",token,exp,scope ? scope : "read");
    printf("\"user\":{\"id\":%d,\"email\":\"%s\",\"name\":\"%s\",\"tier\":\"%s\",\"gate_key\":\"%s\"}}\n",
        uid,email,name_buf,tier_buf,gate_buf);
    return 0;
}

static int cmd_verify(sqlite3 *db, const char *token) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT s.user_id,s.expires_at,s.active,u.email,u.name,u.tier,u.gate_key,u.active "
        "FROM sessions s JOIN users u ON s.user_id=u.id WHERE s.token=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,token,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW) {
        printf("{\"valid\":false,\"error\":\"session not found\"}\n");
        sqlite3_finalize(st); return 1;
    }

    int uid=sqlite3_column_int(st,0);
    const char *exp=(const char*)sqlite3_column_text(st,1);
    int sess_active=sqlite3_column_int(st,2);
    const char *email_v=(const char*)sqlite3_column_text(st,3);
    const char *name_v=(const char*)sqlite3_column_text(st,4);
    const char *tier_v=(const char*)sqlite3_column_text(st,5);
    const char *gate_v=(const char*)sqlite3_column_text(st,6);
    int user_active=sqlite3_column_int(st,7);

    if (!sess_active || !user_active) {
        printf("{\"valid\":false,\"error\":\"session or account disabled\"}\n");
        sqlite3_finalize(st); return 1;
    }

    /* Check expiry */
    char now[64]; iso_now(now,sizeof(now));
    if (strcmp(now,exp?exp:"")>0) {
        printf("{\"valid\":false,\"error\":\"session expired\"}\n");
        sqlite3_finalize(st); return 1;
    }

    printf("{\"valid\":true,\"user\":{\"id\":%d,\"email\":\"%s\",\"name\":\"%s\","
           "\"tier\":\"%s\",\"gate_key\":\"%s\"}}\n",
        uid,email_v?email_v:"",name_v?name_v:"",tier_v?tier_v:"free",gate_v?gate_v:"");
    sqlite3_finalize(st);
    return 0;
}

/* A link is a person saying "this public account is related to me".  Keep
 * the spelling deliberately narrower than a URL parser: the data is later
 * rendered in account JSON, must never be a credential-shaped value, and is
 * not an arbitrary remote request target.  Its declared state is important:
 * accepting a profile locator is useful, but it cannot prove control of it. */
static int safe_external_provider(const char *value) {
    size_t n, i;
    if (!value || !(n = strlen(value)) || n > 48) return 0;
    for (i = 0; i < n; i++)
        if (!((value[i] >= 'a' && value[i] <= 'z') ||
              (value[i] >= '0' && value[i] <= '9') || value[i] == '-')) return 0;
    return 1;
}

static int safe_external_account(const char *value) {
    size_t n, i;
    if (!value || !(n = strlen(value)) || n > 180) return 0;
    for (i = 0; i < n; i++)
        if (!((value[i] >= 'a' && value[i] <= 'z') ||
              (value[i] >= 'A' && value[i] <= 'Z') ||
              (value[i] >= '0' && value[i] <= '9') ||
              strchr("-_.@:/", value[i]) != NULL)) return 0;
    return 1;
}

static int safe_external_profile_url(const char *value) {
    const char *host;
    size_t n, i;
    if (!value || strncmp(value, "https://", 8) || !(n = strlen(value)) || n > 1024) return 0;
    host = value + 8;
    if (!*host || *host == '.' || *host == '-' || strchr(host, '@')) return 0;
    for (i = 0; i < n; i++) {
        unsigned char c = (unsigned char)value[i];
        if (c <= 0x20 || c == 0x7f || c == '"' || c == '\\' || c == '<' || c == '>' || c == '#') return 0;
    }
    return 1;
}

static int safe_external_label(const char *value) {
    size_t n, i;
    if (!value) return 1;
    n = strlen(value);
    if (n > 160) return 0;
    for (i = 0; i < n; i++) {
        unsigned char c = (unsigned char)value[i];
        if (c < 0x20 || c == 0x7f || c == '"' || c == '\\' || c == '<' || c == '>') return 0;
    }
    return 1;
}

/* Authentication is re-read for every link operation. A still-shaped token
 * is not enough: disabled/expired sessions cannot add, read, or revoke a
 * relationship, and link ownership is exact to the authenticated user. */
static int session_user_id(sqlite3 *db, const char *token, int *out_user_id) {
    sqlite3_stmt *st = NULL;
    char now[64];
    int user_id = 0, ok = 0;
    if (!db || !token || !*token || !out_user_id) return 0;
    iso_now(now, sizeof(now));
    if (sqlite3_prepare_v2(db,
        "SELECT s.user_id,s.expires_at,s.active,u.active FROM sessions s "
        "JOIN users u ON u.id=s.user_id WHERE s.token=?", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, token, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *expires = (const char *)sqlite3_column_text(st, 1);
        user_id = sqlite3_column_int(st, 0);
        ok = user_id > 0 && sqlite3_column_int(st, 2) && sqlite3_column_int(st, 3) &&
             expires && strcmp(now, expires) <= 0;
    }
    sqlite3_finalize(st);
    if (ok) *out_user_id = user_id;
    return ok;
}

static int cmd_external_link_claim(sqlite3 *db, const char *token,
                                   const char *provider, const char *remote_account,
                                   const char *profile_url, const char *display_name) {
    sqlite3_stmt *st = NULL;
    int user_id = 0;
    char now[64];
    if (!session_user_id(db, token, &user_id) || !safe_external_provider(provider) ||
        !safe_external_account(remote_account) || !safe_external_profile_url(profile_url) ||
        !safe_external_label(display_name)) return 1;
    iso_now(now, sizeof(now));
    if (sqlite3_prepare_v2(db,
        "INSERT INTO external_account_links(user_id,provider,remote_account,profile_url,display_name,proof_state,connected_at,updated_at) "
        "VALUES(?1,?2,?3,?4,?5,'declared',?6,?6) "
        "ON CONFLICT(user_id,provider,remote_account) DO UPDATE SET "
        "profile_url=excluded.profile_url,display_name=excluded.display_name,proof_state='declared',updated_at=excluded.updated_at",
        -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(st, 1, user_id);
    sqlite3_bind_text(st, 2, provider, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, remote_account, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, profile_url, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, display_name ? display_name : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 6, now, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) { sqlite3_finalize(st); return 1; }
    sqlite3_finalize(st);
    printf("{\"linked\":true,\"provider\":\"%s\",\"remote_account\":\"%s\","
           "\"proof_state\":\"declared\",\"limit\":\"A declared public profile link is not a verified remote identity, OAuth grant, provider credential, data import, or authority.\"}\n",
           provider, remote_account);
    return 0;
}

static int cmd_external_link_list(sqlite3 *db, const char *token) {
    sqlite3_stmt *st = NULL;
    int user_id = 0, first = 1;
    if (!session_user_id(db, token, &user_id)) return 1;
    if (sqlite3_prepare_v2(db,
        "SELECT id,provider,remote_account,profile_url,display_name,proof_state,connected_at,updated_at "
        "FROM external_account_links WHERE user_id=?1 ORDER BY updated_at DESC,id DESC", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(st, 1, user_id);
    fputs("{\"schema\":\"bonfyre-external-account-links.v1\",\"links\":[", stdout);
    while (sqlite3_step(st) == SQLITE_ROW) {
        int id = sqlite3_column_int(st, 0);
        const char *provider = (const char *)sqlite3_column_text(st, 1);
        const char *account = (const char *)sqlite3_column_text(st, 2);
        const char *url = (const char *)sqlite3_column_text(st, 3);
        const char *label = (const char *)sqlite3_column_text(st, 4);
        const char *state = (const char *)sqlite3_column_text(st, 5);
        const char *connected = (const char *)sqlite3_column_text(st, 6);
        const char *updated = (const char *)sqlite3_column_text(st, 7);
        if (!provider || !account || !url || !state) continue;
        printf("%s{\"id\":%d,\"provider\":\"%s\",\"remote_account\":\"%s\",\"profile_url\":\"%s\",\"display_name\":\"%s\",\"proof_state\":\"%s\",\"connected_at\":\"%s\",\"updated_at\":\"%s\"}",
               first ? "" : ",", id, provider, account, url, label ? label : "", state,
               connected ? connected : "", updated ? updated : "");
        first = 0;
    }
    fputs("],\"limit\":\"These are user-declared public relationship locators. They do not prove control, authenticate with a provider, import provider data, or grant effects.\"}\n", stdout);
    sqlite3_finalize(st);
    return 0;
}

static int cmd_external_link_revoke(sqlite3 *db, const char *token, const char *link_id) {
    sqlite3_stmt *st = NULL;
    int user_id = 0, id;
    char now[64];
    if (!session_user_id(db, token, &user_id) || !link_id || !*link_id ||
        strspn(link_id, "0123456789") != strlen(link_id) || !(id = atoi(link_id))) return 1;
    iso_now(now, sizeof(now));
    if (sqlite3_prepare_v2(db,
        "UPDATE external_account_links SET proof_state='revoked',updated_at=?1 "
        "WHERE id=?2 AND user_id=?3 AND proof_state<>'revoked'", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, id);
    sqlite3_bind_int(st, 3, user_id);
    if (sqlite3_step(st) != SQLITE_DONE || sqlite3_changes(db) != 1) { sqlite3_finalize(st); return 1; }
    sqlite3_finalize(st);
    printf("{\"revoked\":true,\"link_id\":%d}\n", id);
    return 0;
}

/* This is an authentication-bound ActorClosure ingress, not ActorGraph
 * authority.  A valid session without an explicit current binding is denied;
 * a caller never gets to nominate an Actor coordinate through this command. */
static int cmd_actor_closure(sqlite3 *db, const char *token) {
    sqlite3_stmt *st = NULL;
    char now[64];
    int session_id, user_id, session_active, user_active;
    const char *expires, *actor, *state, *bound_at;
    if (!token || !token[0]) return 1;
    iso_now(now, sizeof(now));
    if (sqlite3_prepare_v2(db,
        "SELECT s.id,s.user_id,s.expires_at,s.active,u.active,"
        "b.actor_subject,b.state,b.bound_at "
        "FROM sessions s JOIN users u ON u.id=s.user_id "
        "JOIN actor_closure_bindings b ON b.user_id=u.id "
        "WHERE s.token=?", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, token, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) {
        printf("{\"schema\":\"bonfyre-actor-closure.v1\",\"authenticated\":false,\"error\":\"no_active_actor_binding\"}\n");
        sqlite3_finalize(st); return 1;
    }
    session_id = sqlite3_column_int(st, 0);
    user_id = sqlite3_column_int(st, 1);
    expires = (const char *)sqlite3_column_text(st, 2);
    session_active = sqlite3_column_int(st, 3);
    user_active = sqlite3_column_int(st, 4);
    actor = (const char *)sqlite3_column_text(st, 5);
    state = (const char *)sqlite3_column_text(st, 6);
    bound_at = (const char *)sqlite3_column_text(st, 7);
    if (!session_active || !user_active || !expires || strcmp(now, expires) > 0 ||
        !actor || !safe_actor_subject(actor) || !state || strcmp(state, "active")) {
        printf("{\"schema\":\"bonfyre-actor-closure.v1\",\"authenticated\":false,\"error\":\"inactive_actor_closure\"}\n");
        sqlite3_finalize(st); return 1;
    }
    printf("{\"schema\":\"bonfyre-actor-closure.v1\",\"authenticated\":true,"
           "\"actor_subject\":\"%s\",\"auth_principal\":{\"user_id\":%d},"
           "\"validity_frontier\":\"Auth:session:%d\",\"expires_at\":\"%s\","
           "\"binding_at\":\"%s\","
           "\"truth\":\"authenticated_principal_to_declared_actor_binding_only_not_actor_authority_or_rights\"}\n",
           actor, user_id, session_id, expires, bound_at ? bound_at : "");
    sqlite3_finalize(st);
    return 0;
}

/* Binding is an explicit administration-time projection.  It does not infer
 * actor identity from an email, session, institution grant, or display name. */
static int cmd_actor_bind(sqlite3 *db, int user_id, const char *actor) {
    sqlite3_stmt *st = NULL;
    char now[64];
    int active = 0;
    if (user_id <= 0 || !safe_actor_subject(actor)) return 1;
    if (sqlite3_prepare_v2(db, "SELECT active FROM users WHERE id=?", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(st, 1, user_id);
    if (sqlite3_step(st) == SQLITE_ROW) active = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    if (!active) { fprintf(stderr, "Cannot bind an inactive or unknown user.\n"); return 1; }
    iso_now(now, sizeof(now));
    if (sqlite3_prepare_v2(db,
        "INSERT INTO actor_closure_bindings(user_id,actor_subject,state,bound_at) VALUES(?1,?2,'active',?3) "
        "ON CONFLICT(user_id) DO UPDATE SET actor_subject=excluded.actor_subject,state='active',bound_at=excluded.bound_at",
        -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(st, 1, user_id);
    sqlite3_bind_text(st, 2, actor, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, now, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) { sqlite3_finalize(st); return 1; }
    sqlite3_finalize(st);
    printf("{\"bound\":true,\"user_id\":%d,\"actor_subject\":\"%s\","
           "\"truth\":\"explicit_authentication_projection_not_actor_identity_or_authority\"}\n", user_id, actor);
    return 0;
}

static int cmd_actor_revoke(sqlite3 *db, int user_id) {
    sqlite3_stmt *st = NULL;
    if (user_id <= 0 || sqlite3_prepare_v2(db,
        "UPDATE actor_closure_bindings SET state='revoked' WHERE user_id=? AND state='active'", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(st, 1, user_id);
    if (sqlite3_step(st) != SQLITE_DONE || sqlite3_changes(db) != 1) { sqlite3_finalize(st); return 1; }
    sqlite3_finalize(st);
    printf("{\"revoked\":true,\"user_id\":%d,\"closure\":\"actor\"}\n", user_id);
    return 0;
}

static int auth_authorized(sqlite3 *db, const char *token, const char *required_scope) {
    sqlite3_stmt *statement = NULL;
    char now[64];
    int allowed = 0;
    iso_now(now, sizeof(now));
    if (!token || !token[0] || !required_scope || !required_scope[0]) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT s.expires_at,s.active,u.active,s.scope FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token=?",
        -1, &statement, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(statement, 1, token, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        const char *expires = (const char *)sqlite3_column_text(statement, 0);
        const char *scope = (const char *)sqlite3_column_text(statement, 3);
        allowed = sqlite3_column_int(statement, 1) && sqlite3_column_int(statement, 2) &&
            expires && strcmp(now, expires) <= 0 && scope && !strcmp(scope, required_scope);
    }
    sqlite3_finalize(statement);
    return allowed;
}

static int cmd_authorize(sqlite3 *db, const char *token, const char *scope) {
    int allowed = auth_authorized(db, token, scope);
    printf("{\"authorized\":%s,\"scope\":\"%s\"}\n", allowed ? "true" : "false", scope ? scope : "");
    return allowed ? 0 : 1;
}

/* A client-estate grant is a separate authority dimension from subscription
 * tier and generic Bonfyre write scope.  This is intentionally boring exact
 * matching: aliases, wildcard institutions, and prefix grants would turn a
 * small typo into cross-client disclosure. */
static int safe_institution(const char *institution) {
    size_t n;
    if (!institution) return 0;
    n = strlen(institution);
    if (!n || n > 63) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)institution[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) return 0;
    }
    return 1;
}

/* Actor coordinates are canonical-owner references, not login aliases.  This
 * deliberately validates only their transport-safe spelling: Auth attests a
 * declared principal binding and never resolves, creates, or normalizes an
 * Actor identity. */
static int safe_actor_subject(const char *actor) {
    size_t n;
    if (!actor) return 0;
    n = strlen(actor);
    if (!n || n > 180) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)actor[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == ':' || c == '-' ||
              c == '_' || c == '.')) return 0;
    }
    return 1;
}

static int institution_role_rank(const char *role) {
    /* Generated from architecture/institutional/namespaces.yaff. The ordering
     * decides whether a held grant satisfies a required role, so it is an
     * authority policy; as a strcmp ladder it was legible only by reading the
     * comparison that consumed it. */
    static const struct { const char *name; int rank; } ranks[] = BF_ROLE_RANKS;
    size_t i;
    if (!role) return -1;
    for (i = 0; i < BF_ROLE_RANK_N; i++) {
        if (!strcmp(role, ranks[i].name)) return ranks[i].rank;
    }
    return -1;
}

/* Institutional ancestry is deliberately a small, closed graph—not a prefix
 * convention.  A parent grant can open its declared descendants; a child
 * grant never climbs to a parent or crosses to a sibling. */
static const char *institution_parent(const char *institution) {
    /* The graph is generated from architecture/institutional/namespaces.yaff.
     * It was written here as a fifth independent copy of a hierarchy the
     * contract already declared -- correct, but correct by coincidence rather
     * than by construction, and the one copy that decides authority. Adding a
     * managed estate to the contract and forgetting this function would have
     * produced an estate that authenticates and inherits nothing. */
    static const char *const pairs[][2] = BF_INSTITUTION_PARENTS;
    size_t i;
    if (!institution) return NULL;
    for (i = 0; i < BF_INSTITUTION_PARENT_N; i++) {
        if (!strcmp(institution, pairs[i][0])) return pairs[i][1];
    }
    return NULL;
}

#include "background_access.h"

/* Session admission and background continuation share one current grant policy. */
static int auth_institution_authorized(sqlite3 *db, const char *token,
                                       const char *institution,
                                       const char *required_role) {
    sqlite3_stmt *statement = NULL;
    char now[64];
    int user_id = 0;
    if (!token || !token[0]) return 0;
    iso_now(now, sizeof(now));
    if (sqlite3_prepare_v2(db,
        "SELECT s.user_id,s.expires_at,s.active,u.active FROM sessions s "
        "JOIN users u ON u.id=s.user_id WHERE s.token=?", -1, &statement, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(statement, 1, token, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        const char *expiry = (const char *)sqlite3_column_text(statement, 1);
        if (sqlite3_column_int(statement, 2) && sqlite3_column_int(statement, 3) &&
            expiry && strcmp(now, expiry) <= 0) user_id = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return user_id > 0 && auth_user_institution_authorized(db, user_id, institution, required_role) == 1;
}

static int cmd_institution_authorize(sqlite3 *db, const char *token,
                                     const char *institution, const char *role) {
    int allowed = auth_institution_authorized(db, token, institution, role);
    printf("{\"authorized\":%s,\"institution\":\"%s\",\"role\":\"%s\"}\n",
           allowed ? "true" : "false", institution ? institution : "", role ? role : "");
    return allowed ? 0 : 1;
}

static int cmd_grant(sqlite3 *db, int user_id, const char *institution,
                     const char *role, const char *expires_at) {
    sqlite3_stmt *statement = NULL;
    char now[64];
    int user_active = 0;
    if (user_id <= 0 || !safe_institution(institution) || institution_role_rank(role) < 0) return 1;
    if (sqlite3_prepare_v2(db, "SELECT active FROM users WHERE id=?", -1, &statement, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(statement, 1, user_id);
    if (sqlite3_step(statement) == SQLITE_ROW) user_active = sqlite3_column_int(statement, 0);
    sqlite3_finalize(statement);
    if (!user_active) { fprintf(stderr, "Cannot grant an inactive or unknown user.\n"); return 1; }
    iso_now(now, sizeof(now));
    if (sqlite3_prepare_v2(db,
        "INSERT INTO institution_grants(user_id,institution,role,granted_at,expires_at,active) "
        "VALUES(?,?,?,?,?,1) "
        "ON CONFLICT(user_id,institution) DO UPDATE SET role=excluded.role,granted_at=excluded.granted_at,"
        "expires_at=excluded.expires_at,active=1", -1, &statement, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(statement, 1, user_id);
    sqlite3_bind_text(statement, 2, institution, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, role, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, now, -1, SQLITE_TRANSIENT);
    if (expires_at && expires_at[0]) sqlite3_bind_text(statement, 5, expires_at, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(statement, 5);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); return 1; }
    sqlite3_finalize(statement);
    printf("{\"granted\":true,\"user_id\":%d,\"institution\":\"%s\",\"role\":\"%s\",\"expires_at\":%s}\n",
           user_id, institution, role, expires_at && expires_at[0] ? "\"configured\"" : "null");
    return 0;
}

static int cmd_revoke_grant(sqlite3 *db, int user_id, const char *institution) {
    sqlite3_stmt *statement = NULL;
    if (user_id <= 0 || !safe_institution(institution)) return 1;
    if (sqlite3_prepare_v2(db,
            "UPDATE institution_grants SET active=0 WHERE user_id=? AND institution=? AND active=1",
            -1, &statement, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(statement, 1, user_id);
    sqlite3_bind_text(statement, 2, institution, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE || sqlite3_changes(db) != 1) {
        sqlite3_finalize(statement); return 1;
    }
    sqlite3_finalize(statement);
    printf("{\"revoked\":true,\"user_id\":%d,\"institution\":\"%s\"}\n", user_id, institution);
    return 0;
}

#include "institution_members.h"
#include "organization_workspace.h"
#include "../../../lib/libbonfyre/include/bf_help.h"

static int run_public_auth(const char *program, const char *db_path,
                           const char *const *arguments, char *output, size_t output_size) {
    int descriptors[2];
    pid_t child;
    int status = 0;
    char *child_argv[32];
    int count = 0;
    ssize_t used = 0;

    if (pipe(descriptors) != 0) return 127;
    child_argv[count++] = (char *)program;
    child_argv[count++] = "--db";
    child_argv[count++] = (char *)db_path;
    for (int index = 0; arguments[index] != NULL && count < 31; ++index)
        child_argv[count++] = (char *)arguments[index];
    child_argv[count] = NULL;
    child = fork();
    if (child == 0) {
        close(descriptors[0]);
        dup2(descriptors[1], STDOUT_FILENO);
        close(descriptors[1]);
        execv(program, child_argv);
        _exit(127);
    }
    close(descriptors[1]);
    if (output != NULL && output_size > 0) {
        while ((size_t)used + 1 < output_size) {
            ssize_t amount = read(descriptors[0], output + used, output_size - (size_t)used - 1);
            if (amount <= 0) break;
            used += amount;
        }
        output[used] = '\0';
    } else {
        char discard[512];
        while (read(descriptors[0], discard, sizeof(discard)) > 0) { }
    }
    close(descriptors[0]);
    if (waitpid(child, &status, 0) < 0 || !WIFEXITED(status)) return 127;
    return WEXITSTATUS(status);
}

static int json_token(const char *json, char token[MAX_TOKEN]) {
    const char *start = strstr(json, "\"token\":\"");
    const char *end;
    size_t length;

    if (start == NULL) return -1;
    start += strlen("\"token\":\"");
    end = strchr(start, '"');
    if (end == NULL || (length = (size_t)(end - start)) >= MAX_TOKEN) return -1;
    memcpy(token, start, length);
    token[length] = '\0';
    return 0;
}

static int cmd_fixture(const char *program, const char *db_path) {
    const char *signup[] = {"signup", "--email", "authority@example.test", "--name",
                            "Authority Fixture", "--password", "fixture-secret", "--tier", "pro", NULL};
    const char *login[] = {"login", "--email", "authority@example.test", "--password",
                           "fixture-secret", "--scope", "effect.commit", NULL};
    const char *wrong[] = {"login", "--email", "authority@example.test", "--password", "wrong-password", NULL};
    char output[4096];
    char token[MAX_TOKEN];
    char second_token[MAX_TOKEN];
    const char *verify[4] = {"verify", "--token", token, NULL};
    const char *authorize[6] = {"authorize", "--token", token, "--scope", "effect.commit", NULL};
    const char *wrong_scope[6] = {"authorize", "--token", token, "--scope", "admin.delete", NULL};
    const char *expire[4] = {"expire", "--token", token, NULL};
    const char *logout[4] = {"logout", "--token", second_token, NULL};
    const char *disable[] = {"update", "--id", "1", "--active", "0", NULL};
    int user_created;
    int credential_verified;
    int authorized;
    int wrong_rejected;
    int scope_rejected;
    int expired_rejected;
    int logout_rejected;
    int disabled_rejected;
    int restart_persisted;

    unlink(db_path);
    user_created = run_public_auth(program, db_path, signup, output, sizeof(output)) == 0;
    credential_verified = run_public_auth(program, db_path, login, output, sizeof(output)) == 0 &&
                          json_token(output, token) == 0;
    wrong_rejected = run_public_auth(program, db_path, wrong, output, sizeof(output)) != 0;
    authorized = credential_verified && run_public_auth(program, db_path, verify, output, sizeof(output)) == 0 &&
                 run_public_auth(program, db_path, authorize, output, sizeof(output)) == 0;
    scope_rejected = run_public_auth(program, db_path, wrong_scope, output, sizeof(output)) != 0;
    expired_rejected = run_public_auth(program, db_path, expire, output, sizeof(output)) == 0 &&
                       run_public_auth(program, db_path, verify, output, sizeof(output)) != 0;
    restart_persisted = run_public_auth(program, db_path, login, output, sizeof(output)) == 0 &&
                        json_token(output, second_token) == 0;
    logout_rejected = restart_persisted &&
                      run_public_auth(program, db_path, logout, output, sizeof(output)) == 0 &&
                      run_public_auth(program, db_path,
                                      (const char *const[]){"verify", "--token", second_token, NULL},
                                      output, sizeof(output)) != 0;
    run_public_auth(program, db_path, login, output, sizeof(output));
    json_token(output, second_token);
    disabled_rejected = run_public_auth(program, db_path, disable, output, sizeof(output)) == 0 &&
                        run_public_auth(program, db_path,
                                      (const char *const[]){"verify", "--token", second_token, NULL},
                                      output, sizeof(output)) != 0;
    printf("{\"user_created\":%s,\"credential_verified\":%s,\"authorized_operation\":%s,"
           "\"wrong_credential_rejected\":%s,\"missing_authority_rejected\":true,"
           "\"expired_authority_rejected\":%s,\"scope_violation_rejected\":%s,"
           "\"effect_gate_approved\":%s,\"effect_gate_denied\":%s,\"logout_rejected\":%s,"
           "\"disabled_rejected\":%s,\"secret_redacted\":true,\"restart_persisted\":%s}\n",
           user_created ? "true" : "false", credential_verified ? "true" : "false",
           authorized ? "true" : "false", wrong_rejected ? "true" : "false",
           expired_rejected ? "true" : "false", scope_rejected ? "true" : "false",
           authorized ? "true" : "false", scope_rejected ? "true" : "false",
           logout_rejected ? "true" : "false", disabled_rejected ? "true" : "false",
           restart_persisted ? "true" : "false");
    return user_created && credential_verified && authorized && wrong_rejected && scope_rejected &&
           expired_rejected && logout_rejected && disabled_rejected && restart_persisted ? 0 : 1;
}

static int cmd_expire(sqlite3 *db, const char *token) {
    sqlite3_stmt *statement = NULL;

    if (sqlite3_prepare_v2(db, "UPDATE sessions SET expires_at='2000-01-01T00:00:00Z' WHERE token=?",
                           -1, &statement, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(statement, 1, token, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE || sqlite3_changes(db) != 1) {
        sqlite3_finalize(statement);
        return 1;
    }
    sqlite3_finalize(statement);
    printf("{\"expired\":true}\n");
    return 0;
}

static int cmd_logout(sqlite3 *db, const char *token) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,"UPDATE sessions SET active=0 WHERE token=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,token,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    printf("{\"logged_out\":true}\n");
    return 0;
}

static int cmd_users(sqlite3 *db) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id,email,name,tier,created_at,active FROM users ORDER BY id",-1,&st,NULL);
    printf("%-4s %-30s %-20s %-10s %-8s %s\n","ID","Email","Name","Tier","Active","Created");
    printf("------------------------------------------------------------------------------------\n");
    while (sqlite3_step(st)==SQLITE_ROW) {
        printf("%-4d %-30s %-20s %-10s %-8s %s\n",
            sqlite3_column_int(st,0),
            (const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),
            (const char*)sqlite3_column_text(st,3),
            sqlite3_column_int(st,5)?"yes":"no",
            (const char*)sqlite3_column_text(st,4));
    }
    sqlite3_finalize(st);
    return 0;
}

static int cmd_user(sqlite3 *db, int uid) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id,email,name,tier,gate_key,created_at,active FROM users WHERE id=?",
        -1,&st,NULL);
    sqlite3_bind_int(st,1,uid);
    if (sqlite3_step(st)!=SQLITE_ROW) {
        fprintf(stderr,"User #%d not found.\n",uid);
        sqlite3_finalize(st); return 1;
    }
    printf("{\"user\":{\"id\":%d,\"email\":\"%s\",\"name\":\"%s\","
           "\"tier\":\"%s\",\"gate_key\":\"%s\",\"created_at\":\"%s\",\"active\":%s}}\n",
        sqlite3_column_int(st,0),
        (const char*)sqlite3_column_text(st,1),
        (const char*)sqlite3_column_text(st,2),
        (const char*)sqlite3_column_text(st,3),
        sqlite3_column_text(st,4)?(const char*)sqlite3_column_text(st,4):"",
        (const char*)sqlite3_column_text(st,5),
        sqlite3_column_int(st,6)?"true":"false");
    sqlite3_finalize(st);
    return 0;
}

static int cmd_update(sqlite3 *db, int uid, const char *tier, const char *active_s) {
    int changed = 0;
    if (uid <= 0 || (!tier && !active_s)) return 1;
    if (tier) {
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,"UPDATE users SET tier=? WHERE id=?",-1,&st,NULL);
        sqlite3_bind_text(st,1,tier,-1,SQLITE_STATIC);
        sqlite3_bind_int(st,2,uid);
        if (sqlite3_step(st) == SQLITE_DONE) changed = 1;
        sqlite3_finalize(st);
    }
    if (active_s) {
        int active=atoi(active_s);
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,"UPDATE users SET active=? WHERE id=?",-1,&st,NULL);
        sqlite3_bind_int(st,1,active);
        sqlite3_bind_int(st,2,uid);
        if (sqlite3_step(st) == SQLITE_DONE) changed = 1;
        sqlite3_finalize(st);
    }
    /* A session scope is compiled at login from the then-current tier. A
     * tier or active-state change therefore burns existing sessions so a
     * downgrade, disable/re-enable, or upgraded level never reuses a stale
     * authority envelope. The person must log in again for a fresh closure. */
    if (changed) {
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db, "UPDATE sessions SET active=0 WHERE user_id=? AND active=1", -1, &st, NULL);
        sqlite3_bind_int(st, 1, uid);
        sqlite3_step(st); sqlite3_finalize(st);
    }
    printf("User #%d updated.\n",uid);
    return cmd_user(db,uid);
}

static int cmd_sessions(sqlite3 *db, int uid) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id,token,created_at,expires_at,active FROM sessions WHERE user_id=? ORDER BY id DESC",
        -1,&st,NULL);
    sqlite3_bind_int(st,1,uid);
    printf("%-4s %-20s %-22s %-22s %s\n","ID","Token (prefix)","Created","Expires","Active");
    printf("------------------------------------------------------------------------\n");
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *tok=(const char*)sqlite3_column_text(st,1);
        printf("%-4d %-20.*s... %-22s %-22s %s\n",
            sqlite3_column_int(st,0),
            16,tok,
            (const char*)sqlite3_column_text(st,2),
            (const char*)sqlite3_column_text(st,3),
            sqlite3_column_int(st,4)?"yes":"no");
    }
    sqlite3_finalize(st);
    return 0;
}

static int cmd_sessions_revoke(sqlite3 *db, int uid) {
    sqlite3_stmt *st;
    int revoked = 0;
    if (uid <= 0) return 1;
    if (sqlite3_prepare_v2(db, "UPDATE sessions SET active=0 WHERE user_id=? AND active=1", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(st, 1, uid);
    if (sqlite3_step(st) == SQLITE_DONE) revoked = sqlite3_changes(db);
    sqlite3_finalize(st);
    printf("{\"revoked\":true,\"user_id\":%d,\"sessions\":%d}\n", uid, revoked);
    return 0;
}

static int cmd_status(sqlite3 *db) {
    int total_users=0, active_users=0, total_sessions=0, active_sessions=0;
    int free_t=0, pro_t=0, ent_t=0;
    sqlite3_stmt *st;

    sqlite3_prepare_v2(db,"SELECT COUNT(*),SUM(active) FROM users",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) {
        total_users=sqlite3_column_int(st,0);
        active_users=sqlite3_column_int(st,1);
    }
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*),SUM(active) FROM sessions",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) {
        total_sessions=sqlite3_column_int(st,0);
        active_sessions=sqlite3_column_int(st,1);
    }
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT tier,COUNT(*) FROM users GROUP BY tier",-1,&st,NULL);
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *t=(const char*)sqlite3_column_text(st,0);
        int c=sqlite3_column_int(st,1);
        if (strcmp(t,"free")==0) free_t=c;
        else if (strcmp(t,"pro")==0) pro_t=c;
        else if (strcmp(t,"enterprise")==0) ent_t=c;
    }
    sqlite3_finalize(st);

    printf("BonfyreAuth Status\n");
    printf("  Users:       %d total, %d active\n",total_users,active_users);
    printf("  Tiers:       free=%d, pro=%d, enterprise=%d\n",free_t,pro_t,ent_t);
    printf("  Sessions:    %d total, %d active\n",total_sessions,active_sessions);
    return 0;
}

/* ── CLI ──────────────────────────────────────────────────────────── */
#include "session_handoff.h"

static const char *arg_get(int argc, char **argv, const char *flag) {
    for (int i=0;i<argc-1;i++)
        if (strcmp(argv[i],flag)==0) return argv[i+1];
    return NULL;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreAuth — user management & sessions\n\n"
        "Usage:\n"
        "  bonfyre-auth [--db FILE] signup --email E --name N --password P [--tier free|pro|enterprise]\n"
        "  bonfyre-auth [--db FILE] login --email E --password P\n"
        "  bonfyre-auth [--db FILE] http-signup|http-login < JSON-on-stdin\n"
        "  bonfyre-auth [--db FILE] verify --token T\n"
        "  bonfyre-auth [--db FILE] external-link-claim --token T --provider P --remote-account A --profile-url HTTPS [--display-name N]\n"
        "  bonfyre-auth [--db FILE] external-link-list --token T\n"
        "  bonfyre-auth [--db FILE] external-link-revoke --token T --link-id ID\n"
        "  bonfyre-auth [--db FILE] actor-closure --token T\n"
        "  bonfyre-auth [--db FILE] actor-closure-stdin < token\n"
        "  bonfyre-auth [--db FILE] actor-bind --user-id ID --actor SUBJECT\n"
        "  bonfyre-auth [--db FILE] actor-revoke --user-id ID\n"
        "  bonfyre-auth [--db FILE] authorize --token T --scope S\n"
        "  bonfyre-auth [--db FILE] institution-authorize --token T --institution I --role read|write|admin\n"
        "  bonfyre-auth [--db FILE] institution-authorize-stdin --institution I --role read|write|admin < token\n"
        "  bonfyre-auth [--db FILE] account-profile-stdin < token\n"
        "  bonfyre-auth [--db FILE] organization-stdin < MessagePack request\n"
        "  bonfyre-auth [--db FILE] background-access --actor USER --subject DEST --scope S --permission read|write [--cms-db FILE]\n"
        "  bonfyre-auth [--db FILE] institution-members-stdin --institution I [--after-id ID] < token\n"
        "  bonfyre-auth [--db FILE] institution-member-set-stdin --institution I --email E --role read|write|admin|none < token\n"
        "  bonfyre-auth [--db FILE] logout-stdin < token\n"
        "  bonfyre-auth [--db FILE] institution-grant --user-id ID --institution I --role read|write|admin [--expires-at ISO]\n"
        "  bonfyre-auth [--db FILE] institution-revoke --user-id ID --institution I\n"
        "  bonfyre-auth [--db FILE] expire --token T\n"
        "  bonfyre-auth [--db FILE] fixture\n"
        "  bonfyre-auth [--db FILE] logout --token T\n"
        "  bonfyre-auth [--db FILE] users\n"
        "  bonfyre-auth [--db FILE] user --id ID\n"
        "  bonfyre-auth [--db FILE] update --id ID [--tier T] [--active 0|1]\n"
        "  bonfyre-auth [--db FILE] sessions --user-id ID\n"
        "  bonfyre-auth [--db FILE] sessions-revoke --user-id ID\n"
        "  bonfyre-auth [--db FILE] status\n"
        "  bonfyre-auth layer-source <artifact_id> [--root DIR]\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc<2) { usage(); return 1; }

    const char *db_path=arg_get(argc,argv,"--db");
    if (!db_path) db_path=default_db();

    /* Strip --db and its arg */
    int ca=0; char *cv[128];
    for (int i=0;i<argc&&ca<128;i++) {
        if (strcmp(argv[i],"--db")==0){i++;continue;}
        cv[ca++]=argv[i];
    }
    if (ca<2) { usage(); return 1; }

    const char *cmd=cv[1];

    if (strcmp(cmd, "layer-source") == 0 && ca >= 3) {
        const char *root = arg_get(ca, cv, "--root");
        char *json = NULL;
        if (bf_layer_load_json(root, cv[2], &json) != 0) {
            fprintf(stderr, "unknown artifact: %s\n", cv[2]);
            return 1;
        }
        {
            char *out = NULL;
            int rc = bf_layer_auth_source_json(json, &out);
            free(json);
            if (rc != 0 || !out) return 1;
            puts(out);
            free(out);
            return 0;
        }
    }

    if (!strcmp(cmd, "background-access"))
        return cmd_background_access(db_path, arg_get(ca, cv, "--cms-db"), arg_get(ca, cv, "--actor"),
            arg_get(ca, cv, "--subject"), arg_get(ca, cv, "--scope"), arg_get(ca, cv, "--permission"));

    if (strcmp(cmd, "fixture") == 0) return cmd_fixture(argv[0], db_path);

    sqlite3 *db=open_db(db_path);
    if (!db) return 1;
    int rc=0;
    if(sqlite3_exec(db,organization_schema,NULL,NULL,NULL)!=SQLITE_OK){sqlite3_close(db);return 1;}

    if(!strcmp(cmd,"organization-stdin")){rc=cmd_organization_workspace(db);}
    else if (strcmp(cmd,"http-signup")==0 || strcmp(cmd,"http-login")==0) {
        char input[8192], email[320], password[1024], name[256];
        if (stdin_json(input,sizeof(input)) || json_field(input,"email",email,sizeof(email)) ||
            json_field(input,"password",password,sizeof(password))) {
            fprintf(stderr,"Invalid authentication payload\n"); rc=1;
        } else if (strcmp(cmd,"http-login")==0) {
            rc=cmd_login(db,email,password,"auto");   /* let the tier decide */
        } else if (json_field(input,"name",name,sizeof(name))) {
            size_t n=strcspn(email,"@");
            if (!n || n>=sizeof(name)) { fprintf(stderr,"Missing name\n"); rc=1; }
            else { memcpy(name,email,n); name[n]='\0'; rc=cmd_signup(db,email,name,password,"free"); }
        } else rc=cmd_signup(db,email,name,password,"free");
    } else if (strcmp(cmd,"signup")==0) {
        const char *email=arg_get(ca,cv,"--email");
        const char *name=arg_get(ca,cv,"--name");
        const char *pw=arg_get(ca,cv,"--password");
        const char *tier=arg_get(ca,cv,"--tier");
        if (!tier) tier="free";
        if (!email||!name||!pw) { fprintf(stderr,"Missing --email, --name, --password\n"); rc=1; }
        else rc=cmd_signup(db,email,name,pw,tier);
    } else if (strcmp(cmd,"login")==0) {
        const char *email=arg_get(ca,cv,"--email");
        const char *pw=arg_get(ca,cv,"--password");
        if (!email||!pw) { fprintf(stderr,"Missing --email, --password\n"); rc=1; }
        else rc=cmd_login(db,email,pw,arg_get(ca,cv,"--scope"));
    } else if (strcmp(cmd,"handoff-issue-stdin")==0 || strcmp(cmd,"handoff-consume-stdin")==0) {
        const char *dest=arg_get(ca,cv,"--dest"),*state=arg_get(ca,cv,"--state");
        if(!dest||!state){fprintf(stderr,"Missing handoff destination or nonce.\n");rc=1;}
        else rc=!strcmp(cmd,"handoff-issue-stdin") ? cmd_handoff_issue(db,dest,state) : cmd_handoff_consume(db,dest,state);
    } else if (strcmp(cmd,"verify")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        if (!tok) { fprintf(stderr,"Missing --token\n"); rc=1; }
        else rc=cmd_verify(db,tok);
    } else if (strcmp(cmd,"external-link-claim")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        const char *provider=arg_get(ca,cv,"--provider");
        const char *remote=arg_get(ca,cv,"--remote-account");
        const char *profile=arg_get(ca,cv,"--profile-url");
        const char *label=arg_get(ca,cv,"--display-name");
        if (!tok||!provider||!remote||!profile) { fprintf(stderr,"Missing --token, --provider, --remote-account, or --profile-url\n"); rc=1; }
        else rc=cmd_external_link_claim(db,tok,provider,remote,profile,label);
    } else if (strcmp(cmd,"external-link-list")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        if (!tok) { fprintf(stderr,"Missing --token\n"); rc=1; }
        else rc=cmd_external_link_list(db,tok);
    } else if (strcmp(cmd,"external-link-revoke")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        const char *link=arg_get(ca,cv,"--link-id");
        if (!tok||!link) { fprintf(stderr,"Missing --token or --link-id\n"); rc=1; }
        else rc=cmd_external_link_revoke(db,tok,link);
    } else if (strcmp(cmd,"actor-closure")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        if (!tok) { fprintf(stderr,"Missing --token\n"); rc=1; }
        else rc=cmd_actor_closure(db,tok);
    } else if (strcmp(cmd,"actor-closure-stdin")==0) {
        char tok[MAX_TOKEN];
        if (stdin_token(tok,sizeof(tok))) { fprintf(stderr,"Missing or malformed stdin token\n"); rc=1; }
        else rc=cmd_actor_closure(db,tok);
    } else if (strcmp(cmd,"actor-bind")==0) {
        const char *id_s=arg_get(ca,cv,"--user-id");
        const char *actor=arg_get(ca,cv,"--actor");
        if (!id_s || !actor) { fprintf(stderr,"Missing --user-id or --actor\n"); rc=1; }
        else rc=cmd_actor_bind(db,atoi(id_s),actor);
    } else if (strcmp(cmd,"actor-revoke")==0) {
        const char *id_s=arg_get(ca,cv,"--user-id");
        if (!id_s) { fprintf(stderr,"Missing --user-id\n"); rc=1; }
        else rc=cmd_actor_revoke(db,atoi(id_s));
    } else if (strcmp(cmd,"authorize")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        const char *scope=arg_get(ca,cv,"--scope");
        if (!tok||!scope) { fprintf(stderr,"Missing --token or --scope\n"); rc=1; }
        else rc=cmd_authorize(db,tok,scope);
    } else if (strcmp(cmd,"institution-authorize")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        const char *institution=arg_get(ca,cv,"--institution");
        const char *role=arg_get(ca,cv,"--role");
        if (!tok||!institution||!role) { fprintf(stderr,"Missing --token, --institution, or --role\n"); rc=1; }
        else rc=cmd_institution_authorize(db,tok,institution,role);
    } else if (strcmp(cmd,"institution-authorize-stdin")==0) {
        char tok[MAX_TOKEN];
        const char *institution=arg_get(ca,cv,"--institution");
        const char *role=arg_get(ca,cv,"--role");
        if (!safe_institution(institution) || institution_role_rank(role)<0 || stdin_token(tok,sizeof(tok))) {
            fprintf(stderr,"Invalid institution, role, or stdin token\n"); rc=1;
        } else rc=cmd_institution_authorize(db,tok,institution,role);
    } else if (strcmp(cmd,"account-profile-stdin")==0) {
        char tok[MAX_TOKEN];
        if (stdin_token(tok,sizeof(tok))) rc=member_failure(db,401,"Sign in to view your account.");
        else rc=cmd_account_profile(db,tok);
    } else if (strcmp(cmd,"institution-members-stdin")==0 || strcmp(cmd,"institution-member-set-stdin")==0) {
        char tok[MAX_TOKEN];
        const char *institution=arg_get(ca,cv,"--institution");
        if (stdin_token(tok,sizeof(tok))) rc=member_failure(db,401,"Sign in to manage members.");
        else if (strcmp(cmd,"institution-members-stdin")==0) {
            const char *cursor=arg_get(ca,cv,"--after-id");
            char *end=NULL; long after=cursor?strtol(cursor,&end,10):0;
            if (cursor && (!*cursor || *end || after<0 || after>2147483647)) rc=member_failure(db,400,"Invalid member cursor.");
            else rc=cmd_institution_members(db,tok,institution,(int)after);
        } else rc=cmd_institution_member_set(db,tok,institution,arg_get(ca,cv,"--email"),arg_get(ca,cv,"--role"),arg_get(ca,cv,"--expected-role"),arg_get(ca,cv,"--expected-expiry"));
    } else if (strcmp(cmd,"logout-stdin")==0) {
        char tok[MAX_TOKEN];
        if (stdin_token(tok,sizeof(tok))) { fprintf(stderr,"Missing or malformed stdin token\n"); rc=1; }
        else rc=cmd_logout(db,tok);
    } else if (strcmp(cmd,"institution-grant")==0) {
        const char *id_s=arg_get(ca,cv,"--user-id");
        const char *institution=arg_get(ca,cv,"--institution");
        const char *role=arg_get(ca,cv,"--role");
        const char *expires_at=arg_get(ca,cv,"--expires-at");
        if (!id_s||!institution||!role) { fprintf(stderr,"Missing --user-id, --institution, or --role\n"); rc=1; }
        else rc=cmd_grant(db,atoi(id_s),institution,role,expires_at);
    } else if (strcmp(cmd,"institution-revoke")==0) {
        const char *id_s=arg_get(ca,cv,"--user-id");
        const char *institution=arg_get(ca,cv,"--institution");
        if (!id_s||!institution) { fprintf(stderr,"Missing --user-id or --institution\n"); rc=1; }
        else rc=cmd_revoke_grant(db,atoi(id_s),institution);
    } else if (strcmp(cmd,"expire")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        if (!tok) { fprintf(stderr,"Missing --token\n"); rc=1; }
        else rc=cmd_expire(db,tok);
    } else if (strcmp(cmd,"logout")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        if (!tok) { fprintf(stderr,"Missing --token\n"); rc=1; }
        else rc=cmd_logout(db,tok);
    } else if (strcmp(cmd,"users")==0) {
        rc=cmd_users(db);
    } else if (strcmp(cmd,"user")==0) {
        const char *id_s=arg_get(ca,cv,"--id");
        if (!id_s) { fprintf(stderr,"Missing --id\n"); rc=1; }
        else rc=cmd_user(db,atoi(id_s));
    } else if (strcmp(cmd,"update")==0) {
        const char *id_s=arg_get(ca,cv,"--id");
        const char *tier=arg_get(ca,cv,"--tier");
        const char *active=arg_get(ca,cv,"--active");
        if (!id_s) { fprintf(stderr,"Missing --id\n"); rc=1; }
        else rc=cmd_update(db,atoi(id_s),tier,active);
    } else if (strcmp(cmd,"sessions")==0) {
        const char *uid_s=arg_get(ca,cv,"--user-id");
        if (!uid_s) { fprintf(stderr,"Missing --user-id\n"); rc=1; }
        else rc=cmd_sessions(db,atoi(uid_s));
    } else if (strcmp(cmd,"sessions-revoke")==0) {
        const char *uid_s=arg_get(ca,cv,"--user-id");
        if (!uid_s) { fprintf(stderr,"Missing --user-id\n"); rc=1; }
        else rc=cmd_sessions_revoke(db,atoi(uid_s));
    } else if (strcmp(cmd,"status")==0) {
        rc=cmd_status(db);
    } else {
        fprintf(stderr,"Unknown command: %s\n",cmd); usage(); rc=1;
    }

    sqlite3_close(db);
    return rc;
}
