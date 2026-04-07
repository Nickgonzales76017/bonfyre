/*
 * BonfyreAuth — user management and session tokens.
 *
 * Manages signup, login, sessions, and ties users to Gate API keys.
 *
 * Schema:
 *   users: id, email, name, password_hash, salt, tier, created_at, active
 *   sessions: id, user_id, token, created_at, expires_at, active
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
 *   bonfyre-auth status
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <time.h>
#include <sqlite3.h>

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

static void hash_password(const char *salt, const char *password, char *out64) {
    char combined[512];
    snprintf(combined,sizeof(combined),"%s:%s",salt,password);
    uint8_t hash[32];
    sha256(combined,strlen(combined),hash);
    hex_str(hash,32,out64);
}

static void generate_salt(char *salt, size_t sz) {
    FILE *f=fopen("/dev/urandom","rb");
    uint8_t bytes[16];
    if (f) { fread(bytes,1,sizeof(bytes),f); fclose(f); }
    else { for(int i=0;i<16;i++) bytes[i]=(uint8_t)(rand()&0xff); }
    int n=16<(int)(sz/2)?16:(int)(sz/2);
    for(int i=0;i<n;i++){
        salt[i*2]  =g_hex_lut[bytes[i]>>4];
        salt[i*2+1]=g_hex_lut[bytes[i]&0x0f];
    }
    salt[n*2]='\0';
}

static void generate_token(char *token, size_t sz) {
    FILE *f=fopen("/dev/urandom","rb");
    uint8_t bytes[32];
    if (f) { fread(bytes,1,sizeof(bytes),f); fclose(f); }
    else { for(int i=0;i<32;i++) bytes[i]=(uint8_t)(rand()&0xff); }
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

static void iso_now(char *buf, size_t sz) {
    time_t t=time(NULL); struct tm tm; gmtime_r(&t,&tm);
    strftime(buf,sz,"%Y-%m-%dT%H:%M:%SZ",&tm);
}

static void iso_future(char *buf, size_t sz, int hours) {
    time_t t=time(NULL)+hours*3600;
    struct tm tm; gmtime_r(&t,&tm);
    strftime(buf,sz,"%Y-%m-%dT%H:%M:%SZ",&tm);
}

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
    "  active INTEGER NOT NULL DEFAULT 1,"
    "  FOREIGN KEY (user_id) REFERENCES users(id)"
    ");";

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
    return db;
}

static const char *default_db(void) {
    static char p[MAX_PATH];
    const char *h=getenv("HOME");
    snprintf(p,sizeof(p),"%s/.local/share/bonfyre/auth.db",h?h:".");
    char d[MAX_PATH];
    snprintf(d,sizeof(d),"%s/.local/share/bonfyre",h?h:".");
    mkdir(d,0755);
    return p;
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

static int cmd_login(sqlite3 *db, const char *email, const char *password) {
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
        "INSERT INTO sessions (user_id,token,created_at,expires_at) VALUES (?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_int(st,1,uid);
    sqlite3_bind_text(st,2,token,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,ts,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,exp,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    printf("{\"session\":{\"token\":\"%s\",\"expires_at\":\"%s\"},",token,exp);
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
    if (tier) {
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,"UPDATE users SET tier=? WHERE id=?",-1,&st,NULL);
        sqlite3_bind_text(st,1,tier,-1,SQLITE_STATIC);
        sqlite3_bind_int(st,2,uid);
        sqlite3_step(st); sqlite3_finalize(st);
    }
    if (active_s) {
        int active=atoi(active_s);
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,"UPDATE users SET active=? WHERE id=?",-1,&st,NULL);
        sqlite3_bind_int(st,1,active);
        sqlite3_bind_int(st,2,uid);
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
        "  bonfyre-auth [--db FILE] verify --token T\n"
        "  bonfyre-auth [--db FILE] logout --token T\n"
        "  bonfyre-auth [--db FILE] users\n"
        "  bonfyre-auth [--db FILE] user --id ID\n"
        "  bonfyre-auth [--db FILE] update --id ID [--tier T] [--active 0|1]\n"
        "  bonfyre-auth [--db FILE] sessions --user-id ID\n"
        "  bonfyre-auth [--db FILE] status\n");
}

int main(int argc, char **argv) {
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

    sqlite3 *db=open_db(db_path);
    if (!db) return 1;
    int rc=0;

    if (strcmp(cmd,"signup")==0) {
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
        else rc=cmd_login(db,email,pw);
    } else if (strcmp(cmd,"verify")==0) {
        const char *tok=arg_get(ca,cv,"--token");
        if (!tok) { fprintf(stderr,"Missing --token\n"); rc=1; }
        else rc=cmd_verify(db,tok);
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
    } else if (strcmp(cmd,"status")==0) {
        rc=cmd_status(db);
    } else {
        fprintf(stderr,"Unknown command: %s\n",cmd); usage(); rc=1;
    }

    sqlite3_close(db);
    return rc;
}
