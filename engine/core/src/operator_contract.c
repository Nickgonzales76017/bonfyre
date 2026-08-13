#define _POSIX_C_SOURCE 200809L
#include "bf_operator_contract.h"
#include "bonfyre.h"

#include <stdarg.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

static void contract_error(char *error, size_t size, const char *format, ...) {
    va_list values;
    if (!size) return;
    va_start(values, format); vsnprintf(error, size, format, values); va_end(values);
}

static const char *argument_value(const BfContractContext *context, const char *name) {
    if (!context || !name) return NULL;
    for (size_t index = 0; index < context->argument_count; ++index)
        if (context->arguments[index].name && !strcmp(context->arguments[index].name, name))
            return context->arguments[index].value;
    return NULL;
}

static const char *placeholder_value(const char *token, const BfContractContext *context) {
    if (!strcmp(token, "${binary}")) return context->binary;
    if (!strcmp(token, "${input.path}")) return context->input_path;
    if (!strcmp(token, "${input.dir}")) return context->input_dir;
    if (!strcmp(token, "${input.uri}")) return context->input_uri;
    if (!strcmp(token, "${mission.id}")) return context->mission_id;
    if (!strcmp(token, "${mission.dir}")) return context->mission_dir;
    if (!strcmp(token, "${output.dir}")) return context->output_dir;
    if (!strcmp(token, "${state.dir}")) return context->state_dir;
    static const char argument_prefix[] = "${argument.";
    size_t prefix = sizeof(argument_prefix) - 1;
    size_t length = strlen(token);
    if (!strncmp(token, argument_prefix, prefix) && length > prefix + 1 && token[length - 1] == '}') {
        char name[128];
        size_t name_length = length - prefix - 1;
        if (name_length >= sizeof(name)) return NULL;
        memcpy(name, token + prefix, name_length);
        name[name_length] = '\0';
        return argument_value(context, name);
    }
    return NULL;
}

static int supported_output_discovery(const char *mode) {
    return !strcmp(mode, "stdout") || !strcmp(mode, "stderr") || !strcmp(mode, "stdout-json") ||
           !strcmp(mode, "database-state") || !strcmp(mode, "namespace-state") || !strcmp(mode, "stream") ||
           !strncmp(mode, "output.dir/", 11) || !strncmp(mode, "glob:", 5) || !strncmp(mode, "manifest:", 9);
}

static int supported_probe(const char *probe, int quality) {
    static const char *workload[] = { "output-artifact", "structured-output", "output-exists", "database-row", "namespace-object", "nonempty-stream", "exit-plus-artifact", "manifest-complete", "relation-created", "state-transition", NULL };
    static const char *quality_probes[] = { "sha256-output", "json-schema", "structured-output", "artifact-integrity", "database-invariant", "namespace-invariant", "relation-integrity", "semantic-fixture", "round-trip", "reference-comparison", "output-exists", "quant-roundtrip", "qwen-blender-line", "model-embedding", "vector-store-state", "queue-workflow-state", "auth-authority-state", "graph-lineage", "project-space-state", "cms-crud-state", NULL };
    const char *const *values = quality ? quality_probes : workload;
    for (; *values; ++values) if (!strcmp(*values, probe)) return 1;
    return 0;
}

static int supported_working_directory(const char *policy) {
    return !strcmp(policy, "mission.dir") || !strcmp(policy, "output.dir") ||
           !strcmp(policy, "state.dir") || !strcmp(policy, "runtime.dir") ||
           !strncmp(policy, "root:", 5);
}

int bf_operator_contract_validate(const BfOperatorContract *contract, char *error, size_t error_size) {
    if (!contract || !contract->id[0] || (strcmp(contract->invocation_kind, "native-process") && strcmp(contract->invocation_kind, "blocked")) ||
        !contract->argv_template[0] || !contract->input_binding[0] || !contract->output_discovery[0] ||
        !contract->environment_allowlist[0] || !contract->working_directory_policy[0] ||
        !contract->timeout_seconds || !contract->output_limit_bytes || !contract->workload_probe[0] || !contract->quality_probe[0] ||
        !supported_output_discovery(contract->output_discovery) ||
        !supported_working_directory(contract->working_directory_policy) ||
        !supported_probe(contract->workload_probe, 0) || !supported_probe(contract->quality_probe, 1)) {
        contract_error(error, error_size, "incomplete or unsupported operator contract"); return -1;
    }
    if (strncmp(contract->retry_policy, "bounded:", 8) || !contract->environment_allowlist[0] ||
        strchr(contract->environment_allowlist, '\n') || strchr(contract->environment_allowlist, '`') ||
        strchr(contract->environment_allowlist, ';')) {
        contract_error(error, error_size, "invalid retry policy or environment allowlist"); return -1;
    }
    for (const char *entry = contract->environment_allowlist; *entry; ) {
        const char *separator = strchr(entry, ',');
        const char *equal = strchr(entry, '=');
        size_t length = separator ? (size_t)(separator-entry) : strlen(entry);
        if (!equal || equal >= entry + length || equal == entry) {
            contract_error(error,error_size,"invalid environment entry"); return -1;
        }
        entry += length; if (*entry == ',') ++entry;
    }
    return 0;
}

static int expand_token(const char *token, const BfContractContext *context, char **result,
                        char *error, size_t error_size) {
    char value[BF_CONTRACT_TEMPLATE_MAX] = "";
    size_t position = 0;
    const char *cursor = token;
    while (*cursor) {
        if (cursor[0] != '$' || cursor[1] != '{') {
            if (*cursor == ';' || *cursor == '\n' || *cursor == '\r' || *cursor == '`') {
                contract_error(error, error_size, "unsafe argv token"); return -1;
            }
            if (position + 1 >= sizeof(value)) return -1;
            value[position++] = *cursor++;
            continue;
        }
        const char *end = strchr(cursor, '}');
        if (!end || strstr(cursor, "$(")) { contract_error(error, error_size, "malformed placeholder"); return -1; }
        size_t length = (size_t)(end - cursor + 1);
        char placeholder[192];
        if (length >= sizeof(placeholder)) return -1;
        memcpy(placeholder, cursor, length); placeholder[length] = '\0';
        const char *replacement = placeholder_value(placeholder, context);
        if (!replacement || !replacement[0] || position + strlen(replacement) >= sizeof(value)) {
            contract_error(error, error_size, "unbound placeholder: %s", placeholder); return -1;
        }
        memcpy(value + position, replacement, strlen(replacement));
        position += strlen(replacement); cursor = end + 1;
    }
    if (!position) { contract_error(error, error_size, "empty argv token"); return -1; }
    *result = strdup(value);
    return *result ? 0 : -1;
}

int bf_operator_contract_expand(const BfOperatorContract *contract, const BfContractContext *context, BfContractExpansion *expanded, char *error, size_t error_size) {
    char template_copy[BF_CONTRACT_TEMPLATE_MAX]; char *save = NULL; char *token;
    if (bf_operator_contract_validate(contract, error, error_size) || !context || !expanded) return -1;
    memset(expanded, 0, sizeof(*expanded)); snprintf(template_copy, sizeof(template_copy), "%s", contract->argv_template);
    for (token = strtok_r(template_copy, " ", &save); token; token = strtok_r(NULL, " ", &save)) {
        if (expanded->argc + 1 >= BF_CONTRACT_ARGV_MAX || expand_token(token, context, &expanded->argv[expanded->argc], error, error_size)) {
            bf_operator_contract_free_expansion(expanded); return -1;
        }
        ++expanded->argc;
    }
    if (!expanded->argc || strcmp(expanded->argv[0], context->binary)) { contract_error(error, error_size, "argv must start with ${binary}"); bf_operator_contract_free_expansion(expanded); return -1; }
    return 0;
}

void bf_operator_contract_free_expansion(BfContractExpansion *expanded) {
    if (!expanded) return;
    for (size_t index = 0; index < expanded->argc; ++index) free(expanded->argv[index]);
    memset(expanded, 0, sizeof(*expanded));
}

static int contract_insert(sqlite3 *db, const BfOperatorContract *contract,
                           const char *path, const char *hash, FILE *error_stream) {
    sqlite3_stmt *statement = NULL;
    const char *sql = "INSERT INTO operator_contracts(id,generation,invocation_kind,argv_template,input_binding,output_discovery,environment_allowlist,working_directory_policy,timeout_seconds,output_limit_bytes,retry_policy,workload_probe,quality_probe,source_path,source_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement,1,contract->id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,2,contract->generation,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(statement,3,contract->invocation_kind,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,4,contract->argv_template,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(statement,5,contract->input_binding,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,6,contract->output_discovery,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(statement,7,contract->environment_allowlist,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,8,contract->working_directory_policy,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(statement,9,(int)contract->timeout_seconds); sqlite3_bind_int64(statement,10,(sqlite3_int64)contract->output_limit_bytes);
    sqlite3_bind_text(statement,11,contract->retry_policy,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,12,contract->workload_probe,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(statement,13,contract->quality_probe,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,14,path,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,15,hash,-1,SQLITE_TRANSIENT);
    int rc = sqlite3_step(statement); sqlite3_finalize(statement);
    if (rc != SQLITE_DONE && error_stream) fprintf(error_stream, "fabric: contract insert failed: %s\n", sqlite3_errmsg(db));
    return rc == SQLITE_DONE ? 0 : -1;
}

static int validate_template_declaration(const char *template, char *error, size_t error_size) {
    if (!template || !*template || strstr(template, "$(") || strchr(template, '`') || strchr(template, ';') || strchr(template, '\n')) {
        contract_error(error, error_size, "unsafe or empty argv template"); return -1;
    }
    char copy[BF_CONTRACT_TEMPLATE_MAX];
    snprintf(copy, sizeof(copy), "%s", template);
    char *save = NULL; char *token = strtok_r(copy, " ", &save); size_t count = 0;
    while (token) {
        if (++count >= BF_CONTRACT_ARGV_MAX) { contract_error(error,error_size,"too many argv tokens"); return -1; }
        for (char *cursor = token; (cursor = strstr(cursor, "${")); ) {
            char *end = strchr(cursor, '}');
            if (!end) { contract_error(error,error_size,"unterminated placeholder"); return -1; }
            char placeholder[192]; size_t length = (size_t)(end - cursor + 1);
            if (length >= sizeof(placeholder)) return -1;
            memcpy(placeholder,cursor,length); placeholder[length]='\0';
            if (strcmp(placeholder,"${binary}") && strcmp(placeholder,"${input.path}") &&
                strcmp(placeholder,"${input.dir}") &&
                strcmp(placeholder,"${input.uri}") && strcmp(placeholder,"${mission.id}") &&
                strcmp(placeholder,"${mission.dir}") && strcmp(placeholder,"${output.dir}") &&
                strcmp(placeholder,"${state.dir}") && strncmp(placeholder,"${argument.",11)) {
                contract_error(error,error_size,"unknown placeholder: %s",placeholder); return -1;
            }
            if (!strncmp(placeholder,"${argument.",11) && (length <= 12 || placeholder[length-1] != '}')) {
                contract_error(error,error_size,"invalid argument placeholder"); return -1;
            }
            cursor = end + 1;
        }
        token = strtok_r(NULL," ",&save);
    }
    if (!count || strncmp(template,"${binary}",9)) { contract_error(error,error_size,"argv must start with ${binary}"); return -1; }
    return 0;
}

static int parse_unsigned_value(const char *value, unsigned long long *number) {
    char *end = NULL; errno = 0;
    if (!value || !*value || *value == '-') return -1;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (errno || !end || *end || !parsed) return -1;
    *number = parsed; return 0;
}

int bf_operator_contracts_compile(sqlite3 *db, const char *contracts_path,
                                  const char *generation, FILE *error_stream) {
    FILE *file = fopen(contracts_path, "r"); char line[2048], hash[65] = "";
    BfOperatorContract contract = {0}; unsigned field_mask = 0; int count = 0;
    if (!file) { if (error_stream) fprintf(error_stream,"fabric: missing contracts %s\n",contracts_path); return -1; }
    sqlite3_stmt *delete_statement = NULL;
    sqlite3_prepare_v2(db, "DELETE FROM operator_contract_bindings", -1, &delete_statement, NULL);
    if (!delete_statement || sqlite3_step(delete_statement) != SQLITE_DONE) { sqlite3_finalize(delete_statement); fclose(file); return -1; }
    sqlite3_finalize(delete_statement);
    sqlite3_prepare_v2(db, "DELETE FROM operator_contracts", -1, &delete_statement, NULL);
    if (!delete_statement || sqlite3_step(delete_statement) != SQLITE_DONE) { sqlite3_finalize(delete_statement); fclose(file); return -1; }
    sqlite3_finalize(delete_statement);
    if (bf_sha256_file(contracts_path, hash) != 0) { fclose(file); return -1; }
    while (fgets(line,sizeof(line),file)) {
        char key[64], value[1024]; char *trimmed=line;
        while (*trimmed==' '||*trimmed=='\t') ++trimmed;
        if (*trimmed=='#'||*trimmed=='\n'||!*trimmed) continue;
        if (sscanf(trimmed,"contract %95s",value)==1) {
            if (contract.id[0]) { char error[256]; if (field_mask != 0x7ff || validate_template_declaration(contract.argv_template,error,sizeof(error)) || bf_operator_contract_validate(&contract,error,sizeof(error)) || contract_insert(db,&contract,contracts_path,hash,error_stream)) { if(error_stream)fprintf(error_stream,"fabric: invalid contract %s: %s\n",contract.id,error); fclose(file); return -1; } ++count; }
            memset(&contract,0,sizeof(contract)); field_mask=0; snprintf(contract.id,sizeof(contract.id),"%s",value); snprintf(contract.generation,sizeof(contract.generation),"%s",generation); continue;
        }
        if (!contract.id[0] || sscanf(trimmed,"%63s %1023[^\n]",key,value)!=2) { fclose(file); return -1; }
        unsigned bit=0; unsigned long long number=0;
        if (!strcmp(key,"invocation_kind")) {bit=1; snprintf(contract.invocation_kind,sizeof(contract.invocation_kind),"%s",value);}
        else if (!strcmp(key,"argv")) {bit=2; snprintf(contract.argv_template,sizeof(contract.argv_template),"%s",value);}
        else if (!strcmp(key,"input_binding")) {bit=4; snprintf(contract.input_binding,sizeof(contract.input_binding),"%s",value);}
        else if (!strcmp(key,"output_discovery")) {bit=8; snprintf(contract.output_discovery,sizeof(contract.output_discovery),"%s",value);}
        else if (!strcmp(key,"environment")) {bit=16; snprintf(contract.environment_allowlist,sizeof(contract.environment_allowlist),"%s",value);}
        else if (!strcmp(key,"working_directory")) {bit=32; snprintf(contract.working_directory_policy,sizeof(contract.working_directory_policy),"%s",value);}
        else if (!strcmp(key,"timeout_seconds")) {bit=64; if(parse_unsigned_value(value,&number)||number>UINT_MAX){fclose(file);return -1;} contract.timeout_seconds=(unsigned)number;}
        else if (!strcmp(key,"output_limit_bytes")) {bit=128; if(parse_unsigned_value(value,&number)||number>SIZE_MAX){fclose(file);return -1;} contract.output_limit_bytes=(size_t)number;}
        else if (!strcmp(key,"retry_policy")) {bit=256; snprintf(contract.retry_policy,sizeof(contract.retry_policy),"%s",value);}
        else if (!strcmp(key,"workload_probe")) {bit=512; snprintf(contract.workload_probe,sizeof(contract.workload_probe),"%s",value);}
        else if (!strcmp(key,"quality_probe")) {bit=1024; snprintf(contract.quality_probe,sizeof(contract.quality_probe),"%s",value);}
        else { if(error_stream) fprintf(error_stream,"fabric: unknown contract field %s\n",key); fclose(file); return -1; }
        if (field_mask & bit) { if(error_stream)fprintf(error_stream,"fabric: duplicate contract field %s\n",key); fclose(file); return -1; }
        field_mask |= bit;
    }
    if (contract.id[0]) { char error[256]; if (field_mask != 0x7ff || validate_template_declaration(contract.argv_template,error,sizeof(error)) || bf_operator_contract_validate(&contract,error,sizeof(error)) || contract_insert(db,&contract,contracts_path,hash,error_stream)) { if(error_stream)fprintf(error_stream,"fabric: invalid contract %s: %s\n",contract.id,error); fclose(file); return -1; } ++count; }
    fclose(file); return count ? 0 : -1;
}

static int contract_exists(sqlite3 *db, const char *id) {
    sqlite3_stmt *statement = NULL; int present = 0;
    sqlite3_prepare_v2(db, "SELECT 1 FROM operator_contracts WHERE id=?", -1, &statement, NULL);
    sqlite3_bind_text(statement, 1, id, -1, SQLITE_TRANSIENT);
    present = sqlite3_step(statement) == SQLITE_ROW; sqlite3_finalize(statement); return present;
}

static int operator_exists(sqlite3 *db, const char *id) {
    sqlite3_stmt *statement = NULL; int present = 0;
    sqlite3_prepare_v2(db, "SELECT 1 FROM catalog WHERE id=?", -1, &statement, NULL);
    sqlite3_bind_text(statement, 1, id, -1, SQLITE_TRANSIENT);
    present = sqlite3_step(statement) == SQLITE_ROW; sqlite3_finalize(statement); return present;
}

int bf_operator_contract_bindings_compile(sqlite3 *db, const char *bindings_path,
                                          const char *generation, FILE *error_stream) {
    FILE *file = fopen(bindings_path, "r"); char line[4096]; int count = 0;
    if (!file) { if(error_stream) fprintf(error_stream,"fabric: missing bindings %s\n",bindings_path); return -1; }
    if (!fgets(line,sizeof(line),file) || strcmp(line,"operator_id\tcontract_id\tfamily\targument_defaults\tworkload_fixture\tquality_probe_override\n")) { fclose(file); return -1; }
    sqlite3_stmt *clear = NULL; sqlite3_prepare_v2(db,"DELETE FROM operator_contract_bindings",-1,&clear,NULL);
    if (!clear || sqlite3_step(clear)!=SQLITE_DONE) { sqlite3_finalize(clear); fclose(file); return -1; } sqlite3_finalize(clear);
    while (fgets(line,sizeof(line),file)) {
        char *fields[6], *cursor=line; int field=0; sqlite3_stmt *insert=NULL;
        while (field < 6 && cursor) { fields[field++]=cursor; cursor=strchr(cursor,'\t'); if(cursor)*cursor++='\0'; }
        if (field!=6 || cursor || !fields[0][0] || !fields[1][0] || !fields[2][0] || fields[3][0]!='{') { fclose(file); return -1; }
        fields[5][strcspn(fields[5],"\r\n")]='\0'; fields[4][strcspn(fields[4],"\r\n")]='\0';
        if (!operator_exists(db,fields[0]) || !contract_exists(db,fields[1])) { if(error_stream) fprintf(error_stream,"fabric: unknown binding reference %s -> %s\n",fields[0],fields[1]); fclose(file); return -1; }
        sqlite3_prepare_v2(db,"INSERT INTO operator_contract_bindings(operator_id,contract_id,generation,family,argument_defaults,workload_fixture,quality_probe_override) VALUES(?,?,?,?,?,?,?)",-1,&insert,NULL);
        sqlite3_bind_text(insert,1,fields[0],-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,2,fields[1],-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,3,generation,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,4,fields[2],-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,5,fields[3],-1,SQLITE_TRANSIENT);
        if(fields[4][0])sqlite3_bind_text(insert,6,fields[4],-1,SQLITE_TRANSIENT);else sqlite3_bind_null(insert,6); if(fields[5][0])sqlite3_bind_text(insert,7,fields[5],-1,SQLITE_TRANSIENT);else sqlite3_bind_null(insert,7);
        if(sqlite3_step(insert)!=SQLITE_DONE){sqlite3_finalize(insert);fclose(file);return -1;} sqlite3_finalize(insert); ++count;
    }
    fclose(file); if(count!=93){if(error_stream)fprintf(error_stream,"fabric: expected 93 contract bindings, got %d\n",count);return -1;} return 0;
}

int bf_operator_contract_load(sqlite3 *db, const char *operator_id,
                              const char *active_generation,
                              BfOperatorContract *contract,
                              BfOperatorContractBinding *binding,
                              FILE *error_stream) {
    sqlite3_stmt *statement = NULL;
    const char *sql = "SELECT c.id,c.generation,c.invocation_kind,c.argv_template,c.input_binding,c.output_discovery,c.environment_allowlist,c.working_directory_policy,c.timeout_seconds,c.output_limit_bytes,c.retry_policy,c.workload_probe,c.quality_probe,b.operator_id,b.contract_id,b.generation,b.family,b.argument_defaults,COALESCE(b.workload_fixture,''),COALESCE(b.quality_probe_override,'') FROM catalog o JOIN catalog_bindings cb ON cb.operator_id=o.id JOIN operator_contract_bindings b ON b.operator_id=o.id JOIN operator_contracts c ON c.id=b.contract_id JOIN fabric_meta m ON m.key='catalog_generation' WHERE o.id=? AND m.value=? AND cb.catalog_generation=? AND b.generation=? AND c.generation=? AND cb.binding_state='bound'";
    if (!db || !operator_id || !active_generation || !contract || !binding || sqlite3_prepare_v2(db,sql,-1,&statement,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_text(statement,1,operator_id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,2,active_generation,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,3,active_generation,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,4,active_generation,-1,SQLITE_TRANSIENT); sqlite3_bind_text(statement,5,active_generation,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(statement)!=SQLITE_ROW){
        sqlite3_finalize(statement);
        sqlite3_stmt *exists = NULL;
        sqlite3_prepare_v2(db,"SELECT 1 FROM catalog WHERE id=?",-1,&exists,NULL); sqlite3_bind_text(exists,1,operator_id,-1,SQLITE_TRANSIENT);
        int known = sqlite3_step(exists)==SQLITE_ROW; sqlite3_finalize(exists);
        if(error_stream) fprintf(error_stream, "fabric: %s for %s\n", known ? "missing, unbound, or stale contract binding" : "unknown operator", operator_id);
        return known ? -3 : -4;
    }
    memset(contract,0,sizeof(*contract)); memset(binding,0,sizeof(*binding));
#define COPY(field,column) snprintf(field,sizeof(field),"%s",sqlite3_column_text(statement,column)?(const char*)sqlite3_column_text(statement,column):"")
    COPY(contract->id,0); COPY(contract->generation,1); COPY(contract->invocation_kind,2); COPY(contract->argv_template,3); COPY(contract->input_binding,4); COPY(contract->output_discovery,5); COPY(contract->environment_allowlist,6); COPY(contract->working_directory_policy,7); contract->timeout_seconds=(unsigned)sqlite3_column_int(statement,8); contract->output_limit_bytes=(size_t)sqlite3_column_int64(statement,9); COPY(contract->retry_policy,10); COPY(contract->workload_probe,11); COPY(contract->quality_probe,12); COPY(binding->operator_id,13); COPY(binding->contract_id,14); COPY(binding->generation,15); COPY(binding->family,16); COPY(binding->argument_defaults,17); COPY(binding->workload_fixture,18); COPY(binding->quality_probe_override,19);
#undef COPY
    sqlite3_finalize(statement);
    if(!strcmp(contract->invocation_kind,"blocked")){if(error_stream)fprintf(error_stream,"fabric: operator is explicitly blocked: %s\n",operator_id);return -2;}
    char validation_error[256] = "";
    int validation_rc = bf_operator_contract_validate(contract, validation_error,
                                                       sizeof(validation_error));
    if (validation_rc && error_stream) {
        fprintf(error_stream, "fabric: invalid compiled contract %s: %s\n",
                contract->id, validation_error[0] ? validation_error : "validation failed");
    }
    return validation_rc;
}
