#define _POSIX_C_SOURCE 200809L
#include "bf_workgraph.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#define CHECK(condition) do { if (!(condition)) { fprintf(stderr,"checkpoint failure at line %d: %s\n",__LINE__,#condition); return 1; } } while (0)

int main(void) {
    char path[]="/tmp/bf-checkpoint-XXXXXX";
    int fd=mkstemp(path); CHECK(fd>=0); close(fd); unlink(path);
    BfWorkgraph *graph=NULL; BfWorkgraphCheckpoint record;
    CHECK(bf_workgraph_open(path,&graph,stderr)==0);
    CHECK(bf_workgraph_create_mission(graph,"m").status==BF_WORKGRAPH_OK);
    BfWorkgraphNodeSpec spec={0}; spec.mission_id="m";spec.node_id="n";spec.operator_id="routine";
    CHECK(bf_workgraph_add_node(graph,&spec).status==BF_WORKGRAPH_OK);
    CHECK(bf_workgraph_checkpoint_read(graph,"m","missing",&record).status==BF_WORKGRAPH_NOT_FOUND);
    BfWorkgraphResult claim=bf_workgraph_claim_node(graph,"m","n",&(BfWorkgraphClaimSpec){"coach",NULL,60000});
    CHECK(claim.status==BF_WORKGRAPH_OK);
    CHECK(bf_workgraph_checkpoint_read(graph,"m","n",&record).status==BF_WORKGRAPH_OK && record.revision==0);
    CHECK(bf_workgraph_checkpoint_save(graph,"m","n","other",claim.claim_token,0,"file:///draft-1",&record).status==BF_WORKGRAPH_STALE_CLAIM);
    CHECK(bf_workgraph_checkpoint_save(graph,"m","n","coach",claim.claim_token,-1,"file:///draft-1",&record).status==BF_WORKGRAPH_INVALID);
    bf_workgraph_close(graph);
    /* Two independent processes compete for the same revision. Exactly one wins. */
    int barrier[2];CHECK(pipe(barrier)==0);
    pid_t children[2];
    for(int i=0;i<2;i++) {
        children[i]=fork();CHECK(children[i]>=0);
        if(children[i]==0) {
            close(barrier[1]);char byte; if(read(barrier[0],&byte,1)!=1)_exit(20);close(barrier[0]);
            BfWorkgraph *child=NULL;if(bf_workgraph_open(path,&child,stderr))_exit(21);
            BfWorkgraphResult saved=bf_workgraph_checkpoint_save(child,"m","n","coach",claim.claim_token,0,i?"file:///draft-b":"file:///draft-a",&record);
            bf_workgraph_close(child);_exit(saved.status==BF_WORKGRAPH_OK?0:saved.status==BF_WORKGRAPH_CONFLICT?10:22);
        }
    }
    close(barrier[0]);CHECK(write(barrier[1],"go",2)==2);close(barrier[1]);
    int wins=0,conflicts=0;
    for(int i=0;i<2;i++){int status;CHECK(waitpid(children[i],&status,0)==children[i]);CHECK(WIFEXITED(status));wins+=WEXITSTATUS(status)==0;conflicts+=WEXITSTATUS(status)==10;}
    CHECK(wins==1 && conflicts==1);
    CHECK(bf_workgraph_open(path,&graph,stderr)==0);
    CHECK(bf_workgraph_checkpoint_read(graph,"m","n",&record).status==BF_WORKGRAPH_OK && record.revision==1);
    CHECK(bf_workgraph_checkpoint_save(graph,"m","n","coach",claim.claim_token,1,"file:///draft-final",&record).status==BF_WORKGRAPH_OK && record.revision==2);
    CHECK(bf_workgraph_complete_checkpoint(graph,"m","n","coach",claim.claim_token,"file:///delivery-old",1).status==BF_WORKGRAPH_CONFLICT);
    CHECK(!strcmp(bf_workgraph_status(graph,"m","n").node_status,"running"));
    sqlite3 *fault=NULL;CHECK(sqlite3_open(path,&fault)==SQLITE_OK);
    CHECK(sqlite3_exec(fault,"CREATE TRIGGER refuse_completion BEFORE UPDATE OF status ON workgraph_nodes WHEN NEW.status='complete' BEGIN SELECT RAISE(ABORT,'fixture failure'); END",NULL,NULL,NULL)==SQLITE_OK);
    CHECK(bf_workgraph_complete_checkpoint(graph,"m","n","coach",claim.claim_token,"file:///delivery-final",2).status==BF_WORKGRAPH_STORAGE_ERROR);
    CHECK(!strcmp(bf_workgraph_status(graph,"m","n").node_status,"running"));
    CHECK(sqlite3_exec(fault,"DROP TRIGGER refuse_completion",NULL,NULL,NULL)==SQLITE_OK);sqlite3_close(fault);
    CHECK(bf_workgraph_complete_checkpoint(graph,"m","n","coach",claim.claim_token,"file:///delivery-final",2).status==BF_WORKGRAPH_OK);
    CHECK(bf_workgraph_checkpoint_save(graph,"m","n","coach",claim.claim_token,2,"file:///late-draft",&record).status==BF_WORKGRAPH_STALE_CLAIM);
    bf_workgraph_close(graph);
    /* Existing v8 databases gain checkpoints without changing their work. */
    sqlite3 *db=NULL;CHECK(sqlite3_open(path,&db)==SQLITE_OK);
    CHECK(sqlite3_exec(db,"DROP TABLE workgraph_checkpoints;DELETE FROM schema_migrations WHERE version=9;UPDATE fabric_meta SET value='8' WHERE key='schema_version'",NULL,NULL,NULL)==SQLITE_OK);sqlite3_close(db);
    CHECK(bf_workgraph_open(path,&graph,stderr)==0);
    CHECK(!strcmp(bf_workgraph_status(graph,"m","n").node_status,"complete"));
    CHECK(bf_workgraph_checkpoint_read(graph,"m","n",&record).status==BF_WORKGRAPH_OK && record.revision==0);
    bf_workgraph_close(graph);unlink(path);puts("native checkpoints: concurrent save, stale delivery, owner and migration checks passed");return 0;
}
