#include "admit.h"
#include "compile.h"
#include <stdio.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static const char *opt(int argc,char **argv,const char *flag){
    for(int i=2;i+1<argc;i++)if(!strcmp(argv[i],flag))return argv[i+1];return NULL;
}
static int has(int argc,char **argv,const char *flag){
    for(int i=2;i<argc;i++)if(!strcmp(argv[i],flag))return 1;return 0;
}
static void usage(FILE *out){
    fputs("BonfyreFragmentMigration 0.2.0\n"
          "  bonfyre-fragment-migration compile LITERAL_MANIFEST --source-map FILE --out-dir DIR [--evidence-ref REF]\n"
          "  bonfyre-fragment-migration admit DRAFTS_JSONL --db FILE --schemas DIR --namespace NS --cms-bin FILE [--apply]\n"
          "Compile emits draft/review projections only. Admit never overwrites an existing coordinate and never publishes.\n",out);
}
int main(int argc,char **argv){
    enum { FM_HELP, FM_ADMIT, FM_COMPILE };
    static const BfCommandEntry fm_commands[] = {
        {"--help", FM_HELP, 0, NULL},
        {"admit", FM_ADMIT, 0, NULL},
        {"compile", FM_COMPILE, 0, NULL},
        {"help", FM_HELP, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(fm_commands,
        sizeof(fm_commands) / sizeof(fm_commands[0]), argv[1]);
    if(argc==2&&command&&command->id==FM_HELP){usage(stdout);return 0;}
    if(argc<3){usage(stderr);return 2;}
    if(command&&command->id==FM_COMPILE){
        const char *map=opt(argc,argv,"--source-map"),*out=opt(argc,argv,"--out-dir");
        const char *evidence=opt(argc,argv,"--evidence-ref");
        if(!map||!out){usage(stderr);return 2;}
        return bf_fm_compile(argv[2],map,out,evidence);
    }
    if(command&&command->id==FM_ADMIT){
        const char *db=opt(argc,argv,"--db"),*schemas=opt(argc,argv,"--schemas");
        const char *ns=opt(argc,argv,"--namespace"),*cms=opt(argc,argv,"--cms-bin");
        if(!db||!schemas||!ns||!cms){usage(stderr);return 2;}
        return bf_fm_admit(argv[2],db,schemas,ns,cms,has(argc,argv,"--apply"));
    }
    usage(stderr);return 2;
}
