/*
 * Native release preflight for an operator-owned tenant host/CMS scope map.
 * No CMS, credentials, network request, publication or effect is performed.
 * This is the same C parser consumed by Bonfyre WebAddress.
 */
#include <stdio.h>
#include <string.h>
#include "tenant_host_binding.h"

int main(int argc,char **argv){
    int scoped=argc==3&&argv[2]&&!strcmp(argv[2],"--require-scoped");
    if(argc==2&&argv[1]&&!strcmp(argv[1],"--help")){
        puts("usage: bonfyre-tenant-hosts-check PATH_TO_TSV [--require-scoped]");
        return 0;
    }
    if((argc!=2&&!scoped)||!argv[1]||!*argv[1]){
        fputs("usage: bonfyre-tenant-hosts-check PATH_TO_TSV [--require-scoped]\n",stderr);
        return 2;
    }
    if(bf_tenant_hosts_load(argv[1])!=1){
        fputs("TENANT_HOST_MAP_REFUSED: missing, malformed, ambiguous or unsafe\n",stderr);
        return 2;
    }
    for(size_t i=0;i<bf_tenant_host_count;i++){
        const BfTenantHostBinding *row=&bf_tenant_hosts[i];
        if(scoped&&!row->cms_namespace[0]){
            fputs("TENANT_HOST_MAP_REFUSED: unnamed CMS owner in scoped release\n",stderr);
            return 2;
        }
    }
    for(size_t i=0;i<bf_tenant_host_count;i++){
        const BfTenantHostBinding *row=&bf_tenant_hosts[i];
        if(row->cms_namespace[0])
            printf("%s\t%s\t%s\t%s\n",row->territory,row->public_host,
                   row->account_host,row->cms_namespace);
        else
            printf("%s\t%s\t%s\n",row->territory,row->public_host,row->account_host);
    }
    printf("TENANT_HOST_MAP_VALID entries=%zu scoped=%d\n",bf_tenant_host_count,scoped);
    return 0;
}
