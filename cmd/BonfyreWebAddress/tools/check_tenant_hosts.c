/*
 * Release preflight for a destination-owned tenant host map.
 * No CMS, credentials, network request, publication or effect is performed.
 * This is an actual C consumer of the same parser WebAddress uses.
 */
#include <stdio.h>
#include <string.h>
#include "tenant_host_binding.h"

int main(int argc,char **argv){
    if(argc!=2 || !argv[1] || !*argv[1] || !strcmp(argv[1],"--help")){
        fputs("usage: bonfyre-tenant-hosts-check PATH_TO_TENANT_HOSTS_TSV\n",stderr);
        return argc==2&&argv[1]&&!strcmp(argv[1],"--help")?0:2;
    }
    if(bf_tenant_hosts_load(argv[1])!=1){
        fputs("TENANT_HOST_MAP_REFUSED: missing, malformed, ambiguous or unsafe\n",stderr);
        return 2;
    }
    for(size_t i=0;i<bf_tenant_host_count;i++)
        printf("%s\t%s\t%s\n",bf_tenant_hosts[i].territory,
               bf_tenant_hosts[i].public_host,bf_tenant_hosts[i].account_host);
    printf("TENANT_HOST_MAP_VALID entries=%zu\n",bf_tenant_host_count);
    return 0;
}
