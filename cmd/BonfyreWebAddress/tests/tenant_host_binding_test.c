#include "tenant_host_binding.h"
#include <stdio.h>
#include <string.h>

#define CHECK(c, reason) do { if (!(c)) { fprintf(stderr, "FAIL: %s\n", reason); return 1; } } while (0)

int main(int argc, char **argv) {
    if (argc != 4) return 2;
    CHECK(bf_tenant_hosts_load(NULL) == 0, "unconfigured deployment is distinguishable from malformed config");
    CHECK(bf_tenant_hosts_status() == 0, "unconfigured status");
    CHECK(bf_tenant_hosts_load(argv[1]) == 1, "valid exact host map");
    CHECK(bf_tenant_hosts_status() == 1 && bf_tenant_host_count == 4, "four explicit territories");
    CHECK(!strcmp(bf_tenant_public_host("work"), "demo.example.net"), "work public host");
    CHECK(!strcmp(bf_tenant_account_host("work"), "login.demo.example.net"), "work login host");
    CHECK(!strcmp(bf_tenant_territory_for_host("auth.live.example.net"), "live"), "account host maps to territory");
    CHECK(!strcmp(bf_tenant_territory_for_host("people.example.net"), "life"), "apex host maps to territory");
    CHECK(!bf_tenant_territory_for_host("tenant.people.example.net"), "no guessed inherited subdomain authority");
    CHECK(bf_tenant_is_declared_subhost("tenant.people.example.net", "life"), "subhost eligibility only for separate address verification");
    CHECK(!bf_tenant_is_declared_subhost("people.example.net.evil.test", "life"), "spoofed suffix refused");
    CHECK(!bf_tenant_territory_for_host("aurekai.work"), "no automatic Aurekai tenant");
    CHECK(!bf_tenant_public_host("unknown"), "unknown territory refused");
    CHECK(bf_tenant_is_account_host("login.demo.example.net"), "exact login host recognized");
    CHECK(!bf_tenant_is_account_host("attacker.login.demo.example.net"), "login host suffix cannot escalate");

    CHECK(bf_tenant_hosts_load(argv[2]) == -1, "duplicate host map refused");
    CHECK(bf_tenant_hosts_status() == -1, "malformed config fails closed");
    CHECK(!bf_tenant_public_host("work"), "old host not retained when reload fails");
    CHECK(bf_tenant_hosts_load(argv[3]) == -1, "unsafe host refused");
    CHECK(!bf_tenant_account_host("work"), "bad config grants no login host");
    CHECK(bf_tenant_hosts_load("/no/such/tenant-map.conf") == -1, "missing configured file fails closed");
    CHECK(bf_tenant_hosts_load(argv[1]) == 1, "valid reload restores exact authority");
    puts("tenant_host_binding_test: PASS");
    return 0;
}
