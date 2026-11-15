// Which public origin each estate territory answers on. Compiled by
// cmd/BonfyreWebAddress/tools/compile_estate_hosts.sh into
// cmd/BonfyreWebAddress/generated/native_estate_hosts.h as BF_ORIGIN_<NAME>
// string macros and one bf_estate_hosts[] table. A page names a territory;
// it never writes an https://aurekai.* literal.
package control

#EstateHost: {
	territory: =~"^[a-z]+$"
	host:      =~"^([a-z]+\\.)?aurekai\\.(work|life|live|shop)$"
	// territory: one of the four public territories; service: an estate subdomain.
	role: "territory" | "service"
}

estate_hosts: [...#EstateHost] & [
	{territory: "work", host: "aurekai.work", role: "territory"},
	{territory: "life", host: "aurekai.life", role: "territory"},
	{territory: "live", host: "aurekai.live", role: "territory"},
	{territory: "shop", host: "aurekai.shop", role: "territory"},
	{territory: "signin", host: "signin.aurekai.work", role: "service"},
	{territory: "docs", host: "docs.aurekai.work", role: "service"},
	{territory: "internet", host: "internet.aurekai.work", role: "service"},
	{territory: "nightgrid", host: "nightgrid.aurekai.live", role: "service"},
]
