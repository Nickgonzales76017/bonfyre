CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11 -D_DEFAULT_SOURCE
XML_JSON_CFLAGS := $(shell pkg-config --cflags libxml-2.0 json-c)
XML_JSON_LIBS := $(shell pkg-config --libs libxml-2.0 json-c)

MEMBRANE_ROOT ?= /opt/bonfyre/native-src
REPRESENTATION_PREFIX ?= $(HOME)/.bonfyre
REPRESENTATION_TESTS := $(sort $(wildcard tests/representation-manifold*.test.mjs tests/representation_manifold*.test.mjs)) tests/representation-release.test.mjs

.PHONY: test install-membrane install-content-screen install-representation-manifold install-representation-forge install-representation-delta install-representation-substrait install-representation-zarr install-representation-fabric install-representation-carrier install-representation-report install-representation-adbc install-representation-zero-copy install-publisher-hosts install-program-fabric install-edition install-entity-resolution install-price-observation install-rate-discovery

foreign-xml: foreign_xml.c
	$(CC) $(CFLAGS) $(XML_JSON_CFLAGS) -o $@ $< $(XML_JSON_LIBS)

test: foreign-xml recurring-commerce-check
	bun test tests/feed_parse.test.mjs tests/sitemap_delta.test.mjs tests/foreign_discover.test.mjs tests/membrane_store.test.mjs tests/membrane_install.test.mjs tests/resource_admission.test.mjs tests/admitted_frontier.test.mjs tests/representation_trial.test.mjs tests/content_screen.test.mjs tests/content_screen_install.test.mjs $(REPRESENTATION_TESTS) ../cmd/BonfyreZarrForge/tests/zarr_forge.test.mjs tests/compose_publisher_hosts.test.mjs tests/compile_program_fabric.test.mjs tests/program-workspace-owner.test.mjs tests/publish_edition.test.mjs tests/resolve_entities.test.mjs tests/observe_prices.test.mjs tests/sweep_rates.test.mjs tests/price_install.test.mjs

# Run on the target host: foreign-xml is compiled for that host, never copied
# from a developer Mac. Paths match the systemd and WebAddress contracts.
install-membrane: foreign-xml
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/cmd/BonfyreSourceFrontier/tools $(MEMBRANE_ROOT)/schemas/control
	install -m 0755 foreign-xml foreign-discover.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 membrane-store.mjs machine-readership.mjs sitemap-delta.mjs feed-parse.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 foreign-discovery.cue machine-membrane.cue sitemap-delta.cue feed-observation.cue $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../cmd/BonfyreSourceFrontier/tools/foreign_observation.mjs ../cmd/BonfyreSourceFrontier/tools/robots_policy.mjs $(MEMBRANE_ROOT)/cmd/BonfyreSourceFrontier/tools/
	install -m 0644 ../schemas/control/machine-readership.cue $(MEMBRANE_ROOT)/schemas/control/

install-content-screen:
	install -d $(MEMBRANE_ROOT)/scripts
	install -m 0755 content-screen.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 content-screen.cue $(MEMBRANE_ROOT)/scripts/

.PHONY: install-representation-release test-representation-release
install-representation-release:
	bun representation-release.mjs --prefix "$(REPRESENTATION_PREFIX)"
test-representation-release:
	bun test tests/representation-release.test.mjs

install-representation-manifold:
	$(MAKE) -C ../cmd/BonfyreHash all
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/schemas/storage $(MEMBRANE_ROOT)/cmd/BonfyreHash
	install -m 0755 ../cmd/BonfyreHash/bonfyre-hash $(MEMBRANE_ROOT)/cmd/BonfyreHash/
	install -m 0755 representation-manifold-shift.mjs representation-manifold-gc.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 arrow-runtime-verify.mjs representation-manifold-table-cut.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../schemas/storage/representation-portfolio.cue ../schemas/storage/representation-manifold.sql $(MEMBRANE_ROOT)/schemas/storage/

install-representation-forge:
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/schemas/storage
	install -m 0755 representation-manifold-forge.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 representation-forge.cue columnar_body.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../schemas/storage/representation-manifold.sql $(MEMBRANE_ROOT)/schemas/storage/

install-representation-delta:
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/schemas/storage
	install -m 0755 representation-manifold-delta.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 columnar_body.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../schemas/storage/representation-manifold.sql $(MEMBRANE_ROOT)/schemas/storage/

install-representation-substrait:
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/schemas/storage
	install -m 0755 representation-manifold-substrait.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 columnar_body.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../schemas/storage/representation-manifold.sql $(MEMBRANE_ROOT)/schemas/storage/

install-representation-zarr:
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/schemas/storage
	install -m 0755 representation-manifold-zarr.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 arrow-runtime-verify.mjs columnar_body.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../schemas/storage/representation-manifold.sql $(MEMBRANE_ROOT)/schemas/storage/

install-representation-fabric: install-representation-manifold install-representation-substrait install-representation-carrier install-representation-report install-representation-adbc install-representation-forge install-representation-delta
	install -m 0755 representation-manifold-fabric.mjs representation-manifold-zero-copy.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 representation-manifold-gc.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 arrow-runtime-verify.mjs columnar_body.mjs $(MEMBRANE_ROOT)/scripts/

install-representation-carrier:
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/schemas/storage
	install -m 0755 representation-manifold-carrier.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 columnar_body.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../schemas/storage/representation-manifold.sql $(MEMBRANE_ROOT)/schemas/storage/

install-representation-report:
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/schemas/storage
	install -m 0755 representation-manifold-report.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../schemas/storage/representation-manifold.sql $(MEMBRANE_ROOT)/schemas/storage/

install-representation-adbc:
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/schemas/storage $(MEMBRANE_ROOT)/cmd/BonfyreADBCProbe/src
	install -m 0755 representation-manifold-adbc.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 columnar_body.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../schemas/storage/representation-manifold.sql $(MEMBRANE_ROOT)/schemas/storage/
	install -m 0644 ../cmd/BonfyreADBCProbe/Makefile $(MEMBRANE_ROOT)/cmd/BonfyreADBCProbe/
	install -m 0644 ../cmd/BonfyreADBCProbe/src/main.c $(MEMBRANE_ROOT)/cmd/BonfyreADBCProbe/src/

install-representation-zero-copy: install-representation-fabric

install-publisher-hosts:
	install -d $(MEMBRANE_ROOT)/scripts
	install -m 0755 compose-publisher-hosts.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 publisher-host.cue publisher-host.sql $(MEMBRANE_ROOT)/scripts/

install-program-fabric:
	$(MAKE) -C ../cmd/BonfyreExternalEstate bonfyre-external-estate
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/cmd/BonfyreExternalEstate/tools $(MEMBRANE_ROOT)/integrations/browser
	install -m 0755 compile-program-fabric.mjs project-program-service-policy.mjs program-workspace-owner.mjs resolve_program_sources.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 program-fabric.cue $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../integrations/browser/workspace-msgpack.mjs $(MEMBRANE_ROOT)/integrations/browser/
	install -m 0644 ../cmd/BonfyreExternalEstate/tools/resolve_program_demand.mjs $(MEMBRANE_ROOT)/cmd/BonfyreExternalEstate/tools/
	install -m 0755 ../cmd/BonfyreExternalEstate/bonfyre-external-estate $(MEMBRANE_ROOT)/cmd/BonfyreExternalEstate/

install-edition:
	install -d $(MEMBRANE_ROOT)/scripts
	install -m 0755 publish-edition.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 edition-policy.cue edition.css $(MEMBRANE_ROOT)/scripts/

install-entity-resolution:
	install -d $(MEMBRANE_ROOT)/scripts
	install -m 0755 resolve-entities.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 entity-resolution.cue $(MEMBRANE_ROOT)/scripts/

install-price-observation:
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/cmd/BonfyreSourceFrontier/tools
	install -m 0755 observe-prices.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 price-observation.cue public-source-fetch.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../cmd/BonfyreSourceFrontier/tools/robots_policy.mjs $(MEMBRANE_ROOT)/cmd/BonfyreSourceFrontier/tools/

install-rate-discovery:
	install -d $(MEMBRANE_ROOT)/scripts $(MEMBRANE_ROOT)/cmd/BonfyreSourceFrontier/tools
	install -m 0755 sweep-rates.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 rate-discovery.cue public-source-fetch.mjs $(MEMBRANE_ROOT)/scripts/
	install -m 0644 ../cmd/BonfyreSourceFrontier/tools/robots_policy.mjs $(MEMBRANE_ROOT)/cmd/BonfyreSourceFrontier/tools/

.PHONY: recurring-commerce-browser recurring-commerce-check
recurring-commerce-browser:
	bun project-recurring-commerce-browser.mjs --write
	bun project-recurring-commerce-browser.mjs --check
recurring-commerce-check:
	bun project-recurring-commerce-browser.mjs --check
