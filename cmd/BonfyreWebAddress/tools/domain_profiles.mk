# Included by WebAddress and exercised directly in isolated build fixtures.
# CUE owns names and field mappings. This recipe publishes only compilable C.
DOMAIN_PROFILE_HEADER ?= generated/native_domain_profiles.h
DOMAIN_PROFILE_COMPILER ?= tools/compile_domain_profiles.zig
DOMAIN_PROFILE_SCHEMAS ?= $(ROOT)/schemas/control
CUE ?= cue
ZIG ?= zig
DOMAIN_PROFILE_INPUTS = $(wildcard $(DOMAIN_PROFILE_SCHEMAS)/*.cue) $(DOMAIN_PROFILE_COMPILER)

$(DOMAIN_PROFILE_HEADER): $(DOMAIN_PROFILE_INPUTS)
	@set -eu; \
	mkdir -p "$(@D)"; \
	tmp=$$(mktemp -d "$(@D)/.domain-profiles.XXXXXX"); \
	trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
	(cd "$(DOMAIN_PROFILE_SCHEMAS)" && $(CUE) export . -e nativeDomainProfiles --out json) > "$$tmp/input.json"; \
	if ! $(ZIG) run "$(DOMAIN_PROFILE_COMPILER)" -- "$$tmp/input.json" "$$tmp/profiles.h" 2>"$$tmp/zig.err"; then \
	  cat "$$tmp/zig.err" >&2; \
	  if grep -q "FileNotFound" "$$tmp/zig.err" && grep -q "libcompiler_rt.a" "$$tmp/zig.err"; then \
	    echo "Zig shared cache has a missing derived archive; retrying this generator with disposable cache" >&2; \
	    $(ZIG) run --cache-dir "$$tmp/zig-local" --global-cache-dir "$$tmp/zig-global" "$(DOMAIN_PROFILE_COMPILER)" -- "$$tmp/input.json" "$$tmp/profiles.h"; \
	  else exit 1; fi; \
	fi; \
	printf '%s\n' '#include "profiles.h"' 'int main(void) { return BF_NATIVE_DOMAIN_PROFILE_COUNT == 0; }' > "$$tmp/check.c"; \
	$(CC) $(CFLAGS) -std=c11 -Wall -Wextra -Werror -fsyntax-only "$$tmp/check.c"; \
	if ! cmp -s "$$tmp/profiles.h" "$@"; then mv -f "$$tmp/profiles.h" "$@"; fi

.PHONY: domain-profiles-generate
# Explicit recovery also handles a newer-but-stale retained header. The normal
# build follows declared inputs; the comparison preserves an unchanged header.
domain-profiles-generate:
	@$(MAKE) --no-print-directory -B "$(DOMAIN_PROFILE_HEADER)"
