CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
LIB := $(ROOT)/lib/libbonfyre
EXPERIENCE := $(ROOT)/cmd/BonfyreExperienceMap/src
PROGRAM := bonfyre-experience-binding
DOMAIN_JSON := generated/native-domain-profiles.json
DOMAIN_HEADER := generated/native_domain_profiles.h
SRC := src/main.c src/domain_binding.c src/html_metadata.c $(EXPERIENCE)/protocol_binding.c $(EXPERIENCE)/json_canon.c $(LIB)/src/bf_sha256.c

.PHONY: all clean test

all: $(PROGRAM)

$(DOMAIN_JSON): domain-profile.cue
	mkdir -p generated
	cue export domain-profile.cue -e nativeDomainProfiles --out json > $@

$(DOMAIN_HEADER): $(DOMAIN_JSON) tools/compile_domain_profiles.zig
	zig run tools/compile_domain_profiles.zig -- $(DOMAIN_JSON) $(DOMAIN_HEADER)

$(PROGRAM): $(SRC) $(DOMAIN_HEADER)
	$(CC) $(CFLAGS) -I$(EXPERIENCE) -I$(LIB)/include -o $@ $(SRC) -lsqlite3

test: $(PROGRAM)
	sh ./tests/native_binding_test.sh ./$(PROGRAM)

clean:
	rm -f $(PROGRAM) $(DOMAIN_JSON) $(DOMAIN_HEADER)
