CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../lib/libbonfyre/include
TARGET = bonfyre-surface-scene
SRC = src/main.c
LIB = ../../lib/libbonfyre/libbonfyre.a

all: $(TARGET) test

$(TARGET): $(SRC) $(LIB)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LIB) -lsqlite3

$(LIB):
	$(MAKE) -C ../../lib/libbonfyre

test: $(TARGET)
	@sh tests/surface_scene_test.sh
	@node tests/surface_runtime_test.mjs
	@node --test tests/reusable-interactions.test.mjs
	@node --test tests/workspace-context.test.mjs
	@node tests/workspace_native_test.mjs
	@sh tests/compile_neighbourhood_native_test.sh
	@sh tests/compile_kin_native_test.sh
	@bun test tests/stage_closure.test.mjs
	@node tests/stage_compute_test.mjs
	@node tests/stitch_composition_test.mjs
	@node tests/experience_composition_test.mjs
	@bun ../BonfyreWebAddress/tools/compile_experience_programs.mjs ../BonfyreWebAddress/generated/native_page_composition.h ../BonfyreWebAddress/generated/native_experience_composition.h --check
	@tmp=$$(mktemp /tmp/bonfyre-exp-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror ../BonfyreWebAddress/tests/native_experience_composition_test.c -o "$$tmp" && "$$tmp"
	@tmp=$$(mktemp /tmp/bonfyre-apex-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror ../BonfyreWebAddress/tests/route_presentation_test.c ../BonfyreWebAddress/src/site_presentation.c -lsqlite3 -o "$$tmp" && "$$tmp"
	@node tests/native_experience_browser_test.mjs
	@node tests/stitch_dom_test.mjs
	@node tests/stitch_workspace_test.mjs
	@node tests/stitch_native_owner_test.mjs

clean:
	rm -f $(TARGET)

.PHONY: all test clean

check: test
