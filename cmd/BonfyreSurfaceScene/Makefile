CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../lib/libbonfyre/include
TARGET = bonfyre-surface-scene
SRC = src/main.c
LIB = ../../lib/libbonfyre/libbonfyre.a

all: $(TARGET) authoring-bundle test

$(TARGET): $(SRC) $(LIB)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LIB) -lsqlite3

$(LIB):
	$(MAKE) -C ../../lib/libbonfyre

AUTHORING_SRC = ../../components/BonfyreServiceSite/authoring
AUTHORING_JS = ../../site/matrix/authoring-editor.js
AUTHORING_CSS = ../../site/matrix/authoring-editor.css
.PHONY: authoring-bundle
authoring-bundle: $(AUTHORING_JS) $(AUTHORING_CSS)
$(AUTHORING_JS): $(AUTHORING_SRC)/entry.mjs $(AUTHORING_SRC)/launch.mjs $(AUTHORING_SRC)/unified-editor.mjs $(AUTHORING_SRC)/native-cms.mjs $(AUTHORING_SRC)/package.json runtime/authoring-tree.mjs runtime/authoring-prototype.mjs
	cd $(AUTHORING_SRC) && bun install --frozen-lockfile --ignore-scripts
	cd $(AUTHORING_SRC) && bun build ./entry.mjs --target browser --minify --outfile ../../../site/matrix/authoring-editor.js
$(AUTHORING_CSS): $(AUTHORING_JS) $(AUTHORING_SRC)/authoring.css
	@{ cat $(AUTHORING_SRC)/node_modules/grapesjs/dist/css/grapes.min.css; \
	   sed '/^@import "https:\/\/rsms\.me\/inter\/inter\.css";/d' $(AUTHORING_SRC)/node_modules/@puckeditor/core/dist/index.css; \
	   cat $(AUTHORING_SRC)/authoring.css; } > $(AUTHORING_CSS)

test: $(TARGET)
	@node --test tests/authoring_prototype.test.mjs
	@node --test tests/authoring-tree.test.mjs
	@node --test tests/authoring-cms.test.mjs
	@node tools/compile_authoring.mjs --check
	@node tools/compile_authoring_native_tree.mjs --check
	@tmp=$$(mktemp /tmp/bonfyre-native-authoring-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror tests/native_authoring_tree_test.c -o "$$tmp" && "$$tmp"
	@tmp=$$(mktemp /tmp/bonfyre-authoring-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror tests/native_authoring_contract_test.c -o "$$tmp" && "$$tmp"
	@node ../../scripts/audit-native-library-candidates.mjs --verify
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
