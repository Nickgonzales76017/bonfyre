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
authoring-bundle: $(AUTHORING_JS) $(AUTHORING_CSS) authoring-reader
$(AUTHORING_JS): $(AUTHORING_SRC)/entry.mjs $(AUTHORING_SRC)/launch.mjs $(AUTHORING_SRC)/unified-editor.mjs $(AUTHORING_SRC)/native-cms.mjs $(AUTHORING_SRC)/journal-import.mjs $(AUTHORING_SRC)/sha256-portable.mjs $(AUTHORING_SRC)/review-preview.mjs $(AUTHORING_SRC)/audio-entry.mjs $(AUTHORING_SRC)/article-narration.mjs $(AUTHORING_SRC)/page-directory.mjs $(AUTHORING_SRC)/package.json runtime/authoring-tree.mjs runtime/authoring-prototype.mjs $(AUTHORING_SRC)/site-section-catalogue.generated.mjs
$(AUTHORING_JS): $(AUTHORING_SRC)/component-palette.mjs
	cd $(AUTHORING_SRC) && bun install --frozen-lockfile --ignore-scripts
	cd $(AUTHORING_SRC) && bun build ./entry.mjs --target browser --minify --outfile ../../../site/matrix/authoring-editor.js
$(AUTHORING_CSS): $(AUTHORING_JS) $(AUTHORING_SRC)/authoring.css
	@{ cat $(AUTHORING_SRC)/node_modules/grapesjs/dist/css/grapes.min.css; \
	   sed '/^@import "https:\/\/rsms\.me\/inter\/inter\.css";/d' $(AUTHORING_SRC)/node_modules/@puckeditor/core/dist/index.css; \
	   cat $(AUTHORING_SRC)/authoring.css; } > $(AUTHORING_CSS)
.PHONY: authoring-reader
authoring-reader:
	@node tools/compile_authoring_reader.mjs --write


.PHONY: release-test
release-test:
	@bun test tests/authored-site-release.test.mjs

.PHONY: markdown-test
markdown-test:
	@tmp=$$(mktemp /tmp/bonfyre-native-md4c-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror -Ivendor/md4c tests/native_authoring_markdown_test.c src/authoring_markdown.c vendor/md4c/md4c.c vendor/md4c/md4c-html.c vendor/md4c/entity.c -o "$$tmp" && "$$tmp"
test: $(TARGET)
	@tmp=$$(mktemp /tmp/bonfyre-native-authoring-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror -I../../lib/libbonfyre/include tests/native_authoring_tree_test.c $(LIB) -lm -o "$$tmp" && "$$tmp"
	@tmp=$$(mktemp /tmp/bonfyre-native-components-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror -I../../lib/libbonfyre/include tests/native_authoring_components_test.c src/authoring_native_components.c src/authoring_editorial.c src/authoring_template.c vendor/mustach/mini-mustach.c $(LIB) -lm -o "$$tmp" && "$$tmp"
	@tmp=$$(mktemp /tmp/bonfyre-public-document-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror -I../../lib/libbonfyre/include tests/native_public_authored_document_test.c ../BonfyreWebAddress/src/site_authored_document.c src/authoring_native_components.c src/authoring_editorial.c src/authoring_template.c vendor/mustach/mini-mustach.c $(LIB) -lsqlite3 -lm -o "$$tmp" && "$$tmp"
	@tmp=$$(mktemp /tmp/bonfyre-template-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror -I../../lib/libbonfyre/include tests/native_authoring_template_test.c src/authoring_template.c vendor/mustach/mini-mustach.c $(LIB) -lm -o "$$tmp" && "$$tmp"
	@tmp=$$(mktemp /tmp/bonfyre-native-md4c-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror -Ivendor/md4c tests/native_authoring_markdown_test.c src/authoring_markdown.c vendor/md4c/md4c.c vendor/md4c/md4c-html.c vendor/md4c/entity.c -o "$$tmp" && "$$tmp"
	@node --test tests/authoring_prototype.test.mjs
	@node --test tests/authoring-tree.test.mjs
	@node --test tests/authoring-cms.test.mjs
	@bun test tests/journal-import.test.mjs
	@bun test tests/audio-entry.test.mjs
	@bun test tests/article-narration.test.mjs
	@node --test tests/authoring-section-parity.test.mjs
	@node --test tests/structured-fields.test.mjs
	@node --test tests/authoring-ids.test.mjs
	@bun test tests/blank-canvas-authoring.test.mjs
	@bun test tests/authored-page-renderer.test.mjs
	@bun test tests/authored-static-candidate.test.mjs
	@bun test tests/authored-site-candidate.test.mjs
	@bun test tests/authored-site-release.test.mjs
	@bun test tests/native_browser_authoring_parity.test.mjs
	@bun test ../BonfyreWebAddress/tests/cms_advanced_composer.test.mjs
	@node tools/compile_authoring_sections.mjs --check
	@node tools/compile_authoring.mjs --check
	@node tools/compile_authoring_native_tree.mjs --check
	@node tools/compile_authoring_native_templates.mjs --check
	@node tools/compile_authoring_reader.mjs --check
	@tmp=$$(mktemp /tmp/bonfyre-native-site-fragment-XXXXXX); trap 'rm -f "$$tmp"' EXIT; $(CC) -std=c11 -Wall -Wextra -Werror -D_DARWIN_C_SOURCE -I../../lib/libbonfyre/include -I../BonfyreWebAddress/src ../BonfyreWebAddress/tests/site_fragment_projection_test.c ../BonfyreWebAddress/src/site_fragment_projection.c src/authoring_template.c vendor/mustach/mini-mustach.c $(LIB) -lsqlite3 -lm -o "$$tmp" && "$$tmp"
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
