# Independent bounded native/publication witness, kept separate from broad
# SurfaceScene browser suites so release safety remains verifiable under load.
CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -Werror
MD_ROOT = ../BonfyreSurfaceScene
MD_VENDOR = $(MD_ROOT)/vendor/md4c
MD_SOURCES = $(MD_ROOT)/src/authoring_markdown.c \
 $(MD_VENDOR)/md4c.c $(MD_VENDOR)/md4c-html.c $(MD_VENDOR)/entity.c
MARKDOWN = bonfyre-markdown
.PHONY: all test clean
all: test
$(MARKDOWN): src/markdown.c $(MD_SOURCES) $(MD_ROOT)/src/authoring_markdown.h
	$(CC) $(CFLAGS) -I$(MD_VENDOR) src/markdown.c $(MD_SOURCES) -o $@
test: $(MARKDOWN)
	@tmp=$$(mktemp /tmp/bonfyre-release-md-XXXXXX); trap 'rm -f "$$tmp"' EXIT; \
	  printf '%s\n' '# Original article' 'Source **verified** and retained.' | ./$(MARKDOWN) --fragment > "$$tmp" && \
	  grep -q '<strong>verified</strong>' "$$tmp" && \
	  ! printf '%s\n' '[unsafe](javascript:alert(1))' | ./$(MARKDOWN) --fragment > "$$tmp" 2>/dev/null && \
	  test ! -s "$$tmp"
	@bun test ../BonfyreSurfaceScene/tests/authored-site-release.test.mjs
	@bun test tests/section_runtime_parity.test.mjs
	@node --test ../../scripts/tests/verify-authored-release.test.mjs
clean:
	rm -f $(MARKDOWN)
