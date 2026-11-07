# bonfyre-atlas: the Architecture Atlas in C (validate, export, query, wiring).
# The field contract (architecture/atlas-fields.cue) is compiled in with its
# digest; a changed CUE is re-exported at run time instead of trusted stale.
CC      ?= cc
LIB      = ../../lib/libbonfyre
CUE_SRC  = ../../architecture/atlas-fields.cue
CFLAGS  ?= -Os -Wall -Wextra -Werror -std=gnu11 -D_GNU_SOURCE -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE
CFLAGS  += -ffp-contract=off -I$(LIB)/include -Ibuild -flto -ffunction-sections -fdata-sections
# Dense one-statement-per-clause C is deliberate here; GCC reads it as misleading indentation.
CFLAGS  += -Wno-misleading-indentation
LDFLAGS += -flto
ifeq ($(shell uname),Darwin)
LDFLAGS += -Wl,-dead_strip
else
LDFLAGS += -Wl,--gc-sections
endif
TARGET   = bonfyre-atlas

.PHONY: all clean test install

all: $(TARGET)

build/fields.h: $(CUE_SRC)
	@mkdir -p build
	cue export $(CUE_SRC) > build/fields.json
	{ echo '/* generated from atlas-fields.cue by make; do not edit */'; \
	  echo "static const char atlas_fields_cue_sha256[] = \"$$(shasum -a 256 < $(CUE_SRC) | cut -d' ' -f1)\";"; \
	  echo 'static const unsigned char atlas_fields_json[] = {'; \
	  od -An -v -tx1 build/fields.json | sed 's/\([0-9a-f][0-9a-f]\)/0x\1,/g'; \
	  echo '0x00};'; } > $@

$(LIB)/libbonfyre.a:
	$(MAKE) -C $(LIB)

$(TARGET): build/fields.h src/main.c src/witness_grade.h src/build_edges.h src/contracts.h src/contracts_templates.h $(LIB)/libbonfyre.a
	$(CC) $(CFLAGS) src/main.c $(LIB)/libbonfyre.a $(LDFLAGS) -lm -o $@

test: $(TARGET)
	env -u BONFYRE_ATLAS_DIR -u BONFYRE_REPO_ROOT -u BONFYRE_ROOT bash tests/parity.sh
	sh tests/backing.sh
	sh tests/witness-grade.sh
	sh tests/build-edges.sh
	sh tests/contracts.sh

install: $(TARGET)
	@dest=$${DEST:-$$HOME/.bonfyre/bin}; mkdir -p "$$dest"; \
	install -m 755 $(TARGET) "$$dest/.$(TARGET).new" && mv -f "$$dest/.$(TARGET).new" "$$dest/$(TARGET)"; \
	echo "installed $$dest/$(TARGET)"

clean:
	rm -rf build $(TARGET)
