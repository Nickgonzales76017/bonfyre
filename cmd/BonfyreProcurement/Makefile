CC      ?= cc
.DEFAULT_GOAL := verify
LIB      = ../../lib/libbonfyre
CORE     = ../../engine/core
CFLAGS  ?= -Os -Wall -Wextra -Werror -std=gnu11 -D_GNU_SOURCE -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE
CFLAGS  += -I$(LIB)/include -I$(CORE)/include -Isrc -flto -ffunction-sections -fdata-sections
SODIUM_CFLAGS := $(shell pkg-config --cflags libsodium 2>/dev/null)
SODIUM_LIBS := $(shell pkg-config --libs libsodium 2>/dev/null || echo -lsodium)
LDFLAGS += -flto
ifeq ($(shell uname),Darwin)
LDFLAGS += -Wl,-dead_strip
else
LDFLAGS += -Wl,--gc-sections
endif
TARGET   = bonfyre-procurement
WASM     = wasm/procurement.wasm
WASM_SRC = wasm/export.c src/procurement.c $(LIB)/bf_jv.c $(LIB)/bf_json.c $(LIB)/bf_jsonw.c $(LIB)/src/bf_sha256.c
WASM_EXP = -Wl,--export=bf_procurement_wasm_run -Wl,--export=bf_procurement_wasm_reset \
           -Wl,--export=bf_procurement_wasm_output -Wl,--export=bf_procurement_wasm_output_len \
           -Wl,--export=bf_procurement_wasm_error -Wl,--export=bf_procurement_wasm_error_len \
           -Wl,--export=malloc -Wl,--export=free -Wl,--export-memory

.PHONY: surface-acceptance
surface-acceptance:
	@test -n "$(RELEASE)" || (echo "RELEASE=/absolute/retained/release is required" >&2; exit 2)
	@test -n "$(GROUP)" || (echo "GROUP=<acceptance-group-index> is required" >&2; exit 2)
	bun ../../scripts/release/public_surface_rollout.mjs verify-group "$(RELEASE)" "$(GROUP)"

.PHONY: verify all clean test
verify: $(TARGET) tests/procurement_test tests/effect_test.sh $(WASM) tests/wasm_import_test
	./tests/procurement_test
	./tests/effect_test.sh ./$(TARGET)
	./tests/wasm_import_test $(WASM)
	@node -e 'const fs=require("fs"),crypto=require("crypto"); const b=fs.readFileSync("$(WASM)"); let inst; const wasi={fd_prestat_get:()=>8,fd_prestat_dir_name:()=>8,proc_exit:c=>{throw Error("wasi exit "+c)},random_get:(p,n)=>{crypto.randomFillSync(new Uint8Array(inst.exports.memory.buffer,p,n));return 0}}; WebAssembly.instantiate(b,{wasi_snapshot_preview1:wasi}).then(({instance:i})=>{inst=i;if(i.exports._initialize)i.exports._initialize();for(const n of ["memory","malloc","free","bf_procurement_wasm_run","bf_procurement_wasm_error"])if(!(n in i.exports))throw Error("missing "+n);const p=i.exports.malloc(2);new Uint8Array(i.exports.memory.buffer,p,2).set([123,125]);if(i.exports.bf_procurement_wasm_run(1,p,2,0,0)!==0)throw Error("invalid RFQ accepted");i.exports.free(p);console.log("procurement wasm browser ABI: ok")})'
	bun test ../../integrations/browser/tests/procurement-wasm.test.mjs ../../integrations/browser/tests/procurement-enactment-client.test.mjs
	bun test ../../scripts/tests/room-procurement.test.mjs
	bun test ../../scripts/tests/procurement-downstream-workspaces.test.mjs
	bun test ../../scripts/release/tests/workspace_upgrade.test.mjs
	bun test ../../scripts/release/tests/public_surface_rollout.test.mjs
	bun ../../scripts/release/public_surface_rollout.mjs status
	$(MAKE) -C ../BonfyreWebAddress bonfyre-web-address
	bun test ../BonfyreWebAddress/tests/procurement_approval_status.test.mjs ../BonfyreWebAddress/tests/procurement_downstream_status.test.mjs ../BonfyreWebAddress/tests/procurement_enactment_readiness.test.mjs ../BonfyreWebAddress/tests/procurement_enactment_effect.test.mjs
	BONFYRE_ROOM_BASE_ROOT="$(HOME)/.bonfyre/workspace-packages" bun test ../../scripts/tests/room-procurement-browser.test.mjs
	@if command -v cue >/dev/null 2>&1; then cue vet -c=false contracts/procurement.cue; fi

all: $(TARGET)

$(LIB)/libbonfyre.a:
	$(MAKE) -C $(LIB)

$(CORE)/libbonfyre-fabric.a:
	$(MAKE) -C $(CORE)

$(TARGET): src/main.c src/procurement.c src/procurement.h src/effect.c src/effect.h $(LIB)/libbonfyre.a $(CORE)/libbonfyre-fabric.a
	$(CC) $(CFLAGS) $(SODIUM_CFLAGS) src/main.c src/procurement.c src/effect.c $(LIB)/libbonfyre.a $(CORE)/libbonfyre-fabric.a $(LDFLAGS) -lm -lpthread -lsqlite3 $(SODIUM_LIBS) -o $@

tests/procurement_test: tests/procurement_test.c src/procurement.c src/procurement.h $(LIB)/libbonfyre.a
	$(CC) $(CFLAGS) tests/procurement_test.c src/procurement.c $(LIB)/libbonfyre.a $(LDFLAGS) -lm -lpthread -lsqlite3 -o $@

$(WASM): $(WASM_SRC) src/procurement.h
	@test "$$(zig version)" = "0.13.0" || (echo "pinned Zig 0.13.0 required" >&2; exit 1)
	zig cc -target wasm32-wasi -mexec-model=reactor -O3 -g0 -ffp-contract=off -I$(LIB)/include -Isrc $(WASM_SRC) -lm \
		-Wl,--no-entry $(WASM_EXP) -Wl,--strip-all -Wl,--max-memory=0x2000000 -o $@

tests/wasm_import_test: tests/wasm_import_test.c
	$(CC) -Os -Wall -Wextra -Werror -std=c11 $< -o $@

test: verify

clean:
	rm -f $(TARGET) tests/procurement_test tests/wasm_import_test $(WASM)
