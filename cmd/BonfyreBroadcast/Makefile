CC := cc
CFLAGS := -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include -I../../engine/core/include
LDFLAGS := ../../engine/core/libbonfyre-fabric.a -L../../lib/libbonfyre -lbonfyre -lsqlite3

SRC := src/main.c
BUILD := build
BIN := bonfyre-broadcast
PROGRAMS := generated/programs.json

.PHONY: all clean test programs

all: $(BUILD)/$(BIN) $(PROGRAMS)

$(BUILD)/$(BIN): $(SRC) src/narration_intake.h src/narration_worklist.h src/waveform.h src/media_asset.h src/editorial_mix.h src/narration_assembly.h | $(BUILD)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(BUILD):
	mkdir -p $(BUILD)

# The program clocks, engines and pools are records; this is their export.
$(PROGRAMS): ../../schemas/control/broadcast-programs.cue
	cd ../.. && cue export ./schemas/control -e broadcast --out json > cmd/BonfyreBroadcast/$(PROGRAMS)

programs: $(PROGRAMS)

test: $(BUILD)/$(BIN) $(PROGRAMS)
	sh tests/broadcast_test.sh $(BUILD)/$(BIN)
	sh ../../scripts/tests/static_media_export_test.sh
	sh tests/narration_intake_test.sh $(BUILD)/$(BIN)
	$(CC) $(CFLAGS) -o $(BUILD)/article-revision-test tests/article_revision_test.c $(LDFLAGS)
	$(BUILD)/article-revision-test
	sh tests/editorial_mix_test.sh $(BUILD)/$(BIN)
	sh tests/narration_assembly_test.sh $(BUILD)/$(BIN)
	@bun test tests/editorial_music_library.test.mjs
	@cd ../.. && bun scripts/release/public_surface_rollout.mjs status
	@cd ../.. && bun scripts/release/local_candidate_inventory.mjs

clean:
	rm -rf $(BUILD)
