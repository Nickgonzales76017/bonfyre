TARGET = target/release/bonfyre-restate-native

.PHONY: all check test release clean
all: $(TARGET)
$(TARGET): Cargo.toml src/main.rs
	cargo build --release
check:
	cargo check
test: release
	./tests/discovery.sh
release: $(TARGET)
clean:
	cargo clean
