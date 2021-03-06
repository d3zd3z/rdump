# Simple case, build test framework

all: cargo-all
test: cargo-test

# Cargo doesn't give much control over the build, and doesn't quite
# get it right, either.
CARGO = cargo
CARGO_TEST = cargo test

cargo-all:
	$(CARGO) build

cargo-test:
	$(CARGO_TEST)

.PHONY: all test

SQLITE3_PATH = ../rustsqlite

RUSTC = rustc
RUST_FLAGS := -g -O

# RUST_FLAGS := $(RUST_FLAGS) -C prefer-dynamic

BINS = rdump
DEPS = -L target/deps -L target
SRC = $(shell find src -name '*.rs' -not -path 'src/bin*')
SQLITE3 = target/deps/libsqlite3.rlib
LIBPOOL = target/libpool.rlib
BIN_TARGETS = $(patsubst %,target/%,$(BINS))
TEST_TARGET = target/tests/pool

rust-all: $(BIN_TARGETS) $(TEST_TARGET)

# test: $(TEST_TARGET)

$(SQLITE3): $(wildcard $(SQLITE3_PATH)/src/*.rs)
	mkdir -p target/deps
	$(RUSTC) $(RUST_FLAGS) --out-dir target/deps $(SQLITE3_PATH)/src/sqlite3.rs

$(LIBPOOL): $(SRC) $(SQLITE3)
	mkdir -p target
	$(RUSTC) $(RUST_FLAGS) --out-dir target $(DEPS) src/libpool/lib.rs

$(BIN_TARGETS): target/%: src/bin/%.rs $(SQLITE3) $(LIBPOOL)
	$(RUSTC) $(RUST_FLAGS) $(DEPS) --out-dir target $<

target/tests/pool: $(SRC) $(SQLITE3)
	mkdir -p target/tests
	$(RUSTC) $(RUST_FLAGS) --test --out-dir target/tests $(DEPS) src/libpool/lib.rs

test_lib: lib.rs
	$(RUSTC) $(RUST_FLAGS) --dep-info deps.mk --test -o $@ lib.rs

clean:
	rm -rf target

.PHONY: clean
