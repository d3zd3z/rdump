# Simple case, build test framework

all: rust-all

# Cargo doesn't give much control over the build, and doesn't quite
# get it right, either.
CARGO = cargo
CARGO_TEST = cargo-test

cargo-all:
	$(CARGO) build -v

cargo-test:
	$(CARGO_TEST) -v

.PHONY: all test

SQLITE3_PATH = ../rustsqlite

RUSTC = rustc
RUST_FLAGS := -g -O

# RUST_FLAGS := $(RUST_FLAGS) -C prefer-dynamic

BINS = rdump
DEPS = -L target/deps -L target
SRC = $(shell find src -name '*.rs' -not -path 'src/bin*')
SQLITE3 = target/deps/$(shell $(RUSTC) --print-file-name $(SQLITE3_PATH)/src/sqlite3/lib.rs)
LIBPOOL = target/$(shell $(RUSTC) --crate-type=lib --print-file-name src/libpool/lib.rs)
BIN_TARGETS = $(patsubst %,target/%,$(BINS))
TEST_TARGET = target/tests/pool

rust-all: $(BIN_TARGETS) $(TEST_TARGET)

test: $(TEST_TARGET)

$(SQLITE3): $(wildcard $(SQLITE3_PATH)/src/sqlite3/*.rs)
	mkdir -p target/deps
	$(RUSTC) $(RUST_FLAGS) --out-dir target/deps $(SQLITE3_PATH)/src/sqlite3/lib.rs

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

# This isn't quite right.  If the deps are removed, but not test_lib,
# it won't decide to compile the library.
-include deps.mk

clean:
	rm -f deps.mk test_lib

.PHONY: clean
