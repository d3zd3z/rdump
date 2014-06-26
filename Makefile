# Simple case, build test framework

RUSTC = rustc
RUST_FLAGS := -g -O

RUST_FLAGS := $(RUST_FLAGS) -C prefer-dynamic

test_lib: .force
	$(RUSTC) $(RUST_FLAGS) --test -o $@ lib.rs

.PHONY: .force
