# Simple case, build test framework

RUSTC = rustc
RUST_FLAGS := -g -O

RUST_FLAGS := $(RUST_FLAGS) -C prefer-dynamic

test_lib:
	$(RUSTC) $(RUST_FLAGS) --dep-info deps.mk --test -o $@ lib.rs

-include deps.mk
