# Simple case, build test framework

RUSTC = rustc
RUST_FLAGS := -g -O

RUST_FLAGS := $(RUST_FLAGS) -C prefer-dynamic

test_lib: lib.rs
	$(RUSTC) $(RUST_FLAGS) --dep-info deps.mk --test -o $@ lib.rs

# This isn't quite right.  If the deps are removed, but not test_lib,
# it won't decide to compile the library.
-include deps.mk

clean:
	rm -f deps.mk test_lib

.PHONY: clean
