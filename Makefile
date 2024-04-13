.PHONY: fmt clippy clean build pack all test ci

all: clean fmt clippy test pack

ci: fmt clippy test

fmt:
	cargo fmt --all --

clippy:
	cargo clippy --  -D warnings

clean:
	rm -rf ./target

build:
	cargo build

pack:
	cargo build --release

test:
	cargo test --all
