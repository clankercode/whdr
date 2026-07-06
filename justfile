# whdr common workflows.
# Builds and tests are capped at 2 threads (`-j 2`) per system etiquette.

# List available recipes.
default:
    @just --list

# Build the whole workspace (all bins), release-less debug profile.
build:
    cargo build --workspace -j 2

# Build release binaries (what install-service.sh installs).
build-release:
    cargo build --workspace --bins --release -j 2

# Run the full test suite (mirrors CI: --all-features).
test:
    cargo test --workspace --all-features -j 2

# Format all code in place.
fmt:
    cargo fmt --all

# Check formatting without writing (mirrors CI).
fmt-check:
    cargo fmt --all --check

# Lint with warnings denied (mirrors CI).
clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Fast type-check without producing binaries.
check:
    cargo check --workspace --all-targets -j 2

# Run everything CI runs, in CI order (fmt-check, clippy, test).
ci: fmt-check clippy test

# Preview the systemd install plan without touching the machine.
install-dry:
    scripts/install-service.sh --dry-run
