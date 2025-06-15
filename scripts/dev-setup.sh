    #!/bin/bash

HOOK_FILE=".git/hooks/pre-commit"
cat > "$HOOK_FILE" << 'EOF'
#!/bin/bash
set -e

# Run Rustfmt
cargo fmt -- --check
if ! git diff --exit-code; then
    echo "ERROR: Rust code was formatted. Please rereview and re-commit."
    exit 1
fi

# Run Clippy
cargo clippy -- -D warnings

# Run tests
cargo test

EOF
chmod +x "$HOOK_FILE"
echo "INFO: pre-commit hook installed."