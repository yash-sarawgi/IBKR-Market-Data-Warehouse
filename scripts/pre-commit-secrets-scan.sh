#!/usr/bin/env bash
# Pre-commit hook: scan staged files for secrets, API keys, passwords, and PII.
# Blocks the commit if any suspicious patterns are found.
# Compatible with macOS grep (uses ERE, no PCRE dependency).

set -euo pipefail

RED='\033[0;31m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
NC='\033[0m'

# Get list of staged files (added/modified, not deleted)
STAGED_FILES=$(git diff --cached --name-only --diff-filter=ACM)

if [ -z "$STAGED_FILES" ]; then
    exit 0
fi

ISSUES_FOUND=0
TMPDIR_SCAN=$(mktemp -d)
trap 'rm -rf "$TMPDIR_SCAN"' EXIT

# ── Pattern definitions (ERE-compatible for macOS grep -E) ───────────

PATTERN_LABELS=(
    "AWS Access Key"
    "AWS Secret Key"
    "Generic API Key assignment"
    "Generic Secret assignment"
    "Generic Password assignment"
    "Private Key header"
    "GitHub Token (ghp/gho/ghu/ghs/ghr)"
    "Slack Token"
    "Google API Key"
    "Connection string with password"
    "IB credentials (hardcoded)"
)

PATTERNS=(
    'AKIA[0-9A-Z]{16,}'
    '(aws_secret_access_key|aws_secret|AWS_SECRET)[[:space:]]*=[[:space:]]*[A-Za-z0-9/+=]{20,}'
    '(api[_-]?key|apikey|API_KEY)[[:space:]]*=[[:space:]]*["'"'"'][A-Za-z0-9_/-]{16,}["'"'"']'
    '(secret[_-]?key|SECRET_KEY|client_secret)[[:space:]]*=[[:space:]]*["'"'"'][A-Za-z0-9_/-]{16,}["'"'"']'
    '(password|passwd|pwd|PASSWORD|PASSWD)[[:space:]]*=[[:space:]]*["'"'"'][^"'"'"']{8,}["'"'"']'
    '-----BEGIN (RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----'
    'gh[pousr]_[A-Za-z0-9_]{36,}'
    'xox[bporas]-[0-9]{10,}-[A-Za-z0-9]{10,}'
    'AIza[0-9A-Za-z_-]{35}'
    '(mysql|postgres|postgresql|mongodb|redis)://[^:]+:[^@]+@'
    '(IbLoginId|IbPassword)[[:space:]]*=[[:space:]]*[^Y[:space:]][^[:space:]]*'
)

# ── File-type exclusions ─────────────────────────────────────────────

should_scan() {
    local file="$1"
    case "$file" in
        *.lock|*.png|*.jpg|*.jpeg|*.gif|*.ico|*.woff|*.woff2|*.ttf|*.eot)
            return 1 ;;
        *.pyc|*.pyo|*.so|*.dylib|*.o|*.a|*.parquet|*.duckdb)
            return 1 ;;
        *)
            return 0 ;;
    esac
}

# ── Allowlist for known false positives ──────────────────────────────

is_allowed() {
    local file="$1"
    local line="$2"

    # Test files with dummy tokens
    if [[ "$file" == tests/* ]]; then
        if echo "$line" | grep -qiE '(test-token|fake|mock|dummy|env-token|test_|_test)'; then
            return 0
        fi
    fi

    # Documentation placeholders
    if echo "$line" | grep -qiE '(YOUR_|your-api-key|placeholder|changeme|CHANGEME|<YOUR|example\.com)'; then
        return 0
    fi

    # .env.example / .env.sample files
    if [[ "$file" == *.env.example ]] || [[ "$file" == *.env.sample ]]; then
        return 0
    fi

    # Comments (Python, Shell, JS, YAML)
    if echo "$line" | grep -qE '^[[:space:]]*(#|//|/\*|\*|--|;)'; then
        return 0
    fi

    # Python env var reads
    if echo "$line" | grep -qE 'os\.(environ|getenv)|environ\.get'; then
        return 0
    fi

    # Error messages / raise statements
    if echo "$line" | grep -qiE '(raise |Error\(|Exception\(|".*export |logging\.)'; then
        return 0
    fi

    # YAML/OpenAPI spec example values
    if [[ "$file" == *.yaml ]] || [[ "$file" == *.yml ]]; then
        if echo "$line" | grep -qiE '(example|description|x-example|abc123|<YOUR_|\$ref)'; then
            return 0
        fi
    fi

    # Method/function signatures accepting sensitive params
    if echo "$line" | grep -qE 'def .*(token|password|secret|key)'; then
        return 0
    fi

    # Assert statements in tests
    if echo "$line" | grep -qE '^[[:space:]]*assert '; then
        return 0
    fi

    return 1
}

# ── Main scan ────────────────────────────────────────────────────────

echo "Scanning staged files for secrets..."

for file in $STAGED_FILES; do
    if ! should_scan "$file"; then
        continue
    fi

    if [ ! -f "$file" ]; then
        continue
    fi

    # Get staged content (what will actually be committed)
    STAGED_CONTENT_FILE="$TMPDIR_SCAN/$(basename "$file")"
    git show ":$file" > "$STAGED_CONTENT_FILE" 2>/dev/null || continue

    for i in "${!PATTERNS[@]}"; do
        MATCHES=$(grep -niE "${PATTERNS[$i]}" "$STAGED_CONTENT_FILE" 2>/dev/null || true)

        if [ -n "$MATCHES" ]; then
            while IFS= read -r match_line; do
                [ -z "$match_line" ] && continue

                LINE_NUM=$(echo "$match_line" | cut -d: -f1)
                LINE_CONTENT=$(echo "$match_line" | cut -d: -f2-)

                # Check allowlist
                if is_allowed "$file" "$LINE_CONTENT"; then
                    continue
                fi

                if [ "$ISSUES_FOUND" -eq 0 ]; then
                    echo ""
                    echo -e "${RED}═══ SECRETS DETECTED — COMMIT BLOCKED ═══${NC}"
                    echo ""
                fi

                ISSUES_FOUND=$((ISSUES_FOUND + 1))
                echo -e "${YELLOW}[${ISSUES_FOUND}] ${PATTERN_LABELS[$i]}${NC}"
                echo -e "    File: ${RED}${file}:${LINE_NUM}${NC}"
                # Truncate long lines
                DISPLAY_LINE=$(echo "$LINE_CONTENT" | cut -c1-120)
                echo -e "    Line: ${DISPLAY_LINE}"
                echo ""
            done <<< "$MATCHES"
        fi
    done
done

# ── .env file check ──────────────────────────────────────────────────

for file in $STAGED_FILES; do
    BASENAME=$(basename "$file")
    if [[ "$BASENAME" == ".env" ]]; then
        if [ "$ISSUES_FOUND" -eq 0 ]; then
            echo ""
            echo -e "${RED}═══ SECRETS DETECTED — COMMIT BLOCKED ═══${NC}"
            echo ""
        fi
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
        echo -e "${YELLOW}[${ISSUES_FOUND}] .env file staged for commit${NC}"
        echo -e "    File: ${RED}${file}${NC}"
        echo -e "    .env files should never be committed. Add to .gitignore."
        echo ""
    fi
done

# ── Result ───────────────────────────────────────────────────────────

if [ "$ISSUES_FOUND" -gt 0 ]; then
    echo -e "${RED}Found ${ISSUES_FOUND} potential secret(s). Commit aborted.${NC}"
    echo ""
    echo "To fix:"
    echo "  1. Remove the secret from the file"
    echo "  2. Use environment variables instead"
    echo "  3. If this is a false positive, add to the allowlist in .git/hooks/pre-commit"
    echo ""
    echo "To bypass (use with caution):"
    echo "  git commit --no-verify"
    echo ""
    exit 1
fi

echo -e "${GREEN}No secrets found. Proceeding with commit.${NC}"
exit 0
