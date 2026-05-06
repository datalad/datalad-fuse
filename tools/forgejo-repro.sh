#!/bin/bash
# Minimal reproducer for the Forgejo-aneksajo test workflow.
#
# Starts a headless Forgejo-aneksajo container, creates a public repo
# via the API, pushes a basic commit, clones it anonymously (no creds)
# to confirm it is public, then tears everything down.
#
# A second (opt-in) phase attempts the git-annex piece: init an annex
# in the local repo, push, and try to fetch content from the aneksajo
# annex/objects endpoint.  Enable with ANNEX=1.
#
# The script overrides HOME to a clean temp directory so results are
# not affected by the user's personal git config or credentials.
#
# Usage:
#   tools/forgejo-repro.sh                    # basic git push/pull round-trip
#   ANNEX=1 tools/forgejo-repro.sh            # also exercise git-annex
#   ANNEX=1 NO_CRED_HELPER=1 tools/forgejo-repro.sh
#                                             # demonstrate the credential bug
#   KEEP=1 tools/forgejo-repro.sh             # leave container running
#   RUNTIME=docker tools/forgejo-repro.sh     # force docker
#
# Environment variables:
#   IMAGE             — container image (default: aneksajo v14.0.3-git-annex2)
#   RUNTIME           — podman or docker (auto-detected)
#   KEEP=1            — keep container + workdir after exit
#   ANNEX=1           — run git-annex phase
#   NO_CRED_HELPER=1  — skip credential-store setup (demonstrates failure)
#
# Exits 0 on success, non-zero with a clear error message otherwise.

set -euo pipefail

# --- Configuration --------------------------------------------------------

# The bare "forgejo-rootless" tag is the *upstream* Forgejo image —
# not the aneksajo build.  Pin explicitly to a git-annex tag.
IMAGE="${IMAGE:-codeberg.org/forgejo-aneksajo/forgejo-aneksajo:v14.0.3-git-annex2-rootless}"
CONTAINER_NAME="${CONTAINER_NAME:-datalad-fuse-forgejo-repro}"
INTERNAL_PORT=3000
ADMIN_USER="${ADMIN_USER:-testadmin}"
ADMIN_PASS="${ADMIN_PASS:-testpass123!}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@test.nil}"
REPO_NAME="${REPO_NAME:-repro-$(date +%s)}"
RUNTIME="${RUNTIME:-}"
KEEP="${KEEP:-0}"
ANNEX="${ANNEX:-0}"
NO_CRED_HELPER="${NO_CRED_HELPER:-0}"

WORKDIR="$(mktemp -d -t forgejo-repro.XXXXXX)"
LOCAL_REPO="$WORKDIR/local"
CLONE_DIR="$WORKDIR/clone"

log()  { printf '\033[1;34m[repro]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m[FAIL]\033[0m  %s\n' "$*" >&2; exit 1; }
pass() { printf '\033[1;32m[OK]\033[0m    %s\n' "$*" >&2; }

# Prepare a clean HOME for git/git-annex operations (applied later,
# after the container is running — podman needs the real HOME for its
# image cache under ~/.local/share/containers/).
FAKE_HOME="$WORKDIR/home"
mkdir -p "$FAKE_HOME"
cat > "$FAKE_HOME/.gitconfig" << 'GITCFG'
[annex "security"]
    allowed-url-schemes = http https file
    allowed-http-addresses = all

[user]
    name = Test User
    email = test@test.nil
GITCFG

log "git-annex version: $(git annex version --raw)"

# --- Runtime detection ----------------------------------------------------

if [[ -z "$RUNTIME" ]]; then
    for rt in podman docker; do
        if command -v "$rt" >/dev/null 2>&1; then RUNTIME="$rt"; break; fi
    done
fi
[[ -n "$RUNTIME" ]] || fail "No container runtime (podman/docker) found"
log "Using runtime: $RUNTIME"

# --- Cleanup trap ---------------------------------------------------------

cleanup() {
    local rc=$?
    if [[ "$KEEP" == "1" ]]; then
        log "KEEP=1 — leaving container $CONTAINER_NAME running"
        log "       workdir: $WORKDIR"
    else
        log "Cleaning up…"
        "$RUNTIME" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
        # git-annex stores objects read-only (including parent dirs),
        # so rm -rf fails unless we restore write permission first.
        chmod -R u+w "$WORKDIR" 2>/dev/null || true
        rm -rf "$WORKDIR"
    fi
    exit "$rc"
}
trap cleanup EXIT

# --- Start container ------------------------------------------------------

# Nuke a stale container with the same name (ok if it doesn't exist).
"$RUNTIME" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

log "Starting $IMAGE …"
CID=$("$RUNTIME" run -d \
    --name "$CONTAINER_NAME" \
    -p "$INTERNAL_PORT" \
    -e FORGEJO__security__INSTALL_LOCK=true \
    "$IMAGE")
log "Container: ${CID:0:12}"

# Extract the mapped host port.
MAPPED=$("$RUNTIME" port "$CID" "$INTERNAL_PORT" | head -n1)
HOST_PORT="${MAPPED##*:}"
[[ -n "$HOST_PORT" ]] || fail "Could not determine host port (got: $MAPPED)"
URL="http://127.0.0.1:$HOST_PORT"
log "Forgejo URL: $URL"

# --- Wait for readiness ---------------------------------------------------

log "Waiting for Forgejo API …"
for i in {1..30}; do
    if curl -fsS "$URL/api/forgejo/v1/version" >/dev/null 2>&1; then
        pass "API responsive after ${i}s"
        break
    fi
    sleep 2
    [[ $i -eq 30 ]] && fail "Forgejo did not become ready in 60s"
done

VERSION=$(curl -fsS "$URL/api/forgejo/v1/version" | jq -r .version)
log "Version: $VERSION"
if [[ "$VERSION" == *git-annex* ]]; then
    pass "aneksajo detected (version contains 'git-annex')"
else
    log "WARN: version string does not contain 'git-annex' — may not be aneksajo"
fi

# --- Create admin user ----------------------------------------------------

log "Creating admin user '$ADMIN_USER' …"
"$RUNTIME" exec "$CID" forgejo admin user create \
    --admin \
    --username "$ADMIN_USER" \
    --password "$ADMIN_PASS" \
    --email "$ADMIN_EMAIL" \
    --must-change-password=false

# --- Create API token -----------------------------------------------------

log "Creating API token …"
TOKEN=$(curl -fsS -u "$ADMIN_USER:$ADMIN_PASS" \
    -H 'Content-Type: application/json' \
    -X POST "$URL/api/v1/users/$ADMIN_USER/tokens" \
    -d "{\"name\":\"repro-$(date +%s)\",\"scopes\":[\"all\"]}" \
    | jq -r .sha1)
[[ -n "$TOKEN" && "$TOKEN" != "null" ]] || fail "Failed to obtain API token"
pass "Got token (${TOKEN:0:8}…)"

# --- Create public repo via API -------------------------------------------

log "Creating public repo '$REPO_NAME' …"
curl -fsS \
    -H "Authorization: token $TOKEN" \
    -H 'Content-Type: application/json' \
    -X POST "$URL/api/v1/user/repos" \
    -d "{\"name\":\"$REPO_NAME\",\"auto_init\":false,\"private\":false,\"default_branch\":\"main\"}" \
    >/dev/null

# Forgejo's API returns clone_url with the container-internal host/port.
# Construct it directly from the host-mapped URL instead.
CLONE_URL="$URL/$ADMIN_USER/$REPO_NAME.git"
log "Clone URL: $CLONE_URL"

# Auth URL: token in USERNAME field, empty password.
# (Works around git-annex bug where "..." is sent instead of the password.)
AUTH_URL="http://$TOKEN:@127.0.0.1:$HOST_PORT/$ADMIN_USER/$REPO_NAME.git"

# --- Switch to clean HOME ------------------------------------------------
# Now that the container is running (podman used the real HOME for its
# image cache), switch to the fake HOME so git/git-annex operations
# run in a pristine environment — mirroring the pytest tmp_home fixture.
export HOME="$FAKE_HOME"
export USERPROFILE="$FAKE_HOME"
unset XDG_CONFIG_HOME 2>/dev/null || true
export GIT_TERMINAL_PROMPT=0
log "HOME switched to $FAKE_HOME (clean, no credentials)"

# --- Local repo, basic commit, push ---------------------------------------

log "Building local repo at $LOCAL_REPO …"
mkdir -p "$LOCAL_REPO"
cd "$LOCAL_REPO"
git init -q -b main
echo "# $REPO_NAME" > README.md
git add README.md
git commit -q -m "Initial commit"

log "Pushing to Forgejo (token:@ auth) …"
git push -q "$AUTH_URL" main

pass "Push succeeded"

# --- Clone anonymously (no creds) -----------------------------------------

log "Cloning anonymously from $CLONE_URL …"
git clone -q "$CLONE_URL" "$CLONE_DIR" \
    || fail "Anonymous clone failed — repo is not public?"

[[ -f "$CLONE_DIR/README.md" ]] || fail "README.md missing in anonymous clone"
pass "Anonymous clone works — repo is public"

# --- Optional: exercise git-annex -----------------------------------------

if [[ "$ANNEX" == "1" ]]; then
    log "--- ANNEX phase -------------------------------------------------"
    cd "$LOCAL_REPO"
    git annex init -q "repro-local"

    # Allow localhost connections for p2p endpoint discovery.
    git config annex.security.allowed-ip-addresses 127.0.0.1

    # Create a binary-ish file and commit via git-annex.
    printf 'annexed content %s\n' "$(date)" > data.bin
    git annex add -q data.bin
    git commit -q -m "Add annexed data"

    # Configure a dedicated remote with auth on pushurl.
    git remote add forgejo "$CLONE_URL"
    git config remote.forgejo.pushurl "$AUTH_URL"

    # git-annex probes the remote's .git/config via `git credential fill`.
    # With GIT_TERMINAL_PROMPT=0 and no credential helper, that exits 128
    # and git-annex marks the remote annex-ignore — even though the
    # resource is publicly accessible.  Set up credential-store so
    # git-annex can authenticate when probing.
    #
    # NO_CRED_HELPER=1 skips this to demonstrate the bug.
    if [[ "$NO_CRED_HELPER" == "1" ]]; then
        log "NO_CRED_HELPER=1 — skipping credential-store (expect failure)"
    else
        git config --global credential.helper store
        echo "$AUTH_URL" > "$HOME/.git-credentials"
    fi

    # Push both main and git-annex branches (aneksajo does NOT
    # auto-create a git-annex branch), then fetch + init so the
    # remote UUID is discovered.  Retry — ordering can be racy.
    ANNEX_DEBUG_FLAG=""
    if [[ "$NO_CRED_HELPER" == "1" ]]; then
        ANNEX_DEBUG_FLAG="--debug"
        log "We will set -x to provide details on invocations and show current .git/config"
        set -x
        cat .git/config
    fi
    ok=0
    for i in 1 2 3 4; do
        git push -q forgejo main git-annex 2>&1 || true
        git fetch -q forgejo 2>&1 || true
        # shellcheck disable=SC2086  # intentional word-split of debug flag
        git annex init $ANNEX_DEBUG_FLAG -q "repro-local" 2>&1 \
            | { grep -i -E '(credential|annex-ignore|usable|error|fail|uuid)' || true; } \
            | sed 's/^/  [annex init] /' >&2
        git config --unset remote.forgejo.annex-ignore 2>/dev/null || true
        git config --unset remote.forgejo.annex-ignore-auto 2>/dev/null || true
        if git config --get remote.forgejo.annex-uuid >/dev/null 2>&1; then
            pass "annex-uuid discovered on iter $i"
            ok=1
            break
        fi
        log "iter $i: no annex-uuid yet"
        sleep 0.5
    done
    if [[ $ok -eq 0 ]]; then
        if [[ "$NO_CRED_HELPER" == "1" ]]; then
            log "Expected failure — git-annex could not discover UUID without credential helper"
            log "The remote's .git/config IS publicly accessible:"
            HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$CLONE_URL/config")
            log "  curl $CLONE_URL/config → HTTP $HTTP_CODE"
            log "But git-annex uses 'git credential fill' which exits 128"
            log "in a clean HOME with no helpers and GIT_TERMINAL_PROMPT=0."
            fail "annex-uuid NOT discovered (NO_CRED_HELPER=1 demo)"
        else
            fail "git-annex could not discover remote annex-uuid"
        fi
    fi

    # Fix up the auto-discovered annexurl (uses container-internal port).
    git config remote.forgejo.annexurl "annex+$URL/git-annex-p2phttp"

    git annex copy --to=forgejo data.bin \
        || fail "git annex copy to forgejo failed"
    pass "Annex content transferred"

    # Verify the annex/objects endpoint is reachable anonymously.
    KEY=$(git annex lookupkey data.bin)
    # shellcheck disable=SC2016  # git-annex format placeholders, not shell vars
    OBJECT_PATH=$(git annex examinekey \
        --format='annex/objects/${hashdirlower}${key}/${key}' "$KEY")
    ANNEX_URL="$URL/$ADMIN_USER/$REPO_NAME/$OBJECT_PATH"
    log "Probing $ANNEX_URL"
    HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$ANNEX_URL")
    if [[ "$HTTP_CODE" == "200" ]]; then
        pass "annex/objects endpoint serves content (HTTP 200)"
    else
        fail "annex/objects endpoint returned HTTP $HTTP_CODE"
    fi
fi

pass "All checks passed."
