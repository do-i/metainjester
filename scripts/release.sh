#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
#
# Release manager.
#
# The interactive front-end lives in menu.toml (lazymenu-cli); this script only
# takes explicit subcommands.
#
# Versioning:
#   - Calendar versioning, year.month.sequence, tagged v<version> (e.g.
#     v2026.8.1). The sequence is computed from existing tags for the current
#     month, so the common case needs no version argument.
#   - Unlike thumbgrid, the version is NOT derived at build time: the Arch
#     package is built from Cargo.toml, and arch-package.yml refuses to build
#     when the tag and Cargo.toml disagree. So a release has to bump Cargo.toml
#     (and Cargo.lock, which records the version too, and which makepkg reads
#     with --frozen). This script makes that bump on develop, as its own commit,
#     and waits for CI on it before tagging.
#
# Branching model:
#   - `develop` is the integration branch, and the branch this script must be
#     run from. Feature branches merge into it.
#   - `main` never receives commits directly. It only ever advances by a
#     fast-forward of `develop` (done here), followed by a tag. A direct push
#     to main breaks that invariant; this script detects it and refuses rather
#     than guessing what to do.
#
# What "cut" does:
#   1. Refuses a dirty tree, or a local develop with unpushed commits (they'd
#      be silently left out), or an origin/main that is not an ancestor of
#      origin/develop (a direct push to main since the last release).
#   2. Bumps Cargo.toml + Cargo.lock to the new version and pushes that commit
#      to develop, unless Cargo.toml is already at the target version.
#   3. Waits for a green checks.yml run on develop's tip.
#   4. Fast-forwards main to that commit, tags it, and pushes both atomically.
#
# Pushing the tag starts arch-package.yml, which builds the package, attaches
# it to the GitHub release, and refreshes the gh-pages pacman repo.
#
# Usage:
#   scripts/release.sh status               # show state, change nothing
#   scripts/release.sh cut                  # cut a release
#   scripts/release.sh cut 2026.8.4         # cut an explicit version
#   scripts/release.sh --dry-run cut        # show what would happen
#   scripts/release.sh --skip-ci-check cut  # bypass the CI gate
#   scripts/release.sh --yes cut            # skip the confirmation prompt

set -euo pipefail

DEFAULT_BRANCH="main"
DEV_BRANCH="develop"
WORKFLOW="checks.yml"
CI_POLL_SECONDS=20
CI_TIMEOUT_SECONDS=1800

DRY_RUN=0
SKIP_CI_CHECK=0
ASSUME_YES=0
EXPLICIT_VERSION=""
COMMAND=""

# Prints this file's leading comment block, so --help is just the header.
print_usage() {
    awk 'NR > 1 { if (/^#/) { sub(/^# ?/, ""); print } else { exit } }' "$0"
}

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        --skip-ci-check) SKIP_CI_CHECK=1 ;;
        --yes|-y) ASSUME_YES=1 ;;
        -h|--help) print_usage; exit 0 ;;
        cut|status) COMMAND="$arg" ;;
        v*) EXPLICIT_VERSION="${arg#v}" ;;
        [0-9]*) EXPLICIT_VERSION="$arg" ;;
        *) echo "error: unknown argument '$arg'" >&2; exit 2 ;;
    esac
done

run() {
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "  [dry-run] $*"
    else
        "$@"
    fi
}

cd "$(git rev-parse --show-toplevel)"

cargo_version() {
    # Only the first `version =`, which is the [package] one; dependency
    # entries below it use the same key.
    sed -n '0,/^version = /s/^version = "\(.*\)"/\1/p' Cargo.toml | head -1
}

# Auto CalVer: major=year, minor=month, micro=sequential within the month,
# based on today's date and existing tags.
auto_next_version() {
    local year month prefix last_micro micro
    year="$(date +%Y)"
    month="$(date +%-m)"
    prefix="v${year}.${month}."
    last_micro="$(git tag --list "${prefix}*" \
        | sed "s|^${prefix}||" \
        | grep -E '^[0-9]+$' \
        | sort -n | tail -1 || true)"
    if [[ -z "$last_micro" ]]; then
        micro=1
    else
        micro=$((last_micro + 1))
    fi
    echo "${year}.${month}.${micro}"
}

fetch_all() {
    echo "Fetching origin..."
    if ! git fetch --quiet origin "$DEFAULT_BRANCH" "$DEV_BRANCH"; then
        echo "error: failed to fetch origin/$DEFAULT_BRANCH or origin/$DEV_BRANCH." >&2
        exit 1
    fi
    git fetch --quiet --tags origin \
        || echo "warning: 'git fetch --tags' reported issues; continuing with local tags." >&2
}

check_on_dev_branch() {
    local current
    current="$(git rev-parse --abbrev-ref HEAD)"
    if [[ "$current" != "$DEV_BRANCH" ]]; then
        echo "error: releases are cut from $DEV_BRANCH, but you are on '$current'." >&2
        echo "The version bump is committed to $DEV_BRANCH, so run this from there:" >&2
        echo "  git checkout $DEV_BRANCH" >&2
        exit 1
    fi
}

check_not_diverged() {
    if ! git merge-base --is-ancestor "origin/$DEFAULT_BRANCH" "origin/$DEV_BRANCH"; then
        echo "error: origin/$DEFAULT_BRANCH is not an ancestor of origin/$DEV_BRANCH." >&2
        echo "$DEFAULT_BRANCH has commits that aren't on $DEV_BRANCH - probably a direct push to $DEFAULT_BRANCH." >&2
        echo "Commits on $DEFAULT_BRANCH but missing from $DEV_BRANCH:" >&2
        git log --oneline "origin/$DEV_BRANCH..origin/$DEFAULT_BRANCH" >&2
        echo >&2
        echo "Rebase $DEV_BRANCH onto $DEFAULT_BRANCH and push it, then retry." >&2
        exit 1
    fi
}

# The release always cuts from origin/$DEV_BRANCH. Commits that exist only
# locally would be silently left out, and the tag would land on a stale commit.
# Refuse rather than ship the wrong tree; the fix is always "push develop first".
check_local_develop_pushed() {
    local ahead
    ahead="$(git rev-list --count "origin/$DEV_BRANCH..$DEV_BRANCH")"
    if [[ "$ahead" -gt 0 ]]; then
        echo "error: local $DEV_BRANCH is $ahead commit(s) ahead of origin/$DEV_BRANCH." >&2
        echo "The release cuts from origin/$DEV_BRANCH, so these would NOT be released:" >&2
        git log --oneline "origin/$DEV_BRANCH..$DEV_BRANCH" >&2
        echo >&2
        echo "Push them first ('git push origin $DEV_BRANCH'), let CI go green, then retry." >&2
        exit 1
    fi
}

# Returns "<status> <conclusion>" for the newest checks.yml run on $1, or the
# empty string when no run exists for that commit yet.
ci_run_state() {
    local sha="$1"
    gh run list --branch "$DEV_BRANCH" --workflow "$WORKFLOW" \
        --json headSha,status,conclusion --limit 30 \
        --jq "[.[] | select(.headSha == \"$sha\")][0] | \"\(.status) \(.conclusion)\"" 2>/dev/null || true
}

# Waits for a green checks.yml run on $1. A release must not be tagged on a
# commit CI has not vouched for, and after a version bump the run has usually
# only just been queued, so waiting is the normal path rather than an error.
check_ci_status() {
    local sha="$1"
    if [[ "$SKIP_CI_CHECK" == "1" ]]; then
        echo "warning: skipping CI status check (--skip-ci-check)." >&2
        return
    fi
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "  [dry-run] would wait for $WORKFLOW to pass on ${sha:0:12}"
        return
    fi
    if ! command -v gh &>/dev/null; then
        echo "error: gh CLI not found; cannot verify CI status for $sha." >&2
        echo "Install gh, or pass --skip-ci-check to override." >&2
        exit 1
    fi

    local waited=0 state status conclusion
    while true; do
        state="$(ci_run_state "$sha")"
        read -r status conclusion <<<"$state"

        if [[ "$status" == "completed" ]]; then
            if [[ "$conclusion" == "success" ]]; then
                echo "CI check: $WORKFLOW passed for $DEV_BRANCH @ ${sha:0:12}."
                return
            fi
            echo "error: $WORKFLOW for ${sha:0:12} concluded '$conclusion', not success." >&2
            echo "Fix $DEV_BRANCH and retry; the version bump commit is already pushed." >&2
            exit 1
        fi

        if (( waited >= CI_TIMEOUT_SECONDS )); then
            if [[ -z "$status" || "$status" == "null" ]]; then
                echo "error: no $WORKFLOW run appeared for ${sha:0:12} after ${waited}s." >&2
            else
                echo "error: $WORKFLOW for ${sha:0:12} still '$status' after ${waited}s." >&2
            fi
            local slug
            slug="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)"
            if [[ -n "$slug" ]]; then
                echo "Check https://github.com/$slug/actions" >&2
            fi
            echo "The version bump commit is already on $DEV_BRANCH; rerun cut when CI is green." >&2
            exit 1
        fi

        if [[ -z "$status" || "$status" == "null" ]]; then
            echo "Waiting for $WORKFLOW to start on ${sha:0:12}... (${waited}s)"
        else
            echo "Waiting for $WORKFLOW ($status) on ${sha:0:12}... (${waited}s)"
        fi
        sleep "$CI_POLL_SECONDS"
        waited=$((waited + CI_POLL_SECONDS))
    done
}

confirm() {
    local prompt="$1"
    if [[ "$ASSUME_YES" == "1" || "$DRY_RUN" == "1" ]]; then
        return 0
    fi
    local reply
    read -rp "$prompt [y/N] " reply
    [[ "$reply" =~ ^[Yy]$ ]]
}

# Bumps Cargo.toml and Cargo.lock to $1 and pushes the commit to develop.
# Cargo.lock carries the package version too, and the PKGBUILD builds with
# --frozen, so a lock left at the old version fails the packaging build.
bump_version() {
    local version="$1"
    echo "Bumping Cargo.toml to $version."
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "  [dry-run] sed Cargo.toml, refresh Cargo.lock, commit, push $DEV_BRANCH"
        return
    fi
    sed -i "0,/^version = /s|^version = .*|version = \"$version\"|" Cargo.toml
    # `cargo metadata` rewrites Cargo.lock's package entry without building.
    cargo metadata --format-version 1 >/dev/null
    if [[ "$(cargo_version)" != "$version" ]]; then
        echo "error: Cargo.toml still reads '$(cargo_version)' after the bump." >&2
        exit 1
    fi
    git add Cargo.toml Cargo.lock
    git commit -q -m "release $version"
    git push -q origin "$DEV_BRANCH"
    echo "Pushed the version bump to $DEV_BRANCH."
}

show_status() {
    fetch_all
    local main_sha develop_sha
    main_sha="$(git rev-parse "origin/$DEFAULT_BRANCH")"
    develop_sha="$(git rev-parse "origin/$DEV_BRANCH")"
    echo "origin/$DEFAULT_BRANCH: ${main_sha:0:12}"
    echo "origin/$DEV_BRANCH:  ${develop_sha:0:12}"
    if git show-ref --verify --quiet "refs/heads/$DEV_BRANCH"; then
        local local_ahead
        local_ahead="$(git rev-list --count "origin/$DEV_BRANCH..$DEV_BRANCH")"
        if [[ "$local_ahead" -gt 0 ]]; then
            echo "WARNING: local $DEV_BRANCH is $local_ahead commit(s) ahead of origin -" \
                 "push before cutting or they won't be released."
        fi
    fi
    echo
    if git merge-base --is-ancestor "origin/$DEFAULT_BRANCH" "origin/$DEV_BRANCH"; then
        echo "Commits on $DEV_BRANCH not yet on $DEFAULT_BRANCH:"
        git log --oneline "origin/$DEFAULT_BRANCH..origin/$DEV_BRANCH"
    else
        echo "WARNING: $DEFAULT_BRANCH has diverged from $DEV_BRANCH (direct push to $DEFAULT_BRANCH?)."
        echo "Commits on $DEFAULT_BRANCH not on $DEV_BRANCH:"
        git log --oneline "origin/$DEV_BRANCH..origin/$DEFAULT_BRANCH"
    fi
    echo
    echo "Cargo.toml version:              $(cargo_version)"
    echo "Latest tag:                      $(git describe --tags --abbrev=0 2>/dev/null || echo '(none)')"
    echo "Auto-computed version for today: $(auto_next_version)"
}

cmd_cut() {
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "error: working tree is dirty. Commit or stash first." >&2
        exit 1
    fi
    check_on_dev_branch
    fetch_all
    check_local_develop_pushed
    check_not_diverged

    local version tag
    version="${EXPLICIT_VERSION:-$(auto_next_version)}"
    tag="v${version}"
    if ! [[ "$version" =~ ^[0-9]{4}\.[0-9]{1,2}\.[0-9]+$ ]]; then
        echo "error: version '$version' is not year.month.sequence (e.g. 2026.8.1)." >&2
        exit 1
    fi
    if git rev-parse "$tag" >/dev/null 2>&1; then
        echo "error: tag $tag already exists." >&2
        exit 1
    fi

    echo "Releasing $tag."
    echo "Commits landing on $DEFAULT_BRANCH:"
    git log --oneline "origin/$DEFAULT_BRANCH..origin/$DEV_BRANCH"
    echo
    if ! confirm "Proceed with release $tag?"; then
        echo "Aborted."
        exit 1
    fi

    # The bump is its own commit on develop, so the tagged tree really does
    # contain the version it claims. Skipped when Cargo.toml already matches
    # (a retry after a failed cut, or a hand-made bump).
    if [[ "$(cargo_version)" != "$version" ]]; then
        bump_version "$version"
    else
        echo "Cargo.toml is already at $version; no bump needed."
    fi

    local develop_sha
    if [[ "$DRY_RUN" == "1" ]]; then
        develop_sha="$(git rev-parse "origin/$DEV_BRANCH")"
    else
        develop_sha="$(git rev-parse "$DEV_BRANCH")"
    fi
    check_ci_status "$develop_sha"

    run git tag -a "$tag" "$develop_sha" -m "metainjester ${version}"
    run git push --atomic origin \
        "${develop_sha}:refs/heads/${DEFAULT_BRANCH}" "refs/tags/${tag}"
    # Keep local main from silently diverging after the push-only ff above.
    # Safe to force here: cut runs from develop, so main is not checked out.
    if [[ "$DRY_RUN" != "1" ]] && git show-ref --verify --quiet "refs/heads/$DEFAULT_BRANCH"; then
        if git merge-base --is-ancestor "$DEFAULT_BRANCH" "$develop_sha"; then
            git branch -f "$DEFAULT_BRANCH" "$develop_sha"
            echo "Fast-forwarded local $DEFAULT_BRANCH to ${develop_sha:0:12}."
        else
            echo "warning: local $DEFAULT_BRANCH has diverged; not updating it." >&2
        fi
    fi

    echo
    echo "Released $tag."
    echo "arch-package.yml will build the package, attach it to the release,"
    echo "and refresh the gh-pages pacman repo."
}

case "$COMMAND" in
    cut) cmd_cut ;;
    status) show_status ;;
    "")
        echo "error: no command given; expected 'cut' or 'status'." >&2
        echo >&2
        print_usage >&2
        exit 2
        ;;
esac
