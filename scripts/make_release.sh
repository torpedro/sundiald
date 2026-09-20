#!/usr/bin/env bash
# Interactive release helper for sundiald.
#
# Shows the current version, prompts for the next one, updates every file that
# records it, and optionally updates the changelog, commits, and tags.
# Publishing is separate and manual; see docs/releases.md.

set -euo pipefail

PROJECT_NAME="sundiald"
TAG_MESSAGE_PREFIX="Release"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

# confirm <prompt> <default: y|n>; returns 0 for yes.
confirm() {
    local prompt=$1 default=$2 reply hint
    if [ "$default" = y ]; then hint='[Y/n]'; else hint='[y/N]'; fi
    read -r -p "$prompt $hint " reply
    reply=${reply:-$default}
    [[ $reply =~ ^[Yy]$ ]]
}

current_version() {
    sed -n '/^\[package\]/,/^\[/p' Cargo.toml |
        sed -n 's/^version = "\(.*\)"$/\1/p' | head -1
}

# bump <version> <major|minor|patch>
bump() {
    local core=${1%%-*} part=$2 major minor patch
    IFS=. read -r major minor patch <<<"$core"
    case $part in
    major) printf '%d.0.0\n' "$((major + 1))" ;;
    minor) printf '%d.%d.0\n' "$major" "$((minor + 1))" ;;
    patch) printf '%d.%d.%d\n' "$major" "$minor" "$((patch + 1))" ;;
    esac
}

git rev-parse --git-dir >/dev/null 2>&1 || die "not a git repository"

current=$(current_version)
[ -n "$current" ] || die "could not read the package version from Cargo.toml"

printf '\n%s release\n\n  Current version: %s\n\n' "$PROJECT_NAME" "$current"
printf '  1) patch   %s\n' "$(bump "$current" patch)"
printf '  2) minor   %s\n' "$(bump "$current" minor)"
printf '  3) major   %s\n' "$(bump "$current" major)"
printf '  4) custom\n  q) quit\n\n'

read -r -p 'Select [1-4/q]: ' choice
case $choice in
1) target=$(bump "$current" patch) ;;
2) target=$(bump "$current" minor) ;;
3) target=$(bump "$current" major) ;;
4) read -r -p 'Version: ' target ;;
q | Q) exit 0 ;;
*) die "no such option: $choice" ;;
esac

[[ $target =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]] ||
    die "not a version: $target"
[ "$target" != "$current" ] || die "already at $target"

tag="v$target"
git rev-parse -q --verify "refs/tags/$tag" >/dev/null &&
    die "tag $tag already exists; published tags must not move"

if [ -n "$(git status --porcelain)" ]; then
    printf '\nThe working tree has uncommitted changes:\n\n'
    git status --short
    printf '\n'
    confirm 'Continue anyway?' n || exit 0
fi

printf '\nSetting version %s ...\n\n' "$target"
sed -i "0,/^version = \"$current\"$/s//version = \"$target\"/" Cargo.toml
[ "$(current_version)" = "$target" ] || die "Cargo.toml was not updated"
cargo check --quiet
printf 'Cargo.toml and Cargo.lock updated.\n'

changelog_heading="## $target - $(date +%F)"
if grep -q '^## Unreleased$' CHANGELOG.md &&
    confirm $'\n'"Move the CHANGELOG \"Unreleased\" entries under $target?" y; then
    sed -i "0,/^## Unreleased$/s//## Unreleased\n\n$changelog_heading/" CHANGELOG.md
    printf 'CHANGELOG.md: entries moved under %s\n' "${changelog_heading#\#\# }"
fi

printf '\nChanged files:\n\n'
git status --short
printf '\n'

if confirm "Commit as \"Release $target\"?" y; then
    git add -A
    git commit -q -m "Release $target"
    printf 'Committed %s\n' "$(git rev-parse --short HEAD)"
else
    printf 'Left uncommitted. A tag would not include these changes.\n'
fi

printf '\nThe documented process publishes to crates.io before tagging, so that a tag\n'
printf 'only ever names a release that reached the registry.\n'
printf 'See docs/releases.md steps 4 and 5.\n\n'

if confirm "Create tag $tag now anyway?" n; then
    git tag -a "$tag" -m "$TAG_MESSAGE_PREFIX $target"
    printf '\nCreated %s locally. It is not pushed.\n' "$tag"
    printf '  push:   git push origin %s\n' "$tag"
    printf '  undo:   git tag -d %s\n' "$tag"
else
    printf '\nNo tag created. After publishing:\n'
    printf '  git tag -a %s -m "%s %s" && git push origin %s\n' \
        "$tag" "$TAG_MESSAGE_PREFIX" "$target" "$tag"
fi

printf '\nNext: docs/releases.md step 3 (verify the package).\n\n'
