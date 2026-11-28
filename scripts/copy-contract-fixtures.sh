#!/usr/bin/env bash
# Copies the published contract's conformance fixtures into conformance/v1/,
# byte for byte. The published manifest (manifest.sha256) names every file and
# its sha256. Each file is fetched and checked against it, and conformance/v1/
# is replaced only once every file matches: a file the manifest no longer
# names is removed. On any failure nothing in conformance/v1/ changes.
#
# Run by .github/workflows/contract-fixtures.yml, which opens the copy pull
# request. By hand, from any directory:
#   bash scripts/copy-contract-fixtures.sh
# With an argument, the sha256 of the manifest file the copy is for (a deploy
# that may still be rolling out): the script waits up to
# CONTRACT_WAIT_SECONDS (default 900) for agentchat.me to serve it, then copies
# what is served, with a warning.
#   bash scripts/copy-contract-fixtures.sh <sha256 of manifest.sha256>
set -euo pipefail
cd "$(dirname "$0")/.."

# CONTRACT_BASE_URL only for testing this script against a local server.
base="${CONTRACT_BASE_URL:-https://agentchat.me/contract/v1/conformance}"
# CONTRACT_DEST: where the copy goes when it is not this checkout's
# conformance/v1/ (contract-fixtures.yml runs a copy of this script from
# outside the checkout).
dest="${CONTRACT_DEST:-conformance/v1}"
expect="${1:-}"
wait_s="${CONTRACT_WAIT_SECONDS:-900}"

# The copy replaces the directory's contents, so only ever a conformance/v1
# directory, and never through a symlink.
case "$dest" in
  conformance/v1 | */conformance/v1) ;;
  *) echo "::error::CONTRACT_DEST must be a conformance/v1 directory: $dest"; exit 2 ;;
esac
if [ -L "$dest" ]; then
  echo "::error::$dest is a symlink; the copy writes only into a real directory."
  exit 2
fi
if [ -n "$expect" ] && ! [[ "$expect" =~ ^[0-9a-f]{64}$ ]]; then
  echo "::error::The expected manifest digest must be 64 lowercase hex characters."
  exit 2
fi
if ! [[ "$wait_s" =~ ^[0-9]+$ ]]; then
  echo "::error::CONTRACT_WAIT_SECONDS must be a whole number of seconds."
  exit 2
fi

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

fetch() {
  curl -fsSL --connect-timeout 10 --max-time 30 --retry 3 \
    -H 'Cache-Control: no-cache' "$base/$1" -o "$2"
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
copy="$tmp/copy"
names="$tmp/names"

# A manifest line is "<sha256>  <name>"; a name is a plain file name (no
# path, no leading dot), so a copy can only ever write inside conformance/v1/.
check_manifest() {
  local file="$1" line count=0
  if [ ! -s "$file" ] || [ -n "$(tail -c 1 "$file")" ]; then
    echo "::error::The published manifest is empty or does not end with a newline."
    return 1
  fi
  : > "$names"
  while IFS= read -r line; do
    if ! [[ "$line" =~ ^[0-9a-f]{64}\ \ ([A-Za-z0-9][A-Za-z0-9._-]*)$ ]] \
      || [ "${BASH_REMATCH[1]}" = "manifest.sha256" ]; then
      echo "::error::The published manifest has a line that is not \"<sha256>  <file name>\": $line"
      return 1
    fi
    printf '%s\n' "${BASH_REMATCH[1]}" >> "$names"
    count=$((count + 1))
  done < "$file"
  if [ "$count" -eq 0 ]; then
    echo "::error::The published manifest names no files."
    return 1
  fi
}

# One attempt: the manifest, then every file it names, each checked against
# it. 0: $copy holds a consistent copy. 1: try again (fetch failed, a deploy
# landed between two fetches, or not yet the expected manifest). A malformed
# manifest ends the script.
attempt() {
  local want="$1" sha name got
  rm -rf "$copy"
  mkdir "$copy"
  if ! fetch manifest.sha256 "$copy/manifest.sha256"; then
    echo "Could not fetch $base/manifest.sha256."
    return 1
  fi
  if [ -n "$want" ] && [ "$(sha256_of "$copy/manifest.sha256")" != "$want" ]; then
    echo "agentchat.me does not serve manifest $want yet."
    return 1
  fi
  check_manifest "$copy/manifest.sha256" || exit 1
  while read -r sha name; do
    if ! fetch "$name" "$copy/$name"; then
      echo "Could not fetch $base/$name."
      return 1
    fi
    got="$(sha256_of "$copy/$name")"
    if [ "$got" != "$sha" ]; then
      echo "$name has sha256 $got, not the manifest's $sha (a deploy in between?)."
      return 1
    fi
  done < "$copy/manifest.sha256"
}

# Without an expected manifest, a minute of retries covers a deploy landing
# between two fetches.
if [ -n "$expect" ]; then window="$wait_s"; else window=60; fi
deadline=$(($(date +%s) + window))
want="$expect"
until attempt "$want"; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    if [ -n "$want" ]; then
      echo "::warning::agentchat.me did not serve manifest $want within ${wait_s}s; copying what it serves."
      want=""
      deadline=$(($(date +%s) + 60))
      continue
    fi
    echo "::error::Could not copy a consistent set of fixtures from $base."
    exit 1
  fi
  sleep 20
done

mkdir -p "$dest"
for path in "$dest"/* "$dest"/.[!.]*; do
  [ -e "$path" ] || [ -L "$path" ] || continue
  file="$(basename "$path")"
  if [ "$file" != "manifest.sha256" ] && ! grep -qxF -e "$file" "$names"; then
    rm -rf "$path"
  fi
done
# Removed first, so a symlink in the way is replaced, never written through.
while read -r name; do rm -rf "${dest:?}/$name"; done < "$names"
rm -f "$dest/manifest.sha256"
cp "$copy"/* "$dest"/

echo "conformance/v1 is the published contract (manifest sha256 $(sha256_of "$dest/manifest.sha256"))."
