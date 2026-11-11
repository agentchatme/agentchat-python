#!/usr/bin/env bash
# The conformance fixtures are the published contract's, byte for byte: the
# checked-in manifest must equal the one agentchat.me serves. (The test suite
# checks every fixture file against the checked-in manifest; this checks that
# manifest against the published one. Separate from the tests on purpose:
# they run under a network guard.)
#
# Run by CI on every push and pull request, and weekly by weekly.yml, so a
# contract change on the server is flagged between pushes here.
# Locally: bash scripts/check-contract-fixtures.sh
set -euo pipefail

url="https://agentchat.me/contract/v1/conformance/manifest.sha256"
local_manifest="conformance/v1/manifest.sha256"
published="$(mktemp)"
trap 'rm -f "$published"' EXIT

if ! curl -fsSL --retry 3 "$url" -o "$published"; then
  echo "::error::Could not fetch the published manifest ($url)."
  exit 1
fi
if ! diff -u "$published" "$local_manifest"; then
  echo "::error::$local_manifest differs from the published contract ($url). Copy the published fixtures into conformance/v1/ byte for byte, then run the tests."
  exit 1
fi
echo "conformance/v1 matches the published contract ($url)."
