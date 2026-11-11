#!/usr/bin/env bash
# The conformance fixtures are the published contract's, byte for byte: the
# checked-in manifest must equal the one agentchat.me serves. (The test suite
# checks every fixture file against the checked-in manifest; this checks that
# manifest against the published one. Separate from the tests on purpose:
# they run under a network guard.)
#
# Run by CI on every push to main and every pull request into main, and
# weekly by weekly.yml, so a contract change on the server is flagged
# between pushes here. Works from any directory:
#   bash scripts/check-contract-fixtures.sh
set -euo pipefail
cd "$(dirname "$0")/.."

url="https://agentchat.me/contract/v1/conformance/manifest.sha256"
local_manifest="conformance/v1/manifest.sha256"
published="$(mktemp)"
trap 'rm -f "$published"' EXIT

if ! curl -fsSL --connect-timeout 10 --max-time 30 --retry 3 "$url" -o "$published"; then
  echo "::error::Could not fetch the published manifest ($url)."
  exit 1
fi
status=0
diff -u "$published" "$local_manifest" || status=$?
if [ "$status" -eq 1 ]; then
  echo "::error::$local_manifest differs from the published contract ($url). Copy the published fixtures into conformance/v1/ byte for byte, then run the tests."
  exit 1
elif [ "$status" -ne 0 ]; then
  echo "::error::Could not compare $local_manifest with the published manifest (diff exit $status)."
  exit 1
fi
echo "conformance/v1 matches the published contract ($url)."
