#!/usr/bin/env bash
# Opens or updates the one pull request that copies the published contract's
# conformance fixtures into conformance/v1/. Run by
# .github/workflows/contract-fixtures.yml after a full-history checkout.
#
# Needs:
# - GH_TOKEN: the workflow's own token, with contents and pull-requests write,
#   and actions write to start the checks. Pull request runs from that
#   token's pushes wait for a person to approve them, so the job starts the
#   required checks itself (workflow_dispatch of ci.yml and
#   pr-verification.yml); they land on the branch's head commit, where the
#   pull request's checks are read. Opening the pull request needs the
#   repository setting "Allow GitHub Actions to create and approve pull
#   requests" (Settings → Actions → General).
# - RUN_URL.
# - COPIER: a copy of scripts/copy-contract-fixtures.sh outside the checkout.
#   The job switches the checkout to the copy branch, and a branch someone
#   else pushed to must not choose the code that runs with this token (the
#   workflow copies this script out too).
# - EXPECT_MANIFEST, optional: the sha256 of the manifest.sha256 a core
#   publish announced.
#
# One branch (contract-fixtures/copy), one pull request from it:
# - main already has the published fixtures: the job's own branch is deleted
#   and its pull request closed.
# - They differ, and the branch is the job's own (only its commits, touching
#   only conformance/v1/), or is gone, or was merged: the branch is rebuilt
#   as main plus one copy commit and force-pushed, only when that changes it.
#   So the PR stays up to date with main, and a later copy replaces an
#   earlier one.
# - They differ, and someone else has pushed to the branch (say, a fix for
#   what the copy broke): the job never rewrites it. It adds a later copy as
#   a new commit, when there is one, and leaves the pull request to them.
set -euo pipefail
: "${COPIER:?the path of a copy of scripts/copy-contract-fixtures.sh}"
: "${RUN_URL:?the URL of this run}"

branch="contract-fixtures/copy"
bot="github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>"
source_url="https://agentchat.me/contract/v1/conformance/"

# The copy first: it may wait for a deploy to roll out, and the branch is
# read after it, so what the job does matches the branch as it is now.
staging="$(mktemp -d)/conformance/v1"
CONTRACT_DEST="$staging" bash "$COPIER" ${EXPECT_MANIFEST:+"$EXPECT_MANIFEST"}
digest="$(sha256sum "$staging/manifest.sha256" | cut -d' ' -f1)"
title="conformance: copy contract v1 fixtures (manifest ${digest:0:12})"

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git fetch --no-tags --quiet origin main
remote_head="$(git ls-remote --heads origin "refs/heads/$branch" | cut -f1)"
# Only a pull request from this repository's own branch: a fork's branch can
# carry the same name.
pr="$(gh pr list --head "$branch" --base main --state open --json number,isCrossRepository \
  --jq '[.[] | select(.isCrossRepository | not)][0].number // empty')"

own_only=true
if [ -n "$remote_head" ]; then
  git fetch --no-tags --quiet origin "+refs/heads/$branch:refs/remotes/origin/$branch"
  # The job's own: every commit authored and committed by it (an amend or a
  # rebase by someone else changes the committer), touching only the fixtures.
  # Anything unreadable counts as someone else's.
  if ! people="$(git log --format='%an <%ae>%n%cn <%ce>' "origin/main..origin/$branch")" \
    || ! paths="$(git diff --name-only "origin/main...origin/$branch")"; then
    own_only=false
  elif grep -qvxF -e "$bot" -e '' <<<"$people" \
    || grep -qv -e '^conformance/v1/' -e '^$' <<<"$paths"; then
    own_only=false
  fi
  # A branch whose head was merged is disposable: its work is on main.
  if [ "$own_only" = false ] && [ -z "$pr" ]; then
    merged="$(gh pr list --head "$branch" --base main --state merged --json headRefOid,isCrossRepository \
      --jq "[.[] | select((.isCrossRepository | not) and .headRefOid == \"$remote_head\")] | length")"
    if [ "$merged" != 0 ]; then own_only=true; fi
  fi
fi

if [ "$own_only" = true ]; then base=origin/main; else base="origin/$branch"; fi
git checkout --quiet -B "$branch" "$base"
rm -rf conformance/v1
mkdir -p conformance/v1
cp "$staging"/* conformance/v1/
git add -A conformance/v1

# Deletes the job's own branch, only if it still is what the job read.
delete_branch() {
  git push --quiet --force-with-lease="refs/heads/$branch:$remote_head" origin --delete "$branch"
}

if git diff --cached --quiet origin/main -- conformance/v1; then
  echo "main already has the published fixtures (manifest $digest)."
  if [ "$own_only" = false ]; then
    echo "::notice::$branch has commits from others; it and its pull request stay as they are."
    exit 0
  fi
  if [ -n "$pr" ]; then
    gh pr comment "$pr" --body "main already has the published fixtures (manifest \`$digest\`), so this copy is not needed. $RUN_URL"
  fi
  if [ -n "$remote_head" ]; then delete_branch; fi
  if [ -n "$pr" ]; then
    gh pr close "$pr" || echo "#$pr closed with its branch."
  fi
  exit 0
fi

if ! git diff --cached --quiet; then
  git commit --quiet -m "$title" \
    -m "Copied byte for byte from $source_url by .github/workflows/contract-fixtures.yml: $RUN_URL"
fi

pushed=false
if [ "$own_only" = true ]; then
  if [ -n "$remote_head" ] \
    && [ "$(git rev-parse 'HEAD^{tree}')" = "$(git rev-parse "origin/$branch^{tree}")" ] \
    && [ "$(git rev-parse "origin/$branch^")" = "$(git rev-parse origin/main)" ]; then
    echo "$branch already is main plus this copy."
    git reset --quiet --hard "origin/$branch"
  else
    # A rebuild that changes workflow files (main's changed since the last
    # copy) is a push the workflow's own token may not make: start a new
    # branch and pull request instead of updating the old ones.
    if [ -n "$remote_head" ] && ! git diff --quiet "$remote_head" HEAD -- .github/workflows; then
      if [ -n "$pr" ]; then
        gh pr comment "$pr" --body "main's workflow files changed since this copy; a new pull request replaces this one. $RUN_URL"
      fi
      delete_branch
      if [ -n "$pr" ]; then gh pr close "$pr" || echo "#$pr closed with its branch."; fi
      remote_head=""
      pr=""
    fi
    git push --quiet --force-with-lease="refs/heads/$branch:$remote_head" origin "HEAD:refs/heads/$branch"
    pushed=true
    echo "Pushed $branch: main plus the copy of manifest $digest."
  fi
elif [ "$(git rev-parse HEAD)" != "$remote_head" ]; then
  git push --quiet origin "HEAD:refs/heads/$branch"
  pushed=true
  echo "Added the copy of manifest $digest to $branch, on top of the commits there."
fi

body="$(mktemp)"
cat > "$body" <<EOF
## What changes

The published contract's conformance fixtures, copied into \`conformance/v1/\` byte for byte from $source_url (manifest sha256 \`$digest\`). Each file was checked against the published manifest before it was written. Opened by \`.github/workflows/contract-fixtures.yml\`: $RUN_URL

Against main:

\`\`\`
$(git diff --stat origin/main HEAD -- conformance/v1)
\`\`\`

## Verification

- Tests: CI on this branch runs the whole suite on the copied fixtures; the conformance tests check every file against the copied manifest.
- Review: not needed: a byte-for-byte copy of the published contract, nothing else.
- Live check: n/a: no release; the fixtures change only what the tests check.

## Risk and rollback

Red CI here means this SDK disagrees with the published contract. Fix the SDK (on this branch, or in a pull request of its own), or fix the contract in core. The job never rewrites a branch someone else has pushed to; it adds later copies as new commits. Green, this auto-merges. Revert the merge to undo.
EOF

created=false
if [ -z "$pr" ]; then
  if ! url="$(gh pr create --base main --head "$branch" --title "$title" --body-file "$body")"; then
    echo "::error::Could not open the copy pull request from $branch. When GitHub says Actions is not permitted to create pull requests: Settings → Actions → General → Workflow permissions → tick \"Allow GitHub Actions to create and approve pull requests\"."
    exit 1
  fi
  pr="${url##*/}"
  created=true
  echo "Opened $url."
elif [ "$pushed" = true ] && [ "$own_only" = true ]; then
  gh pr edit "$pr" --title "$title" --body-file "$body"
fi
if [ "$own_only" = true ] && { [ "$created" = true ] || [ "$pushed" = true ]; }; then
  gh pr merge "$pr" --auto --squash || echo "::warning::Could not turn on auto-merge for #$pr."
fi

# The required checks on the branch head, each once, when the head is this
# job's commit: a pull request run from this token's push waits for approval
# and may never run, so only runs this job started count. (Someone else's
# push starts its own checks.) CI, and PR verification for this pull request.
head_sha="$(git rev-parse HEAD)"
if [ "$(git log -1 --format='%cn <%ce>' HEAD)" != "$bot" ]; then
  echo "Copy pull request: #$pr. Its head is someone else's commit, which started its own checks."
  exit 0
fi
start_once() {
  local workflow="$1" started
  shift
  started="$(gh run list --workflow "$workflow" --commit "$head_sha" --event workflow_dispatch --json conclusion \
    --jq '[.[] | select(.conclusion != "cancelled")] | length')"
  if [ "$started" = 0 ]; then
    gh workflow run "$workflow" --ref "$branch" "$@"
    echo "Started $workflow on $branch ($head_sha)."
  fi
}
start_once ci.yml
start_once pr-verification.yml -f pr="$pr"
echo "Copy pull request: #$pr."
