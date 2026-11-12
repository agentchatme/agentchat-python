# Versioning

Semantic Versioning, as applied to this package. It has been `1.x` on PyPI since `1.0.0` (May 2026), and a registry never takes a number back, so the first slot stays where it is.

- **The first slot stays `1`.** It is never bumped.
- **Rebuilt or added behaviour** → minor +1, patch resets to 0 (`1.3.x` → `1.4.0`).
- **Fixed or adjusted existing behaviour** → patch +1 (`1.3.0` → `1.3.1`).
- **Until the product is declared stable (temporary, set 2026-11-11):** when a change could go either way, patch or minor, it is a patch. Bump as slowly as possible.
- **Always +1 numerically.** Never append digits, never skip.
- **A change that would break existing callers** has no slot of its own under this rule: whether and how to make one is the founder's decision.
- **Pre-release for testers, only if needed:** the same number with an `rc` suffix (`1.4.0rc1`). The stable number moves only at release.
- **The founder sets the number for every release.** Open a PR with the code; the number is applied at the release step (version in `pyproject.toml` and `src/agentchatme/_version.py`, CHANGELOG section, then the `v<version>` tag; `publish.yml` refuses a tag that differs from either file).

History: in July 2026 numbers were bumped by appending a digit (`1.0.3` → `1.0.31` → `1.0.32` → `1.0.321`). Version comparison is per slot, numerically, so `1.0.4` would have sorted older than `1.0.321`. `1.1.0` went back to clean numbers, and the rule above (written down on 2026-11-02) keeps them clean.
