# Cutting a whdr release

**When to use:** you want to ship a new tagged version of whdr — bump the
version, tag it, and let CI build the binaries and publish a GitHub Release.

The release build lives in `.github/workflows/release.yml`. It triggers on any
`v*` tag push (and can be run manually via `workflow_dispatch`). It **refuses to
build** unless the tag version exactly matches `workspace.package.version` in
`Cargo.toml`, so the bump-commit and the tag must agree. That guard is why the
order below matters.

## What the workflow does (so you know what you're triggering)

1. `cargo fmt --check`, `clippy -D warnings`, `cargo test` (locked) — a red gate.
2. Validates the tag version `== workspace.package.version`.
3. Builds release binaries: `whdr-server`, `whdr`, `whdr-ext-dev`,
   `whdr-ext-github`, `whdr-ext-teams`, `whdr-ext-hmac` (linux-x64).
4. Packages `whdr-<version>-linux-x64.tar.gz` + `SHA256SUMS` (bundles the
   binaries, README, and the three LICENSE files).
5. Creates the GitHub Release with **auto-generated notes** (`--generate-notes`,
   changelog from merged PRs/commits since the last tag) and attaches the
   archive. Re-running for an existing tag re-uploads with `--clobber`.

> The changelog is GitHub's auto-generated notes. If you want a curated
> changelog instead, edit the release body after it's created, or maintain a
> `CHANGELOG.md` and paste the section in — the workflow doesn't read one.

## Release steps

Do this on `master`, clean tree, everything you want in the release already
merged and pushed.

1. **Pick the version.** Semver `MAJOR.MINOR.PATCH` (pre-release like
   `0.2.0-rc.1` is allowed). Call it `X.Y.Z` below.

2. **Bump the workspace version** in `Cargo.toml`:
   ```
   [workspace.package]
   version = "X.Y.Z"
   ```
   All crates inherit this via `version.workspace = true`, so this one line
   covers the whole workspace.

3. **Refresh the lockfile** so `Cargo.lock` records the new version (the
   workflow builds `--locked` and will fail on a stale lock):
   ```sh
   cargo check --workspace
   ```

4. **Sanity-check locally** (mirrors the CI gate; 2 threads per repo norm):
   ```sh
   just ci        # fmt-check + clippy + test
   ```

5. **Commit** the bump:
   ```sh
   git commit -am "release: vX.Y.Z"
   ```

6. **Tag and push.** Push the commit first, then the tag — the tag push is what
   fires the workflow:
   ```sh
   git push origin master
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

7. **Watch the run:**
   ```sh
   gh run watch $(gh run list --workflow=Release --limit 1 --json databaseId --jq '.[0].databaseId')
   ```
   On success, `gh release view vX.Y.Z --web` shows the release + archive.

## If it goes wrong

- **Version-mismatch failure:** the tag and `workspace.package.version` disagree.
  Fix `Cargo.toml`, re-commit, delete and re-push the tag:
  ```sh
  git tag -d vX.Y.Z && git push origin :refs/tags/vX.Y.Z
  git tag vX.Y.Z && git push origin vX.Y.Z
  ```
- **Need a dry run without a tag:** trigger `workflow_dispatch` with
  `draft: true` — it builds and creates a *draft* release you can inspect and
  delete.
  ```sh
  gh workflow run Release -f version=X.Y.Z -f draft=true
  ```
- **Adding a new shippable binary** (e.g. a new `whdr-ext-*`): add it to the
  `for bin in …` list in `release.yml`, or it won't be in the archive.
