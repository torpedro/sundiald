# Releasing sundiald

Releases are published manually to crates.io from a committed checkout. Run the
commands below from the repository root. Examples use `0.1.1`; substitute the
version being released throughout. For the first release, keep `0.1.0` if it has
not already been published.

sundiald depends on `flares-client` from crates.io. If this release needs client
behavior that is not yet published, release [flares](https://github.com/torpedro/flares)
first and update the dependency version here before continuing.

## One-time setup

Sign in to [crates.io](https://crates.io), verify your email, and create an
[API token](https://crates.io/settings/tokens) with permission to publish
`sundiald`. For subsequent releases, your account must be a crate owner.
For the first release, confirm the crate name is available.

```sh
cargo login
```

Paste the token at the prompt. Keep it out of the repository.
See Cargo's [publishing guide](https://doc.rust-lang.org/cargo/reference/publishing.html)
for account setup.

## 1. Choose the version

Use this project policy:

| Change | Version example |
| --- | --- |
| Compatible fixes or features before 1.0 | `0.1.0` → `0.1.1` |
| Breaking changes before 1.0 | `0.1.1` → `0.2.0` |
| First stable public interface | `0.x.y` → `1.0.0` |
| Compatible fixes after 1.0 | `1.0.0` → `1.0.1` |
| Compatible features after 1.0 | `1.0.1` → `1.1.0` |
| Breaking changes after 1.0 | `1.x.y` → `2.0.0` |

Consider CLI arguments, YAML configuration, HTTP API behavior, and persisted
state when assessing compatibility. Document migration steps for breaking changes.

## 2. Prepare and test

Start from the intended release branch with unrelated changes committed or set
aside. Update the README and examples for any changed behavior. Make sure
`CHANGELOG.md` records everything in this release under `## Unreleased`, including
migration steps for breaking changes; those entries become the release notes.

Set the version and update the changelog in one step:

```sh
./scripts/make_release.sh
```

It shows the current version, offers the next patch/minor/major or a custom one,
edits `Cargo.toml`, refreshes `Cargo.lock`, offers to move the changelog entries
under the new heading, and offers to commit. It then runs the packaging dry run from
step 3 and offers to publish (step 4) and tag (step 5), in that order, so you can
drive the whole release from it or stop at any prompt.

Then run the checks:

```sh
cargo check
cargo fmt --check
cargo test --locked
git diff --check
git diff -- Cargo.toml Cargo.lock
```

Keep `Cargo.lock` committed. Review unexpected dependency changes; a version bump
does not require a general `cargo update`.

If you declined the script's commit prompt, stage `Cargo.toml`, `Cargo.lock`,
`CHANGELOG.md`, and any release documentation changes explicitly, then commit:

```sh
git add Cargo.toml Cargo.lock CHANGELOG.md
git commit -m "sundiald 0.1.1"
git status --short
```

The final status output should be empty. Keep this commit checked out through
publishing and tagging.

## 3. Verify the package

```sh
cargo package --list --locked
cargo publish --dry-run --locked --registry crates-io
```

Review the file list for the source, README, license, examples, and lockfile.
The dry run packages and builds the extracted crate without uploading it. Fix
any errors or relevant warnings, commit the fixes, and repeat the checks. Release
checks should run without `--allow-dirty` or `--no-verify`.
See the [cargo publish reference](https://doc.rust-lang.org/cargo/commands/cargo-publish.html).

## 4. Publish

Push the release commit to the intended remote branch, then publish:

```sh
git push
cargo publish --locked --registry crates-io
```

Confirm the version appears on [crates.io](https://crates.io/crates/sundiald).
If Cargo times out waiting for the index, check the registry before retrying:
the upload may already have succeeded. Published versions cannot be overwritten.
See the [cargo publish reference](https://doc.rust-lang.org/cargo/commands/cargo-publish.html)
and [publishing guide](https://doc.rust-lang.org/cargo/reference/publishing.html).

## 5. Tag and announce

After confirming publication, tag the same commit:

```sh
git tag -a v0.1.1 -m "sundiald 0.1.1"
git push origin v0.1.1
```

Create a GitHub release for that tag, using the changelog entries for this version
as its description and including any migration steps.

## 6. Verify the public installation

Install from crates.io in a fresh directory, so the source checkout cannot mask
missing files:

```sh
cargo install sundiald --version '=0.1.1' --locked
sundiald --help
```

Installation builds the executable. Configuring and enabling the systemd service
is a separate step described in the README.

## Correcting a published release

Fix the issue and repeat this process with a new version. Preserve existing tags
so each continues to identify the source that was published. If a release has a
serious defect, consider yanking that version:

```sh
cargo yank sundiald --version 0.1.1 --registry crates-io
```

Yanking does not uninstall existing binaries or remove the published source.
Explain the issue and replacement version in the release notes.
