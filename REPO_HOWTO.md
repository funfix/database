# How-to

## Publishing

The base version lives in `gradle.properties` as `project.version`. It must not
include `-SNAPSHOT`.

Run `manual-publish` with `stable_version` set to `false` for a snapshot. The
workflow publishes the effective version as `<base>-SNAPSHOT` and runs the
Scala snapshot alias. Snapshot publishing must be enabled for the `org.funfix`
namespace in the [Central Portal](https://central.sonatype.com/).

For a stable release, set `stable_version` to `true` and set
`ref_to_publish` to `refs/tags/v<base>`. The release script fetches tags and
requires that exact tag to point at `HEAD`. It publishes `delayedqueue-jvm`
and the cross-built `delayedqueue-scala` artifacts for Scala 2.13 and 3.3.
The Gradle and Scala publication commands request automatic publication to
Maven Central.

### Prepare a stable release

1. Set `project.version` in `gradle.properties` to the release version.
2. Update the Javadoc, Scaladoc, and dependency examples in `README.md` to that
   version.
3. Run `make check-all`.

Commit those changes and create and push the matching tag, such as `v1.2.3`.
Then run `manual-publish` with `ref_to_publish` set to `refs/tags/v1.2.3` and
enable `stable_version`.

## Publication secrets

The manual publication workflow uses these existing repository secrets:

- `GH_TOKEN`
- `MAVEN_USERNAME`
- `MAVEN_PASSWORD`
- `GRADLE_SIGNING_KEY_ID`
- `GRADLE_SIGNING_KEY_PASSWORD`
- `GRADLE_SIGNING_KEY`
- `SBT_PGP_KEY_ID`
- `SBT_PGP_PASSPHRASE`
- `SBT_PGP_SECRET`

`SBT_PGP_SECRET` is the base64-encoded private PGP key. `MAVEN_USERNAME` and
`MAVEN_PASSWORD` provide both Gradle Maven Central and sbt Sonatype
credentials.

## Automation

Renovate and OpenCode use the existing organization-owned `Funfix` GitHub App.
Its installation includes `funfix/database`. Each workflow requests an
installation token scoped to this repository. The repository Actions secrets
are `AUTOMATION_APP_ID` and `AUTOMATION_APP_PRIVATE_KEY`, along with
`OPENCODE_API_KEY` for OpenCode.

The app uses these permissions:

- Organization: `Members: read-only`
- Repository: `Administration: read-only`
- Repository: `Checks: read and write`
- Repository: `Commit statuses: read and write`
- Repository: `Contents: read and write`
- Repository: `Dependabot alerts: read-only`
- Repository: `Issues: read and write`
- Repository: `Pull requests: read and write`
- Repository: `Workflows: read and write`
- Repository: `Metadata: read-only`, granted automatically by GitHub

`Contents: read and write` permits Git pushes. `Workflows: read and write` is
needed when automation changes files under `.github/workflows`.
