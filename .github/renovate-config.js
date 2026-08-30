module.exports = {
  platform: "github",
  repositories: ["funfix/database"],
  branchPrefix: "renovate/",
  gitAuthor: "Renovate Bot <renovate@funfix.org>",
  onboarding: false,
  requireConfig: "optional",
  recreateWhen: "always",
  prHourlyLimit: 0,
  separateMajorMinor: false,
  gitIgnoredAuthors: ["github-actions[bot]@users.noreply.github.com"],

  extends: [":dependencyDashboard"],

  enabledManagers: ["github-actions", "gradle", "gradle-wrapper", "sbt"],

  ignorePaths: ["**/.gradle/**"],

  packageRules: [
    {
      description: "Group all dependency updates into a single PR",
      matchManagers: ["github-actions", "gradle", "gradle-wrapper", "sbt"],
      groupName: "dependencies",
      groupSlug: "all-dependencies",
      group: {
        commitMessageTopic: "dependencies",
        commitMessageExtra: "",
      },
    },
    {
      description: "Only use stable dotted numeric JVM dependency versions",
      matchManagers: ["gradle", "gradle-wrapper", "sbt"],
      allowedVersions: "/^\\d+(?:\\.\\d+)+$/",
    },
    {
      description: "Keep sbt on the 1.x line",
      matchManagers: ["sbt"],
      matchPackageNames: ["sbt/sbt"],
      allowedVersions: "/^1\\.\\d+\\.\\d+$/",
    },
    {
      description: "Keep Scala 2 on the 2.13.x line",
      matchManagers: ["sbt"],
      matchPackageNames: ["org.scala-lang:scala-library"],
      allowedVersions: "/^2\\.13\\.\\d+$/",
    },
    {
      description: "Ignore derived Scala binary versions",
      matchManagers: ["sbt"],
      matchPackageNames: ["org.scala-lang:scala-library"],
      matchCurrentValue: "/^\\d+\\.\\d+$/",
      enabled: false,
    },
    {
      description: "Keep Scala on the 3.3.x line",
      matchManagers: ["sbt"],
      matchPackageNames: ["org.scala-lang:scala3-library_3"],
      allowedVersions: "/^3\\.3\\.\\d+$/",
    },
    {
      description: "Disable updates for libraryDependencySchemes entries (not real versions)",
      matchManagers: ["sbt"],
      matchCurrentValue: "/^(early-semver|semver-spec|pvp|always|strict)$/",
      enabled: false,
    },
    {
      description: "Wait one week before proposing dependency updates",
      matchManagers: ["github-actions", "gradle", "gradle-wrapper", "sbt"],
      minimumReleaseAge: "7 days",
      minimumReleaseAgeBehaviour: "timestamp-optional",
    },
  ],
};
