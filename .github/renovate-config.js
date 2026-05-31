module.exports = {
  platform: "github",
  repositories: ["funfix/database"],
  gitAuthor: "Renovate Bot <renovate@funfix.org>",
  branchPrefix: "renovate/",
  onboarding: false,
  requireConfig: "optional",
  recreateWhen: "always",

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
      description: "Wait one week before proposing dependency updates",
      matchManagers: ["github-actions", "gradle", "gradle-wrapper", "sbt"],
      minimumReleaseAge: "1 days",
      minimumReleaseAgeBehaviour: "timestamp-optional",
    },
  ],
};
