module.exports = {
  platform: 'github',
  repositories: [process.env.GITHUB_REPOSITORY],
  onboarding: false,
  requireConfig: 'optional',

  extends: ['config:recommended'],

  enabledManagers: ['github-actions', 'gradle', 'gradle-wrapper', 'sbt'],

  ignorePaths: ['**/.gradle/**'],

  packageRules: [
    {
      description: 'Wait one week before proposing dependency updates',
      matchManagers: ['github-actions', 'gradle', 'gradle-wrapper', 'sbt'],
      minimumReleaseAge: '7 days',
    },
  ],
};
