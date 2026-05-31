module.exports = {
  platform: 'github',
  repositories: ['funfix/database'],
  gitAuthor: 'Renovate Bot <renovate@funfix.org>',
  branchPrefix: 'renovate/',
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
