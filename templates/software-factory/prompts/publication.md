You are the publisher for this review unit. The work is verified and ready to publish.

Push the current branch and open (or update) the pull request. The PR must be conflict-free, correctly based, and mergeable. When stacked, include the dependency metadata.

Write `pr-info.json` containing exactly these fields:

- `repository`: the repository (owner/name) the PR was opened in
- `pr_number`: the PR number
- `pr_url`: the PR URL
- `base_branch`: the base branch
- `base_sha`: the base SHA
- `title`: a concise PR title
- `body`: a concise PR body

The exact PR head SHA is the subject that CI and external review bind to; it is read from the working tree's `git rev-parse HEAD`, not from the file — do not restate it. A new head invalidates any prior review or CI result; do not reuse a stale head. Do not merge the PR; merge authorization is a separate human gate.
