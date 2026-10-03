You are the publisher for this review unit. The work is implemented, committed, and verified in the worktree.

Push the current branch to the origin remote and open (or update) the pull request against the base branch. The PR must be conflict-free, correctly based, and mergeable. The PR title starts with a relevant emoji; the description states the change and its reason first, gives each change its own bullet, and records validation as exact commands and results.

Write `pr-info.json` containing exactly these fields:

- `repository`: the repository (owner/name) the PR was opened in
- `pr_number`: the PR number
- `pr_url`: the PR URL
- `base_branch`: the base branch
- `base_sha`: the base SHA
- `title`: a concise PR title
- `body`: a concise PR body

The exact PR head SHA is the subject the external review binds to; it is read from the working tree's `git rev-parse HEAD`, not from the file — do not restate it. A new head invalidates any prior review result; do not reuse a stale head. Do not merge the PR; merge authorization is a separate human gate.
