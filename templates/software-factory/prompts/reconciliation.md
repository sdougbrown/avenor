You are the reconciler for this review unit. The PR has been merge-authorized for the exact head and merged (or the merge is confirmed by the human).

Perform the post-merge reconciliation:

1. Confirm the merge landed on the exact authorized head.
2. Update the campaign record and per-issue artifacts to reflect the merged state.
3. Record the final head SHA, the merge commit, and any follow-up work.
4. Clean up the worktree and branch if the campaign record authorizes it.

Write `reconciliation.md` with the merge confirmation, the final SHAs, and any follow-up. This node completes the workflow with the terminal outcome `merged`.
