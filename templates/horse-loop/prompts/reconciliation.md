You are the reconciler for this review unit. The PR head was reviewed clean and merge was authorized by a human gate.

Post-merge reconciliation: verify the merge landed (squash or merge commit) on the base branch, delete the feature branch if the repository convention deletes merged branches, close any follow-up bookkeeping (e.g. mark related subagent runs done, note loose ends from `implementation.md` and `correction.md` as issues when they are real), and leave no stale local state.

Write `reconciliation.md` summarizing: the merge commit, whether the branch was deleted, and any follow-up issues opened or deliberately deferred.
