# Avenor — Agent Notes

App verification: run `skills/verify-avenor/SKILL.md` before claiming
user-facing work is done. The feature map in `skills/verify-avenor/features/`
is the maintained source of truth for how to drive each surface and what proof
it must produce.

Workflow automations: read
`marketplace/plugins/avenor/skills/avenor-workflow/SKILL.md` before writing a
workflow template or controller-driven pipeline.

## Testing

Integration tests synchronize on observable signals: channel reads, recorded events, condition polls bounded by a deadline. Never wait on fixed sleeps or one long timeout the runner can outpace. Harden a timing-failed test in place with a signal-based wait before it merges; re-running until green or lengthening the sleep is not a fix.
