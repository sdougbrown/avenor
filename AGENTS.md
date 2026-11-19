# Avenor — Agent Notes

App verification: run `skills/verify-avenor/SKILL.md` before claiming
user-facing work is done. The feature map in `skills/verify-avenor/features/`
is the maintained source of truth for how to drive each surface and what proof
it must produce.

Workflow automations: read
`marketplace/plugins/avenor/skills/avenor-workflow/SKILL.md` before writing a
workflow template or controller-driven pipeline.

## Workflow template reliability

Templates and the snapshot store must keep unattended workflows operable and diagnosable through backend outages:

- Size each template's `default_retry_policy.max_attempts` (in the template JSON) so a minutes-long outage does not terminate an instance. The review-pr-auto family has repeatedly exhausted a minimal budget inside a single outage. Space attempts with backoff — failure bursts last minutes, not seconds.
- When a node attempt fails, persist the error or timeout reason in the snapshot. A snapshot that records `error: null` on a failed attempt leaves the instance untriageable from the store; the reason belongs there, not only in the logs.
