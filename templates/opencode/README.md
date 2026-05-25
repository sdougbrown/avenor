# Avenor OpenCode Template Pack

A starting point for consumers that run OpenCode agents through Avenor.

The pieces are meant to be used together:

- `opencode.json` — agent definitions: jockey, horse, mule, groom with permission boundaries
- `prompts/jockey.md` — lead agent: plans, delegates, integrates, verifies
- `prompts/horse.md` — bounded implementation executor for work requiring local judgment
- `prompts/mule.md` — literal executor for small mechanical tasks
- `prompts/groom.md` — hidden script runner used by jockey to invoke avenor
- `skills/dispatch-jockey/SKILL.md` — starts a jockey run, monitors events, waits for completion
- `skills/answer-jockey/SKILL.md` — answers Avenor file permission requests via `avenor answer`

## Permission model

In the Avenor world, jockey asks operator questions through ACP permission requests, not prose markers like `QUESTION:`. When the backend emits a permission request, Avenor writes a `.req` file and emits a `permission.request` event. The answer-jockey skill reads that file, presents the question to the operator, and calls `avenor answer` to write the response atomically.

See [docs/permission-handler.md](../../docs/permission-handler.md) for the full request/response format and timeout behavior.

## Adapting these templates

These are starting points, not runtime policy. Adapt them to your own:

- Agent names and model assignments
- Permission model (auto-approve, file handler, control socket)
- Verification standards and test commands
- Delegation boundaries between jockey, horse, and mule

`opencode.json` uses `{HOME}/your-project*` as a placeholder in every agent's `external_directory` allowlist. Replace it with your actual project root before use — the trailing `*` is needed for subdirectory access, but the path itself must be specific to your setup.

For the full CLI reference and common orchestration patterns, see [docs/cli.md](../../docs/cli.md).
