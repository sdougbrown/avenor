---
layout: home

hero:
  name: 🏇 Avenor
  tagline: Agent orchestration harness — dispatch across runtimes, monitor events, handle permissions.
  actions:
    - theme: brand
      text: CLI Reference
      link: /cli
    - theme: alt
      text: GitHub
      link: https://github.com/sdougbrown/avenor

features:
  - icon: 🐎
    title: Cross-runtime dispatch
    details: Run OpenCode, Claude Code, Codex, Gemini, Cursor, or pi from any top-level agent. Avenor brokers the boundary so your sub-agents can spawn sub-agents.
  - icon: 🪵
    title: Structured event log
    details: Every run writes typed NDJSON events. Tail them live with avenor watch, classify by severity, drive permission responses and downstream triggers.
  - icon: 🥕
    title: Permission brokering
    details: File handler, control socket, or auto-approve. Backends forward tool approval requests; avenor answer writes the response atomically without blocking the run.
  - icon: 🪣
    title: Long-lived supervision
    details: avenor stable manages multiple child runtimes under one socket. Spawn, cancel, tail, and prompt individual runs without restarting the supervisor.
---

## Install

```bash
curl -fsSL https://avenor.douggo.com/install.sh | sh
```

Or with Go: `go install github.com/sdougbrown/avenor/cmd/avenor@latest`

Binaries for all platforms on [GitHub Releases](https://github.com/sdougbrown/avenor/releases/latest).
