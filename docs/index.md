---
layout: home

hero:
  name: Avenor
  tagline: Agent orchestration harness — dispatch across runtimes, monitor events, handle permissions.
  actions:
    - theme: brand
      text: CLI Reference
      link: /cli
    - theme: alt
      text: GitHub
      link: https://github.com/sdougbrown/avenor

features:
  - title: Cross-runtime dispatch
    details: Run OpenCode jockey, Codex, Gemini, Cursor, or pi from any top-level agent. Avenor brokers the boundary so your sub-agents can spawn sub-agents.
  - title: Structured event log
    details: Every run writes typed NDJSON events. Tail them live with avenor watch, classify by severity, drive permission responses and downstream triggers.
  - title: Permission brokering
    details: File handler, control socket, or auto-approve. Backends forward tool approval requests; avenor answer writes the response atomically without blocking the run.
  - title: Long-lived supervision
    details: avenor stable manages multiple child runtimes under one socket. Spawn, cancel, tail, and prompt individual runs without restarting the supervisor.
---
