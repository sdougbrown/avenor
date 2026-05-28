# Pony — In-process Agent Backend

When you need a self-contained agent that doesn't depend on Node, Bun, or a separate binary being installed on the host, you use Pony.

Previously, Avenor required an external agent process (like OpenCode or Codex) to handle the model loop. This created a dependency on the runtime environment of those external processes. Pony removes that boundary by running the model loop directly inside the Avenor binary.

## Endpoint Configuration

The config file needs two top-level fields to connect to a model provider:

- **`base_url`** — The full URL of an OpenAI-compatible endpoint. This works with Ollama, LM Studio, OpenRouter, vLLM, and anything that exposes a `/v1/chat/completions` route. The adapter appends `/chat/completions` automatically, so `http://localhost:4000/v1` and `http://localhost:4000` both work.
- **`api_key_env`** — The name of an environment variable that holds the API key. Pony reads it with `os.Getenv` at startup. It does not read from files or support path-based references like `os.environ/KEY_NAME`.

  ```json
  {
    "base_url": "http://localhost:11434/v1",
    "api_key_env": "OLLAMA_API_KEY"
  }
  ```

  Then set the variable before running:

  ```bash
  export OLLAMA_API_KEY="sk-..."
  avenor --backend pony --pony-config pony.json --agent mule --prompt "hello"
  ```

  For local endpoints that don't require authentication, set the var to any value:

  ```bash
  export OLLAMA_API_KEY="sk-no-auth"
  ```

  Or use the `AVENOR_PONY_API_KEY` convention from earlier smoke tests. The key point: an empty or unset variable sends `Authorization: Bearer ` with a trailing space, which some servers reject. Always set it to something, even a dummy value.

## Agent Profiles

Pony is configured via a JSON file. Instead of a single global setting, you define named **profiles**. Each profile specifies its own model, system prompt, initial prompt, and which tools are allowed to run.

```json
{
  "base_url": "http://localhost:11434/v1",
  "api_key_env": "OLLAMA_API_KEY",
  "default_agent": "mule",
  "profiles": {
    "jockey": {
      "model": "gpt-4o",
      "system_prompt": "You are a jockey. Your job is to orchestrate other agents to complete complex tasks.",
      "initial_prompt": "Break the incoming task into independent units. Spawn all mules up front with spawn_agents, then wait_for_done on each child to collect results.",
      "tools": {
        "local": false,
        "orchestration": true
      },
      "max_tokens": 8192
    },
    "mule": {
      "model": "llama3.1",
      "system_prompt": "You are a mule. You perform mechanical tasks like reading and writing files.",
      "initial_prompt": "You have file and shell tools. Do exactly what is asked — no scope creep, no extra features.",
      "inject_agents_md": true,
      "tools": {
        "local": true,
        "orchestration": false
      }
    }
  }
}
```

## Prompt Lifecycle

Pony manages conversation history using four distinct layers of prompting. On session start, they assemble in this order:

1. **System Prompt**: Defines the agent's core identity and rules. Sent with every API turn.
2. **AGENTS.md** (if `inject_agents_md: true`): Walks up from the working directory to find `AGENTS.md` and appends it as a first-turn user message. This is how project conventions reach the agent.
3. **Initial Prompt**: A user message sent only on the first turn. Use this for run-level context that doesn't need to persist across every turn.
4. **Task Prompt**: The actual instruction provided at invocation time via `--prompt`, `--prompt-file`, or the control socket. This is appended as the most recent user message when `Prompt` is called.

On follow-up prompts (multi-turn within a single session), only the system prompt and the new task prompt are sent. The AGENTS.md and initial prompt are not re-injected.

## Tool Surface

Tools are gated by profile configuration. A profile can have `local` tools, `orchestration` tools, both, or neither.

### Local Tools
Enabled via `"local": true`. These allow the agent to interact with the local filesystem and shell. By default, additional skill directories are read-access only; write and edit operations remain restricted unless the directory is explicitly write-allowed.
- `file_read`: Read file contents.
- `file_write`: Write new files.
- `file_edit`: Perform partial edits to existing files.
- `glob`: Match file patterns.
- `grep`: Search file contents.
- `list_dir`: List directory contents.
- `shell`: Execute shell commands.

### Orchestration Tools
Enabled via `"orchestration": true`. These allow a **jockey** agent to manage child agents:
- `spawn_agent`: Create a new child agent session.
- `spawn_agents`: Create multiple child agent sessions in a single call for parallel execution.
- `send_prompt`: Send a follow-up message to a child.
- `get_status`: Check if a child is running or finished.
- `wait_for_done`: Block until a child agent completes its task.
- `send_to_parent`: From a child, ask the parent for clarification (emits `child.question`).

When a child calls `send_to_parent`, the supervisor emits a `child.question` event to the parent subscription with this payload shape:

```json
{
  "event": "child.question",
  "runtime_id": "<parent_runtime_id>",
  "session_id": "<parent_session_id>",
  "child_id": "<child_runtime_id>",
  "message": "Which package should I use?",
  "request_id": "cq_123"
}
```

The parent should reply with `send_prompt` to `child_id`, and should include `request_id` to make duplicate reply handling idempotent.

This payload is defined as a shared runtime contract in `internal/runtime/contracts.go` (`ChildQuestionPayload` and `EventChildQuestion`) so other runtimes (including Mustang) can consume the same schema.

## The Wire Format

Pony uses the OpenAI Chat Completions wire format. This means it is compatible with any provider that implements an OpenAI-compatible API, such as Ollama, LM Studio, OpenRouter, or vLLM.

To use a local endpoint like Ollama or LM Studio:

```bash
avenor --backend pony \
  --pony-config pony.json \
  --agent mule \
  --prompt "Refactor the user module" \
  --dir /repo
```

Select a different profile with `--agent`:

```bash
avenor --backend pony \
  --pony-config pony.json \
  --agent jockey \
  --control-socket /tmp/avenor.sock \
  --prompt "Review the codebase and fix lint errors" \
  --dir /repo
```

## Reasoning Blocks

When using models that support reasoning (like `kimi-k2`), Pony surfaces the internal thought process. Reasoning content is emitted as `agent.thought_chunk` events, allowing you to observe the agent's internal monologue before it produces tool calls or text.

## Boundaries

Pony is a lean execution loop. It does **not** currently handle:
- **Vision/Images**: It is a text-based backend.
- **Prompt Caching**: It does not manage provider-specific cache headers.
- **Anthropic Native API**: It currently only supports OpenAI-compatible adapters.
- **Approval-Gated Tools**: Tools run as soon as the model requests them.
- **Multi-provider Fallback**: If the specified endpoint fails, the session fails.

## Roadmap

The following capabilities are planned:
- **Skill Loading**: Composable system prompts that load directly from `.skill` files.
- **Parallel Subagents**: Allowing a jockey to spawn multiple agents concurrently and collect their results via structured routing.
