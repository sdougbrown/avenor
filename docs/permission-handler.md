# File Permission Handler

Your backend paused mid-execution and is asking for approval to proceed. You're not sitting at a socket, and you'd rather not block on a live connection. That's what the file permission handler is for.

When Avenor encounters a permission request, it writes a JSON file to disk, polls for your answer in a separate file, then relays your decision back to the backend. This lets you decouple approval from the Avenor process itself: an agent, a human operator, or any external system can read the request file, apply its own logic, and drop a response file whenever it's ready.

```sh
--permission-handler file:/tmp/avenor-permission
```

## How it works

When the backend emits a permission request, Avenor writes to:

```
/tmp/avenor-permission.req
```

It then polls continuously for:

```
/tmp/avenor-permission.req.response
```

The default timeout is 10 minutes. The default polling cadence is 500 milliseconds. If no response arrives within the timeout window, the handler returns a timeout error and the execution halts.

Avenor also emits a `permission.request` NDJSON event after writing the request file, so downstream listeners can react in parallel with the file-based protocol.

### Request file

The `.req` file is JSON:

```json
{
  "request_id": "17",
  "session_id": "ses_123",
  "tool": "bash",
  "question": "Run command?",
  "options": [
    {"optionId": "allow", "kind": "allow"},
    {"optionId": "deny", "kind": "reject"}
  ],
  "payload": {
    "request_id": "17",
    "tool": "bash",
    "question": "Run command?",
    "options": [
      {"optionId": "allow", "kind": "allow"},
      {"optionId": "deny", "kind": "reject"}
    ]
  }
}
```

| Field | Required? | What it means |
|-------|-----------|---------------|
| `request_id` | Yes | Unique identifier for this request. The response must reference it. |
| `session_id` | No | Session ID if one exists. |
| `tool` | No | The tool or skill that triggered the request (e.g., "bash"). |
| `question` | No | Human-readable text explaining what approval is needed. |
| `options` | No | Array of available choices, each with `optionId` (the code name) and `kind` (the semantics: "allow" or "reject"). |
| `payload` | No | The full normalized event fields for consumers that need the complete picture. |

### Response file

After reading the request and making a decision, write the response to:

```
/tmp/avenor-permission.req.response
```

The response is also JSON:

```json
{
  "outcome": "selected",
  "option_id": "allow",
  "message": "Approved by operator"
}
```

| Field | Default | What it means |
|-------|---------|---------------|
| `outcome` | "selected" | Either "selected" (the operator chose one of the provided options) or "cancelled" (the request was cancelled, blocking execution). Omit to default to "selected". |
| `option_id` | Required | The `optionId` from the request's option list. Avenor uses this to determine whether to allow or deny. |
| `message` | Empty string | Optional note or explanation. Relayed back to the backend. |

## Important: Stale response cleanup

Before writing a new `.req` file, Avenor deletes any existing `.req.response` from the previous request. This is a footgun if you're slow: if you take longer than expected to write your response, you may overwrite a stale one. The handler still reads and uses it — you just won't see evidence of the old request on disk.

**Takeaway:** Don't assume the presence of a `.req.response` file means you answered the currently pending request. The response file can be younger or older than you think. If you need to track request-response pairing, lean on `request_id` matching.

## Cleanup behavior

Avenor does not delete request or response files after completion. Surrounding tools and operators can inspect what happened, when, and what was decided. You're responsible for cleanup if you want it.

## Failures and timeouts

If no response file appears within the timeout window (default 10 minutes), Avenor returns a timeout error and halts execution. The request file remains on disk for inspection. At that point, either:

- An external system is responsible for creating the response file (if the timeout was a race)
- Or the execution has failed and you need to debug why the approval process stalled

There's no automatic retry or fallback — the decision to hang or error is entirely yours.

## Writing responses with `avenor answer`

Manually constructing the JSON response file is error-prone: you must format it correctly, validate the option ID, and write it atomically. The `avenor answer` command does that for you.

It reads the `.req` file, validates your chosen option against the offered set, and writes the `.req.response` file in one atomic operation. The command returns 0 on success, 2 on validation errors (missing file, invalid option, response already exists), and 1 on I/O failures.

### Basic usage

```sh
avenor answer /tmp/avenor-permission --option allow
```

This reads `/tmp/avenor-permission.req`, validates that `allow` is in the offered options, and writes to `/tmp/avenor-permission.req.response`. The `<perm-base>` argument (here `/tmp/avenor-permission`) must match the path you passed to `--permission-handler file:<path>`.

### All flags

| Flag | Required? | Default | Behavior |
|------|-----------|---------|----------|
| `--option <id>` | Yes | — | The option ID from the request's option list. Validation fails (exit 2) if it's not offered. |
| `--message <text>` | No | Empty string | Free-text note included in the response. Relayed back to the backend. |
| `--outcome <value>` | No | "selected" | Either "selected" (permission was granted/denied based on the option) or "cancelled" (request blocked). Exit 2 if any other value. |
| `--force` | No | false | Overwrite an existing response file. Without this flag, writing when a response already exists fails with exit 2. |

### Examples

Select an option and include a message:

```sh
avenor answer /tmp/avenor-permission --option allow --message "Approved by operator"
```

Cancel the request (block execution):

```sh
avenor answer /tmp/avenor-permission --option deny --outcome cancelled
```

Overwrite a previous response:

```sh
avenor answer /tmp/avenor-permission --option allow --force
```

### Validation behavior

`avenor answer` validates in this order:

1. Parse flags; return 2 if `--option` is missing or `--outcome` is not "selected" or "cancelled".
2. Check that the `.req` file exists; return 2 if missing, return 1 on I/O errors.
3. Parse the `.req` as JSON; return 2 on parse error.
4. Validate `--option` against the option IDs in the request; return 2 if not found.
5. Construct the response and write it atomically; return 1 on I/O errors.

If the option is not in the offered set, the error message lists all valid options:

```
avenor answer: option "invalid" is not in the offered set; valid options: allow, deny
```

### Atomic write and `--force`

The response file is written atomically via a temporary file to avoid partial writes. Without `--force`, the link operation fails with exit 2 if a response already exists (TOCTOU-safe).

```sh
avenor answer /tmp/avenor-permission --option allow
avenor answer: response already exists at /tmp/avenor-permission.req.response; pass --force to overwrite
```

With `--force`, the temporary file is renamed, overwriting the existing response:

```sh
avenor answer /tmp/avenor-permission --option allow --force
```

### What not to do

Calling `avenor answer` when no `.req` file exists fails with exit 2:

```sh
avenor answer /tmp/avenor-permission --option allow
avenor answer: request file /tmp/avenor-permission.req does not exist
```

The command does not create or manage the `.req` file — only Avenor does. If you're writing a response to a permission request, the `.req` file must already exist.

Calling `avenor answer` multiple times without `--force` fails on the second call:

```sh
avenor answer /tmp/avenor-permission --option allow
avenor answer /tmp/avenor-permission --option deny  # exit 2: response already exists
```

Use `--force` to overwrite, or manually remove the `.req.response` file first.
