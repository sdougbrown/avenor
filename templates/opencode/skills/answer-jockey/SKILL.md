# answer-jockey

Use this skill when the operator needs to answer an Avenor `permission.request` handled through `--permission-handler file:<path>`.

## Inputs

You need the base handler path. Avenor writes the request to `<path>.req`. Read it to find the question, tool, and available options, then use `avenor answer` to write the response.

## Workflow

1. Read `<path>.req` to get the question and options.
2. Present the question, tool, and options to the operator.
3. Ask the operator to choose one option.
4. Run `avenor answer` to write the response atomically:

```sh
avenor answer <path> --option <option-id>
```

`<path>` is the base handler path (without `.req`). `--option` must match one of the `optionId` values from the request file. `avenor answer` validates this and fails with exit 2 if the option is not in the offered set.

To include an operator message:

```sh
avenor answer <path> --option allow --message "Approved by operator"
```

To cancel the request instead of selecting an option:

```sh
avenor answer <path> --option <any-valid-id> --outcome cancelled
```

## Request Shape

```json
{
  "request_id": "17",
  "session_id": "ses_123",
  "tool": "bash",
  "question": "Run command?",
  "options": [
    {"optionId": "allow", "kind": "allow"},
    {"optionId": "deny", "kind": "reject"}
  ]
}
```

If there is no safe answer, choose the deny/reject option when present. Do not use `QUESTION:` prose markers.
