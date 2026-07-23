# Active-generation reconnect brief

## Bounded attempt R1 — CLOSED

Artifacts: `raw/reconnect-first-stream.safe.json`, `raw/reconnect-send.safe.json`, `raw/reconnect-snapshot-before-reconnect.safe.json`, `raw/reconnect-second-stream.safe.json`, `raw/reconnect-snapshot-after-reconnect.safe.json`, and compact comparison `raw/reconnect-comparison.json`.

1. A fresh cascade was started, stream 1 was opened, then a bounded no-tool numeric-output turn was sent.
2. Stream 1 received two data frames and was deliberately closed with `intentional_disconnect_after_data_frames`; it did not receive a terminal trailer. Its selected updates moved `AgentStateUpdate.status` from `1=IDLE`/`fully_idle=true` to `2=RUNNING`.
3. The snapshot immediately after loss and before reconnection was a successful empty `GetCascadeTrajectoryStepsResponse` data frame (`length: 0`), not a terminal/error response.
4. A new `StreamAgentStateUpdates` request on the same opaque conversation/cascade context then received 31 data frames. It carries indexed updates for steps 0–3, repeated step-2 planner-response replacements while step 2 is `8=GENERATING`, then step 2 `3=DONE`, and later `status=1` plus `fully_idle=true`.
5. The recovery snapshot after that stream contains the populated trajectory. It has a successful gRPC-Web terminal trailer. The stream itself stayed subscribed until the bounded client read timed out after terminal state updates; absence of a stream trailer is not treated as generation activity.

### Exact observed recovery behavior

* The first and second stream have **zero** identical data-frame hashes. The second stream is therefore not byte-for-byte replay of the two observed pre-loss frames.
* The first stream had no steps and the pre-reconnect snapshot was empty; the second stream supplied the initial indexed steps plus subsequent generating/replacement updates. This proves delivery continues after an intentional active-stream loss.
* There is no request sequence/resume-token field in this attempt (the request used only `conversation_id=1`), so the capture cannot distinguish a server-side current-state rehydration from a future-only subscription whose first meaningful updates happened after loss. Label it **non-byte-identical recovery/continuation**, not “resume token,” “replay,” or “append-only.”

## Required reconciliation algorithm (fixture-ready)

1. **Scope ownership:** bind a subscription to the opaque `StartCascadeResponse.cascade_id`, then admit an update only if its redacted/equality-preserved `AgentStateUpdate.conversation_id` is the expected subscribed conversation and retain `trajectory_id` as an opaque second key. On mismatch, emit a diagnostic and fetch a snapshot; do not merge.
2. **Identity:** for every `StepsUpdate`, require equal lengths for `indices` and `steps`; key a streamed step by `(trajectory_id, step_index)`, where `step_index = indices[i]`. A `GetCascadeTrajectorySteps` item has no wire index, so merge it only as a snapshot page at the requested `step_offset`; never invent an index from its contents.
3. **State replacement/dedupe:** canonicalize the typed protobuf step (deterministic marshal, or a semantic hash of the generated message). For an existing key: identical hash => drop; different hash => replace stored state and process fields as a new version. Never append a whole replacement message to a previous message.
4. **Text emission:** retain the last full `CortexStepPlannerResponse.response` value per step key. On a changed full value, emit only the suffix if and only if the new value has the old value as an exact byte prefix. Equal => no emission. Non-prefix replacement => emit a reconciliation diagnostic and an explicit replace event (or wait for snapshot); never slice/replay it as a delta. Apply the same conservative rule independently to `thinking`/`raw_thinking` only when those fields are actually present.
5. **Tool correlation:** retain `ChatToolCall.id` only when present; do not infer it from a `Step` index. Record the tool oneof step and its result/error as versions of the same step key. A result may arrive as a changed tool-step payload (R1 read-only evidence: `Step.list_directory.results=3`), not necessarily as a planner `tool_calls` update.
6. **Recovery loop:** on EOF, timeout, HTTP/gRPC trailer error, or a transport failure, fetch `GetCascadeTrajectorySteps` first, merge by the above state rules, then reopen `StreamAgentStateUpdates`. Reopen even after an empty snapshot while status is not the evidence-backed terminal predicate. Back off only boundedly; preserve the last successful store across attempts.
7. **Termination:** terminate only after a successful state read/update shows `CascadeRunStatus.IDLE (1)` **and** `AgentStateUpdate.fully_idle=true`, with no outstanding requested interaction. A successful grpc-web trailer proves that request completed, not that a cascade is terminal.

## Fixtures to add in Stages 10–12

* `reconnect-first-empty-steps`: initial status only, then transport loss.
* `reconnect-snapshot-empty`: successful zero-byte `GetCascadeTrajectoryStepsResponse` during active generation.
* `reconnect-second-indexed`: indices `[0]`, `[1]`, repeated `[2]` full planner-response replacements, then step 2 DONE, final IDLE/fully_idle.
* `duplicate-version`: same `(trajectory_id,index)` plus same canonical protobuf -> no output.
* `prefix-replacement` and `non-prefix-replacement`: suffix-only versus diagnostic/replace behavior.
* `list-directory-tool-result`: planner tool call plus Step field 15 result, with values redacted fixture-side.

No second attempt was required: R1 deliberately lost an active stream, observed post-loss running updates and final terminal state, and supplies the necessary conservative reconciliation rules. It does **not** claim a server replay cursor or byte-identical replay.
