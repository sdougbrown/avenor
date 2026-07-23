# agy 1.1.5 typed field map (evidence only)

**Scope.** Exact protobuf names/numbers/types are from the locally extracted 1.1.5 descriptors. `observed` means that exact path occurred in the cited redacted capture. A listed descriptor field without an observation is **not** a claim that it is populated or semantic in this build/run. Opaque string values and all text/content are redacted in captures.

## Envelope and roots

| Wire path | Descriptor type | Evidence |
|---|---|---|
| grpc-web data frame `0x00` → `StreamAgentStateUpdatesResponse.update=1` | `exa.jetski_cortex_pb.StreamAgentStateUpdatesResponse` → `AgentStateUpdate` | `raw/jetski_cortex.decoded.txt` lines 1718–1726; `raw/text-stream.safe.json` and `raw/reconnect-second-stream.safe.json` |
| `GetCascadeTrajectoryStepsResponse.steps=1` (repeated) | `gemini_coder.Step` | `raw/language_server.decoded.txt` lines 4363–4372; `raw/targeted-text-typed-snapshot.json` |

`StreamAgentStateUpdates` and `GetCascadeTrajectorySteps` therefore carry the same `gemini_coder.Step` element type, but stream updates additionally pair changed elements with `StepsUpdate.indices`.

## Identity, ordering, status, terminal state

| Exact field path (number/type) | Observation / interpretation boundary |
|---|---|
| `AgentStateUpdate.conversation_id=1:string`; `trajectory_id=2:string` | Observed as distinct redacted 36-byte opaque values in stream: `raw/reconnect-first-stream.safe.json`, `raw/reconnect-second-stream.safe.json`. Do not equate either name with the `StartCascadeResponse.cascade_id` without an explicit validation call. |
| `AgentStateUpdate.status=3:CascadeRunStatus`; `executable_status=4:CascadeRunStatus`; `executor_loop_status=5:CascadeRunStatus`; `fully_idle=18:bool` | Observed. `CascadeRunStatus` descriptor maps `1=IDLE`, `2=RUNNING`, `3=CANCELING`, `4=BUSY` in `raw/cortex.decoded.txt` lines 23028–23048. Active reconnect capture records `2`, later `1` with `fully_idle=1`: `raw/reconnect-comparison.json`. This is the evidence-backed terminal predicate for this run: `status==IDLE && fully_idle==true`; do not use trailer absence as a terminal predicate. |
| `AgentStateUpdate.main_trajectory_update=7:TrajectoryUpdate` → `TrajectoryUpdate.steps_update=1:StepsUpdate` → `StepsUpdate.indices=1:repeated uint32`, `steps=2:repeated gemini_coder.Step`, `total_length=3:uint32` | Descriptor: `raw/jetski_cortex.decoded.txt` lines 660–784. Reconnect capture observes matching step indices 0,1,2,3 and total-length updates: `raw/reconnect-comparison.json`. The capture supports positional pairing of each `indices[i]` to `steps[i]` within one update only. |
| `gemini_coder.Step.type=1:CortexStepType`; `status=4:CortexStepStatus`; `metadata=5:CortexStepMetadata` | Descriptor: `followup/raw/trajectory.decoded.txt` lines 224–279. The safe snapshots observe completed status numeric `3`; the enum maps `3=DONE`, `8=GENERATING`, etc. in `raw/cortex.decoded.txt` lines 22972–23020. |
| Snapshot ordinal | `GetCascadeTrajectoryStepsResponse.steps` has no separate index field. The response order is observed but **no cross-call ordinal/offset reconciliation rule was experimentally established**. Use stream `StepsUpdate.indices` as the identity source, and treat snapshot position as a state read requiring a separately tracked requested `step_offset`. |

## Assistant text and thinking

| Exact field path (number/type) | State |
|---|---|
| `Step.planner_response=20:CortexStepPlannerResponse` → `response=1:string` | **Observed** in `raw/targeted-text-typed-snapshot.json` (redacted 16-byte text) and `raw/targeted-readonly-typed-snapshot.json`. Descriptor excerpt: `followup/raw/trajectory.decoded.txt` Step field 20; `raw/cortex.decoded.txt` lines 15573–15583. This is the only observed assistant-text carrier. |
| `...planner_response.thinking=3:string`; `raw_thinking=16:string`; `modified_response=8:string`; `thinking_redacted=5:bool`; `thinking_duration=11:google.protobuf.Duration` | Descriptor fields: `raw/cortex.decoded.txt` lines 15573–15660. **Absent in all fresh safe/reconnect snapshots and streams.** No aliasing of `response` to thinking is justified. |

The reconnect capture contains repeated `Step.planner_response` updates at the same observed index `2` with increasing redacted message lengths while the step has status `8=GENERATING`, then the same step is status `3=DONE` (`raw/reconnect-comparison.json`). This establishes replacement/update behavior for the opaque full protobuf value, not an independently proven character-delta encoding.

## Tool calls, read-only result, errors, and interactions

| Exact field path (number/type) | State |
|---|---|
| `Step.planner_response=20` → `tool_calls=7:repeated exa.codeium_common_pb.ChatToolCall` | **Observed** in the harmless ListDirectory turn: `raw/targeted-readonly-typed-snapshot.json`. `ChatToolCall` is `id=1:string`, `name=2:string`, `arguments_json=3:string`, with invalid/original/signature fields 4–9 (`raw/codeium_common.decoded.txt` lines 8226–8295). Values are redacted; no tool-call ID correlation is inferred. |
| `Step.list_directory=15:CortexStepListDirectory` → `directory_path_uri=1:string`, `children=2:repeated string`, `results=3:repeated ListDirectoryResult`, `dir_not_found=4:bool`, `file_permission_request=5:FilePermissionInteractionSpec` | **Observed** `directory_path_uri` and `results` in the read-only disposable-workspace turn: `raw/targeted-readonly-typed-snapshot.json`. Descriptor excerpt: `raw/cortex.decoded.txt` lines 14223–14261; the Step oneof selector is `followup/raw/trajectory.decoded.txt` lines 1050–1090. This is an observed read-only tool result carrier. `children`, `dir_not_found`, and `file_permission_request` are absent in that capture. |
| `Step.error=31:CortexErrorDetails`; `TrajectoryUpdate.last_step_error=7:CortexErrorDetails` | Descriptor-backed error paths (`followup/raw/trajectory.decoded.txt` lines 255–267; `raw/jetski_cortex.decoded.txt` lines 706–717). **Absent in fresh captures.** Do not map a missing/error-free tool result to either path. |
| `Step.requested_interaction=56:RequestedInteraction`; `Step.completed_interactions=147:repeated CompletedInteraction` | Descriptor paths are in `followup/raw/trajectory.decoded.txt` lines 280–296. **Absent in fresh read-only capture** (the ListDirectory completed without a request). |
| `HandleCascadeUserInteractionRequest.cascade_id=1:string`, `interaction=2:CascadeUserInteraction`; `interaction.trajectory_id=1:string`, `step_index=2:uint32`, oneof `deploy=4:CascadeDeployInteraction`; `deploy.cancel=1:bool` | Descriptor excerpts: `raw/language_server.decoded.txt` lines 5420–5435; `raw/cortex.decoded.txt` lines 12037–12234 and 11116–11139. Prior preserved denial uses exactly trajectory, step and deploy field 4 with an empty deploy payload, returns grpc status 0: `../raw/permission-deny.safe.json`, `../raw/permission-result.json`. It is a default-false/denial probe only; other variants, timeout/replacement, and response semantics are **absent**. |

## Usage/token counts and model/version metadata

| Exact field path (number/type) | State |
|---|---|
| `Step.metadata=5:CortexStepMetadata` → `model_usage=9:ModelUsageStats` → `model=1:Model`, `input_tokens=2:uint64`, `output_tokens=3:uint64`, `cache_write_tokens=4:uint64`, `cache_read_tokens=5:uint64`, `thinking_output_tokens=9:uint64`, `response_output_tokens=10:uint64` | Exact descriptor: `raw/cortex.decoded.txt` lines 3795–3890 and `raw/codeium_common.decoded.txt` lines 8803–8895. A completed reconnect snapshot contains `metadata.model_usage` with fields 1,2,3,5,6,8,10,11 (including the listed token fields that appear); see `raw/reconnect-snapshot-after-reconnect.safe.json`. The map does **not** assert a billing interpretation beyond descriptor names. |
| `Step.metadata.generator_model=11:Model`; `requested_model=13:ModelOrAlias`; `model_info=24:ModelInfo`; `step_generation_version=21:uint32` | Descriptor: `raw/cortex.decoded.txt` lines 3795–3905. `generator_model=312` and `step_generation_version=1` occur in the reconnect snapshot (`raw/reconnect-snapshot-after-reconnect.safe.json`). `requested_model` and `model_info` are **absent** in fresh captures. |
| `CortexStepGeneratorMetadata.chat_model=1:ChatModelMetadata` → `response_model=19:string`, `response_model_full=22:string`, `model_display_name=21:string` | Descriptor: `raw/cortex.decoded.txt` lines 3021–3070 and 15520–15630. **Not fetched/observed in this follow-up.** This is a possible descriptor path, not a validated mapping. |
| `GetAvailableModelsResponse.response=1:FetchAvailableModelsResponse` → `models=1:map<string,ModelDetails>` | Descriptor: `../raw/language_server.decoded.txt` lines 207–215 and `../raw/model_configs.decoded.txt` lines 611–620. Both operational listeners answered the probe; outputs are redacted in `raw/listener-role-heartbeat-models.safe.json`. Model slug/display/version strings are not retained or mapped. |

## Go implementation recommendation

**Use generated bindings from an exact, versioned descriptor/source closure; use `google.golang.org/protobuf` reflection only as a controlled fallback. Do not use a hand-written protowire mapper for typed steps.**

* Generated bindings make the nested `Step` oneof, enums, maps, repeated positional `indices`/`steps`, and `ChatToolCall` type-checkable. Store the descriptor-set SHA-256 and agy version beside generated sources; regenerate only after descriptor diff review. The present module has no protobuf runtime dependency (`avenor/go.mod`), so this adds and pins `google.golang.org/protobuf` plus a reproducible local generator toolchain. Generated code must come from the complete 1.1.5 import closure, not only the five extracted files.
* A dynamic descriptor (`descriptorpb.FileDescriptorSet` + `protoreflect`) is acceptable for an inspect/compatibility layer, and can decode a descriptor mismatch without rebuilding. It has the same protobuf runtime dependency and requires loading/resolving the full import closure; it moves type failures to runtime and should not be the primary Stage 10–12 event mapper.
* A small `protowire` layer is suitable only for grpc-web envelopes/trailers and perhaps the already-validated unary outer requests. It is unsuitable for these typed payloads: it would manually encode schema coupling for nested messages/oneofs/enums and silently drift across agy versions.
