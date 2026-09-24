#!/bin/sh
req=$(cat)
printf '%s' "$req" > input.json
summary=$(printf '%s' "$req" | sed 's/\\/\\\\/g; s/"/\\"/g')
printf '{"version":1,"result":"pending","subject":{"type":"pull_request","repository":"sdougbrown/avenor","pull_request":143,"revision":"cc793f7"},"observed_at":"2026-01-01T00:00:00Z","summary":"%s"}' "$summary"
