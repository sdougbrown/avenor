# Stage 9 redaction record

An explicit scan was run on the curated corpus for home-directory paths, PEM certificate/private-key blocks, numerical loopback addresses, and numeric process IDs. The patterns were passed from shell variables so the scan command itself did not become a false positive.

Result: **no matches**. The follow-up source scan independently found no literal home paths, numeric loopback ports, or process IDs. Fixtures retain only protocol structure, fixed service/method names, redacted lengths, and SHA-256 provenance. They do not retain prompt/reply content, opaque IDs, certificates, ports, PIDs, or caches.
