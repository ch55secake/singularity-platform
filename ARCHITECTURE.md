# Architecture

```text
Windows collectors
        │ HTTPS + bearer token
        ▼
Singularity Platform ingress
        │ JSONL spool / future ClickHouse queue
        ▼
Metadata downloader
        │ Valve replay CDN
        ▼
Protobuf metadata parser
        │
        ▼
Demo downloader → Haste Source 2 parser
        │
        ▼
ClickHouse analytics tables
```

The platform never logs into Steam. Match salts are supplied by consenting
clients, and all server-side work begins only after a valid salt submission.
