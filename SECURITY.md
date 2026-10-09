# Security

- Require a unique bearer token per collector. Provision, rotate, and revoke
  tokens with the offline credentials command; only SHA-256 token hashes are
  stored.
- Keep ingestion reachable only through the private LAN or Tailscale.
- Never log salts, tokens, or Steam session data. Error responses contain only
  generic messages.
- Keep `SINGULARITY_CREDENTIALS_PATH` private and mode `0600`; restart after
  emergency revocation so the running process reloads the file.
- Do not add Steam login or Game Coordinator code to the platform.
- Treat raw replay files as private temporary data.
- Obtain consent before storing player-linked match data.
- Provide deletion and retention controls before exposing the service beyond
  the friends group.
