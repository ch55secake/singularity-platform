# Security

- Require a unique bearer token per collector.
- Keep ingestion reachable only through the private LAN or Tailscale.
- Never log salts, tokens, or Steam session data.
- Do not add Steam login or Game Coordinator code to the platform.
- Treat raw replay files as private temporary data.
- Obtain consent before storing player-linked match data.
- Provide deletion and retention controls before exposing the service beyond
  the friends group.
