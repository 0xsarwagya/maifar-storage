# Security

## Reporting a vulnerability

Please report security issues **privately** to the Maifar / Actimi team (use your organization’s standard security contact or internal issue tracker). Do not open a public issue for undisclosed vulnerabilities.

Include:

- Affected component (MQTT ingest, HTTP API, database layer, etc.)
- Steps to reproduce or proof-of-concept, if safe to share
- Suspected impact

## Notes

- This service exposes an HTTP read API and connects outbound to MQTT and PostgreSQL. Harden network access, use TLS for MQTT and Postgres where supported, and protect `DATABASE_URL` and `MQTT_URL` credentials.
- Interactive API docs load third-party scripts from CDNs; use only in trusted environments or vendor those assets if policy requires it.
