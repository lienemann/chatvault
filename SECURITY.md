# Security policy

chatvault handles deeply private data. The threat model and reporting path are
spelled out below.

## Threat model

In scope:

- **Local-only by design.** chatvault makes no network calls. A change that
  introduces network I/O without an explicit user opt-in is a vulnerability.
- **Backup-key handling.** The 64-character backup key decrypts everything.
  chatvault stores it in `XDG_CONFIG_HOME/chatvault/wa.key` with `chmod 600`
  and never logs or echoes it.
- **Side-channel leakage.** Tracebacks, log lines, and crash reports must not
  contain message bodies, JIDs, contact names, or media paths beyond the file
  count level.
- **Export discipline.** Markdown / JSONL exports name the slice they emit and
  default to opt-in for personally identifying fields where applicable.

Out of scope (these are not chatvault's job):

- Disk encryption of the host system.
- Filesystem permissions of the WA install directory itself.
- Compromise of the device by malware with full filesystem access.

## Reporting

Please report vulnerabilities privately, not in a public issue:

- Open a GitHub Security Advisory on the repo, **or**
- Email the maintainer (address listed in `pyproject.toml`).

Expected acknowledgement window: 7 days. Coordinated disclosure is the default.

## What is not a vulnerability

- A query that returns private data when the user explicitly invokes it.
- A schema field that exposes information present in the source database.
- The presence of a 64-character key in the config dir at `chmod 600`.
