# ObzenFlow's Security Policy

The ObzenFlow project takes security seriously. If you believe you have found a security vulnerability, please report it responsibly so we can investigate and fix it.

## Reporting a Vulnerability

**Preferred:** Use **GitHub Security Advisories** for private reporting (GitHub repo → `Security` → `Report a vulnerability`).

If private reporting is not available, open a GitHub issue asking for a private channel **without including sensitive details** (no exploits, tokens, customer data, or endpoint URLs).

### What to Include

- A clear description of the issue and potential impact
- Steps to reproduce (ideally a minimal PoC)
- Affected versions/commit SHA
- Any relevant logs, screenshots, or configs (with secrets redacted)

## Coordinated Disclosure

- We aim to acknowledge reports with urgency.
- We’ll provide a status update once we can reproduce and assess severity.
- We’ll coordinate a fix and disclosure timeline with the reporter when possible.

## Scope

In scope:
- Vulnerabilities in this repository’s code and official release artifacts

Out of scope (generally):
- Issues in third-party services or infrastructure outside this repo
- Social engineering, phishing, physical attacks
- Findings that require already-compromised credentials or local root/admin access

## Supported Versions

Security fixes are typically applied to:
- `main` (and the latest release, once releases are published)

Older versions may not receive patches; upgrading is recommended.

