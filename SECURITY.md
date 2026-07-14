# Security Policy

## Supported Versions

| Version | Supported |
|---|---|
| 1.x | Yes |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it privately.

**Do not open a public GitHub issue** for security vulnerabilities.

**Contact:** houcem0508@gmail.com

Include in your report:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested remediation

You will receive a response within 5 business days. If the issue is confirmed, a fix will be
released as soon as practicable and you will be credited in the release notes (unless you
prefer to remain anonymous).

## Scope

This is a research and portfolio project demonstrating schema drift detection techniques.
It does not handle personal data, credentials, or production deployments.

The most relevant security considerations are:
- Kafka broker authentication configuration (not included in this repo — use SASL/SSL for production)
- Docker image dependency updates (reported via Dependabot)
- Python dependency vulnerabilities (run `pip audit` to check)
