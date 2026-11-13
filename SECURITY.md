# Security policy

AwanFS is not production-ready yet (see the status in README.md). Reports are still welcome.

## Reporting a vulnerability

Report it privately through GitHub: **Security → Report a vulnerability** on this repository. Do not open a public issue.

Please include the version (commit), the platform, and the smallest request sequence that shows the problem. You will get an answer within 7 days.

## Scope

In scope: the `s3-server` binary and its crate, including request parsing, SigV4 checks, IAM policies, TLS, and the on-disk format (a crafted volume or index that crashes or corrupts recovery).

Out of scope: the benchmark scripts, and deployments that disable authentication (`--allow-anonymous`).
