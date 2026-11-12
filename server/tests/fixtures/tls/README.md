Test-only certificates for tls_test.rs (#30): self-signed, P-256, CN=localhost,
SAN localhost and 127.0.0.1, valid 100 years. cert2/key2 is a second pair for the reload test.
Generated with: openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 36500 -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
Never use these keys outside tests.
