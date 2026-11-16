# Bucket CORS

## Sub-features
- `PUT` / `GET` / `DELETE /b?cors` (persisted in `buckets.manifest`, #149, #116)
- `OPTIONS` CORS preflight (`Origin` + `Access-Control-Request-Method`)
- `Access-Control-Allow-Origin` and `Vary: Origin` on `GET`/`HEAD`/`PUT` with `Origin` header

## How to get to it (user POV)
`put-bucket-cors`, `get-bucket-cors`, `delete-bucket-cors`, plus browser/curl `OPTIONS` preflight and `Origin`-bearing `GET`.

## Driving it with the aws CLI
`j_cors`. Proof: `put-bucket-cors` configures `https://example.com` for `GET`; `curl -X OPTIONS` with `Origin: https://example.com` and `Access-Control-Request-Method: GET` returns `Access-Control-Allow-Origin: https://example.com`; `curl` `GET` with `Origin: https://example.com` returns `Access-Control-Allow-Origin: https://example.com`; `delete-bucket-cors` removes it.

## Gotchas
- Presigned URLs with SigV2 (`*_v2`) are not supported; AwanFS accepts SigV4 and anonymous requests only.

Source: `server/src/ops/cors.rs`, `server/src/cors.rs`, `server/src/s3_api.rs`.
