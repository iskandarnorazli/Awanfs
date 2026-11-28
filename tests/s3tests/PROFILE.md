# Conformance profile

This document states the exact `awanfs` and client configuration that `tests/s3tests/start_server.sh` uses when running `ceph/s3-tests`. Every "passes N tests" count is under this profile.

`scripts/check_profile.py` enforces that every flag and `AWANFS_*` setting in `start_server.sh` has a row here (and vice versa), that `.github/workflows/s3-tests.yml` does not invoke `awanfs` outside `start_server.sh`, and that skip reasons in `CONFORMANCE_MAP.md` are listed here.

## Settings

| Setting | Value in profile | Default | Tests that need it | Safe in production? |
|---|---|---|---|---|
| `--port` | `9000` | `9000` | All (`s3tests.conf` targets `127.0.0.1:9000`) | Yes |
| `--data-dir` | `/tmp/awanfs-data` (`$AWANFS_DATA_DIR`) | `S3_DATA_DIR` / temp | All (fresh storage directory per suite run) | Yes (use persistent storage) |
| `--auto-compaction` | `off` | `on` | All (avoids background compaction during short suite runs) | No (keep `on` in production to reclaim dead space) |
| `--region` | `default` | `us-east-1` | `test_bucket_get_location` (`api_name = default` in `s3tests.conf`) | Yes (set to deployment region) |
| `--default-bucket-ownership` | `ObjectWriter` | `BucketOwnerEnforced` | ACL tests assuming ACLs enabled on new buckets (`#122`, D5) | Optional (production recommendation: `BucketOwnerEnforced`) |
| `--config` | `/tmp/awanfs-users.toml` (copied from `tests/s3tests/users.toml` with mode `0600`) | none | Multi-user and policy tests (`alt`, `tenant`) | Yes (with real keys and scoped policies) |
| `--lifecycle-day-seconds` | `2` | `86400` | Lifecycle expiration tests (`lc_debug_interval = 2` in `s3tests.conf`) | No (keep `86400` in production; test-only flag emits startup warning) |
| `AWANFS_ACCESS_KEY` | `AWANFSMAINKEY0000000` | none | All (`[s3 main]` in `s3tests.conf`) | No (public test credential) |
| `AWANFS_SECRET_KEY` | `awanfs-main-secret-for-s3-tests-0000000000` | none | All (`[s3 main]` in `s3tests.conf`) | No (public test credential) |
| `AWANFS_LOG` | `off` | `on` (JSON line per request on stderr) | All (keeps `/tmp/awanfs.log` bounded) | Optional (keep `on` for request audit logs) |
| `tests/s3tests/users.toml` | `alt` and `tenant` in isolated accounts (`id`, `display_name`, `email`) with explicit self-account S3 actions | none | Multi-user tests (`*_alt`, `*_tenant`) | Yes (isolated per-account principals; explicit actions replace `s3:*`) |
| `tests/s3tests/s3tests.conf` | `[DEFAULT]`, `[fixtures]`, `[s3 main]`, `[s3 alt]`, `[s3 tenant]`, `[iam*]` | none | All (`S3TEST_CONF`) | N/A (client test config) |

## Skips

Skipped tests in `tests/s3tests/CONFORMANCE_MAP.md` are skipped by `pytest` because the profile or client environment lacks a required capability.

| Skip reason | Area | What would un-skip it |
|---|---|---|
| `ceph extension to bucket logging not supported at client` | `bucket-logging` (#153) | Installing ceph's custom botocore service model extension and implementing RGW logging journal and rollover endpoints |
| `kms_keyid` | `encryption` (#121) | Setting `kms_keyid` in `s3tests.conf` and passing a key file via `--sse-master-key-file` |
| `[s3 cloud] section missing cloud_storage_class` | `storage-classes` (out of scope, #162) | Configuring `[s3 cloud]` with `cloud_storage_class` in `tests/s3tests/s3tests.conf` |
| `no cloud_storage_class configured` | `storage-classes` (out of scope, #162) | Configuring `cloud_storage_class` in `tests/s3tests/s3tests.conf` |
| `requires 2 or more storage classes` | `storage-classes` (out of scope, #162) | Configuring at least two storage classes in `tests/s3tests/s3tests.conf` |
| `requires 3 or more storage classes` | `storage-classes` (out of scope, #162) | Configuring at least three storage classes in `tests/s3tests/s3tests.conf` |
| `got empty parameter set for (source_mode_key, source_storage_class, dest_storage_class)` | `storage-classes` (out of scope, #162) | Configuring at least two storage classes so pytest generates parameters for `test_lifecycle_transition_encrypted` |
| `[iam root] user does not belong to an account` | `policy` (#122) | Configuring an account-scoped `[iam root]` user for `test_bucket_policy_deny_self_denied_policy` and `test_bucket_policy_deny_self_denied_policy_confirm_header` |

## Planned settings

Area issues that add new profile settings update `start_server.sh` and the **Settings** table above in the same PR:

- `#118`: implemented: `--lifecycle-day-seconds 2` (test-only flag with startup warning) matching `lc_debug_interval = 2` in `s3tests.conf`
- `#122`: implemented: `--default-bucket-ownership ObjectWriter` (production default stays `BucketOwnerEnforced`), `[s3 main]` uses `user_id = root` and `display_name = root` matching `ROOT_ACCOUNT = "root"`, and unsigned requests without `--allow-anonymous` authenticate as `Principal::Anonymous` checked by `authorize()`
- `#121`: `--sse-master-key-file` and `kms_keyid` in `s3tests.conf`
- `#140`: `--region` matching `api_name`
- `#151`: implemented: `users.toml` sets `account`, `id`, `display_name`, and `email` matching `s3tests.conf`, with explicit self-account S3 actions instead of `s3:*`

## Suite expectations differing from AWS

- `#112` (conditional deletes): `ceph/s3-tests` marks conditional delete tests `fails_on_aws`. For `PUT` and `CompleteMultipartUpload`, a delete marker counts as absent (`If-Match` returns `404 NoSuchKey`, `If-None-Match` succeeds). For `DeleteObject` and `DeleteObjects`, missing keys/versions succeed (`204` / `<Deleted>`), while a delete marker is treated as an existing entry (`If-Match: *` succeeds; mismatched `ETag`, `LastModifiedTime`, or `Size` returns `PreconditionFailed`). No extra server flag is required.
- `#118` (lifecycle delete markers & RGW behavior): `test_lifecycle_deletemarker_expiration_with_days_tag` sets `Expiration.Days = 5` and `NoncurrentVersionExpiration.NoncurrentDays = 1` without `ExpiredObjectDeleteMarker`, expecting a sole delete marker to remain until `Expiration.Days` elapses rather than disappearing immediately. When an `Expiration` rule applies to a key, AwanFS also suppresses immediate removal of a sole delete marker until `Days`/`Date` elapses, matching the suite.
- `#121` (SSE-C over HTTP and wrong-key status code): Ceph `s3-tests` runs SSE-C over plain HTTP (`is_secure = False` in `s3tests.conf`) and expects HTTP `400` (`InvalidArgument`) when reading or copying an SSE-C object or part with a different SSE-C key (`test_encryption_sse_c_other_key`, `test_encryption_sse_c_multipart_bad_download`, `test_copy_part_enc`). Production deployments still require TLS under `--profile production`.
