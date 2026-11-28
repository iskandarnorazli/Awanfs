# ceph s3-tests that AwanFS covers with its own version

Some ceph s3-tests cannot run against AwanFS as written, for a reason that is not a missing feature. The suite signs every POST form with Signature Version 2, which AwanFS does not accept (owner, 2026-11-17, `CONFORMANCE_DESIGN.md` section 9). AwanFS keeps its own version of each such test: the same request and the same expected answer, signed with SigV4. That version is how the suite test counts as done (owner decision, [#119](https://github.com/iskandarnorazli/Awanfs/issues/119#issuecomment-7191269534)). `scripts/features.py check` checks that each row matches a `ported` entry in `features.toml` and that each AwanFS test exists ([#433](https://github.com/iskandarnorazli/Awanfs/issues/433)).

| suite test | why ported | AwanFS test |
|---|---|---|
| `test_s3::test_post_object_anonymous_request` | unsigned form to a `public-read-write` bucket (canned ACLs are #122); runs with anonymous access | `server/tests/post_object_suite_test.rs::test_post_object_anonymous_request` |
| `test_s3::test_post_object_authenticated_request` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_authenticated_request` |
| `test_s3::test_post_object_authenticated_no_content_type` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_authenticated_no_content_type` |
| `test_s3::test_post_object_authenticated_request_bad_access_key` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_authenticated_request_bad_access_key` |
| `test_s3::test_post_object_set_success_code` | unsigned form to a `public-read-write` bucket (canned ACLs are #122); runs with anonymous access | `server/tests/post_object_suite_test.rs::test_post_object_set_success_code` |
| `test_s3::test_post_object_set_invalid_success_code` | unsigned form to a `public-read-write` bucket (canned ACLs are #122); runs with anonymous access | `server/tests/post_object_suite_test.rs::test_post_object_set_invalid_success_code` |
| `test_s3::test_post_object_upload_larger_than_chunk` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_upload_larger_than_chunk` |
| `test_s3::test_post_object_set_key_from_filename` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_set_key_from_filename` |
| `test_s3::test_post_object_ignored_header` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_ignored_header` |
| `test_s3::test_post_object_case_insensitive_condition_fields` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_case_insensitive_condition_fields` |
| `test_s3::test_post_object_escaped_field_values` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_escaped_field_values` |
| `test_s3::test_post_object_success_redirect_action` | SigV2 form signature; the suite follows the redirect anonymously (canned ACLs are #122), the port checks the 303's Location | `server/tests/post_object_suite_test.rs::test_post_object_success_redirect_action` |
| `test_s3::test_post_object_invalid_signature` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_invalid_signature` |
| `test_s3::test_post_object_invalid_access_key` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_invalid_access_key` |
| `test_s3::test_post_object_invalid_date_format` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_invalid_date_format` |
| `test_s3::test_post_object_no_key_specified` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_no_key_specified` |
| `test_s3::test_post_object_missing_signature` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_missing_signature` |
| `test_s3::test_post_object_missing_policy_condition` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_missing_policy_condition` |
| `test_s3::test_post_object_user_specified_header` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_user_specified_header` |
| `test_s3::test_post_object_request_missing_policy_specified_field` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_request_missing_policy_specified_field` |
| `test_s3::test_post_object_condition_is_case_sensitive` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_condition_is_case_sensitive` |
| `test_s3::test_post_object_expires_is_case_sensitive` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_expires_is_case_sensitive` |
| `test_s3::test_post_object_expired_policy` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_expired_policy` |
| `test_s3::test_post_object_wrong_bucket` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_wrong_bucket` |
| `test_s3::test_post_object_invalid_request_field_value` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_invalid_request_field_value` |
| `test_s3::test_post_object_missing_expires_condition` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_missing_expires_condition` |
| `test_s3::test_post_object_missing_conditions_list` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_missing_conditions_list` |
| `test_s3::test_post_object_upload_size_limit_exceeded` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_upload_size_limit_exceeded` |
| `test_s3::test_post_object_missing_content_length_argument` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_missing_content_length_argument` |
| `test_s3::test_post_object_invalid_content_length_argument` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_invalid_content_length_argument` |
| `test_s3::test_post_object_upload_size_below_minimum` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_upload_size_below_minimum` |
| `test_s3::test_post_object_upload_size_rgw_chunk_size_bug` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_upload_size_rgw_chunk_size_bug` |
| `test_s3::test_post_object_empty_conditions` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_empty_conditions` |
| `test_s3::test_post_object_tags_anonymous_request` | unsigned form to a `public-read-write` bucket (canned ACLs are #122); runs with anonymous access | `server/tests/post_object_suite_test.rs::test_post_object_tags_anonymous_request` |
| `test_s3::test_post_object_tags_authenticated_request` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_tags_authenticated_request` |
| `test_s3::test_post_object_upload_checksum` | SigV2 form signature | `server/tests/post_object_suite_test.rs::test_post_object_upload_checksum` |
| `test_s3::test_encryption_sse_c_post_object_authenticated_request` | SigV2 form signature | `server/tests/sse_c_test.rs::test_encryption_sse_c_post_object_authenticated_request` |


## SigV2 request signing (`sigv2`, #433)

The suite signs these with SigV2. Each one names the AwanFS test that checks the same behaviour signed with SigV4: a port in `server/tests/sigv2_ports_test.rs`, an older Rust test, or the SigV4 version of the same suite test (in `passing.txt`).

A missing or unreadable `x-amz-date` gets 400 `AuthorizationHeaderMalformed`. The suite (SigV2, against RGW) expects 403 `AccessDenied`. AWS's documentation names no status for it, and MinIO answers 400 (`ErrMissingDateHeader`, `ErrMalformedDate` in its `cmd/api-errors.go`), so AwanFS keeps 400.

| suite test | why ported | AwanFS test |
|---|---|---|
| `test_s3::test_cors_presigned_get_object_v2` | SigV2 presigned URL; the same test signed with SigV4 is in the suite | `test_s3::test_cors_presigned_get_object` |
| `test_s3::test_cors_presigned_get_object_tenant_v2` | SigV2 presigned URL; the same test signed with SigV4 is in the suite | `test_s3::test_cors_presigned_get_object_tenant` |
| `test_s3::test_cors_presigned_put_object_v2` | SigV2 presigned URL; the same test signed with SigV4 is in the suite | `test_s3::test_cors_presigned_put_object` |
| `test_s3::test_cors_presigned_put_object_tenant_v2` | SigV2 presigned URL; the same test signed with SigV4 is in the suite | `test_s3::test_cors_presigned_put_object_tenant` |
| `test_headers::test_object_create_bad_md5_invalid_garbage_aws2` | SigV2 signature; an unreadable Content-MD5 | `server/tests/s3_details_test.rs::content_md5_rejects_non_canonical_base64` |
| `test_headers::test_object_create_bad_contentlength_mismatch_below_aws2` | SigV2 signature; a body shorter than its declared length | `server/tests/streaming_put_test.rs::a_streamed_chunked_body_shorter_or_longer_than_declared_is_refused_and_releases_stream` |
| `test_headers::test_object_create_bad_authorization_incorrect_aws2` | SigV2 signature; a signature that does not match | `server/tests/auth_test.rs::a_changed_signature_or_secret_is_refused` |
| `test_headers::test_object_create_bad_authorization_invalid_aws2` | SigV2 signature; an Authorization header that is not a signature (400 AuthorizationHeaderMalformed; the suite expects InvalidArgument) | `server/tests/sigv2_ports_test.rs::test_object_create_bad_authorization_invalid_aws2` |
| `test_headers::test_object_create_bad_ua_empty_aws2` | SigV2 signature; an empty User-Agent (succeeds) | `server/tests/sigv2_ports_test.rs::test_object_create_bad_ua_empty_aws2` |
| `test_headers::test_object_create_bad_ua_none_aws2` | SigV2 signature; no User-Agent (succeeds) | `server/tests/sigv2_ports_test.rs::test_object_create_bad_ua_none_aws2` |
| `test_headers::test_object_create_bad_date_invalid_aws2` | SigV2 signature; an unreadable date (400 AuthorizationHeaderMalformed; the suite expects 403) | `server/tests/sigv2_ports_test.rs::test_object_create_bad_date_invalid_aws2` |
| `test_headers::test_object_create_bad_date_empty_aws2` | SigV2 signature; an empty date (400 AuthorizationHeaderMalformed; the suite expects 403) | `server/tests/sigv2_ports_test.rs::test_object_create_bad_date_empty_aws2` |
| `test_headers::test_object_create_bad_date_none_aws2` | SigV2 signature; no date (400 AuthorizationHeaderMalformed; the suite expects 403) | `server/tests/sigv2_ports_test.rs::test_object_create_bad_date_none_aws2` |
| `test_headers::test_object_create_bad_date_before_today_aws2` | SigV2 signature; a date far from now (403 RequestTimeTooSkewed) | `server/tests/auth_test.rs::unknown_access_key_and_skewed_clock_are_refused` |
| `test_headers::test_object_create_bad_date_before_epoch_aws2` | SigV2 signature; a date before 1970 (400 AuthorizationHeaderMalformed; the suite expects 403) | `server/tests/sigv2_ports_test.rs::test_object_create_bad_date_before_epoch_aws2` |
| `test_headers::test_object_create_bad_date_after_end_aws2` | SigV2 signature; a date in 9999 (403 RequestTimeTooSkewed) | `server/tests/sigv2_ports_test.rs::test_object_create_bad_date_after_end_aws2` |
| `test_headers::test_bucket_create_bad_authorization_invalid_aws2` | SigV2 signature; an Authorization header that is not a signature (400 AuthorizationHeaderMalformed; the suite expects InvalidArgument) | `server/tests/sigv2_ports_test.rs::test_bucket_create_bad_authorization_invalid_aws2` |
| `test_headers::test_bucket_create_bad_ua_empty_aws2` | SigV2 signature; an empty User-Agent (succeeds) | `server/tests/sigv2_ports_test.rs::test_bucket_create_bad_ua_empty_aws2` |
| `test_headers::test_bucket_create_bad_ua_none_aws2` | SigV2 signature; no User-Agent (succeeds) | `server/tests/sigv2_ports_test.rs::test_bucket_create_bad_ua_none_aws2` |
| `test_headers::test_bucket_create_bad_date_invalid_aws2` | SigV2 signature; an unreadable date (400 AuthorizationHeaderMalformed; the suite expects 403) | `server/tests/sigv2_ports_test.rs::test_bucket_create_bad_date_invalid_aws2` |
| `test_headers::test_bucket_create_bad_date_empty_aws2` | SigV2 signature; an empty date (400 AuthorizationHeaderMalformed; the suite expects 403) | `server/tests/sigv2_ports_test.rs::test_bucket_create_bad_date_empty_aws2` |
| `test_headers::test_bucket_create_bad_date_none_aws2` | SigV2 signature; no date (400 AuthorizationHeaderMalformed; the suite expects 403) | `server/tests/sigv2_ports_test.rs::test_bucket_create_bad_date_none_aws2` |
| `test_headers::test_bucket_create_bad_date_before_today_aws2` | SigV2 signature; a date far from now (403 RequestTimeTooSkewed) | `server/tests/auth_test.rs::unknown_access_key_and_skewed_clock_are_refused` |
| `test_headers::test_bucket_create_bad_date_after_today_aws2` | SigV2 signature; a date far from now (403 RequestTimeTooSkewed) | `server/tests/auth_test.rs::unknown_access_key_and_skewed_clock_are_refused` |
| `test_headers::test_bucket_create_bad_date_before_epoch_aws2` | SigV2 signature; a date before 1970 (400 AuthorizationHeaderMalformed; the suite expects 403) | `server/tests/sigv2_ports_test.rs::test_bucket_create_bad_date_before_epoch_aws2` |
| `test_s3::test_bucket_policy_set_condition_operator_end_with_IfExists` | Suite asserts boto3 raises `MalformedPolicy` on `aws:Referer` with `StringLikeIfExists`; owner decision ([#122](https://github.com/iskandarnorazli/Awanfs/issues/122#issuecomment-7216749965) option 2A) accepts and evaluates `StringLikeIfExists` with `aws:Referer` | `server/tests/access_ported_test.rs::test_bucket_policy_set_condition_operator_end_with_if_exists` |
| `test_s3::test_object_raw_get_x_amz_expires_out_range_zero` | Suite expects 403 AccessDenied for `X-Amz-Expires=0`; owner decision ([#122](https://github.com/iskandarnorazli/Awanfs/issues/122#issuecomment-7216749965) option 3A-ii) returns 400 `AuthorizationQueryParametersError` | `server/tests/access_ported_test.rs::test_object_raw_get_x_amz_expires_out_range_and_put_expired_port` |
| `test_s3::test_object_raw_get_x_amz_expires_out_max_range` | Suite expects 403 AccessDenied for `X-Amz-Expires` above 7 days; owner decision ([#122](https://github.com/iskandarnorazli/Awanfs/issues/122#issuecomment-7216749965) option 3A-ii) returns 400 `AuthorizationQueryParametersError` | `server/tests/access_ported_test.rs::test_object_raw_get_x_amz_expires_out_range_and_put_expired_port` |
| `test_s3::test_object_raw_get_x_amz_expires_out_positive_range` | Suite expects 403 AccessDenied for negative `X-Amz-Expires`; owner decision ([#122](https://github.com/iskandarnorazli/Awanfs/issues/122#issuecomment-7216749965) option 3A-ii) returns 400 `AuthorizationQueryParametersError` | `server/tests/access_ported_test.rs::test_object_raw_get_x_amz_expires_out_range_and_put_expired_port` |
| `test_s3::test_object_raw_put_authenticated_expired` | Suite expects 403 AccessDenied for `X-Amz-Expires=-1`; owner decision ([#122](https://github.com/iskandarnorazli/Awanfs/issues/122#issuecomment-7216749965) option 3A-ii) returns 400 `AuthorizationQueryParametersError` | `server/tests/access_ported_test.rs::test_object_raw_get_x_amz_expires_out_range_and_put_expired_port` |
