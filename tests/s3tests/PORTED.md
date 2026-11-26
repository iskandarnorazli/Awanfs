# ceph s3-tests that AwanFS covers with its own version

Some ceph s3-tests cannot run against AwanFS as written, for a reason that is not a missing feature. The suite signs every POST form with Signature Version 2, which AwanFS does not accept (owner, 2026-11-17, `CONFORMANCE_DESIGN.md` section 9). AwanFS keeps its own version of each such test: the same request and the same expected answer, signed with SigV4. That version is how the suite test counts as done (owner decision, [#119](https://github.com/iskandarnorazli/Awanfs/issues/119#issuecomment-7191269534)). [#433](https://github.com/iskandarnorazli/Awanfs/issues/433) adds the other areas and makes the registry read this file.

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
