# s3-tests ratchet log

Every test removed from [`passing.txt`](passing.txt) must have a row here with its
reason and the decision that allowed it (`scripts/check_ratchet_log.py`).

| test | removed in | reason | decision |
|---|---|---|---|
| test_s3::test_block_public_policy_with_principal | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_bucket_acl_canned_private_to_private | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_bucket_concurrent_set_canned_acl | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_bucket_create_special_key_names | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_bucket_policy | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_bucket_policy_another_bucket | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_bucketv2_policy | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_bucketv2_policy_another_bucket | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_expected_bucket_owner | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_get_tags_acl_public | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_multipart_upload_on_a_bucket_with_policy | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_object_acl_full_control_verify_owner | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_object_lock_changing_mode_from_governance_with_bypass | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_object_lock_get_legal_hold | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_object_lock_put_legal_hold | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_post_object_authenticated_request_bad_access_key | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_post_object_expired_policy | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_post_object_invalid_access_key | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_post_object_invalid_request_field_value | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_post_object_invalid_signature | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_post_object_missing_policy_condition | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_post_object_request_missing_policy_specified_field | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_post_object_wrong_bucket | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_put_bucket_encryption_kms | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_put_bucket_encryption_s3 | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_put_tags_acl_public | dde3ca2 | fell through to another operation before #148; 501 now | owner, 2026-11-17, #148 option A; ARCHITECTURE.md decision 61 |
| test_s3::test_object_copy_canned_acl | f3b672d | copied source object using x-amz-copy-source and expected 200 without ACL support (#151 enforces account ownership and returns 501 NotImplemented for x-amz-acl per decision 61) | owner, 2026-11-24, #151 option 3A (issuecomment-7136163072); ARCHITECTURE.md decision 61 |
