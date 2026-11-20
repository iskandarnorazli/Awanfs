## Goal
Verify Must pass checking outcomes from saved fixtures.

## Gate
<details><summary>Must pass: 4 tests, each with its failure in run 12345678</summary>

| test | first line of the failure |
|---|---|
| `test_s3::test_alpha_passes` | assert 400 == 200 |
| `test_s3::test_beta_not_listed` | assert 404 == 200 |
| `test_s3::test_gamma_fails` | AssertionError: assert 500 == 200 |
| `test_s3::test_delta_moved` | assert 403 == 200 |

</details>

- `cargo test` green on both platforms.
