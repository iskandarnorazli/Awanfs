# Contributing

Thanks for your interest. The plan of record is the issue milestones and `ROADMAP_AND_DEPENDENCIES.md`; decisions are in `ARCHITECTURE.md`. Open an issue before a large change.

## Rules every change follows

- **A test that fails before the change.** Write the test first. Then turn the fix off and show the test fail again (the negative control), and say so in the commit message.
- **No wall-clock thresholds in tests.** Count work with `S3ServiceState::work` or check behaviour.
- **Never weaken, skip or delete a test** to get green.
- **Windows and Linux.** CI runs both; a red job on either is a regression.
- **On-disk format** changes follow `FORMAT.md`: a newer format is refused, older ones are read as they are.
- **Evidence in the commit message:** the tests added, the commands run and their results.

## Running the tests

```
cargo test --locked --manifest-path server/Cargo.toml
```

A change to PUT, DELETE, CopyObject, multipart completion, `write_needle` or recovery also runs the object-version checks described in `verification/`.

## License

By contributing, you agree that your contributions are licensed under the Apache License, Version 2.0 (`LICENSE`).
