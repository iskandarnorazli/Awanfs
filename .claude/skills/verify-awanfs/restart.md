# Restart keeps data

## Sub-features
- PUT, stop the server, start it on the same data directory, GET

## How to get to it (user POV)
Stopping and starting the server process.

## Driving it with the aws CLI
`j_restart`. Proof: the object's bytes and the bucket survive a process restart.

## Gotchas
A process stop keeps what reached the page cache; power loss needs #29.
