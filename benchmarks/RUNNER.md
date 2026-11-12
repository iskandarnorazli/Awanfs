# Benchmark runner on your own machine

`.github/workflows/bench-selfhosted.yml` runs the long benchmarks (#58) on a GitHub Actions
self-hosted runner with the label `bench`: manually (Actions, "Benchmarks on the self-hosted
runner", Run workflow) or weekly when the runner is online. It never runs on pull requests.

## Install the runner (Windows, about 10 minutes)

1. Install the tools the benchmarks use, then open a new terminal so they are on PATH:
   - Rust (rustup, stable), Python 3.10+, Git for Windows, Go 1.22+ (to build MinIO and mc).
2. On GitHub: the AwanFS repo, Settings, Actions, Runners, **New self-hosted runner**,
   Windows x64. Keep that page open: it shows a registration token valid for one hour.
3. In PowerShell:
   ```powershell
   mkdir C:\actions-runner; cd C:\actions-runner
   # Download and extract with the two commands the GitHub page shows (they pin the version).
   .\config.cmd --url https://github.com/iskandarnorazli/Awanfs --token <TOKEN> `
       --name laptop-bench --labels bench --work _work --runasservice
   ```
   `--runasservice` installs a Windows service that starts with the machine. Without it, run
   `.\run.cmd` when you want the runner online.
4. Check: Settings, Actions, Runners shows `laptop-bench` as Idle with the labels
   `self-hosted, Windows, X64, bench`.

On Linux, the same page gives the commands; use `./config.sh ... --labels bench` and
`sudo ./svc.sh install && sudo ./svc.sh start`.

## Before a run whose numbers you will quote

- Plugged in, power plan "Best performance" (Windows) or the `performance` governor (Linux).
- Close other heavy programs; no other benchmark or build running.
- Windows Defender scans every file the servers write and can halve their throughput. Exclude
  the runner's work folder (`C:\actions-runner\_work`) in Windows Security, Virus and threat
  protection, Exclusions, and say in the report that you did.
- The disk under the work folder is the disk being measured: put the runner on the NVMe drive.

## Safety

The repository is private, so only collaborators can trigger workflows. The workflow runs on
`workflow_dispatch` and `schedule` only, never on `pull_request`, so code from a pull request
never runs on this machine. Do not add `runs-on: [self-hosted, bench]` to a workflow that runs
on pull requests.

## Remove it

`.\config.cmd remove --token <TOKEN from the Runners page>` (or `./svc.sh uninstall` first on
Linux).
