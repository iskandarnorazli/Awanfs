# Benchmark runner on your own machine

`.github/workflows/bench-selfhosted.yml` runs the benchmarks that need more than 8 cores (#58)
on a GitHub Actions self-hosted runner with the label `bench`: manually (Actions, "Benchmarks on
the self-hosted runner", Run workflow) or weekly when the runner is online. It refuses machines
with 8 cores or fewer and never runs on pull requests. Everything else, the other benchmarks
included, runs on GitHub-hosted runners (decision 35).

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

## Safety (the repository is public)

Anyone can open a pull request, and a pull request from a fork can edit workflow files,
including `runs-on`. GitHub runs such a workflow only after a maintainer approves it, so:

- Settings, Actions, General, "Approval for running fork pull request workflows from
  contributors": choose **Require approval for all external contributors**.
- Never approve a workflow run from a fork when the pull request changes anything under
  `.github/`. Read the diff first.
- Only `bench-selfhosted.yml` may use `runs-on: [self-hosted, bench]`, and it runs on
  `workflow_dispatch` and `schedule` only. Never add a self-hosted runner to a workflow that
  runs on `pull_request` or `pull_request_target`.
- Keep the runner offline (stop the service) when you are not benchmarking.

## Remove it

`.\config.cmd remove --token <TOKEN from the Runners page>` (or `./svc.sh uninstall` first on
Linux).
