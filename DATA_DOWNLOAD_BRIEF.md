# Brief Manual: ERCOT Data Download (Setup -> Run)

This guide is self-contained and uses this repo's downloader.
By default workflow, `make download` runs download + monthly sorting in one step (based on config/flags).

Date window used below:
- Annual runs from `2018` through `2024` (inclusive)
- Each run uses `YYYY-01-01` to `YYYY-12-31`

Dataset mapping:
- `905` -> `NP6-905-CD` (Settlement Point Prices)
- `732` -> `NP4-732-CD` (Wind Power Production)
- `233` -> `NP3-233-CD` (Hourly Resource Outage Capacity)

## 1) Enter project root
Why this step: run all commands from repo root so paths like `config/...` and `scripts/...` resolve correctly.

```bash
cd /Users/cielo69/github/spring-2026-electricity-TX
```

## 2) Create Python environment and install packages
Why this step: isolate dependencies and ensure the downloader can import `requests` and `pyyaml`.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install requests pyyaml
```

## 3) Create local download config
Why this step: `config/download.yaml` stores non-secret defaults and is ignored by git.

```bash
mkdir -p config
cp -i config/download.sample.yaml config/download.yaml
```

## 4) Export ERCOT credentials in your shell
Why this step: the downloader reads these env vars, so secrets stay out of command history and files.

```bash
read -r "ERCOT_API_USERNAME?Username: "
read -rs "ERCOT_API_PASSWORD?Password: "; echo
read -r "ERCOT_SUBSCRIPTION_KEY?Subscription key: "
export ERCOT_API_USERNAME ERCOT_API_PASSWORD ERCOT_SUBSCRIPTION_KEY
```

Optional quick check (no secret values printed):

```bash
for v in ERCOT_API_USERNAME ERCOT_API_PASSWORD ERCOT_SUBSCRIPTION_KEY; do
  [ -n "${(P)v}" ] && echo "$v=SET" || echo "$v=NOT_SET"
done
```

## 5) Run annual downloads (separate per dataset)
Why this step: one-year windows are easier to monitor/resume and keep each run smaller. Keep `--bulk-chunk-size` at `256` unless troubleshooting (`1..2048` allowed), and use `--bulk-progress-every` to reduce log frequency.
If you want explicit in-run sorting behavior, add:
- `--sort-monthly-output ascending`
- `--monthly-sort-strategy timestamp` (or `forecast-aware` when needed)

Instruction to update year:
- Set `YEAR` to `2018`, run all three commands.
- Change `YEAR` to `2019`, `2020`, ... up to `2024`.

```bash
YEAR="2018"            # update this: 2018 -> 2024
FROM_DATE="${YEAR}-01-01"
TO_DATE="${YEAR}-12-31"
```

### Dataset 905 (`NP6-905-CD`)
```bash
make download DOWNLOAD_FLAGS="--datasets-only --dataset NP6-905-CD --from-date ${FROM_DATE} --to-date ${TO_DATE} --bulk-chunk-size 256 --bulk-progress-every 10 --archive-progress-pages 10 --file-timing-frequency daily"
```

### Dataset 732 (`NP4-732-CD`)
```bash
make download DOWNLOAD_FLAGS="--datasets-only --dataset NP4-732-CD --from-date ${FROM_DATE} --to-date ${TO_DATE} --bulk-chunk-size 256 --bulk-progress-every 10 --archive-progress-pages 10 --file-timing-frequency daily"
```

### Dataset 233 (`NP3-233-CD`)
```bash
make download DOWNLOAD_FLAGS="--datasets-only --dataset NP3-233-CD --from-date ${FROM_DATE} --to-date ${TO_DATE} --bulk-chunk-size 256 --bulk-progress-every 10 --archive-progress-pages 10 --file-timing-frequency daily"
```

## 6) Check latest run output
Why this step: print summary, failures, and tail of `run.log` from the newest run folder.

```bash
make last-run
```

## 7) Check resume checkpoints
Why this step: confirm per-dataset resume state for restart after interruption.

```bash
make resume-status
```

Optional (re-sort existing local CSV only, no API download):

```bash
make sort_csv SORT_FLAGS="--dataset NP6-905-CD --from-date ${FROM_DATE} --to-date ${TO_DATE}"
```

## 8) Clear credentials when done
Why this step: remove sensitive env vars from the current shell session.

```bash
unset ERCOT_API_USERNAME ERCOT_API_PASSWORD ERCOT_SUBSCRIPTION_KEY
```

## Output location
- Raw files: `data/raw/ercot/<DATASET_ID>/<YYYY>/<MM>/...`
- Run logs: `logs/downloads/<timestamp>/...` (structured lines like `EVENT_NAME key=value ...`)
- Resume checkpoints: `state/<DATASET_ID>.json`
