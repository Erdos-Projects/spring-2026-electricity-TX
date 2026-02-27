# Brief Manual: ERCOT Data Download (Setup -> Run)

This is the shortest end-to-end flow for this repo.
`make download` performs API download and monthly sorting in one run.

Shared range used here:
- `2017-07-01` to `2024-12-31`

Supported datasets in this workflow:
- `NP6-346-CD`
- `NP3-565-CD`
- `NP4-732-CD`
- `NP4-745-CD`
- `NP6-905-CD`
- `NP6-788-CD`
- `NP4-523-CD`
- `NP4-188-CD`
- `NP3-233-CD`
- `NP3-911-ER`

## 1) Enter project root
Why this step: make sure config and script paths resolve.

```bash
cd /Users/cielo69/github/spring-2026-electricity-TX
```

## 2) Create Python environment
Why this step: isolate dependencies for downloader execution.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install requests pyyaml tqdm
```

## 3) Create local config
Why this step: keep local settings in a git-ignored file.

```bash
mkdir -p config
cp -i config/download.sample.yaml config/download.yaml
```

Config note: `delete_source_after_consolidation` defaults to `false` in the sample config.
This keeps per-doc source CSVs on disk after monthly consolidation so `backfill_post_datetime.py` can read them.
Do not set this to `true` until `postDateTime` backfill is complete for all datasets.

## 4) Export ERCOT credentials
Why this step: keep secrets out of files and command history.

```bash
read -r "ERCOT_API_USERNAME?Username: "
read -rs "ERCOT_API_PASSWORD?Password: "; echo
read -r "ERCOT_SUBSCRIPTION_KEY?Subscription key: "
export ERCOT_API_USERNAME ERCOT_API_PASSWORD ERCOT_SUBSCRIPTION_KEY
```

Optional quick check:

```bash
for v in ERCOT_API_USERNAME ERCOT_API_PASSWORD ERCOT_SUBSCRIPTION_KEY; do
  [ -n "${(P)v}" ] && echo "$v=SET" || echo "$v=NOT_SET"
done
```

## 5) Run full shared download range
Why this step: pull all supported datasets for the agreed shared window.

```bash
make download DOWNLOAD_FLAGS="--datasets-only \
--dataset NP6-346-CD \
--dataset NP3-565-CD \
--dataset NP4-732-CD \
--dataset NP4-745-CD \
--dataset NP6-905-CD \
--dataset NP6-788-CD \
--dataset NP4-523-CD \
--dataset NP4-188-CD \
--dataset NP3-233-CD \
--dataset NP3-911-ER \
--from-date 2017-07-01 \
--to-date 2024-12-31 \
--sort-monthly-output ascending \
--monthly-sort-strategy auto \
--bulk-chunk-size 256 \
--bulk-progress-every 10 \
--archive-progress-pages 10 \
--file-timing-frequency daily"
```

Range note:
- `NP6-331-CD` starts at `2025-12-04`, so it is intentionally omitted from the `2017-07-01` to `2024-12-31` command.

## 6) Check run output
Why this step: verify summary, failures, and latest log quickly.

```bash
make last-run
```

## 7) Check resume state
Why this step: confirm checkpoint status if you need to rerun after interruption.

```bash
make resume-status
```

## 8) Add Missing `postDateTime` (No Rebuild)
Why this step: keep existing monthly files, fill missing `postDateTime`, verify coverage, and remove redundant per-doc source files.

Date range used here:
- `2017-07-01` to `2026-02-26` (today).

Dry run first:

```bash
python3 scripts/backfill_post_datetime.py \
  --dataset NP3-911-ER \
  --from-date 2017-07-01 \
  --to-date 2026-02-26 \
  --mode add-missing \
  --order none \
  --fetch-missing-post-datetime \
  --download-missing-sources \
  --bulk-chunk-size 256 \
  --dry-run
```

Apply + verify/sort + cleanup:

```bash
# 1) backfill (bulk downloads missing sources automatically, 256 docs per request)
python3 scripts/backfill_post_datetime.py \
  --dataset NP3-911-ER \
  --from-date 2017-07-01 \
  --to-date 2026-02-26 \
  --mode add-missing \
  --order none \
  --fetch-missing-post-datetime \
  --download-missing-sources \
  --bulk-chunk-size 256

# 2) verify + enforce ascending order if needed
python3 scripts/backfill_post_datetime.py \
  --dataset NP3-911-ER \
  --from-date 2017-07-01 \
  --to-date 2026-02-26 \
  --mode add-missing \
  --order ascending \
  --verify

# 3) delete redundant source files once coverage is complete
python3 scripts/backfill_post_datetime.py \
  --dataset NP3-911-ER \
  --from-date 2017-07-01 \
  --to-date 2026-02-26 \
  --mode add-missing \
  --order none \
  --delete-redundant-sources

# 3-alt) archive redundant source files instead of deleting
python3 scripts/backfill_post_datetime.py \
  --dataset NP3-911-ER \
  --from-date 2017-07-01 \
  --to-date 2026-02-26 \
  --mode add-missing \
  --order none \
  --archive-redundant-sources-dir data/archive/ercot
```

Known-good single-command rebuild + verify:

```bash
python3 scripts/backfill_post_datetime.py \
  --dataset NP4-732-CD \
  --data-root data/raw/ercot \
  --state-dir state \
  --manifest-path data/raw/ercot/download_manifest.json \
  --mode rebuild \
  --order ascending \
  --verify
```

Large dataset note (NP6-905-CD):
- NP6-905-CD monthly CSVs are ~108MB with ~2.9M rows and all `postDateTime` empty.
- Use `--mode rebuild` to skip loading the existing monthly CSV entirely (~29s/month vs ~47s):

```bash
python3 scripts/backfill_post_datetime.py \
  --dataset NP6-905-CD \
  --from-date 2017-07-01 \
  --to-date 2026-02-26 \
  --mode rebuild \
  --order none \
  --bulk-chunk-size 256
```

Backfill performance summary:
- Missing source files are fetched in bulk (256 docs per POST request, not one GET per doc).
- Row counting uses fast line splitting (9.4× faster than DictReader).
- Each source file is read once and cached in memory for the full backfill pass.

## 9) Clear credentials
Why this step: remove secrets from current shell session.

```bash
unset ERCOT_API_USERNAME ERCOT_API_PASSWORD ERCOT_SUBSCRIPTION_KEY
```

## Output locations
- Raw files: `data/raw/ercot/<DATASET_ID>/<YYYY>/<MM>/...`
- Logs: `logs/downloads/<timestamp>/...`
- Resume checkpoints: `state/<DATASET_ID>.json`
