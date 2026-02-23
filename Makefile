.PHONY: help download sort_csv last-run resume-status estimate-time estimate-size lock-dataset lock-346 unlock-dataset unlock-346

PYTHON ?= python3
SCRIPT ?= scripts/download_ercot_public_reports.py
LOGS_DIR ?= logs/downloads
STATE_DIR ?= state
AS_OF ?=
EST_DATASET ?=
LOCK_DATASET ?= NP6-346-CD

# Download target variables
DOWNLOAD_CONFIG ?= config/download.yaml
DOWNLOAD_FLAGS ?=
SORT_FLAGS ?=

help:
	@echo "Available targets:"
	@echo "  make download        Run downloader with --config (download + optional monthly sorting)"
	@echo "  make sort_csv        Optional: re-sort local monthly CSV files only"
	@echo "  make last-run        Show latest run summary/log/failures from logs/downloads"
	@echo "  make resume-status   Show per-dataset checkpoint status from state/*.json"
	@echo "  make estimate-time   Estimate per-dataset total download time from logs + local fallback"
	@echo "  make estimate-size   Estimate per-dataset total storage from monthly CSV sizes"
	@echo "  make lock-dataset    Lock all CSV files for one dataset via Git LFS (LOCK_DATASET=...)"
	@echo "  make lock-346        Shortcut for LOCK_DATASET=NP6-346-CD"
	@echo "  make unlock-dataset  Unlock all CSV files for one dataset via Git LFS (LOCK_DATASET=...)"
	@echo "  make unlock-346      Shortcut for LOCK_DATASET=NP6-346-CD"
	@echo ""
	@echo "Examples:"
	@echo "  make download"
	@echo "  make download DOWNLOAD_CONFIG=config/download.sample.yaml"
	@echo "  make download DOWNLOAD_FLAGS='--datasets-only --dataset NP4-188-CD --dataset NP4-523-CD --from-date 2025-11-01 --to-date 2025-12-31'"
	@echo "  make sort_csv"
	@echo "  make sort_csv SORT_FLAGS='--dataset NP6-905-CD --from-date 2025-01-01 --to-date 2025-12-31'"
	@echo "  make estimate-time"
	@echo "  make estimate-time AS_OF=2026-02-21"
	@echo "  make estimate-time EST_DATASET=NP6-346-CD"
	@echo "  make estimate-size"
	@echo "  make estimate-size AS_OF=2026-02-22"
	@echo "  make estimate-size EST_DATASET=NP6-346-CD"
	@echo "  make lock-346"
	@echo "  make lock-dataset LOCK_DATASET=NP6-346-CD"
	@echo "  make unlock-346"
	@echo "  make unlock-dataset LOCK_DATASET=NP6-346-CD"

download:
	@if [ ! -f "$(DOWNLOAD_CONFIG)" ]; then \
		echo "Config not found: $(DOWNLOAD_CONFIG)"; \
		echo "Create one from sample:"; \
		echo "  cp config/download.sample.yaml config/download.yaml"; \
		exit 1; \
	fi; \
	echo "Running downloader with config: $(DOWNLOAD_CONFIG)"; \
	$(PYTHON) $(SCRIPT) --config "$(DOWNLOAD_CONFIG)" $(DOWNLOAD_FLAGS)

sort_csv:
	$(PYTHON) scripts/sort_csv.py \
		--data-root "data/raw/ercot" \
		--order ascending \
		--strategy forecast-aware \
		$(SORT_FLAGS)

last-run:
	@latest="$$(find "$(LOGS_DIR)" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort | tail -n 1)"; \
	if [ -z "$$latest" ]; then \
		echo "No run folders found in $(LOGS_DIR)"; \
		exit 1; \
	fi; \
	echo "Latest run: $$latest"; \
	echo "--- summary.json ---"; \
	if [ -f "$$latest/summary.json" ]; then \
		cat "$$latest/summary.json"; \
	else \
		echo "missing: $$latest/summary.json"; \
	fi; \
	echo "--- failures.csv (first 40 lines) ---"; \
	if [ -f "$$latest/failures.csv" ]; then \
		sed -n '1,40p' "$$latest/failures.csv"; \
	else \
		echo "missing: $$latest/failures.csv"; \
	fi; \
	echo "--- run.log (last 40 lines) ---"; \
	if [ -f "$$latest/run.log" ]; then \
		tail -n 40 "$$latest/run.log"; \
	else \
		echo "missing: $$latest/run.log"; \
	fi

resume-status:
	@if [ ! -d "$(STATE_DIR)" ]; then \
		echo "State directory not found: $(STATE_DIR)"; \
		exit 1; \
	fi; \
	STATE_DIR="$(STATE_DIR)" $(PYTHON) scripts/show_resume_status.py

estimate-time:
	@args=""; \
	if [ -n "$(AS_OF)" ]; then args="$$args --as-of $(AS_OF)"; fi; \
	if [ -n "$(EST_DATASET)" ]; then args="$$args --dataset $(EST_DATASET)"; fi; \
	$(PYTHON) scripts/estimate_download_time.py --logs-dir "$(LOGS_DIR)" $$args

estimate-size:
	@args=""; \
	if [ -n "$(AS_OF)" ]; then args="$$args --as-of $(AS_OF)"; fi; \
	if [ -n "$(EST_DATASET)" ]; then args="$$args --dataset $(EST_DATASET)"; fi; \
	$(PYTHON) scripts/estimate_dataset_size.py --data-root "data/raw/ercot" $$args

lock-dataset:
	@dataset="$(LOCK_DATASET)"; \
	root="data/raw/ercot/$$dataset"; \
	if [ ! -d "$$root" ]; then \
		echo "Dataset directory not found: $$root"; \
		exit 1; \
	fi; \
	echo "Locking CSV files under $$root ..."; \
	find "$$root" -type f -name "*.csv" -print0 | \
	while IFS= read -r -d '' f; do \
		echo "git lfs lock $$f"; \
		git lfs lock "$$f" || true; \
	done; \
	echo "Done. Check locks with: git lfs locks | rg \"$$dataset\""

lock-346:
	@$(MAKE) lock-dataset LOCK_DATASET=NP6-346-CD

unlock-dataset:
	@dataset="$(LOCK_DATASET)"; \
	root="data/raw/ercot/$$dataset"; \
	if [ ! -d "$$root" ]; then \
		echo "Dataset directory not found: $$root"; \
		exit 1; \
	fi; \
	echo "Unlocking CSV files under $$root ..."; \
	find "$$root" -type f -name "*.csv" -print0 | \
	while IFS= read -r -d '' f; do \
		echo "git lfs unlock $$f"; \
		git lfs unlock "$$f" || true; \
	done; \
	echo "Done. Check locks with: git lfs locks | rg \"$$dataset\""

unlock-346:
	@$(MAKE) unlock-dataset LOCK_DATASET=NP6-346-CD
