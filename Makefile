.PHONY: help download last-run resume-status estimate-time estimate-size

PYTHON ?= python3
SCRIPT ?= scripts/download_ercot_public_reports.py
CONFIG ?= config/download.yaml
LOGS_DIR ?= logs/downloads
STATE_DIR ?= state
DOWNLOAD_ARGS ?=
AS_OF ?=
EST_DATASET ?=

help:
	@echo "Available targets:"
	@echo "  make download        Run downloader with --config (default: config/download.yaml)"
	@echo "  make last-run        Show latest run summary/log/failures from logs/downloads"
	@echo "  make resume-status   Show per-dataset checkpoint status from state/*.json"
	@echo "  make estimate-time   Estimate per-dataset total download time from logs + local fallback"
	@echo "  make estimate-size   Estimate per-dataset total storage from monthly CSV sizes"
	@echo ""
	@echo "Examples:"
	@echo "  make download"
	@echo "  make download CONFIG=config/download.sample.yaml"
	@echo "  make download DOWNLOAD_ARGS='--datasets-only --dataset NP4-188-CD --dataset NP4-523-CD --from-date 2025-11-01 --to-date 2025-12-31'"
	@echo "  make estimate-time"
	@echo "  make estimate-time AS_OF=2026-02-21"
	@echo "  make estimate-time EST_DATASET=NP6-346-CD"
	@echo "  make estimate-size"
	@echo "  make estimate-size AS_OF=2026-02-22"
	@echo "  make estimate-size EST_DATASET=NP6-346-CD"

download:
	@if [ ! -f "$(CONFIG)" ]; then \
		echo "Config not found: $(CONFIG)"; \
		echo "Create one from sample:"; \
		echo "  cp config/download.sample.yaml config/download.yaml"; \
		exit 1; \
	fi; \
	echo "Running downloader with config: $(CONFIG)"; \
	$(PYTHON) $(SCRIPT) --config "$(CONFIG)" $(DOWNLOAD_ARGS)

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
