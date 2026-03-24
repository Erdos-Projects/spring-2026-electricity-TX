#!/usr/bin/env python3
"""Execute remaining cells without output: 41, 49-54.
Uses nbclient to run them in a single kernel with all dependencies."""
import nbformat
from jupyter_client.manager import KernelManager
from nbclient import NotebookClient

NB_PATH = 'model_regression.ipynb'

# Dependencies that need to run first (all have existing outputs, but
# we need them in this kernel for state):
# Cell 5: §6 baseline (loads data, trains Ridge/XGB v1, defines FEATURE_COLS)
# Cell 12: add_engineered_features, XGB v2, FEATURE_COLS_V2
# Cell 14: make_seasonal, build_garch_vol
# Cell 29: §7.3 GARCH + XGB v3 CV (defines FEAT_V2_XGB, FEAT_V3, combined, TRAIN_END_FINAL)
# Cell 30: §7.3 final model training (trains XGB v3 tuned, saves pkl)
# Cell 45: §9 setup (defines resid, spike_mask, test, combined, y_te, pred_v3)

# For cell 41: needs 12, 14, 29 (for FEAT_V2_XGB, build_garch_vol, add_engineered_features)
# For cells 49-54: needs 12, 14, 29, 30, 45 (for resid, test, combined, spike_mask)

# Run dependencies + target cells
DEPS = [5, 12, 14, 29, 30, 45]
TARGET_CELLS = [41, 49, 50, 51, 52, 53, 54]
ALL_CELLS = DEPS + TARGET_CELLS

nb = nbformat.read(NB_PATH, as_version=4)

km = KernelManager(kernel_name='python3')
km.start_kernel()
kc = km.client()
kc.wait_for_ready(timeout=120)

client = NotebookClient(nb, timeout=3600, kernel_name='python3')
client.km = km
client.kc = kc

print(f"Executing {len(ALL_CELLS)} cells: {ALL_CELLS}\n", flush=True)

for i in ALL_CELLS:
    cell = nb.cells[i]
    first_line = cell.source.split('\n')[0][:75]
    is_target = i in TARGET_CELLS
    marker = " ★" if is_target else ""
    print(f"{'='*70}", flush=True)
    print(f"Cell {i}{marker}: {first_line}", flush=True)
    print(f"{'='*70}", flush=True)

    try:
        client.execute_cell(cell, i)
    except Exception as e:
        print(f"  ERROR: {e}", flush=True)
        if is_target:
            print(f"  Target cell {i} FAILED — continuing", flush=True)
        continue

    # Print outputs for target cells
    if is_target:
        for out in cell.get('outputs', []):
            if out.output_type == 'stream':
                print(out.text, end='', flush=True)
            elif out.output_type == 'execute_result':
                if 'text/plain' in out.data:
                    print(out.data['text/plain'], flush=True)
            elif out.output_type == 'error':
                print(f"  ERROR: {out.ename}: {out.evalue}", flush=True)
            elif out.output_type == 'display_data':
                if 'text/plain' in out.data:
                    print(f"  [figure: {out.data['text/plain'][:60]}]", flush=True)
    else:
        print(f"  (dep cell done)", flush=True)
    print(flush=True)

km.shutdown_kernel()

# Save outputs for target cells only
nb_save = nbformat.read(NB_PATH, as_version=4)
for i in TARGET_CELLS:
    nb_save.cells[i]['outputs'] = list(nb.cells[i].get('outputs', []))
nbformat.write(nb_save, NB_PATH)
print(f"\nDone. Saved outputs for cells {TARGET_CELLS}", flush=True)
