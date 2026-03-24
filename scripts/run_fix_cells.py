#!/usr/bin/env python3
"""Execute fixed cells 41 and 49, saving outputs into the notebook."""
import nbformat
from jupyter_client.manager import KernelManager
from nbclient import NotebookClient

NB_PATH = 'model_regression.ipynb'

# Dependencies:
# Cell 5: §6 baseline
# Cell 12: add_engineered_features
# Cell 14: make_seasonal, build_garch_vol
# Cell 29: §7.3 GARCH + XGB v3 CV (FEAT_V2_XGB)
# Cell 30: §7.3 final model training
# Cell 45: §9 setup (combined, test, y_te, FEAT_V2_names)

DEPS = [5, 12, 14, 29, 30, 45]
TARGET_CELLS = [41, 49]
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
