#!/usr/bin/env python3
"""Execute all code cells in presentation.ipynb and save outputs."""
import nbformat
from jupyter_client.manager import KernelManager
from nbclient import NotebookClient

NB_PATH = 'presentation.ipynb'

nb = nbformat.read(NB_PATH, as_version=4)

km = KernelManager(kernel_name='python3')
km.start_kernel()
kc = km.client()
kc.wait_for_ready(timeout=120)

client = NotebookClient(nb, timeout=600, kernel_name='python3')
client.km = km
client.kc = kc

code_cells = [i for i, c in enumerate(nb.cells) if c.cell_type == 'code']
print(f"Running {len(code_cells)} code cells: {code_cells}\n", flush=True)

for i in code_cells:
    cell = nb.cells[i]
    first_line = cell.source.split('\n')[0][:75]
    print(f"Cell {i}: {first_line}", flush=True)

    try:
        client.execute_cell(cell, i)
    except Exception as e:
        print(f"  ERROR: {e}", flush=True)
        continue

    for out in cell.get('outputs', []):
        if out.output_type == 'stream':
            print(out.text[:200], end='', flush=True)
        elif out.output_type == 'error':
            print(f"  ERROR: {out.ename}: {out.evalue}", flush=True)
        elif out.output_type == 'display_data':
            if 'text/plain' in out.data:
                print(f"  [figure: {out.data['text/plain'][:60]}]", flush=True)
    print(flush=True)

km.shutdown_kernel()

nbformat.write(nb, NB_PATH)
print(f"\nDone. Saved outputs for {NB_PATH}", flush=True)
