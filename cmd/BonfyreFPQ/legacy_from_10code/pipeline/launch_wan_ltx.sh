#!/bin/bash
export OMP_NUM_THREADS=32
export OMP_STACKSIZE=8M
export OPENBLAS_NUM_THREADS=8
bash /workspace/pod_compress.sh /workspace/models_wan_ltx.txt
