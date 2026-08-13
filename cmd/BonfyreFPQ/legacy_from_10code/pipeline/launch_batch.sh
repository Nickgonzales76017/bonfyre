#!/bin/bash
export OMP_NUM_THREADS=16
export OMP_STACKSIZE=8M
export OPENBLAS_NUM_THREADS=4
# Copy OpenMP source over git version after clone
if [ -f /workspace/v4_optimizations.c ]; then
    echo "Overlay: copying SCP'd v4_optimizations.c over git version"
    sleep 2  # wait for git clone in pod_compress.sh
fi
bash /workspace/pod_compress.sh /workspace/models_batch.txt
