#ifndef BF_MATRIX_CLOSURE_H
#define BF_MATRIX_CLOSURE_H

/*
 * Recursive Matrix Closure is a compiler pattern, not a semantic owner.
 *
 * It makes the exact public Matrix cut addressable as RO-Crate JSON-LD and a
 * Zarr v3 group. The group is intentionally sparse: Matrix resolves the exact
 * semantic plane; other named owners may publish immutable, receipted
 * references for the same published cut without moving their state into
 * Matrix. Unbound planes remain explicit rather than fabricated.
 */

#include <stdio.h>

#include "bf_matrix.h"

int bf_matrix_closure_emit_ro_crate(FILE *out, BfMatrix *matrix,
                                    const BfMatrixAddressResolution *resolution,
                                    const char *crate_url,
                                    const char *zarr_url);
int bf_matrix_closure_emit_zarr_group(FILE *out, BfMatrix *matrix,
                                      const BfMatrixAddressResolution *resolution,
                                      const char *crate_url);

#endif
