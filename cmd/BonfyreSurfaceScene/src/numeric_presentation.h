/* Numeric presentation: the content-addressed layout vocabulary declared in
 * schemas/control/numeric-presentation.cue, natively. Normalize, address,
 * factor a family into a shared basis plus sparse residuals, expand a variant,
 * lower an exact numeric selection to HVM4 (checked against the C reference
 * and an egglog equivalence witness), lower the selection to CSS, and intern
 * styles from CMS readback snapshots into a derived shared-body cache.
 *
 * Presentation only: no CMS write, no publication, no route or account in
 * any address. A generated stylesheet is an unsigned derived candidate. */
#ifndef BONFYRE_SURFACE_NUMERIC_PRESENTATION_H
#define BONFYRE_SURFACE_NUMERIC_PRESENTATION_H
#include "../generated/native_numeric_presentation.h"

typedef struct {
    unsigned has;                 /* bit per property */
    int value[BF_NP_PROPS];       /* enum index or exact integer */
} BfNpView;

typedef struct { BfNpView view[BF_NP_VIEWPORTS]; } BfNpStyle;

/* `bonfyre-surface-scene numeric <verb> ...`; reads JSON on stdin. */
int bf_numeric_main(int argc, char **argv, const char *self);

#endif
