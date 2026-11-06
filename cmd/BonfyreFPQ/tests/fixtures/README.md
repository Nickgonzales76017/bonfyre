These are small FPQ! storage fixtures, with synthetic seed programs and QJL words.

`legacy-v3.fpq` was written by `src/serialize.c` from repository commit
`173d984a8e952d46f50aef9dbd3e9438be38ab8b`, compiled against the current native
objects. It contains one 1 × 256 tensor named `legacy`, in DIRECT mode, with a
ROT base seed and RAMP block seed. The block carries distortion 0.75, radius
1.25, and 65 QJL projections stored in two words.

`legacy-v4.fpq` changes only the version word to 4. This matches the interim
writer that advertised version 4 while writing the v3 body with flags zero.
Versions 1 and 2 change the version word and remove the mode byte, base-presence
byte and base seed, as required by those older layouts. They therefore load
with the default FWHT mode and no base seed.

`tests/serialization.c` checks their fields, decoded output before and after
conversion to the dictionary format, and refusal at every truncation boundary.
These fixtures establish storage compatibility; they contain no trained model
and do not establish model quality or compression of a real model.
