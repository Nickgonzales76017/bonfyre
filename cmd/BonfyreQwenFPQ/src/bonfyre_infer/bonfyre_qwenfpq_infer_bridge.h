#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Stage 1 bridge.
 * This is intentionally not wired into the old Qwen binary yet.
 * Stage 2 uses this to emit/validate a BonfyreGraph plan from Qwen config.
 */
int bonfyre_qwenfpq_infer_bridge_stage1(void);

#ifdef __cplusplus
}
#endif
