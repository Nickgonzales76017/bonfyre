/*
 * bridge_orchestrator.h — Pipeline bridge / orchestrator helpers
 */
#ifndef BRIDGE_ORCHESTRATOR_H
#define BRIDGE_ORCHESTRATOR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdatomic.h>

/*
 * bf_orchestrator_kick — wake the orchestrator.
 *
 * Tries ioctl(orch_fd, BF_ORCH_IOCTL_KICK) first; if the kernel module is
 * absent or orch_fd is not the orch device, falls back to writing 1 to
 * event_fd (eventfd semantics).
 *
 * @orch_fd   Orchestrator device fd, or -1 if not available.
 * @event_fd  Fallback eventfd; pass -1 if no eventfd either.
 *
 * Returns 0 on success, -1 on error.
 */
int bf_orchestrator_kick(int orch_fd, int event_fd);

/*
 * bf_orchestrator_poll_spin — hybrid spin-then-poll wait.
 *
 * Spins on *seq != expect for up to min(timeout_ns, 10 µs), then
 * falls back to poll(fd, POLLIN, remaining_ms).
 *
 * @seq         _Atomic uint64_t progress counter to watch.
 * @expect      Stale value; returns 1 when *seq diverges from expect.
 * @fd          fd for poll() fallback (pass -1 to skip poll phase).
 * @timeout_ns  Total wait budget in nanoseconds (0 → 10 µs spin only).
 *
 * Returns  1  — condition observed (*seq != expect)
 *          0  — timeout
 *         -1  — poll() error (errno set)
 */
int bf_orchestrator_poll_spin(_Atomic uint64_t *seq, uint64_t expect,
                               int fd, uint64_t timeout_ns);

#ifdef __cplusplus
}
#endif

#endif /* BRIDGE_ORCHESTRATOR_H */
