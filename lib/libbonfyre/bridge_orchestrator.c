/*
 * bridge_orchestrator.c — Pipeline bridge / orchestrator helpers
 *
 * Provides:
 *   bf_orchestrator_kick()       — send a kick event to the orchestrator fd.
 *                                  Attempts ioctl(BF_ORCH_IOCTL_KICK) first;
 *                                  falls back to write(event_fd) if the ioctl
 *                                  is unavailable (kernel module not loaded).
 *
 *   bf_orchestrator_poll_spin()  — Hybrid 10 µs spin-then-poll wait.
 *                                  Spins on an atomic seq counter for up to
 *                                  timeout_ns (default 10 000 ns = 10 µs),
 *                                  then falls back to poll() for the
 *                                  remaining budget.
 *
 * BF_ORCH_IOCTL_KICK: the kernel ioctl number used to wake the
 * orchestrator without an explicit eventfd write.  Defined only when
 * the kernel module headers are present.  Falls back to eventfd writes
 * transparently.
 */

#define _POSIX_C_SOURCE 200809L
#include "bridge_orchestrator.h"

#include <stdatomic.h>
#include <stdint.h>
#include <limits.h>
#include <time.h>
#include <poll.h>
#include <unistd.h>
#include <errno.h>

#ifdef HAVE_SYS_IOCTL
#  include <sys/ioctl.h>
#endif

/* ── ioctl kick number ──────────────────────────────────────────── *
 *
 * Define BF_ORCH_IOCTL_KICK externally (e.g. via -DBF_ORCH_IOCTL_KICK=0xBF01)
 * when building with the kernel module.  Without it the code falls back
 * to write(event_fd) at compile time.
 */
#ifndef BF_ORCH_IOCTL_KICK
#  define BF_ORCH_IOCTL_KICK 0   /* sentinel: ioctl unavailable */
#  define BF_ORCH_IOCTL_UNAVAILABLE
#endif

/* ── arch-specific pause hint ──────────────────────────────────── */
#if defined(__x86_64__) || defined(__i386__)
#  define CPU_RELAX() __asm__ volatile("pause" ::: "memory")
#elif defined(__aarch64__) || defined(__arm__)
#  define CPU_RELAX() __asm__ volatile("yield" ::: "memory")
#else
#  define CPU_RELAX() atomic_thread_fence(memory_order_seq_cst)
#endif

/* ── helpers ────────────────────────────────────────────────────── */

static inline uint64_t monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

/* ── public API ─────────────────────────────────────────────────── */

/*
 * bf_orchestrator_kick — kick the orchestrator.
 *
 * @orch_fd   fd opened on the orchestrator device / event fd
 * @event_fd  fallback eventfd; pass -1 if no separate eventfd
 *
 * Tries ioctl(orch_fd, BF_ORCH_IOCTL_KICK, 0) when kernel module is
 * present.  On ENOTTY (fd is not the orch device) or when the kernel
 * module is absent at compile time, writes a uint64_t 1 to event_fd.
 *
 * Returns 0 on success, -1 on error (errno set by the failing syscall).
 */
int bf_orchestrator_kick(int orch_fd, int event_fd) {
#ifndef BF_ORCH_IOCTL_UNAVAILABLE
    if (ioctl(orch_fd, (unsigned long)BF_ORCH_IOCTL_KICK, 0) == 0)
        return 0;
    if (errno != ENOTTY && errno != EINVAL)
        return -1;
    /* ENOTTY/EINVAL → orch_fd is not the orch device; fall through */
    (void)orch_fd;
#else
    (void)orch_fd;
#endif
    /* Eventfd fallback: write a uint64_t 1 to wake a waiting reader */
    if (event_fd < 0) { errno = EBADF; return -1; }
    const uint64_t one = 1;
    ssize_t r;
    do { r = write(event_fd, &one, sizeof(one)); } while (r < 0 && errno == EINTR);
    return (r == (ssize_t)sizeof(one)) ? 0 : -1;
}

/*
 * bf_orchestrator_poll_spin — hybrid spin-then-poll.
 *
 * @fd          fd to fall back to poll() on (POLLIN)
 * @seq         atomic seq counter to spin on
 * @expect      value that signals progress (spin exits when *seq != expect)
 * @timeout_ns  total budget in nanoseconds (0 → use default 10 µs spin only)
 *
 * Phase 1 (spin): busy-waits checking `*seq != expect` for up to
 * min(timeout_ns, 10 000 ns).  Uses arch-specific CPU_RELAX().
 *
 * Phase 2 (poll): if the spin window elapsed without a change and
 * timeout_ns > 10 000, calls poll() for the remaining budget.
 *
 * Returns  1 if the condition was observed (*seq != expect)
 *          0 on timeout
 *         -1 on poll() error (errno set)
 */
#define SPIN_WINDOW_NS UINT64_C(10000)   /* 10 µs */

int bf_orchestrator_poll_spin(_Atomic uint64_t *seq, uint64_t expect,
                               int fd, uint64_t timeout_ns) {
    if (!timeout_ns) timeout_ns = SPIN_WINDOW_NS;

    const uint64_t deadline = monotonic_ns() + timeout_ns;
    const uint64_t spin_end = monotonic_ns() +
        (timeout_ns < SPIN_WINDOW_NS ? timeout_ns : SPIN_WINDOW_NS);

    /* ── Phase 1: spin ── */
    while (monotonic_ns() < spin_end) {
        if (atomic_load_explicit(seq, memory_order_acquire) != expect)
            return 1;
        CPU_RELAX();
    }

    /* Quick check before entering poll */
    if (atomic_load_explicit(seq, memory_order_acquire) != expect)
        return 1;

    if (fd < 0) return 0;   /* no fd to poll, budget spent */

    /* ── Phase 2: poll ── */
    for (;;) {
        uint64_t now = monotonic_ns();
        if (now >= deadline) break;

        /* Check the atomic one more time before each poll syscall */
        if (atomic_load_explicit(seq, memory_order_acquire) != expect)
            return 1;

        uint64_t remaining_ms = (deadline - now + 999999u) / 1000000u;
        if (remaining_ms > (uint64_t)INT_MAX) remaining_ms = INT_MAX;

        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int r = poll(&pfd, 1, (int)remaining_ms);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r > 0) {
            /* fd became readable; re-check seq */
            if (atomic_load_explicit(seq, memory_order_acquire) != expect)
                return 1;
        }
    }

    return 0;  /* timeout */
}
