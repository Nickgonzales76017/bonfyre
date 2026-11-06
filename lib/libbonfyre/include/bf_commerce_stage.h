/*
 * bf_commerce_stage.h — how an organ that performs one act of a sale records
 * that act in the estate's commerce chain.
 *
 * The chain is kept by bonfyre-commerce: fifteen stages per exchange, each
 * requiring the one before. An organ that quotes or takes a payment does not
 * keep its own idea of where an exchange stands; it asks the chain to record
 * the stage, and the chain refuses when the stage before is not established.
 * The organ then undoes what it was about to keep.
 *
 * The chain's organ is run as a process, found through BONFYRE_COMMERCE_BIN
 * or PATH, so the rule lives in one place.
 *
 * Header-only: an organ includes it and needs nothing rebuilt.
 */
#ifndef BF_COMMERCE_STAGE_H
#define BF_COMMERCE_STAGE_H

#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

#define BF_COMMERCE_STAGE_RECORDED 0
#define BF_COMMERCE_STAGE_REFUSED  3   /* the stage before is not established */

/* Records one stage for an exchange. Returns BF_COMMERCE_STAGE_RECORDED,
 * BF_COMMERCE_STAGE_REFUSED when the chain says the prior stage is missing,
 * or another non-zero value when the chain could not be asked at all. */
static inline int bf_commerce_stage_record(const char *commerce_db, const char *exchange, const char *stage,
                                           const char *owner, const char *detail) {
    const char *organ = getenv("BONFYRE_COMMERCE_BIN");
    char *argv[] = {
        (char *)(organ && *organ ? organ : "bonfyre-commerce"), "record",
        "--db", (char *)commerce_db, "--exchange", (char *)exchange, "--stage", (char *)stage,
        "--owner", (char *)owner, "--detail", (char *)(detail ? detail : ""), NULL
    };
    posix_spawn_file_actions_t actions;
    pid_t pid;
    int status = 0, rc;
    /* The chain's own report goes nowhere; its refusal reason stays on stderr. */
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, "/dev/null", 1 /* O_WRONLY */, 0);
    rc = posix_spawnp(&pid, argv[0], &actions, NULL, argv, environ);
    posix_spawn_file_actions_destroy(&actions);
    if (rc != 0) {
        fprintf(stderr, "cannot run %s to record the %s stage: %s\n", argv[0], stage, strerror(rc));
        return 127;
    }
    if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) return 126;
    return WEXITSTATUS(status);
}

#endif /* BF_COMMERCE_STAGE_H */
