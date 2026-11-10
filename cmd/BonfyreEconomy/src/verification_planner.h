#ifndef BF_VERIFICATION_PLANNER_H
#define BF_VERIFICATION_PLANNER_H
#include <sqlite3.h>
/* An Economy-owned cost/coverage candidate cut. Not an evidence or dispatch owner. */
int bf_verification_command(sqlite3 *db, int argc, char **argv);
#endif
