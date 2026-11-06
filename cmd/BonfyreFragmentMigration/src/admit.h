#ifndef BONFYRE_FRAGMENT_MIGRATION_ADMIT_H
#define BONFYRE_FRAGMENT_MIGRATION_ADMIT_H

int bf_fm_admit(const char *drafts, const char *db_path, const char *schemas,
                const char *namespace_name, const char *cms_bin, int apply);

#endif
