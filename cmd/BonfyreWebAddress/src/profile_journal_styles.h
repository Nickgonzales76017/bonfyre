#ifndef BONFYRE_PROFILE_JOURNAL_STYLES_H
#define BONFYRE_PROFILE_JOURNAL_STYLES_H
#include "ui_tree.h"
/* Shared profile journal styles are compiled from PostCSS. */
static void ph_journal_styles(void){
 ui_start(stdout,"link");ui_attr(stdout,"rel","stylesheet");ui_attr(stdout,"href","/assets/profile-journal.css");ui_gt(stdout);
}
#endif
