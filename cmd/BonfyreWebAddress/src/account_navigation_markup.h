#ifndef BONFYRE_ACCOUNT_NAVIGATION_MARKUP_H
#define BONFYRE_ACCOUNT_NAVIGATION_MARKUP_H
#define BF_AUREKAI_ACTIVITY_ASSETS \
"<link rel=\"stylesheet\" href=\"/apps/aurekai-activity.css\">" \
"<script type=\"module\" src=\"/apps/aurekai-activity.mjs\"></script>"
#define BF_ACCOUNT_STYLE ""
/* Bounded, ephemeral navigation only. Auth still owns session and return checks. */
#define BF_ACCOUNT_REFRESH BF_AUREKAI_ACTIVITY_ASSETS
#endif
