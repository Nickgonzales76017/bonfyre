#ifndef BONFYRE_PUBLIC_HOME_SEARCH_H
#define BONFYRE_PUBLIC_HOME_SEARCH_H

/* Inject the native-HTMX public observation search aperture into an authored
 * home document. The browser submits q/limit only; it never renders source
 * records or calls the external-estate JSON service itself. */
int bf_apply_public_home_search(char **bodyp);

#endif
