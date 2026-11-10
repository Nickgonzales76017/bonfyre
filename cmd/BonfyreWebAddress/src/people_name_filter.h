#ifndef BONFYRE_PEOPLE_NAME_FILTER_H
#define BONFYRE_PEOPLE_NAME_FILTER_H
#include <string.h>
/* A printed credit that reads as a person's name. Feeds put domains, mailboxes,
 * handles, shows and whole outlets in the same field; those are kept in the
 * archive but are not people, and listing them under People made the page read
 * as a list of websites. This only filters; it never joins two names. */
static int people_word(const char *lower, const char *word, int whole) {
    size_t n = strlen(word);
    for (const char *p = strstr(lower, word); p; p = strstr(p + 1, word)) {
        int before = p == lower || p[-1] == ' ', after = p[n] == 0 || p[n] == ' ';
        if (!whole || (before && after)) return 1;
    }
    return 0;
}
static int people_name_plausible(const char *s) {
    static const char *const stems[] = {"podcast","network","universit","college","magazine","studio","associat","department","council",
        "institute","foundation","society","services","library","editorial","editors","commons","archive","developers","official","company",
        "government","agency","channel","journal","heritage","america","productions","records","newsroom","weekly","media","radio","press",NULL};
    static const char *const words[] = {"news","show","team","staff","inc","llc","ltd","post","times","tv","fm","club","daily","the","home","group","office","review","trust","games","blog","admin","webmaster","unknown","anonymous",NULL};
    char lower[128]; size_t n = s ? strlen(s) : 0, i; int letters = 0;
    if (n < 2 || n >= sizeof(lower) || (s[0] >= 'a' && s[0] <= 'z')) return 0;
    for (i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if ((c >= '0' && c <= '9') || strchr("@/:|#<>\\_=+[]{}", c)) return 0;
        if (c == '.' && i + 1 < n && s[i + 1] != ' ') return 0;
        if (c >= 'A' && c <= 'Z' && i + 2 < n && s[i + 1] >= 'A' && s[i + 1] <= 'Z' && s[i + 2] >= 'A' && s[i + 2] <= 'Z') return 0;
        if ((c | 32) >= 'a' && (c | 32) <= 'z') letters++;
        lower[i] = (char)(c >= 'A' && c <= 'Z' ? c | 32 : c);
    }
    lower[n] = 0;
    if (letters < 2 || !strchr(lower, ' ')) return 0;
    for (i = 0; stems[i]; i++) if (people_word(lower, stems[i], 0)) return 0;
    for (i = 0; words[i]; i++) if (people_word(lower, words[i], 1)) return 0;
    return 1;
}
#endif
