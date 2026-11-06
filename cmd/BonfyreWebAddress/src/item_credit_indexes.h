#ifndef BF_ITEM_CREDIT_INDEXES_H
#define BF_ITEM_CREDIT_INDEXES_H
/* Derived read paths only: no item identity, payload or admission changes. */
#define BF_ITEM_CREDIT_INDEXES \
    "CREATE INDEX IF NOT EXISTS item_credit_scan ON item(family,by,source,at);" \
    "CREATE INDEX IF NOT EXISTS item_publisher_read ON item(publisher);"
#endif
