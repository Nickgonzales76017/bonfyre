#ifndef BONFYRE_PUBLIC_CODE_H
#define BONFYRE_PUBLIC_CODE_H
#include <stddef.h>
#include <stdio.h>

/* Presentation of an already-resolved, guest-readable item address. This is
 * the durable/contextual web-URI carrier only: no capability, identity claim,
 * session, redemption, pairing, ticket, payment or private snapshot is issued. */
#define BF_PUBLIC_CODE_URI_MAX 512
int bf_public_code_url(const char *host,const char *path,char *out,size_t cap);
int bf_public_code_emit_svg(FILE *out,const char *url);
int bf_public_code_emit_details(FILE *out,const char *host,const char *path);

/* Recognize only the QR representation suffix. The caller still resolves the
 * original item through its ordinary public reader before emitting any code. */
int bf_public_code_item_path(const char *request,char *item,size_t cap);
#endif
