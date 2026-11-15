#ifndef BONFYRE_CMS_PAGE_COMPOSER_VISUAL_H
#define BONFYRE_CMS_PAGE_COMPOSER_VISUAL_H
#include "form_tree.h"
/* Shell for the visual composition workspace: the @bonfyre/authoring bundle
 * (Puck structure, GrapesJS design, Editor.js article, narration handoff)
 * mounts on [data-aurekai-authoring] and talks to /cms/compose/page, the
 * same digest-guarded endpoint as the section composer. ?site= selects a
 * profile site workspace the bearer owns; the endpoint enforces it. */
static const char *cms_page_composer_visual_head =
"<link rel=\"stylesheet\" href=\"/assets/authoring-shell.css\">"
"<link rel=\"stylesheet\" href=\"/assets/authoring-grapes.css\">"
"<div class=\"ak-wb-strip\"><span>Studio</span><span>Visual editor</span><span class=\"ak-live\">Same saved page as the composer</span></div>"
"<section class=\"cmc-head\"><div><p class=\"ak-kicker\"><a href=\"/studio\">Studio</a> &middot; <a href=\"/cms/compose\">Composer</a> &middot; Visual</p>"
"<h1>Arrange it by eye.</h1><p class=\"ak-lede\">Structure, design and article writing over the same page record the composer saves.</p></div>"
"";
static const char *cms_page_composer_visual_tail =
"</section>"
"<div class=\"authoring-page cmc-visual\" data-aurekai-authoring><p class=\"ak-empty\">Opening the visual workspace\u2026</p></div>"
"<script type=\"module\" src=\"/assets/authoring-editor.js\"></script>";
/* The page around one "open a page address" form, which the shared renderer draws. */
static void cms_page_composer_visual_emit(void){
    static const FtInput route[]={{.label="Page address",.name="route",.placeholder="/your-page",.maxlength="127"}};
    static const FtForm open_page={.id="cmc-open",.method="get",.action="/cms/compose/advanced",.label="Open",.hook="cmc-open",.inputs=route,.ninputs=1};
    fputs(cms_page_composer_visual_head,stdout);ft_form(stdout,&open_page);fputs(cms_page_composer_visual_tail,stdout);
}
#endif
