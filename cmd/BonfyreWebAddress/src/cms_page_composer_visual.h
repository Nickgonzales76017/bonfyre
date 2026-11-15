#ifndef BONFYRE_CMS_PAGE_COMPOSER_VISUAL_H
#define BONFYRE_CMS_PAGE_COMPOSER_VISUAL_H
/* Shell for the visual composition workspace (Stitch screen
 * aurekai_publishing_cms_artboard_imposition_editor: pages rail, canvas,
 * inspector). The @bonfyre/authoring bundle mounts on [data-aurekai-authoring]
 * and talks to /cms/compose/page, the same digest-guarded endpoint as the
 * section composer; it carries its own page list, so the shell adds no second
 * "open a page" form. ?site= selects a profile site workspace the bearer
 * owns; the endpoint enforces it. Styles: authoring.pcss on the Stitch tokens. */
static void cms_page_composer_visual_emit(void){
    fputs("<link rel=\"stylesheet\" href=\"/assets/authoring-shell.css\">"
          "<link rel=\"stylesheet\" href=\"/assets/authoring-grapes.css\">", stdout);
    fputs("<div class=\"ak-wb-strip\"><span>Studio</span><span>Visual editor</span><span class=\"ak-live\">Same saved page as the composer</span></div>", stdout);
    static const UiLayoutValue head_desktop[] = {{BF_NP_LAYOUT, 0 /* stack */}, {BF_NP_GAP, 12}};
    static const UiLayoutValue head_phone[] = {{BF_NP_GAP, 8}};
    static const UiLayout head = {{[BF_NP_VP_DESKTOP] = {head_desktop, 2}, [BF_NP_VP_PHONE] = {head_phone, 1}}};
    ui_layout_open(stdout, "section", "cmc-head cmc-visual-head", &head);
    fputs("<div>", stdout);
    fputs("<p class=\"ak-kicker\"><a href=\"/studio\">Studio</a> &middot; <a href=\"/cms/compose\">Composer</a> &middot; Visual</p>", stdout);
    ui_text(stdout, "h1", NULL, "Arrange it by eye.");
    ui_text(stdout, "p", "ak-lede", "Pick a page on the left, shape it on the canvas, and set the selected part on the right. Saving keeps a private draft; publishing is a separate review.");
    fputs("</div></section>", stdout);
    fputs("<div class=\"authoring-page cmc-visual\" data-aurekai-authoring>", stdout);
    ui_text(stdout, "p", "ak-empty", "Opening the visual workspace\xe2\x80\xa6");
    fputs("</div><script type=\"module\" src=\"/assets/authoring-editor.js\"></script>", stdout);
}
#endif
