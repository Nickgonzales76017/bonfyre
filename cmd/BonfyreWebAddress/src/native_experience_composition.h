#ifndef BONFYRE_NATIVE_EXPERIENCE_COMPOSITOR_H
#define BONFYRE_NATIVE_EXPERIENCE_COMPOSITOR_H
/* The only structural plan is generated from SurfaceScene experience-programs;
 * the native PageComposition owner supplies the material and region order.
 * CSS/reader modes never create material, an effect, a grant or a publication. */
#include "ui_tree.h"
#include "../generated/native_experience_composition.h"

static const BfNativeExperienceProgram *bf_native_experience_find(const char *pattern) {
    if (!pattern) return NULL;
    for (size_t i=0;i<BF_NATIVE_EXPERIENCE_COUNT;i++)
        if (!strcmp(pattern,bf_native_experience_programs[i].pattern))
            return &bf_native_experience_programs[i];
    return NULL;
}
static const char *bf_native_experience_lane(unsigned lane) {
    switch(lane) {
        case BF_EXP_HERO: return "hero";
        case BF_EXP_RAIL: return "rail";
        case BF_EXP_WIDE: return "wide";
        default: return "primary";
    }
}
static void bf_native_experience_emit_head(FILE *out,const BfNativeExperienceProgram *program) {
    if (!program) return;
    ui_elem(out, "link", (const UiAttr[]){{"rel", "stylesheet"}, {"href", "/assets/native-experience.css"}}, 2, 0);
}
static void bf_native_experience_begin(FILE *out,const BfNativeExperienceProgram *program) {
    if (!program) return;
    ui_start(out,"div");ui_attr(out,"class","ak-experience");ui_attr_open(out,"data-ak-experience");fprintf(out,"%s",program->pattern);ui_attr_close(out);
    ui_attr_open(out,"data-exp-shell");fprintf(out,"%s",program->shell);ui_attr_close(out);ui_attr(out,"data-exp-view","overview");ui_gt(out);
    ui_start(out,"nav");ui_attr(out,"class","ak-experience-controls");ui_flag(out,"data-exp-controls");ui_attr(out,"aria-label","Reading layouts");ui_flag(out,"hidden");ui_gt(out);
    ui_start(out,"div");ui_attr(out,"class","ak-exp-view-buttons");ui_attr(out,"role","group");ui_attr(out,"aria-label","Arrange sections");ui_gt(out);
    ui_elem(out,"button",(const UiAttr[]){{"type","button"},{"data-exp-mode","overview"},{"aria-pressed","true"}},3,0);ui_copy(out,"Overview");ui_close(out,"button");
    if(program->views & BF_EXP_VIEW_FOCUS)
        (ui_elem(out, "button", (const UiAttr[]){{"type", "button"}, {"data-exp-mode", "focus"}, {"aria-pressed", "false"}}, 3, 0), ui_lit(out, "Focus"), ui_close(out, "button"));
    if(program->views & BF_EXP_VIEW_COMPARE)
        (ui_elem(out, "button", (const UiAttr[]){{"type", "button"}, {"data-exp-mode", "compare"}, {"aria-pressed", "false"}}, 3, 0), ui_lit(out, "Compare"), ui_close(out, "button"));
    (ui_close(out, "div"), ui_open(out, "label", "ak-exp-search"), ui_lit(out, "Find in these sections"), ui_elem(out, "input", (const UiAttr[]){{"type", "search"}, {"data-exp-search", NULL}, {"autocomplete", "off"}, {"aria-label", "Find in published sections"}}, 4, 0), ui_close(out, "label"), ui_elem(out, "div", (const UiAttr[]){{"class", "ak-exp-lanes"}, {"data-exp-lanes", NULL}, {"role", "group"}, {"aria-label", "Show sections by role"}, {"hidden", NULL}}, 5, 0), ui_elem(out, "button", (const UiAttr[]){{"type", "button"}, {"data-exp-lane-filter", "all"}, {"aria-pressed", "true"}}, 3, 0), ui_lit(out, "All sections"), ui_close(out, "button"), ui_elem(out, "button", (const UiAttr[]){{"type", "button"}, {"data-exp-lane-filter", "hero"}, {"aria-pressed", "false"}, {"hidden", NULL}}, 4, 0), ui_lit(out, "Highlights"), ui_close(out, "button"), ui_elem(out, "button", (const UiAttr[]){{"type", "button"}, {"data-exp-lane-filter", "primary"}, {"aria-pressed", "false"}, {"hidden", NULL}}, 4, 0), ui_lit(out, "Main"), ui_close(out, "button"), ui_elem(out, "button", (const UiAttr[]){{"type", "button"}, {"data-exp-lane-filter", "rail"}, {"aria-pressed", "false"}, {"hidden", NULL}}, 4, 0), ui_lit(out, "Related"), ui_close(out, "button"), ui_elem(out, "button", (const UiAttr[]){{"type", "button"}, {"data-exp-lane-filter", "wide"}, {"aria-pressed", "false"}, {"hidden", NULL}}, 4, 0), ui_lit(out, "Expanded"), ui_close(out, "button"), ui_close(out, "div"), ui_elem(out, "output", (const UiAttr[]){{"data-exp-status", NULL}, {"role", "status"}, {"aria-live", "polite"}}, 3, 0), ui_close(out, "output"), ui_close(out, "nav"));
}
static void bf_native_experience_region(FILE *out,const BfNativeExperienceProgram *program,
                                        size_t region_index,size_t render_index) {
    if (!program || region_index>=BF_PAGE_REGION_COUNT) return;
    fprintf(out," data-ak-region=\"%zu\" data-exp-lane=\"%s\" data-exp-region-index=\"%zu\"",
            render_index,bf_native_experience_lane(program->lane[region_index]),region_index);
}
static void bf_native_experience_focus(FILE *out,const BfNativeExperienceProgram *program) {
    if (!program || !(program->views & BF_EXP_VIEW_FOCUS)) return;
    (ui_elem(out, "button", (const UiAttr[]){{"type", "button"}, {"data-exp-focus", NULL}, {"hidden", NULL}, {"aria-pressed", "false"}}, 4, 0), ui_lit(out, "Focus section"), ui_close(out, "button"));
}
static void bf_native_experience_end(FILE *out,const BfNativeExperienceProgram *program) {
    if (!program) return;
    ui_close(out, "div");
    ui_script(out, "(()=>{'use strict';\nconst root=document.currentScript?.previousElementSibling;\nif(!root?.matches('[data-ak-experience]'))return;\nconst sections=[...root.querySelectorAll(':scope>[data-ak-region]')],bar=root.querySelector('[data-exp-controls]');\nconst buttons=[...root.querySelectorAll('[data-exp-mode]')],status=root.querySelector('[data-exp-status]'),search=root.querySelector('[data-exp-search]');\nconst laneButtons=[...root.querySelectorAll('[data-exp-lane-filter]')],laneGroup=root.querySelector('[data-exp-lanes]');\nif(!sections.length||!bar)return;\nbar.hidden=false;const liveLanes=new Set(sections.map(section=>section.dataset.expLane));\nif(laneGroup&&liveLanes.size>1){laneGroup.hidden=false;for(const button of laneButtons)if(button.dataset.expLaneFilter!=='all')button.hidden=!liveLanes.has(button.dataset.expLaneFilter);}\nfor(const button of buttons)if(button.dataset.expMode==='compare'&&sections.length<2)button.hidden=true;for(const section of sections){const focus=section.querySelector(':scope>[data-exp-focus]');if(focus)focus.hidden=false;}\nlet view='overview',lane='all',picked=[sections[0].dataset.akRegion,sections[1]?.dataset.akRegion].filter(Boolean);\nfunction update(){const query=search?.value.trim().toLocaleLowerCase()||'';const visible=sections.filter(section=>(view==='overview'||picked.includes(section.dataset.akRegion)&&\n(view==='compare'||section.dataset.akRegion===picked[0]))&&(!query||section.textContent.toLocaleLowerCase().includes(query))&&(lane==='all'||section.dataset.expLane===lane));\nfor(const section of sections){const yes=visible.includes(section);section.hidden=!yes;\nconst focus=section.querySelector(':scope>[data-exp-focus]');if(focus)focus.setAttribute('aria-pressed',String(view!=='overview'&&picked.includes(section.dataset.akRegion)));}\nfor(const button of buttons)button.setAttribute('aria-pressed',String(button.dataset.expMode===view));\nfor(const button of laneButtons)button.setAttribute('aria-pressed',String(button.dataset.expLaneFilter===lane));\nroot.dataset.expView=view;status.textContent=query?visible.length+' of '+sections.length+' published sections match':lane!=='all'?'Showing '+visible.length+' published '+lane+' sections':view==='overview'?'All '+sections.length+' published sections':\nview==='focus'?'Reading one section':visible.length+' sections side by side';}\nfor(const section of sections){const focus=section.querySelector(':scope>[data-exp-focus]');focus?.addEventListener('click',()=>{\nconst id=section.dataset.akRegion;\nif(view==='compare'){if(picked.includes(id)){if(picked.length>1)picked=picked.filter(x=>x!==id);}else picked=[...picked.slice(-1),id];}\nelse{picked=[id];view='focus';}update();});}\nfor(const button of buttons)button.addEventListener('click',()=>{const next=button.dataset.expMode;\nif(next==='compare'&&sections.length<2)return;\nif(next==='focus'&&!picked.length)picked=[sections[0].dataset.akRegion];\nif(next==='compare'&&picked.length<2)picked=[picked[0],sections.find(s=>s.dataset.akRegion!==picked[0]).dataset.akRegion];\nview=next;update();});\nsearch?.addEventListener('input',update);\nfor(const button of laneButtons)button.addEventListener('click',()=>{if(button.hidden)return;lane=button.dataset.expLaneFilter;view='overview';update();});\nroot.addEventListener('keydown',event=>{if(event.key==='/'&&!event.ctrlKey&&!event.altKey&&!event.metaKey&&!['INPUT','TEXTAREA'].includes(event.target?.tagName)){event.preventDefault();search?.focus();return;}\nif(event.key==='Escape'){if(search?.value){event.preventDefault();search.value='';update();search.focus();return;}\nif(view!=='overview'){event.preventDefault();view='overview';update();buttons[0]?.focus();return;}\nif(lane!=='all'){event.preventDefault();lane='all';update();laneButtons[0]?.focus();}}});\nupdate();\n})();");
}
#endif
