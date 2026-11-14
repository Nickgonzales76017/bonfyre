#ifndef BONFYRE_NATIVE_EXPERIENCE_COMPOSITOR_H
#define BONFYRE_NATIVE_EXPERIENCE_COMPOSITOR_H
/* The only structural plan is generated from SurfaceScene experience-programs;
 * the native PageComposition owner supplies the material and region order.
 * CSS/reader modes never create material, an effect, a grant or a publication. */
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
    fputs("<link rel=\"stylesheet\" href=\"/assets/native-experience.css\">",out);
}
static void bf_native_experience_begin(FILE *out,const BfNativeExperienceProgram *program) {
    if (!program) return;
    fprintf(out,"<div class=\"ak-experience\" data-ak-experience=\"%s\" data-exp-shell=\"%s\" data-exp-view=\"overview\">",
            program->pattern,program->shell);
    fputs("<nav class=\"ak-experience-controls\" data-exp-controls aria-label=\"Reading layouts\" hidden>"
          "<div class=\"ak-exp-view-buttons\" role=\"group\" aria-label=\"Arrange sections\">"
          "<button type=\"button\" data-exp-mode=\"overview\" aria-pressed=\"true\">Overview</button>",out);
    if(program->views & BF_EXP_VIEW_FOCUS)
        fputs("<button type=\"button\" data-exp-mode=\"focus\" aria-pressed=\"false\">Focus</button>",out);
    if(program->views & BF_EXP_VIEW_COMPARE)
        fputs("<button type=\"button\" data-exp-mode=\"compare\" aria-pressed=\"false\">Compare</button>",out);
    fputs("</div><label class=\"ak-exp-search\">Find in these sections<input type=\"search\" data-exp-search autocomplete=\"off\" aria-label=\"Find in published sections\"></label><div class=\"ak-exp-lanes\" data-exp-lanes role=\"group\" aria-label=\"Show sections by role\" hidden><button type=\"button\" data-exp-lane-filter=\"all\" aria-pressed=\"true\">All sections</button><button type=\"button\" data-exp-lane-filter=\"hero\" aria-pressed=\"false\" hidden>Highlights</button><button type=\"button\" data-exp-lane-filter=\"primary\" aria-pressed=\"false\" hidden>Main</button><button type=\"button\" data-exp-lane-filter=\"rail\" aria-pressed=\"false\" hidden>Related</button><button type=\"button\" data-exp-lane-filter=\"wide\" aria-pressed=\"false\" hidden>Expanded</button></div><output data-exp-status role=\"status\" aria-live=\"polite\"></output></nav>",out);
}
static void bf_native_experience_region(FILE *out,const BfNativeExperienceProgram *program,
                                        size_t region_index,size_t render_index) {
    if (!program || region_index>=BF_PAGE_REGION_COUNT) return;
    fprintf(out," data-ak-region=\"%zu\" data-exp-lane=\"%s\" data-exp-region-index=\"%zu\"",
            render_index,bf_native_experience_lane(program->lane[region_index]),region_index);
}
static void bf_native_experience_focus(FILE *out,const BfNativeExperienceProgram *program) {
    if (!program || !(program->views & BF_EXP_VIEW_FOCUS)) return;
    fputs("<button type=\"button\" data-exp-focus hidden aria-pressed=\"false\">Focus section</button>",out);
}
static void bf_native_experience_end(FILE *out,const BfNativeExperienceProgram *program) {
    if (!program) return;
    fputs("</div>",out);
    fputs("<script>(()=>{'use strict';\nconst root=document.currentScript?.previousElementSibling;\nif(!root?.matches('[data-ak-experience]'))return;\nconst sections=[...root.querySelectorAll(':scope>[data-ak-region]')],bar=root.querySelector('[data-exp-controls]');\nconst buttons=[...root.querySelectorAll('[data-exp-mode]')],status=root.querySelector('[data-exp-status]'),search=root.querySelector('[data-exp-search]');\nconst laneButtons=[...root.querySelectorAll('[data-exp-lane-filter]')],laneGroup=root.querySelector('[data-exp-lanes]');\nif(!sections.length||!bar)return;\nbar.hidden=false;const liveLanes=new Set(sections.map(section=>section.dataset.expLane));\nif(laneGroup&&liveLanes.size>1){laneGroup.hidden=false;for(const button of laneButtons)if(button.dataset.expLaneFilter!=='all')button.hidden=!liveLanes.has(button.dataset.expLaneFilter);}\nfor(const button of buttons)if(button.dataset.expMode==='compare'&&sections.length<2)button.hidden=true;for(const section of sections){const focus=section.querySelector(':scope>[data-exp-focus]');if(focus)focus.hidden=false;}\nlet view='overview',lane='all',picked=[sections[0].dataset.akRegion,sections[1]?.dataset.akRegion].filter(Boolean);\nfunction update(){const query=search?.value.trim().toLocaleLowerCase()||'';const visible=sections.filter(section=>(view==='overview'||picked.includes(section.dataset.akRegion)&&\n(view==='compare'||section.dataset.akRegion===picked[0]))&&(!query||section.textContent.toLocaleLowerCase().includes(query))&&(lane==='all'||section.dataset.expLane===lane));\nfor(const section of sections){const yes=visible.includes(section);section.hidden=!yes;\nconst focus=section.querySelector(':scope>[data-exp-focus]');if(focus)focus.setAttribute('aria-pressed',String(view!=='overview'&&picked.includes(section.dataset.akRegion)));}\nfor(const button of buttons)button.setAttribute('aria-pressed',String(button.dataset.expMode===view));\nfor(const button of laneButtons)button.setAttribute('aria-pressed',String(button.dataset.expLaneFilter===lane));\nroot.dataset.expView=view;status.textContent=query?visible.length+' of '+sections.length+' published sections match':lane!=='all'?'Showing '+visible.length+' published '+lane+' sections':view==='overview'?'All '+sections.length+' published sections':\nview==='focus'?'Reading one section':visible.length+' sections side by side';}\nfor(const section of sections){const focus=section.querySelector(':scope>[data-exp-focus]');focus?.addEventListener('click',()=>{\nconst id=section.dataset.akRegion;\nif(view==='compare'){if(picked.includes(id)){if(picked.length>1)picked=picked.filter(x=>x!==id);}else picked=[...picked.slice(-1),id];}\nelse{picked=[id];view='focus';}update();});}\nfor(const button of buttons)button.addEventListener('click',()=>{const next=button.dataset.expMode;\nif(next==='compare'&&sections.length<2)return;\nif(next==='focus'&&!picked.length)picked=[sections[0].dataset.akRegion];\nif(next==='compare'&&picked.length<2)picked=[picked[0],sections.find(s=>s.dataset.akRegion!==picked[0]).dataset.akRegion];\nview=next;update();});\nsearch?.addEventListener('input',update);\nfor(const button of laneButtons)button.addEventListener('click',()=>{if(button.hidden)return;lane=button.dataset.expLaneFilter;view='overview';update();});\nroot.addEventListener('keydown',event=>{if(event.key==='/'&&!event.ctrlKey&&!event.altKey&&!event.metaKey&&!['INPUT','TEXTAREA'].includes(event.target?.tagName)){event.preventDefault();search?.focus();return;}\nif(event.key==='Escape'){if(search?.value){event.preventDefault();search.value='';update();search.focus();return;}\nif(view!=='overview'){event.preventDefault();view='overview';update();buttons[0]?.focus();return;}\nif(lane!=='all'){event.preventDefault();lane='all';update();laneButtons[0]?.focus();}}});\nupdate();\n})();</script>",out);
}
#endif
