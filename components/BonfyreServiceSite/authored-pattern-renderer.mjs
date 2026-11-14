import {PATTERN_BY_KIND} from "./authoring/presentation-patterns.mjs";
// Pure authored primitives: no raw HTML, no CSS injection, no user scripts.
const q=x=>String(x??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const ar=x=>Array.isArray(x)?x:[];
const el=(tag,attrs,body)=>"<"+tag+(attrs?" "+attrs:"")+">"+body+"</"+tag+">";
const fail=s=>{throw Error("authored component: "+s)};
const url=value=>{
 const s=String(value??"");
 if(!s)return "";
 if(/^#[a-z][a-z0-9_-]*$/i.test(s))return q(s);
 if(s.startsWith("/")&&!s.startsWith("//")&&!s.includes("..")&&!/[<>"'\s\\]/.test(s))return q(s);
 if(/^https:\/\/[^/\s@"'<>]+\.[a-z]{2,}(?:[/?#][^\s"'<>]*)?$/i.test(s))return q(s);
 fail("unsafe component link");
};
const paragraph=x=>x?el("p","",q(x)):"";
const title=p=>el("h2","",q(p.title||p.heading||""));
const list=(items,cls="ak-links")=>el("ul",'class="'+cls+'"',ar(items).map(x=>el("li","",
 x.href?el("a",'href="'+url(x.href)+'"',q(x.label)):el("span","",q(x.label)))).join(""));
const actions=items=>el("div",'class="ak-actions"',ar(items).map(x=>
 x.href?el("a",'class="ak-button" href="'+url(x.href)+'"',q(x.label)):
 el("span",'class="ak-button" aria-disabled="true"',q(x.label))).join(""));
const image=p=>p?.src?'<img loading="lazy" src="'+url(p.src)+'" alt="'+q(p.alt)+'">':"";
const inputFields=(items,id)=>ar(items).map((p,i)=>{
 const type=p.type||"text",key=id+"-field-"+i,name=String(p.name||"field"+i).replace(/[^a-z0-9_-]/gi,"");
 if(!["text","email","tel","number","url","date","password","textarea","select","checkbox"].includes(type))fail("unsafe input type");
 const attr=' id="'+key+'" name="'+q(name)+'" '+(p.required?"required":"");
 const control=type==="textarea"?el("textarea",attr,''):
 type==="select"?el("select",attr,ar(p.options).map(x=>el("option",'value="'+q(x.value)+'"',q(x.label))).join("")):
 '<input type="'+type+'"'+attr+'>';
 return el("label",'class="ak-field" for="'+key+'"',el("span","",q(p.label))+control);
}).join("");
const viewportDevices=["desktop","tablet","phone"];
const viewportLayouts=new Set(["stack","grid","columns","carousel","masonry","split","full-bleed","feature"]);
export function authoredResponsiveAttributes(node){
 const attrs=[];
 for(const device of viewportDevices){
  const value=node?.responsive?.[device];
  if(!value||value.layout==null)continue;
  if(!viewportLayouts.has(value.layout))fail("unsupported responsive layout");
  const columns=value.columns??2;
  if(!Number.isInteger(columns)||columns<1||columns>12)fail("unsupported responsive column count");
  attrs.push(`data-${device}-layout="${q(value.layout)}" data-${device}-columns="${columns}"`);
 }
 return attrs.length?" "+attrs.join(" "):"";
}
// C's eight registered Editor.js tools are also the browser/static grammar.
// Keep markup and escaping aligned with NATIVE_EDITORIAL_TEMPLATES; unsupported
// blocks are refused rather than imported or silently discarded.
export function renderAuthoredEditorial(blocks){
 if(blocks==null)return "";
 if(!Array.isArray(blocks)||blocks.length>256)fail("invalid editorial blocks");
 const scalar=value=>value==null?"":typeof value==="string"||typeof value==="number"?
  q(value):fail("invalid editorial field");
 const itemText=item=>typeof item==="string"?scalar(item):
  item&&typeof item==="object"?scalar(item.text??item.content??""):fail("invalid editorial item");
 return blocks.map(block=>{
  if(!block||typeof block!=="object"||!block.data||
     typeof block.data!=="object"||Array.isArray(block.data))
   fail("invalid editorial block");
  const d=block.data;
  switch(block.type){
   case "paragraph":return '<p class="ak-editorial-paragraph">'+scalar(d.text)+"</p>";
   case "header":{
    const level=Number.isInteger(d.level)&&d.level>=2&&d.level<=6?d.level:2;
    return "<h"+level+">"+scalar(d.text)+"</h"+level+">";
   }
   case "quote":return '<blockquote class="ak-editorial-quote"><p>'+scalar(d.text)+
    "</p>"+(d.caption?"<cite>"+scalar(d.caption)+"</cite>":"")+"</blockquote>";
   case "code":return '<pre class="ak-editorial-code"><code>'+scalar(d.code)+"</code></pre>";
   case "delimiter":return '<hr class="ak-editorial-break">';
   case "list":{
    if(d.style!=null&&!["ordered","unordered"].includes(d.style))
     fail("unsupported editorial list style");
    const ordered=d.style==="ordered",tag=ordered?"ol":"ul";
    const rows=Array.isArray(d.items)?d.items:fail("invalid editorial list");
    return "<"+tag+' class="ak-editorial-list">'+
     rows.map(item=>"<li>"+itemText(item)+"</li>").join("")+"</"+tag+">";
   }
   case "checklist":{
    const rows=Array.isArray(d.items)?d.items:fail("invalid editorial checklist");
    return '<ul class="ak-editorial-checklist">'+rows.map(item=>
     '<li><input type="checkbox" disabled'+(item&&typeof item==="object"&&item.checked===true?" checked":"")+
     "> "+itemText(item)+"</li>").join("")+"</ul>";
   }
   case "table":{
    const rows=Array.isArray(d.content)?d.content:fail("invalid editorial table");
    return '<div class="ak-editorial-table"><table><tbody>'+
     rows.map(row=>Array.isArray(row)?"<tr>"+row.map(value=>"<td>"+scalar(value)+"</td>").join("")+"</tr>":
      fail("invalid editorial row")).join("")+"</tbody></table></div>";
   }
   default:fail("unknown editorial tool "+block.type);
  }
 }).join("");
}

export function renderAuthoredPattern(node,children=""){
 const spec=PATTERN_BY_KIND.get(node.kind);if(!spec)fail("unregistered pattern "+node.kind);
 const p=node.props.payload||{},kind=node.kind.slice(3),key="ak-"+node.id;
 const attrs='class="ak-pattern ak-'+kind+'" data-ak-id="'+q(node.id)+'"'+authoredResponsiveAttributes(node);
 const shell=body=>el("section",attrs,body);
 switch(kind){
  case "section":return shell(title({title:p.heading||node.props.heading||"Section"})+paragraph(p.lede)+children);
  case "container":case "columns":case "grid":case "stack":
   return el("div",attrs+' data-ak-columns="'+Math.min(12,Math.max(1,Number(p.columns)||2))+'"',children);
  case "hero":return shell(el("small",'class="ak-eyebrow"',q(p.eyebrow))+title(p)+paragraph(p.lede)+image(p.image)+actions(p.actions)+children);
  case "card":return shell(image(p.image)+title(p)+paragraph(p.body)+actions(p.actions)+children);
  case "alert":return shell(el("div",'class="ak-alert ak-tone-'+q(p.tone)+'" role="status"',
   title(p)+paragraph(p.body)+(p.dismissible?'<button type="button" data-ak-dismiss aria-label="Dismiss alert">×</button>':"")));
  case "badge":return el("span",attrs+' role="status"',q(p.label));
  case "buttons":case "button-group":return shell(actions(p.items));
  case "breadcrumb":return el("nav",attrs+' aria-label="Breadcrumb"',list(p.items,"ak-breadcrumbs"));
  case "navbar":return el("nav",attrs+' aria-label="Main navigation"',
   el("strong","",q(p.brand))+'<button type="button" data-ak-menu aria-expanded="false" aria-controls="'+key+'-links">Menu</button>'+
   el("div",'id="'+key+'-links" class="ak-nav-links"',list(p.links))+children);
  case "nav-tabs":return shell(el("div",'role="tablist" aria-label="Sections" class="ak-tabs"',
   ar(p.tabs).map((x,i)=>el("button",'type="button" role="tab" data-ak-tab="'+i+'" id="'+key+'-tab-'+i+'" aria-controls="'+key+'-panel-'+i+'" aria-selected="'+(!i)+'" tabindex="'+(i?"-1":"0")+'"',q(x.label))).join(""))+
   ar(p.tabs).map((x,i)=>el("section",'role="tabpanel" id="'+key+'-panel-'+i+'" aria-labelledby="'+key+'-tab-'+i+'" data-ak-panel="'+i+'"'+(i?" hidden":""),paragraph(x.body))).join(""));
  case "scrollspy":return shell(el("nav",'aria-label="Page sections"',list(p.links,"ak-scrollspy-links"))+children);
  case "pagination":return el("nav",attrs+' aria-label="Pagination"',list(p.pages));
  case "accordion":return shell(ar(p.items).map((x,i)=>el("details",'class="ak-disclosure"',el("summary","",q(x.title))+paragraph(x.body))).join(""));
  case "collapse":return shell(el("details",'class="ak-disclosure"',el("summary","",q(p.label))+paragraph(p.body)));
  case "dropdown":return shell(el("details",'class="ak-dropdown"',el("summary","",q(p.label))+list(p.items)));
  case "modal":case "offcanvas":return shell('<button type="button" class="ak-button" data-ak-dialog="'+key+'-dialog">'+q(p.label)+"</button>"+
   el("dialog",'id="'+key+'-dialog" class="ak-dialog '+(kind==="offcanvas"?"ak-side":"")+'" aria-labelledby="'+key+'-title"',
    el("header","",el("h2",'id="'+key+'-title"',q(p.title))+'<button type="button" data-ak-close aria-label="Close">×</button>')+paragraph(p.body)));
  case "toast":return shell(el("aside",'class="ak-toast-body" role="status"',title(p)+paragraph(p.body)+
   (p.dismissible?'<button type="button" data-ak-dismiss aria-label="Dismiss notification">×</button>':"")));
  case "popover":return shell('<button type="button" data-ak-popover aria-expanded="false" aria-controls="'+key+'-popover">'+q(p.label)+"</button>"+
   el("aside",'id="'+key+'-popover" class="ak-popover-body" hidden',title(p)+paragraph(p.body)));
  case "tooltip":return el("span",attrs+' tabindex="0" aria-describedby="'+key+'-tooltip"',q(p.label)+
   el("span",'id="'+key+'-tooltip" role="tooltip" class="ak-tooltip-body"',q(p.hint)));
  case "carousel":return shell(el("div",'class="ak-slides" aria-roledescription="carousel"',ar(p.slides).map((x,i)=>
   el("article",'data-ak-slide="'+i+'" aria-label="Slide '+(i+1)+' of '+p.slides.length+'"'+(i?" hidden":""),image(x)+el("h3","",q(x.title))+paragraph(x.body))).join(""))+
   el("nav",'aria-label="Carousel controls"','<button type="button" data-ak-prev>Previous</button><button type="button" data-ak-next>Next</button>'));
  case "progress":return shell(el("label","",q(p.label)+'<progress value="'+Math.max(0,Number(p.value)||0)+'" max="'+Math.max(1,Number(p.max)||100)+'"></progress>'));
  case "spinner":return shell(el("span",'role="status" class="ak-spinner" aria-label="'+q(p.label||"Loading")+'"',""));
  case "placeholder":return shell(el("div",'class="ak-placeholders" aria-hidden="true"',
   Array.from({length:Math.min(12,Math.max(1,Number(p.lines)||3))},()=>'<span></span>').join("")));
  case "list-group":return shell(list(p.items));
  case "table":return shell(el("div",'class="ak-table-wrap"',el("table","",
   el("thead","",el("tr","",ar(p.headers).map(x=>el("th",'scope="col"',q(x))).join("")))+
   el("tbody","",ar(p.rows).map(row=>el("tr","",ar(row).map(x=>el("td","",q(x))).join(""))).join("")))));
  case "form":return shell(title(p)+el("form",'data-ak-effect="review-required" aria-label="'+q(p.title)+'"',
   inputFields(p.fields,key)+children+'<button type="submit" disabled>Review a destination to enable submission</button>'));
  case "input":return shell(inputFields([p],key));
  case "checks-radios":return shell(el("fieldset","",el("legend","",q(p.legend))+
   ar(p.items).map(x=>el("label","",'<input type="'+(p.type==="checkbox"?"checkbox":"radio")+'" name="'+key+'-choice" value="'+q(x.value)+'"> '+q(x.label))).join("")));
  case "select":return shell(inputFields([{...p,type:"select"}],key));
  case "range":return shell(el("label","",q(p.label)+' <input type="range" data-ak-range min="'+Number(p.min||0)+'" max="'+Number(p.max||100)+'" value="'+Number(p.value||0)+'"><output>'+Number(p.value||0)+"</output>"));
  case "documentation":return shell(el("header","",title(p)+paragraph(p.lede))+
   el("nav",'aria-label="On this page"',list(p.toc))+el("article",'class="ak-docs-body"',children));
  default:fail("no renderer for "+kind);
 }
}
