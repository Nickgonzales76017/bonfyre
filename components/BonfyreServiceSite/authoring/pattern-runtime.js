// Shared, dependency-free interactions for Aurekai-authored pages.
// User documents cannot register executable handlers. No network or external API.
(function(){
"use strict";
function rootOf(node){return node.closest(".ak-pattern");}
document.addEventListener("click",function(event){
 const button=event.target.closest("button");
 if(!button)return;
 const host=rootOf(button);
 if(!host)return;
 if(button.hasAttribute("data-ak-dismiss")){
  (button.closest(".ak-alert")||button).hidden=true;return;
 }
 if(button.hasAttribute("data-ak-dialog")){
  const dialog=document.getElementById(button.getAttribute("data-ak-dialog"));
  if(dialog&&dialog.closest(".ak-pattern")===host&&typeof dialog.showModal==="function"){
   dialog.showModal();dialog.querySelector("[data-ak-close]")?.focus();
  }
  return;
 }
 if(button.hasAttribute("data-ak-close")){
  button.closest("dialog")?.close();return;
 }
 if(button.hasAttribute("data-ak-menu")){
  const list=host.querySelector(".ak-nav-links");
  if(list){
   const expanded=list.classList.toggle("ak-menu-open");
   button.setAttribute("aria-expanded",String(expanded));
  }
  return;
 }
 if(button.hasAttribute("data-ak-tab")){
  const index=Number(button.getAttribute("data-ak-tab"));
  if(!Number.isInteger(index))return;
  for(const tab of host.querySelectorAll('[role="tab"][data-ak-tab]')){
   const active=tab===button;
   tab.setAttribute("aria-selected",String(active));
   tab.tabIndex=active?0:-1;
  }
  for(const panel of host.querySelectorAll('[role="tabpanel"][data-ak-panel]'))
   panel.hidden=Number(panel.getAttribute("data-ak-panel"))!==index;
  return;
 }
 if(button.hasAttribute("data-ak-popover")){
  const id=button.getAttribute("aria-controls");
  const panel=document.getElementById(id);
  if(panel&&host.contains(panel)){
   panel.hidden=!panel.hidden;
   button.setAttribute("aria-expanded",String(!panel.hidden));
  }
  return;
 }
 if(button.hasAttribute("data-ak-next")||button.hasAttribute("data-ak-prev")){
  const slides=[...host.querySelectorAll("[data-ak-slide]")];
  if(!slides.length)return;
  const previous=Math.max(0,slides.findIndex(x=>!x.hidden));
  const delta=button.hasAttribute("data-ak-next")?1:-1;
  slides[(previous+delta+slides.length)%slides.length].hidden=false;
  slides[previous].hidden=true;
 }
});
document.addEventListener("keydown",function(e){
 const tab=e.target.closest?.('[role="tab"][data-ak-tab]');
 if(!tab||!["ArrowLeft","ArrowRight","Home","End"].includes(e.key))return;
 const host=rootOf(tab),tabs=host?[...host.querySelectorAll('[role="tab"][data-ak-tab]')]:[];
 if(!tabs.length)return;
 const idx=tabs.indexOf(tab);
 const next=e.key==="Home"?0:e.key==="End"?tabs.length-1:
 (idx+(e.key==="ArrowRight"?1:-1)+tabs.length)%tabs.length;
 e.preventDefault();tabs[next].click();tabs[next].focus();
});
document.addEventListener("input",function(e){
 if(!e.target.matches?.("[data-ak-range]"))return;
 const output=e.target.parentElement?.querySelector("output");
 if(output)output.value=e.target.value;
});
})();
