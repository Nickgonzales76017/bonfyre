(function(){"use strict";
var search=document.getElementById("discover-search"),grid=document.getElementById("discover-grid");
var buttons=Array.prototype.slice.call(document.querySelectorAll("[data-kind-filter]"));
var count=document.getElementById("discover-count"),empty=document.getElementById("discover-empty"),note=document.getElementById("discover-note");var kind="all",articles=0;
if(!search||!grid)return;
function apply(){var q=search.value.toLowerCase().trim(),shown=0;
Array.prototype.forEach.call(grid.querySelectorAll("[data-item]"),function(c){var yes=(kind==="all"||c.dataset.kind===kind)&&(!q||c.dataset.search.indexOf(q)!==-1);c.hidden=!yes;if(yes)shown++;});
var total=grid.querySelectorAll("[data-item]").length;
count.textContent="Showing "+shown+" of "+total+" entries"+(articles?" ("+articles+" journal articles included)":"");empty.hidden=shown>0;}
buttons.forEach(function(b){b.addEventListener("click",function(){kind=b.dataset.kindFilter;buttons.forEach(function(k){var on=k===b;k.classList.toggle("active",on);k.setAttribute("aria-pressed",String(on));});apply();});});
search.addEventListener("input",apply);
fetch("./article-index.tsv",{credentials:"same-origin"}).then(function(r){if(!r.ok)throw Error("index unavailable");return r.text();}).then(function(data){
var lines=data.trim().split(/\r?\n/),cols=lines.shift().split("\t");if(!["slug","category","title","description","topics"].every(function(x){return cols.indexOf(x)!==-1;}))throw Error("unexpected format");
var frag=document.createDocumentFragment();
lines.forEach(function(line){var parts=line.split("\t"),row={};cols.forEach(function(h,i){row[h]=parts[i]||"";});if(!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(row.slug))return;
var c=document.createElement("article");c.className="discovery-card";c.dataset.item="";c.dataset.kind="article";c.dataset.search=[row.title,row.description,row.category,row.topics].join(" ").toLowerCase();
var icon=document.createElement("span");icon.className="glyph";icon.setAttribute("aria-hidden","true");icon.innerHTML='<svg viewBox="0 0 24 24"><use href="../assets/product-icons.svg#file"></use></svg>';c.appendChild(icon);
var type=document.createElement("small");type.textContent=row.category+" / Journal";c.appendChild(type);
var h=document.createElement("h3");h.textContent=row.title;c.appendChild(h);
var p=document.createElement("p");p.textContent=row.description;c.appendChild(p);
var act=document.createElement("div");act.className="card-links";var a=document.createElement("a");a.href="../blog/"+encodeURIComponent(row.slug)+"/";a.textContent="Read article ↗";act.appendChild(a);c.appendChild(act);frag.appendChild(c);articles++;});
grid.appendChild(frag);apply();
}).catch(function(){note.textContent="The journal search index could not be loaded. ";var a=document.createElement("a");a.href="../blog/";a.textContent="Open the full journal ↗";note.appendChild(a);apply();});
apply();})();