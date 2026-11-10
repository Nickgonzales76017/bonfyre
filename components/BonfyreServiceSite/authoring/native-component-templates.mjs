// GENERATED SOURCE SPEC: checked against visual editor patterns before native header generation.
import {AUTHORING_PATTERNS} from "./presentation-patterns.mjs";
import {AUTHORING_BLOCKS} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-prototype.mjs";
export const NATIVE_AUTHORING_TEMPLATES=Object.freeze([
  {
    "kind": "ui:section",
    "start": "<section class=\"ak-pattern ak-section\" data-ak-id=\"{{id}}\"><h2>{{payload.heading}}</h2><p>{{payload.lede}}</p>",
    "end": "</section>"
  },
  {
    "kind": "ui:container",
    "start": "<div class=\"ak-pattern ak-container\" data-ak-id=\"{{id}}\" data-ak-columns=\"{{payload.columns}}\">",
    "end": "</div>"
  },
  {
    "kind": "ui:columns",
    "start": "<div class=\"ak-pattern ak-columns\" data-ak-id=\"{{id}}\" data-ak-columns=\"{{payload.columns}}\">",
    "end": "</div>"
  },
  {
    "kind": "ui:grid",
    "start": "<div class=\"ak-pattern ak-grid\" data-ak-id=\"{{id}}\" data-ak-columns=\"{{payload.columns}}\">",
    "end": "</div>"
  },
  {
    "kind": "ui:stack",
    "start": "<div class=\"ak-pattern ak-stack\" data-ak-id=\"{{id}}\" data-ak-columns=\"{{payload.columns}}\">",
    "end": "</div>"
  },
  {
    "kind": "ui:hero",
    "start": "<section class=\"ak-pattern ak-hero\" data-ak-id=\"{{id}}\"><small class=\"ak-eyebrow\">{{payload.eyebrow}}</small><h2>{{payload.title}}</h2><p>{{payload.lede}}</p>{{#payload.image.src}}<img loading=\"lazy\" src=\"{{payload.image.src}}\" alt=\"{{payload.image.alt}}\">{{/payload.image.src}}<div class=\"ak-actions\">{{#payload.actions}}<a class=\"ak-button\" href=\"{{href}}\">{{label}}</a>{{/payload.actions}}</div>",
    "end": "</section>"
  },
  {
    "kind": "ui:card",
    "start": "<section class=\"ak-pattern ak-card\" data-ak-id=\"{{id}}\">{{#payload.image.src}}<img loading=\"lazy\" src=\"{{payload.image.src}}\" alt=\"{{payload.image.alt}}\">{{/payload.image.src}}<h2>{{payload.title}}</h2><p>{{payload.body}}</p><div class=\"ak-actions\">{{#payload.actions}}<a class=\"ak-button\" href=\"{{href}}\">{{label}}</a>{{/payload.actions}}</div>",
    "end": "</section>"
  },
  {
    "kind": "ui:alert",
    "start": "<section class=\"ak-pattern ak-alert\" data-ak-id=\"{{id}}\"><div class=\"ak-alert\" role=\"status\"><h2>{{payload.title}}</h2><p>{{payload.body}}</p>{{#payload.dismissible}}<button type=\"button\" data-ak-dismiss aria-label=\"Dismiss alert\">×</button>{{/payload.dismissible}}</div>",
    "end": "</section>"
  },
  {
    "kind": "ui:badge",
    "start": "<span class=\"ak-pattern ak-badge\" data-ak-id=\"{{id}}\" role=\"status\">{{payload.label}}",
    "end": "</span>"
  },
  {
    "kind": "ui:buttons",
    "start": "<section class=\"ak-pattern ak-buttons\" data-ak-id=\"{{id}}\"><div class=\"ak-actions\">{{#payload.items}}<a class=\"ak-button\" href=\"{{href}}\">{{label}}</a>{{/payload.items}}</div>",
    "end": "</section>"
  },
  {
    "kind": "ui:button-group",
    "start": "<section class=\"ak-pattern ak-button-group\" data-ak-id=\"{{id}}\"><div class=\"ak-actions\">{{#payload.items}}<a class=\"ak-button\" href=\"{{href}}\">{{label}}</a>{{/payload.items}}</div>",
    "end": "</section>"
  },
  {
    "kind": "ui:breadcrumb",
    "start": "<nav class=\"ak-pattern ak-breadcrumb\" data-ak-id=\"{{id}}\" aria-label=\"Breadcrumb\"><ul class=\"ak-breadcrumbs\">{{#payload.items}}<li>{{#href}}<a href=\"{{href}}\">{{label}}</a>{{/href}}{{^href}}<span>{{label}}</span>{{/href}}</li>{{/payload.items}}</ul>",
    "end": "</nav>"
  },
  {
    "kind": "ui:navbar",
    "start": "<nav class=\"ak-pattern ak-navbar\" data-ak-id=\"{{id}}\" aria-label=\"Main navigation\"><strong>{{payload.brand}}</strong><button type=\"button\" data-ak-menu aria-expanded=\"false\" aria-controls=\"ak-{{id}}-links\">Menu</button><div class=\"ak-nav-links\" id=\"ak-{{id}}-links\"><ul class=\"ak-links\">{{#payload.links}}<li><a href=\"{{href}}\">{{label}}</a></li>{{/payload.links}}</ul></div>",
    "end": "</nav>"
  },
  {
    "kind": "ui:nav-tabs",
    "start": "<section class=\"ak-pattern ak-nav-tabs\" data-ak-id=\"{{id}}\"><div role=\"tablist\" class=\"ak-tabs\" aria-label=\"Sections\">{{#payload.tabs}}<button type=\"button\" role=\"tab\" data-ak-tab=\"{{ak_index}}\" id=\"ak-{{id}}-tab-{{ak_index}}\" aria-controls=\"ak-{{id}}-panel-{{ak_index}}\" aria-selected=\"{{ak_first}}\" tabindex=\"{{ak_tabindex}}\">{{label}}</button>{{/payload.tabs}}</div>{{#payload.tabs}}<section role=\"tabpanel\" id=\"ak-{{id}}-panel-{{ak_index}}\" aria-labelledby=\"ak-{{id}}-tab-{{ak_index}}\" data-ak-panel=\"{{ak_index}}\"{{^ak_first}} hidden{{/ak_first}}><p>{{body}}</p></section>{{/payload.tabs}}",
    "end": "</section>"
  },
  {
    "kind": "ui:scrollspy",
    "start": "<section class=\"ak-pattern ak-scrollspy\" data-ak-id=\"{{id}}\"><nav aria-label=\"Page sections\"><ul class=\"ak-scrollspy-links\">{{#payload.links}}<li><a href=\"{{href}}\">{{label}}</a></li>{{/payload.links}}</ul></nav>",
    "end": "</section>"
  },
  {
    "kind": "ui:pagination",
    "start": "<nav class=\"ak-pattern ak-pagination\" data-ak-id=\"{{id}}\" aria-label=\"Pagination\"><ul class=\"ak-links\">{{#payload.pages}}<li><a href=\"{{href}}\">{{label}}</a></li>{{/payload.pages}}</ul>",
    "end": "</nav>"
  },
  {
    "kind": "ui:accordion",
    "start": "<section class=\"ak-pattern ak-accordion\" data-ak-id=\"{{id}}\">{{#payload.items}}<details class=\"ak-disclosure\"><summary>{{title}}</summary><p>{{body}}</p></details>{{/payload.items}}",
    "end": "</section>"
  },
  {
    "kind": "ui:collapse",
    "start": "<section class=\"ak-pattern ak-collapse\" data-ak-id=\"{{id}}\"><details class=\"ak-disclosure\"><summary>{{payload.label}}</summary><p>{{payload.body}}</p></details>",
    "end": "</section>"
  },
  {
    "kind": "ui:dropdown",
    "start": "<section class=\"ak-pattern ak-dropdown\" data-ak-id=\"{{id}}\"><details class=\"ak-dropdown\"><summary>{{payload.label}}</summary><ul class=\"ak-links\">{{#payload.items}}<li>{{#href}}<a href=\"{{href}}\">{{label}}</a>{{/href}}{{^href}}<span>{{label}}</span>{{/href}}</li>{{/payload.items}}</ul></details>",
    "end": "</section>"
  },
  {
    "kind": "ui:modal",
    "start": "<section class=\"ak-pattern ak-modal\" data-ak-id=\"{{id}}\"><button type=\"button\" class=\"ak-button\" data-ak-dialog=\"ak-{{id}}-dialog\">{{payload.label}}</button><dialog id=\"ak-{{id}}-dialog\" class=\"ak-dialog\" aria-labelledby=\"ak-{{id}}-title\"><header><h2 id=\"ak-{{id}}-title\">{{payload.title}}</h2><button type=\"button\" data-ak-close aria-label=\"Close\">×</button></header><p>{{payload.body}}</p></dialog>",
    "end": "</section>"
  },
  {
    "kind": "ui:offcanvas",
    "start": "<section class=\"ak-pattern ak-offcanvas\" data-ak-id=\"{{id}}\"><button type=\"button\" class=\"ak-button\" data-ak-dialog=\"ak-{{id}}-dialog\">{{payload.label}}</button><dialog id=\"ak-{{id}}-dialog\" class=\"ak-dialog ak-side\" aria-labelledby=\"ak-{{id}}-title\"><header><h2 id=\"ak-{{id}}-title\">{{payload.title}}</h2><button type=\"button\" data-ak-close aria-label=\"Close\">×</button></header><p>{{payload.body}}</p></dialog>",
    "end": "</section>"
  },
  {
    "kind": "ui:toast",
    "start": "<section class=\"ak-pattern ak-toast\" data-ak-id=\"{{id}}\"><aside class=\"ak-toast-body\" role=\"status\"><h2>{{payload.title}}</h2><p>{{payload.body}}</p>{{#payload.dismissible}}<button type=\"button\" data-ak-dismiss aria-label=\"Dismiss notification\">×</button>{{/payload.dismissible}}</aside>",
    "end": "</section>"
  },
  {
    "kind": "ui:popover",
    "start": "<section class=\"ak-pattern ak-popover\" data-ak-id=\"{{id}}\"><button type=\"button\" data-ak-popover aria-expanded=\"false\" aria-controls=\"ak-{{id}}-popover\">{{payload.label}}</button><aside id=\"ak-{{id}}-popover\" class=\"ak-popover-body\" hidden><h2>{{payload.title}}</h2><p>{{payload.body}}</p></aside>",
    "end": "</section>"
  },
  {
    "kind": "ui:tooltip",
    "start": "<span class=\"ak-pattern ak-tooltip\" data-ak-id=\"{{id}}\" tabindex=\"0\" aria-describedby=\"ak-{{id}}-tooltip\"><span id=\"ak-{{id}}-tooltip\" role=\"tooltip\" class=\"ak-tooltip-body\">{{payload.hint}}</span>",
    "end": "</span>"
  },
  {
    "kind": "ui:carousel",
    "start": "<section class=\"ak-pattern ak-carousel\" data-ak-id=\"{{id}}\"><div class=\"ak-slides\" aria-roledescription=\"carousel\">{{#payload.slides}}<article data-ak-slide=\"{{ak_index}}\" aria-label=\"Slide {{ak_number}} of {{ak_total}}\"{{^ak_first}} hidden{{/ak_first}}>{{#src}}<img loading=\"lazy\" src=\"{{src}}\" alt=\"{{alt}}\">{{/src}}<h3>{{title}}</h3><p>{{body}}</p></article>{{/payload.slides}}</div><nav aria-label=\"Carousel controls\"><button type=\"button\" data-ak-prev>Previous</button><button type=\"button\" data-ak-next>Next</button></nav>",
    "end": "</section>"
  },
  {
    "kind": "ui:progress",
    "start": "<section class=\"ak-pattern ak-progress\" data-ak-id=\"{{id}}\"><label>{{payload.label}} <progress max=\"{{payload.max}}\" value=\"{{payload.value}}\"></progress></label>",
    "end": "</section>"
  },
  {
    "kind": "ui:spinner",
    "start": "<section class=\"ak-pattern ak-spinner\" data-ak-id=\"{{id}}\"><span class=\"ak-spinner\" role=\"status\" aria-label=\"{{payload.label}}\"></span>",
    "end": "</section>"
  },
  {
    "kind": "ui:placeholder",
    "start": "<section class=\"ak-pattern ak-placeholder\" data-ak-id=\"{{id}}\"><div class=\"ak-placeholders\" aria-hidden=\"true\"><span></span><span></span><span></span></div>",
    "end": "</section>"
  },
  {
    "kind": "ui:list-group",
    "start": "<section class=\"ak-pattern ak-list-group\" data-ak-id=\"{{id}}\"><ul class=\"ak-links\">{{#payload.items}}<li>{{#href}}<a href=\"{{href}}\">{{label}}</a>{{/href}}{{^href}}<span>{{label}}</span>{{/href}}</li>{{/payload.items}}</ul>",
    "end": "</section>"
  },
  {
    "kind": "ui:table",
    "start": "<section class=\"ak-pattern ak-table\" data-ak-id=\"{{id}}\"><div class=\"ak-table-wrap\"><table><thead><tr>{{#payload.headers}}<th scope=\"col\">{{.}}</th>{{/payload.headers}}</tr></thead><tbody>{{#payload.rows}}<tr>{{#.}}<td>{{.}}</td>{{/.}}</tr>{{/payload.rows}}</tbody></table></div>",
    "end": "</section>"
  },
  {
    "kind": "ui:form",
    "start": "<section class=\"ak-pattern ak-form\" data-ak-id=\"{{id}}\"><h2>{{payload.title}}</h2><form data-ak-effect=\"review-required\" aria-label=\"{{payload.title}}\">{{#payload.fields}}<label class=\"ak-field\"><span>{{label}}</span>{{#ak_textarea}}<textarea name=\"{{name}}\"{{#required}} required{{/required}}></textarea>{{/ak_textarea}}{{#ak_select}}<select name=\"{{name}}\">{{#options}}<option value=\"{{value}}\">{{label}}</option>{{/options}}</select>{{/ak_select}}{{#ak_regular}}<input name=\"{{name}}\" type=\"{{type}}\"{{#required}} required{{/required}}>{{/ak_regular}}</label>{{/payload.fields}}<button type=\"submit\" disabled>Review a destination to enable submission</button></form>",
    "end": "</section>"
  },
  {
    "kind": "ui:input",
    "start": "<section class=\"ak-pattern ak-input\" data-ak-id=\"{{id}}\"><label class=\"ak-field\"><span>{{payload.label}}</span><input name=\"{{payload.name}}\" type=\"{{payload.type}}\"{{#payload.required}} required{{/payload.required}}></label>",
    "end": "</section>"
  },
  {
    "kind": "ui:checks-radios",
    "start": "<section class=\"ak-pattern ak-checks-radios\" data-ak-id=\"{{id}}\"><fieldset><legend>{{payload.legend}}</legend>{{#payload.items}}<label><input type=\"{{payload.type}}\" name=\"ak-{{id}}-choice\" value=\"{{value}}\"> {{label}}</label>{{/payload.items}}</fieldset>",
    "end": "</section>"
  },
  {
    "kind": "ui:select",
    "start": "<section class=\"ak-pattern ak-select\" data-ak-id=\"{{id}}\"><label class=\"ak-field\"><span>{{payload.label}}</span><select name=\"{{payload.name}}\">{{#payload.options}}<option value=\"{{value}}\">{{label}}</option>{{/payload.options}}</select></label>",
    "end": "</section>"
  },
  {
    "kind": "ui:range",
    "start": "<section class=\"ak-pattern ak-range\" data-ak-id=\"{{id}}\"><label>{{payload.label}} <input type=\"range\" data-ak-range min=\"{{payload.min}}\" max=\"{{payload.max}}\" value=\"{{payload.value}}\"><output>{{payload.value}}</output></label>",
    "end": "</section>"
  },
  {
    "kind": "ui:documentation",
    "start": "<section class=\"ak-pattern ak-documentation\" data-ak-id=\"{{id}}\"><header><h2>{{payload.title}}</h2><p>{{payload.lede}}</p></header><nav aria-label=\"On this page\"><ul class=\"ak-links\">{{#payload.toc}}<li><a href=\"{{href}}\">{{label}}</a></li>{{/payload.toc}}</ul></nav><article class=\"ak-docs-body\"",
    "end": "</article></section>"
  },
  {
    "kind": "profile-hero",
    "start": "<section class=\"authored-profile-hero\" data-ak-id=\"{{id}}\"><h2>{{heading}}</h2><p>{{body}}</p></section>",
    "end": ""
  },
  {
    "kind": "profile-section",
    "start": "<section class=\"authored-profile-section\" data-ak-id=\"{{id}}\"><h2>{{heading}}</h2>{{#body}}<p>{{body}}</p>{{/body}}",
    "end": "</section>"
  },
  {
    "kind": "profile-card",
    "start": "<section class=\"authored-profile-card\" data-ak-id=\"{{id}}\"><h2>{{heading}}</h2><p>{{body}}</p></section>",
    "end": ""
  },
  {
    "kind": "profile-story",
    "start": "<section class=\"authored-profile-story\" data-ak-id=\"{{id}}\"><h2>{{heading}}</h2>{{#body}}<p>{{body}}</p>{{/body}}",
    "end": "</section>"
  },
  {
    "kind": "profile-quote",
    "start": "<section class=\"authored-profile-quote\" data-ak-id=\"{{id}}\"><h2>{{heading}}</h2><p>{{body}}</p></section>",
    "end": ""
  },
  {
    "kind": "profile-metric",
    "start": "<section class=\"authored-profile-metric\" data-ak-id=\"{{id}}\"><h2>{{heading}}</h2><p>{{body}}</p></section>",
    "end": ""
  },
  {
    "kind": "profile-action",
    "start": "<section class=\"authored-profile-action\" data-ak-id=\"{{id}}\"><h2>{{heading}}</h2><p>{{body}}</p></section>",
    "end": ""
  }
]);
const names=NATIVE_AUTHORING_TEMPLATES.map(x=>x.kind);
if(new Set(names).size!==names.length)throw Error("duplicate native component template");
for(const item of [...AUTHORING_PATTERNS.map(x=>x.kind),...AUTHORING_BLOCKS.map(x=>x.kind)])if(!names.includes(item))throw Error("native component template missing "+item);
// Exact Editor.js tool grammar compiled beside the 43 visual components.
// Markup is first-party; all user text remains HTML-escaped by native Mustach.
export const NATIVE_EDITORIAL_TEMPLATES=Object.freeze([
  {
    "kind": "paragraph",
    "markup": "<p class=\"ak-editorial-paragraph\">{{data.text}}</p>"
  },
  {
    "kind": "header",
    "markup": "{{#ak_h2}}<h2>{{data.text}}</h2>{{/ak_h2}}{{#ak_h3}}<h3>{{data.text}}</h3>{{/ak_h3}}{{#ak_h4}}<h4>{{data.text}}</h4>{{/ak_h4}}{{#ak_h5}}<h5>{{data.text}}</h5>{{/ak_h5}}{{#ak_h6}}<h6>{{data.text}}</h6>{{/ak_h6}}"
  },
  {
    "kind": "quote",
    "markup": "<blockquote class=\"ak-editorial-quote\"><p>{{data.text}}</p>{{#data.caption}}<cite>{{data.caption}}</cite>{{/data.caption}}</blockquote>"
  },
  {
    "kind": "code",
    "markup": "<pre class=\"ak-editorial-code\"><code>{{data.code}}</code></pre>"
  },
  {
    "kind": "delimiter",
    "markup": "<hr class=\"ak-editorial-break\">"
  },
  {
    "kind": "list",
    "markup": "{{#ak_ordered}}<ol class=\"ak-editorial-list\">{{#data.items}}<li>{{.}}{{text}}{{content}}</li>{{/data.items}}</ol>{{/ak_ordered}}{{^ak_ordered}}<ul class=\"ak-editorial-list\">{{#data.items}}<li>{{.}}{{text}}{{content}}</li>{{/data.items}}</ul>{{/ak_ordered}}"
  },
  {
    "kind": "checklist",
    "markup": "<ul class=\"ak-editorial-checklist\">{{#data.items}}<li><input type=\"checkbox\" disabled{{#checked}} checked{{/checked}}> {{.}}{{text}}{{content}}</li>{{/data.items}}</ul>"
  },
  {
    "kind": "table",
    "markup": "<div class=\"ak-editorial-table\"><table><tbody>{{#data.content}}<tr>{{#.}}<td>{{.}}</td>{{/.}}</tr>{{/data.content}}</tbody></table></div>"
  }
]);
