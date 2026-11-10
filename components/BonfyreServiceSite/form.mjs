// The request form. One per page, always at #ask. A form is either a flat list
// of fields or a few named steps; a field is typed text or a choice between
// stated options. What is typed is sent to the site's own /api/lead when the
// site keeps requests on a server, and otherwise (or if that fails) is handed
// to the visitor's mail app addressed to the business.
const input = "w-full bg-surface-container-lowest border border-surface-container-high focus:border-secondary focus:ring-1 focus:ring-secondary text-display placeholder:text-outline text-base sm:text-sm rounded-lg pr-4 py-3.5 transition-all outline-none";
const label = "block text-xs font-mono text-on-surface-variant mb-1 uppercase tracking-wider";

export const formFields = form => form.steps ? form.steps.flatMap(step => step.fields) : form.fields;

export function checkForm(form, fail) {
  for (const need of ["id", "title", "button", "sent", "subject"]) if (!form[need]) fail(`form.${need} is required`);
  if (!/^[a-z][a-z0-9-]*$/.test(form.id)) fail(`form.id "${form.id}" must be lower-case letters, digits and hyphens`);
  if (form.steps && form.fields) fail("a form has steps or fields, not both");
  const fields = formFields(form);
  if (!Array.isArray(fields) || !fields.length) fail("a form needs at least one field");
  const labels = new Set();
  for (const f of fields) {
    if (!f.label || labels.has(f.label)) fail(`form field label "${f.label}" is missing or repeated`);
    labels.add(f.label);
    if (!["text", "tel", "email", "number", "choice", "select", "textarea", undefined].includes(f.type)) fail(`form field "${f.label}" has type "${f.type}"`);
    if (["choice","select"].includes(f.type) && !(f.options?.length >= 2)) fail(`choice "${f.label}" needs at least two options`);
  }
  if (!fields.some(f => !f.optional && (f.type === "tel" || f.type === "email"))) fail("a form must ask for a phone or an email, or nobody can answer it");
}

export function renderForm(form, site, h) {
  let n = 0;
  const field = f => {
    const id = `ask-${n++}`;
    if (f.type === "choice") return `<fieldset class="${f.wide === false ? "" : "sm:col-span-2"}" data-label="${h.esc(f.label)}">
                <legend class="${label}">${h.esc(f.label)}</legend>
                <div class="grid grid-cols-1 ${f.options.length % 3 === 0 ? "sm:grid-cols-3" : "sm:grid-cols-2"} gap-2.5">
                  ${f.options.map((o, i) => { const opt = typeof o === "string" ? { label: o } : o; return `<label class="relative flex flex-col gap-0.5 cursor-pointer rounded-lg border border-surface-container-high bg-surface-container-lowest px-4 py-3 min-h-[48px] justify-center has-[:checked]:border-secondary has-[:checked]:bg-secondary/10 has-[:focus-visible]:ring-1 has-[:focus-visible]:ring-secondary transition-colors">
                    <input type="radio" name="${id}" value="${h.esc(opt.label)}"${f.optional || i ? "" : " required"} class="sr-only" />
                    <span class="text-sm font-semibold text-display">${h.esc(opt.label)}</span>
                    ${opt.text ? `<span class="text-xs text-on-surface-variant leading-snug">${h.esc(opt.text)}</span>` : ""}
                  </label>`; }).join("\n                  ")}
                </div>
              </fieldset>`;
    if(f.type==="select")return `<div class="${f.wide?"sm:col-span-2":""}"><label for="${id}" class="${label}">${h.esc(f.label)}${f.optional?" (optional)":""}</label><select id="${id}" data-label="${h.esc(f.label)}" class="${input} pl-4"${f.optional?"":" required"}><option value="">${h.esc(f.placeholder??"Choose an option")}</option>${f.options.map(o=>{const v=typeof o==="string"?o:o.label;return `<option value="${h.esc(v)}">${h.esc(v)}</option>`;}).join("")}</select></div>`;
    if(f.type==="textarea")return `<div class="sm:col-span-2"><label for="${id}" class="${label}">${h.esc(f.label)}${f.optional?" (optional)":""}</label><textarea id="${id}" data-label="${h.esc(f.label)}" rows="4" maxlength="300" placeholder="${h.esc(f.placeholder??"")}" class="${input} pl-4"${f.optional?"":" required"}></textarea></div>`;
    return `<div class="${f.wide ? "sm:col-span-2" : ""}">
                <label for="${id}" class="${label}">${h.esc(f.label)}${f.optional ? ' <span class="normal-case tracking-normal">(optional)</span>' : ""}</label>
                <div class="relative">
                  ${h.icon(f.icon ?? "edit", "absolute left-3.5 top-1/2 -translate-y-1/2 text-outline text-lg")}
                  <input id="${id}" data-label="${h.esc(f.label)}" type="${h.esc(f.type ?? "text")}"${f.type === "tel" ? ' inputmode="tel" autocomplete="tel"' : f.type === "email" ? ' autocomplete="email"' : f.type === "number" ? ' inputmode="numeric"' : ""}${f.optional ? "" : " required"} maxlength="300" placeholder="${h.esc(f.placeholder ?? "")}" class="${input} pl-11" />
                </div>
              </div>`;
  };
  const grid = fields => `<div class="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
              ${fields.map(field).join("\n              ")}
            </div>`;
  const body = form.steps
    ? form.steps.map((step, i) => `<fieldset data-step class="flex flex-col gap-3.5">
            <legend class="text-sm font-headline font-bold text-display mb-3 flex items-center justify-between w-full"><span>${i + 1}. ${h.esc(step.title)}</span><span class="text-xs font-mono font-medium text-on-surface-variant">Step ${i + 1} of ${form.steps.length}</span></legend>
            ${grid(step.fields)}
          </fieldset>`).join("\n          ")
    : grid(form.fields);
  return `<div id="ask" class="scroll-mt-24 bg-surface-container border border-surface-container-highest/80 rounded-2xl p-5 sm:p-7 shadow-2xl relative">
          <div class="flex flex-wrap items-center justify-between gap-2 pb-4 mb-4 border-b border-surface-container-high">
            <span class="text-xs font-mono uppercase tracking-wider text-display font-semibold flex items-center gap-1.5">
              ${h.icon("bolt", "text-secondary text-base")}
              ${h.esc(form.title)}
            </span>
            ${form.promise ? `<span class="text-xs font-mono text-secondary bg-surface-container-high px-2 py-0.5 rounded border border-surface-container-highest">${h.esc(form.promise)}</span>` : ""}
          </div>
          <form class="ask-form flex flex-col gap-3.5" data-form="${h.esc(form.id)}" data-subject="${h.esc(form.subject)}" data-closing="${h.esc(form.closing ?? "")}" action="/api/lead" method="post">
          ${body}
          ${form.notice ? `<p class="text-xs leading-relaxed text-on-surface-variant">${h.esc(form.notice)}${form.privacy_href?` <a class="underline" href="${h.esc(h.localHref(form.privacy_href))}">Data and privacy</a>`:""}</p>` : ""}
            <div class="hidden" aria-hidden="true"><label>Leave this empty <input type="text" name="company_website" tabindex="-1" autocomplete="off" /></label></div>
            <div class="flex gap-3 mt-2">
              <button type="button" data-back class="hidden shrink-0 bg-surface-container-high hover:bg-surface-bright border border-surface-container-highest text-display font-headline font-bold text-sm uppercase tracking-wider px-5 py-4 rounded-lg transition-colors">Back</button>
              <button type="button" data-next class="hidden w-full bg-secondary hover:bg-secondary-dark text-surface font-headline font-bold text-sm uppercase tracking-wider py-4 rounded-lg shadow-xl shadow-secondary/20 transition-all items-center justify-center gap-2"><span>Next</span>${h.icon("arrow_forward", "text-lg leading-none")}</button>
              <button type="submit" class="w-full bg-secondary hover:bg-secondary-dark text-surface font-headline font-bold text-sm uppercase tracking-wider py-4 px-3 rounded-lg shadow-xl shadow-secondary/20 hover:shadow-secondary/30 transition-all flex items-center justify-center gap-2 group disabled:opacity-60">
                ${h.icon("lock", "text-lg leading-none")}
                <span>${h.esc(form.button)}</span>
                ${h.icon("arrow_forward", "text-lg leading-none group-hover:translate-x-1 transition-transform")}
              </button>
            </div>
          </form>
          <div class="ask-sent hidden mt-4 p-4 rounded-lg bg-secondary/15 border border-secondary/30 text-secondary text-sm font-medium flex items-center gap-3" role="status" tabindex="-1">
            ${h.icon("check_circle", "text-xl shrink-0")}
            <span>${h.esc(form.sent)}</span>
          </div>
          <div class="ask-mailed hidden mt-4 p-4 rounded-lg bg-secondary/15 border border-secondary/30 text-secondary text-sm font-medium flex items-center gap-3" role="status" tabindex="-1">
            ${h.icon("mail", "text-xl shrink-0")}
            <span>Your mail app has opened with your details. Press send and it comes straight to us.</span>
          </div>
          <div class="ask-failed hidden mt-4 p-4 rounded-lg bg-surface-container-high border border-surface-container-highest text-display text-sm font-medium flex items-center gap-3" role="alert" tabindex="-1">
            ${h.icon("error", "text-xl shrink-0")}
            <span>That could not be sent just now. Nothing was lost on your side; please try again in a little while.</span>
          </div>
          ${(form.checks ?? []).length ? `<div class="grid grid-cols-2 sm:grid-cols-4 gap-2.5 mt-5 pt-4 border-t border-surface-container-high">
            ${form.checks.map(c => `<div class="flex items-center gap-1.5 text-xs text-on-surface-variant">
              ${h.icon("check_circle", "text-secondary text-sm")}
              <span>${h.esc(c)}</span>
            </div>`).join("\n            ")}
          </div>` : ""}
        </div>`;
}

// Runs in the visitor's browser. Kept as a function so the build can print it.
export function formBehaviour(email, server) {
  document.querySelectorAll("form.ask-form").forEach(function (form) {
    form.querySelectorAll("input[required],textarea[required],select[required]").forEach(function(field){if(field.type!=="radio")field.addEventListener("input",function(){field.setCustomValidity(field.value.trim()?"":"Please complete this field.");});});
    var box = form.parentNode, steps = [].slice.call(form.querySelectorAll("fieldset[data-step]")), at = 0;
    var next = form.querySelector("[data-next]"), back = form.querySelector("[data-back]"), send = form.querySelector("[type=submit]");
    var complete = function (step) {
      var fields = [].slice.call(step.querySelectorAll("input, select, textarea"));
      for (var field of fields) if(field.required && field.type!=="radio")field.setCustomValidity(field.value.trim()?"":"Please complete this field.");
      for (var i = 0; i < fields.length; i++) if (!fields[i].checkValidity()) { fields[i].reportValidity(); if (fields[i].type === "radio") fields[i].closest("fieldset").scrollIntoView({ block: "center" }); return false; }
      return true;
    };
    var show = function () {
      steps.forEach(function (step, i) { step.classList.toggle("hidden", i !== at); });
      var last = at === steps.length - 1;
      back.classList.toggle("hidden", at === 0);
      next.classList.toggle("hidden", last); next.classList.toggle("flex", !last);
      send.classList.toggle("hidden", !last); send.classList.toggle("flex", last);
    };
    if (steps.length > 1) {
      show();
      next.addEventListener("click", function () { if (complete(steps[at])) { at++; show(); steps[at].querySelector("input").focus({ preventScroll: true }); } });
      back.addEventListener("click", function () { at--; show(); });
    }
    var done = function (kind) { form.classList.add("hidden"); var note = box.querySelector(".ask-" + kind); note.classList.remove("hidden"); note.focus(); };
    form.addEventListener("submit", function (event) {
      event.preventDefault();
      if(!complete(form))return;
      var fields = [], first = "";
      form.querySelectorAll("input[data-label], select[data-label], textarea[data-label], fieldset[data-label]").forEach(function (field) {
        var picked = field.tagName === "FIELDSET" ? field.querySelector("input:checked") : field, value = picked ? picked.value.trim() : "";
        if (!first) first = value;
        if (value) fields.push({ label: field.dataset.label, value: value });
      });
      var mail = function () {
        var lines = fields.map(function (f) { return f.label + ": " + f.value; });
        if (form.dataset.closing) lines.push("", form.dataset.closing);
        var link = document.createElement("a");
        link.href = "mailto:" + email + "?subject=" + encodeURIComponent(form.dataset.subject + (first ? ": " + first : "")) + "&body=" + encodeURIComponent(lines.join("\n"));
        link.hidden = true; document.body.appendChild(link); link.click(); link.remove();
        done("mailed");
      };
      if (!server) return mail();
      send.disabled = true; box.querySelector(".ask-failed").classList.add("hidden");
      fetch(form.action, { method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ form: form.dataset.form, page: location.pathname, company_website: form.elements.company_website.value, fields: fields }) })
        .then(function (answer) { if (answer.status !== 201) throw new Error("not kept"); done("sent"); })
        .catch(function () { send.disabled = false; if (email) return mail(); var note = box.querySelector(".ask-failed"); note.classList.remove("hidden"); note.focus(); });
    });
  });
}
