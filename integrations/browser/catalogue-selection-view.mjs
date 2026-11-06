// Mount inside a ServiceSite-owned region. Reuses its CSS and injected browser store; sends nothing.
import { makeSelection, validateSelection, selectionKey, selectionBrief, selectionSummary } from './catalogue-selection.mjs';

export async function mountCatalogueSelection(root, config, storage) {
  if (!root?.ownerDocument || typeof storage?.saveLocal !== 'function' || typeof storage?.readLocal !== 'function')
    throw Error('Selection needs a page region and the existing browser store.');
  const doc = root.ownerDocument, events = new AbortController();
  const { catalogue, store, project_ref, title = 'Your selection' } = config;
  const products = catalogue.rows.filter(product => product.store === store);
  if (!products.length) throw Error('No retained products for this store.');
  const key = selectionKey(project_ref, store);
  let choices = structuredClone(config.items ?? []), revision = 0, draft = null, busy = true, destroyed = false;
  const make = (tag, className = '', content = '') => {
    const element = doc.createElement(tag); element.className = className; element.textContent = content; return element;
  };
  const field = (label, input) => { const node = make('label'); node.append(make('span', '', label), input); return node; };
  const button = (label, id, action) => {
    const node = make('button', 'cw-button', label); node.type = 'button'; node.dataset.selection = id;
    node.addEventListener('click', action, { signal: events.signal }); return node;
  };
  const shell = make('section', 'catalogue-workspace cw-selection'), header = make('header', 'cw-top');
  const style = make('style');
  style.textContent = `.cw-selection .cw-browser{position:static;height:auto;width:auto;padding:24px;min-width:0}
    .cw-selection .cw-detail{display:block;padding-bottom:32px}.cw-selection .cw-fields textarea{min-height:6em}
    .cw-selection .cw-fields select{width:100%;min-width:0;min-height:44px;margin-top:7px;font:inherit}
    .cw-selection .cw-actions-buttons{flex-wrap:wrap}.cw-selection .cw-actions-buttons .cw-button{flex:1 1 9rem}
    .cw-selection h3,.cw-selection li{overflow-wrap:anywhere}.cw-selection .cw-top{height:auto;min-height:76px}
    @media(max-width:720px){.cw-selection .cw-layout{display:flex;flex-direction:column}.cw-selection .cw-detail{order:-1}}`;
  shell.append(style);

  header.append(make('h2', 'cw-brand', title), make('span', 'cw-preview', 'Enquiry draft'));
  const layout = make('div', 'cw-layout'), inputs = make('aside', 'cw-browser'), detail = make('div', 'cw-detail');
  const product = make('select'), option = make('select'), quantity = make('input'), note = make('input');
  product.dataset.selection = 'product'; option.dataset.selection = 'option'; quantity.dataset.selection = 'quantity'; note.dataset.selection = 'note';
  quantity.type = 'number'; quantity.min = '1'; quantity.max = '10000'; quantity.step = '1'; quantity.value = '1'; note.maxLength = 1000;
  for (const item of products) { const entry = make('option', '', item.title); entry.value = item.id; product.append(entry); }
  const showOptions = () => {
    option.replaceChildren();
    for (const item of products.find(item => item.id === product.value).variants) {
      const entry = make('option', '', item.label); entry.value = item.id; option.append(entry);
    }
  };
  showOptions(); product.addEventListener('change', showOptions, { signal: events.signal });
  const purpose = make('textarea'), requirements = make('textarea');
  purpose.dataset.selection = 'purpose'; requirements.dataset.selection = 'requirements';
  purpose.maxLength = 4000; requirements.maxLength = 24000; purpose.rows = 3; requirements.rows = 4;
  purpose.value = config.purpose ?? ''; requirements.value = (config.requirements ?? []).join('\n');
  const rows = make('ol', 'cw-fields'), totals = make('p'), state = make('p', 'cw-message');
  rows.dataset.selection = 'items'; totals.dataset.selection = 'totals'; state.dataset.selection = 'status'; state.setAttribute('role', 'status');
  const source = make('details'), brief = make('pre'); brief.style.whiteSpace = 'pre-wrap'; brief.style.overflowWrap = 'anywhere';
  brief.dataset.selection = 'brief'; source.append(make('summary', '', 'Review the enquiry and source links'), brief);
  function buttons() {
    for (const control of shell.querySelectorAll('button, input, select, textarea')) control.disabled = busy;
    save.disabled = busy || !draft; download.disabled = busy || !draft;
  }
  function draw() {
    rows.replaceChildren();
    try {
      if (!choices.length) { draft = null; totals.textContent = 'Choose the items you want to discuss.'; brief.textContent = ''; buttons(); return; }
      draft = makeSelection({ project_ref, store, title, purpose: purpose.value,
        requirements: requirements.value.split('\n').filter(value => value.trim()), items: choices }, catalogue);
      for (const [index, line] of draft.items.entries()) {
        const row = make('li'); row.append(make('h3', '', line.title), make('p', '', `${line.quantity} × ${line.option}`));
        if (line.note) row.append(make('p', '', line.note));
        row.append(button('Remove', 'remove', () => { choices.splice(index, 1); state.textContent = 'Unsaved changes.'; draw(); }));
        rows.append(row);
      }
      const summary = selectionSummary(draft, catalogue);
      totals.textContent = summary.observed_subtotals.map(total => `${total.currency} ${total.amount} observed subtotal`).join(' · ')
        || 'Ask the business to confirm prices and currency.';
      if (summary.observed_subtotals.length && summary.price_coverage === 'incomplete') totals.textContent += ' · Incomplete pricing.';
      brief.textContent = selectionBrief(draft, catalogue);
    } catch (error) { draft = null; state.textContent = error.message; totals.textContent = 'Fix the selection before saving.'; brief.textContent = ''; }
    buttons();
  }
  const add = button('Add to selection', 'add', () => {
    const item = { product_id: product.value, variant_id: option.value, quantity: Number(quantity.value), note: note.value };
    if (!quantity.checkValidity() || !Number.isSafeInteger(item.quantity) || item.quantity < 1) { state.textContent = 'Use a whole quantity from 1 to 10000.'; return; }
    const candidate = structuredClone(choices), index = candidate.findIndex(line => line.product_id === item.product_id && line.variant_id === item.variant_id);
    if (index >= 0) { candidate[index].quantity += item.quantity; if (item.note) candidate[index].note = item.note; }
    else candidate.push(item);
    try {
      makeSelection({ project_ref, store, title, purpose: purpose.value, requirements: [], items: candidate }, catalogue);
      choices = candidate; state.textContent = 'Unsaved changes.'; draw();
    } catch (error) { state.textContent = error.message; }
  });
  async function load(initial = false) {
    busy = true; buttons();
    try {
      const saved = (await storage.readLocal(key)).find(row => row.key === key);
      if (destroyed) return;
      if (saved) {
        const restored = validateSelection(saved.value, catalogue);
        if (restored.project_ref !== project_ref || restored.store !== store) throw Error('Saved selection belongs to another project.');
        choices = restored.items.map(({ product_id, variant_id, quantity, note }) => ({ product_id, variant_id, quantity, note }));
        purpose.value = restored.purpose; requirements.value = restored.requirements.join('\n'); revision = saved.revision;
        state.textContent = 'Reopened the selection saved on this device.';
      } else state.textContent = initial ? 'You can prepare an enquiry without sending anything.' : 'No saved selection on this device.';
    } catch (error) { state.textContent = error.message + ' The current selection has not been replaced.'; }
    finally { if (!destroyed) { busy = false; draw(); } }
  }
  const save = button('Save on this device', 'save', async () => {
    if (!draft || busy) return;
    const value = structuredClone(draft); busy = true; buttons();
    try { const result = await storage.saveLocal(key, value, revision); revision = result.revision; state.textContent = 'Saved on this device. Nothing sent.'; }
    catch (error) { state.textContent = error.message; }
    finally { if (!destroyed) { busy = false; buttons(); } }
  });
  const reload = button('Reopen saved selection', 'reload', () => load());
  const download = button('Download enquiry', 'download', () => {
    if (!draft) return;
    const url = URL.createObjectURL(new Blob([selectionBrief(draft, catalogue)], { type: 'text/plain;charset=utf-8' }));
    const link = make('a'); link.href = url; link.download = 'enquiry.txt'; doc.body.append(link); link.click(); link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000); state.textContent = 'Enquiry downloaded. Nothing sent.';
  });
  for (const input of [purpose, requirements]) input.addEventListener('input', () => { state.textContent = 'Unsaved changes.'; draw(); }, { signal: events.signal });
  const inputFields = make('div', 'cw-fields');
  inputFields.append(field('Product', product), field('Option', option), field('Quantity', quantity), field('Item note', note), add,
    field('What are you planning?', purpose), field('Questions or requirements (one per line)', requirements));
  inputs.append(inputFields);
  const actions = make('div', 'cw-actions-buttons'); actions.append(save, reload, download);
  detail.append(make('h3', '', 'Your project'), rows, totals,
    make('p', '', 'Prices and availability need confirmation. This is not an order or a quotation.'), source, actions, state);
  layout.append(inputs, detail); shell.append(header, layout); root.replaceChildren(shell);
  await load(true);
  return { getDraft: () => draft ? structuredClone(draft) : null, destroy() { destroyed = true; events.abort(); root.replaceChildren(); } };
}
