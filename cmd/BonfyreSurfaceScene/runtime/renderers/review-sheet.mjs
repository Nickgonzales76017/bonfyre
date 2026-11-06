/** Review the exact candidate supplied by a domain owner. Confirmation is a UI
 * operation, not approval or permission. No source or persistence is inferred.
 */
export function escapeText(value) {
  return String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
}
const valueText = value => value === '' || value === null || value === undefined ? 'Empty' : String(value);
export function changeMarkup(changes) {
  if (!Array.isArray(changes) || changes.length > 20000) throw Error('The change review exceeds its display bound.');
  return `<div class="cw-bulk-diffs">${changes.map(change =>
    `<div class="cw-bulk-diff"><div><strong>${escapeText(change.title)}</strong>` +
    `<small>${escapeText(change.option ?? change.subject ?? '')} · ${escapeText(change.field)}</small></div>` +
    `<del>${escapeText(valueText(change.before))}</del><ins>${escapeText(valueText(change.after))}</ins></div>`
  ).join('')}</div>`;
}
export function createReviewSheet({dialog, title, body, confirm, status, currentVersion, renderChanges = changeMarkup}) {
  if (!dialog || !title || !body || !confirm || !status || typeof currentVersion !== 'function') {
    throw new TypeError('A review sheet needs its rendered controls and a current edit version.');
  }
  let candidate = null, busy = false, focus = null;
  const cancel = event => { if (busy) event.preventDefault(); };
  dialog.addEventListener('cancel', cancel);
  return {
    get open() { return dialog.open; },
    get candidate() { return candidate; },
    show({heading, changes, action, label, payload}) {
      if (busy) throw Error('A reviewed operation is still being saved.');
      candidate = {version: currentVersion(), action, payload};
      title.textContent = heading; body.innerHTML = renderChanges(changes);
      confirm.textContent = label; confirm.dataset.gridAction = action; confirm.disabled = false;
      status.textContent = 'Saved records unchanged'; focus = dialog.ownerDocument.activeElement;
      if (!dialog.open) dialog.showModal();
    },
    take(action) {
      if (!candidate || !dialog.open || candidate.action !== action) throw Error('Review the current changes first.');
      if (!Object.is(candidate.version, currentVersion())) throw Error('The working copy changed. Review these changes again.');
      return candidate.payload;
    },
    pending(value, text = '') { busy = value; confirm.disabled = value; if (text) status.textContent = text; },
    message(text) { status.textContent = text; },
    close() {
      if (busy) return false;
      candidate = null; dialog.close();
      if (focus?.isConnected) focus.focus();
      return true;
    },
    destroy() { busy = false; candidate = null; if (dialog.open) dialog.close(); dialog.removeEventListener('cancel', cancel); }
  };
}
