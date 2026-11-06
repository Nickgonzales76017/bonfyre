// Presentation routing over a caller's current authorized workspace list.
// Callers still authorize every server operation against its native owner.
export function workspacePath(base, workspace, view) {
  return `${base}/${encodeURIComponent(workspace)}/${encodeURIComponent(view)}`;
}
export function workspaceRoute(url, base, workspaces, views) {
  const parts = url.pathname.slice(base.length).split('/').filter(Boolean);
  return workspaceSelection({workspace:parts[0],view:parts[1]??url.hash.slice(1)},workspaces,views);
}
export function workspaceSelection(requested, workspaces, views) {
  const workspace = workspaces.find(w => w.id === requested.workspace) ?? workspaces[0] ?? null;
  const available = workspace ? views.filter(v => v.available(workspace)) : [];
  const view = available.find(v => v.id === requested.view) ?? available[0] ?? null;
  return {workspace, view, available};
}
export const escapeHTML = value => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
export function workspaceLinks(base, workspace, views, active) {
  if (!workspace) return '';
  return views.filter(v => v.available(workspace)).map(v => `<a href="${workspacePath(base, workspace.id, v.id)}" data-action="mode.${escapeHTML(v.id)}"${active === v.id ? ' aria-current="page"' : ''}>${escapeHTML(v.label)}</a>`).join('');
}
