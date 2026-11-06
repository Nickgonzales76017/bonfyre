import {readLocal} from './store.mjs';
import {workspaceSelection, escapeHTML as esc} from './workspace-navigation.mjs';
import {plannerViews, projectRows, projectsView, comparisonView, comparisonBrief} from './planner-projects.mjs';

export function bindProjects({root, catalogue, prefix, current, open, create, copy, canLeave, isBusy, fail}) {
  const ui = name => root.querySelector(`[data-ui="${name}"]`);
  let view = 'layout', projects = [], query = '', transitioning = false, lastURL = location.href;
  const requestedView = url => workspaceSelection({workspace: prefix, view: url.searchParams.get('view')}, [{id: prefix}], plannerViews).view.id;
  const urlFor = (nextView, id = current().id) => {
    const url = new URL(location.href); url.searchParams.set('view', nextView); url.searchParams.set('project', id);
    return url;
  };
  const writeURL = (url, replace = false) => {history[replace ? 'replaceState' : 'pushState'](null, '', url); lastURL = location.href;};
  function renderComparison(preferred) {
    const left = preferred || ui('compare-left').value || current().id;
    const right = ui('compare-right').value || new URL(location.href).searchParams.get('against');
    const options = projects.map(row => `<option value="${esc(row.value.plan.id)}">${esc(row.value.plan.title)}</option>`).join('');
    ui('compare-left').innerHTML = options; ui('compare-right').innerHTML = options;
    const a = projects.find(row => row.value.plan.id === left) ?? projects[0];
    const b = projects.find(row => row.value.plan.id === right && row !== a) ?? projects.find(row => row !== a);
    if (a) ui('compare-left').value = a.value.plan.id;
    if (b) ui('compare-right').value = b.value.plan.id;
    ui('download-comparison').disabled = !a || !b;
    ui('compare-left').disabled = ui('compare-right').disabled = projects.length < 2;
    ui('comparison').innerHTML = a && b ? comparisonView(a.value.plan, b.value.plan, catalogue) : '<p class="fp-empty">Save two plans to compare them. Use “Save as another option” to try a different arrangement while keeping the first.</p>';
  }
  function render() {
    ui('save-controls').hidden = !['layout', 'fixtures'].includes(view);
    for (const id of ['layout', 'fixtures', 'projects', 'compare']) ui(id).hidden = id !== view;
    for (const button of root.querySelectorAll('[data-planner-view]')) {
      if (button.dataset.plannerView === view) button.setAttribute('aria-current', 'page'); else button.removeAttribute('aria-current');
    }
    ui('project-list').innerHTML = projectsView(projects, current().id, query);
    if (view === 'compare') renderComparison();
    root.dataset.workspaceView = view;
  }
  async function refresh() {
    const result = projectRows(await readLocal(prefix), prefix, catalogue); projects = result.projects;
    ui('unavailable-projects').textContent = result.unavailable.length ? `${result.unavailable.length} saved plan(s) use unavailable or changed product information. Their stored copies remain untouched.` : '';
    render();
  }
  async function navigate(url, {replace = false, pop = false, action} = {}) {
    if (transitioning || isBusy()) {if (pop) history.replaceState(null, '', lastURL); return;}
    const id = url.searchParams.get('project');
    if ((action === create || !action && id && id !== current().id) && !canLeave()) {if (pop) history.replaceState(null, '', lastURL); return;}
    transitioning = true; root.inert = true;
    try {
      await refresh();
      if (action) action();
      else if (id && id !== current().id) {
        const row = projects.find(row => row.value.plan.id === id);
        if (!row) throw Error('This saved project is unavailable on this browser. Open another project from Saved projects.');
        open(row);
      }
      view = requestedView(url); url.searchParams.set('project', current().id); url.searchParams.set('view', view);
      if (!pop) writeURL(url, replace); else {history.replaceState(null, '', url); lastURL = location.href;}
      if (pop && view === 'compare') {
        ui('compare-left').value = url.searchParams.get('compare') ?? '';
        ui('compare-right').value = url.searchParams.get('against') ?? '';
      }
      render();
    } catch (error) {fail(error.message); if (pop) history.replaceState(null, '', lastURL);}
    finally {transitioning = false; root.inert = false;}
  }
  root.addEventListener('click', event => {
    const tab = event.target.closest('[data-planner-view]');
    if (tab) void navigate(urlFor(tab.dataset.plannerView));
    const row = event.target.closest('[data-project-open]');
    if (row) void navigate(urlFor('layout', row.dataset.projectOpen));
    const compare = event.target.closest('[data-project-compare]');
    if (compare) {
      const id = compare.dataset.projectCompare;
      void navigate(urlFor('compare')).then(() => {if (view === 'compare') {renderComparison(id); retainComparison();}});
    }
  });
  ui('project-search').onsubmit = event => {event.preventDefault(); query = new FormData(event.target).get('query'); render();};
  ui('new-project').onclick = () => void navigate(urlFor('layout'), {action: create});
  ui('duplicate-project').onclick = () => void navigate(urlFor('layout'), {action: copy});
  function retainComparison() {
    const url = urlFor('compare'); url.searchParams.set('against', ui('compare-right').value); url.searchParams.set('compare', ui('compare-left').value); writeURL(url, true);
  }
  for (const side of ['left', 'right']) ui('compare-' + side).onchange = () => {
    // Keep the user's selected side when both selectors name the same plan.
    if (ui('compare-left').value === ui('compare-right').value) {
      const other = side === 'left' ? 'right' : 'left';
      ui('compare-' + other).value = projects.find(row => row.value.plan.id !== ui('compare-' + side).value)?.value.plan.id ?? '';
    }
    renderComparison();
    retainComparison();
  };
  ui('download-comparison').onclick = () => {
    const a = projects.find(row => row.value.plan.id === ui('compare-left').value), b = projects.find(row => row.value.plan.id === ui('compare-right').value);
    if (!a || !b) return;
    const url = URL.createObjectURL(new Blob([comparisonBrief(a.value.plan, b.value.plan, catalogue)], {type: 'text/plain;charset=utf-8'}));
    const link = document.createElement('a'); link.href = url; link.download = 'fixture-plan-comparison.txt'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
  addEventListener('popstate', () => void navigate(new URL(location.href), {pop: true}));
  addEventListener('beforeunload', event => {if (current().unsaved) {event.preventDefault(); event.returnValue = '';}});
  return {
    refresh,
    async init() {view = requestedView(new URL(location.href)); await refresh(); renderComparison(new URL(location.href).searchParams.get('compare')); writeURL(urlFor(view), true); render();},
  };
}
