// Executable composition of existing SurfaceScene coordinates. Data and effects
// stay in the registered binding. This module owns transient interaction only.
const list = value => Array.isArray(value) ? value : [];
const terms = value => typeof value === 'string' ? value.split(',').map(s => s.trim()).filter(Boolean) : list(value);
const id = value => { if (typeof value !== 'string' || !value || value.length > 240 || /[\u0000-\u001f\u007f]/u.test(value)) throw Error('Invalid workspace coordinate.'); return value; };
const immutable = value => Object.freeze(value);

// Selection coordinates are small immutable values, never retained material bodies.
function selectionSnapshot(value) {
  let nodes = 0, chars = 0; const active = new Set();
  const copy = (v, depth) => {
    if (++nodes > 4096 || depth > 16) throw Error('Selection exceeds its coordinate bound.');
    if (v === null || typeof v === 'boolean') return v;
    if (typeof v === 'number' && Number.isFinite(v)) return v;
    if (typeof v === 'string') { chars += v.length; if (chars > 65536) throw Error('Selection exceeds its text bound.'); return v; }
    if (!v || typeof v !== 'object' || active.has(v)) throw Error('Selection must contain acyclic plain coordinates.');
    const array = Array.isArray(v);
    if (!array && ![Object.prototype, null].includes(Object.getPrototypeOf(v))) throw Error('Selection must refer to material, not contain a mutable material object.');
    const keys = Reflect.ownKeys(v), descriptors = Object.getOwnPropertyDescriptors(v);
    if (array && (v.length > 4096 || keys.length !== v.length + 1)) throw Error('Selection arrays must be dense and bounded.');
    if (keys.some(k => typeof k !== 'string' || (!array || k !== 'length') && (!descriptors[k].enumerable || !Object.hasOwn(descriptors[k], 'value')))) throw Error('Selection has unsupported properties.');
    active.add(v); const out = array ? [] : {};
    for (const k of keys) {
      if (array && k === 'length') continue;
      if (array && (!/^(0|[1-9][0-9]*)$/.test(k) || Number(k) >= v.length)) throw Error('Selection has unsupported array properties.');
      chars += k.length; if (chars > 65536) throw Error('Selection exceeds its text bound.');
      Object.defineProperty(out, k, {value: copy(descriptors[k].value, depth + 1), enumerable: true});
    }
    active.delete(v); return immutable(out);
  };
  return copy(value, 0);
}
function sameSelection(a, b) {
  if (Object.is(a, b)) return true;
  if (!a || !b || typeof a !== 'object' || typeof b !== 'object' || Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every(k => Object.hasOwn(b, k) && sameSelection(a[k], b[k]));
}
function commandDirectory(commands) {
  // Exposing a mutable Map allowed callers to bypass the compiled action aperture.
  return immutable({get: key => commands.get(key), has: key => commands.has(key),
    values: () => commands.values(), keys: () => commands.keys(), entries: () => commands.entries(),
    get size() { return commands.size; }, [Symbol.iterator]: () => commands[Symbol.iterator]()});
}

export function compileWorkspace(scene, contributions) {
  if (scene?.schema !== 'bonfyre-surface-scene.v1') throw Error('A workspace requires its derived SurfaceScene.');
  id(scene.source_digest); id(scene.focus?.subject_address); id(scene.focus?.actor_scope);
  if (!(contributions instanceof Map)) throw Error('Bindings must be explicitly registered.');
  const slots = new Map(), bindings = new Set(), panels = [], commands = new Map();
  if (list(scene.slots).length > 64 || list(scene.bindings).length > 128) throw Error('Workspace exceeds its composition bound.');
  for (const slot of list(scene.slots)) {
    id(slot.slot_id); if (slots.has(slot.slot_id)) throw Error('Duplicate semantic slot.');
    if (!['section','page','workspace'].includes(slot.extent)) throw Error('This renderer needs a section, page or workspace extent.');
    slots.set(slot.slot_id, slot);
  }
  for (const binding of list(scene.bindings)) {
    id(binding.binding_id); if (bindings.has(binding.binding_id)) throw Error('Duplicate application binding.');
    bindings.add(binding.binding_id);
    const slot = slots.get(binding.slot_id), adapter = contributions.get(binding.binding_id);
    if (!slot) throw Error('Application binding has no semantic slot.');
    if (!list(slot.accepts).includes(binding.application_kind)) throw Error('The slot does not accept this application kind.');
    if (!adapter) throw Error(`No executable binding for ${binding.binding_id}.`);
    if (adapter.kind !== binding.application_kind) throw Error('The registered binding has a different application kind.');
    if (typeof adapter.element !== 'function') throw Error('A binding must supply its existing content element.');
    id(adapter.label);
    if (list(adapter.commands).length > 64) throw Error('Binding exceeds its command bound.');
    if (slot.extent === 'section' && list(adapter.commands).length) throw Error('A section is a reference without effect controls.');
    const aperture = new Set(terms(binding.action_aperture));
    for (const command of list(adapter.commands)) {
      id(command.id); id(command.label);
      if (!aperture.has(command.id) || commands.has(command.id)) throw Error('Command is ambiguous or outside its action aperture.');
      if (typeof command.run !== 'function' || typeof command.enabled !== 'function') throw Error('Command requires its owner handler and current availability.');
      if (command.reads !== undefined && (!Array.isArray(command.reads) || command.reads.length > 128 || new Set(command.reads).size !== command.reads.length)) throw Error('Command read bindings must be a bounded distinct list.');
      if (command.reads !== undefined) command.reads.forEach(id);
      if (command.resultPolicy !== undefined && !['owner','preview'].includes(command.resultPolicy)) throw Error('Unknown command result policy.');
      commands.set(command.id, immutable({...command, reads: command.reads === undefined ? null : immutable([...new Set([binding.binding_id, ...command.reads])]), binding: binding.binding_id}));
    }
    panels.push(immutable({id: binding.binding_id, slot: slot.slot_id, kind: binding.application_kind,
      continuity: binding.continuity_key || '', extent: slot.extent, label: adapter.label,
      adapter, ordinal: slot.ordinal ?? panels.length}));
  }
  for (const command of commands.values()) if (command.reads?.some(key => !bindings.has(key))) throw Error('Command reads an unregistered workspace binding.');
  // max_items bounds owner material, not the number of mounted panes.
  const recipes = list(scene.interaction?.desktop_posture?.layouts);
  if (!recipes.length || recipes.length > 12) throw Error('A workspace needs bounded declared layouts.');
  const recipeNames = new Set();
  for (const recipe of recipes) {
    id(recipe.id); if (recipeNames.has(recipe.id)) throw Error('Duplicate workspace layout.'); recipeNames.add(recipe.id);
    const placed = new Set();
    for (const placement of list(recipe.panels)) {
      if (!bindings.has(placement.id) || placed.has(placement.id)) throw Error('Layout names an unknown or repeated binding.');
      if (placement.relative && !placed.has(placement.relative)) throw Error('Place a referenced panel before its dependant.');
      if (!['within','left','right','above','below'].includes(placement.direction ?? 'within')) throw Error('Unsupported panel direction.');
      placed.add(placement.id);
    }
    if (!placed.size) throw Error('A workspace layout cannot be empty.');
  }
  const initial = scene.interaction?.desktop_posture?.initial_layout ?? recipes[0].id;
  if (!recipeNames.has(initial)) throw Error('Initial layout is undeclared.');
  return immutable({revision: scene.source_digest, focus: immutable({...scene.focus}),
    panels: immutable(panels.sort((a,b) => a.ordinal-b.ordinal)), commands: commandDirectory(commands),
    layouts: immutable(recipes.map(r => immutable({...r, panels: immutable(r.panels.map(p => immutable({...p})))}))),
    initial});
}

export class WorkspaceSession {
  #plan; #closed = false; #listeners = new Set(); #selection = new Map(); #running = new Map();
  #epoch = 0; #generation = 0; #selectionEpochs = new Map(); #listenerErrors = [];
  constructor(scene, contributions) { this.#plan = compileWorkspace(scene, contributions); }
  get plan() { return this.#plan; }
  get closed() { return this.#closed; }
  diagnostics() { return this.#listenerErrors.map(row => ({...row})); }
  subscribe(listener) {
    if (this.#closed) throw Error('Workspace is closed.');
    if (typeof listener !== 'function') throw Error('A workspace listener must be callable.');
    this.#listeners.add(listener); return () => this.#listeners.delete(listener);
  }
  #emit(event) {
    for (const listener of [...this.#listeners]) {
      if (this.#closed) break;
      try { listener(immutable(event)); }
      catch (error) {
        this.#listenerErrors.push({type:event.type, message:String(error?.message ?? error).slice(0,1000)});
        if (this.#listenerErrors.length > 32) this.#listenerErrors.shift();
      }
    }
  }
  context(bindingId) {
    const panel = this.#plan.panels.find(p => p.id === bindingId);
    if (!panel || this.#closed) throw Error('This workspace binding is unavailable.');
    return immutable({revision:this.#plan.revision, focus:this.#plan.focus, binding:panel.id,
      selected:this.#selection.get(panel.continuity) ?? null, epoch:this.#epoch,
      generation:this.#generation, selectionEpoch:this.#selectionEpochs.get(panel.continuity) ?? 0,
      ownerVersion:panel.adapter.version?.()});
  }
  select(bindingId, selected) {
    const panel = this.#plan.panels.find(p => p.id === bindingId), context = this.context(bindingId);
    if (!panel.continuity) throw Error('This binding has no selection continuity.');
    const retained = selectionSnapshot(selected);
    if (retained !== null && (typeof panel.adapter.acceptSelection !== 'function' || !panel.adapter.acceptSelection(retained, context))) throw Error('Selection does not belong to this source.');
    if (sameSelection(this.#selection.get(panel.continuity) ?? null, retained)) return false;
    this.#selection.set(panel.continuity,retained); this.#epoch++;
    this.#selectionEpochs.set(panel.continuity,(this.#selectionEpochs.get(panel.continuity) ?? 0) + 1);
    this.#emit({type:'selection', continuity:panel.continuity, selected:retained, origin:bindingId}); return true;
  }
  available(commandId) {
    const command = this.#plan.commands.get(commandId);
    if (!command || this.#closed) return {enabled:false,reason:'This command is not part of the current workspace.'};
    if (this.#running.has(commandId)) return {enabled:false,reason:'Operation in progress.'};
    let result;
    try {result = command.enabled(this.context(command.binding));} catch {return {enabled:false,reason:'The owning workspace is currently unavailable.'};}
    return result === true ? {enabled:true,reason:''} : {enabled:false,reason:typeof result === 'string' ? result : 'Unavailable in the current selection.'};
  }
  commands(query='') {
    const words = String(query).toLocaleLowerCase().trim().split(/\s+/u).filter(Boolean);
    return [...this.#plan.commands.values()].filter(c => words.every(w => `${c.label} ${c.group ?? ''} ${c.id}`.toLocaleLowerCase().includes(w)))
      .map(c => ({...c,...this.available(c.id)}));
  }
  async execute(commandId) {
    const command = this.#plan.commands.get(commandId), availability = this.available(commandId);
    if (!availability.enabled) throw Error(availability.reason);
    const context = this.context(command.binding), controller = new AbortController();
    // Undeclared commands retain conservative workspace-wide selection invalidation.
    // Explicit reads capture each dependency, including its owner revision and group epoch.
    const inputs = immutable(Object.fromEntries((command.reads ?? [command.binding]).map(key => [key,this.context(key)])));
    const isCurrent = () => {
      if (this.#closed || controller.signal.aborted || context.generation !== this.#generation || command.reads === null && context.epoch !== this.#epoch) return false;
      try { return Object.values(inputs).every(before => {
        const now = this.context(before.binding);
        return before.selectionEpoch === now.selectionEpoch && Object.is(before.ownerVersion,now.ownerVersion);
      }); } catch { return false; }
    };
    this.#running.set(commandId,controller);
    try {
      this.#emit({type:'commands'});
      if (!isCurrent()) throw Error('Workspace changed before the operation started.');
      // The owner must still check current authority, domain revisions and input membership
      // at mutation. A local currency check neither reserves resources nor commits work.
      const result = await command.run({...context,inputs,signal:controller.signal,isCurrent});
      if (!this.#closed) {
        const current = isCurrent();
        // Preview completions cannot decorate a newly selected object. Owner-operation
        // results remain results even after their own write advances a revision.
        this.#emit({type:command.resultPolicy === 'preview' && !current ? 'stale-result' : 'result',id:commandId,result,context,inputs,current});
      }
      // Preserve the actual owner result; staleness never means an effect did not happen.
      return result;
    } finally { this.#running.delete(commandId); if(!this.#closed)this.#emit({type:'commands'}); }
  }
  refresh() { if (!this.#closed) this.#emit({type:'commands'}); }
  rebind(scene, contributions) {
    if(this.#closed)throw Error('Workspace is closed.');
    const next = compileWorkspace(scene,contributions);
    if (this.#running.size || this.#plan.panels.some(p=>p.adapter.dirty?.())) throw Error('Finish or retain open work before changing the workspace source.');
    this.#plan=next; this.#selection.clear(); this.#selectionEpochs.clear(); this.#epoch++; this.#generation++; this.#emit({type:'rebind'});
  }
  dispose() {if(this.#closed)return;this.#closed=true;for(const c of this.#running.values())c.abort();this.#running.clear();this.#listeners.clear();this.#selection.clear();this.#selectionEpochs.clear();}
}
