import { readFileSync, writeFileSync, existsSync, mkdirSync, mkdtempSync, rmSync, renameSync } from 'node:fs';
import { join, resolve, dirname } from 'node:path';
import { homedir, tmpdir } from 'node:os';
import { createHash, randomUUID } from 'node:crypto';
import { Database } from 'bun:sqlite';
import { checkRecording, readRecording } from './document-recording.mjs';

const here = import.meta.dir;
const hash = value => createHash('sha256').update(value).digest('hex');
const escape = text => String(text).replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]));
const binary = (name, nativeDirectory) => join(nativeDirectory ?? join(homedir(), '.bonfyre/bin'), 'bonfyre-' + name);

async function native(args) {
  const child = Bun.spawn(args, { stdout:'pipe', stderr:'pipe' });
  const timer = setTimeout(() => child.kill(), 60000);
  try {
    const [out, err, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
    if (code !== 0) throw Error('The local document operation failed. ' + err.slice(-700));
    return out;
  } finally { clearTimeout(timer); }
}

export function readCollection(file) {
  const raw = readFileSync(file);
  if (raw.length > 1_000_000) throw Error('Use a collection smaller than 1 MB for this workspace.');
  const collection = JSON.parse(raw);
  if (collection.schema !== 'service-site.documents.v1' || typeof collection.title !== 'string' ||
      !Array.isArray(collection.documents) || collection.documents.length > 100) throw Error('Invalid document collection.');
  const ids = new Set();
  const documents = collection.documents.map(d => {
    if (!/^[a-z0-9][a-z0-9-]{0,63}$/.test(d.id ?? '') || ids.has(d.id) ||
        typeof d.title !== 'string' || !d.title.trim() || d.title.length > 200 ||
        typeof d.text !== 'string' || !d.text.trim() || d.text.length > 100000) throw Error('Each document needs a unique ID, title and bounded text.');
    ids.add(d.id);
    const paragraphs = d.text.split(/\n\s*\n/).filter(x => x.trim());
    const media = d.media === undefined ? null : checkRecording(dirname(file),d.media,paragraphs);
    const digest = hash(media ? JSON.stringify({text:d.text,media}) : d.text);
    const passages = [];
    for (let paragraph = 0; paragraph < paragraphs.length; paragraph++) {
      const words = paragraphs[paragraph].trim().split(/\s+/);
      for (let offset = 0; offset < words.length; offset += 160) {
        passages.push({ id: `${d.id}:${digest.slice(0,16)}:${passages.length}`, document_id:d.id, paragraph:paragraph + 1,
          text:words.slice(offset, offset + 180).join(' '), ...(media ? {recording:media.segments[paragraph],review:media.review} : {}) });
      }
    }
    return { id:d.id, title:d.title, text:d.text, digest, passages, ...(media?{media}:{}) };
  });
  return { title:collection.title, synthetic:collection.synthetic === true, documents, fingerprint:hash(raw) };
}

export async function prepareDocuments(dir, { run = native, nativeDirectory } = {}) {
  const file = join(resolve(dir), 'documents.json'), collection = readCollection(file);
  const state = join(resolve(dir), '.documents');
  mkdirSync(state, { recursive:true, mode:0o700 });
  const db = join(state, 'index.db'), stamp = join(state, 'index.json');
  if (existsSync(db)) {
    if (!existsSync(stamp) || JSON.parse(readFileSync(stamp)).fingerprint !== collection.fingerprint)
      throw Error('The collection changed. Keep the old index for review and use a new collection folder to index this version.');
  } else {
    const temporary = mkdtempSync(join(state, 'prepare-'));
    try {
      const rows = collection.documents.flatMap(d => d.passages);
      if (!rows.length) throw Error('Add a document before preparing this collection.');
      const input = join(temporary, 'passages.jsonl'), nextDb = join(temporary, 'index.db');
      writeFileSync(input, rows.map(p => JSON.stringify({ id:p.id, text:'search_document: '+p.text })).join('\n') + '\n', { mode:0o600 });
      const receipt = JSON.parse(await run([binary('embed', nativeDirectory), '--input-jsonl', input, '--backend', 'onnx', '--model', 'nomic-embed-text-onnx', '--insert-db', nextDb, '--out', join(temporary,'embeddings.json')]));
      if (receipt.stored !== rows.length || receipt.failed || receipt.not_stored || receipt.degraded || receipt.backend !== 'onnx-runtime-native')
        throw Error('The local embedding engine did not index every passage with the requested model.');
      const opened = new Database(nextDb, { readonly:true });
      let stored;
      try { stored = new Set(opened.query('SELECT id FROM artifacts').all().map(r => r.id)); }
      finally { opened.close(); }
      if (stored.size !== rows.length || rows.some(p => !stored.has(p.id))) throw Error('Some document passages were not indexed.');
      // Native Embed closes its database before this move. The main file carries its completed transaction.
      renameSync(nextDb, db);
      writeFileSync(stamp, JSON.stringify({ fingerprint:collection.fingerprint, passages:rows.length, embedding:receipt, embedding_tasks:'nomic-search-v1' })+'\n', { mode:0o600 });
    } finally { rmSync(temporary, { recursive:true, force:true }); }
  }
  return createDocumentOwner({ file, db, collection, run, nativeDirectory, embeddingTasks:JSON.parse(readFileSync(stamp)).embedding_tasks });
}

export function createDocumentOwner({ file, db, collection, run = native, nativeDirectory, embeddingTasks }) {
  let busy = false;
  const active = () => {
    if (hash(readFileSync(file)) !== collection.fingerprint) throw Error('The source collection changed. Restart with a reviewed index before continuing.');
    for (const d of collection.documents) if (d.media) readRecording(dirname(file),d.media);
    const opened = new Database(db, { readonly:true });
    try { return new Set(opened.query('SELECT id FROM artifacts').all().map(r => r.id)); }
    finally { opened.close(); }
  };
  const visible = ids => collection.documents.map(d => ({ id:d.id, title:d.title, digest:d.digest,
    indexed:d.passages.every(p => ids.has(p.id)), passage_count:d.passages.length,
    ...(d.media?{duration_ms:d.media.duration_ms,content_type:d.media.content_type}:{}) }));
  const serial = async work => {
    if (busy) throw Error('Another document operation is running. Try again when it finishes.');
    busy = true;
    try { return await work(); } finally { busy = false; }
  };
  return {
    list() { return { title:collection.title, synthetic:collection.synthetic, collection_id:collection.fingerprint, documents:visible(active()) }; },
    source(id) {
      const ids = active(), d = collection.documents.find(d => d.id === id);
      if (!d || !d.passages.every(p => ids.has(p.id))) throw Error('This document is no longer in search.');
      return { id:d.id, title:d.title, digest:d.digest, text:d.text, passages:d.passages, ...(d.media?{media:d.media}:{}) };
    },
    media(id) {
      const ids=active(), d=collection.documents.find(d=>d.id===id);
      if(!d?.media || !d.passages.every(p=>ids.has(p.id))) throw Error('This recording is no longer in search.');
      return {bytes:readRecording(dirname(file),d.media),type:d.media.content_type};
    },
    async search(query, mode = 'semantic') {
      if (typeof query !== 'string' || !query.trim() || query.length > 500 || !['semantic','phrase'].includes(mode))
        throw Error('Enter a question or phrase of 1 to 500 characters.');
      return serial(async () => {
        const ids = active(), docs = collection.documents.filter(d => d.passages.every(p => ids.has(p.id)));
        if (!docs.length) return { mode, results:[], message:'No documents remain in search.' };
        const byId = new Map(docs.flatMap(d => d.passages.map(p => [p.id, { ...p, title:d.title, digest:d.digest }])));
        if (mode === 'phrase') {
          const needle = query.trim().toLocaleLowerCase();
          return { mode, results:[...byId.values()].filter(p => p.text.toLocaleLowerCase().includes(needle)).slice(0,10),
            message:'Exact phrase matches in the indexed passages.' };
        }
        const temp = mkdtempSync(join(tmpdir(), 'bonfyre-document-query-'));
        try {
          writeFileSync(join(temp,'query.txt'), (embeddingTasks==='nomic-search-v1'?'search_query: ':'')+query.trim(), { mode:0o600 });
          await run([binary('embed', nativeDirectory), '--text', join(temp,'query.txt'), '--out', join(temp,'query.json'), '--backend','onnx','--model','nomic-embed-text-onnx']);
          const found = JSON.parse(await run([binary('vec', nativeDirectory),'search',db,join(temp,'query.json'),'--exact','--top','10']));
          active();
          return { mode, results:found.results.filter(r => byId.has(r.id)).map(r => byId.get(r.id)),
            message:'Passages ranked by relevance to your question.' };
        } finally { rmSync(temp, { recursive:true, force:true }); }
      });
    },
    async remove(id, digest) {
      return serial(async () => {
        active();
        const d = collection.documents.find(d => d.id === id);
        if (!d || digest !== d.digest) throw Error('The selected source changed. Reopen it before removing it.');
        const removed = JSON.parse(await run([binary('vec', nativeDirectory),'delete',db,...d.passages.map(p => p.id)]));
        const ids = active();
        if (d.passages.some(p => ids.has(p.id))) throw Error('Document removal was incomplete.');
        return { id, removed, message:'Removed from search. The original collection and any saved notes remain on this Mac.' };
      });
    }
  };
}

export function documentPage(token) {
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="document-session" content="${escape(token)}"><title>Local workspace</title><link rel="stylesheet" href="/documents.css"></head><body>
  <header><a href="/">Aurekai <span>LOCAL WORKSPACE</span></a><p id="privacy">Search runs on this Mac.</p></header>
  <main><div class="intro"><p class="eyebrow" id="workspace-kind">DOCUMENT WORKSPACE</p><h1 id="title">Your documents</h1><p id="collection-note"></p></div>
  <form id="search-form"><label for="query">What are you looking for?</label><div class="search-row"><input id="query" maxlength="500" placeholder="Search your documents…" autocomplete="off"><select id="mode" aria-label="Search method"><option value="semantic">By meaning</option><option value="phrase">Exact phrase</option></select><button id="search">Find sources</button></div></form>
  <p id="status" role="status" aria-live="polite">Opening the local collection…</p><p id="error" role="alert" hidden></p>
  <div class="workspace"><aside><h2>In this collection</h2><div id="documents"></div><h2>Your saved notes</h2><div id="notes"></div></aside>
  <section class="results-pane"><h2 id="results-title">Start with a question</h2><p id="guidance">Open a recording or document, or search for a passage.</p><div id="results"></div></section>
  <article id="source" hidden><button type="button" id="close-source" class="quiet">Close source</button><p class="eyebrow">SOURCE</p><h2 id="source-title"></h2><p id="source-credit" hidden></p><a id="source-link" target="_blank" rel="noopener noreferrer" hidden>View original</a><p id="transcript-status" hidden></p><video id="recording-player" controls preload="metadata" hidden></video><div id="recording-controls" hidden><label><input id="follow-recording" type="checkbox" checked> Follow playback</label><button id="download-transcript" type="button" class="quiet">Download transcript</button></div><div id="source-text"></div><p id="citation"></p>
  <label for="note">Your note</label><textarea id="note" maxlength="4000" rows="4" placeholder="Write what you want to remember, in your own words."></textarea><button id="save-note" type="button">Save cited note</button><p id="save-state" role="status"></p>
  <details><summary>Remove this document from search</summary><p>The original document, saved notes and local embedding cache remain. This is not secure erasure.</p><button id="remove" type="button" class="danger">Remove from search</button></details></article></div></main>
  <footer>Bonfÿre · Aurekai</footer><script type="module" src="/documents.mjs"></script></body></html>`;
}

export function createDocumentHandler(owner, { origin, token = randomUUID() }) {
  const mediaCookie='bf_media_'+hash(origin).slice(0,12);
  const assets = { '/documents.mjs':['document-client.mjs','text/javascript'], '/documents.css':['document-workspace.css','text/css'],
    '/store.mjs':['../../integrations/browser/extension/store.mjs','text/javascript'],
    ...Object.fromEntries(['dm-sans','dm-mono','dm-mono-medium','instrument-serif','instrument-serif-italic'].map(name => ['/fonts/'+name+'.woff2',['fonts/'+name+'.woff2','font/woff2']])) };
  const respond = (body, status = 200, type = 'application/json') => new Response(type === 'application/json' ? JSON.stringify(body) : body,
    { status, headers:{ 'Content-Type':type.startsWith('font/') ? type : type+'; charset=utf-8', 'Cache-Control':'no-store', 'X-Content-Type-Options':'nosniff',
      'Content-Security-Policy':"default-src 'self'; connect-src 'self'; media-src 'self' blob:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'" } });
  return async request => {
    const url = new URL(request.url);
    if (url.origin !== origin || request.headers.get('host') && request.headers.get('host') !== new URL(origin).host ||
        request.headers.get('origin') && request.headers.get('origin') !== origin) return respond({error:'This workspace accepts requests from its own local page.'},403);
    if (request.method === 'GET' && url.pathname === '/') {
      const page=respond(documentPage(token),200,'text/html');
      page.headers.set('Set-Cookie',`${mediaCookie}=${encodeURIComponent(token)}; HttpOnly; SameSite=Strict; Path=/api/media`);
      return page;
    }
    if (request.method === 'GET' && assets[url.pathname]) {
      const [file,type] = assets[url.pathname]; return respond(readFileSync(join(here,file),type.startsWith('font/') ? undefined : 'utf8'),200,type);
    }
    const mediaRequest=url.pathname==='/api/media'&&['GET','HEAD'].includes(request.method);
    const mediaSession=mediaRequest&&(request.headers.get('cookie')||'').split(';').some(value=>value.trim()===`${mediaCookie}=${encodeURIComponent(token)}`);
    if (request.headers.get('x-document-session') !== token&&!mediaSession) return respond({error:'Reopen the local workspace.'},403);
    try {
      if (request.method === 'GET' && url.pathname === '/api/documents') return respond(owner.list());
      if (request.method === 'GET' && url.pathname === '/api/source') return respond(owner.source(url.searchParams.get('id')));
      if (mediaRequest) {
        const media=owner.media(url.searchParams.get('id'));
        const size=media.bytes.length,range=request.headers.get('range');let start=0,end=size-1,status=200;
        const headers={'Content-Type':media.type,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Accept-Ranges':'bytes'};
        if(range){
          const match=/^bytes=(\d*)-(\d*)$/.exec(range);
          if(!match||(!match[1]&&!match[2]))return new Response(null,{status:416,headers:{...headers,'Content-Range':`bytes */${size}`}});
          if(match[1]){start=Number(match[1]);end=match[2]?Math.min(Number(match[2]),size-1):size-1;}
          else {const suffix=Number(match[2]);start=suffix>0?Math.max(0,size-suffix):size;}
          if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start>=size||start>end)return new Response(null,{status:416,headers:{...headers,'Content-Range':`bytes */${size}`}});
          status=206;headers['Content-Range']=`bytes ${start}-${end}/${size}`;
        }
        headers['Content-Length']=String(end-start+1);
        return new Response(request.method==='HEAD'?null:media.bytes.subarray(start,end+1),{status,headers});
      }
      if (request.method === 'POST' && ['/api/search','/api/remove'].includes(url.pathname)) {
        if (!request.headers.get('content-type')?.startsWith('application/json')) return respond({error:'Expected JSON.'},415);
        const text = await request.text(); if (text.length > 8192) return respond({error:'Request too large.'},413);
        const body = JSON.parse(text);
        return respond(url.pathname === '/api/search' ? await owner.search(body.query,body.mode) : await owner.remove(body.id,body.digest));
      }
      return respond({error:'Not found.'},404);
    } catch (error) { return respond({error:error.message},400); }
  };
}

export async function serveDocuments(dir, port = 4177, options = {}) {
  if (!Number.isInteger(port) || port < 1024 || port > 65535) throw Error('Use a local port from 1024 to 65535.');
  const owner = await prepareDocuments(dir, options), origin = `http://127.0.0.1:${port}`;
  const server = Bun.serve({ hostname:'127.0.0.1', port, maxRequestBodySize:8192, idleTimeout:90,
    fetch:createDocumentHandler(owner,{origin}) });
  console.log(JSON.stringify({url:origin,collection:owner.list().title,synthetic:owner.list().synthetic}));
  for (const signal of ['SIGINT','SIGTERM']) process.once(signal, () => { server.stop(true); process.exit(0); });
  return server;
}
