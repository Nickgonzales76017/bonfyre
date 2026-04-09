(function(global) {
  var repoMap = {
    'bonfyre': 'https://github.com/Nickgonzales76017/bonfyre',
    'bonfyre-core': 'https://github.com/Nickgonzales76017/bonfyre-core',
    'bonfyre-intake': 'https://github.com/Nickgonzales76017/bonfyre-intake',
    'bonfyre-pipeline': 'https://github.com/Nickgonzales76017/bonfyre-pipeline',
    'bonfyre-embed': 'https://github.com/Nickgonzales76017/bonfyre-embed',
    'bonfyre-cms': 'https://github.com/Nickgonzales76017/bonfyre-cms',
    'bonfyre-tel': 'https://github.com/Nickgonzales76017/bonfyre-tel',
    'liblambda-tensors': 'https://github.com/Nickgonzales76017/liblambda-tensors'
  };

  async function fetchJson(path) {
    var resp = await fetch(String(path || '').replace(/^\/+/, '') + '?v=' + Date.now(), {
      cache: 'no-store'
    });
    if (!resp.ok) return null;
    return resp.json();
  }

  async function fetchText(path) {
    var resp = await fetch(String(path || '').replace(/^\/+/, '') + '?v=' + Date.now(), {
      cache: 'no-store'
    });
    if (!resp.ok) return null;
    return resp.text();
  }

  function cloneItems(items) {
    return Array.isArray(items) ? items.slice() : [];
  }

  function normalizeToken(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value).replace(/[&<>\"']/g, function(ch) {
      return ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
      })[ch];
    });
  }

  function formatArtifactText(raw, href) {
    var text = String(raw || '').trim();
    if (!text) return '';
    if (/\.json($|\?)/.test(href || '')) {
      try {
        return JSON.stringify(JSON.parse(text), null, 2);
      } catch (_) {}
    }
    return text;
  }

  function markdownToHtml(text) {
    return escapeHtml(String(text || ''))
      .replace(/^### (.*)$/gm, '<h4>$1</h4>')
      .replace(/^## (.*)$/gm, '<h3>$1</h3>')
      .replace(/^# (.*)$/gm, '<h2>$1</h2>')
      .replace(/^\- (.*)$/gm, '<li>$1</li>')
      .replace(/\n\n+/g, '</p><p>')
      .replace(/(<li>.*<\/li>)/gs, '<ul>$1</ul>');
  }

  function injectStyles() {
    if (document.getElementById('bonfyre-demo-boot-styles')) return;
    var style = document.createElement('style');
    style.id = 'bonfyre-demo-boot-styles';
    style.textContent = [
      '.tag,.operator-chip,.output-chip,.swap-chip{cursor:pointer;transition:transform .12s ease,border-color .12s ease,opacity .12s ease;}',
      '.tag:hover,.operator-chip:hover,.output-chip:hover,.swap-chip:hover{transform:translateY(-1px);opacity:.92;border-color:#ff6600;}',
      '.bonfyre-preview-actions{display:flex;gap:.5rem;flex-wrap:wrap;margin-top:.75rem;}',
      '.bonfyre-preview-link{display:inline-flex;align-items:center;gap:.35rem;padding:.45rem .75rem;border-radius:6px;border:1px solid #333;color:#ff6600;text-decoration:none;font-size:.8rem;font-weight:600;background:rgba(255,102,0,.06);}',
      '.bonfyre-demo-search{display:none;margin-top:.85rem;}',
      '.bonfyre-demo-search-copy{font-size:.78rem;color:#a3a3a3;margin-bottom:.55rem;line-height:1.45;}',
      '.bonfyre-demo-search-input{width:100%;padding:.55rem .65rem;border-radius:6px;border:1px solid #333;background:#0f1117;color:#e5e7eb;margin-bottom:.65rem;}',
      '.bonfyre-demo-search-results{display:grid;gap:.45rem;}',
      '.bonfyre-demo-result{width:100%;text-align:left;padding:.65rem .75rem;border-radius:8px;border:1px solid #2f3b4b;background:#0d131d;color:#e5e7eb;cursor:pointer;}',
      '.bonfyre-demo-result strong{display:block;color:#ff6600;margin-bottom:.2rem;font-size:.8rem;}',
      '.bonfyre-demo-result span{display:block;font-size:.76rem;line-height:1.4;margin-bottom:.2rem;}',
      '.bonfyre-demo-result em{display:block;font-size:.72rem;color:#9ca3af;font-style:normal;}',
      '.bonfyre-demo-search-empty{padding:.65rem .75rem;border:1px solid #2f3b4b;border-radius:8px;background:#0d131d;color:#9ca3af;font-size:.76rem;}',
      '.bonfyre-detail-card{margin-top:.75rem;padding:.85rem;border:1px solid #2f3b4b;border-radius:10px;background:#0d131d;}',
      '.bonfyre-detail-card h4{font-size:.9rem;color:#ff6600;margin-bottom:.35rem;}',
      '.bonfyre-detail-meta{font-size:.73rem;color:#9ca3af;margin-bottom:.45rem;}',
      '.bonfyre-detail-copy{font-size:.8rem;line-height:1.5;color:#e5e7eb;margin-bottom:.6rem;}',
      '.bonfyre-detail-tags{display:flex;gap:.4rem;flex-wrap:wrap;margin-bottom:.55rem;}',
      '.bonfyre-detail-tag{font-size:.68rem;padding:.22rem .45rem;border-radius:999px;border:1px solid #334155;background:#111827;color:#cbd5e1;}',
      '.bonfyre-source-strip{margin:.8rem 0 .2rem;padding:.7rem .8rem;border:1px solid #223047;border-radius:10px;background:#0b111a;}',
      '.bonfyre-source-kicker{font-size:.72rem;letter-spacing:.08em;text-transform:uppercase;color:#93c5fd;margin-bottom:.3rem;}',
      '.bonfyre-source-copy{font-size:.78rem;line-height:1.45;color:#cbd5e1;margin-bottom:.5rem;}',
      '.bonfyre-source-actions{display:flex;gap:.45rem;flex-wrap:wrap;}',
      '.bonfyre-source-link{display:inline-flex;align-items:center;gap:.35rem;padding:.4rem .68rem;border-radius:999px;border:1px solid #334155;background:#111827;color:#e5e7eb;text-decoration:none;font-size:.74rem;font-weight:600;}',
      '.bonfyre-source-link:hover{border-color:#ff6600;color:#ff6600;}',
      '.bonfyre-artifact-view{margin-top:.75rem;padding:.85rem;border:1px solid #2f3b4b;border-radius:10px;background:#0d131d;}',
      '.bonfyre-artifact-view h4{font-size:.85rem;color:#ff6600;margin-bottom:.4rem;}',
      '.bonfyre-artifact-body{font-size:.78rem;line-height:1.5;color:#e5e7eb;max-height:320px;overflow:auto;white-space:pre-wrap;}',
      '.bonfyre-artifact-body p{margin-bottom:.65rem;}',
      '.bonfyre-artifact-body h2,.bonfyre-artifact-body h3,.bonfyre-artifact-body h4{color:#f8fafc;margin:.55rem 0 .3rem;}',
      '.bonfyre-artifact-body ul{padding-left:1rem;margin:.35rem 0 .65rem;}',
      '.bonfyre-dataset-overview{margin-bottom:1rem;padding:1rem;border:1px solid #223047;border-radius:12px;background:linear-gradient(180deg,#0f1520,#0b1016);}',
      '.bonfyre-dataset-head{display:flex;justify-content:space-between;gap:1rem;flex-wrap:wrap;align-items:flex-start;margin-bottom:.75rem;}',
      '.bonfyre-dataset-head h3{font-size:1rem;color:#f8fafc;margin-bottom:.2rem;}',
      '.bonfyre-dataset-copy{font-size:.82rem;color:#a3a3a3;line-height:1.5;max-width:58rem;}',
      '.bonfyre-dataset-stats{display:flex;gap:.55rem;flex-wrap:wrap;}',
      '.bonfyre-dataset-stat{min-width:112px;padding:.65rem .75rem;border-radius:10px;border:1px solid #2f3b4b;background:#0b111a;}',
      '.bonfyre-dataset-stat strong{display:block;color:#ff6600;font-size:1rem;margin-bottom:.18rem;}',
      '.bonfyre-dataset-stat span{display:block;font-size:.72rem;color:#9ca3af;}',
      '.bonfyre-dataset-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:.75rem;}',
      '.bonfyre-dataset-panel{padding:.8rem;border-radius:10px;border:1px solid #223047;background:#0b111a;}',
      '.bonfyre-dataset-panel h4{font-size:.78rem;letter-spacing:.08em;text-transform:uppercase;color:#93c5fd;margin-bottom:.55rem;}',
      '.bonfyre-dataset-panel p{font-size:.78rem;color:#a3a3a3;line-height:1.5;margin-bottom:.55rem;}',
      '.bonfyre-query-pills{display:flex;gap:.45rem;flex-wrap:wrap;}',
      '.bonfyre-query-pill{padding:.35rem .6rem;border-radius:999px;border:1px solid #334155;background:#111827;color:#e5e7eb;font-size:.75rem;cursor:pointer;}',
      '.bonfyre-query-pill:hover{border-color:#ff6600;color:#ff6600;}',
      '.bonfyre-board-limit{margin:0 0 1rem;padding:.7rem .85rem;border:1px solid #223047;border-radius:10px;background:#0b111a;display:flex;justify-content:space-between;align-items:center;gap:.75rem;flex-wrap:wrap;}',
      '.bonfyre-board-limit-copy{font-size:.78rem;color:#a3a3a3;}',
      '.bonfyre-board-limit button{padding:.42rem .7rem;border-radius:6px;border:1px solid #38465d;background:rgba(255,255,255,.02);color:#e5e7eb;font-size:.76rem;font-weight:600;cursor:pointer;}',
      '.bonfyre-board-limit button:hover{border-color:#ff6600;color:#ff6600;}'
    ].join('');
    document.head.appendChild(style);
  }

  function openModuleReference(name) {
    var token = normalizeToken(name);
    if (!token) return;
    var url = repoMap[token];
    if (!url) {
      var query = token
        .split('-')
        .map(function(part) { return part ? part.charAt(0).toUpperCase() + part.slice(1) : ''; })
        .join('');
      url = 'https://github.com/Nickgonzales76017/bonfyre-oss/search?q=' + encodeURIComponent(query);
    }
    global.open(url, '_blank', 'noopener');
  }

  function ensurePreviewNodes() {
    var panel = document.getElementById('wasmPreview') || document.getElementById('intakePreview');
    var content = document.getElementById('previewContent') || document.getElementById('intakeContent');
    if (!panel || !content) return null;
    var actions = panel.querySelector('.bonfyre-preview-actions');
    if (!actions) {
      actions = document.createElement('div');
      actions.className = 'bonfyre-preview-actions';
      content.insertAdjacentElement('afterend', actions);
    }
    var searchRoot = panel.querySelector('.bonfyre-demo-search');
    if (!searchRoot) {
      searchRoot = document.createElement('div');
      searchRoot.className = 'bonfyre-demo-search';
      searchRoot.innerHTML = [
        '<div class="bonfyre-demo-search-copy"></div>',
        '<input class="bonfyre-demo-search-input" type="search" placeholder="Search this corpus">',
        '<div class="bonfyre-demo-search-results"></div>'
      ].join('');
      actions.insertAdjacentElement('afterend', searchRoot);
    }
    return {
      panel: panel,
      content: content,
      actions: actions,
      searchRoot: searchRoot,
      searchCopy: searchRoot.querySelector('.bonfyre-demo-search-copy'),
      searchInput: searchRoot.querySelector('.bonfyre-demo-search-input'),
      searchResults: searchRoot.querySelector('.bonfyre-demo-search-results')
    };
  }

  function hideSearch(nodes) {
    if (nodes && nodes.searchRoot) nodes.searchRoot.style.display = 'none';
  }

  function clearDetail(nodes) {
    if (!nodes) return;
    var detail = nodes.panel.querySelector('.bonfyre-detail-card');
    if (detail) detail.remove();
    var artifact = nodes.panel.querySelector('.bonfyre-artifact-view');
    if (artifact) artifact.remove();
  }

  function setPreviewHtml(html, link) {
    var nodes = ensurePreviewNodes();
    if (!nodes) return false;
    clearDetail(nodes);
    nodes.panel.style.display = 'block';
    nodes.content.innerHTML = html;
    nodes.actions.innerHTML = '';
    hideSearch(nodes);
    if (link && link.href) {
      var anchor = document.createElement('a');
      anchor.className = 'bonfyre-preview-link';
      anchor.href = String(link.href);
      anchor.target = '_blank';
      anchor.rel = 'noreferrer noopener';
      anchor.textContent = link.label || 'Open artifact';
      nodes.actions.appendChild(anchor);
    }
    return true;
  }

  function setPreviewState(text, link) {
    var nodes = ensurePreviewNodes();
    if (!nodes) return false;
    clearDetail(nodes);
    nodes.panel.style.display = 'block';
    nodes.content.textContent = String(text || '');
    nodes.actions.innerHTML = '';
    hideSearch(nodes);
    if (link && link.href) {
      var anchor = document.createElement('a');
      anchor.className = 'bonfyre-preview-link';
      anchor.href = String(link.href);
      anchor.target = '_blank';
      anchor.rel = 'noreferrer noopener';
      anchor.textContent = link.label || 'Open artifact';
      nodes.actions.appendChild(anchor);
    }
    return true;
  }

  function setPreviewText(text) {
    var nodes = ensurePreviewNodes();
    if (!nodes) return false;
    clearDetail(nodes);
    nodes.actions.innerHTML = '';
    nodes.panel.style.display = 'block';
    nodes.content.textContent = String(text || '');
    hideSearch(nodes);
    return true;
  }

  async function openArtifactInPreview(note, link) {
    if (!link || !link.href) return false;
    var raw = await fetchText(link.href);
    if (!raw) return setPreviewState(note || 'This output is available for this demo item.', link);
    var body = formatArtifactText(raw, link.href);
    var htmlBody = /\.md($|\?)/.test(link.href || '') ? '<div class="bonfyre-artifact-body"><p>' + markdownToHtml(body) + '</p></div>' : '<div class="bonfyre-artifact-body">' + escapeHtml(body) + '</div>';
    return setPreviewHtml(
      '<div class="bonfyre-artifact-view">' +
        '<h4>' + escapeHtml(link.label || 'Artifact preview') + '</h4>' +
        (note ? '<div class="bonfyre-detail-copy">' + escapeHtml(note) + '</div>' : '') +
        htmlBody +
      '</div>',
      link
    );
  }

  function buildDetailCard(item) {
    var sourceStrip = buildSourceStrip(item);
    return '<div class="bonfyre-detail-card">' +
      '<h4>' + escapeHtml(item.file || 'Demo record') + '</h4>' +
      '<div class="bonfyre-detail-meta">' + escapeHtml(item.time || 'Reference corpus') + '</div>' +
      sourceStrip +
      '<div class="bonfyre-detail-copy">' + escapeHtml(item.whyItMatters || item.brief || '') + '</div>' +
      '<div class="bonfyre-detail-tags">' + (item.tags || []).slice(0, 5).map(function(tag) {
        return '<span class="bonfyre-detail-tag">' + escapeHtml(tag) + '</span>';
      }).join('') + '</div>' +
    '</div>';
  }

  function getSourceLinks(item) {
    if (!item) return [];
    var links = [];
    if (item.sourceUrl) {
      links.push({
        href: item.sourceUrl,
        label: item.sourceLabel || 'Open source',
        title: item.sourceTitle || '',
        publisher: item.publisher || '',
        license: item.license || ''
      });
    }
    if (Array.isArray(item.sourceLinks)) {
      for (var i = 0; i < item.sourceLinks.length; i++) {
        if (item.sourceLinks[i] && item.sourceLinks[i].href) links.push(item.sourceLinks[i]);
      }
    }
    return links.filter(function(link, index, arr) {
      return arr.findIndex(function(other) {
        return other && link && other.href === link.href && (other.label || '') === (link.label || '');
      }) === index;
    });
  }

  function buildSourceSummary(item) {
    var parts = [];
    if (item && item.sourceTitle) parts.push(item.sourceTitle);
    if (item && item.publisher) parts.push(item.publisher);
    if (item && item.license) parts.push(item.license);
    return parts.join(' · ');
  }

  function buildSourceStrip(item) {
    var links = getSourceLinks(item);
    if (!links.length) return '';
    var summary = buildSourceSummary(item);
    var copy = item.sourceCopy || 'Trace this card back to the public origin Bonfyre processed.';
    return '<div class="bonfyre-source-strip" data-bonfyre-source-strip="1">' +
      '<div class="bonfyre-source-kicker">Provenance</div>' +
      '<div class="bonfyre-source-copy">' + escapeHtml(copy) + (summary ? ' <span style="color:#9ca3af;">' + escapeHtml(summary) + '</span>' : '') + '</div>' +
      '<div class="bonfyre-source-actions">' + links.map(function(link) {
        return '<a class="bonfyre-source-link" href="' + escapeHtml(link.href) + '" target="_blank" rel="noreferrer noopener">' + escapeHtml(link.label || 'Open public source') + '</a>';
      }).join('') + '</div>' +
    '</div>';
  }

  function scoreItemForQuery(item, query) {
    var hay = [
      item.file,
      item.brief,
      (item.tags || []).join(' '),
      item.searchSummary,
      item.whyItMatters,
      item.compositionNote
    ].join(' ').toLowerCase();
    if (!query) return 1;
    if (hay.indexOf(query) === -1) return 0;
    var score = 1;
    if (String(item.file || '').toLowerCase().indexOf(query) !== -1) score += 3;
    if ((item.tags || []).join(' ').toLowerCase().indexOf(query) !== -1) score += 2;
    if (String(item.brief || '').toLowerCase().indexOf(query) !== -1) score += 1;
    return score;
  }

  function renderSearchResults(items, query) {
    var ranked = cloneItems(items)
      .map(function(item) { return { item: item, score: scoreItemForQuery(item, query) }; })
      .filter(function(entry) { return entry.score > 0; })
      .sort(function(a, b) { return b.score - a.score; })
      .slice(0, 6);
    if (!ranked.length) return '<div class="bonfyre-demo-search-empty">No matching records yet.</div>';
    return ranked.map(function(entry) {
      var item = entry.item;
      return '<button type="button" class="bonfyre-demo-result" data-demo-open="' + escapeHtml(item.id) + '">' +
        '<strong>' + escapeHtml(item.file || 'Corpus item') + '</strong>' +
        '<span>' + escapeHtml(item.searchSummary || item.brief || '') + '</span>' +
        '<em>' + escapeHtml((item.tags || []).slice(0, 3).join(' · ')) + '</em>' +
      '</button>';
    }).join('');
  }

  function openSearchExperience(item, label, items) {
    var nodes = ensurePreviewNodes();
    if (!nodes) return false;
    clearDetail(nodes);
    nodes.panel.style.display = 'block';
    nodes.content.textContent = String((item && item.outputNotes && item.outputNotes[label]) || 'Search across the corpus to see why this output matters.');
    nodes.actions.innerHTML = '';
    nodes.searchRoot.style.display = 'block';
    nodes.searchCopy.textContent = item.searchIntro || 'Multiple records make search, reuse, and compression visible instead of theoretical.';
    nodes.searchInput.value = '';
    nodes.searchResults.innerHTML = renderSearchResults(items, '');
    nodes.searchInput.oninput = function() {
      nodes.searchResults.innerHTML = renderSearchResults(items, String(nodes.searchInput.value || '').trim().toLowerCase());
    };
    nodes.searchResults.onclick = function(event) {
      var button = event.target.closest('[data-demo-open]');
      if (!button) return;
      var targetId = button.getAttribute('data-demo-open');
      var target = cloneItems(items).find(function(entry) { return String(entry.id) === String(targetId); });
      if (!target) return;
      clearDetail(nodes);
      nodes.content.textContent = String((item && item.outputNotes && item.outputNotes[label]) || 'Search across the corpus to see why this output matters.');
      nodes.searchRoot.insertAdjacentHTML('afterend', buildDetailCard(target));
    };
    return true;
  }

  function collectTopTags(items) {
    var counts = {};
    for (var i = 0; i < items.length; i++) {
      var tags = Array.isArray(items[i].tags) ? items[i].tags : [];
      for (var j = 0; j < tags.length; j++) {
        var raw = String(tags[j] || '').trim();
        var key = normalizeToken(raw);
        if (!key || key === 'demo' || key === 'flagged' || key === 'urgent' || key === 'all') continue;
        counts[raw] = (counts[raw] || 0) + 1;
      }
    }
    return Object.keys(counts)
      .map(function(tag) { return { tag: tag, count: counts[tag] }; })
      .sort(function(a, b) { return b.count - a.count; })
      .slice(0, 8);
  }

  function buildSuggestedQueries(items, topTags) {
    var queries = [];
    for (var i = 0; i < topTags.length && queries.length < 4; i++) {
      queries.push(topTags[i].tag);
    }
    for (var j = 0; j < items.length && queries.length < 6; j++) {
      var summary = String(items[j].searchSummary || '').split(',')[0].trim();
      if (summary && queries.indexOf(summary) === -1) queries.push(summary);
    }
    return queries.slice(0, 6);
  }

  function renderDatasetOverview(items) {
    if (!items.length) return '';
    var flagged = 0;
    var outputs = {};
    for (var i = 0; i < items.length; i++) {
      if (items[i].flagged) flagged += 1;
      var list = Array.isArray(items[i].outputs) ? items[i].outputs : [];
      for (var j = 0; j < list.length; j++) outputs[list[j]] = (outputs[list[j]] || 0) + 1;
    }
    var topTags = collectTopTags(items);
    var suggestions = buildSuggestedQueries(items, topTags);
    var topOutputs = Object.keys(outputs)
      .map(function(name) { return { name: name, count: outputs[name] }; })
      .sort(function(a, b) { return b.count - a.count; })
      .slice(0, 4);
    var lead = items[0] || {};
    return '<section class="bonfyre-dataset-overview" data-bonfyre-overview="1">' +
      '<div class="bonfyre-dataset-head">' +
        '<div>' +
          '<h3>Explore The Reference Corpus</h3>' +
          '<div class="bonfyre-dataset-copy">' + escapeHtml(lead.whyItMatters || 'These reference records are here to show search, reuse, and branching outputs across a larger corpus, not just on one isolated item.') + '</div>' +
        '</div>' +
        '<div class="bonfyre-dataset-stats">' +
          '<div class="bonfyre-dataset-stat"><strong>' + String(items.length) + '</strong><span>reference records</span></div>' +
          '<div class="bonfyre-dataset-stat"><strong>' + String(flagged) + '</strong><span>flagged or high-signal items</span></div>' +
          '<div class="bonfyre-dataset-stat"><strong>' + String(topTags.length) + '</strong><span>repeatable themes</span></div>' +
        '</div>' +
      '</div>' +
      '<div class="bonfyre-dataset-grid">' +
        '<div class="bonfyre-dataset-panel">' +
          '<h4>Try These Searches</h4>' +
          '<p>Start with a repeated topic and Bonfyre will show how the same system turns many records into something searchable and reusable.</p>' +
          '<div class="bonfyre-query-pills">' + suggestions.map(function(query) {
            return '<button type="button" class="bonfyre-query-pill" data-demo-query="' + escapeHtml(query) + '">' + escapeHtml(query) + '</button>';
          }).join('') + '</div>' +
        '</div>' +
        '<div class="bonfyre-dataset-panel">' +
          '<h4>Recurring Themes</h4>' +
          '<p>These are the strongest clusters in the corpus right now.</p>' +
          '<div class="bonfyre-query-pills">' + topTags.map(function(entry) {
            return '<button type="button" class="bonfyre-query-pill" data-demo-query="' + escapeHtml(entry.tag) + '">' + escapeHtml(entry.tag) + ' · ' + String(entry.count) + '</button>';
          }).join('') + '</div>' +
        '</div>' +
        '<div class="bonfyre-dataset-panel">' +
          '<h4>What The Corpus Produces</h4>' +
          '<p>The point is not just more files. It is more surfaces from the same underlying records.</p>' +
          '<div class="bonfyre-query-pills">' + topOutputs.map(function(entry) {
            return '<span class="bonfyre-query-pill" style="cursor:default;">' + escapeHtml(entry.name) + ' · ' + String(entry.count) + '</span>';
          }).join('') + '</div>' +
        '</div>' +
      '</div>' +
    '</section>';
  }

  function setSearchExperienceQuery(item, label, items, query) {
    var nodes = ensurePreviewNodes();
    if (!nodes) return false;
    clearDetail(nodes);
    nodes.panel.style.display = 'block';
    nodes.content.textContent = String((item && item.outputNotes && item.outputNotes[label]) || 'Search across the corpus to see why this output matters.');
    nodes.actions.innerHTML = '';
    nodes.searchRoot.style.display = 'block';
    nodes.searchCopy.textContent = item.searchIntro || 'Multiple records make search, reuse, and compression visible instead of theoretical.';
    nodes.searchInput.value = query || '';
    nodes.searchResults.innerHTML = renderSearchResults(items, String(query || '').trim().toLowerCase());
    nodes.searchInput.oninput = function() {
      nodes.searchResults.innerHTML = renderSearchResults(items, String(nodes.searchInput.value || '').trim().toLowerCase());
    };
    nodes.searchResults.onclick = function(event) {
      var button = event.target.closest('[data-demo-open]');
      if (!button) return;
      var targetId = button.getAttribute('data-demo-open');
      var target = cloneItems(items).find(function(entry) { return String(entry.id) === String(targetId); });
      if (!target) return;
      clearDetail(nodes);
      nodes.content.textContent = String((item && item.outputNotes && item.outputNotes[label]) || 'Search across the corpus to see why this output matters.');
      nodes.searchRoot.insertAdjacentHTML('afterend', buildDetailCard(target));
    };
    return true;
  }

  function applyBoardEnhancements(options) {
    var container = document.getElementById('boardContent');
    if (!container) return;
    var items = cloneItems(options.getItems());
    var cards = Array.prototype.slice.call(container.querySelectorAll('.card[data-item-id]'));
    var existingOverview = container.querySelector('[data-bonfyre-overview="1"]');
    if (!cards.length) {
      if (existingOverview) existingOverview.remove();
      var oldLimit = container.querySelector('[data-bonfyre-board-limit="1"]');
      if (oldLimit) oldLimit.remove();
      return;
    }

    var stamp = items.map(function(item) { return item && item.id; }).join('|');
    if (!existingOverview || existingOverview.getAttribute('data-bonfyre-stamp') !== stamp) {
      if (existingOverview) existingOverview.remove();
      container.insertAdjacentHTML('afterbegin', renderDatasetOverview(items));
      existingOverview = container.querySelector('[data-bonfyre-overview="1"]');
      if (existingOverview) existingOverview.setAttribute('data-bonfyre-stamp', stamp);
      cards = Array.prototype.slice.call(container.querySelectorAll('.card[data-item-id]'));
    }

    var limitNode = container.querySelector('[data-bonfyre-board-limit="1"]');
    var boardKey = options.storeKey + '_expand_cards';
    var expanded = localStorage.getItem(boardKey) === '1';
    var visibleLimit = 24;
    var hasOverflow = cards.length > visibleLimit;

    for (var i = 0; i < cards.length; i++) {
      cards[i].style.display = (!hasOverflow || expanded || i < visibleLimit) ? '' : 'none';
    }

    for (var j = 0; j < cards.length; j++) {
      var card = cards[j];
      var existingSource = card.querySelector('[data-bonfyre-source-strip="1"]');
      if (existingSource) existingSource.remove();
      var itemId = card.getAttribute('data-item-id');
      var item = null;
      for (var k = 0; k < items.length; k++) {
        if (items[k] && String(items[k].id) === String(itemId)) {
          item = items[k];
          break;
        }
      }
      var sourceHtml = buildSourceStrip(item);
      if (!sourceHtml) continue;
      var meta = card.querySelector('.card-meta');
      if (meta) meta.insertAdjacentHTML('afterend', sourceHtml);
      else card.insertAdjacentHTML('afterbegin', sourceHtml);
    }

    if (!hasOverflow) {
      if (limitNode) limitNode.remove();
      return;
    }

    var copy = expanded
      ? 'Showing all ' + String(cards.length) + ' records in this view.'
      : 'Showing the first ' + String(visibleLimit) + ' records so the patterns stay readable. Search or expand when you want the full corpus.';
    var buttonLabel = expanded ? 'Show Fewer Records' : 'Show All ' + String(cards.length) + ' Records';

    if (!limitNode) {
      var overviewNode = container.querySelector('[data-bonfyre-overview="1"]');
      var insertAfter = overviewNode || cards[0];
      if (insertAfter) {
        insertAfter.insertAdjacentHTML('afterend',
          '<div class="bonfyre-board-limit" data-bonfyre-board-limit="1">' +
            '<div class="bonfyre-board-limit-copy"></div>' +
            '<button type="button" data-bonfyre-board-toggle="1"></button>' +
          '</div>'
        );
      }
      limitNode = container.querySelector('[data-bonfyre-board-limit="1"]');
    }

    if (limitNode) {
      var copyNode = limitNode.querySelector('.bonfyre-board-limit-copy');
      var buttonNode = limitNode.querySelector('[data-bonfyre-board-toggle="1"]');
      if (copyNode) copyNode.textContent = copy;
      if (buttonNode) {
        buttonNode.textContent = buttonLabel;
        buttonNode.onclick = function() {
          var next = localStorage.getItem(boardKey) === '1' ? '0' : '1';
          localStorage.setItem(boardKey, next);
          applyBoardEnhancements(options);
        };
      }
    }
  }

  function activateMatchingFilter(rawText) {
    var text = normalizeToken(rawText);
    if (!text) return false;
    var selectors = ['.filter-item', '.area-item'];
    for (var i = 0; i < selectors.length; i++) {
      var nodes = document.querySelectorAll(selectors[i]);
      for (var j = 0; j < nodes.length; j++) {
        var node = nodes[j];
        var key = normalizeToken(node.dataset.filter || node.dataset.area || node.textContent);
        if (key === text) {
          node.click();
          return true;
        }
      }
    }
    var fallback = document.querySelector('.filter-item[data-filter="flagged"], .area-item[data-area="urgent"]');
    if (fallback) {
      fallback.click();
      return true;
    }
    return false;
  }

  function getItemByCard(options, chip) {
    var card = chip.closest('[data-item-id]');
    if (!card) return null;
    var itemId = card.getAttribute('data-item-id');
    var items = cloneItems(options.getItems());
    for (var i = 0; i < items.length; i++) {
      if (items[i] && String(items[i].id) === String(itemId)) return items[i];
    }
    return null;
  }

  function getMappedAction(item, bucket, label) {
    if (!item || !item[bucket]) return null;
    return item[bucket][label] || item[bucket][normalizeToken(label)] || null;
  }

  function itemHasToken(item, token) {
    if (!item || !token) return false;
    if (token === 'flagged' || token === 'urgent') return !!item.flagged;
    var tags = Array.isArray(item.tags) ? item.tags : [];
    for (var i = 0; i < tags.length; i++) {
      if (normalizeToken(tags[i]) === token) return true;
    }
    var category = normalizeToken(item.category || item.area || item.kind || '');
    return category === token;
  }

  function refreshCounts(options) {
    var items = cloneItems(options.getItems());
    var filterNodes = document.querySelectorAll('.filter-item');
    for (var i = 0; i < filterNodes.length; i++) {
      var filterNode = filterNodes[i];
      var key = normalizeToken(filterNode.dataset.filter || filterNode.textContent);
      var countNode = filterNode.querySelector('.filter-count');
      if (!countNode) continue;
      if (key === 'all') {
        countNode.textContent = String(items.length);
        continue;
      }
      var total = 0;
      for (var j = 0; j < items.length; j++) {
        if (itemHasToken(items[j], key)) total += 1;
      }
      countNode.textContent = String(total);
    }

    var areaNodes = document.querySelectorAll('.area-item');
    for (var k = 0; k < areaNodes.length; k++) {
      var areaNode = areaNodes[k];
      var areaKey = normalizeToken(areaNode.dataset.area || areaNode.textContent);
      var areaCountNode = areaNode.querySelector('.area-count');
      if (!areaCountNode) continue;
      if (areaKey === 'all') {
        areaCountNode.textContent = String(items.length);
        continue;
      }
      var areaTotal = 0;
      for (var m = 0; m < items.length; m++) {
        if (itemHasToken(items[m], areaKey)) areaTotal += 1;
      }
      areaCountNode.textContent = String(areaTotal);
    }
  }

  function bindInteractions(options) {
    if (document.body && document.body.dataset.bonfyreDemoBootBound === '1') return;
    if (document.body) document.body.dataset.bonfyreDemoBootBound = '1';

    document.addEventListener('click', function(event) {
      var moduleChip = event.target.closest('.operator-chip,.swap-chip');
      if (moduleChip) {
        event.preventDefault();
        var item = getItemByCard(options, moduleChip);
        var moduleLabel = moduleChip.textContent.trim();
        var moduleAction = getMappedAction(item, 'moduleActions', moduleLabel);
        var moduleNote = item && item.moduleNotes && (item.moduleNotes[moduleLabel] || item.moduleNotes[normalizeToken(moduleLabel)]);
        if (moduleAction && moduleAction.href) {
          openArtifactInPreview(moduleAction.note || moduleNote || (moduleLabel + ' is available for this demo item.'), {
            href: moduleAction.href,
            label: moduleAction.label || 'Open stage artifact'
          });
          return;
        }
        if (moduleNote) {
          setPreviewState(moduleNote, {
            href: repoMap[normalizeToken(moduleLabel)] || repoMap.bonfyre,
            label: 'Open module repo'
          });
          return;
        }
        openModuleReference(moduleLabel);
        return;
      }

      var outputChip = event.target.closest('.output-chip');
      if (outputChip) {
        event.preventDefault();
        var item = getItemByCard(options, outputChip);
        var label = outputChip.textContent.trim();
        var outputLink = getMappedAction(item, 'outputLinks', label);
        var allItems = cloneItems(options.getItems());
        if (item && Array.isArray(item.searchOutputs) && item.searchOutputs.indexOf(label) !== -1) {
          if (openSearchExperience(item, label, allItems)) return;
        }
        if (item && item.outputNotes && item.outputNotes[label]) {
          if (outputLink && outputLink.href) {
            openArtifactInPreview(item.outputNotes[label], {
              href: outputLink.href || outputLink,
              label: outputLink.label || 'Open emitted artifact'
            });
            return;
          }
          if (setPreviewState(item.outputNotes[label], null)) return;
        }
        if (outputLink) {
          if (openArtifactInPreview(label + ' is available for this demo item.', {
            href: outputLink.href || outputLink,
            label: outputLink.label || 'Open emitted artifact'
          })) return;
        }
      }

      var tagChip = event.target.closest('.tag');
      if (tagChip) {
        event.preventDefault();
        activateMatchingFilter(tagChip.textContent);
        setTimeout(function() { refreshCounts(options); }, 0);
        return;
      }

      var filterNode = event.target.closest('.filter-item,.area-item');
      if (filterNode) {
        setTimeout(function() {
          refreshCounts(options);
          applyBoardEnhancements(options);
        }, 0);
      }

      var queryNode = event.target.closest('[data-demo-query]');
      if (queryNode) {
        event.preventDefault();
        var query = queryNode.getAttribute('data-demo-query') || queryNode.textContent || '';
        var queryItems = cloneItems(options.getItems());
        var seed = queryItems[0] || {};
        setSearchExperienceQuery(seed, (seed.searchOutputs && seed.searchOutputs[0]) || 'search', queryItems, query);
      }
    });
  }

  async function applyDemoState(options, hidden) {
    var current = cloneItems(options.getItems());
    current = current.filter(function(item) { return !(item && item.demo); });

    if (!hidden) {
      var demo = await fetchJson(options.demoPath);
      if (Array.isArray(demo)) {
        for (var i = demo.length - 1; i >= 0; i--) current.unshift(demo[i]);
      } else if (demo) {
        current.unshift(demo);
      }
    }

    options.setItems(current);
    localStorage.setItem(options.storeKey, JSON.stringify(current));
    options.renderBoard();
    applyBoardEnhancements(options);
    refreshCounts(options);
  }

  function init(options) {
    if (!options || !options.getItems || !options.setItems || !options.renderBoard || !options.storeKey || !options.demoPath) {
      return;
    }

    injectStyles();
    bindInteractions(options);

    var button = document.getElementById(options.buttonId || 'toggleDemoBtn');
    if (!button) return;

    var hiddenKey = options.hiddenKey || (options.storeKey + '_hide_demo');
    var hidden = localStorage.getItem(hiddenKey) === '1';

    function updateButton() {
      button.textContent = hidden ? 'Show Corpus' : 'Hide Corpus';
    }

    var observerTarget = document.getElementById('boardContent');
    if (observerTarget && !observerTarget.dataset.bonfyreObserverBound) {
      observerTarget.dataset.bonfyreObserverBound = '1';
      var observer = new MutationObserver(function() {
        applyBoardEnhancements(options);
      });
      observer.observe(observerTarget, { childList: true, subtree: false });
    }

    button.addEventListener('click', function() {
      hidden = !hidden;
      localStorage.setItem(hiddenKey, hidden ? '1' : '0');
      updateButton();
      applyDemoState(options, hidden);
    });

    updateButton();
    applyDemoState(options, hidden);
    setTimeout(function() {
      refreshCounts(options);
      applyBoardEnhancements(options);
    }, 0);
  }

  global.BonfyreDemoBoot = {
    init: init
  };
})(window);
