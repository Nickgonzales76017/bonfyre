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
      '.bonfyre-artifact-view{margin-top:.75rem;padding:.85rem;border:1px solid #2f3b4b;border-radius:10px;background:#0d131d;}',
      '.bonfyre-artifact-view h4{font-size:.85rem;color:#ff6600;margin-bottom:.4rem;}',
      '.bonfyre-artifact-body{font-size:.78rem;line-height:1.5;color:#e5e7eb;max-height:320px;overflow:auto;white-space:pre-wrap;}',
      '.bonfyre-artifact-body p{margin-bottom:.65rem;}',
      '.bonfyre-artifact-body h2,.bonfyre-artifact-body h3,.bonfyre-artifact-body h4{color:#f8fafc;margin:.55rem 0 .3rem;}',
      '.bonfyre-artifact-body ul{padding-left:1rem;margin:.35rem 0 .65rem;}'
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
        '<input class="bonfyre-demo-search-input" type="search" placeholder="Search this demo dataset">',
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
    return '<div class="bonfyre-detail-card">' +
      '<h4>' + escapeHtml(item.file || 'Demo record') + '</h4>' +
      '<div class="bonfyre-detail-meta">' + escapeHtml(item.time || 'Demo dataset') + '</div>' +
      '<div class="bonfyre-detail-copy">' + escapeHtml(item.whyItMatters || item.brief || '') + '</div>' +
      '<div class="bonfyre-detail-tags">' + (item.tags || []).slice(0, 5).map(function(tag) {
        return '<span class="bonfyre-detail-tag">' + escapeHtml(tag) + '</span>';
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
    if (!ranked.length) return '<div class="bonfyre-demo-search-empty">No matching demo records yet.</div>';
    return ranked.map(function(entry) {
      var item = entry.item;
      return '<button type="button" class="bonfyre-demo-result" data-demo-open="' + escapeHtml(item.id) + '">' +
        '<strong>' + escapeHtml(item.file || 'Demo item') + '</strong>' +
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
    nodes.content.textContent = String((item && item.outputNotes && item.outputNotes[label]) || 'Search across the seeded demo dataset to see why this output matters.');
    nodes.actions.innerHTML = '';
    nodes.searchRoot.style.display = 'block';
    nodes.searchCopy.textContent = item.searchIntro || 'Multiple demo records make search, reuse, and compression visible instead of theoretical.';
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
      nodes.content.textContent = String((item && item.outputNotes && item.outputNotes[label]) || 'Search across the seeded demo dataset to see why this output matters.');
      nodes.searchRoot.insertAdjacentHTML('afterend', buildDetailCard(target));
    };
    return true;
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
        setTimeout(function() { refreshCounts(options); }, 0);
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
      button.textContent = hidden ? 'Show Demo Data' : 'Clear Demo Data';
    }

    button.addEventListener('click', function() {
      hidden = !hidden;
      localStorage.setItem(hiddenKey, hidden ? '1' : '0');
      updateButton();
      applyDemoState(options, hidden);
    });

    updateButton();
    applyDemoState(options, hidden);
    setTimeout(function() { refreshCounts(options); }, 0);
  }

  global.BonfyreDemoBoot = {
    init: init
  };
})(window);
