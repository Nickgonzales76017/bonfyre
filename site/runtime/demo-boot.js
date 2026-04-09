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

  function injectStyles() {
    if (document.getElementById('bonfyre-demo-boot-styles')) return;
    var style = document.createElement('style');
    style.id = 'bonfyre-demo-boot-styles';
    style.textContent = [
      '.tag,.operator-chip,.output-chip,.swap-chip{cursor:pointer;transition:transform .12s ease,border-color .12s ease,opacity .12s ease;}',
      '.tag:hover,.operator-chip:hover,.output-chip:hover,.swap-chip:hover{transform:translateY(-1px);opacity:.92;border-color:#ff6600;}',
      '.bonfyre-preview-actions{display:flex;gap:.5rem;flex-wrap:wrap;margin-top:.75rem;}',
      '.bonfyre-preview-link{display:inline-flex;align-items:center;gap:.35rem;padding:.45rem .75rem;border-radius:6px;border:1px solid #333;color:#ff6600;text-decoration:none;font-size:.8rem;font-weight:600;background:rgba(255,102,0,.06);}'
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
    return { panel: panel, content: content, actions: actions };
  }

  function setPreviewState(text, link) {
    var nodes = ensurePreviewNodes();
    if (!nodes) return false;
    nodes.panel.style.display = 'block';
    nodes.content.textContent = String(text || '');
    nodes.actions.innerHTML = '';
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
    nodes.actions.innerHTML = '';
    nodes.panel.style.display = 'block';
    nodes.content.textContent = String(text || '');
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
          setPreviewState(moduleAction.note || moduleNote || (moduleLabel + ' is available for this demo item.'), {
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
        if (item && item.outputNotes && item.outputNotes[label]) {
          if (setPreviewState(item.outputNotes[label], outputLink ? {
            href: outputLink.href || outputLink,
            label: outputLink.label || 'Open emitted artifact'
          } : null)) return;
        }
        if (outputLink) {
          if (setPreviewState(label + ' is available for this demo item.', {
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
      if (demo) current.unshift(demo);
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
