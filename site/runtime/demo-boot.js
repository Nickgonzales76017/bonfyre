(function(global) {
  var repoMap = {
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
      '.tag:hover,.operator-chip:hover,.output-chip:hover,.swap-chip:hover{transform:translateY(-1px);opacity:.92;border-color:#ff6600;}'
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

  function setPreviewText(text) {
    var panel = document.getElementById('wasmPreview') || document.getElementById('intakePreview');
    var content = document.getElementById('previewContent') || document.getElementById('intakeContent');
    if (!panel || !content) return false;
    panel.style.display = 'block';
    content.textContent = String(text || '');
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

  function bindInteractions(options) {
    if (document.body && document.body.dataset.bonfyreDemoBootBound === '1') return;
    if (document.body) document.body.dataset.bonfyreDemoBootBound = '1';

    document.addEventListener('click', function(event) {
      var moduleChip = event.target.closest('.operator-chip,.swap-chip');
      if (moduleChip) {
        event.preventDefault();
        openModuleReference(moduleChip.textContent);
        return;
      }

      var outputChip = event.target.closest('.output-chip');
      if (outputChip) {
        event.preventDefault();
        var item = getItemByCard(options, outputChip);
        var label = outputChip.textContent.trim();
        if (item && item.outputNotes && item.outputNotes[label]) {
          if (setPreviewText(item.outputNotes[label])) return;
        }
      }

      var tagChip = event.target.closest('.tag');
      if (tagChip) {
        event.preventDefault();
        activateMatchingFilter(tagChip.textContent);
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
  }

  global.BonfyreDemoBoot = {
    init: init
  };
})(window);
