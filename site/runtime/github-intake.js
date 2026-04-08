(function(global) {
  function createPreparedName(name) {
    var safe = String(name || 'upload').replace(/[^a-zA-Z0-9._-]/g, '_');
    var ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    return ts + '_' + safe;
  }

  function createPreparedDownload(file, preparedName) {
    return {
      href: URL.createObjectURL(file),
      download: preparedName
    };
  }

  function buildUploadFolderUrl(config) {
    var repo = config.repo;
    var branch = config.branch || 'main';
    var inputPath = config.inputPath || 'input';
    var subdir = config.subdir ? '/' + config.subdir.replace(/^\/+/, '') : '';
    return 'https://github.com/' + repo + '/tree/' + branch + '/' + inputPath + subdir;
  }

  async function fetchRawFile(config) {
    var url = 'https://raw.githubusercontent.com/' + config.repo + '/' + config.branch + '/' + config.path;
    var resp = await fetch(url, { cache: 'no-store' });
    if (!resp.ok) return null;
    return resp.text();
  }

  function upsertItem(items, item) {
    return [item].concat((items || []).filter(function(existing) { return existing.id !== item.id; }));
  }

  function parseBriefPayload(raw) {
    if (!raw) return raw;
    try {
      var parsed = JSON.parse(raw);
      return parsed.summary || parsed.brief || raw;
    } catch (_) {
      return raw;
    }
  }

  function isTextLikeFile(file) {
    return !!(file && (String(file.type || '').startsWith('text/') || /\.(c|h|py|js|ts|go|rs|md|txt)$/i.test(file.name || '')));
  }

  function buildIntakeMessage(options) {
    var lines = [
      'Prepared file: ' + options.preparedName,
      'Repo path: ' + options.targetPath
    ];
    if (options.routeLabel) lines.splice(1, 0, 'Route: ' + options.routeLabel);
    lines.push(
      '',
      'Safer launch path:',
      '1. Download the intake-ready copy from this panel.',
      '2. Open the GitHub input folder.',
      '3. Use GitHub\'s own "Add file" -> "Upload files" flow.',
      '4. Commit to ' + options.branch + ' to trigger Bonfyre Actions.',
      '',
      'This keeps write auth inside GitHub instead of storing a PAT in the browser.'
    );
    return lines.join('\n');
  }

  global.BonfyreGitHubIntake = {
    createPreparedName: createPreparedName,
    createPreparedDownload: createPreparedDownload,
    buildUploadFolderUrl: buildUploadFolderUrl,
    fetchRawFile: fetchRawFile,
    upsertItem: upsertItem,
    parseBriefPayload: parseBriefPayload,
    isTextLikeFile: isTextLikeFile,
    buildIntakeMessage: buildIntakeMessage
  };
})(window);
