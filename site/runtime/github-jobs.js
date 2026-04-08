(function(global) {
  function persistList(storageKey, items) {
    localStorage.setItem(storageKey, JSON.stringify(items || []));
  }

  function updateItem(items, id, mutator) {
    return (items || []).map(function(item) {
      if (!item || item.id !== id) return item;
      var next = Object.assign({}, item);
      mutator(next);
      return next;
    });
  }

  function upsertAndPersist(storageKey, items, item) {
    var next = (global.BonfyreGitHubIntake && global.BonfyreGitHubIntake.upsertItem)
      ? global.BonfyreGitHubIntake.upsertItem(items, item)
      : [item].concat((items || []).filter(function(existing) { return existing.id !== item.id; }));
    persistList(storageKey, next);
    return next;
  }

  function updateAndPersist(storageKey, items, id, mutator) {
    var next = updateItem(items, id, mutator);
    persistList(storageKey, next);
    return next;
  }

  function statusLabel(status) {
    if (status === 'complete') return '✓ Complete';
    if (status === 'processing') return '⏳ Processing…';
    return '↗ Ready for GitHub Upload';
  }

  function statusColor(status) {
    if (status === 'complete') return 'var(--green)';
    if (status === 'processing') return 'var(--yellow)';
    return '#818cf8';
  }

  function buildReadyMessage(appName) {
    return (
      'GitHub-native launch ready.\n\n' +
      '1. Download the intake-ready copy.\n' +
      '2. Upload it in the repo input folder using GitHub web UI.\n' +
      '3. Commit on the configured branch.\n\n' +
      appName + ' will poll the public artifact outputs after that commit lands.'
    );
  }

  function showIntakePreview(options) {
    var panel = document.getElementById(options.panelId || 'intakePreview');
    var content = document.getElementById(options.contentId || 'intakeContent');
    var downloadLink = document.getElementById(options.downloadId || 'downloadPrepared');
    var uploadLink = document.getElementById(options.uploadId || 'openUploadFolder');

    if (!panel || !content || !downloadLink || !uploadLink) return;

    panel.style.display = 'block';
    content.textContent = global.BonfyreGitHubIntake.buildIntakeMessage({
      preparedName: options.intake.file,
      routeLabel: options.routeLabel,
      targetPath: options.intake.targetPath,
      branch: options.branch
    });
    downloadLink.href = options.preparedDownload.href;
    downloadLink.download = options.preparedDownload.download;
    uploadLink.href = options.intake.uploadFolderUrl;
  }

  function markUploaded(options) {
    if (!options.activeIntake) return options.items || [];

    options.updateStep('commit', 'done');
    options.updateStep(options.processingStep || 'process', 'active');

    var nextItems = updateAndPersist(
      options.storageKey,
      options.items || [],
      options.activeIntake.id,
      function(item) {
        item.status = 'processing';
      }
    );

    if (options.renderBoard) options.renderBoard();

    if (!options.activeIntake.pollingStarted) {
      options.activeIntake.pollingStarted = true;
      if (options.onStartPoll) options.onStartPoll(options.activeIntake);
    }

    return nextItems;
  }

  function touchLastUpdate(elementId) {
    var el = document.getElementById(elementId || 'lastUpdate');
    if (el) el.textContent = 'Last update: ' + new Date().toLocaleTimeString();
  }

  function pollArtifacts(options) {
    var attempts = 0;
    var maxAttempts = options.maxAttempts || 60;
    var intervalMs = options.intervalMs || 5000;
    var stopped = false;

    var timer = setInterval(async function() {
      if (stopped) return;
      attempts += 1;
      if (attempts > maxAttempts) {
        clearInterval(timer);
        return;
      }

      try {
        await options.onPoll({
          attempt: attempts,
          stop: function() {
            stopped = true;
            clearInterval(timer);
          }
        });
      } catch (err) {
        if (options.onError) options.onError(err);
      }
    }, intervalMs);

    return timer;
  }

  global.BonfyreGitHubJobs = {
    buildReadyMessage: buildReadyMessage,
    markUploaded: markUploaded,
    persistList: persistList,
    pollArtifacts: pollArtifacts,
    showIntakePreview: showIntakePreview,
    statusColor: statusColor,
    statusLabel: statusLabel,
    touchLastUpdate: touchLastUpdate,
    updateAndPersist: updateAndPersist,
    updateItem: updateItem,
    upsertAndPersist: upsertAndPersist
  };
})(window);
