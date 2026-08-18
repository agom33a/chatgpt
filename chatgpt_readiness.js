const CHATGPT_READINESS_PROBES = Object.freeze([
  { name: 'edge', url: 'https://chatgpt.com/cdn-cgi/trace', kind: 'edge' },
  { name: 'session', url: 'https://chatgpt.com/api/auth/session', kind: 'api' },
  { name: 'account', url: 'https://chatgpt.com/backend-api/me', kind: 'api' }
]);

function normalizeProbeResult(probe, resource, elapsedMs) {
  const status = Number(resource?.httpStatusCode || 0);
  const netError = Number(resource?.netError || 0);
  const success = resource?.success === true || status > 0;
  return {
    name: probe.name,
    kind: probe.kind,
    url: probe.url,
    ok: success && netError === 0,
    status,
    elapsedMs,
    netError,
    error: resource?.netErrorName || (!success ? 'no HTTP response' : null)
  };
}

function classifyChatGPTReadiness(results) {
  const completed = Array.isArray(results) ? results : [];
  const api = completed.filter(result => result.kind === 'api' && result.ok);
  const edge = completed.filter(result => result.kind === 'edge' && result.ok);
  const fastest = completed
    .filter(result => result.ok)
    .sort((left, right) => left.elapsedMs - right.elapsedMs)[0] || null;

  if (api.length) {
    return { state: 'api-ready', ready: true, fastest, apiReady: true, edgeReady: !!edge.length };
  }
  if (edge.length) {
    return { state: 'edge-only', ready: false, fastest, apiReady: false, edgeReady: true };
  }
  return { state: 'blocked', ready: false, fastest: null, apiReady: false, edgeReady: false };
}

function selectReadinessRecovery(readiness, genericInternetHealthy, alreadyReopenedPool = false) {
  if (readiness?.state === 'api-ready') return 'none';
  if (!genericInternetHealthy) return 'wait-generic-route';
  if (!alreadyReopenedPool) return 'reopen-connection-pool';
  return 'reject-chatgpt-endpoint';
}

async function runChatGPTReadinessBenchmark(cdp, options = {}) {
  const probes = options.probes || CHATGPT_READINESS_PROBES;
  const timeoutMs = options.timeoutMs || 15000;
  const createTarget = options.createTarget !== false;
  const targets = [];

  const runProbe = async probe => {
    const startedAt = Date.now();
    let targetId = null;
    let sessionId = null;
    try {
      if (createTarget) {
        const target = await cdp.send('Target.createTarget', {
          url: 'about:blank',
          background: true
        });
        targetId = target.targetId;
        targets.push(targetId);
        const attached = await cdp.send('Target.attachToTarget', {
          targetId,
          flatten: true
        });
        sessionId = attached.sessionId;
        await cdp.send('Network.enable', {}, sessionId);
        try {
          await cdp.send('Network.setBypassServiceWorker', { bypass: true }, sessionId);
        } catch {}
      }

      const response = await cdp.send('Network.loadNetworkResource', {
        url: `${probe.url}${probe.url.includes('?') ? '&' : '?'}readiness=${Date.now()}`,
        options: {
          disableCache: true,
          includeCredentials: true
        }
      }, sessionId, timeoutMs);
      return normalizeProbeResult(probe, response?.resource, Date.now() - startedAt);
    } catch (error) {
      return {
        name: probe.name,
        kind: probe.kind,
        url: probe.url,
        ok: false,
        status: 0,
        elapsedMs: Date.now() - startedAt,
        netError: 0,
        error: error.message
      };
    }
  };

  // Separate blank targets create simultaneous requests without depending on
  // the visible renderer, its service worker, document cache, or page state.
  const results = await Promise.all(probes.map(runProbe));
  await Promise.all(targets.map(targetId =>
    cdp.send('Target.closeTarget', { targetId }).catch(() => null)
  ));

  return {
    ...classifyChatGPTReadiness(results),
    elapsedMs: Math.max(0, ...results.map(result => result.elapsedMs)),
    results
  };
}

module.exports = {
  CHATGPT_READINESS_PROBES,
  normalizeProbeResult,
  classifyChatGPTReadiness,
  selectReadinessRecovery,
  runChatGPTReadinessBenchmark
};
