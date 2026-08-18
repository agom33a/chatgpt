const test = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeProbeResult,
  classifyChatGPTReadiness,
  selectReadinessRecovery,
  runChatGPTReadinessBenchmark
} = require('./chatgpt_readiness');

test('an API HTTP response proves ChatGPT-specific transport readiness', () => {
  const result = classifyChatGPTReadiness([
    { name: 'session', kind: 'api', ok: true, status: 401, elapsedMs: 90 },
    { name: 'edge', kind: 'edge', ok: false, status: 0, elapsedMs: 200 }
  ]);

  assert.equal(result.state, 'api-ready');
  assert.equal(result.ready, true);
  assert.equal(selectReadinessRecovery(result, true), 'none');
});

test('edge-only success reopens only the connection pool', () => {
  const result = classifyChatGPTReadiness([
    { name: 'edge', kind: 'edge', ok: true, status: 200, elapsedMs: 25 },
    { name: 'session', kind: 'api', ok: false, status: 0, elapsedMs: 500 }
  ]);

  assert.equal(result.state, 'edge-only');
  assert.equal(selectReadinessRecovery(result, true), 'reopen-connection-pool');
});

test('generic-only health reopens the pool once, then rejects the endpoint', () => {
  const blocked = classifyChatGPTReadiness([
    { name: 'session', kind: 'api', ok: false, status: 0, elapsedMs: 500 }
  ]);

  assert.equal(selectReadinessRecovery(blocked, true, false), 'reopen-connection-pool');
  assert.equal(selectReadinessRecovery(blocked, true, true), 'reject-chatgpt-endpoint');
  assert.equal(selectReadinessRecovery(blocked, false, false), 'wait-generic-route');
});

test('normalizes Chromium network errors separately from HTTP failures', () => {
  const reset = normalizeProbeResult(
    { name: 'account', kind: 'api', url: 'https://chatgpt.com/backend-api/me' },
    { success: false, netError: -101, netErrorName: 'net::ERR_CONNECTION_RESET' },
    321
  );

  assert.equal(reset.ok, false);
  assert.equal(reset.netError, -101);
  assert.equal(reset.error, 'net::ERR_CONNECTION_RESET');
});

test('benchmark races isolated targets and records every outcome', async () => {
  let nextTarget = 0;
  const closed = [];
  const cdp = {
    async send(method, params, sessionId) {
      if (method === 'Target.createTarget') return { targetId: `target-${++nextTarget}` };
      if (method === 'Target.attachToTarget') return { sessionId: `session-${params.targetId}` };
      if (method === 'Network.enable' || method === 'Network.setBypassServiceWorker') return {};
      if (method === 'Target.closeTarget') {
        closed.push(params.targetId);
        return {};
      }
      if (method === 'Network.loadNetworkResource') {
        const slow = params.url.includes('/api/auth/session');
        await new Promise(resolve => setTimeout(resolve, slow ? 30 : 5));
        return { resource: { success: true, httpStatusCode: slow ? 200 : 204, netError: 0 } };
      }
      throw new Error(`unexpected method ${method} on ${sessionId}`);
    }
  };

  const benchmark = await runChatGPTReadinessBenchmark(cdp, {
    probes: [
      { name: 'edge', kind: 'edge', url: 'https://chatgpt.com/cdn-cgi/trace' },
      { name: 'session', kind: 'api', url: 'https://chatgpt.com/api/auth/session' }
    ]
  });

  assert.equal(benchmark.state, 'api-ready');
  assert.equal(benchmark.results.length, 2);
  assert.equal(benchmark.fastest.name, 'edge');
  assert.deepEqual(closed.sort(), ['target-1', 'target-2']);
});
