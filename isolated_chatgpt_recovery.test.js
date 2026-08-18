'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { runIsolatedChatGPTRecovery } = require('./isolated_chatgpt_recovery');

function mockCdp(outcomes) {
  let nextTarget = 0;
  const targetStrategies = new Map();
  const sessionTargets = new Map();
  const calls = [];

  return {
    calls,
    async send(method, params = {}, sessionId = null) {
      calls.push({ method, params, sessionId });
      if (method === 'Target.createTarget') {
        const targetId = `target-${++nextTarget}`;
        const match = String(params.url).match(/[?&]strategy=([^&]+)/);
        if (match) targetStrategies.set(targetId, decodeURIComponent(match[1]));
        return { targetId };
      }
      if (method === 'Target.attachToTarget') {
        const attachedSession = `session-${params.targetId}`;
        sessionTargets.set(attachedSession, params.targetId);
        return { sessionId: attachedSession };
      }
      if (method === 'Target.closeTarget') return { success: true };

      const targetId = sessionTargets.get(sessionId);
      assert.ok(targetId, `${method} must be scoped to a background session`);
      if (method === 'Page.navigate') {
        const match = String(params.url).match(/[?&]strategy=([^&]+)/);
        if (match) targetStrategies.set(targetId, decodeURIComponent(match[1]));
        return {};
      }
      if (method !== 'Runtime.evaluate') return {};
      if (params.expression.includes('document.readyState')) {
        return {
          result: {
            value: JSON.stringify({
              host: 'chatgpt.com',
              protocol: 'https:',
              ready: 'complete',
              hasBody: true
            })
          }
        };
      }

      const strategy = targetStrategies.get(targetId);
      const outcome = outcomes[strategy];
      if (outcome.delayMs) {
        await new Promise(resolve => setTimeout(resolve, outcome.delayMs));
      }
      if (outcome.error) throw new Error(outcome.error);
      return {
        result: {
          value: JSON.stringify({
            loggedIn: true,
            email: 'mock@example.com',
            sessionStatus: 200,
            status: 200,
            country: outcome.country,
            challenge: false
          })
        }
      };
    }
  };
}

test('isolated race accepts PH, closes every target, and never touches cookies or visible page', async () => {
  const cdp = mockCdp({
    'root-document': { country: 'PH', delayMs: 2 },
    'auth-refresh-document': { country: 'EG', delayMs: 1 },
    'account-endpoint-bootstrap': { error: 'ERR_CONNECTION_RESET', delayMs: 1 }
  });

  const result = await runIsolatedChatGPTRecovery(cdp, {
    expectedCountry: 'PH',
    timeoutMs: 1000,
    targetReadyTimeoutMs: 100,
    verificationSettleMs: 1,
    raceId: 'deterministic'
  });

  assert.equal(result.strategy, 'root-document');
  assert.equal(result.verification.country, 'PH');

  const created = cdp.calls.filter(call => call.method === 'Target.createTarget');
  const closed = cdp.calls.filter(call => call.method === 'Target.closeTarget');
  assert.equal(created.length, 3);
  assert.equal(closed.length, 3);

  const cookieWrites = cdp.calls.filter(call =>
    call.method === 'Network.setCookie' ||
    call.method === 'Network.deleteCookies' ||
    call.method === 'Network.clearBrowserCookies'
  );
  assert.deepEqual(cookieWrites, []);

  const unscopedPageCalls = cdp.calls.filter(call =>
    /^(Page|Runtime|Network)\./.test(call.method) && !call.sessionId
  );
  assert.deepEqual(unscopedPageCalls, []);
});

test('authenticated old-country result is rejected even when it finishes first', async () => {
  const cdp = mockCdp({
    'root-document': { country: 'EG', delayMs: 0 },
    'auth-refresh-document': { country: 'PH', delayMs: 2 },
    'account-endpoint-bootstrap': { country: 'EG', delayMs: 1 }
  });
  const rejected = [];

  const result = await runIsolatedChatGPTRecovery(cdp, {
    expectedCountry: 'PH',
    timeoutMs: 1000,
    targetReadyTimeoutMs: 100,
    verificationSettleMs: 1,
    raceId: 'country-gate',
    trace: (event, details) => {
      if (event === 'recovery_strategy_rejected') rejected.push(details);
    }
  });

  assert.equal(result.strategy, 'auth-refresh-document');
  assert.ok(rejected.some(item => item.country === 'EG'));
});
