'use strict';

const { raceRecoveryStrategies } = require('./recovery_race');

const sleep = (ms, signal) => new Promise((resolve, reject) => {
  if (signal?.aborted) return reject(new Error(`cancelled: ${signal.reason || 'aborted'}`));
  const timer = setTimeout(resolve, ms);
  signal?.addEventListener('abort', () => {
    clearTimeout(timer);
    reject(new Error(`cancelled: ${signal.reason || 'aborted'}`));
  }, { once: true });
});

const marker = (raceId, strategy) =>
  `recovery_race=${encodeURIComponent(raceId)}&strategy=${encodeURIComponent(strategy)}`;

async function closeTarget(cdp, targetId) {
  if (!targetId) return;
  try { await cdp.send('Target.closeTarget', { targetId }); } catch {}
}

async function attachBackgroundTarget(cdp, { initialUrl = 'about:blank', signal }) {
  const target = await cdp.send('Target.createTarget', {
    url: initialUrl,
    background: true
  });
  const targetId = target.targetId;
  let closed = false;
  const close = async () => {
    if (closed) return;
    closed = true;
    await closeTarget(cdp, targetId);
  };
  signal.addEventListener('abort', () => { void close(); }, { once: true });
  if (signal.aborted) {
    await close();
    throw new Error(`cancelled: ${signal.reason || 'aborted'}`);
  }

  try {
    const attached = await cdp.send('Target.attachToTarget', {
      targetId,
      flatten: true
    });
    const sessionId = attached.sessionId;
    await Promise.all([
      cdp.send('Network.enable', {}, sessionId),
      cdp.send('Page.enable', {}, sessionId),
      cdp.send('Runtime.enable', {}, sessionId)
    ]);
    try {
      await cdp.send('Network.setBypassServiceWorker', { bypass: true }, sessionId);
    } catch {}
    return { targetId, sessionId, close };
  } catch (error) {
    await close();
    throw error;
  }
}

async function navigate(cdp, sessionId, url, signal) {
  if (signal.aborted) throw new Error(`cancelled: ${signal.reason || 'aborted'}`);
  const result = await cdp.send('Page.navigate', { url }, sessionId, 30000);
  if (result?.errorText) throw new Error(result.errorText);
}

async function waitForChatGPTContext(cdp, sessionId, signal, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last = {};
  while (Date.now() < deadline) {
    if (signal.aborted) throw new Error(`cancelled: ${signal.reason || 'aborted'}`);
    try {
      const evaluated = await cdp.send('Runtime.evaluate', {
        expression: `JSON.stringify({
          host: location.hostname,
          protocol: location.protocol,
          ready: document.readyState,
          hasBody: !!document.body
        })`,
        returnByValue: true,
        timeout: 2000
      }, sessionId, 3000);
      last = JSON.parse(evaluated.result?.value || '{}');
      if (last.host === 'chatgpt.com' && last.hasBody &&
          (last.ready === 'interactive' || last.ready === 'complete')) return last;
      if (/^chrome-error:/i.test(last.protocol || '')) {
        throw new Error('ChatGPT navigation reached a Chrome error page');
      }
    } catch (error) {
      if (/Chrome error page/.test(error.message)) throw error;
    }
    await sleep(150, signal);
  }
  throw new Error(`ChatGPT context did not become ready in ${timeoutMs}ms`);
}

async function probeAuthenticatedCountry(cdp, sessionId, signal) {
  if (signal.aborted) throw new Error(`cancelled: ${signal.reason || 'aborted'}`);
  const evaluated = await cdp.send('Runtime.evaluate', {
    expression: `(async () => {
      const read = async url => {
        try {
          const response = await fetch(url, {credentials:'include', cache:'no-store'});
          const text = await response.text();
          let json = null;
          try { json = JSON.parse(text); } catch {}
          return {status:response.status, json, html:json === null && /^\\s*</.test(text)};
        } catch (error) {
          return {status:0, error:error.message};
        }
      };
      const [session, me] = await Promise.all([
        read('/api/auth/session'),
        read('/backend-api/me')
      ]);
      return JSON.stringify({
        loggedIn: !!session.json?.user,
        email: session.json?.user?.email || null,
        sessionStatus: session.status,
        status: me.status,
        country: me.json?.country || null,
        challenge: session.html || me.html,
        error: session.error || me.error || null
      });
    })()`,
    awaitPromise: true,
    returnByValue: true,
    timeout: 20000
  }, sessionId, 25000);
  return JSON.parse(evaluated.result?.value || '{}');
}

async function verifyAuthenticatedCountry(cdp, sessionId, expectedCountry, signal, settleMs) {
  const first = await probeAuthenticatedCountry(cdp, sessionId, signal);
  if (!first.loggedIn || first.sessionStatus !== 200 || first.status !== 200 ||
      first.country !== expectedCountry) {
    return { ok: false, state: first, samples: 1 };
  }
  await sleep(settleMs, signal);
  const second = await probeAuthenticatedCountry(cdp, sessionId, signal);
  const ok = second.loggedIn && second.sessionStatus === 200 && second.status === 200 &&
    second.country === expectedCountry && second.country === first.country;
  return { ok, state: second, first, samples: 2 };
}

function strategyDefinitions(raceId) {
  const query = strategy => marker(raceId, strategy);
  return [
    {
      name: 'root-document',
      initialUrl: 'about:blank',
      steps: [
        { type: 'navigate', url: `https://chatgpt.com/?${query('root-document')}` }
      ]
    },
    {
      name: 'auth-refresh-document',
      initialUrl: 'about:blank',
      steps: [
        {
          type: 'navigate',
          url: `https://chatgpt.com/api/auth/session?refresh=true&reason=integrity_state_mismatch&${query('auth-refresh-document')}`
        },
        { type: 'navigate', url: `https://chatgpt.com/?${query('auth-refresh-document')}` }
      ]
    },
    {
      name: 'account-endpoint-bootstrap',
      // Starting at createTarget exercises Chromium's browser-level navigation
      // path instead of issuing Page.navigate for the first request.
      initialUrl: `https://chatgpt.com/backend-api/me?${query('account-endpoint-bootstrap')}`,
      waitForInitial: true,
      steps: [
        { type: 'navigate', url: `https://chatgpt.com/?${query('account-endpoint-bootstrap')}` }
      ]
    }
  ];
}

async function runIsolatedChatGPTRecovery(cdp, {
  expectedCountry,
  timeoutMs = 90000,
  targetReadyTimeoutMs = 30000,
  verificationSettleMs = 900,
  trace = () => {},
  raceId = `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
  definitions = strategyDefinitions(raceId)
}) {
  if (!/^[A-Z]{2}$/.test(expectedCountry || '')) {
    throw new Error('expectedCountry must be an uppercase ISO-3166 alpha-2 code');
  }

  const strategies = definitions.map(definition => ({
    name: definition.name,
    run: async ({ signal }) => {
      let target;
      const startedAt = Date.now();
      try {
        target = await attachBackgroundTarget(cdp, {
          initialUrl: definition.initialUrl,
          signal
        });
        if (definition.waitForInitial) {
          await waitForChatGPTContext(
            cdp,
            target.sessionId,
            signal,
            targetReadyTimeoutMs
          );
        }
        for (const step of definition.steps) {
          await navigate(cdp, target.sessionId, step.url, signal);
          await waitForChatGPTContext(
            cdp,
            target.sessionId,
            signal,
            targetReadyTimeoutMs
          );
        }
        // Even createTarget(initialUrl) strategies must have committed a
        // ChatGPT origin before relative authenticated fetches are allowed.
        await waitForChatGPTContext(cdp, target.sessionId, signal, targetReadyTimeoutMs);
        const verification = await verifyAuthenticatedCountry(
          cdp,
          target.sessionId,
          expectedCountry,
          signal,
          verificationSettleMs
        );
        return {
          ready: true,
          targetId: target.targetId,
          verification,
          elapsedMs: Date.now() - startedAt
        };
      } finally {
        if (target) await target.close();
      }
    }
  }));

  return raceRecoveryStrategies({
    strategies,
    expectedCountry,
    timeoutMs,
    trace,
    validate: async candidate => {
      const verification = candidate?.verification;
      const state = verification?.state || {};
      const ok = candidate?.ready && verification?.ok &&
        verification.samples === 2 && state.loggedIn &&
        state.sessionStatus === 200 && state.status === 200 &&
        state.country === expectedCountry;
      return {
        ok,
        country: state.country || null,
        reason: ok
          ? null
          : `verification failed (session=${state.sessionStatus || 0}, me=${state.status || 0}, country=${state.country || '?'})`
      };
    }
  });
}

module.exports = {
  runIsolatedChatGPTRecovery,
  strategyDefinitions,
  verifyAuthenticatedCountry
};
