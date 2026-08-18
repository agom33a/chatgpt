const test = require('node:test');
const assert = require('node:assert/strict');
const { runIsolatedCheckout, normalizeAuth } = require('./isolated_checkout');

function createMockCdp(visibleUrl, options = {}) {
  const calls = [];
  const sessionId = 'isolated-session';
  const country = options.country || 'PH';
  const send = async (method, params = {}, callSessionId = null) => {
    calls.push({ method, params, sessionId: callSessionId });
    if (method === 'Target.createTarget') return { targetId: 'api-target' };
    if (method === 'Target.attachToTarget') return { sessionId };
    if (method === 'Target.closeTarget') return { success: true };
    if (method === 'Page.navigate') return { frameId: 'api-frame' };
    if (method !== 'Runtime.evaluate') return {};

    const expression = params.expression || '';
    if (expression.includes('isolated:country:')) {
      return { result: { value: JSON.stringify({ ok: true, country, status: 200 }) } };
    }
    if (expression.includes('host:location.hostname')) {
      return { result: { value: JSON.stringify({ host: 'chatgpt.com', ready: 'complete' }) } };
    }
    if (expression.includes('isolated:sentinel')) {
      return { result: { value: JSON.stringify({ status: 200, token: 'sentinel-token' }) } };
    }
    if (expression.includes('isolated:checkout')) {
      if (options.hangCheckout) return new Promise(() => {});
      return {
        result: {
          value: JSON.stringify({
            ok: true,
            status: 200,
            body: JSON.stringify({
              checkout_session_id: 'checkout-id',
              processor_entity: 'openai_llc',
              billing_details: { country, currency: 'PHP' }
            })
          })
        }
      };
    }
    throw new Error(`Unexpected evaluation: ${expression.slice(0, 80)}`);
  };
  return { visibleUrl, calls, send };
}

for (const visibleUrl of ['about:blank', 'chrome-error://chromewebdata/']) {
  test(`checkout succeeds while visible page is ${visibleUrl}`, async () => {
    const cdp = createMockCdp(visibleUrl);
    const traces = [];
    const result = await runIsolatedCheckout({
      cdp,
      apiSession: { accessToken: 'secret-access-token', account: { id: 'account-id' } },
      currencyForCountry: country => country === 'PH' ? 'PHP' : 'USD',
      confirm: async detected => ({ confirmed: detected.reliable, country: detected.country }),
      trace: (event, details) => traces.push({ event, details })
    });

    assert.equal(result.url, 'https://chatgpt.com/checkout/openai_llc/checkout-id');
    assert.equal(result.country, 'PH');
    assert.equal(result.currency, 'PHP');
    assert.ok(cdp.calls.some(call => call.method === 'Target.closeTarget'));

    const targetEvaluations = cdp.calls.filter(call => call.method === 'Runtime.evaluate');
    assert.ok(targetEvaluations.length > 0);
    assert.ok(targetEvaluations.every(call => call.sessionId === 'isolated-session'));
    assert.ok(cdp.calls
      .filter(call => call.method === 'Page.navigate')
      .every(call => call.sessionId === 'isolated-session'));
    assert.equal(JSON.stringify(traces).includes('secret-access-token'), false);
    assert.equal(cdp.visibleUrl, visibleUrl, 'the visible target was not navigated');
  });
}

test('declining country confirmation does not create checkout', async () => {
  const cdp = createMockCdp('chrome-error://chromewebdata/');
  const result = await runIsolatedCheckout({
    cdp,
    apiSession: { accessToken: 'token', account: 'account-id' },
    currencyForCountry: () => 'PHP',
    confirm: async () => ({ confirmed: false })
  });

  assert.deepEqual(result, { cancelled: true });
  assert.equal(cdp.calls.some(call =>
    call.method === 'Runtime.evaluate' &&
    call.params.expression.includes('isolated:checkout')), false);
  assert.ok(cdp.calls.some(call => call.method === 'Target.closeTarget'));
});

test('an idle checkout times out and closes its API target', async () => {
  const cdp = createMockCdp('about:blank', { hangCheckout: true });
  await assert.rejects(runIsolatedCheckout({
    cdp,
    apiSession: { accessToken: 'token', account: { id: 'account-id' } },
    currencyForCountry: () => 'PHP',
    confirm: async detected => ({ confirmed: true, country: detected.country }),
    idleTimeoutMs: 25,
    hardTimeoutMs: 1000
  }), /checkout_request timed out|stopped making progress/);
  assert.ok(cdp.calls.some(call => call.method === 'Target.closeTarget'));
});

test('cancelling an active checkout closes its API target', async () => {
  const cdp = createMockCdp('chrome-error://chromewebdata/', { hangCheckout: true });
  const controller = new AbortController();
  const checkout = runIsolatedCheckout({
    cdp,
    apiSession: { accessToken: 'token', account: { id: 'account-id' } },
    currencyForCountry: () => 'PHP',
    confirm: async detected => {
      setTimeout(() => controller.abort(new Error('route changed')), 10);
      return { confirmed: true, country: detected.country };
    },
    signal: controller.signal,
    idleTimeoutMs: 500,
    hardTimeoutMs: 1000
  });
  await assert.rejects(checkout, error =>
    error.name === 'AbortError' && /route changed/.test(error.message));
  assert.ok(cdp.calls.some(call => call.method === 'Target.closeTarget'));
});

test('time waiting for country confirmation does not consume the idle budget', async () => {
  const cdp = createMockCdp('about:blank');
  const result = await runIsolatedCheckout({
    cdp,
    apiSession: { accessToken: 'token', account: { id: 'account-id' } },
    currencyForCountry: () => 'PHP',
    confirm: async detected => {
      await new Promise(resolve => setTimeout(resolve, 30));
      return { confirmed: true, country: detected.country };
    },
    idleTimeoutMs: 10,
    hardTimeoutMs: 1000
  });
  assert.equal(result.country, 'PH');
});

test('normalizes account identity without exposing credentials', () => {
  assert.deepEqual(
    normalizeAuth({ accessToken: 'token', account: { account_id: 'acct' } }),
    { token: 'token', account: 'acct' }
  );
});
