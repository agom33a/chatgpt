const crypto = require('crypto');

const CHATGPT_ORIGIN = 'https://chatgpt.com';
const COUNTRY_PROVIDERS = [
  { name: 'api.country.is', url: 'https://api.country.is/', field: 'country' },
  { name: 'ipwho.is', url: 'https://ipwho.is/', field: 'country_code' },
  { name: 'ipinfo.io', url: 'https://ipinfo.io/json', field: 'country' }
];

const fingerprint = value => value
  ? crypto.createHash('sha256').update(String(value)).digest('hex').slice(0, 8)
  : 'none';

function abortError(reason = 'Checkout cancelled') {
  const error = new Error(reason);
  error.name = 'AbortError';
  return error;
}

class ProgressBudget {
  constructor({ signal, trace, idleTimeoutMs = 30000, hardTimeoutMs = 180000 }) {
    this.signal = signal;
    this.trace = trace;
    this.idleTimeoutMs = idleTimeoutMs;
    this.hardDeadline = Date.now() + hardTimeoutMs;
    this.lastProgressAt = Date.now();
  }

  check() {
    if (this.signal?.aborted) throw abortError(this.signal.reason?.message || 'Checkout cancelled');
    if (Date.now() >= this.hardDeadline) throw new Error('Checkout hard timeout exceeded');
    if (Date.now() - this.lastProgressAt >= this.idleTimeoutMs) {
      throw new Error('Checkout stopped making progress');
    }
  }

  progress(stage, details = {}) {
    this.check();
    this.lastProgressAt = Date.now();
    this.trace(stage, details);
  }

  async run(stage, timeoutMs, operation) {
    this.progress(`${stage}_started`);
    const remainingHardMs = this.hardDeadline - Date.now();
    const budgetMs = Math.max(1, Math.min(timeoutMs, this.idleTimeoutMs, remainingHardMs));
    let timer;
    let abortListener;
    try {
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${stage} timed out after ${budgetMs}ms without progress`)), budgetMs);
      });
      const cancelled = new Promise((_, reject) => {
        if (!this.signal) return;
        abortListener = () => reject(abortError(this.signal.reason?.message || 'Checkout cancelled'));
        this.signal.addEventListener('abort', abortListener, { once: true });
      });
      const result = await Promise.race([operation(), timeout, cancelled]);
      this.progress(`${stage}_succeeded`);
      return result;
    } catch (error) {
      this.trace(`${stage}_failed`, { error: error.message });
      throw error;
    } finally {
      clearTimeout(timer);
      if (abortListener) this.signal.removeEventListener('abort', abortListener);
    }
  }
}

function normalizeAuth(apiSession) {
  const account = typeof apiSession?.account === 'string'
    ? apiSession.account
    : apiSession?.account?.id || apiSession?.account?.account_id || null;
  return {
    token: typeof apiSession?.accessToken === 'string' ? apiSession.accessToken : null,
    account
  };
}

async function createApiTarget(cdp, budget) {
  const target = await budget.run('api_target_create', 10000, () =>
    cdp.send('Target.createTarget', { url: 'about:blank', background: true }, null, 10000));
  const targetId = target?.targetId;
  if (!targetId) throw new Error('CDP did not return an API target id');

  try {
    const attached = await budget.run('api_target_attach', 10000, () =>
      cdp.send('Target.attachToTarget', { targetId, flatten: true }, null, 10000));
    if (!attached?.sessionId) throw new Error('CDP did not attach the API target');
    const sessionId = attached.sessionId;
    await Promise.all([
      cdp.send('Runtime.enable', {}, sessionId, 10000),
      cdp.send('Page.enable', {}, sessionId, 10000),
      cdp.send('Network.enable', {}, sessionId, 10000)
    ]);
    try {
      await cdp.send('Network.setBypassServiceWorker', { bypass: true }, sessionId, 5000);
    } catch {}
    budget.progress('api_target_ready', { targetId, sessionId });
    return { targetId, sessionId };
  } catch (error) {
    try { await cdp.send('Target.closeTarget', { targetId }, null, 5000); } catch {}
    throw error;
  }
}

async function evaluateJson(cdp, sessionId, expression, timeoutMs) {
  const response = await cdp.send('Runtime.evaluate', {
    expression,
    awaitPromise: true,
    returnByValue: true,
    timeout: timeoutMs
  }, sessionId, timeoutMs + 5000);
  const value = response?.result?.value;
  if (typeof value !== 'string') throw new Error('API target returned no result');
  return JSON.parse(value);
}

async function probeCountry(cdp, sessionId, budget, provider, timeoutMs = 8000) {
  const expression = `(async () => {
    /* isolated:country:${provider.name} */
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ${timeoutMs});
    try {
      const response = await fetch(${JSON.stringify(provider.url)} +
        (${JSON.stringify(provider.url)}.includes('?') ? '&' : '?') + '_=' + Date.now(), {
        cache: 'no-store',
        signal: controller.signal
      });
      const data = await response.json();
      const country = String(data[${JSON.stringify(provider.field)}] || '').toUpperCase();
      return JSON.stringify({
        ok: response.ok && /^[A-Z]{2}$/.test(country),
        country,
        status: response.status
      });
    } catch (error) {
      return JSON.stringify({ok:false, error:error.name === 'AbortError' ? 'provider timeout' : error.message});
    } finally {
      clearTimeout(timer);
    }
  })()`;
  return budget.run(`country_probe_${provider.name.replace(/\W/g, '_')}`, timeoutMs + 7000,
    () => evaluateJson(cdp, sessionId, expression, timeoutMs + 2000));
}

async function detectExitCountry(cdp, sessionId, budget) {
  const observations = [];
  for (const provider of COUNTRY_PROVIDERS) {
    budget.check();
    try {
      const result = await probeCountry(cdp, sessionId, budget, provider);
      observations.push({
        provider: provider.name,
        ok: !!result.ok,
        country: /^[A-Z]{2}$/.test(result.country || '') ? result.country : null
      });
    } catch (error) {
      observations.push({ provider: provider.name, ok: false, country: null, error: error.message });
    }
  }

  const counts = new Map();
  for (const item of observations.filter(item => item.ok && item.country)) {
    counts.set(item.country, (counts.get(item.country) || 0) + 1);
  }
  const ranked = [...counts.entries()].sort((a, b) => b[1] - a[1]);
  const country = ranked[0]?.[0] || null;
  const agreeingProviders = ranked[0]?.[1] || 0;
  const reliable = agreeingProviders >= 2;
  budget.progress('exit_country_observed', {
    country,
    reliable,
    agreeingProviders,
    observations
  });
  return { country, reliable, agreeingProviders, observations };
}

async function establishApiOrigin(cdp, sessionId, budget) {
  const marker = `isolated_checkout=${Date.now()}`;
  await budget.run('api_origin_navigation', 30000, async () => {
    const result = await cdp.send('Page.navigate', {
      url: `${CHATGPT_ORIGIN}/api/auth/session?${marker}`
    }, sessionId, 25000);
    if (result?.errorText) throw new Error(`API origin navigation failed: ${result.errorText}`);

    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
      budget.check();
      try {
        const state = await evaluateJson(cdp, sessionId,
          `JSON.stringify({host:location.hostname,ready:document.readyState})`, 3000);
        if (state.host === 'chatgpt.com' && /interactive|complete/.test(state.ready || '')) return;
      } catch {}
      await new Promise(resolve => setTimeout(resolve, 250));
    }
    throw new Error('The isolated API target could not reach chatgpt.com');
  });
}

async function resolveAuth(cdp, sessionId, apiSession, budget) {
  const supplied = normalizeAuth(apiSession);
  if (supplied.token) {
    budget.progress('api_auth_ready', {
      source: 'session_api.json',
      tokenFingerprint: fingerprint(supplied.token),
      accountKnown: !!supplied.account
    });
    return supplied;
  }

  const expression = `(async () => {
    /* isolated:session */
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch('/api/auth/session', {
        credentials:'include', cache:'no-store', signal:controller.signal
      });
      const data = await response.json();
      const account = typeof data.account === 'string'
        ? data.account : (data.account?.id || data.account?.account_id || null);
      return JSON.stringify({status:response.status, token:data.accessToken || null, account});
    } catch (error) {
      return JSON.stringify({status:0,error:error.message});
    } finally { clearTimeout(timer); }
  })()`;
  const result = await budget.run('api_auth_session', 22000,
    () => evaluateJson(cdp, sessionId, expression, 18000));
  if (!result.token) throw new Error(`No access token available in isolated API target (HTTP ${result.status || 0})`);
  budget.progress('api_auth_ready', {
    source: 'isolated session endpoint',
    tokenFingerprint: fingerprint(result.token),
    accountKnown: !!result.account
  });
  return { token: result.token, account: result.account };
}

async function requestSentinel(cdp, sessionId, auth, budget) {
  const expression = `(async () => {
    /* isolated:sentinel */
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12000);
    try {
      const headers = {
        'Content-Type':'application/json',
        'Authorization':'Bearer ' + ${JSON.stringify(auth.token)},
        'OAI-Language':'en-US'
      };
      if (${JSON.stringify(auth.account)}) headers['ChatGPT-Account-ID'] = ${JSON.stringify(auth.account)};
      const response = await fetch('/backend-api/sentinel/chat-requirements', {
        method:'POST', headers, body:'{}', credentials:'include', signal:controller.signal
      });
      const data = response.ok ? await response.json().catch(() => ({})) : {};
      return JSON.stringify({status:response.status, token:data.token || null});
    } catch (error) {
      return JSON.stringify({status:0,error:error.name === 'AbortError' ? 'sentinel timeout' : error.message});
    } finally { clearTimeout(timer); }
  })()`;
  try {
    return await budget.run('sentinel_request', 18000,
      () => evaluateJson(cdp, sessionId, expression, 15000));
  } catch (error) {
    return { status: 0, token: null, error: error.message };
  }
}

async function requestCheckout(cdp, sessionId, auth, sentinel, country, currency, budget) {
  const body = {
    entry_point: 'all_plans_pricing_modal',
    plan_name: 'chatgptplusplan',
    billing_details: { country, currency },
    checkout_ui_mode: 'custom'
  };
  const expression = `(async () => {
    /* isolated:checkout */
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 60000);
    try {
      const headers = {
        'Content-Type':'application/json',
        'Authorization':'Bearer ' + ${JSON.stringify(auth.token)},
        'OAI-Language':'en-US'
      };
      if (${JSON.stringify(auth.account)}) headers['ChatGPT-Account-ID'] = ${JSON.stringify(auth.account)};
      if (${JSON.stringify(sentinel.token || null)}) {
        headers['openai-sentinel-chat-requirements-token'] = ${JSON.stringify(sentinel.token || null)};
      }
      const response = await fetch('/backend-api/payments/checkout', {
        method:'POST',
        headers,
        body:${JSON.stringify(JSON.stringify(body))},
        credentials:'include',
        signal:controller.signal
      });
      const text = await response.text();
      return JSON.stringify({ok:response.ok,status:response.status,body:text});
    } catch (error) {
      return JSON.stringify({ok:false,status:0,error:error.name === 'AbortError' ? 'checkout timeout' : error.message});
    } finally { clearTimeout(timer); }
  })()`;
  const response = await budget.run('checkout_request', 70000,
    () => evaluateJson(cdp, sessionId, expression, 65000));
  if (!response.ok) {
    const detail = response.error || String(response.body || '').slice(0, 300);
    throw new Error(`Checkout HTTP ${response.status || 0}: ${detail}`);
  }
  let data;
  try { data = JSON.parse(response.body); }
  catch { throw new Error('Checkout returned invalid JSON'); }
  if (!data.checkout_session_id) throw new Error('Checkout response had no checkout_session_id');
  return {
    url: `${CHATGPT_ORIGIN}/checkout/${data.processor_entity || 'openai_llc'}/${data.checkout_session_id}`,
    country: data.billing_details?.country || country,
    currency: data.billing_details?.currency || currency,
    sentinelUsed: !!sentinel.token
  };
}

async function runIsolatedCheckout(options) {
  const {
    cdp,
    apiSession,
    currencyForCountry,
    confirm,
    signal,
    trace: rawTrace = () => {},
    idleTimeoutMs,
    hardTimeoutMs
  } = options;
  if (!cdp || typeof cdp.send !== 'function') throw new Error('A CDP client is required');
  if (typeof confirm !== 'function') throw new Error('A checkout confirmation callback is required');

  const trace = (stage, details = {}) => rawTrace('checkout_api_stage', { stage, ...details });
  const budget = new ProgressBudget({ signal, trace, idleTimeoutMs, hardTimeoutMs });
  let target;
  try {
    target = await createApiTarget(cdp, budget);
    const detected = await detectExitCountry(cdp, target.sessionId, budget);
    await establishApiOrigin(cdp, target.sessionId, budget);
    const auth = await resolveAuth(cdp, target.sessionId, apiSession, budget);

    budget.progress('user_confirmation_requested', {
      country: detected.country,
      reliable: detected.reliable,
      agreeingProviders: detected.agreeingProviders
    });
    const decision = await confirm(detected);
    budget.check();
    if (!decision?.confirmed) {
      budget.progress('user_confirmation_declined');
      return { cancelled: true };
    }
    const country = String(decision.country || detected.country || '').toUpperCase();
    if (!/^[A-Z]{2}$/.test(country)) throw new Error('A confirmed 2-letter billing country is required');
    const currency = currencyForCountry(country);
    budget.progress('user_confirmation_received', { country, currency });

    const sentinel = await requestSentinel(cdp, target.sessionId, auth, budget);
    budget.progress('sentinel_result', { status: sentinel.status || 0, tokenReceived: !!sentinel.token });
    const result = await requestCheckout(
      cdp, target.sessionId, auth, sentinel, country, currency, budget);
    budget.progress('checkout_ready', {
      country: result.country,
      currency: result.currency,
      sentinelUsed: result.sentinelUsed
    });
    return result;
  } finally {
    if (target?.targetId) {
      try {
        await cdp.send('Target.closeTarget', { targetId: target.targetId }, null, 5000);
        trace('api_target_closed', { targetId: target.targetId });
      } catch (error) {
        trace('api_target_close_failed', { error: error.message });
      }
    }
  }
}

module.exports = {
  COUNTRY_PROVIDERS,
  ProgressBudget,
  normalizeAuth,
  runIsolatedCheckout
};
