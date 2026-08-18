#!/usr/bin/env node
/**
 * ════════════════════════════════════════════════════════════════
 *   🎯 ChatGPT Manual Browser Smart (v8.0 - Progress driven)
 *
 *   v8.0 additions:
 *     - Waits on observed network progress instead of fixed deadlines
 *     - Escalates exit-IP probe budgets so slow proxies still qualify
 *     - Detects the plan/upgrade surface from the page, not from the URL
 *     - Ignores the sidebar upgrade entry point that is always present
 *
 *   v7.9 additions:
 *     - Ignores third-party (ad/telemetry) transport failures
 *     - Reuses the exit-IP provider that actually answers on this route
 *
 *   v7.8 additions:
 *     - Absorbs the slow first document of a new proxy in a background tab
 *     - Retries the visible load a few times with growing waits
 *     - Reports Page.navigate errors immediately instead of waiting them out
 *     - Stops polling the session while the tab holds an error page
 *
 *   v7.7 additions:
 *     - Stops recreating the duplicate session cookie it had just removed
 *     - Saves every generated pay link to pay_links.txt before opening the tab
 *     - Keeps analytics/telemetry traffic out of the terminal trace
 *     - Halves polling while the connection is idle
 *
 *   v7.6 additions:
 *     - Fixes the chatgpt.comblank URL built from an about:blank pathname
 *     - Restores a working ChatGPT document before requesting a pay link
 *     - Waits for an in-flight recovery instead of billing mid-transition
 *     - Never bills a timezone-derived country without confirmation
 *     - Reads NextAuth chunked session cookies and skips oversized jar writes
 *     - Drops the probe provider that rejects opaque origins
 *
 *   v7.5 additions:
 *     - Splits the sentinel call from checkout and time-boxes it to 12s
 *     - Raises checkout CDP/page timeouts so a slow route no longer aborts it
 *     - Treats the server's set-cookie as the authoritative session token
 *     - Removes duplicate session cookies that made the token oscillate
 *     - Leaves Chrome's error page before probing the proxy route
 *     - Drops the DomainMismatch cookie log flood
 *
 *   v7.4 additions:
 *     - Prints every request/response with exit IP, protocol, and duration
 *     - Reports blocked cookies, set-cookie names, and cookie rotations
 *     - Streams navigation, console, and browser log events
 *     - Logs recovery state transitions and a 10s exit-IP/region snapshot
 *     - Redacts cookie/authorization values to fingerprints
 *     - TRACE=0 silences output, TRACE_ASSETS=1 includes asset traffic
 *
 *   v7.3 additions:
 *     - Bills the country of the real exit IP, not ChatGPT's cached region
 *     - Clears Cloudflare/device cookies pinned to the previous IP
 *     - Reloads the same view so plan cards recompute for the new region
 *     - Reports region drift instead of failing recovery
 *
 *   v7.2 additions:
 *     - Cancels stale trial prompts during a network transition
 *     - Reopens the offer prompt after the new country is ready
 *     - Tracks server-rotated session cookies instead of forcing a stale token
 *     - Primes the first post-IP-change document in a background target
 *   v7.1 additions:
 *     - Persists the session in Chrome's cookie jar, not headers only
 *     - Probes the selected proxy invisibly before touching ChatGPT
 *     - Keeps the current page when it can recover without navigation
 *     - Performs at most one visible refresh after the new country is ready
 *     - Writes safe transition diagnostics to the system temp directory
 *   v7.0 additions:
 *     - State-machine recovery after proxy/country changes
 *     - Detects tunnel resets and network changes before an HTTP response
 *     - Releases stale renderer/cache/socket state before reconnecting
 *     - Confirms login and country twice before reporting success
 *     - CDP requests now clean up timers and surface unexpected failures
 *   v6.2 additions:
 *     - Detects "Rejoin Plus" / "Reactivate Plus" buttons (previous subscribers)
 *     - Also handles promo_campaign URLs (?promo_campaign=plus-1-month-free)
 *     - Shows the exact button label in the prompt
 *   v6.1 additions:
 *     - Detects when user is on pricing page WITHOUT trial offer
 *     - Asks: "Get direct pay link?" — works for Plus checkout without trial
 *   v6.0 additions:
 *     - Reactive 401 detection via Network.responseReceived (zero latency)
 *     - Auto-reloads within 1-2 seconds instead of up to 12s wait
 *   v5.9 additions:
 *     - Background country monitor (checks /backend-api/me every 12s)
 *     - Auto-reloads when session invalidates (401 from VPN switch)
 *   v5.8 improvements:
 *     - Silence Fetch stats while waiting for user input
 *     - Better prompt: [y/N] default explained
 *   v5.7 additions:
 *     - Live debug output for every intercepted request
 *     - Shows Fetch errors, timeouts, and retry attempts
 *   v5.6 fixes:
 *     - Auto-recovery timer on Fetch interceptor (8s max per request)
 *   v5.5 additions:
 *     - Loads unpacked extensions from ./extensions/ folder
 *     - Handles Chrome Web Store metadata (key, update_url) automatically
 *     - Ideal for VPN extensions that need to run inside the browser
 *   v5.4 fixes:
 *     - Country detection runs at CHECKOUT time, not startup
 *     - Handles user switching VPN/extension mid-session correctly
 *     - Shows detected country in the pay link output
 *   v5.3 fixes:
 *     - Country detection now uses /backend-api/me first (100% accurate)
 *     - This is what ChatGPT ACTUALLY sees, not what external APIs guess
 *     - Works perfectly with any VPN/proxy/extension setup
 *   v5.2 fixes:
 *     - CSP bypass so external IP APIs work from inside chatgpt.com
 *     - Timezone → country fallback (works even if all IP APIs fail)
 *   v5.1 fixes:
 *     - Auto-detect country from browser IP
 *     - Use accessToken from session_api.json (fallback if page fetch fails)
 *     - Retry logic for token fetch with cache: no-store
 *     - Better error output when checkout fails
 * ════════════════════════════════════════════════════════════════
 */

const { spawn } = require('child_process');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const os = require('os');
const readline = require('readline');
const crypto = require('crypto');

// Full-visibility tracing. TRACE=0 silences the stream, TRACE_ASSETS=1 also
// prints successful images/fonts/scripts (very noisy, off by default).
const TRACE_ENABLED = process.env.TRACE !== '0';
const TRACE_ASSETS = process.env.TRACE_ASSETS === '1';
// ChatGPT's own analytics endpoints produce hundreds of identical lines that
// bury the session and region events; they stay in the diagnostics file.
const TRACE_TELEMETRY = process.env.TRACE_TELEMETRY === '1';
const TELEMETRY_PATTERN = /chatgpt\.com\/(ces\/|backend-api\/(?:lat|edge)\/)|\/telemetry\/intake|cdn-cgi\/challenge-platform/i;
const isTelemetryUrl = url => !TRACE_TELEMETRY && TELEMETRY_PATTERN.test(String(url || ''));
const ASSET_TYPES = new Set(['Image', 'Font', 'Stylesheet', 'Media', 'Manifest', 'Script', 'Prefetch']);
const SECRET_HEADERS = new Set(['cookie', 'set-cookie', 'authorization', 'proxy-authorization']);
const startedAt = Date.now();

const fingerprint = value =>
  value ? crypto.createHash('sha256').update(String(value)).digest('hex').slice(0, 8) : 'none';

const elapsedStamp = () => `${((Date.now() - startedAt) / 1000).toFixed(3).padStart(9)}s`;

const shortenUrl = (url, max = 96) => {
  try {
    const parsed = new URL(url);
    const tail = parsed.pathname + parsed.search.slice(0, 40);
    const text = parsed.hostname + tail;
    return text.length > max ? text.slice(0, max) + '…' : text;
  } catch {
    return String(url).slice(0, max);
  }
};

const redactHeaders = headers => Object.fromEntries(
  Object.entries(headers || {}).map(([name, value]) => SECRET_HEADERS.has(name.toLowerCase())
    ? [name, `<redacted ${String(value).length}b fp=${fingerprint(value)}>`]
    : [name, String(value).slice(0, 200)])
);

const sleep = ms => new Promise(r => setTimeout(r, ms));
process.on('uncaughtException', e => {
  console.error(`\n❌ Unexpected error: ${e?.stack || e}`);
});
process.on('unhandledRejection', e => {
  console.error(`\n❌ Unhandled promise rejection: ${e?.stack || e}`);
});

const COOKIE_NAME = '__Secure-next-auth.session-token';

const COUNTRY_CURRENCY = {
  US: 'USD', CA: 'CAD', MX: 'MXN',
  DE: 'EUR', FR: 'EUR', IT: 'EUR', ES: 'EUR', NL: 'EUR', BE: 'EUR',
  AT: 'EUR', IE: 'EUR', PT: 'EUR', FI: 'EUR', GR: 'EUR', LU: 'EUR',
  GB: 'GBP', CH: 'CHF', SE: 'SEK', NO: 'NOK', DK: 'DKK',
  PL: 'PLN', CZ: 'CZK', HU: 'HUF', RO: 'RON', TR: 'TRY',
  AE: 'AED', SA: 'SAR', QA: 'QAR', KW: 'KWD', BH: 'BHD',
  OM: 'OMR', JO: 'JOD', IL: 'ILS',
  EG: 'EGP', MA: 'MAD', TN: 'TND',
  JP: 'JPY', KR: 'KRW', CN: 'CNY', HK: 'HKD', TW: 'TWD',
  SG: 'SGD', MY: 'MYR', TH: 'THB', ID: 'IDR', PH: 'PHP',
  VN: 'VND', IN: 'INR', PK: 'PKR',
  AU: 'AUD', NZ: 'NZD',
  BR: 'BRL', AR: 'ARS', CL: 'CLP', CO: 'COP',
  ZA: 'ZAR', NG: 'NGN'
};
const currencyForCountry = c => COUNTRY_CURRENCY[(c || 'US').toUpperCase()] || 'USD';

function findChrome() {
  const paths = [
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    `${process.env.LOCALAPPDATA || ''}\\Google\\Chrome\\Application\\chrome.exe`,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/usr/bin/google-chrome', '/usr/bin/chromium-browser'
  ];
  return paths.find(p => { try { return p && fs.existsSync(p); } catch { return false; } });
}

async function connectCDP(port) {
  for (let i = 0; i < 40; i++) {
    try {
      const targets = await new Promise((res, rej) => {
        http.get(`http://127.0.0.1:${port}/json`, r => {
          let d = ''; r.on('data', c => d += c); r.on('end', () => { try { res(JSON.parse(d)); } catch (e) { rej(e); } });
        }).on('error', rej);
      });
      const page = targets.find(t => t.type === 'page');
      if (page && page.webSocketDebuggerUrl) {
        const ws = new WebSocket(page.webSocketDebuggerUrl, {
          perMessageDeflate: false, maxPayload: 256 * 1024 * 1024
        });
        await new Promise((r, rej) => { ws.on('open', r); ws.on('error', rej); });
        return ws;
      }
    } catch {}
    await sleep(400);
  }
  return null;
}

function cdpClient(ws) {
  let id = 0;
  const pending = new Map();
  const listeners = new Map();
  let alive = true;

  ws.on('message', msg => {
    let m; try { m = JSON.parse(msg.toString()); } catch { return; }
    if (m.id && pending.has(m.id)) {
      const p = pending.get(m.id);
      pending.delete(m.id);
      clearTimeout(p.timer);
      m.error ? p.reject(new Error(m.error.message)) : p.resolve(m.result);
    } else if (m.method && listeners.has(m.method)) {
      listeners.get(m.method).forEach(fn => { try { fn(m.params, m.sessionId || null); } catch {} });
    }
  });
  const rejectPending = reason => {
    alive = false;
    for (const p of pending.values()) {
      clearTimeout(p.timer);
      p.reject(new Error(reason));
    }
    pending.clear();
  };
  ws.on('close', () => rejectPending('CDP connection closed'));
  ws.on('error', e => rejectPending(`CDP connection error: ${e.message}`));

  return {
    send(method, params = {}, sessionId = null, timeoutMs = 30000) {
      if (!alive) return Promise.reject(new Error('CDP closed'));
      const i = ++id;
      return new Promise((res, rej) => {
        const timer = setTimeout(() => {
          if (pending.has(i)) {
            pending.delete(i);
            rej(new Error(`CDP timeout after ${timeoutMs}ms: ${method}`));
          }
        }, timeoutMs);
        pending.set(i, { resolve: res, reject: rej, timer });
        const message = { id: i, method, params };
        if (sessionId) message.sessionId = sessionId;
        try { ws.send(JSON.stringify(message)); }
        catch (e) {
          clearTimeout(timer);
          pending.delete(i);
          rej(e);
        }
      });
    },
    on(method, fn) {
      if (!listeners.has(method)) listeners.set(method, []);
      listeners.get(method).push(fn);
    },
    isAlive: () => alive
  };
}

function startProxyRelay(proxyStr, localPort) {
  return new Promise((resolve, reject) => {
    let pUrl;
    try { pUrl = new URL(proxyStr.startsWith('http') ? proxyStr : `http://${proxyStr}`); }
    catch { return reject(new Error('bad proxy')); }
    const authH = 'Basic ' + Buffer.from(`${decodeURIComponent(pUrl.username || '')}:${decodeURIComponent(pUrl.password || '')}`).toString('base64');
    const host = pUrl.hostname;
    const port = parseInt(pUrl.port) || 823;

    const srv = http.createServer((req, res) => {
      const p = http.request({ hostname: host, port, path: req.url, method: req.method,
        headers: { ...req.headers, 'Proxy-Authorization': authH } }, pR => {
        res.writeHead(pR.statusCode, pR.headers); pR.pipe(res);
      });
      req.pipe(p);
      p.on('error', () => { try { res.end(); } catch {} });
    });
    srv.on('connect', (req, sock, head) => {
      const t = http.request({ hostname: host, port, method: 'CONNECT', path: req.url,
        headers: { 'Proxy-Authorization': authH, Host: req.url } });
      t.on('connect', (_, pS) => {
        sock.write('HTTP/1.1 200 Connection Established\r\n\r\n');
        if (head && head.length) pS.write(head);
        pS.pipe(sock); sock.pipe(pS);
        pS.on('error', () => { try { sock.end(); } catch {} });
        sock.on('error', () => { try { pS.end(); } catch {} });
      });
      t.on('error', () => { try { sock.end(); } catch {} });
      t.end();
    });
    srv.listen(localPort, '127.0.0.1', () => resolve({
      close: () => { try { srv.close(); } catch {} }
    }));
  });
}

let sharedRL = null;
let activeQuestionAbort = null;
let activePrompt = null;
function getRL() {
  if (!sharedRL) sharedRL = readline.createInterface({ input: process.stdin, output: process.stdout });
  return sharedRL;
}

// Keep the trace stream and an open question from overwriting each other.
function writeLine(text) {
  if (activePrompt && sharedRL) {
    process.stdout.write('\r\u001b[2K' + text + '\n');
    process.stdout.write(activePrompt + (sharedRL.line || ''));
    return;
  }
  process.stdout.write(text + '\n');
}

function ask(q) {
  activePrompt = q.split('\n').pop();
  return new Promise(resolve => {
    const controller = new AbortController();
    let settled = false;
    const finish = value => {
      if (settled) return;
      settled = true;
      activePrompt = null;
      if (activeQuestionAbort === cancel) activeQuestionAbort = null;
      resolve(value);
    };
    const cancel = () => {
      controller.abort();
      finish(null);
    };
    activeQuestionAbort = cancel;
    getRL().question(q, { signal: controller.signal }, answer => finish(answer.trim()));
  });
}
function cancelActiveQuestion() {
  if (activeQuestionAbort) activeQuestionAbort();
}

// ── Timezone → Country mapping (fallback if IP APIs fail) ──
const TIMEZONE_TO_COUNTRY = {
  'Africa/Cairo': 'EG', 'Africa/Casablanca': 'MA', 'Africa/Tunis': 'TN',
  'Africa/Algiers': 'DZ', 'Africa/Lagos': 'NG', 'Africa/Johannesburg': 'ZA',
  'Asia/Dubai': 'AE', 'Asia/Riyadh': 'SA', 'Asia/Qatar': 'QA', 'Asia/Kuwait': 'KW',
  'Asia/Bahrain': 'BH', 'Asia/Muscat': 'OM', 'Asia/Amman': 'JO', 'Asia/Jerusalem': 'IL',
  'Asia/Beirut': 'LB', 'Asia/Damascus': 'SY', 'Asia/Baghdad': 'IQ',
  'Asia/Istanbul': 'TR', 'Europe/Istanbul': 'TR',
  'Asia/Tokyo': 'JP', 'Asia/Seoul': 'KR', 'Asia/Shanghai': 'CN', 'Asia/Hong_Kong': 'HK',
  'Asia/Taipei': 'TW', 'Asia/Singapore': 'SG', 'Asia/Kuala_Lumpur': 'MY',
  'Asia/Bangkok': 'TH', 'Asia/Jakarta': 'ID', 'Asia/Manila': 'PH', 'Asia/Ho_Chi_Minh': 'VN',
  'Asia/Kolkata': 'IN', 'Asia/Karachi': 'PK',
  'Europe/London': 'GB', 'Europe/Berlin': 'DE', 'Europe/Paris': 'FR', 'Europe/Rome': 'IT',
  'Europe/Madrid': 'ES', 'Europe/Amsterdam': 'NL', 'Europe/Brussels': 'BE',
  'Europe/Vienna': 'AT', 'Europe/Dublin': 'IE', 'Europe/Lisbon': 'PT',
  'Europe/Helsinki': 'FI', 'Europe/Athens': 'GR', 'Europe/Luxembourg': 'LU',
  'Europe/Zurich': 'CH', 'Europe/Stockholm': 'SE', 'Europe/Oslo': 'NO',
  'Europe/Copenhagen': 'DK', 'Europe/Warsaw': 'PL', 'Europe/Prague': 'CZ',
  'Europe/Budapest': 'HU', 'Europe/Bucharest': 'RO',
  'America/New_York': 'US', 'America/Chicago': 'US', 'America/Denver': 'US',
  'America/Los_Angeles': 'US', 'America/Phoenix': 'US', 'America/Anchorage': 'US',
  'America/Toronto': 'CA', 'America/Vancouver': 'CA', 'America/Montreal': 'CA',
  'America/Mexico_City': 'MX',
  'America/Sao_Paulo': 'BR', 'America/Buenos_Aires': 'AR',
  'America/Santiago': 'CL', 'America/Bogota': 'CO',
  'Australia/Sydney': 'AU', 'Australia/Melbourne': 'AU', 'Pacific/Auckland': 'NZ'
};

// ── Exit-IP country probe (the country the extension actually routes through) ──
// Some providers stall behind certain proxies, so the last one that answered is
// tried first to keep probes fast.
let preferredExitProvider = null;

async function probeExitCountry(cdp, sessionId = null, perProviderTimeoutMs = 4000) {
  const startedAt = Date.now();
  try {
    const res = await cdp.send('Runtime.evaluate', {
      expression: `(async () => {
        // ipapi.co refuses requests from an opaque (null) origin, which is what
        // an about:blank probe page has, so it is not used here.
        const providers = [
          {url:'https://api.country.is/', field:'country', name:'api.country.is'},
          {url:'https://ipwho.is/', field:'country_code', name:'ipwho.is'},
          {url:'https://ipinfo.io/json', field:'country', name:'ipinfo.io'}
        ];
        const preferred = ${JSON.stringify(preferredExitProvider)};
        if (preferred) {
          const index = providers.findIndex(p => p.name === preferred);
          if (index > 0) providers.unshift(providers.splice(index, 1)[0]);
        }
        for (const provider of providers) {
          const controller = new AbortController();
          const timer = setTimeout(() => controller.abort(), ${perProviderTimeoutMs});
          try {
            const joiner = provider.url.includes('?') ? '&' : '?';
            const response = await fetch(provider.url + joiner + '_=' + Date.now(), {
              cache: 'no-store',
              signal: controller.signal
            });
            const data = await response.json();
            const country = String(data[provider.field] || '').toUpperCase();
            if (response.ok && /^[A-Z]{2}$/.test(country)) {
              return JSON.stringify({ok:true,country,provider:provider.name});
            }
          } catch {}
          finally { clearTimeout(timer); }
        }
        return JSON.stringify({ok:false});
      })()`,
      awaitPromise: true, returnByValue: true, timeout: perProviderTimeoutMs * 4 + 6000
    }, sessionId, perProviderTimeoutMs * 4 + 16000);
    const route = JSON.parse(res.result?.value || '{}');
    route.elapsedMs = Date.now() - startedAt;
    if (route.ok && route.provider) preferredExitProvider = route.provider;
    return route;
  } catch (e) {
    return { ok: false, error: e.message, elapsedMs: Date.now() - startedAt };
  }
}

// ── Auto-detect country (real exit IP > ChatGPT's cached view > timezone) ──
async function detectCountry(cdp) {
  // ⭐ Bypass ChatGPT's strict CSP so we can fetch external APIs
  try { await cdp.send('Page.setBypassCSP', { enabled: true }); } catch {}

  // ═══ PRIORITY 1: exit IP of the active proxy/extension route ═══
  // /backend-api/me reports the region the session was established in, so it
  // lags behind an extension country switch and must not win here.
  const route = await probeExitCountry(cdp);
  const routeCountry = route.ok ? route.country : null;

  // ═══ PRIORITY 2: ChatGPT's own view, kept to report region drift ═══
  let accountCountry = null;
  try {
    const res = await cdp.send('Runtime.evaluate', {
      expression: `fetch('/backend-api/me',{credentials:'include',cache:'no-store'}).then(r=>r.json()).then(d=>d.country||'').catch(()=>'')`,
      awaitPromise: true, returnByValue: true, timeout: 8000
    });
    const value = (res.result?.value || '').toUpperCase().trim();
    if (/^[A-Z]{2}$/.test(value)) accountCountry = value;
  } catch {}

  if (routeCountry) {
    return {
      country: routeCountry,
      source: `exit IP (${route.provider})`,
      accountCountry,
      reliable: true,
      drifted: !!accountCountry && accountCountry !== routeCountry
    };
  }
  if (accountCountry) {
    return {
      country: accountCountry,
      source: 'ChatGPT /me',
      accountCountry,
      reliable: true,
      drifted: false
    };
  }

  // ═══ PRIORITY 3: Browser timezone → country (last resort) ═══
  try {
    const tzRes = await cdp.send('Runtime.evaluate', {
      expression: `Intl.DateTimeFormat().resolvedOptions().timeZone`,
      returnByValue: true, timeout: 3000
    });
    const tz = tzRes.result?.value;
    if (tz && TIMEZONE_TO_COUNTRY[tz]) {
      // The machine timezone reflects the real location, not the proxy, so it
      // must never silently decide the billing country.
      return {
        country: TIMEZONE_TO_COUNTRY[tz],
        source: `timezone (${tz})`,
        accountCountry,
        reliable: false,
        drifted: !!accountCountry && accountCountry !== TIMEZONE_TO_COUNTRY[tz]
      };
    }
  } catch {}

  return null;
}

// ── Detect trial offer or upgrade surface in the page ──
// The plan surface is recognised from the page itself, not from the URL: after a
// reload the pricing hash can be gone while the plan cards are still shown.
const OFFER_DETECTION_EXPRESSION = `(() => {
  try {
    if (!document || !document.body) return JSON.stringify({found:false});

    const visible = el => {
      if (!el || el.offsetParent === null) return false;
      const rect = el.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    };
    const text = el => (el.textContent || '').replace(/\\s+/g, ' ').trim();

    // Priority 1: an actual free-trial offer, which we want to skip.
    const claimBtn = document.querySelector('button[aria-label="Claim offer"]');
    if (visible(claimBtn)) return JSON.stringify({found:true, type:'trial', mode:'claim'});
    const trialLabels = ['free offer', 'claim free offer', 'start free trial', 'try it free'];
    const freeBtn = Array.from(document.querySelectorAll('button, a[role="button"]'))
      .find(el => visible(el) && trialLabels.includes(text(el).toLowerCase()));
    if (freeBtn) return JSON.stringify({found:true, type:'trial', mode:'free', buttonLabel: text(freeBtn)});

    // Priority 2: an upgrade action, which means no trial is being offered.
    const upgradeLabels = [
      'upgrade to plus', 'get plus', 'rejoin plus', 'subscribe to plus',
      'reactivate plus', 'go plus', 'upgrade to chatgpt plus', 'resubscribe to plus'
    ];
    const upgradeEl = Array.from(document.querySelectorAll('button, a[role="button"], a[href*="checkout"]'))
      .find(el => visible(el) && upgradeLabels.includes(text(el).toLowerCase()));
    if (!upgradeEl) return JSON.stringify({found:false});

    // The sidebar always shows a generic upgrade entry point for free accounts,
    // so a plan surface must be present before offering to build a pay link.
    const urlSuggestsPricing = location.hash.includes('pricing') ||
      location.pathname.includes('pricing') ||
      location.search.includes('promo_campaign');

    const inDialog = !!upgradeEl.closest('[role="dialog"], dialog');
    const bodyText = text(document.body).toLowerCase();
    const mentionsPlusPlan = bodyText.includes('chatgpt plus') || bodyText.includes('your ai assistant');
    const showsMonthlyPrice = /\\/\\s*month|per month|\\/mo\\b/i.test(bodyText);
    const planSurface = inDialog || (mentionsPlusPlan && showsMonthlyPrice);

    if (!urlSuggestsPricing && !planSurface) {
      return JSON.stringify({found:false, reason:'upgrade button without a plan surface'});
    }

    return JSON.stringify({
      found: true,
      type: 'noTrial',
      buttonLabel: text(upgradeEl),
      surface: urlSuggestsPricing ? 'pricing-url' : (inDialog ? 'plan-dialog' : 'plan-cards')
    });
  } catch (e) { return JSON.stringify({found:false, err:e.message}); }
})()`;

async function detectOffer(cdp) {
  try {
    const res = await cdp.send('Runtime.evaluate', {
      expression: OFFER_DETECTION_EXPRESSION,
      returnByValue: true, timeout: 5000
    });
    return JSON.parse(res.result?.value || '{}');
  } catch { return { found: false }; }
}

// ── Get access token (page first, session file fallback) ──
async function getAccessToken(cdp, apiSession) {
  // Try page /api/auth/session with retry
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `fetch('/api/auth/session',{credentials:'include',cache:'no-store'}).then(r=>r.json()).then(d=>JSON.stringify({token:d.accessToken||null,account:(typeof d.account==='string'?d.account:(d.account&&d.account.id)||null)})).catch(e=>JSON.stringify({error:e.message}))`,
        awaitPromise: true, returnByValue: true, timeout: 20000
      }, null, 25000);
      const data = JSON.parse(res.result?.value || '{}');
      if (data.token) return { token: data.token, account: data.account, source: 'page' };
    } catch {}
    await sleep(1000);
  }

  // Fallback: use accessToken from session_api.json
  if (apiSession.accessToken) {
    const account = apiSession.account?.id || null;
    return { token: apiSession.accessToken, account, source: 'file' };
  }

  throw new Error('No access token available (page fetch failed, session_api.json has no accessToken)');
}

// ── Generate direct pay link ──
async function generateDirectPayLink(cdp, apiSession, country) {
  const currency = currencyForCountry(country);

  console.log('  📥 Fetching access token...');
  const auth = await getAccessToken(cdp, apiSession);
  console.log(`     Token source: ${auth.source}${auth.account ? ` | account: ${auth.account.slice(0,8)}...` : ''}`);

  const body = {
    entry_point: 'all_plans_pricing_modal',
    plan_name: 'chatgptplusplan',
    billing_details: { country, currency },
    checkout_ui_mode: 'custom'
  };

  // The sentinel call is requested separately and time-boxed: it has been
  // observed taking ~30s, and checkout succeeds without it.
  const sentinelExpr = `
    (async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 12000);
      try {
        const res = await fetch('/backend-api/sentinel/chat-requirements', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + ${JSON.stringify(auth.token)},
            'OAI-Language': 'en-US'
          },
          body: JSON.stringify({}),
          credentials: 'include',
          signal: controller.signal
        });
        const data = res.ok ? await res.json().catch(() => ({})) : {};
        return JSON.stringify({ status: res.status, token: data.token || null });
      } catch (e) {
        return JSON.stringify({ status: 0, token: null, error: e.name === 'AbortError' ? 'timed out after 12s' : e.message });
      } finally { clearTimeout(timer); }
    })()
  `;

  let sentinel = { status: 0, token: null };
  try {
    const sentinelRes = await cdp.send('Runtime.evaluate', {
      expression: sentinelExpr, awaitPromise: true, returnByValue: true, timeout: 20000
    }, null, 25000);
    sentinel = JSON.parse(sentinelRes.result?.value || '{}');
  } catch (e) {
    sentinel = { status: 0, token: null, error: e.message };
  }
  console.log(`     Sentinel: HTTP ${sentinel.status || 0}${sentinel.token ? ' (token acquired)' : ` (continuing without token${sentinel.error ? `: ${sentinel.error}` : ''})`}`);

  const evalExpr = `
    (async () => {
      try {
        const authHeader = 'Bearer ' + ${JSON.stringify(auth.token)};
        const accountHeader = ${JSON.stringify(auth.account || null)};
        const sentinelToken = ${JSON.stringify(sentinel.token || null)};
        const sentinelStatus = ${JSON.stringify(sentinel.status || 0)};

        const headers = {
          'Content-Type': 'application/json',
          'Authorization': authHeader,
          'OAI-Language': 'en-US'
        };
        if (accountHeader) headers['ChatGPT-Account-ID'] = accountHeader;
        if (sentinelToken) headers['openai-sentinel-chat-requirements-token'] = sentinelToken;

        const res = await fetch('/backend-api/payments/checkout', {
          method: 'POST', headers,
          body: ${JSON.stringify(JSON.stringify(body))},
          credentials: 'include'
        });
        const text = await res.text();
        return JSON.stringify({ ok: res.ok, status: res.status, body: text, hadSentinel: !!sentinelToken, sentinelStatus });
      } catch(e) { return JSON.stringify({error: e.message}); }
    })()
  `;

  // Checkout itself has been seen taking several seconds under a fresh proxy
  // route, so the page timeout and the CDP timeout both allow for that.
  const evalRes = await cdp.send('Runtime.evaluate', {
    expression: evalExpr, awaitPromise: true, returnByValue: true, timeout: 90000
  }, null, 100000);
  const resp = JSON.parse(evalRes.result?.value || '{}');
  if (resp.error) throw new Error('Fetch error: ' + resp.error);

  if (!resp.ok) {
    const errBody = String(resp.body).slice(0, 400);
    throw new Error(`Checkout HTTP ${resp.status} (sentinel: ${resp.sentinelStatus}, used: ${resp.hadSentinel}): ${errBody}`);
  }

  const data = JSON.parse(resp.body);
  const checkoutSessionId = data.checkout_session_id;
  if (!checkoutSessionId) throw new Error('No checkout_session_id in response: ' + resp.body.slice(0, 200));

  const processor = data.processor_entity || 'openai_llc';
  const payUrl = `https://chatgpt.com/checkout/${processor}/${checkoutSessionId}`;

  return {
    url: payUrl,
    country: data.billing_details?.country,
    currency: data.billing_details?.currency,
    sentinelUsed: resp.hadSentinel
  };
}

async function openInNewTab(cdp, url) {
  await cdp.send('Target.createTarget', { url });
}

const PAY_LINK_FILE = './pay_links.txt';

function savePayLink(result, apiSession) {
  const line = [
    new Date().toISOString(),
    apiSession.user?.email || 'unknown',
    result.country,
    result.currency,
    result.url
  ].join(' | ');
  try {
    fs.appendFileSync(PAY_LINK_FILE, line + '\n');
    return path.resolve(PAY_LINK_FILE);
  } catch {
    return null;
  }
}

// Decides whether a failed request says anything about our own route. Ad and
// telemetry frames fail through proxies constantly and must not start recovery.
const ROUTE_FAILURE_ERRORS = /(ERR_PROXY_CONNECTION_FAILED|ERR_TUNNEL_CONNECTION_FAILED|ERR_CONNECTION_RESET|ERR_CONNECTION_CLOSED|ERR_NETWORK_CHANGED|ERR_INTERNET_DISCONNECTED)/i;

function classifyRequestFailure({ errorText = '', type = '', url = null }) {
  if (!['Document', 'XHR', 'Fetch'].includes(type)) {
    return { relevant: false, reason: 'resource type is not a document or API call' };
  }

  const routeFailure = ROUTE_FAILURE_ERRORS.test(errorText);
  const mainDocumentTimeout = type === 'Document' && /ERR_TIMED_OUT/i.test(errorText);
  if (!routeFailure && !mainDocumentTimeout) {
    return { relevant: false, reason: 'error is not a route failure' };
  }

  let host = '';
  try { host = url ? new URL(url).hostname : ''; } catch {}
  if (/(^|\.)chatgpt\.com$/i.test(host)) return { relevant: true, host };
  // This tab only ever navigates to chatgpt.com or about:blank, so an
  // unidentified main-document failure is still ours.
  if (!url && type === 'Document') return { relevant: true, host: null };
  return { relevant: false, reason: 'third-party host', host: host || null };
}

const MAX_COOKIE_BYTES = 4096;

async function installSessionCookie(cdp, sessionToken) {
  // Chrome rejects cookies above ~4KB; those accounts rely on header injection
  // (and NextAuth's own chunked .0/.1 cookies) instead.
  if (sessionToken.length + COOKIE_NAME.length + 1 > MAX_COOKIE_BYTES) return false;
  try {
    const result = await cdp.send('Network.setCookie', {
      name: COOKIE_NAME,
      value: sessionToken,
      url: 'https://chatgpt.com/',
      path: '/',
      secure: true,
      httpOnly: true,
      sameSite: 'Lax'
    });
    return result?.success !== false;
  } catch {
    return false;
  }
}

// Load-balancer and region-affinity cookies keep routing the session to the
// edge chosen for the previous exit IP, which preserves the old currency.
const EDGE_AFFINITY_COOKIES = [
  '__oailb', '__cflb', 'oai-hlib', 'oai-sc', 'oai-allow-ne', 'oai-nav-state'
];

// Cloudflare's bot-management and clearance cookies are refreshed automatically
// on a new IP. Deleting them proactively only invites a challenge, so they are
// dropped solely after a challenge has already been observed.
const CHALLENGE_COOKIES = ['__cf_bm', '_cfuvid', 'cf_clearance'];

async function clearRegionPinnedCookies(cdp, includeChallenge = false) {
  const names = includeChallenge
    ? [...EDGE_AFFINITY_COOKIES, ...CHALLENGE_COOKIES]
    : EDGE_AFFINITY_COOKIES;
  const cleared = [];
  for (const name of names) {
    for (const domain of ['chatgpt.com', '.chatgpt.com']) {
      try {
        await cdp.send('Network.deleteCookies', { name, domain, path: '/' });
        cleared.push(`${name}@${domain}`);
      } catch {}
    }
  }
  return cleared;
}

// The jar can hold several cookies with this name (our injected host-only copy
// plus the server's domain copy). Returning "the first match" made the active
// token flip back and forth, so duplicates are reported and reconciled.
async function readSessionCookies(cdp) {
  try {
    const result = await cdp.send('Network.getCookies', {
      urls: ['https://chatgpt.com/', 'https://chatgpt.com/api/auth/session']
    });
    const cookies = result.cookies || [];
    const whole = cookies.filter(c => c.name === COOKIE_NAME && (c.value || '').length >= 20);
    if (whole.length) return whole;

    // NextAuth splits large sessions into COOKIE_NAME.0, COOKIE_NAME.1, ...
    const chunkPattern = new RegExp(`^${COOKIE_NAME.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\.(\\d+)$`);
    const chunks = cookies
      .map(c => ({ cookie: c, index: Number((c.name.match(chunkPattern) || [])[1]) }))
      .filter(entry => Number.isInteger(entry.index))
      .sort((a, b) => a.index - b.index);
    if (!chunks.length) return [];

    const value = chunks.map(entry => entry.cookie.value || '').join('');
    if (value.length < 20) return [];
    return [{ ...chunks[0].cookie, name: COOKIE_NAME, value, chunked: true }];
  } catch {
    return [];
  }
}

function parseSessionTokenFromSetCookie(headerValue) {
  const lines = String(headerValue || '').split('\n').map(line => line.trim());
  const readValue = (line, prefix) => line.slice(prefix.length).split(';')[0].trim();

  const whole = lines.find(line => line.startsWith(`${COOKIE_NAME}=`));
  if (whole) {
    const value = readValue(whole, `${COOKIE_NAME}=`);
    if (value.length >= 20) return value;
  }

  const chunkPattern = new RegExp(`^${COOKIE_NAME.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\.(\\d+)=`);
  const chunks = lines
    .map(line => ({ line, index: Number((line.match(chunkPattern) || [])[1]) }))
    .filter(entry => Number.isInteger(entry.index))
    .sort((a, b) => a.index - b.index);
  if (!chunks.length) return null;

  const value = chunks
    .map(entry => readValue(entry.line, entry.line.slice(0, entry.line.indexOf('=') + 1)))
    .join('');
  return value.length >= 20 ? value : null;
}

async function main() {
  console.log('\n' + '='.repeat(60));
  console.log('  🎯 ChatGPT Manual Browser Smart (v8.0 - Progress Driven)');
  console.log('='.repeat(60) + '\n');

  const diagnosticsPath = path.join(os.tmpdir(), 'chatgpt_proxy_diagnostics.jsonl');
  try { fs.writeFileSync(diagnosticsPath, ''); } catch {}

  // Every event goes to the file; TRACE also mirrors it to the terminal so the
  // cause of a stuck country switch is observed instead of guessed.
  const trace = (event, details = {}, line = null) => {
    try {
      fs.appendFileSync(diagnosticsPath, JSON.stringify({
        at: new Date().toISOString(),
        sinceStartMs: Date.now() - startedAt,
        event,
        ...details
      }) + '\n');
    } catch {}
    if (TRACE_ENABLED && line) writeLine(`  ${elapsedStamp()} ${line}`);
  };

  console.log(`  🧾 Trace stream: ${TRACE_ENABLED ? 'ON' : 'OFF'} (TRACE=0 silence | TRACE_ASSETS=1 assets | TRACE_TELEMETRY=1 analytics)`);
  console.log(`  🗂️  Diagnostics file: ${diagnosticsPath}\n`);

  if (!fs.existsSync('./session_api.json')) {
    console.error('❌ session_api.json not found');
    process.exit(1);
  }
  const apiSession = JSON.parse(fs.readFileSync('./session_api.json', 'utf8'));

  let sessionToken = apiSession.sessionToken;
  if (typeof sessionToken !== 'string') { console.error('❌ sessionToken missing'); process.exit(1); }
  sessionToken = sessionToken.trim().replace(/[\r\n\t]/g, '');
  if (sessionToken.length < 20) { console.error('❌ sessionToken invalid'); process.exit(1); }

  const idp = apiSession.user?.idp || 'unknown';
  const cookieSize = sessionToken.length + COOKIE_NAME.length + 1;

  console.log(`  📧 ${apiSession.user?.email || 'unknown'}`);
  console.log(`  🔐 IDP: ${idp}${idp === 'google-oauth2' ? ' (Google OAuth)' : ''}`);
  console.log(`  📏 Cookie size: ${cookieSize} bytes ${cookieSize > 4096 ? '(Fetch injection active)' : ''}`);
  console.log(`  🎫 accessToken in file: ${apiSession.accessToken ? 'YES (fallback ready)' : 'NO'}`);
  console.log('');

  // Load proxies
  let proxies = [];
  try {
    const pf = JSON.parse(fs.readFileSync('./proxies.json', 'utf8'));
    proxies = (Array.isArray(pf) ? pf : [])
      .map(p => typeof p === 'string' ? p : (p.proxyString || p.proxy_string || p.url))
      .filter(Boolean);
  } catch {}

  let proxyStr = null;
  if (proxies.length > 0) {
    const useProxy = ((await ask('  Use proxy? [Y/n]: ')) || '').toLowerCase();
    if (useProxy !== 'n') {
      proxyStr = proxies[0];
      if (proxyStr.includes('{COUNTRY}')) proxyStr = proxyStr.replace(/\{COUNTRY\}/gi, 'us');
      console.log(`  ✓ Using proxy: ${proxyStr.slice(0, 50)}...`);
    } else {
      console.log('  ✓ Direct connection');
    }
  } else {
    console.log('  ℹ️ No proxies.json - direct connection');
  }

  const localPort = 12345 + Math.floor(Math.random() * 100);
  let proxyHandle = null;
  if (proxyStr) proxyHandle = await startProxyRelay(proxyStr, localPort);

  const chromePath = findChrome();
  if (!chromePath) { console.error('❌ Chrome not found'); process.exit(1); }

  const userDir = path.join(os.tmpdir(), `real_chrome_${Date.now()}`);
  fs.mkdirSync(userDir);
  fs.mkdirSync(path.join(userDir, 'Default'), { recursive: true });

  fs.writeFileSync(path.join(userDir, 'Default', 'Preferences'), JSON.stringify({
    profile: { exit_type: 'Normal', exited_cleanly: true, last_engagement_time: Date.now() * 1000,
      default_content_setting_values: { notifications: 2 } },
    intl: { accept_languages: 'en-US,en' },
    browser: { has_seen_welcome_page: true },
    signin: { allowed: true },
    credentials_enable_service: false
  }));

  const cdpPort = 9333 + Math.floor(Math.random() * 100);

  // ⭐ Auto-discover extensions from ./extensions/ folder
  // Each subfolder = one extension. Handles Chrome Web Store metadata that
  // otherwise blocks unpacked loading (key, update_url, differential_fingerprint).
  const extensionsDir = path.resolve('./extensions');
  const extensionPaths = [];
  if (fs.existsSync(extensionsDir)) {
    try {
      const entries = fs.readdirSync(extensionsDir, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isDirectory()) continue;
        const extDir = path.join(extensionsDir, entry.name);
        let manifestDir = null;

        // Direct: extensions/<name>/manifest.json
        if (fs.existsSync(path.join(extDir, 'manifest.json'))) {
          manifestDir = extDir;
          console.log(`  🧩 Found extension: ${entry.name}`);
        } else {
          // Chrome layout: extensions/<id>/<version>/manifest.json
          try {
            const subs = fs.readdirSync(extDir, { withFileTypes: true }).filter(s => s.isDirectory());
            for (const sub of subs) {
              const subDir = path.join(extDir, sub.name);
              if (fs.existsSync(path.join(subDir, 'manifest.json'))) {
                manifestDir = subDir;
                console.log(`  🧩 Found extension: ${entry.name}/${sub.name}`);
                break;
              }
            }
          } catch {}
        }
        if (!manifestDir) continue;

        // Copy to temp dir under profile + strip Web Store restrictions
        const tempExtDir = path.join(userDir, 'ext_' + entry.name);
        try {
          const copyRecursive = (src, dest) => {
            fs.mkdirSync(dest, { recursive: true });
            for (const it of fs.readdirSync(src, { withFileTypes: true })) {
              const s = path.join(src, it.name);
              const d = path.join(dest, it.name);
              if (it.isDirectory()) copyRecursive(s, d);
              else fs.copyFileSync(s, d);
            }
          };
          copyRecursive(manifestDir, tempExtDir);

          const manifestPath = path.join(tempExtDir, 'manifest.json');
          const manifestRaw = fs.readFileSync(manifestPath, 'utf8').replace(/^\uFEFF/, '');
          const manifest = JSON.parse(manifestRaw);
          delete manifest.key;
          delete manifest.update_url;
          delete manifest.differential_fingerprint;
          fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

          extensionPaths.push(tempExtDir);
          console.log(`     └─ Prepared: "${manifest.name || 'unknown'}" (mv${manifest.manifest_version || '?'})`);
        } catch (e) {
          console.log(`     ⚠️ Failed to prepare ${entry.name}: ${e.message}`);
        }
      }
    } catch (e) {
      console.log(`  ⚠️ Extensions folder read error: ${e.message}`);
    }
  }

  const args = [
    `--remote-debugging-port=${cdpPort}`,
    `--user-data-dir=${userDir}`,
    '--no-first-run', '--no-default-browser-check',
    '--disable-blink-features=AutomationControlled',
    '--exclude-switches=enable-automation',
    '--start-maximized',
    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    '--enable-features=NetworkService,NetworkServiceInProcess',
    '--disable-features=OptimizationHints,MediaRouter'
  ];

  // Add extension loading flag if any extensions were prepared
  if (extensionPaths.length > 0) {
    args.push(`--load-extension=${extensionPaths.join(',')}`);
    console.log(`  ✅ Loading ${extensionPaths.length} extension(s)`);
  } else if (fs.existsSync(extensionsDir)) {
    console.log(`  ℹ️ ./extensions/ exists but no valid extensions found`);
  }
  if (proxyStr) {
    args.push(`--proxy-server=http://127.0.0.1:${localPort}`);
    args.push('--proxy-bypass-list=<-loopback>');
  }
  args.push('about:blank');

  console.log('\n  🚀 Opening Chrome...');
  const chrome = spawn(chromePath, args, { stdio: 'ignore' });
  // connectCDP polls for the debugging endpoint, so only a short pause is
  // needed before handing over to that retry loop.
  await sleep(800);

  let cleaning = false;
  const cleanup = () => {
    if (cleaning) return; cleaning = true;
    try { chrome.kill(); } catch {}
    if (proxyHandle) proxyHandle.close();
    if (sharedRL) { try { sharedRL.close(); } catch {} }
    setTimeout(() => { try { fs.rmSync(userDir, { recursive: true, force: true }); } catch {} }, 1000);
  };
  process.on('SIGINT', () => { console.log('\n  👋 Shutting down...'); cleanup(); process.exit(0); });

  const ws = await connectCDP(cdpPort);
  if (!ws) { console.error('❌ CDP failed'); cleanup(); process.exit(1); }
  const cdp = cdpClient(ws);

  try {
    await cdp.send('Network.enable');
    await cdp.send('Page.enable');
    await cdp.send('Runtime.enable');
  } catch (e) { console.error('❌ CDP setup:', e.message); cleanup(); process.exit(1); }
  // Keep route probes independent from ChatGPT's CSP and stale service worker.
  try { await cdp.send('Page.setBypassCSP', { enabled: true }); } catch {}
  try { await cdp.send('Network.setBypassServiceWorker', { bypass: true }); } catch {}

  // Last session cookie value issued by the server, which wins over the jar.
  let serverSessionToken = null;
  // Tracks whether the visible tab currently holds a real chatgpt.com document,
  // so in-page polling is skipped on error/blank pages.
  let pageIsChatGPT = false;
  // Timestamp of the last ChatGPT network event. Waiting logic uses this so a
  // slow but progressing connection is never abandoned on a clock alone.
  let lastChatGPTActivityAt = Date.now();
  const noteChatGPTActivity = url => {
    if (/(^|\/\/|\.)chatgpt\.com(\/|$)/i.test(String(url || ''))) lastChatGPTActivityAt = Date.now();
  };

  // Once the server has issued a session cookie it is authoritative; the jar is
  // only consulted while no server cookie has been seen yet.
  function adoptSessionToken(token, source) {
    if (!token || token === sessionToken) return false;
    const previousFingerprint = fingerprint(sessionToken);
    sessionToken = token;
    trace('session_cookie_rotated', {
      source,
      previousFingerprint,
      fingerprint: fingerprint(sessionToken),
      cookieSize: sessionToken.length + COOKIE_NAME.length + 1
    }, `🔑 session cookie updated (${source}): ${previousFingerprint} → ${fingerprint(sessionToken)}`);
    return true;
  }

  async function syncActiveSessionCookie(source) {
    const cookies = await readSessionCookies(cdp);
    if (!cookies.length) return false;

    const distinct = new Set(cookies.map(c => c.value));
    if (distinct.size > 1) {
      // Keep the authoritative value and remove the stale duplicates that made
      // the active token oscillate between two fingerprints.
      const keep = serverSessionToken && distinct.has(serverSessionToken)
        ? serverSessionToken
        : cookies[0].value;
      for (const cookie of cookies) {
        if (cookie.value === keep) continue;
        try {
          await cdp.send('Network.deleteCookies', {
            name: COOKIE_NAME,
            domain: cookie.domain,
            path: cookie.path || '/'
          });
        } catch {}
      }
      trace('session_cookie_duplicates_resolved', {
        source,
        removed: distinct.size - 1,
        domains: cookies.map(c => c.domain)
      }, `🔑 removed ${distinct.size - 1} duplicate session cookie(s)`);
      // Re-installing here would immediately recreate the duplicate that was
      // just removed, which produced a dedup message every few seconds.
      adoptSessionToken(keep, `${source} (deduplicated)`);
      return true;
    }

    if (serverSessionToken) return false;
    return adoptSessionToken(cookies[0].value, source);
  }

  try { await cdp.send('Log.enable'); } catch {}

  // ── Full-visibility event stream ──────────────────────────────────────────
  const inFlight = new Map(); // requestId → { url, type, method, startedAt }
  const isAsset = type => ASSET_TYPES.has(type);

  cdp.on('Network.requestWillBeSent', params => {
    const url = params.request?.url || '';
    const type = params.type || 'Other';
    noteChatGPTActivity(url);
    inFlight.set(params.requestId, {
      url,
      type,
      method: params.request?.method || 'GET',
      startedAt: Date.now()
    });
    const quiet = (isAsset(type) && !TRACE_ASSETS) || isTelemetryUrl(url);
    trace('request', {
      requestId: params.requestId,
      method: params.request?.method,
      type,
      url,
      headers: redactHeaders(params.request?.headers)
    }, quiet ? null : `→ ${params.request?.method || 'GET'} ${type} ${shortenUrl(url)}`);
  });

  cdp.on('Network.requestWillBeSentExtraInfo', params => {
    // DomainMismatch on third-party edge cookies is normal and would otherwise
    // flood the stream; only the session cookie matters for that reason.
    const blocked = (params.associatedCookies || [])
      .filter(entry => (entry.blockedReasons || []).length)
      .filter(entry => entry.cookie?.name === COOKIE_NAME ||
        (entry.blockedReasons || []).some(reason => reason !== 'DomainMismatch'))
      .map(entry => `${entry.cookie?.name}:${(entry.blockedReasons || []).join('|')}`);
    if (!blocked.length) return;
    trace('cookies_blocked_on_request', {
      requestId: params.requestId,
      blocked
    }, `🚫 cookies blocked → ${blocked.join(', ')}`);
  });

  cdp.on('Network.responseReceived', params => {
    const info = inFlight.get(params.requestId);
    const type = params.type || info?.type || 'Other';
    const response = params.response || {};
    noteChatGPTActivity(response.url);
    const durationMs = info ? Date.now() - info.startedAt : null;
    const quiet = response.status < 400 &&
      ((isAsset(type) && !TRACE_ASSETS) || isTelemetryUrl(response.url));
    trace('response', {
      requestId: params.requestId,
      status: response.status,
      type,
      url: response.url,
      remoteIP: response.remoteIPAddress || null,
      protocol: response.protocol || null,
      fromDiskCache: !!response.fromDiskCache,
      fromServiceWorker: !!response.fromServiceWorker,
      durationMs,
      headers: redactHeaders(response.headers)
    }, quiet ? null : `← ${response.status} ${type} ${shortenUrl(response.url)} ip=${response.remoteIPAddress || '?'}${durationMs === null ? '' : ` ${durationMs}ms`}`);
  });

  cdp.on('Network.loadingFinished', params => {
    inFlight.delete(params.requestId);
  });

  // A Cloudflare challenge looks like a logged-out session unless it is named.
  cdp.on('Network.responseReceived', params => {
    const response = params.response || {};
    if (response.status !== 403 && response.status !== 503) return;
    if (!/chatgpt\.com/i.test(response.url || '')) return;
    const headers = Object.fromEntries(
      Object.entries(response.headers || {}).map(([k, v]) => [k.toLowerCase(), String(v)])
    );
    const mitigated = headers['cf-mitigated'] || null;
    const isHtml = /text\/html/i.test(headers['content-type'] || '');
    if (!mitigated && !isHtml) return;
    challengeSeenAt = Date.now();
    trace('cloudflare_challenge', {
      status: response.status,
      url: response.url,
      mitigated,
      remoteIP: response.remoteIPAddress || null
    }, `🛡️  Cloudflare challenge ${response.status}${mitigated ? ` (${mitigated})` : ''} on ${shortenUrl(response.url)}`);
  });

  cdp.on('Network.responseReceivedExtraInfo', params => {
    const rawSetCookie = params.headers?.['set-cookie'] || params.headers?.['Set-Cookie'] || '';
    const setCookieNames = String(rawSetCookie)
      .split('\n')
      .map(line => line.split('=')[0].trim())
      .filter(Boolean);
    const blocked = (params.blockedCookies || [])
      .map(entry => `${entry.cookie?.name || entry.cookieLine?.split('=')[0]}:${(entry.blockedReasons || []).join('|')}`);

    if (setCookieNames.length || blocked.length) {
      trace('response_cookies', {
        requestId: params.requestId,
        setCookieNames,
        blocked
      }, `🍪 set-cookie [${setCookieNames.join(', ') || 'none'}]${blocked.length ? ` blocked [${blocked.join(', ')}]` : ''}`);
    }

    const issued = parseSessionTokenFromSetCookie(rawSetCookie);
    if (issued) {
      serverSessionToken = issued;
      adoptSessionToken(issued, 'server set-cookie');
    }
  });

  cdp.on('Page.frameNavigated', params => {
    if (params.frame?.parentId) return;
    let host = '';
    try { host = new URL(params.frame?.url || '').hostname; } catch {}
    pageIsChatGPT = host === 'chatgpt.com' && !params.frame?.unreachableUrl;
    trace('frame_navigated', {
      url: params.frame?.url,
      host,
      status: params.frame?.unreachableUrl ? 'unreachable' : 'ok'
    }, `🧭 navigated ${shortenUrl(params.frame?.url || '')}`);
  });

  cdp.on('Page.domContentEventFired', () => {
    trace('dom_content_loaded', {}, '📄 DOMContentLoaded');
  });

  cdp.on('Page.loadEventFired', () => {
    trace('load_event', {}, '📄 load event fired');
  });

  cdp.on('Runtime.exceptionThrown', params => {
    const detail = params.exceptionDetails || {};
    trace('page_exception', {
      text: detail.text,
      message: detail.exception?.description?.slice(0, 300) || null,
      url: detail.url || null
    }, `💥 page exception: ${(detail.exception?.description || detail.text || '').split('\n')[0].slice(0, 160)}`);
  });

  cdp.on('Runtime.consoleAPICalled', params => {
    if (!['error', 'warning', 'assert'].includes(params.type)) return;
    const text = (params.args || [])
      .map(arg => arg.value ?? arg.description ?? arg.type)
      .join(' ')
      .slice(0, 220);
    trace('page_console', { level: params.type, text }, `🖥️  console.${params.type}: ${text}`);
  });

  cdp.on('Log.entryAdded', params => {
    const entry = params.entry || {};
    if (!['error', 'warning'].includes(entry.level)) return;
    trace('browser_log', {
      level: entry.level,
      source: entry.source,
      text: entry.text,
      url: entry.url || null
    }, `📕 ${entry.source}/${entry.level}: ${String(entry.text).slice(0, 200)}`);
  });

  try {
    await cdp.send('Page.addScriptToEvaluateOnNewDocument', {
      source: `
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
        window.chrome = { runtime: {} };
      `
    });
  } catch {}

  console.log('  🔧 Enabling Fetch interception...');
  await cdp.send('Fetch.enable', {
    patterns: [
      { urlPattern: 'https://chatgpt.com/*', requestStage: 'Request' },
      { urlPattern: 'https://*.chatgpt.com/*', requestStage: 'Request' },
      { urlPattern: 'https://*.openai.com/*', requestStage: 'Request' }
    ]
  });

  // ⭐ v5.6 + v5.7: Track paused requests with auto-recovery + verbose debug
  const pausedRequests = new Map(); // requestId → { timer, url, startedAt }
  let requestCounter = 0;
  let successCounter = 0;
  let timeoutCounter = 0;
  let errorCounter = 0;

  const shortUrl = url => shortenUrl(url, 60);

  cdp.on('Fetch.requestPaused', async (params) => {
    const requestId = params.requestId;
    const url = params.request?.url || '';
    const shouldInject = /^https:\/\/([a-z0-9-]+\.)?chatgpt\.com\//i.test(url);
    const short = shortUrl(url);
    const resourceType = params.resourceType || 'Other';
    requestCounter++;

    const quietIntercept = (isAsset(resourceType) && !TRACE_ASSETS) || isTelemetryUrl(url);
    {
      trace('fetch_intercepted', {
        requestId,
        method: params.request?.method,
        resourceType,
        url,
        inject: shouldInject,
        cookieFingerprint: shouldInject ? fingerprint(sessionToken) : null
      }, quietIntercept ? null : `⇢ intercept ${params.request?.method || 'GET'} ${resourceType} ${short}${shouldInject ? ` inject=${fingerprint(sessionToken)}` : ' inject=no'}`);
    }

    // Timeout timer
    const rescueTimer = setTimeout(async () => {
      timeoutCounter++;
      trace('fetch_rescue_timeout', { requestId, url, timeoutCounter },
        `⏱️  rescue timeout #${timeoutCounter} ${short}`);
      try { await cdp.send('Fetch.failRequest', { requestId, errorReason: 'TimedOut' }); } catch {}
      pausedRequests.delete(requestId);
    }, 8000);
    pausedRequests.set(requestId, { timer: rescueTimer, url, startedAt: Date.now() });

    try {
      if (!shouldInject) {
        await cdp.send('Fetch.continueRequest', { requestId });
      } else {
        const reqHeaders = params.request.headers || {};
        const newHeaders = [];
        let hadCookie = false;
        for (const [k, v] of Object.entries(reqHeaders)) {
          if (k.toLowerCase() === 'cookie') {
            hadCookie = true;
            const existing = String(v).split(/;\s*/).filter(c => c && !c.startsWith(COOKIE_NAME + '='));
            existing.push(`${COOKIE_NAME}=${sessionToken}`);
            newHeaders.push({ name: 'Cookie', value: existing.join('; ') });
          } else newHeaders.push({ name: k, value: String(v) });
        }
        if (!hadCookie) newHeaders.push({ name: 'Cookie', value: `${COOKIE_NAME}=${sessionToken}` });
        await cdp.send('Fetch.continueRequest', { requestId, headers: newHeaders });
      }
      clearTimeout(rescueTimer);
      pausedRequests.delete(requestId);
      successCounter++;
    } catch (e) {
      clearTimeout(rescueTimer);
      pausedRequests.delete(requestId);
      errorCounter++;
      trace('fetch_intercept_error', { requestId, url, error: e.message },
        `❌ intercept error #${errorCounter} ${short} — ${e.message}`);
      try { await cdp.send('Fetch.continueRequest', { requestId }); }
      catch { try { await cdp.send('Fetch.failRequest', { requestId, errorReason: 'Failed' }); } catch {} }
    }
  });

  // Tracks whether a question is open; the trace stream redraws it after logs.
  let awaitingInput = false;

  // ⭐ Leak detector: report if too many requests stuck
  const leakDetector = setInterval(() => {
    const pending = pausedRequests.size;
    if (pending > 10) {
      const oldest = [...pausedRequests.values()]
        .sort((a, b) => a.startedAt - b.startedAt)
        .slice(0, 3)
        .map(info => `${Math.round((Date.now() - info.startedAt) / 1000)}s:${shortUrl(info.url)}`);
      trace('paused_request_backlog', { pending, oldest },
        `⚠️  ${pending} paused requests | oldest ${oldest.join(' , ')}`);
    }
  }, 5000);

  // ⭐ Stats reporter
  const statsReporter = setInterval(() => {
    if (requestCounter === 0) return;
    trace('request_stats', {
      total: requestCounter,
      ok: successCounter,
      timeouts: timeoutCounter,
      errors: errorCounter,
      pending: pausedRequests.size,
      inFlight: inFlight.size
    }, `📊 requests ${requestCounter} total | ${successCounter} ok | ${timeoutCounter} timeout | ${errorCounter} error | ${pausedRequests.size} paused`);
  }, 15000);

  console.log('  ✅ Interceptor ready (request rescue: 8s, verbose logging on)\n');

  const cookieInstalled = await installSessionCookie(cdp, sessionToken);
  console.log(`  🍪 Session cookie: ${cookieInstalled ? 'stored in Chrome' : 'header injection fallback'}`);
  trace('session_cookie_install', { success: cookieInstalled, cookieSize });

  console.log('  🌐 Navigating to chatgpt.com...');
  await cdp.send('Page.navigate', { url: 'https://chatgpt.com/' });
  await waitForChatGPTDocument(3000, null, null, { idleGraceMs: 4000 });

  // Verify login. Attempts continue while the connection keeps answering, so a
  // slow proxy gets as many tries as it needs instead of a fixed five.
  let verifiedEmail = null;
  let initialCountry = null;
  for (let attempt = 1; attempt <= 12 && (attempt <= 5 || Date.now() - lastChatGPTActivityAt < 15000); attempt++) {
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `Promise.all([
          fetch('/api/auth/session',{credentials:'include',cache:'no-store'}).then(r=>r.json()),
          fetch('/backend-api/me',{credentials:'include',cache:'no-store'}).then(r=>r.ok?r.json():({}))
        ]).then(([session,me])=>JSON.stringify({
          ok:!!(session&&session.user),
          email:session?.user?.email,
          hasToken:!!session?.accessToken,
          country:me?.country||null
        })).catch(e=>JSON.stringify({ok:false,err:e.message}))`,
        awaitPromise: true, returnByValue: true, timeout: 40000
      }, null, 50000);
      const parsed = JSON.parse(res.result?.value || '{}');
      if (parsed.ok && parsed.hasToken) {
        verifiedEmail = parsed.email;
        initialCountry = parsed.country || null;
        break;
      }
    } catch {}
    await sleep(1500);
  }

  if (verifiedEmail) console.log(`  ✅ Logged in as: ${verifiedEmail}\n`);
  else console.log('  ⚠️ Login verification uncertain (will try anyway)\n');
  await syncActiveSessionCookie('initial login');
  if (initialCountry) console.log(`  🌍 Initial country: ${initialCountry} (${currencyForCountry(initialCountry)})\n`);

  // Country is detected JUST-IN-TIME at checkout, not here.
  // This way if user switches VPN/extension mid-session, we get the fresh country.

  // Keep-alive
  const keepAlive = setInterval(async () => {
    if (!cdp.isAlive()) { clearInterval(keepAlive); return; }
    try { await cdp.send('Browser.getVersion'); } catch {}
  }, 20000);
  const cookieSyncMonitor = setInterval(() => {
    if (cdp.isAlive() && !cleaning) void syncActiveSessionCookie('periodic sync');
  }, 5000);

  // Offer state (declared here so country monitor can reset it)
  let handled = false;
  let lastOfferSeen = false;

  // Connection state shared by all network signals.
  let lastKnownCountry = initialCountry;
  let activeRouteCountry = null;
  let autoReloadInProgress = false;
  let last401At = 0;
  let lastTransportFailureAt = 0;
  let challengeSeenAt = 0;

  // Polling stays fast for a minute after anything interesting happens, then
  // halves its rate so an idle session does not hammer the API every 4s.
  let lastInterestingAt = Date.now();
  let countryTick = 0;
  let snapshotTick = 0;
  const markActivity = () => { lastInterestingAt = Date.now(); };
  const inActivePeriod = () => Date.now() - lastInterestingAt < 60000;
  let recoveryPromise = null;
  let recoveryState = 'idle';
  let recoveryStateSince = Date.now();
  const pendingRecoveryReasons = new Set();

  const setRecoveryState = next => {
    if (recoveryState === next) return;
    const heldMs = Date.now() - recoveryStateSince;
    trace('recovery_state', { from: recoveryState, to: next, heldMs },
      `🧭 recovery state ${recoveryState} → ${next} (${heldMs}ms)`);
    recoveryState = next;
    recoveryStateSince = Date.now();
  };

  // Probe the freshly loaded page instead of assuming that Page.reload means
  // the session has recovered. During a proxy switch ChatGPT commonly returns
  // one logged-out/401 page before accepting the injected session cookie.
  async function probeChatGPTSession(sessionId = null) {
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `(async () => {
          const read = async url => {
            try {
              const response = await fetch(url, {credentials:'include', cache:'no-store'});
              const text = await response.text();
              let json = null;
              try { json = JSON.parse(text); } catch {}
              return {
                status: response.status,
                json,
                html: json === null && /^\\s*</.test(text),
                snippet: json === null ? text.slice(0, 80) : null
              };
            } catch (e) {
              return { status: 0, json: null, html: false, snippet: e.message };
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
            snippet: session.snippet || me.snippet || null
          });
        })()`,
        awaitPromise: true, returnByValue: true, timeout: 45000
      }, sessionId, 55000);
      return JSON.parse(res.result?.value || '{}');
    } catch (e) {
      return { status: 0, error: e.message };
    }
  }

  // Probes run in the page context, and Chrome's network error page rejects
  // every fetch with a chrome-error scheme error. Move to a blank page first so
  // the route check measures the proxy instead of the error page.
  async function ensureProbeableContext() {
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `JSON.stringify({protocol: location.protocol, host: location.hostname})`,
        returnByValue: true, timeout: 3000
      });
      const location = JSON.parse(res.result?.value || '{}');
      const brokenPage = /^chrome-error:/i.test(location.protocol || '') ||
        location.host === 'chromewebdata';
      if (!brokenPage) return false;
      trace('probe_context_reset', { protocol: location.protocol, host: location.host },
        '🧹 leaving Chrome error page so route probes can run');
      await cdp.send('Page.navigate', { url: 'about:blank' });
      await sleep(400);
      return true;
    } catch {
      return false;
    }
  }

  // Waits for a usable route. A changed exit country is preferred, but an
  // unchanged one still counts once the change window passes, so an auth-only
  // failure is not stuck waiting for a country that never changes.
  async function waitForHealthyProxyRoute(previousCountry, timeoutMs = 120000, changeWindowMs = 6000) {
    await ensureProbeableContext();
    const startedProbingAt = Date.now();
    const deadline = startedProbingAt + timeoutMs;
    let probeNumber = 0;
    let lastHealthy = null;

    while (Date.now() < deadline && cdp.isAlive() && !cleaning) {
      probeNumber++;
      setRecoveryState('probing-route');
      // Each attempt gives the route more time, so a genuinely slow proxy is
      // not written off as unreachable.
      const providerBudget = Math.min(4000 + (probeNumber - 1) * 3000, 20000);
      const route = await probeExitCountry(cdp, null, providerBudget);
      const changed = route.ok && (!previousCountry || route.country !== previousCountry);
      trace('proxy_route_probe', {
        probeNumber,
        ok: !!route.ok,
        country: route.country || null,
        provider: route.provider || null,
        changed,
        providerBudgetMs: providerBudget,
        elapsedMs: route.elapsedMs,
        error: route.error || null
      }, `🛰️  route probe #${probeNumber} ${route.ok ? route.country : `unreachable (budget ${providerBudget}ms)`}${route.ok && !changed ? ' (unchanged)' : ''} ${route.elapsedMs}ms`);

      if (changed) return { ...route, changed: true };
      if (route.ok) {
        lastHealthy = route;
        if (Date.now() - startedProbingAt >= changeWindowMs) {
          trace('proxy_route_unchanged_accepted', {
            country: route.country,
            previousCountry,
            waitedMs: Date.now() - startedProbingAt
          }, `🛰️  exit country stayed ${route.country}; continuing recovery`);
          return { ...route, changed: false };
        }
      }
      await sleep(800);
    }
    return lastHealthy ? { ...lastHealthy, changed: false } : null;
  }

  // Waits for the document while the connection is still making progress.
  // minWaitMs is a floor, not a deadline: as long as ChatGPT keeps producing
  // network events the wait continues, so a slow proxy is not mistaken for a
  // dead one. It stops when the connection has been silent for idleGraceMs.
  // expectedMarker guards against reading the previous document: without it a
  // still-loaded old page reports "complete" before the new navigation commits.
  async function waitForChatGPTDocument(minWaitMs = 10000, sessionId = null, expectedMarker = null, options = {}) {
    const idleGraceMs = options.idleGraceMs ?? 12000;
    const hardCapMs = options.hardCapMs ?? 240000;
    const startedAt = Date.now();
    lastChatGPTActivityAt = Date.now();

    const shouldKeepWaiting = () => {
      const elapsed = Date.now() - startedAt;
      if (elapsed >= hardCapMs) return false;
      if (elapsed < minWaitMs) return true;
      return Date.now() - lastChatGPTActivityAt < idleGraceMs;
    };

    while (shouldKeepWaiting() && cdp.isAlive()) {
      try {
        const res = await cdp.send('Runtime.evaluate', {
          expression: `JSON.stringify({
            host: location.hostname,
            href: location.href,
            ready: document.readyState,
            hasBody: !!document.body
          })`,
          returnByValue: true, timeout: 3000
        }, sessionId);
        const page = JSON.parse(res.result?.value || '{}');
        const committed = !expectedMarker || String(page.href || '').includes(expectedMarker);
        if (page.host === 'chatgpt.com' && page.hasBody && committed &&
            (page.ready === 'interactive' || page.ready === 'complete')) return true;
      } catch {}
      await sleep(350);
    }
    return false;
  }

  async function verifyStableSession(sessionId = null) {
    const first = await probeChatGPTSession(sessionId);
    if (!first.loggedIn || first.status !== 200 || !/^[A-Z]{2}$/i.test(first.country || '')) {
      return { ok: false, state: first };
    }

    await sleep(900);
    const second = await probeChatGPTSession(sessionId);
    const stable = second.loggedIn && second.status === 200 &&
      second.country === first.country;
    return { ok: stable, state: second, previousCountry: first.country };
  }

  // Only a real chatgpt.com location may contribute a path. On about:blank
  // location.pathname is "blank", which previously produced chatgpt.comblank.
  async function currentPageLocation() {
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `JSON.stringify({host: location.hostname, path: location.pathname, hash: location.hash})`,
        returnByValue: true, timeout: 3000
      });
      const page = JSON.parse(res.result?.value || '{}');
      const onChatGPT = page.host === 'chatgpt.com';
      const path = typeof page.path === 'string' && page.path.startsWith('/') ? page.path : '/';
      const hash = typeof page.hash === 'string' && page.hash.startsWith('#') ? page.hash : '';
      return {
        onChatGPT,
        path: onChatGPT ? path : '/',
        hash: onChatGPT ? hash : ''
      };
    } catch {
      return { onChatGPT: false, path: '/', hash: '' };
    }
  }

  // Writes our copy of the session cookie only when the jar has none, so the
  // browser's own cookie stays the single source of truth.
  async function ensureSessionCookiePresent(stage) {
    const cookies = await readSessionCookies(cdp);
    if (cookies.length) return false;
    const stored = await installSessionCookie(cdp, sessionToken);
    trace('session_cookie_restored', {
      stage,
      stored,
      fingerprint: fingerprint(sessionToken)
    }, `🍪 session cookie ${stored ? 'restored in the jar' : 'left to header injection'} (${stage})`);
    return stored;
  }

  async function releaseOldRegionState(stage) {
    try { await cdp.send('Network.clearBrowserCache'); } catch {}
    try { await cdp.send('Network.closeIdleConnections'); } catch {}
    const includeChallenge = challengeSeenAt > 0 && Date.now() - challengeSeenAt < 60000;
    const cleared = await clearRegionPinnedCookies(cdp, includeChallenge);
    trace('region_pinned_cookies_cleared', { stage, count: cleared.length, includeChallenge },
      `🧹 cleared ${cleared.length} IP-pinned cookie entries (${stage}${includeChallenge ? ', incl. challenge clearance' : ''})`);
    await ensureSessionCookiePresent(stage);
  }

  async function warmUpChatGPTInBackground(routeCountry, timeoutMs = 10000) {
    let targetId = null;
    try {
      await ensureSessionCookiePresent('background warmup');
      const target = await cdp.send('Target.createTarget', {
        url: 'about:blank',
        background: true
      });
      targetId = target.targetId;
      const attached = await cdp.send('Target.attachToTarget', {
        targetId,
        flatten: true
      });
      const sessionId = attached.sessionId;

      await cdp.send('Network.enable', {}, sessionId);
      await cdp.send('Page.enable', {}, sessionId);
      await cdp.send('Runtime.enable', {}, sessionId);
      try {
        await cdp.send('Network.setBypassServiceWorker', { bypass: true }, sessionId);
      } catch {}

      const warmupMarker = `proxy_warmup=${Date.now()}`;
      await cdp.send('Page.navigate', {
        url: `https://chatgpt.com/?${warmupMarker}`
      }, sessionId);
      const ready = await waitForChatGPTDocument(timeoutMs, sessionId, warmupMarker, { idleGraceMs: 15000 });
      const state = ready ? await probeChatGPTSession(sessionId) : { status: 0 };
      trace('background_warmup', {
        ready,
        loggedIn: !!state.loggedIn,
        status: state.status || 0,
        country: state.country || null,
        routeCountry
      }, `🔥 background warm-up ${ready ? 'loaded' : 'did not load'} (loggedIn=${!!state.loggedIn} me=${state.status || 0})`);
      return { ready, state };
    } catch (e) {
      trace('background_warmup_failed', { error: e.message, routeCountry });
      return { ready: false, state: { error: e.message } };
    } finally {
      if (targetId) {
        try { await cdp.send('Target.closeTarget', { targetId }); } catch {}
      }
      await syncActiveSessionCookie('background warmup');
    }
  }

  // Guarantees the visible tab is a working chatgpt.com document before any
  // checkout request, because relative fetches fail on error/blank pages.
  async function ensureChatGPTPageReady() {
    const page = await currentPageLocation();
    if (page.onChatGPT) {
      const probe = await probeChatGPTSession();
      if (probe.loggedIn && probe.status === 200) return true;
      trace('checkout_page_unhealthy', {
        loggedIn: !!probe.loggedIn,
        status: probe.status || 0,
        challenge: !!probe.challenge
      }, `⚠️ page is on chatgpt.com but not usable (loggedIn=${!!probe.loggedIn} me=${probe.status || 0})`);
    }

    console.log('  🔄 Restoring the ChatGPT page before requesting the pay link...');
    const marker = `checkout_ready=${Date.now()}`;
    try { await cdp.send('Page.navigate', { url: `https://chatgpt.com/?${marker}` }); } catch {}
    if (!await waitForChatGPTDocument(8000, null, marker)) {
      trace('checkout_page_recovery_failed', { stage: 'document_not_ready' });
      return false;
    }

    const probe = await probeChatGPTSession();
    trace('checkout_page_ready', {
      loggedIn: !!probe.loggedIn,
      status: probe.status || 0,
      country: probe.country || null
    }, `🔍 checkout page ready: loggedIn=${!!probe.loggedIn} me=${probe.status || 0} country=${probe.country || '?'}`);
    return !!probe.loggedIn && probe.status === 200;
  }

  // The first document through a freshly selected proxy can take 20s or more,
  // and sometimes times out entirely, so a few escalating attempts are allowed.
  // This is still driven by verified route health, not blind reloading.
  async function navigateOnceAndVerify(route, targetLocation, attempts = 3) {
    setRecoveryState('single-navigation');
    const page = `${targetLocation.path}${targetLocation.hash}`;
    let documentReady = false;
    let refreshMarker = '';

    for (let attempt = 1; attempt <= attempts && !documentReady; attempt++) {
      refreshMarker = `proxy_refresh=${Date.now()}`;
      const freshUrl = `https://chatgpt.com${targetLocation.path}?${refreshMarker}${targetLocation.hash}`;
      trace('navigation_attempt', { attempt, attempts, routeCountry: route.country, url: freshUrl },
        `🔄 loading ChatGPT on ${page} (attempt ${attempt}/${attempts})`);

      try { await cdp.send('Page.stopLoading'); } catch {}
      if (attempt > 1) {
        // Chrome's own error page retries on its own timer; moving to a blank
        // page first cancels that and frees the stalled sockets.
        try { await cdp.send('Page.navigate', { url: 'about:blank' }); } catch {}
        await sleep(400);
        try { await cdp.send('Network.closeIdleConnections'); } catch {}
      }
      try { await cdp.send('Network.setCacheDisabled', { cacheDisabled: true }); } catch {}
      await releaseOldRegionState(attempt === 1 ? 'before_navigation' : `before_retry_${attempt}`);

      let navigationError = null;
      try {
        const result = await cdp.send('Page.navigate', { url: freshUrl }, null, 60000);
        navigationError = result?.errorText || null;
      } catch (e) {
        navigationError = e.message;
      }
      if (navigationError) {
        trace('navigation_error', { attempt, error: navigationError },
          `⚠️ navigation reported ${navigationError}`);
      }

      documentReady = await waitForChatGPTDocument(8000 + (attempt - 1) * 4000, null, refreshMarker);
      if (!documentReady) {
        trace('navigation_timed_out', { attempt, routeCountry: route.country },
          `⚠️ attempt ${attempt} did not finish loading`);
      }
    }

    if (!documentReady) {
      console.log(`  ⚠️ ChatGPT never finished loading through this route after ${attempts} attempts.`);
      console.log('     The proxy answers other sites, so it is likely throttling chatgpt.com.');
      console.log(`     Diagnostics: ${diagnosticsPath}\n`);
      trace('recovery_failed', { stage: 'document_not_ready', routeCountry: route.country, attempts });
      return null;
    }

    setRecoveryState('verifying');
    const verification = await verifyStableSession();
    const state = verification.state || {};
    trace('session_verification', {
      ok: verification.ok,
      loggedIn: !!state.loggedIn,
      status: state.status || 0,
      country: state.country || null,
      challenge: !!state.challenge,
      snippet: state.snippet || null,
      routeCountry: route.country,
      error: state.error || null
    }, `🔍 after refresh: loggedIn=${!!state.loggedIn} me=${state.status || 0} country=${state.country || '?'}${state.challenge ? ' challenge=yes' : ''}`);

    if (verification.ok) {
      return acceptRecovery(state, route, 'single_navigation');
    }

    const detail = state.challenge
      ? `Cloudflare returned a challenge page (session ${state.sessionStatus || '?'}, me ${state.status || '?'})`
      : !state.loggedIn
        ? `session endpoint is logged out (HTTP ${state.sessionStatus || 0})`
        : `country endpoint returned HTTP ${state.status || 0}`;
    console.log(`  ⚠️ First refresh completed, but ${detail}.`);
    console.log(`     Diagnostics: ${diagnosticsPath}\n`);
    trace('recovery_failed', { stage: 'session_verification', detail });
    return null;
  }

  async function performRecovery(reasons) {
    setRecoveryState('probing-route');
    trace('recovery_started', { reasons, knownCountry: lastKnownCountry },
      `🔌 recovery started: ${reasons.join(', ')}`);

    // Force the health check to open a fresh route through the extension.
    try { await cdp.send('Network.closeIdleConnections'); } catch {}

    // A dropped tunnel means the extension is mid-switch, so allow more time
    // for the new exit country; an auth-only failure needs far less.
    const routeChangeExpected = reasons.some(reason => /ERR_|tunnel|connection|network/i.test(reason));
    const route = await waitForHealthyProxyRoute(
      lastKnownCountry,
      120000,
      routeChangeExpected ? 6000 : 2000
    );
    if (!route) {
      console.log('  ⚠️ The selected proxy never answered, even with escalating time budgets.');
      console.log(`     Diagnostics: ${diagnosticsPath}\n`);
      trace('recovery_failed', { stage: 'proxy_route_unreachable' });
      return null;
    }

    trace('proxy_route_ready', {
      country: route.country,
      provider: route.provider,
      changed: route.changed,
      elapsedMs: route.elapsedMs
    }, `✅ route ready: ${route.country} via ${route.provider} (${route.elapsedMs}ms${route.changed ? ', country changed' : ', country unchanged'})`);

    activeRouteCountry = route.country;

    // Persisting the cookie is essential: header injection authenticates a
    // request but does not populate Chrome's cookie jar after an IP change.
    await ensureSessionCookiePresent('recovery');

    // With the tab on an error/blank page every in-page probe fails, so the
    // pre-checks are skipped and the single navigation happens right away.
    const pageBefore = await currentPageLocation();
    if (!pageBefore.onChatGPT) {
      trace('recovery_skipping_precheck', { reason: 'page is not a chatgpt.com document' },
        '↷ page is not on chatgpt.com; warming the route before reloading it');

      // Absorb the slow first request through the new proxy in a background tab
      // so the visible tab does not sit on an error page while it happens.
      setRecoveryState('releasing-old-region');
      await releaseOldRegionState('before_warmup');
      setRecoveryState('background-warmup');
      await warmUpChatGPTInBackground(route.country, 15000);
      await ensureSessionCookiePresent('after warmup');
      return navigateOnceAndVerify(route, { path: '/', hash: '' });
    }

    setRecoveryState('verifying-current-page');
    const current = await verifyStableSession();
    trace('current_page_verification', {
      ok: current.ok,
      loggedIn: !!current.state?.loggedIn,
      status: current.state?.status || 0,
      country: current.state?.country || null,
      challenge: !!current.state?.challenge,
      routeCountry: route.country
    }, `🔍 current page: loggedIn=${!!current.state?.loggedIn} me=${current.state?.status || 0} country=${current.state?.country || '?'}${current.state?.challenge ? ' challenge=yes' : ''}`);
    if (current.ok && current.state.country === route.country) {
      return acceptRecovery(current.state, route, 'no_navigation');
    }

    // Drop the Cloudflare/device cookies issued for the previous exit IP,
    // otherwise ChatGPT keeps serving the old region and its currency.
    setRecoveryState('releasing-old-region');
    await releaseOldRegionState('before_warmup');

    // ChatGPT uses the first document request after an IP change to rebuild the
    // regional edge session. Do that request in a background target so the
    // visible tab never shows the logged-out intermediate state.
    setRecoveryState('background-warmup');
    trace('background_warmup_started', { routeCountry: route.country },
      '🔥 priming ChatGPT session in a background tab');
    await warmUpChatGPTInBackground(route.country);
    await ensureSessionCookiePresent('after warmup');

    const afterWarmup = await verifyStableSession();
    trace('warmup_verification', {
      ok: afterWarmup.ok,
      loggedIn: !!afterWarmup.state?.loggedIn,
      status: afterWarmup.state?.status || 0,
      country: afterWarmup.state?.country || null,
      challenge: !!afterWarmup.state?.challenge,
      routeCountry: route.country
    }, `🔍 after warm-up: loggedIn=${!!afterWarmup.state?.loggedIn} me=${afterWarmup.state?.status || 0} country=${afterWarmup.state?.country || '?'}${afterWarmup.state?.challenge ? ' challenge=yes' : ''}`);
    if (afterWarmup.ok && afterWarmup.state.country === route.country) {
      return acceptRecovery(afterWarmup.state, route, 'background_warmup');
    }

    // Only now, after the proxy, cookies, and edge session are ready, perform
    // the single visible navigation. There is no blind visible retry loop.
    return navigateOnceAndVerify(route, await currentPageLocation());
  }

  function acceptRecovery(state, route, mode) {
    const oldCountry = lastKnownCountry;
    lastKnownCountry = state.country;
    activeRouteCountry = route.country;
    const matchesRoute = state.country === route.country;

    console.log(`  ✅ Session active${state.email ? `: ${state.email}` : ''}`);
    if (matchesRoute) {
      console.log(`  🌍 Country: ${oldCountry || '?'} → ${state.country} (${currencyForCountry(state.country)})\n`);
    } else {
      console.log(`  ⚠️ ChatGPT still reports ${state.country} while the connection exits from ${route.country}.`);
      console.log(`     Checkout will use ${route.country} (${currencyForCountry(route.country)}); the plan cards may lag one refresh behind.\n`);
    }
    trace('recovery_succeeded', {
      mode,
      chatgptCountry: state.country,
      routeCountry: route.country,
      matchesRoute
    });
    return state;
  }

  function requestProxyRecovery(reason) {
    if (cleaning || !cdp.isAlive()) return Promise.resolve(null);
    markActivity();
    pendingRecoveryReasons.add(reason);
    if (recoveryPromise) return recoveryPromise;

    cancelActiveQuestion();
    recoveryPromise = (async () => {
      autoReloadInProgress = true;
      handled = false;
      lastOfferSeen = false;
      const reasons = [...pendingRecoveryReasons];
      pendingRecoveryReasons.clear();
      return performRecovery(reasons);
    })().finally(async () => {
      try { await cdp.send('Network.setCacheDisabled', { cacheDisabled: false }); } catch {}
      autoReloadInProgress = false;
      setRecoveryState('idle');
      recoveryPromise = null;
    });

    return recoveryPromise;
  }

  // ⭐ v6.0: Reactive 401 detection via Network events
  // Instead of relying only on polling /me, we watch ChatGPT's own requests.
  // The moment ANY backend-api call returns 401, we react immediately.
  cdp.on('Network.responseReceived', async (params) => {
    if (autoReloadInProgress || cleaning) return;
    const url = params.response?.url || '';
    const status = params.response?.status;
    if (status !== 401 && status !== 403) return;
    if (!/chatgpt\.com\/(backend-api|api)\//i.test(url)) return;
    // A 403 can be an ordinary endpoint permission/WAF response. Treat it as
    // a session signal only on endpoints that actually describe the session.
    if (status === 403 && !/\/(backend-api\/me|api\/auth\/session)(?:[/?]|$)/i.test(url)) return;

    // Debounce: only react once per 5 seconds
    const now = Date.now();
    if (now - last401At < 5000) return;
    last401At = now;

    const endpoint = url.replace(/^https?:\/\/[^/]+/, '');
    trace('auth_response_failure', {
      status,
      endpoint,
      remoteIP: params.response?.remoteIPAddress || null
    }, `⚡ auth failure ${status} on ${endpoint}`);
    void requestProxyRecovery(`${status} response`);
  });

  // A proxy extension often drops the old tunnel before ChatGPT can return
  // 401. Transport failures therefore provide an earlier recovery signal.
  cdp.on('Network.loadingFailed', params => {
    if (autoReloadInProgress || cleaning || params.canceled) return;
    const error = params.errorText || '';
    const resourceType = params.type || '';
    const failedUrl = inFlight.get(params.requestId)?.url || null;
    inFlight.delete(params.requestId);

    const verdict = classifyRequestFailure({ errorText: error, type: resourceType, url: failedUrl });
    if (!verdict.relevant) {
      if (ROUTE_FAILURE_ERRORS.test(error)) {
        trace('transport_failure_ignored', {
          error,
          resourceType,
          url: failedUrl,
          host: verdict.host || null,
          reason: verdict.reason
        }, `↷ ignoring ${error} from ${verdict.host || resourceType || 'unknown'} (${verdict.reason})`);
      }
      return;
    }

    const now = Date.now();
    if (now - lastTransportFailureAt < 4000) return;
    lastTransportFailureAt = now;
    trace('transport_failure', {
      error,
      resourceType,
      url: failedUrl,
      blockedReason: params.blockedReason || null
    }, `⚡ transport failure ${error} (${resourceType} ${shortenUrl(failedUrl || failedHost)})`);
    void requestProxyRecovery(error.replace(/^net::/, ''));
  });

  // Background country monitor (backup layer + country change detection)
  const countryMonitor = setInterval(async () => {
    if (!cdp.isAlive() || cleaning || autoReloadInProgress || !pageIsChatGPT) return;
    if (!inActivePeriod() && ++countryTick % 2) return;
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `fetch('/backend-api/me',{credentials:'include',cache:'no-store'}).then(async r=>{if(r.ok){const d=await r.json().catch(()=>({}));return JSON.stringify({status:200,country:d.country||null,email:d.email||null});}return JSON.stringify({status:r.status});}).catch(e=>JSON.stringify({status:0,error:e.message}))`,
        awaitPromise: true, returnByValue: true, timeout: 8000
      });
      const data = JSON.parse(res.result?.value || '{}');
      trace('country_monitor', {
        status: data.status,
        country: data.country || null,
        knownCountry: lastKnownCountry,
        routeCountry: activeRouteCountry,
        error: data.error || null
      });

      if (data.status === 401 || data.status === 403) {
        markActivity();
        void requestProxyRecovery(`${data.status} from country check`);
        return;
      }

      if (data.country) {
        if (lastKnownCountry === null) {
          lastKnownCountry = data.country;
          trace('country_established', { country: data.country },
            `🌍 ChatGPT country: ${data.country} (${currencyForCountry(data.country)})`);
        } else if (lastKnownCountry !== data.country) {
          markActivity();
          trace('country_changed_without_recovery', {
            from: lastKnownCountry,
            to: data.country
          }, `🔄 ChatGPT country ${lastKnownCountry} → ${data.country} (${currencyForCountry(data.country)})`);
          lastKnownCountry = data.country;
          handled = false;
          lastOfferSeen = false;
        }
      }
    } catch (e) {
      trace('country_monitor_error', { error: e.message });
    }
  }, 4000);

  // Live snapshot so the exit IP, ChatGPT's region, and the page state can be
  // compared at a glance while a switch is happening.
  const snapshotMonitor = setInterval(async () => {
    if (!cdp.isAlive() || cleaning || autoReloadInProgress) return;
    if (!inActivePeriod() && ++snapshotTick % 2) return;
    const route = await probeExitCountry(cdp);
    const location = await currentPageLocation();
    trace('snapshot', {
      exitCountry: route.ok ? route.country : null,
      exitProvider: route.provider || null,
      exitProbeMs: route.elapsedMs,
      chatgptCountry: lastKnownCountry,
      routeCountryUsedForCheckout: activeRouteCountry,
      cookieFingerprint: fingerprint(sessionToken),
      recoveryState,
      page: `${location.path}${location.hash}`,
      pausedRequests: pausedRequests.size
    }, `📡 exit=${route.ok ? route.country : '?'} chatgpt=${lastKnownCountry || '?'} cookie=${fingerprint(sessionToken)} page=${location.path}${location.hash} state=${recoveryState}`);
  }, 10000);

  console.log('  ' + '─'.repeat(56));
  console.log('  🕵️  MONITORING MODE ACTIVE');
  console.log('  📍 Watching for trial offers OR upgrade/plan surfaces');
  console.log('  ⚡ Proxy recovery state machine: idle');
  console.log('  🌍 Country monitor 4s | exit-IP snapshot 10s (halved while idle)');
  console.log(`  🧾 Trace: ${TRACE_ENABLED ? 'every request, response, cookie, and page event' : 'off'}`);
  console.log('  💡 Press Ctrl+C to quit at any time');
  console.log('  ' + '─'.repeat(56) + '\n');

  while (cdp.isAlive() && !cleaning) {
    await sleep(2500);
    if (autoReloadInProgress) continue;
    if (handled && lastOfferSeen) {
      const cur = await detectOffer(cdp);
      if (!cur.found) { handled = false; lastOfferSeen = false; }
      continue;
    }

    const offer = await detectOffer(cdp);
    if (!offer.found) { lastOfferSeen = false; continue; }

    lastOfferSeen = true;
    if (handled) continue;

    // Different prompt based on whether trial exists or not
    const isTrial = offer.type === 'trial';
    if (isTrial) {
      console.log('\n  🎁 Trial offer detected on the page!');
      awaitingInput = true;
      var rawAnswer = await ask('  Skip trial and get direct Plus checkout?\n  Type "y" for YES, or press Enter for NO: ');
      awaitingInput = false;
    } else {
      // noTrial: user is on pricing page but no trial available
      const btnLabel = offer.buttonLabel || 'Upgrade';
      console.log(`\n  💳 Plan surface detected (${offer.surface || 'unknown'}) — "${btnLabel}" visible, so no free trial is offered`);
      awaitingInput = true;
      var rawAnswer = await ask('  Generate direct Plus pay link now?\n  Type "y" for YES, or press Enter for NO: ');
      awaitingInput = false;
    }

    if (rawAnswer === null) {
      console.log('  ↻ Prompt canceled because the network changed; it will reappear after recovery.\n');
      handled = false;
      lastOfferSeen = false;
      continue;
    }
    const answer = rawAnswer.toLowerCase().trim();

    if (answer !== 'y' && answer !== 'yes') {
      console.log('  ⏭️  Skipped. Won\'t ask again for this page.\n');
      handled = true;
      continue;
    }

    // A recovery may have started while the question was open; the checkout has
    // to run against a live ChatGPT document, never an error or blank page.
    if (recoveryPromise) {
      console.log('  ⏳ Waiting for the connection recovery to finish first...');
      await recoveryPromise;
    }
    if (!await ensureChatGPTPageReady()) {
      console.log('  ⚠️ ChatGPT is not reachable right now, so no pay link was requested.');
      console.log('     The prompt returns once the page is healthy again.\n');
      handled = false;
      lastOfferSeen = false;
      continue;
    }

    // ⭐ Detect country NOW (not at startup) so VPN/extension changes are picked up
    console.log('  🌍 Detecting current country (live)...');
    const detected = await detectCountry(cdp);
    let country = activeRouteCountry || null;
    if (detected && detected.reliable) {
      country = detected.country;
      console.log(`  ✓ ${country} (${currencyForCountry(country)}) via ${detected.source}`);
      if (detected.drifted) {
        console.log(`  ℹ️ ChatGPT still reports ${detected.accountCountry}; billing the exit IP country ${country} instead.`);
      }
      trace('checkout_country_selected', {
        country,
        source: detected.source,
        chatgptCountry: detected.accountCountry || null,
        drifted: !!detected.drifted
      });
    } else {
      const guess = detected?.country || country;
      console.log(`  ⚠️ Could not confirm the country from the connection${detected ? ` (only ${detected.source} available)` : ''}.`);
      trace('checkout_country_unreliable', {
        guess: guess || null,
        source: detected?.source || null
      });
      awaitingInput = true;
      const manual = ((await ask(`  Enter the billing country (2-letter code${guess ? `, Enter=${guess}` : ''}): `)) || '').toUpperCase().trim();
      awaitingInput = false;
      if (/^[A-Z]{2}$/.test(manual)) country = manual;
      else country = guess;
      if (!country) {
        console.log('  ⏭️  No country confirmed, so no pay link was requested.\n');
        handled = true;
        continue;
      }
      console.log(`  ✓ Using: ${country} (${currencyForCountry(country)})`);
    }

    console.log(`  ⏳ Generating direct pay link (Plus${isTrial ? ', no trial' : ''})...`);
    try {
      const result = await generateDirectPayLink(cdp, apiSession, country);
      console.log(`  ✅ Pay link generated!`);
      console.log(`     Country:  ${result.country}`);
      console.log(`     Currency: ${result.currency}`);
      console.log(`     Sentinel: ${result.sentinelUsed ? 'YES' : 'NO'}`);
      console.log(`     URL:      ${result.url}`);

      // Saved before opening the tab: a checkout session stays valid even if
      // the tab fails to load, so the link must never be lost.
      const savedTo = savePayLink(result, apiSession);
      if (savedTo) console.log(`     Saved:    ${savedTo}`);

      console.log('  🌐 Opening in new tab...');
      await openInNewTab(cdp, result.url);
      console.log('  ✅ Done! Complete payment in the new tab.\n');
      handled = true;
    } catch (e) {
      console.log(`  ❌ Failed: ${e.message}`);
      console.log('  ↩️  Will retry if the offer reappears.\n');
      handled = true;
    }
  }

  clearInterval(keepAlive);
  clearInterval(cookieSyncMonitor);
  clearInterval(countryMonitor);
  clearInterval(snapshotMonitor);
  if (typeof leakDetector !== 'undefined') clearInterval(leakDetector);
  if (typeof statsReporter !== 'undefined') clearInterval(statsReporter);
  try { ws.close(); } catch {}
  cleanup();
  process.exit(0);
}

main().catch(e => { console.error('\n❌ Fatal:', e.message); process.exit(1); });
