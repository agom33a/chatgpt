#!/usr/bin/env node
/**
 * ════════════════════════════════════════════════════════════════
 *   🎯 ChatGPT Manual Browser Smart (v7.0 - Proxy recovery state machine)
 *
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
      listeners.get(m.method).forEach(fn => { try { fn(m.params); } catch {} });
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
    send(method, params = {}) {
      if (!alive) return Promise.reject(new Error('CDP closed'));
      const i = ++id;
      return new Promise((res, rej) => {
        const timer = setTimeout(() => {
          if (pending.has(i)) {
            pending.delete(i);
            rej(new Error(`CDP timeout: ${method}`));
          }
        }, 30000);
        pending.set(i, { resolve: res, reject: rej, timer });
        try { ws.send(JSON.stringify({ id: i, method, params })); }
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
function getRL() {
  if (!sharedRL) sharedRL = readline.createInterface({ input: process.stdin, output: process.stdout });
  return sharedRL;
}
function ask(q) {
  return new Promise(r => getRL().question(q, a => r(a.trim())));
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

// ── Auto-detect country (ChatGPT's own view > IP APIs > timezone) ──
async function detectCountry(cdp) {
  // ⭐ Bypass ChatGPT's strict CSP so we can fetch external APIs
  try { await cdp.send('Page.setBypassCSP', { enabled: true }); } catch {}

  // ═══ PRIORITY 1: ChatGPT's own /backend-api/me endpoint ═══
  // This is what ChatGPT ACTUALLY sees for the account - matches the
  // pricing shown on screen exactly. Works with any VPN/proxy/extension.
  try {
    const res = await cdp.send('Runtime.evaluate', {
      expression: `fetch('/backend-api/me',{credentials:'include'}).then(r=>r.json()).then(d=>d.country||'').catch(()=>'')`,
      awaitPromise: true, returnByValue: true, timeout: 8000
    });
    const country = (res.result?.value || '').toUpperCase().trim();
    if (country && /^[A-Z]{2}$/.test(country)) {
      return { country, source: 'ChatGPT /me' };
    }
  } catch {}

  // ═══ PRIORITY 2: External IP APIs (with CSP bypass) ═══
  const providers = [
    { url: 'https://ipapi.co/json/', field: 'country_code' },
    { url: 'https://ipwho.is/', field: 'country_code' },
    { url: 'https://api.country.is/', field: 'country' },
    { url: 'https://ipinfo.io/json', field: 'country' }
  ];
  for (const p of providers) {
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `fetch(${JSON.stringify(p.url)}).then(r=>r.json()).then(d=>d.${p.field}||'').catch(()=>'')`,
        awaitPromise: true, returnByValue: true, timeout: 8000
      });
      const country = (res.result?.value || '').toUpperCase().trim();
      if (country && /^[A-Z]{2}$/.test(country)) return { country, source: `IP (${new URL(p.url).hostname})` };
    } catch {}
  }

  // ═══ PRIORITY 3: Browser timezone → country (last resort) ═══
  try {
    const tzRes = await cdp.send('Runtime.evaluate', {
      expression: `Intl.DateTimeFormat().resolvedOptions().timeZone`,
      returnByValue: true, timeout: 3000
    });
    const tz = tzRes.result?.value;
    if (tz && TIMEZONE_TO_COUNTRY[tz]) {
      return { country: TIMEZONE_TO_COUNTRY[tz], source: `timezone (${tz})` };
    }
  } catch {}

  return null;
}

// ── Detect trial offer button in page ──
async function detectOffer(cdp) {
  try {
    const res = await cdp.send('Runtime.evaluate', {
      expression: `(() => {
        try {
          if (!document || !document.body) return JSON.stringify({found:false});

          // Priority 1: Trial offer buttons
          const claimBtn = document.querySelector('button[aria-label="Claim offer"]');
          if (claimBtn && claimBtn.offsetParent !== null) return JSON.stringify({found:true, type:'trial', mode:'claim'});
          const freeBtn = Array.from(document.querySelectorAll('button')).find(b => {
            const txt = (b.textContent || '').trim();
            return (txt === 'Free offer' || txt === 'Claim free offer') && b.offsetParent !== null;
          });
          if (freeBtn) return JSON.stringify({found:true, type:'trial', mode:'free'});

          // Priority 2: Pricing page without trial (various button labels)
          // Covers: "Upgrade to Plus", "Get Plus", "Rejoin Plus", "Subscribe to Plus"
          const isPricingPage = location.hash.includes('pricing') ||
            location.pathname.includes('pricing') ||
            location.search.includes('promo_campaign');
          if (isPricingPage) {
            const upgradeBtn = Array.from(document.querySelectorAll('button')).find(b => {
              const txt = (b.textContent || '').trim().toLowerCase();
              return (txt === 'upgrade to plus' ||
                      txt === 'get plus' ||
                      txt === 'rejoin plus' ||
                      txt === 'subscribe to plus' ||
                      txt === 'reactivate plus' ||
                      txt === 'go plus') && b.offsetParent !== null;
            });
            if (upgradeBtn) {
              const label = upgradeBtn.textContent.trim();
              return JSON.stringify({found:true, type:'noTrial', buttonLabel: label});
            }
          }

          return JSON.stringify({found:false});
        } catch(e) { return JSON.stringify({found:false, err:e.message}); }
      })()`,
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
        awaitPromise: true, returnByValue: true, timeout: 15000
      });
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

  const evalExpr = `
    (async () => {
      try {
        const authHeader = 'Bearer ' + ${JSON.stringify(auth.token)};
        const accountHeader = ${JSON.stringify(auth.account || null)};

        let sentinelToken = null;
        let sentinelStatus = 0;
        try {
          const chReq = await fetch('/backend-api/sentinel/chat-requirements', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': authHeader,
              'OAI-Language': 'en-US'
            },
            body: JSON.stringify({}),
            credentials: 'include'
          });
          sentinelStatus = chReq.status;
          if (chReq.ok) {
            const chData = await chReq.json();
            sentinelToken = chData.token;
          }
        } catch(e) {}

        await new Promise(r => setTimeout(r, 1500));

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

  const evalRes = await cdp.send('Runtime.evaluate', {
    expression: evalExpr, awaitPromise: true, returnByValue: true, timeout: 30000
  });
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

async function main() {
  console.log('\n' + '='.repeat(60));
  console.log('  🎯 ChatGPT Manual Browser Smart (v7.0 - Proxy Recovery)');
  console.log('='.repeat(60) + '\n');

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
    const useProxy = (await ask('  Use proxy? [Y/n]: ')).toLowerCase();
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
  await sleep(3500);

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
    // A stale service worker can keep ChatGPT attached to the old route after
    // a proxy extension changes country.
    await cdp.send('Network.setBypassServiceWorker', { bypass: true });
  } catch (e) { console.error('❌ CDP setup:', e.message); cleanup(); process.exit(1); }

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

  // Short URL for logging
  const shortUrl = (u) => {
    try {
      const p = new URL(u);
      return p.hostname + p.pathname.slice(0, 60);
    } catch { return u.slice(0, 80); }
  };

  cdp.on('Fetch.requestPaused', async (params) => {
    const requestId = params.requestId;
    const url = params.request?.url || '';
    const shouldInject = /^https:\/\/([a-z0-9-]+\.)?chatgpt\.com\//i.test(url);
    const short = shortUrl(url);
    requestCounter++;

    // Timeout timer
    const rescueTimer = setTimeout(async () => {
      timeoutCounter++;
      console.log(`  ⏱️  [TIMEOUT ${timeoutCounter}] ${short}`);
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
      console.log(`  ❌ [ERROR ${errorCounter}] ${short} — ${e.message}`);
      try { await cdp.send('Fetch.continueRequest', { requestId }); }
      catch { try { await cdp.send('Fetch.failRequest', { requestId, errorReason: 'Failed' }); } catch {} }
    }
  });

  // ⭐ v5.8: Silence background logs while asking the user something
  let awaitingInput = false;

  // ⭐ Leak detector: report if too many requests stuck
  const leakDetector = setInterval(() => {
    if (awaitingInput) return;
    const pending = pausedRequests.size;
    if (pending > 10) {
      console.log(`  ⚠️  WARNING: ${pending} requests currently paused (possible VPN switch, will auto-recover in 8s)`);
      const sorted = [...pausedRequests.entries()]
        .sort((a, b) => a[1].startedAt - b[1].startedAt).slice(0, 3);
      for (const [id, info] of sorted) {
        const age = Math.round((Date.now() - info.startedAt) / 1000);
        console.log(`      → [${age}s old] ${shortUrl(info.url)}`);
      }
    }
  }, 5000);

  // ⭐ Stats reporter every 30s
  const statsReporter = setInterval(() => {
    if (awaitingInput) return;
    if (requestCounter > 0) {
      console.log(`  📊 Requests: ${requestCounter} total | ${successCounter} ok | ${timeoutCounter} timeout | ${errorCounter} error | ${pausedRequests.size} pending`);
    }
  }, 30000);

  console.log('  ✅ Interceptor ready (auto-recovery: 8s, verbose logging on)\n');

  console.log('  🌐 Navigating to chatgpt.com...');
  await cdp.send('Page.navigate', { url: 'https://chatgpt.com/' });
  await sleep(4000);

  // Verify login
  let verifiedEmail = null;
  for (let i = 0; i < 5; i++) {
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `fetch('/api/auth/session',{credentials:'include'}).then(r=>r.json()).then(d=>JSON.stringify({ok:!!(d&&d.user),email:d?.user?.email,hasToken:!!d?.accessToken})).catch(e=>JSON.stringify({ok:false,err:e.message}))`,
        awaitPromise: true, returnByValue: true, timeout: 10000
      });
      const parsed = JSON.parse(res.result?.value || '{}');
      if (parsed.ok && parsed.hasToken) { verifiedEmail = parsed.email; break; }
    } catch {}
    await sleep(1500);
  }

  if (verifiedEmail) console.log(`  ✅ Logged in as: ${verifiedEmail}\n`);
  else console.log('  ⚠️ Login verification uncertain (will try anyway)\n');

  // Country is detected JUST-IN-TIME at checkout, not here.
  // This way if user switches VPN/extension mid-session, we get the fresh country.

  // Keep-alive
  const keepAlive = setInterval(async () => {
    if (!cdp.isAlive()) { clearInterval(keepAlive); return; }
    try { await cdp.send('Browser.getVersion'); } catch {}
  }, 20000);

  // Offer state (declared here so country monitor can reset it)
  let handled = false;
  let lastOfferSeen = false;

  // Connection state shared by all network signals.
  let lastKnownCountry = null;
  let autoReloadInProgress = false;
  let last401At = 0;
  let lastTransportFailureAt = 0;
  let recoveryPromise = null;
  let recoveryState = 'idle';
  const pendingRecoveryReasons = new Set();

  // Probe the freshly loaded page instead of assuming that Page.reload means
  // the session has recovered. During a proxy switch ChatGPT commonly returns
  // one logged-out/401 page before accepting the injected session cookie.
  async function probeChatGPTSession() {
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `Promise.all([
          fetch('/api/auth/session',{credentials:'include',cache:'no-store'})
            .then(r=>r.json()).then(d=>({loggedIn:!!(d&&d.user),email:d?.user?.email||null})),
          fetch('/backend-api/me',{credentials:'include',cache:'no-store'})
            .then(async r=>({status:r.status,country:r.ok?(await r.json().catch(()=>({}))).country||null:null}))
        ]).then(([session,me])=>JSON.stringify({...session,...me}))
          .catch(e=>JSON.stringify({status:0,error:e.message}))`,
        awaitPromise: true, returnByValue: true, timeout: 12000
      });
      return JSON.parse(res.result?.value || '{}');
    } catch (e) {
      return { status: 0, error: e.message };
    }
  }

  async function waitForChatGPTDocument(timeoutMs = 10000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline && cdp.isAlive()) {
      try {
        const res = await cdp.send('Runtime.evaluate', {
          expression: `JSON.stringify({
            host: location.hostname,
            ready: document.readyState,
            hasBody: !!document.body
          })`,
          returnByValue: true, timeout: 3000
        });
        const page = JSON.parse(res.result?.value || '{}');
        if (page.host === 'chatgpt.com' && page.hasBody &&
            (page.ready === 'interactive' || page.ready === 'complete')) return true;
      } catch {}
      await sleep(350);
    }
    return false;
  }

  async function verifyStableSession() {
    const first = await probeChatGPTSession();
    if (!first.loggedIn || first.status !== 200 || !/^[A-Z]{2}$/i.test(first.country || '')) {
      return { ok: false, state: first };
    }

    await sleep(900);
    const second = await probeChatGPTSession();
    const stable = second.loggedIn && second.status === 200 &&
      second.country === first.country;
    return { ok: stable, state: second, previousCountry: first.country };
  }

  async function performRecovery(reasons) {
    recoveryState = 'settling';
    console.log(`\n  🔌 Network transition detected: ${reasons.join(', ')}`);
    console.log('  ⏳ Waiting for the proxy route to settle...');
    await sleep(1800);

    try { await cdp.send('Page.stopLoading'); } catch {}
    try { await cdp.send('Network.setCacheDisabled', { cacheDisabled: true }); } catch {}
    try { await cdp.send('Network.clearBrowserCache'); } catch {}

    for (let attempt = 1; attempt <= 4; attempt++) {
      recoveryState = 'reconnecting';
      console.log(`  🔄 Reconnecting ChatGPT (attempt ${attempt}/4)...`);

      // Move away from the old renderer and close its pooled sockets before
      // loading ChatGPT through the extension's newly selected proxy.
      try { await cdp.send('Page.navigate', { url: 'about:blank' }); } catch {}
      await sleep(500);
      try { await cdp.send('Network.closeIdleConnections'); } catch {}
      try { await cdp.send('Network.clearBrowserCache'); } catch {}

      const freshUrl = `https://chatgpt.com/?proxy_refresh=${Date.now()}`;
      try { await cdp.send('Page.navigate', { url: freshUrl }); } catch {}

      const documentReady = await waitForChatGPTDocument(10000);
      if (!documentReady) {
        console.log('     Page route is not ready yet');
        await sleep(700);
        continue;
      }

      recoveryState = 'verifying';
      const verification = await verifyStableSession();
      const state = verification.state || {};
      if (verification.ok) {
        const oldCountry = lastKnownCountry;
        lastKnownCountry = state.country;
        try {
          await cdp.send('Runtime.evaluate', {
            expression: `history.replaceState(null, '', '/')`,
            returnByValue: true, timeout: 3000
          });
        } catch {}

        console.log(`  ✅ Session restored${state.email ? `: ${state.email}` : ''}`);
        if (oldCountry && oldCountry !== state.country) {
          console.log(`  🌍 Country changed: ${oldCountry} → ${state.country} (${currencyForCountry(state.country)})\n`);
        } else {
          console.log(`  🌍 Country confirmed twice: ${state.country} (${currencyForCountry(state.country)})\n`);
        }
        return state;
      }

      const detail = state.status ? `HTTP ${state.status}` : (state.error || 'session/country not stable');
      console.log(`     Verification failed (${detail})`);
      await sleep(900);
    }

    console.log('  ⚠️ The proxy route still is not stable; monitoring remains active.');
    console.log('     The next network signal will start a fresh recovery cycle.\n');
    return null;
  }

  function requestProxyRecovery(reason) {
    if (cleaning || !cdp.isAlive()) return Promise.resolve(null);
    pendingRecoveryReasons.add(reason);
    if (recoveryPromise) return recoveryPromise;

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
      recoveryState = 'idle';
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
    console.log(`\n  ⚡ Detected ${status} on ${endpoint}`);
    void requestProxyRecovery(`${status} response`);
  });

  // A proxy extension often drops the old tunnel before ChatGPT can return
  // 401. Transport failures therefore provide an earlier recovery signal.
  cdp.on('Network.loadingFailed', params => {
    if (autoReloadInProgress || cleaning || params.canceled) return;
    const error = params.errorText || '';
    const resourceType = params.type || '';
    if (!['Document', 'XHR', 'Fetch'].includes(resourceType)) return;
    const routeFailure = /(ERR_PROXY_CONNECTION_FAILED|ERR_TUNNEL_CONNECTION_FAILED|ERR_CONNECTION_RESET|ERR_CONNECTION_CLOSED|ERR_NETWORK_CHANGED|ERR_INTERNET_DISCONNECTED)/i.test(error);
    const mainDocumentTimeout = resourceType === 'Document' && /ERR_TIMED_OUT/i.test(error);
    if (!routeFailure && !mainDocumentTimeout) return;

    const now = Date.now();
    if (now - lastTransportFailureAt < 4000) return;
    lastTransportFailureAt = now;
    console.log(`\n  ⚡ Transport failure detected: ${error}`);
    void requestProxyRecovery(error.replace(/^net::/, ''));
  });

  // Background country monitor (backup layer + country change detection)
  const countryMonitor = setInterval(async () => {
    if (!cdp.isAlive() || cleaning || awaitingInput || autoReloadInProgress) return;
    try {
      const res = await cdp.send('Runtime.evaluate', {
        expression: `fetch('/backend-api/me',{credentials:'include',cache:'no-store'}).then(async r=>{if(r.ok){const d=await r.json().catch(()=>({}));return JSON.stringify({status:200,country:d.country||null,email:d.email||null});}return JSON.stringify({status:r.status});}).catch(e=>JSON.stringify({status:0,error:e.message}))`,
        awaitPromise: true, returnByValue: true, timeout: 8000
      });
      const data = JSON.parse(res.result?.value || '{}');

      if (data.status === 401 || data.status === 403) {
        void requestProxyRecovery(`${data.status} from country check`);
        return;
      }

      if (data.country) {
        if (lastKnownCountry === null) {
          lastKnownCountry = data.country;
          console.log(`  🌍 Current country: ${data.country} (${currencyForCountry(data.country)})`);
        } else if (lastKnownCountry !== data.country) {
          console.log(`\n  🔄 Country changed: ${lastKnownCountry} → ${data.country} (${currencyForCountry(data.country)})`);
          console.log('  ✅ ChatGPT now using new region. Ready for checkout.\n');
          lastKnownCountry = data.country;
          handled = false;
          lastOfferSeen = false;
        }
      }
    } catch {}
  }, 4000);

  console.log('  ' + '─'.repeat(56));
  console.log('  🕵️  MONITORING MODE ACTIVE');
  console.log('  📍 Watching for trial offers OR pricing pages');
  console.log('  ⚡ Proxy recovery state machine: idle');
  console.log('  🌍 Country change monitor (every 4s as backup)');
  console.log('  💡 Press Ctrl+C to quit at any time');
  console.log('  ' + '─'.repeat(56) + '\n');

  while (cdp.isAlive() && !cleaning) {
    await sleep(2500);
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
      var answer = (await ask('  Skip trial and get direct Plus checkout?\n  Type "y" for YES, or press Enter for NO: ')).toLowerCase().trim();
      awaitingInput = false;
    } else {
      // noTrial: user is on pricing page but no trial available
      const btnLabel = offer.buttonLabel || 'Upgrade';
      console.log(`\n  💳 Pricing page detected — "${btnLabel}" button visible (no trial)`);
      awaitingInput = true;
      var answer = (await ask('  Generate direct Plus pay link now?\n  Type "y" for YES, or press Enter for NO: ')).toLowerCase().trim();
      awaitingInput = false;
    }

    if (answer !== 'y' && answer !== 'yes') {
      console.log('  ⏭️  Skipped. Won\'t ask again for this page.\n');
      handled = true;
      continue;
    }

    // ⭐ Detect country NOW (not at startup) so VPN/extension changes are picked up
    console.log('  🌍 Detecting current country (live)...');
    const detected = await detectCountry(cdp);
    let country = 'US';
    if (detected) {
      country = detected.country;
      console.log(`  ✓ ${country} (${currencyForCountry(country)}) via ${detected.source}`);
    } else {
      console.log('  ⚠️ Detection failed');
      awaitingInput = true;
      const manual = (await ask('  Enter country manually (2-letter code, Enter=US): ')).toUpperCase().trim();
      awaitingInput = false;
      if (manual && /^[A-Z]{2}$/.test(manual)) country = manual;
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
  clearInterval(countryMonitor);
  if (typeof leakDetector !== 'undefined') clearInterval(leakDetector);
  if (typeof statsReporter !== 'undefined') clearInterval(statsReporter);
  try { ws.close(); } catch {}
  cleanup();
  process.exit(0);
}

main().catch(e => { console.error('\n❌ Fatal:', e.message); process.exit(1); });
