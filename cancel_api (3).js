#!/usr/bin/env node
/**
 * ════════════════════════════════════════════════════════════════
 *   🚫 Cancel Subscription (API only — no browser) — v4
 *
 *   v4 addition:
 *     - Proxy support via proxies.json or --proxy flag
 *     - Fixes 401 for accounts registered in a different country
 *   v3 fix:
 *     - Cancel body includes {"account_id": "..."}
 *   v2 changes:
 *     - Full browser-matching headers
 *
 *   Endpoint:
 *     POST /backend-api/subscriptions/cancel
 *
 *   Usage:
 *     node cancel_api                          # session_api.json
 *     node cancel_api ./sessions/acc1.json     # custom file
 *     node cancel_api --dry-run                # info only
 * ════════════════════════════════════════════════════════════════
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const { URL } = require('url');

const COOKIE_NAME = '__Secure-next-auth.session-token';
const HOST = 'chatgpt.com';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const useProxyFlag = args.includes('--proxy');
const noProxyFlag = args.includes('--no-proxy');
const sessionFile = args.find(a => !a.startsWith('--')) || './session_api.json';

const c = {
  reset: '\x1b[0m', bold: '\x1b[1m', dim: '\x1b[2m',
  red: '\x1b[31m', green: '\x1b[32m', yellow: '\x1b[33m',
  cyan: '\x1b[36m'
};
const ok = (m) => console.log(`${c.green}✓${c.reset} ${m}`);
const warn = (m) => console.log(`${c.yellow}⚠${c.reset} ${m}`);
const err = (m) => console.log(`${c.red}✗${c.reset} ${m}`);
const info = (m) => console.log(`${c.cyan}ℹ${c.reset} ${m}`);

// ── Browser-matching headers (matches Chrome 131 exactly) ──
function browserHeaders(sessionToken, accessToken, accountId, extra = {}) {
  return {
    'Host': HOST,
    'User-Agent': UA,
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Cookie': `${COOKIE_NAME}=${sessionToken}`,
    'Authorization': `Bearer ${accessToken}`,
    'ChatGPT-Account-ID': accountId,
    'OAI-Language': 'en-US',
    'Origin': `https://${HOST}`,
    'Referer': `https://${HOST}/`,
    'DNT': '1',
    'Sec-CH-UA': '"Chromium";v="131", "Not_A Brand";v="24", "Google Chrome";v="131"',
    'Sec-CH-UA-Mobile': '?0',
    'Sec-CH-UA-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=1, i',
    ...extra
  };
}

// ── Global proxy config ──
let PROXY = null; // { host, port, authHeader }

function loadProxy() {
  if (noProxyFlag) return null;
  try {
    if (fs.existsSync('./proxies.json')) {
      const raw = JSON.parse(fs.readFileSync('./proxies.json', 'utf8'));
      const list = Array.isArray(raw) ? raw : [];
      const first = list.map(p => typeof p === 'string' ? p : (p.proxyString || p.proxy_string || p.url)).filter(Boolean)[0];
      if (first) {
        let proxyStr = first.replace(/\{COUNTRY\}/gi, 'us');
        if (!proxyStr.startsWith('http')) proxyStr = 'http://' + proxyStr;
        const u = new URL(proxyStr);
        return {
          host: u.hostname,
          port: parseInt(u.port) || 80,
          authHeader: 'Basic ' + Buffer.from(
            `${decodeURIComponent(u.username || '')}:${decodeURIComponent(u.password || '')}`
          ).toString('base64')
        };
      }
    }
  } catch (e) {}
  return null;
}

// ── Request via proxy (CONNECT tunnel) or direct ──
function request(method, pathUrl, headers = {}, body = null) {
  return new Promise((resolve, reject) => {
    const doRequest = (socket) => {
      const options = {
        hostname: HOST, path: pathUrl, method,
        headers: { ...headers },
        socket: socket || undefined,
        agent: false
      };
      if (body !== null && method !== 'GET') {
        const bodyStr = typeof body === 'string' ? body : JSON.stringify(body);
        options.headers['Content-Type'] = 'application/json';
        options.headers['Content-Length'] = Buffer.byteLength(bodyStr);
      }
      const req = https.request(options, (res) => {
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => {
          let buf = Buffer.concat(chunks);
          const enc = String(res.headers['content-encoding'] || '').toLowerCase();
          const zlib = require('zlib');
          try {
            if (enc === 'gzip') buf = zlib.gunzipSync(buf);
            else if (enc === 'deflate') buf = zlib.inflateSync(buf);
            else if (enc === 'br') buf = zlib.brotliDecompressSync(buf);
          } catch {}
          const text = buf.toString('utf8');
          let parsed = null;
          try { parsed = JSON.parse(text); } catch {}
          resolve({ status: res.statusCode, headers: res.headers, body: text, json: parsed });
        });
      });
      req.on('error', reject);
      if (body !== null && method !== 'GET') {
        req.write(typeof body === 'string' ? body : JSON.stringify(body));
      }
      req.end();
    };

    if (PROXY) {
      // HTTP CONNECT tunnel through proxy
      const connectReq = http.request({
        host: PROXY.host, port: PROXY.port, method: 'CONNECT',
        path: `${HOST}:443`,
        headers: { 'Host': `${HOST}:443`, 'Proxy-Authorization': PROXY.authHeader }
      });
      connectReq.on('connect', (res, socket) => {
        if (res.statusCode !== 200) {
          reject(new Error(`Proxy CONNECT failed: ${res.statusCode}`));
          return;
        }
        // Now wrap socket in TLS and issue the real request
        const tls = require('tls');
        const tlsSocket = tls.connect({
          socket, servername: HOST, ALPNProtocols: ['http/1.1']
        }, () => doRequest(tlsSocket));
        tlsSocket.on('error', reject);
      });
      connectReq.on('error', reject);
      connectReq.end();
    } else {
      doRequest();
    }
  });
}

// ── Detect if response is Cloudflare block ──
function isCloudflareBlock(res) {
  return res.status === 403 && (
    res.body.includes('<html') ||
    res.body.includes('cloudflare') ||
    res.body.includes('Attention Required') ||
    (res.headers.server && String(res.headers.server).toLowerCase().includes('cloudflare'))
  );
}

// ── Try to fetch sentinel token ──
async function fetchSentinelToken(headers) {
  try {
    const res = await request('POST', '/backend-api/sentinel/chat-requirements', headers, {});
    if (res.status === 200 && res.json?.token) {
      return res.json.token;
    }
  } catch {}
  return null;
}

// ── Main ──
async function main() {
  console.log('\n' + '='.repeat(56));
  console.log(`  ${c.bold}🚫 Cancel Subscription (API) — v4${c.reset}`);
  console.log('='.repeat(56) + '\n');

  if (!fs.existsSync(sessionFile)) {
    err(`Session file not found: ${sessionFile}`);
    process.exit(1);
  }
  const session = JSON.parse(fs.readFileSync(sessionFile, 'utf8'));
  const sessionToken = String(session.sessionToken || '').trim().replace(/[\r\n\t]/g, '');
  const accessToken = session.accessToken;
  const accountId = session.account?.id;
  const email = session.user?.email;

  if (!sessionToken) { err('sessionToken missing'); process.exit(1); }
  if (!accessToken) { err('accessToken missing'); process.exit(1); }
  if (!accountId) { err('account.id missing'); process.exit(1); }

  info(`Email:       ${email || 'unknown'}`);
  info(`Account ID:  ${accountId}`);
  info(`Session:     ${sessionToken.length} chars`);
  info(`Access:      ${accessToken.length} chars`);
  if (dryRun) warn('DRY RUN MODE');

  // Load proxy if available
  PROXY = loadProxy();
  if (PROXY) {
    info(`Proxy:       ${PROXY.host}:${PROXY.port} ${c.green}(enabled)${c.reset}`);
  } else if (noProxyFlag) {
    info(`Proxy:       ${c.dim}disabled (--no-proxy)${c.reset}`);
  } else {
    info(`Proxy:       ${c.dim}none (no proxies.json)${c.reset}`);
  }
  console.log('');

  const baseHeaders = browserHeaders(sessionToken, accessToken, accountId);

  // ═══ Step 1: Get subscription info ═══
  console.log(`${c.bold}[1/3]${c.reset} Fetching current subscription...`);
  let subRes = await request('GET', `/backend-api/subscriptions?account_id=${accountId}`, baseHeaders);

  if (isCloudflareBlock(subRes)) {
    warn('Cloudflare block detected. Trying with Sentinel challenge...');
    const sentinelToken = await fetchSentinelToken(baseHeaders);
    if (sentinelToken) {
      info('Got sentinel token, retrying...');
      subRes = await request('GET', `/backend-api/subscriptions?account_id=${accountId}`,
        { ...baseHeaders, 'openai-sentinel-chat-requirements-token': sentinelToken });
    }
    if (isCloudflareBlock(subRes)) {
      err('Cloudflare still blocking. Node.js requests are being fingerprinted.');
      err('Cause: OpenAI recently tightened detection for direct API access.');
      console.log('');
      warn('Recommended workaround:');
      console.log('  Use manual_browser_smart.js instead — its requests come from');
      console.log('  a real Chrome instance so Cloudflare accepts them.');
      console.log('');
      console.log(`  ${c.dim}Response preview:${c.reset}`);
      console.log(`  ${c.dim}${subRes.body.slice(0, 200).replace(/\n/g, ' ')}${c.reset}`);
      process.exit(1);
    }
  }

  if (subRes.status === 401) {
    err('401 Unauthorized');
    console.log('');
    warn('Possible causes:');
    console.log('  1. Session token expired → refresh session_api.json');
    console.log('  2. Account registered in different country than your IP');
    console.log(`     → try running with a proxy matching the account\'s country`);
    console.log('  3. Access token expired (rare, usually refreshes automatically)');
    if (!PROXY) {
      console.log('');
      warn('You are running WITHOUT proxy. If the account is US/EU-based,');
      console.log('  add a proxy to proxies.json and try again.');
    }
    process.exit(1);
  }
  if (subRes.status !== 200) {
    err(`Unexpected status ${subRes.status} from /subscriptions`);
    console.log(`   ${c.dim}${subRes.body.slice(0, 300)}${c.reset}`);
    process.exit(1);
  }

  const sub = subRes.json;
  if (!sub || !sub.plan_type) {
    warn('No active subscription found.');
    console.log(`   Response: ${JSON.stringify(sub)}`);
    process.exit(0);
  }

  ok(`Active plan: ${c.bold}${sub.plan_type}${c.reset}`);
  info(`Period:      ${sub.billing_period || 'unknown'}`);
  info(`Started:     ${sub.active_start || 'unknown'}`);
  info(`Ends:        ${sub.active_until || 'unknown'}`);
  if (sub.will_renew === false || sub.cancel_at_period_end) {
    warn('Auto-renewal is ALREADY canceled.');
    ok(`Ends on ${sub.active_until}. Nothing to do.`);
    process.exit(0);
  }
  console.log('');

  if (dryRun) {
    warn('DRY RUN: skipping cancel call.');
    process.exit(0);
  }

  // ═══ Step 2: Cancel ═══
  console.log(`${c.bold}[2/3]${c.reset} Sending cancellation...`);
  const sentinelToken = await fetchSentinelToken(baseHeaders);
  const cancelHeaders = sentinelToken
    ? { ...baseHeaders, 'openai-sentinel-chat-requirements-token': sentinelToken }
    : baseHeaders;
  if (sentinelToken) info('Using sentinel token');

  const cancelRes = await request('POST', '/backend-api/subscriptions/cancel', cancelHeaders, {
    account_id: accountId
  });

  if (cancelRes.status !== 200) {
    err(`Cancel failed with status ${cancelRes.status}`);
    console.log(`   ${c.dim}${cancelRes.body.slice(0, 400)}${c.reset}`);
    process.exit(1);
  }

  ok(`Cancel accepted (200)`);
  if (cancelRes.json) info(`Response: ${JSON.stringify(cancelRes.json)}`);
  console.log('');

  // ═══ Step 3: Verify ═══
  console.log(`${c.bold}[3/3]${c.reset} Verifying...`);
  const verifyRes = await request('GET', `/backend-api/subscriptions?account_id=${accountId}`, baseHeaders);
  if (verifyRes.status === 200 && verifyRes.json) {
    const v = verifyRes.json;
    if (v.will_renew === false || v.cancel_at_period_end) {
      ok(`${c.bold}${c.green}Auto-renewal canceled.${c.reset}`);
      info(`Plus remains active until: ${v.active_until || sub.active_until}`);
    } else {
      warn('Cancel returned 200 but subscription still shows active.');
      warn('Check manually at chatgpt.com/#settings/Billing');
    }
  } else {
    warn(`Could not verify (status ${verifyRes.status}), but cancel went through.`);
  }

  console.log('\n' + '='.repeat(56));
  console.log(`  ${c.green}Done.${c.reset}`);
  console.log('='.repeat(56) + '\n');
}

main().catch(e => {
  console.error(`\n${c.red}❌ Fatal:${c.reset}`, e.message);
  process.exit(1);
});
