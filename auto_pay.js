#!/usr/bin/env node
/**
 * ════════════════════════════════════════════════════════════════
 *   💳 auto_pay.js — fill the checkout and subscribe
 *
 *   Opens a checkout link, fills the card and a billing address,
 *   checks the tax line before committing, presses Subscribe, and
 *   records exactly what came back.
 *
 *   HOW THE FIELDS ARE FILLED
 *     Every input — card and address alike — lives inside Stripe's
 *     own iframe, so ordinary page scripting cannot reach them. This
 *     finds the iframe's execution context, focuses each field there,
 *     and types with real key events. Assigning to .value directly
 *     does not work: Stripe ignores input that never came from a
 *     keystroke.
 *
 *   THE TAX CHECK
 *     Tax follows the billing address — Egypt 14%, UAE 5%, most US
 *     states 0% on digital services. The script re-reads the totals
 *     after the address is entered and refuses to submit if tax is
 *     not what you asked for, so a mistyped address cannot quietly
 *     cost you 14%.
 *
 *   FILES
 *     cards.json    the cards to use
 *     billing.json  the address to enter (optional; a US default is built in)
 *
 *   USAGE
 *     node auto_pay.js --country=PH             makes its own link, then pays it
 *     node auto_pay.js --reuse-link             use the newest link in results/
 *     node auto_pay.js "https://chatgpt.com/checkout/..."
 *     node auto_pay.js --card=2                 pick a card by position
 *     node auto_pay.js --card=redotpay          or by name
 *     node auto_pay.js --proxy=impulse          use a named proxy from proxies.json
 *     node auto_pay.js --link-proxy=impulse     fetch the link through a different
 *                                               provider than the one that pays
 *     node auto_pay.js --link-country=PH        exit country for that step
 *     node auto_pay.js --no-proxy               skip the proxy question
 *     node auto_pay.js --no-bypass              force Stripe through the proxy too
 *     node auto_pay.js --bypass=host1,host2     send more hosts direct
 *     node auto_pay.js --country=PH             exit country, where supported
 *     node auto_pay.js --dry-run                fill everything, don't submit
 *     node auto_pay.js --watch=5                a screenshot every 5s, for
 *                                               running where there is no screen
 *     node auto_pay.js --trace                  print every keystroke and what
 *                                               the field showed before and after
 *     node auto_pay.js --allow-tax              submit even if tax is charged
 *     node auto_pay.js --keep-renewal           leave auto-renewal on
 *     node auto_pay.js --clear-cards            remove saved cards before paying
 *     node auto_pay.js --cancel-existing        also turn off renewal on a plan
 *                                               this run did not pay for
 * ════════════════════════════════════════════════════════════════
 */

const { spawn } = require('child_process');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const net = require('net');

const sleep = ms => new Promise(r => setTimeout(r, ms));

// With --trace, every keystroke is printed with the field's value before and
// after it. Without this the only evidence of a problem is a mangled value at
// the end, which says nothing about which keystroke went wrong or where.
const TRACE = process.argv.includes('--trace');
const VERBOSE_SHOTS = process.argv.includes('--shots');
// --watch=5 takes a picture every five seconds for the whole run
const WATCH = (() => {
  const a = process.argv.find(x => x.startsWith('--watch'));
  if (!a) return 0;
  const v = a.includes('=') ? parseInt(a.split('=')[1]) : 5;
  return Number.isFinite(v) && v > 0 ? v : 5;
})();
const trace = (...a) => { if (TRACE) console.log('         ·', ...a); };
process.on('unhandledRejection', () => {});

// A US state with no sales tax on digital services, used when billing.json
// is absent. Anything here can be overridden per run.
const DEFAULT_BILLING = {
  name: 'Test Test',
  country: 'US',
  addressLine1: 'Little Rock',
  city: 'Little Rock',
  state: 'AR',
  postalCode: '72211'
};

// ── Chrome / CDP ──
const findChrome = () => [
  'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
  'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
  `${process.env.LOCALAPPDATA || ''}\\Google\\Chrome\\Application\\chrome.exe`,
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  '/usr/bin/google-chrome', '/usr/bin/google-chrome-stable',
  '/usr/bin/chromium', '/usr/bin/chromium-browser'
].find(p => { try { return p && fs.existsSync(p); } catch { return false; } });

const httpGetJson = url => new Promise((res, rej) => {
  http.get(url, r => { let d = ''; r.on('data', c => d += c);
    r.on('end', () => { try { res(JSON.parse(d)); } catch (e) { rej(e); } }); }).on('error', rej);
});

async function connectCDP(port) {
  for (let i = 0; i < 60; i++) {
    try {
      const t = await httpGetJson(`http://127.0.0.1:${port}/json`);
      const page = t.find(x => x.type === 'page');
      if (page?.webSocketDebuggerUrl) {
        const ws = new WebSocket(page.webSocketDebuggerUrl);
        await new Promise((r, j) => { ws.on('open', r); ws.on('error', j); });
        return ws;
      }
    } catch {}
    await sleep(300);
  }
  return null;
}

// Stripe's fields live in cross-origin iframes, which Chrome runs in their
// own processes. Their execution contexts never appear in the page's Runtime
// domain, so reaching them means attaching to each frame as its own session
// and addressing commands to that session.
function cdpClient(ws) {
  let id = 0;
  const pending = new Map(), listeners = new Map();
  ws.on('message', m => {
    let msg; try { msg = JSON.parse(m.toString()); } catch { return; }
    if (msg.id && pending.has(msg.id)) {
      const p = pending.get(msg.id); pending.delete(msg.id);
      msg.error ? p.reject(new Error(msg.error.message)) : p.resolve(msg.result);
    } else if (msg.method && listeners.has(msg.method)) {
      listeners.get(msg.method).forEach(f => { try { f(msg.params, msg.sessionId); } catch {} });
    }
  });
  return {
    send(method, params = {}, sessionId) {
      const i = ++id;
      return new Promise((res, rej) => {
        pending.set(i, { resolve: res, reject: rej });
        const frame = { id: i, method, params };
        if (sessionId) frame.sessionId = sessionId;
        try { ws.send(JSON.stringify(frame)); }
        catch (e) { pending.delete(i); rej(e); }
        setTimeout(() => { if (pending.has(i)) { pending.delete(i); rej(new Error('timeout ' + method)); } }, 60000);
      });
    },
    on(e, f) { if (!listeners.has(e)) listeners.set(e, []); listeners.get(e).push(f); },
    isAlive: () => ws.readyState === 1
  };
}

function parseEval(result, fallback = {}) {
  const ex = result?.exceptionDetails;
  if (ex) return { ...fallback, _err: (ex.exception?.description || ex.text || '').split('\n')[0].slice(0, 140) };
  const v = result?.result?.value;
  if (v == null) return fallback;
  if (typeof v === 'object') return v;
  if (typeof v !== 'string') return fallback;
  try { return JSON.parse(v); } catch { return fallback; }
}

// ═══════════════════════════════════════════════════════════════
// Typing into Stripe's iframes
// ═══════════════════════════════════════════════════════════════

// Attaches to every frame as it appears and keeps their session ids. Without
// this, an out-of-process iframe is simply invisible to Runtime.evaluate.
async function attachToFrames(cdp) {
  const sessions = new Map();   // sessionId -> { url }

  cdp.on('Target.attachedToTarget', async (p, parentSessionId) => {
    const { sessionId, targetInfo } = p;
    if (!['iframe', 'page'].includes(targetInfo.type)) return;
    // The parent session and this frame's id are what let a point inside the
    // frame be translated into page coordinates later, without guessing which
    // iframe element owns it.
    sessions.set(sessionId, {
      url: targetInfo.url || '',
      frameId: targetInfo.targetId,
      parent: parentSessionId || null
    });
    try {
      await cdp.send('Runtime.enable', {}, sessionId);
      // Keep attaching as frames nest further down
      await cdp.send('Target.setAutoAttach', {
        autoAttach: true, waitForDebuggerOnStart: false, flatten: true
      }, sessionId);
    } catch {}
  });
  cdp.on('Target.detachedFromTarget', p => sessions.delete(p.sessionId));

  await cdp.send('Target.setAutoAttach', {
    autoAttach: true, waitForDebuggerOnStart: false, flatten: true
  });
  return sessions;
}

// Where a frame's origin sits in top-level page coordinates.
//
// DOM.getBoxModel called on an out-of-process frame returns coordinates
// relative to that frame, not to the page, so a point taken straight from it
// lands somewhere near the top-left of the window. The offset has to be added
// by walking up the frame chain — but identified by frame id, not by matching
// iframe URLs, which is what put clicks on the wrong field before.
async function frameOffset(cdp, sessions, sessionId) {
  let x = 0, y = 0;
  let sid = sessionId;

  for (let depth = 0; depth < 8 && sid; depth++) {
    const info = sessions.get(sid);
    if (!info || !info.frameId) break;
    const parentSid = info.parent || undefined;   // undefined means the page

    try {
      // Ask the parent for the <iframe> element that hosts this frame
      const owner = await cdp.send('DOM.getFrameOwner',
        { frameId: info.frameId }, parentSid);
      if (!owner?.backendNodeId) break;
      const { model } = await cdp.send('DOM.getBoxModel',
        { backendNodeId: owner.backendNodeId }, parentSid);
      if (!model?.content) break;
      x += model.content[0];
      y += model.content[1];
    } catch { break; }

    if (!parentSid) break;   // reached the top-level page
    sid = parentSid;
  }
  return { x, y };
}

// Locates a field and returns where to click it, in page coordinates.
const domReady = new Set();

async function locateField(cdp, sessions, selector) {
  const targets = [undefined, ...[...sessions.keys()]];

  for (const sessionId of targets) {
    const key = sessionId || 'main';
    try {
      if (!domReady.has(key)) {
        await cdp.send('DOM.enable', {}, sessionId);
        domReady.add(key);
      }

      const { root } = await cdp.send('DOM.getDocument', { depth: 0 }, sessionId);
      const { nodeId } = await cdp.send('DOM.querySelector',
        { nodeId: root.nodeId, selector }, sessionId);
      if (!nodeId) continue;

      try { await cdp.send('DOM.scrollIntoViewIfNeeded', { nodeId }, sessionId); } catch {}

      const { model } = await cdp.send('DOM.getBoxModel', { nodeId }, sessionId);
      if (!model?.content || model.width === 0 || model.height === 0) continue;

      const q = model.content;
      const localX = (q[0] + q[2] + q[4] + q[6]) / 4;
      const localY = (q[1] + q[3] + q[5] + q[7]) / 4;

      const off = sessionId ? await frameOffset(cdp, sessions, sessionId) : { x: 0, y: 0 };

      const info = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(() => {
          const el = document.querySelector(${JSON.stringify(selector)});
          return JSON.stringify(el ? { value: el.value ?? '', tag: el.tagName.toLowerCase() } : {});
        })()`,
        returnByValue: true, timeout: 5000
      }, sessionId));

      return { sessionId, selector,
               pageX: localX + off.x, pageY: localY + off.y,
               w: model.width, h: model.height,
               value: info.value ?? '', tag: info.tag };
    } catch {}
  }
  return null;
}

// Confirms the browser's focus is actually on the field before anything is
// typed. Without this a mis-aimed click sends the card number into whichever
// box happened to be focused, and the only clue is a value turning up in the
// wrong place seconds later.
async function focusedIs(cdp, sessions, selector) {
  const want = selector.replace(/^#/, '');
  const targets = [undefined, ...[...sessions.keys()]];
  for (const sessionId of targets) {
    try {
      const d = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(() => {
          const a = document.activeElement;
          if (!a || a === document.body) return JSON.stringify({has:false});
          return JSON.stringify({ has:true, id: a.id || '', name: a.getAttribute('name') || '' });
        })()`, returnByValue: true, timeout: 4000 }, sessionId));
      if (d.has && d.id === want) return true;
    } catch {}
  }
  return false;
}

// Reads a field's current value without moving anything
async function readField(cdp, sessions, selector) {
  const expr = `(() => {
    const el = document.querySelector(${JSON.stringify(selector)});
    return JSON.stringify(el ? { found: true, value: el.value ?? '' } : { found: false });
  })()`;
  const targets = [undefined, ...[...sessions.keys()]];
  for (const sessionId of targets) {
    try {
      const d = parseEval(await cdp.send('Runtime.evaluate',
        { expression: expr, returnByValue: true, timeout: 5000 }, sessionId));
      if (d.found) return d;
    } catch {}
  }
  return null;
}

async function clickAt(cdp, x, y) {
  const px = Math.round(x), py = Math.round(y);
  await cdp.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: px, y: py, button: 'none' });
  await sleep(60 + Math.random() * 90);
  await cdp.send('Input.dispatchMouseEvent', { type: 'mousePressed', x: px, y: py, button: 'left', clickCount: 1 });
  await sleep(40 + Math.random() * 60);
  await cdp.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x: px, y: py, button: 'left', clickCount: 1 });
  await sleep(150);
}

// Types with real key events. Assigning .value is ignored by Stripe's fields,
// and it would also skip the formatting they apply while you type.
async function typeText(cdp, text) {
  const s = String(text);
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    const isDigit = /[0-9]/.test(ch);
    const base = { text: ch, unmodifiedText: ch, key: ch };
    if (isDigit) { base.windowsVirtualKeyCode = ch.charCodeAt(0); base.code = 'Digit' + ch; }
    try {
      // keyDown carrying `text` already makes Chrome produce the character.
      // Sending a separate `char` event on top of it inserts the same
      // character a second time, which is how one keystroke on "4" left the
      // field reading "44" and every retry after it failed the prefix check.
      await cdp.send('Input.dispatchKeyEvent', { type: 'keyDown', ...base });
      await cdp.send('Input.dispatchKeyEvent', { type: 'keyUp', ...base });
    } catch {}

    // Real typing is uneven and much slower than a loop can manage on its own.
    // Digits come in bursts of four on a card, with a beat at each boundary,
    // and every so often a person simply pauses.
    // The pause used to be longer at each group of four, on the theory that
    // it looked human. It also handed the field a moment to reformat itself
    // mid-entry, which is exactly when characters get displaced — so the
    // cadence is now steady and the grouping pause is gone.
    // Measured from a real person filling this same form: 257ms between
    // characters on text fields, 361ms on the card number, with occasional
    // longer pauses. Matching that costs a few seconds and removes the
    // single most obvious tell.
    let gap = 210 + Math.random() * 190;
    if (Math.random() < 0.10) gap += 400 + Math.random() * 900;
    await sleep(gap);
  }
}

async function clearField(cdp) {
  for (let i = 0; i < 30; i++) {
    try {
      await cdp.send('Input.dispatchKeyEvent', { type: 'keyDown', key: 'Backspace',
        windowsVirtualKeyCode: 8, code: 'Backspace' });
      await cdp.send('Input.dispatchKeyEvent', { type: 'keyUp', key: 'Backspace',
        windowsVirtualKeyCode: 8, code: 'Backspace' });
    } catch {}
    await sleep(12);
  }
}

const digitsOf = v => String(v || '').replace(/\D/g, '');

// Waits for a condition instead of guessing at a duration. A slow link or a
// proxy stretches every step here, so nothing is timed — each stage waits for
// the thing it actually needs.
async function waitUntil(check, { timeoutMs = 60000, everyMs = 500 } = {}) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    try { const r = await check(); if (r) return r; } catch {}
    await sleep(everyMs);
  }
  return null;
}

async function fieldExists(cdp, sessions, selector) {
  const f = await locateField(cdp, sessions, selector);
  return f && f.w > 0 ? f : null;
}

// Types a value one character at a time, waiting for each keystroke to land
// before deciding anything.
//
// The previous approach read the field, compared, and typed the next
// character. That cannot tell "the field is empty and finished" apart from
// "the field has not processed the keystroke yet" — both read as empty — so
// the first digit went in twice and the value was wrecked before the third
// character. Waiting for the value to actually change removes the ambiguity:
// a keystroke that had no effect is simply re-sent, and one that did is never
// duplicated.
async function typeVerified(cdp, sessions, selector, want, { numeric, refocus }) {
  const norm = v => numeric ? digitsOf(v) : String(v ?? '');
  const target = norm(want);

  const read = async () => norm((await readField(cdp, sessions, selector))?.value);

  // Polls until the value stops being `from`, or gives up
  const waitForChange = async (from, timeoutMs = 3000) => {
    const until = Date.now() + timeoutMs;
    while (Date.now() < until) {
      await sleep(90);
      const now = await read();
      if (now !== from) return now;
    }
    return from;
  };

  const clearAll = async () => {
    for (let i = 0; i < 4; i++) {
      await clearField(cdp);
      await sleep(250);
      if ((await read()) === '') return true;
    }
    return (await read()) === '';
  };

  let cur = await read();
  let wipes = 0;
  let stuck = 0;

  // Generous ceiling: every character may need a retry and still finish
  const maxRounds = target.length * 5 + 30;

  for (let round = 0; round < maxRounds; round++) {
    if (cur === target) return { ok: true };

    if (!target.startsWith(cur)) {
      trace(`"${cur}" is not a prefix of "${target}" — clearing`);
      if (++wipes > 8) return { ok: false, why: `field kept rejecting input (stuck at "${cur}")` };
      await clearAll();
      cur = await read();
      stuck = 0;
      continue;
    }

    const next = want[cur.length] !== undefined && !numeric
      ? String(want)[cur.length]
      : target[cur.length];

    const before = cur;
    await typeText(cdp, next);
    cur = await waitForChange(before);
    trace(`sent "${next}"   field: "${before}" → "${cur}"` +
          (cur === before ? '   (no effect)' : ''));

    if (cur === before) {
      stuck++;
      // A keystroke with no effect usually means the cursor is no longer in
      // this field. Stripe rebuilds the input when its formatting kicks in —
      // at the fourth digit of a card number, which is exactly where entry
      // was stalling — and focus is lost with it. Put the cursor back rather
      // than keep typing into nothing.
      if (refocus && !(await focusedIs(cdp, sessions, selector))) {
        trace('focus was lost — clicking the field again');
        const back = await refocus();
        if (!back) return { ok: false, why: `lost the cursor at "${cur}" and could not get it back` };
        stuck = 0;
        continue;
      }
      if (stuck > 3) return { ok: false, why: `field stopped accepting input at "${cur}"` };
    } else {
      stuck = 0;
    }
  }

  const final = await read();
  return final === target
    ? { ok: true }
    : { ok: false, why: `reached "${final}" of "${target}"` };
}

async function fillInput(cdp, sessions, selector, value, opts = {}) {
  // Fields appear as the form progresses, so wait for one rather than
  // declaring it missing on the first look.
  let f = null;
  const deadline = Date.now() + (opts.waitMs ?? 8000);
  while (Date.now() < deadline) {
    f = await locateField(cdp, sessions, selector);
    if (f && f.w > 0) break;
    f = null;
    await sleep(600);
  }
  if (!f) return { ok: false, why: 'field never appeared' };

  const want = String(value);
  const wantDigits = digitsOf(want);
  const numeric = wantDigits.length > 0 &&
                  wantDigits.length === want.replace(/[\s/]/g, '').length;

  // Click, then confirm the browser really focused this field before typing
  // a single character into it.
  let focused = false;
  for (let tryNo = 1; tryNo <= 4 && !focused; tryNo++) {
    const spot = tryNo === 1 ? f : (await locateField(cdp, sessions, selector)) || f;
    await clickAt(cdp, spot.pageX, spot.pageY);
    await sleep(250);
    focused = await focusedIs(cdp, sessions, selector);
    if (!focused) await sleep(350);
  }
  if (!focused) {
    // Say where the click actually went and what ended up focused, so a
    // coordinate problem is visible immediately rather than inferred from
    // characters turning up in the wrong box.
    let landed = 'nothing';
    try {
      const d = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(() => {
          const a = document.activeElement;
          return JSON.stringify({ id: a?.id || '', tag: a?.tagName?.toLowerCase() || '' });
        })()`, returnByValue: true, timeout: 4000 }));
      if (d.id || d.tag) landed = d.id ? '#' + d.id : d.tag;
    } catch {}
    return { ok: false,
      why: `clicked at ${Math.round(f.pageX)},${Math.round(f.pageY)} but focus went to ${landed}` };
  }

  trace(`${selector} at ${Math.round(f.pageX)},${Math.round(f.pageY)} ` +
        `in ${f.sessionId ? 'a frame' : 'the page'} — typing "${want}"`);

  const refocus = async () => {
    for (let tryNo = 1; tryNo <= 3; tryNo++) {
      const spot = (await locateField(cdp, sessions, selector)) || f;
      await clickAt(cdp, spot.pageX, spot.pageY);
      await sleep(220);
      if (await focusedIs(cdp, sessions, selector)) return true;
      await sleep(300);
    }
    return false;
  };

  const res = await typeVerified(cdp, sessions, selector, want, { numeric, refocus });
  const after = await readField(cdp, sessions, selector);
  return { ok: res.ok, got: after?.value || '', why: res.ok ? null : res.why };
}

// A <select> genuinely does respond to a scripted change, unlike Stripe's
// text inputs, so this sets it directly and fires the event React listens for.
async function selectOption(cdp, sessions, selector, value) {
  const expr = `(() => {
    const el = document.querySelector(${JSON.stringify(selector)});
    if (!el) return JSON.stringify({found:false});
    const want = ${JSON.stringify(String(value))}.toLowerCase();
    let opt = [...el.options].find(o => o.value.toLowerCase() === want);
    if (!opt) opt = [...el.options].find(o => (o.textContent||'').trim().toLowerCase() === want);
    if (!opt) return JSON.stringify({found:true, matched:false,
      options: [...el.options].slice(0,8).map(o => o.value)});
    el.focus();
    el.value = opt.value;
    el.dispatchEvent(new Event('change', {bubbles:true}));
    el.dispatchEvent(new Event('input', {bubbles:true}));
    return JSON.stringify({found:true, matched:true, value: el.value, label: opt.textContent});
  })()`;

  // Wait for it, since the address block renders after the card is accepted
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    const targets = [undefined, ...[...sessions.keys()]];
    for (const sessionId of targets) {
      try {
        const d = parseEval(await cdp.send('Runtime.evaluate',
          { expression: expr, returnByValue: true, timeout: 5000 }, sessionId));
        if (d.found) return d.matched
          ? { ok: true, label: d.label }
          : { ok: false, why: `no option "${value}" (saw ${(d.options||[]).slice(0,4).join(', ')})` };
      } catch {}
    }
    await sleep(700);
  }
  return { ok: false, why: 'select never appeared' };
}

// Calls a ChatGPT endpoint from inside the page.
//
// The same request issued straight from Node is refused: the sentinel
// endpoint answers with a proof-of-work challenge and turnstile.required,
// and a bare HTTPS request has no way to satisfy either. Issued from the
// page, both have already been dealt with by the browser.
async function apiCall(cdp, method, url, body) {
  const FALLBACK_TOKEN = JSON.stringify(global.__accessToken || '');
  const FALLBACK_ACCT  = JSON.stringify(global.__acctId || '');
  const expr = `(async () => {
      const FALLBACK_TOKEN = ${FALLBACK_TOKEN};
      const FALLBACK_ACCT  = ${FALLBACK_ACCT};
    try {
      const headers = { 'Content-Type': 'application/json', 'OAI-Language': 'en-US' };

      // The cookie alone is not enough for these endpoints — they answer
      // "Unauthorized - Access token is missing" without a bearer token, and
      // an account id header. Both come from the session endpoint, which the
      // page can read for itself.
      try {
        const sess = await (await fetch('/api/auth/session', { credentials: 'include' })).json();
        if (sess?.accessToken) headers['Authorization'] = 'Bearer ' + sess.accessToken;
        if (sess?.account?.id) headers['ChatGPT-Account-ID'] = sess.account.id;
        // "Access token is missing" says the header never arrived, not that
        // it went stale. The file this run started from carries a token, so
        // fall back to it rather than send the request unauthenticated.
        if (!headers['Authorization'] && FALLBACK_TOKEN)
          headers['Authorization'] = 'Bearer ' + FALLBACK_TOKEN;
        if (!headers['ChatGPT-Account-ID'] && FALLBACK_ACCT)
          headers['ChatGPT-Account-ID'] = FALLBACK_ACCT;
      } catch (e) {}

      const opts = { method: ${JSON.stringify(method)}, credentials: 'include', headers };
      ${body ? `opts.body = ${JSON.stringify(JSON.stringify(body))};` : ''}
      const r = await fetch(${JSON.stringify(url)}, opts);
      const t = await r.text();
      let j = null; try { j = JSON.parse(t); } catch {}
      return JSON.stringify({ status: r.status, json: j, text: t.slice(0, 400),
                              hadToken: !!headers['Authorization'] });
    } catch (e) { return JSON.stringify({ status: 0, error: String(e).slice(0, 160) }); }
  })()`;
  return parseEval(await cdp.send('Runtime.evaluate',
    { expression: expr, awaitPromise: true, returnByValue: true, timeout: 30000 }));
}

const whenDate = v => {
  if (!v) return 'unknown';
  const n = Number(v);
  const d = Number.isFinite(n) && n > 1e9 ? new Date(n * (n > 1e12 ? 1 : 1000)) : new Date(v);
  return isNaN(d) ? String(v) : d.toISOString().slice(0, 16).replace('T', ' ');
};

// Turns off auto-renewal for the account that was just charged.
//
// Done here rather than as a separate run because the moment after a
// successful payment is the only one where the subscription certainly exists,
// the session is certainly live, and a browser is already open on the right
// account. A follow-up script would have to re-establish all three.
// Captures the headers the application itself puts on a backend request.
//
// The integrity values are generated by the page and change per request, so
// they cannot be written into a script. Watching one real call and reusing
// its headers is the difference between a request the server accepts and one
// it refuses with 401.
async function borrowHeaders(cdp, timeoutMs = 25000) {
  return new Promise(async resolve => {
    let done = false;
    const finish = v => { if (!done) { done = true; resolve(v); } };

    const onSend = p => {
      const u = p.request?.url || '';
      if (!/chatgpt\.com\/backend-api\//.test(u)) return;
      const h = p.request.headers || {};
      // Only useful if it carries the integrity pair
      const keys = Object.keys(h).map(k => k.toLowerCase());
      if (!keys.some(k => k.startsWith('x-oai-is-'))) return;
      const out = {};
      for (const [k, v] of Object.entries(h)) {
        if (k.startsWith(':')) continue;
        const lk = k.toLowerCase();
        // Everything except what the new request defines for itself
        if (['content-length', 'host', 'cookie', 'accept-encoding'].includes(lk)) continue;
        out[k] = String(v);
      }
      finish(out);
    };

    cdp.on('Network.requestWillBeSent', onSend);

    // Give the page a reason to talk to the backend
    try {
      await cdp.send('Runtime.evaluate', {
        expression: `fetch('/backend-api/me',{credentials:'include'}).catch(()=>{})`,
        returnByValue: true, timeout: 8000
      });
    } catch {}

    setTimeout(() => finish(null), timeoutMs);
  });
}

// Same as apiCall, but sends a specific set of headers.
async function apiCallWith(cdp, method, url, body, headers) {
  const expr = `(async () => {
    try {
      const h = ${JSON.stringify(headers)};
      // The freshest token wins over whatever the borrowed set carried
      try {
        const s = await (await fetch('/api/auth/session?refresh=true',{credentials:'include'})).json();
        if (s && s.accessToken) h['authorization'] = 'Bearer ' + s.accessToken;
        if (s && s.account && s.account.id) h['chatgpt-account-id'] = s.account.id;
      } catch (e) {}
      const o = { method: ${JSON.stringify(method)}, credentials: 'include', headers: h };
      ${body ? `o.body = ${JSON.stringify(JSON.stringify(body))};` : ''}
      const r = await fetch(${JSON.stringify(url)}, o);
      const t = await r.text();
      let j = null; try { j = JSON.parse(t); } catch (e) {}
      return JSON.stringify({ status: r.status, json: j, text: t.slice(0, 400), borrowed: true });
    } catch (e) { return JSON.stringify({ status: 0, error: String(e).slice(0,160) }); }
  })()`;
  return parseEval(await cdp.send('Runtime.evaluate',
    { expression: expr, awaitPromise: true, returnByValue: true, timeout: 30000 }));
}

async function stopAutoRenewal(cdp, opts = {}) {
  const log = m => console.log(`    ${m}`);
  const t0 = Date.now();
  const el = () => ((Date.now() - t0) / 1000).toFixed(1) + 's';

  let accountId = opts.accountId || global.__acctId || null;
  if (!accountId) {
    try {
      const who = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(async()=>{try{
          const d = await (await fetch('/api/auth/session',{credentials:'include'})).json();
          return JSON.stringify({ accountId: d?.account?.id });
        }catch(e){ return JSON.stringify({}); }})()`,
        awaitPromise: true, returnByValue: true, timeout: 15000 }));
      accountId = who.accountId;
    } catch (e) {}
  }
  if (!accountId) return { done: false, why: 'could not read the account id' };

  // Every shape below was measured against a live account. What the endpoint
  // requires, and what it refuses:
  //
  //   body {"account_id": "<id>"}   required — {} and no body both give
  //                                 422 "Field required", and putting the id
  //                                 in the query string does not count
  //   a bearer token                required — the cookie alone gives 401
  //   /api/auth/session?refresh=true  alone gives 401: it returns nothing at
  //                                 all on some loads, so the plain endpoint
  //                                 has to be the fallback
  //   ChatGPT-Account-ID header     optional, kept because it costs nothing
  //
  // Three attempts follow, differing in where the token comes from. The file's
  // token matters because it does not depend on the page, and the page is
  // exactly what stops producing one after a payment.
  const cancelWith = async (bearerMode) => {
    const expr = `(async()=>{try{
      const h={'Content-Type':'application/json','OAI-Language':'en-US',
               'ChatGPT-Account-ID':${JSON.stringify(accountId)}};
      ${bearerMode === 'page'
        ? `try{let s=await (await fetch('/api/auth/session?refresh=true',{credentials:'include'})).json();
             if(!s||!s.accessToken) s=await (await fetch('/api/auth/session',{credentials:'include'})).json();
             if(s&&s.accessToken)h['Authorization']='Bearer '+s.accessToken;}catch(e){}`
        : `h['Authorization']='Bearer '+${JSON.stringify(global.__accessToken || '')};`}
      if(!h['Authorization']) return JSON.stringify({status:0,text:'no token available',skipped:true});
      const r=await fetch('/backend-api/subscriptions/cancel',{
        method:'POST',credentials:'include',headers:h,
        body:JSON.stringify({account_id:${JSON.stringify(accountId)}})});
      const t=await r.text(); let j=null; try{j=JSON.parse(t)}catch(e){}
      return JSON.stringify({status:r.status,json:j,text:t.slice(0,140)});
    }catch(e){return JSON.stringify({status:0,text:String(e).slice(0,110)})}})()`;
    return parseEval(await cdp.send('Runtime.evaluate',
      { expression: expr, awaitPromise: true, returnByValue: true, timeout: 40000 }));
  };

  const readPlan = async () => {
    try {
      return parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(async()=>{try{
          const h={'OAI-Language':'en-US'};
          try{let s=await (await fetch('/api/auth/session?refresh=true',{credentials:'include'})).json();
            if(!s||!s.accessToken) s=await (await fetch('/api/auth/session',{credentials:'include'})).json();
            if(s&&s.accessToken)h['Authorization']='Bearer '+s.accessToken;}catch(e){}
          if(!h['Authorization'] && ${JSON.stringify(global.__accessToken || '')})
            h['Authorization']='Bearer '+${JSON.stringify(global.__accessToken || '')};
          const r=await fetch('/backend-api/subscriptions?account_id=${accountId}',
            {credentials:'include',headers:h});
          const t=await r.text(); let j=null; try{j=JSON.parse(t)}catch(e){}
          return JSON.stringify({status:r.status,willRenew:j&&j.will_renew,
            outcome:j&&j.cancellation_outcome,until:j&&j.active_until,plan:j&&j.plan_type});
        }catch(e){return JSON.stringify({status:0})}})()`,
        awaitPromise: true, returnByValue: true, timeout: 25000 }));
    } catch (e) { return { status: 0 }; }
  };

  // Two agreeing reads, seconds apart. A single read once reported a plan as
  // cancelled when a later read showed it renewing, and a run acted on it.
  const confirmOff = async (tries = 8) => {
    let sawOff = 0;
    for (let i = 0; i < tries; i++) {
      await sleep(2500);
      const p = await readPlan();
      if (p.status !== 200) continue;
      if (p.willRenew === false) {
        if (++sawOff >= 2) return { off: true, until: p.until, outcome: p.outcome };
      } else if (p.willRenew === true) return { off: false, readable: true };
    }
    return { off: false, partial: sawOff === 1, readable: sawOff > 0 };
  };

  // Straight after paying, the account answers "no active subscription found"
  // for a few seconds. A cancel against that is refused for a reason that has
  // nothing to do with the token.
  log(`[${el()}] waiting for the subscription to register...`);
  let arrived = null;
  for (let i = 0; i < 12; i++) {
    await sleep(2500);
    const p = await readPlan();
    if (p.status === 200 && p.plan) {
      arrived = p;
      log(`[${el()}] subscription is visible (${p.plan}, renew=${p.willRenew})`);
      break;
    }
  }
  if (arrived && arrived.willRenew === false) {
    let v = { off: false };
    try { v = await confirmOff(4); } catch (e) {}
    return { done: true, endsAt: (v.off ? v.until : arrived.until) || arrived.until,
             outcome: arrived.outcome, via: 'already off on arrival' };
  }

  const tried = [];

  for (const layer of [
    { name: 'page token', bearer: 'page' },
    { name: 'file token', bearer: 'file' }
  ]) {
    const r = await cancelWith(layer.bearer);
    if (r.skipped) { log(`[${el()}] ${layer.name.padEnd(12)} skipped - no token`); continue; }
    log(`[${el()}] ${layer.name.padEnd(12)} -> ${r.status}` +
        (r.status === 200 ? '  OK' : '  ' + String(r.text||'').replace(/\s+/g,' ').slice(0,80)));
    tried.push({ route: layer.name, status: r.status });
    if (r.status === 200) {
      const v = await confirmOff();
      if (v.off) return { done: true, endsAt: v.until, outcome: v.outcome, via: layer.name, tried };
      if (!v.readable) {
        // Accepted, and the plan cannot be read back right now. Reporting that
        // honestly beats trying another route: doing so once cleared the
        // session and lost a cancellation that had already gone through.
        return { done: true, unverified: true, via: layer.name, tried,
                 why: 'the cancel was accepted but could not be read back - check the billing page' };
      }
      log(`[${el()}] accepted but the plan still renews - next layer`);
    }
  }

  // Last layer: the page's token dies after some payments and a full load is
  // what makes the app fetch a new one.
  log(`[${el()}] reloading so the app mints a token...`);
  try {
    await cdp.send('Page.navigate', { url: 'https://chatgpt.com/' });
    await sleep(9000);
  } catch (e) {}
  const r3 = await cancelWith('page');
  log(`[${el()}] after reload  -> ${r3.status}` +
      (r3.status === 200 ? '  OK' : '  ' + String(r3.text||'').replace(/\s+/g,' ').slice(0,80)));
  tried.push({ route: 'after reload', status: r3.status });
  if (r3.status === 200) {
    const v = await confirmOff();
    if (v.off) return { done: true, endsAt: v.until, outcome: v.outcome, via: 'after reload', tried };
    if (!v.readable) {
      return { done: true, unverified: true, via: 'after reload', tried,
               why: 'the cancel was accepted but could not be read back - check the billing page' };
    }
  }

  return { done: false, tried,
           why: `every layer refused (${tried.map(t=>t.route+'='+t.status).join(', ')})` };
}

// Creates a checkout session from inside this browser.
//
// Generating the link in one browser and paying in another meant two separate
// proxy sessions, and a rotating pool hands out a different exit address to
// each — the price was quoted against one Philippine address and the card was
// then presented from a second. Doing both here removes the possibility.
//
// promo_campaign is dropped from the response handling on purpose: the aim is
// the standing price, not whatever offer the account happens to carry.
// Timings the ported functions rely on. They came across with the code and
// belong with it — moving the functions without them is what left
// CLICK_POLL_MS undefined at the first click.
const CLICK_POLL_MS    = 15000;   // how long to look for the plan button
const CHECKOUT_WAIT_MS = 60000;   // how long to wait for the checkout response

// ═══════════════════════════════════════════════════════════════
// Getting a checkout link
//
// These four functions are taken unchanged from the link generator, which
// has been getting links reliably for weeks. Two earlier attempts here did
// not: a direct POST to /payments/checkout came back "unusual activity",
// because a purchase request arriving seconds after sign-in, from a page
// that never displayed a price, looks like nothing a person would do. What
// works is what the generator does — open the pricing page, pick the plan
// the way the interface intends, and let the app make its own request.
//
// Running it in this browser rather than a separate one keeps the link and
// the payment on a single proxy session. Generating in one browser and
// paying in another drew two different exit addresses from the pool, so the
// price was quoted to one Philippine address and the card presented from
// another.
// ═══════════════════════════════════════════════════════════════

function setupCapture(cdp) {
  const box = { current: null };
  const ids = new Set();

  cdp.on('Network.requestWillBeSent', p => {
    const url = p.request?.url || '';
    if (p.request?.method === 'POST' && /\/backend-api\/payments\/checkout(\?|$)/.test(url)) {
      ids.add(p.requestId);
    }
  });
  cdp.on('Network.responseReceived', p => {
    if (!ids.has(p.requestId)) return;
    const c = box.current;
    if (c && !c.status) { c.requestId = p.requestId; c.status = p.response.status; }
  });
  cdp.on('Network.loadingFinished', async p => {
    const c = box.current;
    if (!c || c.requestId !== p.requestId || c.done) return;
    try {
      const b = await cdp.send('Network.getResponseBody', { requestId: p.requestId });
      c.body = b.base64Encoded ? Buffer.from(b.body, 'base64').toString('utf8') : b.body;
      c.done = true;
    } catch (e) { c.bodyError = e.message; }
  });

  return { box, reset: () => { box.current = {}; } };
}

async function ensurePlanVisible(cdp, plan, maxMs = 20000) {
  const script = `
    (async () => {
      const plan = ${JSON.stringify(plan)};
      const ALIASES = {
        plus:['plus'], pro:['pro'], go:['go'], prolite:['pro lite','prolite'],
        business:['business'], team:['team']
      }[plan] || [plan];

      const sleep = ms => new Promise(r => setTimeout(r, ms));

      // Is a purchasable card for this plan on screen? We look for a heading
      // with the plan's name that has a real CTA button somewhere near it.
      const planCardVisible = () => {
        const heads = [...document.querySelectorAll('h1,h2,h3,h4,div,span,p')];
        for (const h of heads) {
          if (h.offsetParent === null) continue;
          const t = (h.innerText || h.textContent || '').trim().toLowerCase();
          if (t.length > 24) continue;
          const isPlan = ALIASES.some(a => t === a || t === 'chatgpt ' + a);
          if (!isPlan) continue;
          // walk up looking for a CTA inside the same card
          let n = h;
          for (let d = 0; d < 7 && n; d++) {
            n = n.parentElement;
            if (!n) break;
            const btns = [...n.querySelectorAll('button,[role="button"],a')]
              .filter(b => b.offsetParent !== null)
              .map(b => (b.innerText || '').replace(/\s+/g, ' ').trim().toLowerCase());
            if (btns.some(b => /^(get|try|upgrade|subscribe|buy|claim|start|choose|select|switch|rejoin|resubscribe|reactivate|renew|restore)/i.test(b))) {
              return true;
            }
          }
        }
        return false;
      };

      // Any element that looks like part of an audience toggle, whatever
      // tag it uses. ChatGPT has shipped these as buttons, divs and labels.
      const toggleParts = () => {
        const WORDS = ['personal','individual','business','enterprise','business & enterprise','teams'];
        const out = [];
        for (const el of document.querySelectorAll('button,[role="tab"],[role="radio"],[role="button"],div,label,span,a')) {
          if (el.offsetParent === null) continue;
          const t = (el.innerText || el.textContent || '').trim().toLowerCase();
          if (!WORDS.includes(t)) continue;
          const r = el.getBoundingClientRect();
          if (r.width < 40 || r.height < 16 || r.width > 600) continue;
          // Skip wrappers that contain another candidate
          if ([...el.querySelectorAll('*')].some(c =>
                WORDS.includes((c.innerText || c.textContent || '').trim().toLowerCase()))) continue;
          out.push({ el, t });
        }
        // de-dupe by text, keep the smallest box for each
        const byText = {};
        for (const o of out) {
          const r = o.el.getBoundingClientRect();
          const area = r.width * r.height;
          if (!byText[o.t] || area < byText[o.t].area) byText[o.t] = { ...o, area };
        }
        return Object.values(byText);
      };

      const deadline = Date.now() + ${maxMs};
      let clicks = [];

      while (Date.now() < deadline) {
        if (planCardVisible()) {
          return JSON.stringify({ ok: true, clicks, alreadyThere: clicks.length === 0 });
        }

        const parts = toggleParts();
        if (parts.length >= 2) {
          // Click a side we haven't tried yet
          const next = parts.find(p => !clicks.includes(p.t));
          if (next) {
            try { next.el.scrollIntoView({ block: 'center' }); } catch (e) {}
            next.el.click();
            clicks.push(next.t);
            await sleep(1800);
            continue;
          }
        }
        await sleep(500);
      }

      // Report what we saw so a failure is diagnosable
      const parts = toggleParts().map(p => p.t);
      const heads = [...document.querySelectorAll('h1,h2,h3')]
        .filter(h => h.offsetParent !== null)
        .map(h => (h.innerText || '').trim())
        .filter(t => t && t.length < 40);
      return JSON.stringify({
        ok: false, clicks,
        toggles: parts,
        headings: [...new Set(heads)].slice(0, 12)
      });
    })()
  `;
  try {
    const r = await cdp.send('Runtime.evaluate', {
      expression: script, awaitPromise: true, returnByValue: true, timeout: maxMs + 8000
    });
    return parseEval(r);
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

async function clickPlanCTA(cdp, plan, maxMs = CLICK_POLL_MS) {
  const script = `
    (async () => {
      const plan = ${JSON.stringify(plan)};
      const ALIASES = {
        plus:['plus','chatgpt plus'], pro:['pro','chatgpt pro'], go:['go','chatgpt go'],
        prolite:['pro lite','prolite'], business:['business','chatgpt business'],
        team:['team','chatgpt team']
      }[plan] || [plan];

      const TAB      = /^(individual|business|enterprise|business & enterprise|personal|team|monthly|yearly|annual|features|learn|codex|pricing)$/i;
      const STATUS   = /current plan|your plan|manage subscription|active plan/i;
      const NAV_ONLY = /^(free|go|plus|pro|prolite)$/i;
      // Sign-in buttons score on the action pattern alone — "continue with
      // google" reads as an action with no plan name and still clears the
      // threshold, which is how a run ended up inside Google's login flow
      // waiting for a checkout that was never coming.
      const AUTH     = /^(continue with|sign in|log in|sign up|create account|forgot|next$)|google|apple|microsoft|single sign|passkey/i;
      const ACTION   = /^(get|try|upgrade to|upgrade|subscribe to|subscribe|buy|claim|start|choose|select|switch to|rejoin|resubscribe|reactivate|renew|restore|continue with|go)\\b/i;
      const FREEOFFER= /claim.*free|free.*trial|try.*free|start.*trial|free.*offer/i;

      // The plan buttons carry a stable identifier, e.g.
      // data-testid="select-plan-button-plus-upgrade". Matching that is far
      // more reliable than reading the visible label, which varies between
      // "Get Plus", "Upgrade to Plus" and "Rejoin Plus".
      const byTestId = () => {
        const el = document.querySelector(
          '[data-testid*="select-plan-button-' + plan + '" i]');
        if (!el || el.offsetParent === null || el.disabled) return null;
        return { el, s: 999, t: (el.innerText||'').replace(/\s+/g,' ').trim().slice(0,60), via: 'data-testid' };
      };

      const best = () => {
        const byId = byTestId();
        if (byId) return byId;

        const els = document.querySelectorAll('button, [role="button"], a[href*="checkout"], a[href*="upgrade"], a[href*="/pricing/"]');
        const out = [];
        for (const el of els) {
          if (el.disabled || el.offsetParent === null) continue;
          const r = el.getBoundingClientRect();
          // Compact CTAs exist — the "Rejoin Plus" pill is only 34px tall and
          // sits in the header, so the floor has to stay low enough to admit
          // it while still excluding icon-only controls.
          if (r.width < 40 || r.height < 16) continue;
          // An icon inside the button can leave stray whitespace and newlines
          // in innerText, which breaks a pattern anchored with ^.
          const t = (el.innerText || el.textContent || '')
            .replace(/\s+/g, ' ').trim().toLowerCase();
          if (!t || t.length > 80) continue;
          if (TAB.test(t) || STATUS.test(t) || NAV_ONLY.test(t) || AUTH.test(t)) continue;

          // Which plan's card is this button sitting in? A label like
          // "Upgrade to Business" scores on the verb alone, and with the
          // threshold at one hundred that was enough to be clicked while
          // asking for Plus. The card around the button says who it belongs
          // to, and a button inside another plan's card is never the one.
          const OTHERS = ['plus','pro','go','business','team','enterprise']
            .filter(x => !ALIASES.some(a => a.includes(x)));
          let cardText = '';
          let node = el.parentElement;
          for (let up = 0; up < 6 && node; up++, node = node.parentElement) {
            const txt = (node.innerText || '').toLowerCase();
            // A card is the first ancestor carrying both a name and a price
            if (/(?:[a-z]{3}|[$€£¥₱₹])\s?\d/.test(txt) && txt.length < 1200) {
              cardText = txt;
              break;
            }
          }

          const inOurCard    = cardText && ALIASES.some(a => cardText.includes(a));
          const inOtherCard  = cardText && !inOurCard &&
                               OTHERS.some(o => new RegExp('\\b' + o + '\\b').test(cardText));
          if (inOtherCard) continue;

          let s = 0;
          for (const a of ALIASES) if (t.includes(a)) { s += 200; break; }
          if (ACTION.test(t))    s += 100;
          if (FREEOFFER.test(t)) s += 100;
          if (inOurCard)         s += 200;

          // The name has to come from somewhere — the label itself or the
          // card the button lives in. A verb on its own is not enough.
          const named = ALIASES.some(a => t.includes(a)) || inOurCard;
          if (!named || s < 200) continue;
          out.push({ el, s, t: t.slice(0, 60) });
        }
        out.sort((a, b) => b.s - a.s);
        return out[0] || null;
      };

      const start = Date.now();
      while (Date.now() - start < ${maxMs}) {
        const b = best();
        if (b) {
          try { b.el.scrollIntoView({block:'center'}); } catch(e){}
          b.el.click();
          return JSON.stringify({clicked:true, text:b.t, score:b.s, via:b.via || 'score'});
        }
        await new Promise(r => setTimeout(r, 400));
      }
      const seen = [...document.querySelectorAll('button,[role="button"]')]
        .map(b => (b.innerText||'').trim().replace(/\\n/g,' | '))
        .filter(t => t && t.length < 60);
      return JSON.stringify({clicked:false, buttons:[...new Set(seen)].slice(0,12)});
    })()
  `;
  const r = await cdp.send('Runtime.evaluate', {
    expression: script, awaitPromise: true, returnByValue: true, timeout: maxMs + 5000
  });
  return parseEval(r);
}

async function awaitCheckout(cdp, box, timeoutMs = CHECKOUT_WAIT_MS) {
  const start = Date.now();
  let navUrl = null;
  let secondStepTried = false;

  while (Date.now() - start < timeoutMs) {
    if (box.current?.done) break;
    try {
      const r = await cdp.send('Runtime.evaluate', {
        expression: 'window.location.href', returnByValue: true, timeout: 3000
      });
      const cur = String(r.result?.value || '');
      // Both formats appear: cs_ for the hosted flow, oaics_ for the in-page one
      if (/\/checkout\/openai_llc\/(cs|oaics)_[A-Za-z0-9_]+/.test(cur)) {
        navUrl = cur.split('?')[0].split('#')[0];
        break;
      }
    } catch {}

    // Some variants of the modal put a confirmation step between choosing the
    // plan and creating the checkout session. If nothing has happened after a
    // few seconds, look for that step and advance it.
    if (!secondStepTried && Date.now() - start > 6000) {
      secondStepTried = true;
      try {
        const r = await cdp.send('Runtime.evaluate', {
          expression: `(() => {
            const RE = /^(subscribe|confirm|continue|pay|complete|next|proceed)\\b/i;
            for (const b of document.querySelectorAll('button,[role="button"]')) {
              if (b.disabled || b.offsetParent === null) continue;
              const t = (b.innerText||'').replace(/\\s+/g,' ').trim();
              if (t && t.length < 40 && RE.test(t)) { b.click(); return JSON.stringify({clicked:t}); }
            }
            return JSON.stringify({clicked:null});
          })()`,
          returnByValue: true, timeout: 8000
        });
        const st = parseEval(r);
        if (st.clicked) console.log(`\n  Advanced a second step ("${st.clicked}")`);
      } catch {}
    }

    await sleep(400);
  }

  const c = box.current;
  if (c?.done && c.status === 200) {
    const data = JSON.parse(c.body);
    if (!data.checkout_session_id) return { ok: false, error: 'response had no checkout_session_id' };
    return {
      ok: true, via: 'api',
      url: data.url || `https://chatgpt.com/checkout/openai_llc/${data.checkout_session_id}`,
      data
    };
  }
  if (c?.done && c.status !== 200) {
    const msg = String(c.body || '');
    const unusual = /unusual activity/i.test(msg);
    return { ok: false, http: c.status, unusual, error: `HTTP ${c.status}${unusual ? ' (unusual activity)' : ''}` };
  }
  if (navUrl) {
    return {
      ok: true, via: 'navigation', url: navUrl,
      data: { checkout_session_id: navUrl.match(/(cs_[a-z_]+_[A-Za-z0-9]+)/)?.[1] || null }
    };
  }
  // A bare timeout says nothing useful. Report where the page ended up and
  // what is on it, so the next step is obvious rather than guesswork.
  let where = '';
  try {
    const r = await cdp.send('Runtime.evaluate', {
      expression: `(() => {
        const btns = [...document.querySelectorAll('button,[role="button"]')]
          .filter(b => b.offsetParent !== null)
          .map(b => (b.innerText||'').replace(/\\s+/g,' ').trim())
          .filter(t => t && t.length < 40);
        const heads = [...document.querySelectorAll('h1,h2,h3')]
          .filter(h => h.offsetParent !== null)
          .map(h => (h.innerText||'').trim()).filter(t => t && t.length < 60);
        return JSON.stringify({
          url: location.pathname + location.hash,
          heads: [...new Set(heads)].slice(0,5),
          btns: [...new Set(btns)].slice(0,10)
        });
      })()`,
      returnByValue: true, timeout: 8000
    });
    const d = parseEval(r);
    where = `\n     Page is at ${d.url}` +
            (d.heads?.length ? `\n     Showing: ${d.heads.join(' · ')}` : '') +
            (d.btns?.length ? `\n     Buttons: ${d.btns.join(' · ')}` : '');
  } catch {}

  return { ok: false, error: `no checkout response within ${timeoutMs / 1000}s${where}` };
}

// Everything the billing side of the account can say or be asked to do.
//
// Endpoints and response shapes here were taken from a recording of the real
// settings page rather than guessed. An earlier attempt used
// /backend-api/payment_methods — no payments/ segment — and reported no cards
// on an account that plainly had two.
async function readBilling(cdp, accountId) {
  const sub = await apiCall(cdp, 'GET', `/backend-api/subscriptions?account_id=${accountId}`);
  const pm  = await apiCall(cdp, 'GET', `/backend-api/payments/payment_methods?account_id=${accountId}`);
  const s = sub.json || {};
  const cards = pm.json?.payment_methods || [];
  return {
    plan: s.plan_type || null,
    period: s.billing_period || null,
    currency: s.billing_currency || null,
    activeUntil: s.active_until || null,
    willRenew: s.will_renew,
    cancelled: s.cancellation_outcome || null,
    delinquent: !!s.is_delinquent,
    cards: cards.map(c => ({
      id: c.id, brand: c.card?.brand || '?', last4: c.card?.last4 || '????',
      isDefault: c.id === pm.json?.default_payment_method_id
    }))
  };
}

// Takes our own card off the customer's account after the payment.
//
// The card stays on file once it has been used, and the card gets topped up
// and reused for other customers — so a renewal on an account we have left
// would charge us. Cancelling the renewal is the first defence; this is the
// second, and it does not depend on the subscription state at all.
//
// Two rules, both measured against a live account:
//
//   The server refuses to remove the default card outright —
//   400 "Cannot delete default payment method". Where another card exists,
//   handing it the default first clears the way.
//
//   Where ours is the only card, there is nothing to hand it to. Removing it
//   is not possible, and the cancelled renewal has to stand on its own.
//
//   DELETE /backend-api/payments/payment_method/<id>?account_id=<id>
//          → { success: true }     account_id must be in the query; in the
//                                  body the server answers 422 loc ["query"]
//   POST   /backend-api/payments/payment_method/default
//          body { payment_method_id, account_id }  → { success: true }
async function removeOurCard(cdp, accountId, cardNumber, note) {
  const ours = String(cardNumber).replace(/\s/g, '').slice(-4);

  const api = async (method, url, body) => parseEval(await cdp.send('Runtime.evaluate', {
    expression: `(async()=>{try{
      const h={'Content-Type':'application/json','OAI-Language':'en-US',
               'ChatGPT-Account-ID':${JSON.stringify(accountId)}};
      try{let s=await (await fetch('/api/auth/session?refresh=true',{credentials:'include'})).json();
        if(!s||!s.accessToken) s=await (await fetch('/api/auth/session',{credentials:'include'})).json();
        if(s&&s.accessToken)h['Authorization']='Bearer '+s.accessToken;}catch(e){}
      if(!h['Authorization'] && ${JSON.stringify(global.__accessToken || '')})
        h['Authorization']='Bearer '+${JSON.stringify(global.__accessToken || '')};
      const o={method:${JSON.stringify(method)},credentials:'include',headers:h};
      ${body ? `o.body=${JSON.stringify(JSON.stringify(body))};` : ''}
      const r=await fetch(${JSON.stringify(url)},o);
      const t=await r.text(); let j=null; try{j=JSON.parse(t)}catch(e){}
      return JSON.stringify({status:r.status,json:j,text:t.slice(0,130)});
    }catch(e){return JSON.stringify({status:0,text:String(e).slice(0,90)})}})()`,
    awaitPromise: true, returnByValue: true, timeout: 30000 }));

  const list = async () => {
    const r = await api('GET', `/backend-api/payments/payment_methods?account_id=${accountId}`);
    if (r.status !== 200) return null;
    return {
      cards: (r.json?.payment_methods || []).map(c => ({
        id: c.id, brand: c.card?.brand || '?', last4: c.card?.last4 || '????' })),
      defaultId: r.json?.default_payment_method_id || null
    };
  };

  let l = null;
  for (let i = 0; i < 6 && !l; i++) { l = await list(); if (!l) await sleep(2500); }
  if (!l) return { done: false, why: 'could not read the saved cards' };

  const mine = l.cards.filter(c => c.last4 === ours);
  if (!mine.length) return { done: true, nothingToDo: true, why: `no card ending ${ours} on the account` };

  const results = [];
  for (const card of mine) {
    const label = `${card.brand} ****${card.last4}`;

    if (card.id === l.defaultId) {
      const other = l.cards.find(c => c.id !== card.id);
      if (!other) {
        note('card', `****${ours} is the only card on the account — leaving it`);
        results.push({ label, removed: false, why: 'only card on the account' });
        continue;
      }
      note('card', `making ****${other.last4} the default so ours can go`);
      let moved = false;
      for (const body of [
        { payment_method_id: other.id, account_id: accountId },
        { payment_method_id: other.id },
        { id: other.id }
      ]) {
        const r = await api('POST', '/backend-api/payments/payment_method/default', body);
        if (r.status === 200 && r.json?.success !== false) {
          for (let i = 0; i < 5 && !moved; i++) {
            await sleep(1500);
            const again = await list();
            if (again?.defaultId === other.id) { moved = true; l = again; }
          }
        }
        if (moved) break;
      }
      if (!moved) {
        note('card', 'could not move the default — leaving our card in place');
        results.push({ label, removed: false, why: 'default could not be moved' });
        continue;
      }
    }

    const del = await api('DELETE',
      `/backend-api/payments/payment_method/${card.id}?account_id=${accountId}`);
    if (del.status !== 200) {
      note('card', `could not remove ${label} (${del.status})`);
      results.push({ label, removed: false, why: `delete returned ${del.status}` });
      continue;
    }

    // Read it back rather than trust the 200
    let gone = false;
    for (let i = 0; i < 5 && !gone; i++) {
      await sleep(1500);
      const again = await list();
      if (again && !again.cards.some(c => c.id === card.id)) { gone = true; l = again; }
    }
    note('card', gone ? `removed ${label} from the account`
                      : `${label} still shows on the account`);
    results.push({ label, removed: gone });
  }

  const removed = results.filter(r => r.removed);
  return { done: removed.length > 0, results, removed: removed.length,
           left: l.cards.length,
           why: removed.length ? null : (results[0]?.why || 'nothing was removed') };
}

async function deleteCards(cdp, accountId, ids, note) {
  const done = [];
  for (const id of ids) {
    const r = await apiCall(cdp, 'DELETE',
      `/backend-api/payments/payment_method/${id}?account_id=${accountId}`);
    const ok = r.status === 200 && r.json?.success !== false;
    note(ok ? `removed ${id}` : `could not remove ${id} (${r.status})`);
    done.push({ id, ok });
    await sleep(600);
  }
  return done;
}

// Watches the two calls that state, in the site's own words, what the
// payment came to and whether it went through.
//
// Both were taken from a recording of a real subscription:
//
//   POST /backend-api/payments/checkout/taxes
//     → { checkout_session: { currency, amount_subtotal, amount_total,
//                             total_details: { amount_tax, amount_discount } } }
//     amounts are in minor units, so 98214 is ₱982.14 and amount_tax 0 is
//     the tax-free result the address was changed to obtain.
//
//   POST /backend-api/payments/checkout/confirm
//     → { status: "success", type: "payment_intent", … }
//
// The old approach read the totals off the page and decided the outcome from
// whatever text appeared afterwards. That is how a completed payment came back
// as "refused: Think" — a word from the ordinary chat interface, matched as an
// error because the page had already moved on. These two responses are the
// site telling us directly, and neither can be confused with interface text.
function watchPaymentApi(cdp) {
  const state = { taxes: null, confirm: null };
  const want = new Map();   // requestId -> which call

  cdp.on('Network.requestWillBeSent', p => {
    const u = p.request?.url || '';
    if (p.request?.method !== 'POST') return;
    if (/\/backend-api\/payments\/checkout\/taxes/.test(u)) want.set(p.requestId, 'taxes');
    else if (/\/backend-api\/payments\/checkout\/confirm/.test(u)) want.set(p.requestId, 'confirm');
  });

  cdp.on('Network.responseReceived', p => {
    const which = want.get(p.requestId);
    if (which) want.set(p.requestId, { which, status: p.response.status });
  });

  cdp.on('Network.loadingFinished', async p => {
    const entry = want.get(p.requestId);
    if (!entry) return;
    const which = typeof entry === 'string' ? entry : entry.which;
    const status = typeof entry === 'string' ? null : entry.status;
    want.delete(p.requestId);
    let json = null;
    try {
      const b = await cdp.send('Network.getResponseBody', { requestId: p.requestId });
      const text = b.base64Encoded ? Buffer.from(b.body, 'base64').toString('utf8') : b.body;
      try { json = JSON.parse(text); } catch { json = { _raw: String(text).slice(0, 400) }; }
    } catch {}
    state[which] = { at: Date.now(), status, json };
  });

  return {
    state,
    // The amounts the server itself is charging, in major units
    amounts() {
      const cs = state.taxes?.json?.checkout_session;
      if (!cs) return null;
      const div = 100;   // php, usd, egp and the rest of the two-decimal set
      return {
        currency: (cs.currency || '').toUpperCase(),
        subtotal: cs.amount_subtotal != null ? cs.amount_subtotal / div : null,
        total:    cs.amount_total    != null ? cs.amount_total / div : null,
        tax:      cs.total_details?.amount_tax != null ? cs.total_details.amount_tax / div : null,
        discount: cs.total_details?.amount_discount != null ? cs.total_details.amount_discount / div : null,
        paymentStatus: cs.payment_status || null
      };
    },
    // null while nothing has come back yet
    verdict() {
      const c = state.confirm;
      if (!c) return null;
      const st = String(c.json?.status || '').toLowerCase();
      if (c.status === 200 && st === 'success') return { paid: true, raw: c.json };
      return {
        paid: false,
        why: c.json?.error?.message || c.json?.detail || st || `confirm returned ${c.status}`,
        raw: c.json
      };
    },
    reset() { state.taxes = null; state.confirm = null; want.clear(); }
  };
}

// ── Reading the totals, which live in the main page, not the iframe ──
const TOTALS_PROBE = `(() => {
  const lines = (document.body?.innerText || '').split('\\n').map(l => l.trim()).filter(Boolean);
  const MONEY = /([A-Z]{3}|[$€£¥₱₹])\\s?-?[\\d.,]+/;
  const AMOUNT_ONLY = /^-?\\s*(?:[A-Z]{3}|[$€£¥₱₹])\\s?-?[\\d.,]+$/;
  const pick = re => {
    const i = lines.findIndex(l => re.test(l));
    if (i < 0) return '';
    const line = lines[i];
    if (MONEY.test(line)) return line;
    for (let j = i + 1; j <= i + 2 && j < lines.length; j++)
      if (AMOUNT_ONLY.test(lines[j])) return line + '  ' + lines[j];
    return line;
  };
  // Matching on wording alone misses refusals that never use a known word —
  // "Payment was not approved" contains none of them. The page marks any
  // refusal with the same red banner, so read the banner itself and treat
  // whatever it says as the refusal, whatever the wording turns out to be.
  const banners = [...document.querySelectorAll('div,p,span')]
    .filter(el => {
      if (el.offsetParent === null) return false;
      const cls = String(el.className || '');
      const looksRed = /text-red-|bg-red-|border-red-|text-danger|Error|error/i.test(cls);
      if (!looksRed) return false;
      // Keep the innermost element carrying the message, not its wrappers
      return !el.querySelector('[class*="text-red-"],[class*="bg-red-"]');
    })
    .map(el => (el.innerText || '').replace(/\s+/g, ' ').trim())
    .filter(t => t && t.length < 200);

  const WORDS = /declin|insufficient|expired|incorrect|invalid|not approved|unable to|could not|failed|try a different|error/i;
  const wordy = lines.filter(l => WORDS.test(l) && l.length < 200);

  const errs = [...new Set([...banners, ...wordy])];

  return JSON.stringify({
    subscription: pick(/monthly subscription|annual subscription|subtotal/i),
    tax:          pick(/tax|vat/i),
    dueToday:     pick(/due today|total due|amount due/i),
    errors:       errs.slice(0, 5),
    banners:      banners.slice(0, 3),
    url:          location.href
  });
})()`;

// Waits for the tax line to stop moving.
//
// Observed on a real fill: entering an address pushed the total through
// VAT 14% → 5% → 20% → "Estimated tax 0.00" and only reached its final
// "Sales Tax (0%)" seventeen seconds later. "Estimated tax" showing zero is
// not a result, it is the calculation still running — committing on it would
// mean paying against a figure that had not settled.
async function waitForStableTax(cdp, { quietMs = 4000, timeoutMs = 60000, minWaitMs = 30000 } = {}) {
  const read = async () => parseEval(await cdp.send('Runtime.evaluate',
    { expression: TOTALS_PROBE, returnByValue: true, timeout: 8000 }));

  const started = Date.now();
  let last = await read();
  let lastChange = Date.now();
  let seenChanges = 0;

  while (Date.now() - started < timeoutMs) {
    await sleep(600);
    const now = await read();
    const moved = now.tax !== last.tax || now.dueToday !== last.dueToday;
    if (moved) { lastChange = Date.now(); seenChanges++; }
    last = now;

    const elapsed = Date.now() - started;
    const quietFor = Date.now() - lastChange;
    const provisional = /estimated/i.test(now.tax || '');

    // A total that changed and then held still is settled, whatever the clock
    // says — no reason to sit out the rest of the window. But a total that has
    // not moved at all gets the full thirty seconds, because the recalculation
    // can take twenty on a slow connection and cutting it short reads a
    // foreign VAT as the final price.
    if (seenChanges > 0 && quietFor >= quietMs) {
      return { totals: now, settled: true, provisional,
               tookMs: elapsed, changes: seenChanges };
    }
    if (elapsed < minWaitMs) continue;
    // A line still labelled "estimated" needs longer quiet before it counts
    if (quietFor >= (provisional ? quietMs * 2.5 : quietMs)) {
      return { totals: now, settled: true, provisional,
               tookMs: elapsed, changes: seenChanges };
    }
  }
  return { totals: last, settled: false, provisional: /estimated/i.test(last.tax || '') };
}

// Pulls the percentage out of a line like "VAT (14%)  EGP 122.81"
const taxPercent = line => {
  const m = String(line || '').match(/\((\d+(?:\.\d+)?)\s*%\)/);
  return m ? parseFloat(m[1]) : null;
};
// And the amount, so a tax line with no percentage still gets checked
const taxAmount = line => {
  const m = String(line || '').match(/(?:[A-Z]{3}|[$€£¥₱₹])\s?(-?[\d.,]+)\s*$/);
  return m ? parseFloat(m[1].replace(/,/g, '')) : null;
};

// ═══════════════════════════════════════════════════════════════

// Fetches a checkout link on its own browser and its own proxy.
//
// Used when a different provider is named for this step. Making the link and
// paying are refused for different reasons — the link request is the one that
// draws "unusual activity" — and only the payment needs Stripe, which one of
// the providers blocks outright. So the link can go through a provider that
// would be useless for the payment, on a separate network entirely.
async function fetchLinkVia({ token, proxyStr, proxyCountry, plan }) {
  // Keep the file's access token and account id before anything else. The
  // check that happens here needs them, and they were only being stored in
  // the phase that follows.
  try {
    const sf = JSON.parse(fs.readFileSync('./session_api.json', 'utf8'));
    if (sf.accessToken && !global.__accessToken) global.__accessToken = String(sf.accessToken).trim();
    if (sf.account?.id && !global.__acctId) global.__acctId = sf.account.id;
  } catch (e) {}

  console.log('\n' + '─'.repeat(74));
  console.log('  1. Getting a checkout link');
  console.log('─'.repeat(74));

  const cdpPort = 9500 + Math.floor(Math.random() * 300);
  const localPort = 12800 + Math.floor(Math.random() * 400);
  let proxy = null, chrome = null, ws = null;

  const shut = () => {
    try { ws?.close(); } catch {}
    try { chrome?.kill(); } catch {}
    try { proxy?.close(); } catch {}
  };

  try {
    if (proxyStr) {
      // The link step never touches Stripe, so nothing needs to leave the
      // proxy except the address lookup, which this provider refuses.
      proxy = await startProxyRelay(proxyStr, localPort, proxyCountry, ['ip-api.com']);
      const exit = await fetchExitIP(localPort);
      console.log(`  link exit: ${describeIP(exit)}`);
      if (!exit?.query) { shut(); return { ok: false, why: 'the link proxy did not answer' }; }
    }

    // Its own profile, so the two browsers share nothing
    const profile = path.join(process.cwd(), '.chrome_link_profile');
    try { if (fs.existsSync(profile)) fs.rmSync(profile, { recursive: true, force: true }); } catch {}
    fs.mkdirSync(profile, { recursive: true });

    const chromePath = findChrome();
    if (!chromePath) { shut(); return { ok: false, why: 'Chrome not found' }; }

    chrome = spawn(chromePath, [
      `--remote-debugging-port=${cdpPort}`, `--user-data-dir=${profile}`,
      '--no-first-run', '--no-default-browser-check',
      '--disable-blink-features=AutomationControlled',
      '--disable-session-crashed-bubble', '--hide-crash-restore-bubble',
      '--window-size=1280,950',
      ...(process.getuid && process.getuid() === 0
          ? ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] : []),
      ...(proxyStr ? [`--proxy-server=http://127.0.0.1:${localPort}`,
                      '--proxy-bypass-list=<-loopback>'] : []),
      'about:blank'
    ], { stdio: 'ignore' });
    await sleep(3000);

    ws = await connectCDP(cdpPort);
    if (!ws) { shut(); return { ok: false, why: 'could not attach to the link browser' }; }
    const cdp = cdpClient(ws);
    await cdp.send('Network.enable');
    await cdp.send('Page.enable');
    await cdp.send('Runtime.enable');

    const capture = setupCapture(cdp);
  // The site's own statement of what is charged and whether it went through
  const payApi = watchPaymentApi(cdp);

    const COOKIE = '__Secure-next-auth.session-token';
    let injecting = false;
    if (token.length + COOKIE.length + 1 <= 4096) {
      try {
        await cdp.send('Network.setCookie', {
          name: COOKIE, value: token, domain: '.chatgpt.com',
          path: '/', secure: true, httpOnly: true, sameSite: 'Lax' });
      } catch { injecting = true; }
    } else injecting = true;

    await cdp.send('Fetch.enable', { patterns: [{ urlPattern: '*', requestStage: 'Request' }] });
    cdp.on('Fetch.requestPaused', async p2 => {
      const id = p2.requestId;
      try {
        if (!/^https:\/\/([a-z0-9-]+\.)?chatgpt\.com\//i.test(p2.request?.url || '')) {
          await cdp.send('Fetch.continueRequest', { requestId: id }); return;
        }
        let postData = p2.request.postData;
        if (p2.request.method === 'POST' &&
            /\/backend-api\/payments\/checkout(\?|$)/.test(p2.request.url || '') && postData) {
          try {
            const parsed = JSON.parse(postData);
            if (parsed.promo_campaign) { delete parsed.promo_campaign; postData = JSON.stringify(parsed); }
          } catch {}
        }
        const hs = []; let had = false;
        for (const [k, v] of Object.entries(p2.request.headers || {})) {
          if (k.toLowerCase() === 'cookie' && injecting) {
            had = true;
            const rest = String(v).split(/;\s*/).filter(c => c && !c.startsWith(COOKIE + '='));
            rest.push(COOKIE + '=' + token);
            hs.push({ name: 'Cookie', value: rest.join('; ') });
          } else hs.push({ name: k, value: String(v) });
        }
        if (!had && injecting) hs.push({ name: 'Cookie', value: COOKIE + '=' + token });
        const opts = { requestId: id, headers: hs };
        if (postData !== p2.request.postData) opts.postData = Buffer.from(postData).toString('base64');
        await cdp.send('Fetch.continueRequest', opts);
      } catch { try { await cdp.send('Fetch.continueRequest', { requestId: id }); } catch {} }
    });

    console.log('  signing in…');
    const tSign = Date.now();
    const secs = () => ((Date.now() - tSign) / 1000).toFixed(0).padStart(3);
    await cdp.send('Page.navigate', { url: 'https://chatgpt.com/' });
    let email = null;
    for (let i = 0; i < 25; i++) {
      await sleep(1000);
      if (i && i % 3 === 0) {
        // Say where the page has got to, so a slow load is distinguishable
        // from a session that is never going to be accepted.
        try {
          const st = parseEval(await cdp.send('Runtime.evaluate', {
            expression: `(() => JSON.stringify({
              ready: document.readyState,
              url: (location.pathname + location.hash).slice(0, 40),
              body: ((document.body||{}).innerText||'').replace(/\s+/g,' ').trim().slice(0, 50)
            }))()`, returnByValue: true, timeout: 5000 }));
          console.log(`    [${secs()}s] ${st.ready || '?'} at ${st.url || '?'}` +
                      (st.body ? `  "${st.body}"` : '  (blank)'));
        } catch (e) {
          console.log(`    [${secs()}s] page not answering (${String(e.message).slice(0, 40)})`);
        }
      }
      const d = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(async()=>{try{const d=await (await fetch('/api/auth/session',{credentials:'include'})).json();
          return JSON.stringify({ok:!!(d&&d.user),email:d?.user?.email});}catch(e){return JSON.stringify({ok:false});}})()`,
        awaitPromise: true, returnByValue: true, timeout: 10000 }));
      if (d.ok) { email = d.email; break; }
    }
    console.log(email ? `  signed in as ${email}` : '  NOT signed in');
    if (!email) { shut(); return { ok: false, why: 'the session was not accepted on the link proxy' }; }

    // What the account already has, before anything is spent on it.
    try {
      let acctId = global.__acctId;
      if (!acctId) {
        // The file this run started from carries the id; asking the page for
        // it first was slower and could come back empty.
        try {
          const sf = JSON.parse(fs.readFileSync('./session_api.json', 'utf8'));
          if (sf.account?.id) { acctId = sf.account.id; global.__acctId = acctId; }
        } catch (e) {}
      }
      if (!acctId) {
        const w = parseEval(await cdp.send('Runtime.evaluate', {
          expression: `(async()=>{try{const d=await (await fetch('/api/auth/session',{credentials:'include'})).json();
            return JSON.stringify({accountId:d?.account?.id});}catch(e){return JSON.stringify({})}})()`,
          awaitPromise: true, returnByValue: true, timeout: 12000 }));
        acctId = w.accountId;
        if (acctId) global.__acctId = acctId;
      }
      if (!acctId) {
        console.log('  no account id available — cannot check the plan before paying');
      }
      if (acctId) {
        await sleep(6000);
        let sub = { status: 0 };
        for (let attempt = 1; attempt <= 4; attempt++) {
          try {
            sub = await apiCall(cdp, 'GET', `/backend-api/subscriptions?account_id=${acctId}`);
            if (sub.status) break;
          } catch (e) { sub = { status: 0, text: e.message }; }
          await sleep(3000);
        }
        const authDiag = parseEval(await cdp.send('Runtime.evaluate', {
          expression: `(async()=>{try{
            let plain=null, fresh=null;
            try{const a=await (await fetch('/api/auth/session',{credentials:'include'})).json();
              plain=a&&a.accessToken?a.accessToken.length:0;}catch(e){plain=-1}
            try{const b=await (await fetch('/api/auth/session?refresh=true',{credentials:'include'})).json();
              fresh=b&&b.accessToken?b.accessToken.length:0;}catch(e){fresh=-1}
            return JSON.stringify({plain,fresh,url:location.pathname+location.hash});
          }catch(e){return JSON.stringify({err:String(e).slice(0,60)})}})()`,
          awaitPromise: true, returnByValue: true, timeout: 15000 }));
        console.log(`  token sources: page=${authDiag.plain} refresh=${authDiag.fresh}` +
                    ` file=${(global.__accessToken || '').length}  at ${authDiag.url || '?'}`);
        console.log(`  plan check: ${sub.status}` +
                    (sub.json?.plan_type ? `  plan=${sub.json.plan_type}` +
                      (sub.json.active_until ? ` until ${sub.json.active_until}` : ' (no end date)')
                     : '  no plan_type'));
        if (sub.status === 200 && sub.json?.plan_type) {
          const j = sub.json;
          // A plan with no active period is the residue of an old app-store
          // subscription, not a live one.
          const live = !!j.active_until && new Date(j.active_until).getTime() > Date.now();
          if (live) {
            const off = j.will_renew === false;
            console.log('');
            console.log(`  This account is already on ${j.plan_type}` +
                        `${j.billing_period ? ' · ' + j.billing_period : ''}` +
                        `${j.billing_currency ? ' · ' + j.billing_currency : ''}`);
            console.log(`  Runs until  : ${whenDate(j.active_until)}`);
            console.log(`  Auto-renewal: ${off ? 'off' : 'ON — it will charge again on that date'}`);
            shut();
            return { ok: false, alreadySubscribed: true, plan: j.plan_type,
                     until: j.active_until, willRenew: j.will_renew };
          }
          console.log(`  the "${j.plan_type}" record has no active period — treating this account as free`);
          global.__planChecked = 'stale plan record, account is free';
        } else if (sub.status === 404) {
          console.log('  no subscription on this account — good to pay');
          global.__planChecked = 'no subscription';
        } else {
          // 200-with-a-plan and 404 are answers. Anything else is a failure to
          // read, and paying on that is how an account already holding a plan
          // got charged again.
          console.log('');
          console.log(`  Could not read the plan (${sub.status}).` +
                      (sub.status === 401 || sub.status === 403
                        ? ' The session is signed in but the API refuses it —'
                        : ''));
          if (sub.status === 401 || sub.status === 403) {
            console.log('  refresh session_api.json from https://chatgpt.com/api/auth/session');
          }
          console.log('  Refusing to pay without knowing whether this account is subscribed.');
          console.log('');
          shut();
          return { ok: false, why: `plan unreadable (${sub.status})` };
        }
      }
    } catch (e) {
      // Not knowing whether the account is subscribed is a reason to stop,
      // not to carry on. Paying on an unknown is how one account was charged
      // three times.
      console.log(`  could not check the plan: ${e.message}`);
      shut();
      return { ok: false, why: 'could not read the plan before paying — refusing to pay on a guess' };
    }

    await cdp.send('Page.navigate', { url: 'https://chatgpt.com/#pricing' });
    await sleep(2500);
    const vis = await ensurePlanVisible(cdp, plan);
    if (vis?.switched) console.log(`  switched tab (${vis.switched})`);

    const lookAtPage = async () => parseEval(await cdp.send('Runtime.evaluate', {
      expression: `(() => {
        const vis = el => el && el.offsetParent !== null;
        const testids = [...document.querySelectorAll('[data-testid*="select-plan-button"]')]
          .filter(vis).map(b => b.getAttribute('data-testid'));
        const amounts = ((document.body||{}).innerText||'')
          .match(/(?:[A-Z]{3}|[$€£¥₱₹])\s?[\d][\d.,]*/g) || [];
        return JSON.stringify({
          testids,
          amounts: [...new Set(amounts)].slice(0, 6),
          heading: [...document.querySelectorAll('h1,h2')].filter(vis)
            .map(h => (h.innerText||'').trim()).filter(Boolean).slice(0, 3),
          url: location.hash || location.pathname
        });
      })()`, returnByValue: true, timeout: 10000 }));

    let priced = await waitForPrices(cdp, plan);
    if (!priced.ok) {
      // Describe the page first. Reloading three times before looking meant
      // thirty seconds spent on a page that may already have had the button.
      const st = await lookAtPage();
      const wanted = new RegExp('select-plan-button-' + plan, 'i');
      const hasButton = (st.testids || []).some(t => wanted.test(t));
      console.log(`  price detector saw nothing at ${st.url || '?'}`);
      console.log(`    headings : ${(st.heading || []).join(' | ') || '(none)'}`);
      console.log(`    amounts  : ${(st.amounts || []).join(' ') || '(none)'}`);
      console.log(`    plan ids : ${(st.testids || []).join(' ') || '(none)'}`);

      if (hasButton) {
        console.log('  the plan button is there — going on without the price check');
      } else {
        for (let tryNo = 1; tryNo <= 2; tryNo++) {
          console.log(`  no plan button — reloading (${tryNo}/2)`);
          await cdp.send('Page.reload', { ignoreCache: true });
          await sleep(6000);
          await ensurePlanVisible(cdp, plan);
          const again = await lookAtPage();
          if ((again.testids || []).some(t => wanted.test(t))) {
            console.log('  the plan button appeared');
            priced = { ok: true, amounts: again.amounts || [] };
            break;
          }
        }
      }
    }
else {
      console.log(`  prices are showing (${priced.amounts.slice(0, 3).join(', ')})`);
    }

    capture.reset();
    let click = await clickPlanCTA(cdp, plan);
    if (!click.clicked) {
      shut();
      return { ok: false, why: 'no plan button on the pricing page',
               detail: (click.buttons || []).slice(0, 8).join(' · ') };
    }
    console.log(`  clicked "${click.text}"`);

    let got = await awaitCheckout(cdp, capture.box);
    for (let attempt = 2; !got.ok && attempt <= 4 &&
                          /unusual activity/i.test(got.error || ''); attempt++) {
      const pause = 12 * (attempt - 1);
      console.log(`  the payments page errored — reloading and trying again (${attempt}/4)`);
      await sleep(pause * 1000);
      // Reload rather than navigate: the page is already on #pricing, so a
      // navigation changes only the hash and the broken state survives it.
      await cdp.send('Page.reload', { ignoreCache: true });
      await sleep(6000);
      await ensurePlanVisible(cdp, plan);
      await waitForPrices(cdp, plan);
      capture.reset();
      click = await clickPlanCTA(cdp, plan);
      if (!click.clicked) break;
      got = await awaitCheckout(cdp, capture.box);
    }
    if (!got.ok) { shut(); return { ok: false, why: got.error }; }

    const cur = got.data?.billing_details?.currency || got.data?.currency || '';
    const ctry = got.data?.billing_details?.country || proxyCountry || '';
    console.log(`  ${ctry || '?'}/${cur || '?'} · ${plan}`);
    console.log(`  ${got.url}`);

    try {
      fs.mkdirSync('./results', { recursive: true });
      fs.writeFileSync(path.join('./results', `${Date.now()}_${plan}_${ctry || 'xx'}.json`),
        JSON.stringify({ url: got.url, account: email, country: ctry, currency: cur, plan }, null, 2));
    } catch {}

    shut();
    return { ok: true, url: got.url, account: email, country: ctry, currency: cur };
  } catch (e) {
    shut();
    return { ok: false, why: e.message };
  }
}

// Waits for the pricing cards to hold real numbers.
//
// The button is clickable while the prices beside it are still grey
// placeholders — the plan data has not arrived, and pressing it then is what
// produces "The payments page encountered an error". Waiting for a currency
// amount to appear is waiting for the thing the button actually needs.
async function waitForPrices(cdp, plan, timeoutMs = 45000) {
  const started = Date.now();
  let lastSeen = '';
  while (Date.now() - started < timeoutMs) {
    try {
      const d = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(() => {
          const plan = ${JSON.stringify(String(plan || 'plus').toLowerCase())};

          // The pricing view is a panel over the running app, so the page
          // still holds the sidebar — searching the whole document for a
          // block naming the plan found "Chat history" and reported no
          // prices while the buttons were sitting right there. Start from
          // the plan's own button and read the card it belongs to.
          const btn = document.querySelector(
            '[data-testid*="select-plan-button-' + plan + '" i]');
          if (!btn || btn.offsetParent === null)
            return JSON.stringify({ ready: false, why: 'no plan button yet' });

          const MONEY = /(?:[A-Z]{3}|[$€£¥₱₹])\\s?[\\d][\\d.,]*/g;
          let node = btn.parentElement, card = null;
          for (let up = 0; up < 8 && node; up++, node = node.parentElement) {
            const t = node.innerText || '';
            if (MONEY.test(t)) { card = node; break; }
          }
          if (!card) return JSON.stringify({ ready: false, why: 'no price near the button' });

          const amounts = (card.innerText || '').match(MONEY) || [];
          const loading = card.querySelectorAll(
            '[class*="animate-pulse"],[class*="skeleton"],[class*="Skeleton"]').length;

          return JSON.stringify({
            ready: amounts.length > 0 && loading === 0,
            amounts: [...new Set(amounts)].slice(0, 4),
            loading
          });
        })()`, returnByValue: true, timeout: 6000 }));

      const seen = (d.amounts || []).join(',');
      if (d.ready && seen === lastSeen && seen) return { ok: true, amounts: d.amounts };
      lastSeen = seen;
    } catch {}
    await sleep(800);
  }
  return { ok: false, amounts: lastSeen ? lastSeen.split(',') : [] };
}

// Hosts that go direct when a proxy is in use. Stripe is here because the
// residential providers block it outright; the checkout page is unusable
// without it. Nothing about the account or the price passes through these —
// the card processor sees this machine's address, everything else stays on
// the proxy.
const DEFAULT_BYPASS = ['js.stripe.com', 'm.stripe.com', 'm.stripe.network',
                        'api.stripe.com', 'b.stripecdn.com', 'merchant-ui-api.stripe.com',
                        'ip-api.com'];

async function run({ payUrl, card, billing, dryRun, allowTax, proxyStr, proxyCountry, keepRenewal, plan, bypass, noBypass, clearCards, cancelExisting, keepCards }) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const dir = path.join('./payments', `${stamp}_auto`);
  fs.mkdirSync(path.join(dir, 'shots'), { recursive: true });

  const cdpPort = 9950 + Math.floor(Math.random() * 40);
  const localPort = 13400 + Math.floor(Math.random() * 90);

  let proxy = null;
  // --no-bypass is about Stripe. The address lookup is not part of the
  // checkout, and this provider answers 405 to CONNECT for it, so routing it
  // through the proxy only reports "could not be determined" and stops a run
  // that was otherwise fine.
  const bypassHosts = noBypass ? ['ip-api.com']
                               : [...new Set([...DEFAULT_BYPASS, ...(bypass || [])])];
  if (proxyStr) {
    try {
      proxy = await startProxyRelay(proxyStr, localPort, proxyCountry, bypassHosts);
      if (bypassHosts.length) {
        console.log(`  going direct for: ${bypassHosts.slice(0, 4).join(', ')}` +
                    (bypassHosts.length > 4 ? ` and ${bypassHosts.length - 4} more` : ''));
      }
      const exit = await fetchExitIP(localPort);
      console.log(`  proxy exit: ${describeIP(exit)}`);

      // Carrying on past this point with a proxy that cannot say where it
      // comes out wastes a minute and then fails at the pricing page, which
      // is a confusing place to read the failure. The earlier run showed
      // exactly that: "could not be determined", then "NOT signed in", then
      // forty seconds of loading a page that was never going to work.
      if (!exit?.query && !bypassHosts.includes('ip-api.com')) {
        console.error(`\n  The proxy did not answer a request through it.`);
        console.error(`  Check the credentials, or try without --country.\n`);
        try { proxy.close(); } catch {}
        process.exit(1);
      }
      if (proxyCountry && exit?.countryCode &&
          exit.countryCode.toUpperCase() !== proxyCountry.toUpperCase()) {
        console.log(`  asked for ${proxyCountry} but landed in ${exit.countryCode}`);
      }
    } catch (e) {
      console.error(`\n  the proxy could not be started: ${e.message}\n`);
      process.exit(1);
    }
  }

  const profile = path.join(process.cwd(), '.chrome_pay_profile');
  try { if (fs.existsSync(profile)) fs.rmSync(profile, { recursive: true, force: true }); } catch {}
  fs.mkdirSync(profile, { recursive: true });

  const chromePath = findChrome();
  if (!chromePath) { console.error('\n  Chrome not found\n'); process.exit(1); }

  const chrome = spawn(chromePath, [
    `--remote-debugging-port=${cdpPort}`, `--user-data-dir=${profile}`,
    '--no-first-run', '--no-default-browser-check',
    '--disable-blink-features=AutomationControlled',
    '--disable-session-crashed-bubble', '--hide-crash-restore-bubble',
    '--window-size=1280,950',
    ...(process.getuid && process.getuid() === 0
        ? ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] : []),
    ...(proxyStr ? [`--proxy-server=http://127.0.0.1:${localPort}`,
                    '--proxy-bypass-list=<-loopback>'] : []),
    'about:blank'
  ], { stdio: 'ignore' });
  await sleep(3000);

  const ws = await connectCDP(cdpPort);
  if (!ws) { console.error('\n  could not attach to Chrome\n'); process.exit(1); }
  const cdp = cdpClient(ws);

  await cdp.send('Network.enable');
  await cdp.send('Page.enable');
  await cdp.send('Runtime.enable');

  // Attach to every frame. Stripe's inputs live in out-of-process iframes,
  // which are invisible to Runtime.evaluate on the page target — without this
  // there is no `sessions` map, and nothing can find or type into a card field.
  let sessions = await attachToFrames(cdp);

  // Watches for the checkout POST and keeps its response body. Registered now
  // because the request fires the moment the plan button is pressed.
  const capture = setupCapture(cdp);
  // The site's own statement of what is charged and whether it went
  // through. Created in this browser too — it was only made beside the
  // link capture, so the payment loop found nothing defined.
  const payApi = watchPaymentApi(cdp);

  const t0 = Date.now();
  const ms = () => Date.now() - t0;
  const log = [];
  const note = (kind, text) => {
    log.push({ ms: ms(), kind, text });
    console.log(`  [${String(Math.round(ms()/1000)).padStart(3)}s] ${kind.padEnd(9)} ${String(text).slice(0,70)}`);
  };
  let shotN = 0;
  const shot = async tag => {
    try {
      const img = await cdp.send('Page.captureScreenshot', { format: 'png' });
      const name = `${String(++shotN).padStart(2,'0')}_${String(tag).replace(/[^a-z0-9]/gi,'_').slice(0,30)}.png`;
      fs.writeFileSync(path.join(dir, 'shots', name), Buffer.from(img.data, 'base64'));
      if (VERBOSE_SHOTS) console.log(`         · saved shots/${name}`);
    } catch {}
  };

  // On a server there is no window to look at, so a run that stalls leaves no
  // evidence at all unless pictures are taken as it goes.
  let heartbeat = null;
  if (WATCH) {
    let n = 0;
    heartbeat = setInterval(() => { shot(`watch_${String(++n).padStart(3,'0')}`).catch(() => {}); }, WATCH * 1000);
  }

  // ── Session ──
  const COOKIE = '__Secure-next-auth.session-token';
  let token = '';
  try {
    const sf = JSON.parse(fs.readFileSync('./session_api.json','utf8'));
    token = String(sf.sessionToken || '').trim();
    if (sf.account?.id) global.__acctId = sf.account.id;
    if (sf.accessToken) global.__accessToken = String(sf.accessToken).trim();
  }
  catch { console.error('\n  session_api.json not found\n'); try { chrome.kill(); } catch {} process.exit(1); }

  // Prefer a real cookie in Chrome's own jar. A stored cookie is sent on every
  // request the browser makes, including a full navigation to the checkout
  // route, whereas header injection only covers requests that match the
  // interception patterns — which is why the checkout page bounced to login
  // even though the session had been accepted moments earlier.
  //
  // Chrome refuses cookies whose name and value exceed 4096 bytes, which is
  // what Google sign-in produces, so injection stays as the fallback.
  const cookieSize = token.length + COOKIE.length + 1;
  let usingHeaderInjection = false;

  // The interceptor runs for every account, not only the ones whose cookie is
  // too big to store. It has a second job — taking promo_campaign out of the
  // checkout request — and that has to happen regardless of how the session
  // was installed.
  let interceptorOn = false;
  const enableInjection = async (injectCookie) => {
    if (injectCookie) usingHeaderInjection = true;
    if (interceptorOn) return;
    interceptorOn = true;
    await cdp.send('Fetch.enable', {
      patterns: [{ urlPattern: '*', requestStage: 'Request' }]
    });
    cdp.on('Fetch.requestPaused', async p => {
      const id = p.requestId;
      try {
        if (!/^https:\/\/([a-z0-9-]+\.)?chatgpt\.com\//i.test(p.request?.url || '')) {
          await cdp.send('Fetch.continueRequest', { requestId: id }); return;
        }
        // Take promo_campaign out of the checkout request. Left in, the
        // server quotes whatever offer the account happens to carry rather
        // than the standing price, and the amount charged is not the one
        // this run was set up to check.
        let postData = p.request.postData;
        if (p.request.method === 'POST' &&
            /\/backend-api\/payments\/checkout(\?|$)/.test(p.request.url || '') && postData) {
          try {
            const parsed = JSON.parse(postData);
            if (parsed.promo_campaign) {
              delete parsed.promo_campaign;
              postData = JSON.stringify(parsed);
            }
          } catch {}
        }

        const hs = []; let had = false;
        for (const [k, v] of Object.entries(p.request.headers || {})) {
          if (k.toLowerCase() === 'cookie' && usingHeaderInjection) {
            had = true;
            const rest = String(v).split(/;\s*/).filter(c => c && !c.startsWith(COOKIE + '='));
            rest.push(COOKIE + '=' + token);
            hs.push({ name: 'Cookie', value: rest.join('; ') });
          } else hs.push({ name: k, value: String(v) });
        }
        if (!had && usingHeaderInjection) hs.push({ name: 'Cookie', value: COOKIE + '=' + token });

        const opts = { requestId: id, headers: hs };
        if (postData !== p.request.postData) {
          opts.postData = Buffer.from(postData).toString('base64');
        }
        await cdp.send('Fetch.continueRequest', opts);
      } catch { try { await cdp.send('Fetch.continueRequest', { requestId: id }); } catch {} }
    });
  };

  let cookieStored = false;
  if (cookieSize <= 4096) {
    try {
      await cdp.send('Network.setCookie', {
        name: COOKIE, value: token, domain: '.chatgpt.com',
        path: '/', secure: true, httpOnly: true, sameSite: 'Lax'
      });
      cookieStored = true;
    } catch {}
  }
  // Cookie injection only when the jar refused it; the interceptor either way
  await enableInjection(!cookieStored);

  console.log('  signing in…');
  const tSign2 = Date.now();
  const secs2 = () => ((Date.now() - tSign2) / 1000).toFixed(0).padStart(3);
  await cdp.send('Page.navigate', { url: 'https://chatgpt.com/' });
  let email = null;
  for (let i = 0; i < 25; i++) {
    await sleep(1000);
    if (i && i % 3 === 0) {
      try {
        const st = parseEval(await cdp.send('Runtime.evaluate', {
          expression: `(() => JSON.stringify({
            ready: document.readyState,
            url: (location.pathname + location.hash).slice(0, 40),
            body: ((document.body||{}).innerText||'').replace(/\s+/g,' ').trim().slice(0, 50)
          }))()`, returnByValue: true, timeout: 5000 }));
        console.log(`    [${secs2()}s] ${st.ready || '?'} at ${st.url || '?'}` +
                    (st.body ? `  "${st.body}"` : '  (blank)'));
      } catch (e) {
        console.log(`    [${secs2()}s] page busy`);
      }
    }
    try {
      const d = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(async()=>{try{const d=await (await fetch('/api/auth/session',{credentials:'include'})).json();
          return JSON.stringify({ok:!!(d&&d.user),email:d?.user?.email});}catch(e){return JSON.stringify({ok:false});}})()`,
        awaitPromise: true, returnByValue: true, timeout: 10000 }));
      if (d.ok) { email = d.email; break; }
    } catch {}
  }
  console.log(email ? `signed in as ${email}` : 'NOT signed in');

  // A live-looking session can still be dead for real work: /api/auth/session
  // only reads a cookie. A 401 from an actual backend call is what expiry
  // looks like, and it is why a pricing page can bounce to login moments
  // after sign-in appeared to succeed.
  if (email) {
    let probeOk = false;
    // Wait for the app to stop navigating. A call made mid-load dies with
    // "Inspected target navigated or closed", which says nothing about the
    // session and ends the run on a fault that was never checked.
    await sleep(5000);
    let probe = { status: 0 };
    for (let i = 0; i < 4; i++) {
      try {
        probe = await apiCall(cdp, 'GET', '/backend-api/me');
        if (probe.status) break;
      } catch (e) { probe = { status: 0, text: e.message }; }
      await sleep(2500);
    }
    if (probe.status === 401 || probe.status === 403) {
      // Before calling it dead, look at what the page can actually produce.
      // A page holding a stale token and a page unable to mint one are
      // different problems, and only one of them is unrecoverable.
      const t = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(async()=>{try{
          const a=await (await fetch('/api/auth/session',{credentials:'include'})).json();
          const b=await (await fetch('/api/auth/session?refresh=true',{credentials:'include'})).json();
          const same = a&&b&&a.accessToken&&b.accessToken ? a.accessToken===b.accessToken : null;
          return JSON.stringify({
            plainLen: a&&a.accessToken?a.accessToken.length:0,
            refreshLen: b&&b.accessToken?b.accessToken.length:0,
            same,
            fileMatchesPage: null,
            expires: (b&&b.expires)||(a&&a.expires)||null,
            user: (a&&a.user&&a.user.email)||null
          });
        }catch(e){return JSON.stringify({err:String(e).slice(0,100)})}})()`,
        awaitPromise: true, returnByValue: true, timeout: 15000 }));

      const fileTok = global.__accessToken || '';
      console.error('');
      console.error(`  page token (plain)   : ${t.plainLen || 0} chars`);
      console.error(`  page token (refresh) : ${t.refreshLen || 0} chars` +
                    (t.same === true ? '  — identical, the page is not minting a new one' :
                     t.same === false ? '  — different, the page did refresh it' : ''));
      console.error(`  token in the file    : ${fileTok.length} chars`);
      console.error(`  session expires      : ${t.expires || 'not stated'}`);

      // Try the call with each candidate, so the failure is attributed
      const tryWith = async (label, bearer) => {
        if (!bearer) { console.error(`  ${label.padEnd(20)}: none to try`); return null; }
        const r = parseEval(await cdp.send('Runtime.evaluate', {
          expression: `(async()=>{try{
            const r=await fetch('/backend-api/me',{credentials:'include',
              headers:{'Authorization':'Bearer '+${JSON.stringify(bearer)},'OAI-Language':'en-US'}});
            const t=await r.text();
            return JSON.stringify({status:r.status,text:t.slice(0,90)});
          }catch(e){return JSON.stringify({status:0,text:String(e).slice(0,80)})}})()`,
          awaitPromise: true, returnByValue: true, timeout: 15000 }));
        console.error(`  ${label.padEnd(20)}: /me → ${r.status}  ${String(r.text||'').replace(/\s+/g,' ').slice(0,70)}`);
        return r.status;
      };
      await tryWith('with file token', fileTok);
      await tryWith('with page token', null);
      console.error('');

      // ?refresh=true came back with nothing while the plain call returned the
      // same expired token, so the page is serving what it has rather than
      // asking for a new one. A full load is the only thing that makes the
      // app go and get one — try it before declaring the session dead.
      console.error('  trying a full reload to see if the app mints a new token…');
      try {
        await cdp.send('Page.navigate', { url: 'https://chatgpt.com/' });
        await sleep(9000);
        for (let i = 0; i < 12; i++) {
          const after = parseEval(await cdp.send('Runtime.evaluate', {
            expression: `(async()=>{try{
              const a=await (await fetch('/api/auth/session',{credentials:'include'})).json();
              if(!a||!a.accessToken) return JSON.stringify({len:0});
              const r=await fetch('/backend-api/me',{credentials:'include',
                headers:{'Authorization':'Bearer '+a.accessToken,'OAI-Language':'en-US'}});
              return JSON.stringify({len:a.accessToken.length, me:r.status, tok:a.accessToken});
            }catch(e){return JSON.stringify({len:0,err:String(e).slice(0,60)})}})()`,
            awaitPromise: true, returnByValue: true, timeout: 15000 }));
          if (after.me === 200) {
            console.error(`  a fresh token works — carrying on\n`);
            global.__accessToken = after.tok || '';
            probeOk = true;
            break;
          }
          await sleep(2500);
        }
      } catch {}

      if (probeOk) {
        // recovered; skip the exit below
      } else {
      console.error(`  The session reads as signed in but the API refuses it — it has expired.`);
      console.error(`  Refresh session_api.json from https://chatgpt.com/api/auth/session\n`);
      try { chrome.kill(); } catch {}
      try { proxy?.close(); } catch {}
      process.exit(1);
      }
    }
    if (probe.json?.country) console.log(`  account region: ${probe.json.country}`);
    // Keep the id from here. Reading it again after payment fails: the
    // success page is a transition and does not carry the account.
    try {
      const w = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(async()=>{try{const d=await (await fetch('/api/auth/session',{credentials:'include'})).json();
          return JSON.stringify({accountId:d?.account?.id});}catch(e){return JSON.stringify({})}})()`,
        awaitPromise: true, returnByValue: true, timeout: 12000 }));
      if (w.accountId) global.__acctId = w.accountId;
    } catch {}

    // What the account already has decides whether there is anything to buy.
    try {
    let acctId = global.__acctId;
    if (!acctId) {
      const who = parseEval(await cdp.send('Runtime.evaluate', {
        expression: `(async()=>{try{const d=await (await fetch('/api/auth/session',{credentials:'include'})).json();
          return JSON.stringify({accountId:d?.account?.id});}catch(e){return JSON.stringify({})}})()`,
        awaitPromise: true, returnByValue: true, timeout: 12000 }));
      acctId = who.accountId;
      if (acctId) global.__acctId = acctId;
    }

    if (acctId) {
      // Let the app settle. A call made while the page is still navigating
      // dies with "Inspected target navigated or closed", and reading that as
      // "no plan" would mean paying again on an account that already has one.
      await sleep(4000);
      // The link phase already read the plan and judged this account payable.
      // Asking again in the payment browser repeats a round trip and can
      // answer differently if the token has moved on since.
      const alreadyKnown = global.__planChecked;
      if (alreadyKnown) console.log(`  plan already checked in the link phase (${alreadyKnown})`);
      let rawSub = { status: 0 };
      for (let attempt = 1; attempt <= 4; attempt++) {
        try {
          rawSub = await apiCall(cdp, 'GET', `/backend-api/subscriptions?account_id=${acctId}`);
          if (rawSub.status === 200) break;
        } catch (e) {
          rawSub = { status: 0, text: e.message };
        }
        await sleep(2500);
      }
      console.log(`  subscriptions call: ${rawSub.status}` +
                  (rawSub.status !== 200 ? `  ${String(rawSub.text || '').slice(0,120)}` : '') +
                  (rawSub.json?.plan_type ? `  plan=${rawSub.json.plan_type}` : '  (no plan_type in the response)'));
      // 404 is the answer for a free account, not a failure to read one —
      // "No subscription found for account" is exactly what we needed to
      // know. Refusing to pay on it blocked the accounts this tool is for.
      if (rawSub.status === 404) {
        console.log('  no subscription on this account — good to pay');
      } else if (rawSub.status !== 200) {
        console.error('\n  Could not read the plan, so whether this account is already');
        console.error('  subscribed is unknown. Refusing to pay on a guess.\n');
        try { chrome.kill(); } catch {}
        try { proxy?.close(); } catch {}
        process.exit(1);
      }
      // Build from the response already in hand. Asking again returned an
      // empty object, and an empty object reads as "no plan" — which is how
      // an account showing plan=plus one line earlier went on to be charged.
      const pmRes = await apiCall(cdp, 'GET',
        `/backend-api/payments/payment_methods?account_id=${acctId}`);
      const sj = rawSub.json || {};
      const b = {
        plan: rawSub.status === 200 ? (sj.plan_type || null) : null,
        period: sj.billing_period || null,
        currency: sj.billing_currency || null,
        activeUntil: sj.active_until || null,
        willRenew: sj.will_renew,
        cancelled: sj.cancellation_outcome || null,
        delinquent: !!sj.is_delinquent,
        cards: (pmRes.json?.payment_methods || []).map(c => ({
          id: c.id, brand: c.card?.brand || '?', last4: c.card?.last4 || '????',
          isDefault: c.id === pmRes.json?.default_payment_method_id
        }))
      };

      if (b.cards.length) {
        console.log(`  cards on file : ${b.cards.map(c =>
          `${c.brand} ****${c.last4}${c.isDefault ? '*' : ''}`).join(', ')}`);
      }
      if (b.delinquent) {
        console.log(`  ${'\u26a0'}  this account has an unpaid invoice — a new payment may go to that first`);
      }

      // Clearing saved cards before paying, when asked. A card left on file is
      // what the backup-payment setting falls through to, so a run meant for
      // one card can end up charging another.
      if (clearCards && b.cards.length) {
        console.log(`  clearing ${b.cards.length} saved card(s) before paying…`);
        await deleteCards(cdp, acctId, b.cards.map(c => c.id),
          m => console.log(`    ${m}`));
      }

      // A plan with no dates is not a live plan. This account came back
      // plan_type "plus" with active_start and active_until both null,
      // will_renew false, and is_processor_stripe false — the remains of a
      // subscription bought through an app store, long finished. Treating
      // that as active refused to renew an account that is free in practice.
      const looksLive = !!(sj.active_until) &&
                        new Date(sj.active_until).getTime() > Date.now();
      if (b.plan && !looksLive) {
        console.log(`  the "${b.plan}" record has no active period` +
                    (sj.is_processor_stripe === false ? ' and was not billed through Stripe' : '') +
                    ' — treating this account as free');
      }

      if (b.plan && looksLive) {
        // Print what the plan actually says. "already subscribed" with no end
        // date is not enough to act on — an expired plan and a live one look
        // the same from a single field.
        console.log('  raw plan: ' + JSON.stringify({
          plan_type: sj.plan_type, active_start: sj.active_start,
          active_until: sj.active_until, will_renew: sj.will_renew,
          cancellation_outcome: sj.cancellation_outcome,
          billing_currency: sj.billing_currency, is_delinquent: sj.is_delinquent,
          is_processor_stripe: sj.is_processor_stripe,
          grace_period_end_timestamp: sj.grace_period_end_timestamp
        }));
        const off = b.willRenew === false;
        console.log('');
        console.log(`  This account is already on ${b.plan}` +
                    `${b.period ? ' · ' + b.period : ''}${b.currency ? ' · ' + b.currency : ''}`);
        console.log(`  Runs until  : ${whenDate(b.activeUntil)}`);
        console.log(`  Auto-renewal: ${off ? 'off' + (b.cancelled ? ` (${b.cancelled})` : '')
                                            : 'ON — it will charge again on that date'}`);

        // Only when asked. Cancelling a plan this run did not pay for is the
        // person's call, so the tool reports and stops.
        if (!off && cancelExisting) {
          console.log('\n  --cancel-existing was given, so turning it off…');
          const r = await stopAutoRenewal(cdp);
          console.log(r.done
            ? `  auto-renewal is off — access runs to ${whenDate(r.endsAt)}`
            : `  could not turn it off: ${r.why}`);
        } else if (!off) {
          console.log('\n  Nothing was paid and nothing was changed.');
          console.log('  Run again with --cancel-existing to turn the renewal off.');
        }
        try { chrome.kill(); } catch {}
        try { proxy?.close(); } catch {}
        process.exit(0);
      }
    }
    } catch (e) {
      console.error(`\n  Could not check this account's plan: ${e.message}`);
      console.error('  Refusing to pay without knowing whether it is already subscribed.\n');
      try { chrome.kill(); } catch {}
      try { proxy?.close(); } catch {}
      process.exit(1);
    }
  }

  if (!email) {
    // Everything after this needs a session. Pressing on produced a pricing
    // page with no plan on it and a failure message that pointed at the
    // wrong thing.
    console.error(`\n  The session was not accepted, so there is nothing to do.`);
    console.error(`  Either the token has expired — refresh session_api.json —`);
    console.error(`  or the proxy could not reach chatgpt.com.\n`);
    try { chrome.kill(); } catch {}
    try { proxy?.close(); } catch {}
    process.exit(1);
  }
  if (email && !cookieStored) console.log(`  (cookie is ${cookieSize} bytes — using header injection)`);

  // ── Open the checkout ──
  if (!payUrl) {
    note('link', 'opening the pricing page');
    await cdp.send('Page.navigate', { url: 'https://chatgpt.com/#pricing' });
    await sleep(2500);

    const vis = await ensurePlanVisible(cdp, plan);
    if (vis?.switched) note('link', `switched tab (${vis.switched})`);

    const priced = await waitForPrices(cdp, plan);
    if (priced.ok) note('link', `prices are showing (${priced.amounts.slice(0, 3).join(', ')})`);
    else note('link', 'the prices never settled — clicking anyway');

    capture.reset();
    let click = await clickPlanCTA(cdp, plan);
    if (click._pageError) {
      note('stop', `the pricing page script failed: ${click._pageError}`);
      await shot('pricing_error');
      return finish({ account: email, outcome: {
        result: 'not submitted', reason: 'pricing page script failed' } });
    }
    if (!click.clicked) {
      note('stop', 'no plan button was found on the pricing page');
      if (click.buttons?.length) note('detail', click.buttons.slice(0, 8).join(' · '));
      await shot('no_button');
      return finish({ account: email, outcome: {
        result: 'not submitted', reason: 'no plan button on the pricing page' } });
    }
    note('link', `clicked "${click.text}"${click.via === 'data-testid' ? ' (by test id)' : ''}`);

    let got = await awaitCheckout(cdp, capture.box);

    // "Unusual activity" here is the account being told to slow down. The
    // message asks you to try again, so wait and do that, with longer gaps.
    for (let attempt = 2; !got.ok && attempt <= 4 &&
                          /unusual activity/i.test(got.error || ''); attempt++) {
      const pause = 12 * (attempt - 1);
      note('link', `the payments page errored — reloading and trying again (${attempt}/4)`);
      await sleep(pause * 1000);
      // A full reload, not a navigation. The page is already sitting on
      // #pricing, so navigating there again only changes the hash and leaves
      // the broken application state exactly as it was — pressing F5 by hand
      // cleared it where the script could not, and this is the difference.
      await cdp.send('Page.reload', { ignoreCache: true });
      await sleep(6000);
      await ensurePlanVisible(cdp, plan);
      await waitForPrices(cdp, plan);
      capture.reset();
      click = await clickPlanCTA(cdp, plan);
      if (!click.clicked) break;
      got = await awaitCheckout(cdp, capture.box);
    }

    if (!got.ok) {
      note('stop', `no checkout link: ${got.error}`);
      await shot('no_checkout');
      if (/unusual activity/i.test(got.error || '')) {
        note('note', 'this account has made several checkout attempts recently');
        note('note', 'leave it for a while, or run with a different session file');
      }
      return finish({ account: email, outcome: {
        result: 'not submitted', reason: `no checkout link: ${got.error}` } });
    }

    payUrl = got.url;
    const cur = got.data?.billing_details?.currency || got.data?.currency || '';
    const ctry = got.data?.billing_details?.country || proxyCountry || '';
    note('link', `${ctry || '?'}/${cur || '?'} · ${plan}`);
    note('link', payUrl.slice(0, 66));

    try {
      fs.mkdirSync('./results', { recursive: true });
      fs.writeFileSync(path.join('./results', `${Date.now()}_${plan}_${ctry || 'xx'}.json`),
        JSON.stringify({ url: payUrl, account: email, country: ctry, currency: cur, plan }, null, 2));
    } catch {}
    await shot('link_made');
  }

  note('open', payUrl.slice(0, 66));
  await cdp.send('Page.navigate', { url: payUrl });

  // A full navigation to the checkout route is the moment a session that only
  // exists in intercepted headers falls over. Notice it and switch approach
  // rather than sitting on the login page until the timeout.
  for (let attempt = 1; attempt <= 3; attempt++) {
    await sleep(4000);
    let here = '';
    try {
      const r = await cdp.send('Runtime.evaluate',
        { expression: 'location.href', returnByValue: true, timeout: 6000 });
      here = String(r?.result?.value || '');
    } catch {}
    if (!/\/auth\/login/.test(here)) break;

    note('bounced', 'the checkout sent us to the login page');
    await shot(`bounce_${attempt}`);
    if (!usingHeaderInjection) {
      note('retry', 'switching to header injection and reloading');
      await enableInjection(true);
    } else {
      note('retry', 'reloading the checkout');
    }
    await cdp.send('Page.navigate', { url: payUrl });
    if (attempt === 3) {
      note('stop', 'the session is not being accepted on the checkout page');
      return finish({ payUrl, account: email, outcome: {
        result: 'not submitted',
        reason: 'checkout kept redirecting to login — refresh session_api.json for this account' } });
    }
  }

  // Wait for the card field to actually exist before typing at it
  // An account with a card already on file opens on a "Saved" tab, and the
  // new-card fields are not rendered at all until "Card" is selected. Without
  // this the run waits for a field that is never going to appear — and the
  // saved card carries its own old billing address, which is where an
  // unexpected VAT line comes from.
  const chooseCardTab = async () => {
    const targets = [undefined, ...[...sessions.keys()]];
    for (const sessionId of targets) {
      try {
        const d = parseEval(await cdp.send('Runtime.evaluate', {
          expression: `(() => {
            const tab = document.querySelector('[data-testid="card"], #card-tab');
            if (!tab) return JSON.stringify({found:false});
            const already = tab.getAttribute('aria-selected') === 'true';
            if (!already) tab.click();
            return JSON.stringify({found:true, already,
              label: (tab.innerText || '').replace(/\s+/g,' ').trim().slice(0,20)});
          })()`, returnByValue: true, timeout: 5000 }, sessionId));
        if (d.found) return d;
      } catch {}
    }
    return null;
  };

  // Wait for the payment options to finish rendering before touching them.
  // The screenshots showed the tab strip still as grey placeholders while a
  // saved card and an old billing address sat underneath — clicking into that
  // either misses, or is undone when the strip finally settles.
  await waitUntil(async () => {
    const d = parseEval(await cdp.send('Runtime.evaluate', {
      expression: `(() => {
        const tabs = [...document.querySelectorAll('[role="tab"],[data-testid="card"]')]
          .filter(t => t.offsetParent !== null);
        // A placeholder has no label yet
        const labelled = tabs.filter(t => (t.innerText || '').trim().length > 0);
        return JSON.stringify({ settled: tabs.length > 0 && labelled.length === tabs.length });
      })()`, returnByValue: true, timeout: 6000 }));
    return d.settled ? true : null;
  }, { timeoutMs: 30000, everyMs: 700 });
  await sleep(1200);

  const tab = await waitUntil(chooseCardTab, { timeoutMs: 25000, everyMs: 800 });
  if (tab && !tab.already) {
    note('tab', 'a saved card was on file — switched to entering a new one');
    await sleep(1200);
  }

  const ready = await waitUntil(
    () => fieldExists(cdp, sessions, '#payment-numberInput'),
    { timeoutMs: 120000, everyMs: 700 });
  if (!ready) {
    // Say which frames were reachable, so the next fix is not guesswork
    const frames = [...sessions.values()].map(v => v.url.slice(0, 60)).filter(Boolean);
    note('error', 'the card form never appeared');
    note('frames', frames.length ? frames.join(' | ') : 'no iframes were attached at all');
    await shot('no_form');
    return finish({ payUrl, account: email, outcome: {
      result: 'not submitted', reason: 'card field not reachable',
      framesSeen: frames } });
  }
  note('form', `card form is ready (${sessions.size} frame${sessions.size === 1 ? '' : 's'} attached)`);
  await shot('form_ready');

  const before = parseEval(await cdp.send('Runtime.evaluate', {
    expression: TOTALS_PROBE, returnByValue: true, timeout: 8000 }));
  note('price', `${before.subscription} | ${before.tax || 'no tax line'} | ${before.dueToday}`);

  // ── Card ──
  const steps = [];
  const step = async (label, fn) => {
    const r = await fn();
    steps.push({ label, ...r });
    note(r.ok ? 'filled' : 'FAILED', `${label}${r.ok ? '' : ' — ' + r.why}`);
    await shot(`after_${label}`);
    await sleep(400 + Math.random() * 700);   // a beat between fields
    return r.ok;
  };

  let allOk = true;
  allOk &= await step('card number', () => fillInput(cdp, sessions, '#payment-numberInput', card.number));
  allOk &= await step('expiry',      () => fillInput(cdp, sessions, '#payment-expiryInput', card.expiry));
  allOk &= await step('cvc',         () => fillInput(cdp, sessions, '#payment-cvcInput', card.cvc));

  // ── Billing address ──
  // The address section only renders once card, expiry and CVC are all
  // accepted — measured at 0.3s after the last CVC digit. Waiting for it to
  // exist is right; guessing at a duration is not, since a slow link or a
  // proxy stretches everything here.
  note('wait', 'for the address section');

  // An account that has paid before shows its previous billing address as a
  // summary with an "Update" control, not as editable fields — and that old
  // address is what the tax is being calculated against. Open it if it is
  // sitting there closed.
  const openAddressEditor = async () => {
    const targets = [undefined, ...[...sessions.keys()]];
    for (const sessionId of targets) {
      try {
        const d = parseEval(await cdp.send('Runtime.evaluate', {
          expression: `(() => {
            if (document.querySelector('#billingAddress-nameInput')) return JSON.stringify({open:true});
            const btn = [...document.querySelectorAll('button,[role="button"],a')]
              .filter(b => b.offsetParent !== null)
              .find(b => /^(update|edit|change|enter a new address|use a different address)$/i
                          .test((b.innerText || '').replace(/\s+/g,' ').trim()));
            if (!btn) return JSON.stringify({open:false, clicked:false});
            btn.click();
            return JSON.stringify({open:false, clicked:true,
              label: (btn.innerText||'').trim().slice(0,20)});
          })()`, returnByValue: true, timeout: 5000 }, sessionId));
        if (d.open) return { open: true };
        if (d.clicked) return { open: false, clicked: true, label: d.label };
      } catch {}
    }
    return null;
  };

  let addrReady = await waitUntil(
    () => fieldExists(cdp, sessions, '#billingAddress-nameInput'),
    { timeoutMs: 25000, everyMs: 700 });

  if (!addrReady) {
    const opened = await openAddressEditor();
    if (opened?.clicked) {
      note('address', `a saved address was shown — opened it with "${opened.label}"`);
      await sleep(1500);
    }
    addrReady = await waitUntil(
      () => fieldExists(cdp, sessions, '#billingAddress-nameInput'),
      { timeoutMs: 60000, everyMs: 700 });
  }
  if (!addrReady) {
    note('stop', 'the address section never rendered — the card was not accepted');
    return finish({ payUrl, account: email, before, steps, outcome: {
      result: 'not submitted', reason: 'card details were not accepted' } });
  }
  note('ready', 'address section is up');

  allOk &= await step('billing name', () => fillInput(cdp, sessions, '#billingAddress-nameInput', billing.name));
  allOk &= await step('country',      () => selectOption(cdp, sessions, '#billingAddress-countryInput', billing.country));

  // Choosing a country tears down and rebuilds the rest of the address —
  // state and postal code were seen to vanish and return. Anything located
  // before this point is a dead handle, so wait for the new ones.
  note('wait', 'for the address to rebuild after the country change');
  await sleep(800);
  const rebuilt = await waitUntil(
    () => fieldExists(cdp, sessions, '#billingAddress-postalCodeInput'),
    { timeoutMs: 30000, everyMs: 500 });
  if (!rebuilt) note('warn', 'postal code did not come back after the country change');

  allOk &= await step('address',      () => fillInput(cdp, sessions, '#billingAddress-addressLine1Input', billing.addressLine1));
  // The address line is an autocomplete; dismiss its dropdown before moving on
  try { await cdp.send('Input.dispatchKeyEvent', { type: 'keyDown', key: 'Escape',
        windowsVirtualKeyCode: 27, code: 'Escape' }); } catch {}
  await sleep(500);
  allOk &= await step('city',         () => fillInput(cdp, sessions, '#billingAddress-localityInput', billing.city));
  if (billing.state) {
    allOk &= await step('state',      () => selectOption(cdp, sessions, '#billingAddress-administrativeAreaInput', billing.state));
  }
  allOk &= await step('postal code',  () => fillInput(cdp, sessions, '#billingAddress-postalCodeInput', billing.postalCode));

  // Let the total settle rather than reading it once. It was seen to pass
  // through four different tax figures before landing.
  note('wait', 'for the tax to settle');
  let settled = await waitForStableTax(cdp);
  note('wait', `tax settled after ${((settled.tookMs || 0)/1000).toFixed(1)}s` +
               ` (${settled.changes || 0} change${settled.changes === 1 ? '' : 's'} seen)`);

  // Sometimes the server keeps the rate it had before the address was
  // entered — a foreign VAT stays on a US address and the total never moves.
  // Re-entering the address from a different country makes it price the
  // address as new. Yemen is used as the intermediate because it carries no
  // tax, so the second change is unmistakable.
  //
  // Both recoveries cost time, so they only run when tax is actually being
  // charged. An address that priced correctly the first time is untouched.
  const stillTaxed = () => {
    const t = settled.totals?.tax || '';
    if (!t) return false;
    const pct = taxPercent(t);
    const amt = taxAmount(t);
    if (pct === 0) return false;
    if (pct === null && (amt === 0 || amt === null)) return false;
    return true;
  };

  const refillAddress = async () => {
    await step('billing name', () => fillInput(cdp, sessions, '#billingAddress-nameInput', billing.name));
    await step('country',      () => selectOption(cdp, sessions, '#billingAddress-countryInput', billing.country));
    await sleep(800);
    await waitUntil(() => fieldExists(cdp, sessions, '#billingAddress-postalCodeInput'),
                    { timeoutMs: 30000, everyMs: 500 });
    await step('address',      () => fillInput(cdp, sessions, '#billingAddress-addressLine1Input', billing.addressLine1));
    try { await cdp.send('Input.dispatchKeyEvent', { type: 'keyDown', key: 'Escape',
          windowsVirtualKeyCode: 27, code: 'Escape' }); } catch {}
    await sleep(500);
    await step('city',         () => fillInput(cdp, sessions, '#billingAddress-localityInput', billing.city));
    if (billing.state) {
      await step('state',      () => selectOption(cdp, sessions, '#billingAddress-administrativeAreaInput', billing.state));
    }
    await step('postal code',  () => fillInput(cdp, sessions, '#billingAddress-postalCodeInput', billing.postalCode));
  };

  if (stillTaxed()) {
    note('tax', `still charging ${settled.totals?.tax} — switching country away and back`);
    try {
      await step('country → YE', () =>
        selectOption(cdp, sessions, '#billingAddress-countryInput', 'YE'));
      await sleep(5000);
      await refillAddress();
      note('wait', 'for the tax to settle again');
      settled = await waitForStableTax(cdp);
      note('wait', `tax settled after ${((settled.tookMs || 0)/1000).toFixed(1)}s` +
                   ` (${settled.changes || 0} change${settled.changes === 1 ? '' : 's'} seen)`);
    } catch (e) {
      note('tax', `could not re-enter the country: ${e.message}`);
    }
  }

  if (stillTaxed()) {
    // Last resort: the page itself may be holding the old quote. Load the
    // checkout again and fill everything from scratch.
    note('tax', `still charging ${settled.totals?.tax} — reloading the checkout and starting over`);
    try {
      await cdp.send('Page.reload', { ignoreCache: true });
      await sleep(12000);
      sessions = await attachToFrames(cdp);
      const backUp = await waitUntil(
        () => fieldExists(cdp, sessions, '#billingAddress-nameInput'),
        { timeoutMs: 60000, everyMs: 700 });
      if (!backUp) {
        note('tax', 'the form did not come back after the reload');
      } else {
        await step('card number', () => fillInput(cdp, sessions, '#payment-numberInput', card.number));
        await step('expiry',      () => fillInput(cdp, sessions, '#payment-expiryInput', card.expiry));
        await step('cvc',         () => fillInput(cdp, sessions, '#payment-cvcInput', card.cvc));
        await waitUntil(() => fieldExists(cdp, sessions, '#billingAddress-nameInput'),
                        { timeoutMs: 60000, everyMs: 700 });
        await refillAddress();
        note('wait', 'for the tax to settle after the reload');
        settled = await waitForStableTax(cdp);
        note('wait', `tax settled after ${((settled.tookMs || 0)/1000).toFixed(1)}s` +
                     ` (${settled.changes || 0} change${settled.changes === 1 ? '' : 's'} seen)`);
      }
    } catch (e) {
      note('tax', `the reload attempt failed: ${e.message}`);
    }
  }

  const after = settled.totals;
  note('price', `${after.subscription} | ${after.tax || 'no tax line'} | ${after.dueToday}`);
  if (!settled.settled) note('warn', 'the total was still moving when the wait ran out');
  if (settled.provisional) note('warn', 'the tax line still says "estimated"');
  await shot('filled');

  // Prefer the amounts the server quoted. The page text was the only source
  // before, and reading a total off the screen means trusting a render.
  const quoted = payApi.amounts();
  let pct, amt, taxFree;
  if (quoted && quoted.tax != null) {
    amt = quoted.tax;
    pct = null;
    taxFree = quoted.tax === 0;
    note('price', `server quotes ${quoted.currency} ${quoted.total} ` +
                  `(tax ${quoted.tax}${quoted.discount ? `, discount ${quoted.discount}` : ''})`);
  } else {
    pct = taxPercent(after.tax);
    amt = taxAmount(after.tax);
    taxFree = (pct === 0) || (pct === null && (amt === 0 || amt === null));
  }

  const result = { payUrl, account: email, card: card.name || card.number.slice(-4),
                   quoted: payApi.amounts(),
                   billing, before, after, steps, taxPercent: pct, taxAmount: amt };

  if (!allOk) {
    note('stop', 'some fields did not fill — not submitting');
    result.outcome = { result: 'not submitted', reason: 'a field failed to fill' };
    return finish(result);
  }
  if (!taxFree && !allowTax) {
    note('stop', `tax is ${pct != null ? pct + '%' : amt} — not submitting (use --allow-tax to override)`);
    result.outcome = { result: 'not submitted', reason: `tax would be charged: ${after.tax}` };
    return finish(result);
  }
  if (!settled.settled && !allowTax) {
    note('stop', 'the total never stopped changing — not submitting against a moving figure');
    result.outcome = { result: 'not submitted', reason: 'total had not settled' };
    return finish(result);
  }
  if (dryRun) {
    note('dry-run', 'everything is filled; stopping before Subscribe');
    result.outcome = { result: 'dry run' };
    return finish(result);
  }

  // ── Subscribe ──
  note('submit', 'pressing Subscribe');
  const clicked = parseEval(await cdp.send('Runtime.evaluate', {
    expression: `(() => {
      const b = [...document.querySelectorAll('button')].find(x =>
        x.offsetParent !== null && /^subscribe$/i.test((x.innerText||'').trim()));
      if (!b) return JSON.stringify({clicked:false});
      b.click(); return JSON.stringify({clicked:true});
    })()`, returnByValue: true, timeout: 8000 }));
  if (!clicked.clicked) {
    note('error', 'the Subscribe button was not found');
    result.outcome = { result: 'not submitted', reason: 'no Subscribe button' };
    return finish(result);
  }

  // ── Watch what comes back ──
  const start = Date.now();
  let outcome = null, sawAuth = false, seen = new Set();
  while (Date.now() - start < 180000) {
    await sleep(2500);
    let d;
    try {
      d = parseEval(await cdp.send('Runtime.evaluate', {
        expression: TOTALS_PROBE + ` , (() => {
          const t = document.body?.innerText || '';
          return JSON.stringify({
            auth: /purchase authentication|proceed with authentication|3d ?secure/i.test(t),
            done: /thank you|payment (received|complete|successful)|subscription (is )?active|you'?re (now )?subscribed|welcome to/i.test(t),
            url: location.href
          });
        })()`, returnByValue: true, timeout: 8000 }));
    } catch { continue; }

    const page = parseEval(await cdp.send('Runtime.evaluate', {
      expression: TOTALS_PROBE, returnByValue: true, timeout: 8000 }));

    // The site's own answer comes first. A confirm response is unambiguous
    // where page text is not: a completed payment was once reported as
    // "refused: Think" — a word from the chat interface that happened to be
    // on screen once the checkout had moved on.
    const verdict = payApi.verdict();
    if (verdict) {
      if (verdict.paid) {
        note('done', 'the payment was confirmed by the server');
        outcome = { result: 'succeeded', at: ms(), confirmedByApi: true };
        await shot('confirmed');
        break;
      }
      if (!outcome) {
        const l4 = String(card.number).replace(/\s/g, '').slice(-4);
        note('refused', `${verdict.why}  [card ****${l4}]`);
        outcome = {
          result: 'declined', kind: 'refused by the payment API',
          message: verdict.why, at: ms(), confirmedByApi: true,
          card: card.name || null, cardLast4: l4
        };
        await shot('refused_api');
        break;
      }
    }

    // Page text is only consulted while still on the checkout itself. Once
    // the URL has moved on, anything red belongs to the ordinary interface.
    const stillOnCheckout = /\/checkout\//.test(page.url || '');
    for (const e of (stillOnCheckout ? (page.errors || []) : [])) {
      if (seen.has(e)) continue;
      seen.add(e);
      // Real payment wording only — a bare word with no payment vocabulary is
      // interface text leaking in, not a decline.
      if (!/declin|insufficient|expired|incorrect|invalid|not approved|unable to|could not|try a different|do not honou?r|rejected|security code|cvc/i.test(e)) continue;
      // "Your card number is incomplete" is the form checking itself before
      // anything was sent, not a verdict from the bank.
      if (/incomplete|is required|enter a valid|please enter/i.test(e)) {
        note('form', e);
        continue;
      }
      note('refused', `${e}  [card ****${String(card.number).replace(/\s/g,'').slice(-4)}]`);
      // "Payment was not approved" is a refusal with no distinguishing word in
      // it, so anything that reaches this point without matching a specific
      // cause is still a refusal — just one the page did not explain.
      const kind =
        /unable to authenticate|authentication|3d ?secure/i.test(e) ? 'authentication' :
        /insufficient/i.test(e)                    ? 'insufficient funds' :
        /expired/i.test(e)                         ? 'expired card' :
        /cvc|security code/i.test(e)               ? 'wrong CVC' :
        /number.*(incorrect|invalid)/i.test(e)     ? 'wrong number' :
        /not approved|declin|rejected|do not honou?r/i.test(e) ? 'bank declined' :
                                                     'refused without a reason given';
      outcome = outcome || {
        result: 'declined', kind, message: e, at: ms(),
        card: card.name || null, cardLast4: String(card.number).replace(/\s/g,'').slice(-4)
      };
    }

    if (d.auth && !sawAuth) { sawAuth = true; note('3d-secure', 'the bank is asking to authenticate'); await shot('3ds'); }
    if (d.done) { note('done', 'the subscription went through'); outcome = { result: 'succeeded', at: ms() }; await shot('success'); break; }
    if (/\/(success|thank|settings)/i.test(d.url || '') && !/checkout/.test(d.url)) {
      note('done', 'left checkout for ' + d.url.slice(0, 50));
      outcome = outcome || { result: 'succeeded', at: ms(), landedOn: d.url };
      await shot('after'); break;
    }
    if (outcome?.result === 'declined') break;
  }

  await shot('final');
  result.outcome = outcome || { result: 'unknown', note: 'nothing conclusive within 3 minutes' };
  result.authenticationAsked = sawAuth;

  // Turn off renewal while the browser is still on this account and the
  // session is still good. Only after a payment that actually went through —
  // there is nothing to cancel otherwise.
  if (result.outcome.result === 'succeeded' && !keepRenewal) {
    note('renewal', 'turning off auto-renewal');
    let r;
    try {
      r = await stopAutoRenewal(cdp, { afterPayment: true });
    } catch (e) {
      // A payment that succeeded should not be reported as a failed run
      // because the page navigated while the renewal was being switched off.
      r = { done: false, why: `the page moved during cancellation (${String(e.message).slice(0, 60)})` };
    }
    result.autoRenewal = r;

    // Our card is still on file at this point, and a renewal there would
    // charge us rather than the customer.
    if (!keepCards) {
      const rc = await removeOurCard(cdp, global.__acctId, card.number, note);
      result.cardRemoval = rc;
    }
    if (r.done && r.alreadyOff) {
      note('renewal', `already off — access runs to ${whenDate(r.endsAt)}`);
      if (!r.endsAt) note('renewal', 'no end date came back — check it yourself');
    } else if (r.done) {
      note('renewal', `off — access runs to ${whenDate(r.endsAt)}`);
    } else {
      note('renewal', `could not turn it off: ${r.why}`);
    }
  }

  return finish(result);

  function finish(res) {
    if (heartbeat) clearInterval(heartbeat);
    if (res) fs.writeFileSync(path.join(dir, 'result.json'), JSON.stringify(res, null, 2));
    fs.writeFileSync(path.join(dir, 'log.json'), JSON.stringify(log, null, 2));

    const L = [];
    L.push('═'.repeat(74));
    L.push('  AUTOMATED PAYMENT');
    L.push('═'.repeat(74));
    if (res) {
      L.push(`  Account : ${res.account || '?'}`);
      L.push(`  Card    : ****${String(card.number).replace(/\s/g,'').slice(-4)}${card.name ? ' (' + card.name + ')' : ''}`);
      L.push(`  Billing : ${billing.name}, ${billing.addressLine1}, ${billing.city}, ${billing.state || ''} ${billing.postalCode}, ${billing.country}`);
      L.push('');
      L.push('  Totals before the address was entered:');
      L.push(`    ${res.before?.subscription || '?'}`);
      L.push(`    ${res.before?.tax || 'no tax line'}`);
      L.push(`    ${res.before?.dueToday || '?'}`);
      L.push('');
      L.push('  Totals after:');
      L.push(`    ${res.after?.subscription || '?'}`);
      L.push(`    ${res.after?.tax || 'no tax line'}`);
      L.push(`    ${res.after?.dueToday || '?'}`);
      L.push('');
      const failed = (res.steps || []).filter(s => !s.ok);
      if (failed.length) {
        L.push('  Fields that did not fill:');
        failed.forEach(f => L.push(`    ${f.label} — ${f.why}`));
        L.push('');
      }
      if (res.quoted && res.quoted.total != null) {
        L.push('  What the server quoted:');
        L.push(`    ${res.quoted.currency} ${res.quoted.total}   tax ${res.quoted.tax}` +
               (res.quoted.discount ? `   discount ${res.quoted.discount}` : ''));
        L.push('');
      }
      L.push(`  Outcome : ${res.outcome?.result}` +
             (res.outcome?.confirmedByApi ? '   (from the payment API, not page text)' : ''));
      if (res.outcome?.kind)    L.push(`  Kind    : ${res.outcome.kind}`);
      if (res.outcome?.message) L.push(`  Said    : ${res.outcome.message}`);
      if (res.outcome?.reason)  L.push(`  Reason  : ${res.outcome.reason}`);

      // When the payment is refused, say plainly which card it was and what to
      // do about it — with several cards on file the decline is useless
      // without knowing which one to top up, replace, or set aside.
      if (res.outcome?.result === 'declined') {
        const last4 = String(card.number).replace(/\s/g, '').slice(-4);
        L.push('');
        L.push('  ── This card was refused ──');
        L.push(`  Card    : ****${last4}${card.name ? ' (' + card.name + ')' : ''}`);
        const advice = {
          'insufficient funds': 'Top the card up — it is otherwise fine.',
          'expired card':       'The card has expired. Use a different one.',
          'wrong CVC':          'The CVC was wrong. Check it, or the card details are off.',
          'wrong number':       'The number did not check out. Verify the card details.',
          'authentication':     'The card is fine — the bank wanted confirmation and it was not completed.',
          'bank declined':      'The bank refused it. Try a different card.'
        }[res.outcome.kind] || 'Try a different card.';
        L.push(`  What now: ${advice}`);
        // The code is only spent on a real success, so a decline leaves it usable
        L.push('  The activation code is still valid — you can retry.');
      }
      if (res.authenticationAsked) L.push('  The bank asked for its own authentication step.');
      if (res.autoRenewal) {
        const r = res.autoRenewal;
        L.push('');
        L.push(`  Auto-renewal : ${r.done ? (r.alreadyOff ? 'was already off' : 'switched off') : 'STILL ON — ' + r.why}`);
        if (res.cardRemoval) {
          const rc = res.cardRemoval;
          L.push(`  Our card     : ` +
            (rc.nothingToDo ? 'was not on the account' :
             rc.removed ? `removed, ${rc.left} card(s) left on the account` :
             `STILL ON FILE — ${rc.why}`));
        }
        if (r.endsAt) L.push(`  Access until : ${whenDate(r.endsAt)}`);
        if (!r.done) L.push('  Cancel it manually at chatgpt.com/#settings/Billing');
      }
    }
    L.push('═'.repeat(74));
    const text = L.join('\n');
    fs.writeFileSync(path.join(dir, 'report.txt'), text);
    console.log('\n' + text);
    console.log(`\n  Saved to: ${dir}`);
    console.log(`  ${shotN} screenshot${shotN === 1 ? '' : 's'} in ${path.join(dir, 'shots')}\n`);

    if (proxy) {
      const failed = proxy.getFailures();
      if (failed.length) {
        console.log('\n  The proxy would not connect to:');
        failed.forEach(f => console.log(`    ${f.host}  —  ${f.reason}${f.count > 1 ? ` (x${f.count})` : ''}`));
      }
      const retries = proxy.getRetries?.() || 0;
      if (retries) console.log(`\n  Retried ${retries} connection${retries === 1 ? '' : 's'} the proxy first refused`);
      const went = proxy.getBypassed?.() || [];
      if (went.length) console.log(`\n  Went direct (not through the proxy): ${went.join(', ')}`);
      console.log(`  Proxy traffic: ${(proxy.getBytes() / 1024).toFixed(0)} KB`);
    }

    try { ws.close(); } catch {}
    try { chrome.kill(); } catch {}
    try { proxy?.close(); } catch {}
    process.exit(0);
  }
}

// ═══════════════════════════════════════════════════════════════

// ── Proxy ──
// The same relay the link generator uses. Targeting is encoded differently by
// each provider and guessing wrong just fails authentication, so only hosts
// whose format is known get their credentials rewritten.
function startProxyRelay(proxyStr, localPort, country, bypassHosts = []) {
  return new Promise((resolve, reject) => {
    let u;
    try { u = new URL(proxyStr.startsWith('http') ? proxyStr : `http://${proxyStr}`); }
    catch { return reject(new Error('that proxy string could not be parsed')); }

    const safeId = v => String(v).replace(/[^a-zA-Z0-9]/g, '').slice(0, 24);
    const sess = safeId('pay' + Date.now());
    let user = decodeURIComponent(u.username || '');
    let pass = decodeURIComponent(u.password || '');
    const host = u.hostname, port = parseInt(u.port) || 8080;

    const setTok = (str, re, val) => re.test(str) ? str.replace(re, val) : str + val;
    if (/proxy-cheap\.com$/i.test(host)) {
      if (country) pass = setTok(pass, /_country-[A-Za-z]{2}/i, `_country-${country.toUpperCase()}`);
      // A sticky-session token is deliberately not added here. On this
      // account "_country-PH" alone resolves to a Philippine address, while
      // "_country-PH_session-xxx" is answered with "bad gateway" — so the
      // token that was meant to hold one exit address instead removed every
      // exit address. Country targeting on its own is what works.
    } else if (/dataimpulse\.com$/i.test(host)) {
      if (country) user = setTok(user, /__cr\.[A-Za-z]{2}/i, `__cr.${country.toLowerCase()}`);
      user = setTok(user, /__s\.[^_]*/i, `__s.${sess}`);
    }

    const authH = 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');
    let bytes = 0;
    let retried = 0;
    const bypassed = new Set();
    const failures = new Map();
    const noteFail = (target, why) => {
      const h = String(target || '?').split(':')[0];
      const r = failures.get(h) || { reason: why, count: 0 };
      r.reason = why; r.count++; failures.set(h, r);
    };

    const srv = http.createServer((req, res) => {
      const p2 = http.request({ hostname: host, port, path: req.url, method: req.method,
        headers: { ...req.headers, 'Proxy-Authorization': authH } }, pR => {
        res.writeHead(pR.statusCode, pR.headers);
        pR.on('data', c => { bytes += c.length; res.write(c); });
        pR.on('end', () => res.end());
      });
      req.on('data', c => { bytes += c.length; p2.write(c); });
      req.on('end', () => p2.end());
      p2.on('error', e => { noteFail(req.headers?.host || req.url, e.code || e.message); try { res.end(); } catch {} });
      res.on('error', () => {});
    });

    srv.on('connect', (req, sock, head) => {
      const target = req.url;
      const targetHost = String(target || '').split(':')[0];
      let settled = false;
      sock.on('error', () => {});

      // Some hosts have to skip the proxy entirely. DataImpulse answers 403
      // to CONNECT for js.stripe.com as a matter of policy, and without that
      // script the card fields never render — the checkout page loads with no
      // iframes at all. Sending only those hosts straight out keeps the
      // pricing and the account on the proxy's country while letting the
      // payment form load.
      if (bypassHosts.some(h => targetHost === h || targetHost.endsWith('.' + h))) {
        const [, portStr] = String(target).split(':');
        const direct = net.connect(parseInt(portStr) || 443, targetHost, () => {
          settled = true;
          bypassed.add(targetHost);
          try { sock.write('HTTP/1.1 200 Connection Established\r\n\r\n'); } catch { return; }
          if (head?.length) { try { direct.write(head); } catch {} }
          direct.pipe(sock);
          sock.pipe(direct);
        });
        direct.on('error', () => {
          if (settled) return;
          settled = true;
          try { sock.write('HTTP/1.1 502 Bad Gateway\r\n\r\n'); sock.end(); } catch {}
        });
        return;
      }
      // A refusal from the proxy is not one thing. 403 is policy — the
      // provider will not carry this host, and asking again changes nothing.
      // 500 is the gateway having a bad moment: the same host answered 200
      // through this proxy on the very next attempt, so giving up on the
      // first 500 threw away a connection that was available. Retry the
      // transient ones, several times, with room between.
      const MAX_TRIES = 6;
      const fail = (n, why, retryable) => {
        if (settled) return;
        if (retryable && n < MAX_TRIES) {
          retried++;
          setTimeout(() => go(n + 1), 500 * n);
          return;
        }
        settled = true; noteFail(target, why + (n > 1 ? ` after ${n} attempts` : ''));
        try { sock.write('HTTP/1.1 502 Bad Gateway\r\n\r\n'); sock.end(); } catch {}
      };
      const go = n => {
        const t = http.request({ hostname: host, port, method: 'CONNECT', path: target,
          headers: { 'Proxy-Authorization': authH, Host: target } });
        t.on('connect', (res, pS) => {
          if (res.statusCode !== 200) {
            try { pS.destroy(); } catch {}
            const transient = res.statusCode >= 500 || res.statusCode === 429;
            return fail(n, `HTTP ${res.statusCode} on CONNECT`, transient);
          }
          settled = true;
          try { sock.write('HTTP/1.1 200 Connection Established\r\n\r\n'); } catch { return; }
          if (head?.length) { bytes += head.length; try { pS.write(head); } catch {} }
          pS.on('data', c => { bytes += c.length; try { sock.write(c); } catch {} });
          sock.on('data', c => { bytes += c.length; try { pS.write(c); } catch {} });
          pS.on('error', () => { try { sock.end(); } catch {} });
          sock.on('error', () => { try { pS.end(); } catch {} });
          pS.on('end', () => { try { sock.end(); } catch {} });
          sock.on('end', () => { try { pS.end(); } catch {} });
        });
        t.on('response', r => {
          r.resume();
          const transient = r.statusCode >= 500 || r.statusCode === 429;
          fail(n, `proxy refused CONNECT with HTTP ${r.statusCode}`, transient);
        });
        t.on('error', e => fail(n, e.code || e.message, true));
        t.setTimeout(n === 1 ? 15000 : 10000, () => { try { t.destroy(); } catch {} fail(n, 'CONNECT timed out', true); });
        t.end();
      };
      go(1);
    });

    srv.on('error', () => {});
    srv.listen(localPort, '127.0.0.1', () => resolve({
      close: () => { try { srv.close(); } catch {} },
      getBytes: () => bytes,
      getBypassed: () => [...bypassed],
      getRetries: () => retried,
      getFailures: () => [...failures.entries()].map(([h, v]) => ({ host: h, ...v }))
    }));
  });
}

const fetchExitIP = viaPort => new Promise(resolve => {
  const o = viaPort
    ? { host: '127.0.0.1', port: viaPort, path: 'http://ip-api.com/json/?fields=query,country,countryCode,city',
        headers: { Host: 'ip-api.com' } }
    : { host: 'ip-api.com', port: 80, path: '/json/?fields=query,country,countryCode,city' };
  const req = http.request({ ...o, method: 'GET' }, res => {
    let d = ''; res.on('data', c => d += c);
    res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve(null); } });
  });
  req.on('error', () => resolve(null));
  req.setTimeout(15000, () => { try { req.destroy(); } catch {} resolve(null); });
  req.end();
});

const describeIP = e => e?.query
  ? `${e.query} · ${e.country} (${e.countryCode})${e.city ? ' · ' + e.city : ''}`
  : 'could not be determined';

function loadProxies() {
  try {
    const raw = JSON.parse(fs.readFileSync('./proxies.json', 'utf8'));
    return (Array.isArray(raw) ? raw : []).map((x, i) =>
      typeof x === 'string' ? { name: String(i + 1), url: x }
                            : { name: x.name || x.label || String(i + 1),
                                url: x.url || x.proxyString || x.proxy_string });
  } catch { return []; }
}

function ask(q) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise(r => rl.question(q, a => { rl.close(); r(a.trim()); }));
}

function loadCards() {
  try {
    const raw = JSON.parse(fs.readFileSync('./cards.json', 'utf8'));
    return (Array.isArray(raw) ? raw : []).map((c, i) => ({
      name: c.name || String(i + 1),
      number: String(c.number || c.card || '').replace(/\s/g, ''),
      expiry: String(c.expiry || c.exp || ''),
      cvc: String(c.cvc || c.cvv || '')
    })).filter(c => c.number && c.expiry && c.cvc);
  } catch { return []; }
}

function loadBilling() {
  try { return { ...DEFAULT_BILLING, ...JSON.parse(fs.readFileSync('./billing.json', 'utf8')) }; }
  catch { return { ...DEFAULT_BILLING }; }
}

function newestLink() {
  try {
    const files = fs.readdirSync('./results').filter(f => f.endsWith('.json'))
      .map(f => ({ f, t: fs.statSync(path.join('./results', f)).mtimeMs }))
      .sort((a, b) => b.t - a.t);
    if (!files.length) return null;
    return JSON.parse(fs.readFileSync(path.join('./results', files[0].f), 'utf8')).url || null;
  } catch { return null; }
}

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = argv.includes('--dry-run');
  const allowTax = argv.includes('--allow-tax');
  const keepRenewal = argv.includes('--keep-renewal');
  const clearCards = argv.includes('--clear-cards');
  const keepCards = argv.includes('--keep-cards');
  const cancelExisting = argv.includes('--cancel-existing');
  const plan = argv.find(a => a.startsWith('--plan='))?.slice(7) || 'plus';
  const noBypass = argv.includes('--no-bypass');
  const linkProxyFlag = argv.find(a => a.startsWith('--link-proxy='))?.slice(13) || null;
  const linkCountry = argv.find(a => a.startsWith('--link-country='))?.slice(15) || null;
  const bypass = (argv.find(a => a.startsWith('--bypass='))?.slice(9) || '')
    .split(',').map(x => x.trim()).filter(Boolean);
  const pick = argv.find(a => a.startsWith('--card='))?.slice(7);
  const proxyFlag = argv.find(a => a.startsWith('--proxy='))?.slice(8);
  const noProxy = argv.includes('--no-proxy');
  const countryFlag = argv.find(a => a.startsWith('--country='))?.slice(10);

  const cards = loadCards();
  if (!cards.length) {
    console.error('\n  cards.json has no usable card. It should look like:\n');
    console.error('    [{"name":"redotpay","number":"4937242024932285","expiry":"07/29","cvc":"557"}]\n');
    process.exit(1);
  }
  const card = pick
    ? (cards.find(c => c.name.toLowerCase() === pick.toLowerCase()) || cards[parseInt(pick) - 1])
    : cards[0];
  if (!card) { console.error(`\n  no card named "${pick}"\n`); process.exit(1); }

  const billing = loadBilling();
  const proxies = loadProxies();

  console.log('\n' + '═'.repeat(74));
  console.log('  Automated payment');
  console.log('═'.repeat(74));

  // ── The link ──
  // Asked for rather than assumed: a checkout session is single use, and
  // silently reusing the newest one on disk is how a run ends up pointed at
  // a link that was already spent.
  // A link is made in the same browser that will pay it, unless one is given.
  // Two browsers meant two proxy sessions and two exit addresses, so the price
  // was quoted from one and the card presented from another.
  let payUrl = argv.find(a => /https?:\/\//.test(a));
  if (payUrl) payUrl = payUrl.replace(/^[<"'\s]+|[>"'\s]+$/g, '');

  if (!payUrl && argv.includes('--reuse-link')) {
    payUrl = newestLink();
    if (!payUrl) { console.error('\n  No link in results/ to reuse.\n'); process.exit(1); }
    console.log(`\n  Reusing: ${payUrl.slice(0, 66)}…`);
  }
  if (payUrl && !/^https?:\/\//.test(payUrl)) {
    console.error('\n  That is not a link.\n');
    process.exit(1);
  }

  // ── The proxy ──
  let proxyStr = null;
  let proxyName = null;
  if (noProxy) {
    proxyStr = null;
  } else if (proxyFlag) {
    const p = proxies.find(x => x.name.toLowerCase() === proxyFlag.toLowerCase())
           || proxies[parseInt(proxyFlag) - 1];
    if (!p) { console.error(`\n  no proxy named "${proxyFlag}" in proxies.json\n`); process.exit(1); }
    proxyStr = p.url; proxyName = p.name;
  } else if (proxies.length === 1) {
    const yes = !/^n/i.test(await ask(`  Use the proxy (${proxies[0].name})? [Y/n]: `));
    if (yes) { proxyStr = proxies[0].url; proxyName = proxies[0].name; }
  } else if (proxies.length > 1) {
    console.log('');
    proxies.forEach((p, i) => {
      let host; try { host = new URL(p.url).hostname; } catch { host = p.url.slice(0, 30); }
      console.log(`    ${i + 1}. ${p.name}  (${host})`);
    });
    const ans = (await ask('  Proxy? [number, or 0 for none]: ')).trim();
    if (ans !== '0') {
      const p = proxies.find(x => x.name.toLowerCase() === ans.toLowerCase())
             || proxies[(parseInt(ans) || 1) - 1];
      if (p) { proxyStr = p.url; proxyName = p.name; }
    }
  } else {
    console.log('\n  No proxies.json — running on this connection.');
  }

  let proxyCountry = countryFlag || null;
  if (proxyStr && !proxyCountry) {
    const c = (await ask('  Exit country? [Enter to leave as configured]: ')).trim();
    if (c) proxyCountry = c.toUpperCase();
  }

  let linkProxyStr = null, linkProxyName = null;
  if (linkProxyFlag) {
    const lp = proxies.find(x => x.name.toLowerCase() === linkProxyFlag.toLowerCase())
            || proxies[parseInt(linkProxyFlag) - 1];
    if (!lp) { console.error(`\n  no proxy named "${linkProxyFlag}" in proxies.json\n`); process.exit(1); }
    linkProxyStr = lp.url; linkProxyName = lp.name;
  }
  console.log('\n' + '─'.repeat(74));
  console.log(`  Card    : ****${card.number.slice(-4)} (${card.name})`);
  console.log(`  Billing : ${billing.city}, ${billing.state || ''} ${billing.postalCode}, ${billing.country}`);
  console.log(`  Proxy   : ${proxyStr ? proxyName + (proxyCountry ? ' → ' + proxyCountry : '') : 'none'}`);
  console.log(`  Mode    : ${dryRun ? 'dry run — will not submit' : 'will submit'}`);
  console.log(`  Tax     : ${allowTax ? 'will submit even if taxed' : 'will stop if tax is charged'}`);
  console.log(`  Renewal : ${keepRenewal ? 'left on' : 'switched off after a successful payment'}`);
  console.log(`  Link    : ${payUrl ? 'given'
    : linkProxyStr ? `fetched via ${linkProxyName}${linkCountry ? ' → ' + linkCountry : ''}`
    : 'created in this same browser'}`);
  if (proxyStr) console.log(`  Stripe  : ${noBypass ? 'through the proxy' : 'direct — the proxy refuses it'}`);
  console.log('─'.repeat(74) + '\n');

  // When a separate provider is named for the link, that step runs first on
  // its own browser and its own network, and the payment browser is handed
  // the result. Without the flag everything stays in one browser, which is
  // the simpler arrangement and the one that has been working.
  if (!payUrl && linkProxyStr) {
    let token = '';
    try { token = String(JSON.parse(fs.readFileSync('./session_api.json','utf8')).sessionToken||'').trim(); }
    catch { console.error('\n  session_api.json not found\n'); process.exit(1); }

    const got = await fetchLinkVia({
      token, proxyStr: linkProxyStr, proxyCountry: linkCountry || proxyCountry, plan
    });
    if (got.alreadySubscribed) {
      console.log('\n  Nothing was paid and nothing was changed.');
      if (got.willRenew !== false) {
        console.log('  Run again with --cancel-existing to turn the renewal off.');
      }
      console.log('');
      process.exit(0);
    }
    if (!got.ok) {
      console.error(`\n  No link: ${got.why}`);
      if (got.detail) console.error(`  ${got.detail}`);
      if (/unusual activity/i.test(got.why || '')) {
        console.error('\n  The pricing page kept erroring after four reloads.');
        console.error('  Leave the account a while, or use a different session_api.json.');
      }
      console.error('');
      process.exit(1);
    }
    payUrl = got.url;

    console.log('\n' + '─'.repeat(74));
    console.log('  2. Paying it');
    console.log('─'.repeat(74));
  }

  await run({ payUrl, card, billing, dryRun, allowTax, proxyStr, proxyCountry, keepRenewal, plan, bypass, noBypass, clearCards, cancelExisting, keepCards });
}

main().catch(e => { console.error(`\n  ${e.message}\n`); process.exit(1); });
