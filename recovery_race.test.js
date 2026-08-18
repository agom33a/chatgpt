'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { raceRecoveryStrategies } = require('./recovery_race');

const deferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

const candidate = country => ({
  ready: true,
  verification: { ok: true, state: { loggedIn: true, status: 200, country } }
});

const validate = async (value, { expectedCountry }) => {
  const state = value?.verification?.state || {};
  const ok = value?.ready && value?.verification?.ok &&
    state.loggedIn && state.status === 200 && state.country === expectedCountry;
  return { ok, country: state.country, reason: ok ? null : 'authenticated country mismatch' };
};

test('first verified expected-country candidate wins and aborts losers', async () => {
  const slow = deferred();
  const fast = deferred();
  const aborted = [];
  const make = (name, gate) => ({
    name,
    run: ({ signal }) => {
      signal.addEventListener('abort', () => aborted.push(name), { once: true });
      return gate.promise;
    }
  });

  const racing = raceRecoveryStrategies({
    strategies: [make('slow-document', slow), make('fast-auth', fast)],
    validate,
    expectedCountry: 'PH',
    timeoutMs: 1000
  });
  fast.resolve(candidate('PH'));

  const winner = await racing;
  assert.equal(winner.strategy, 'fast-auth');
  assert.deepEqual(aborted, ['slow-document']);
  slow.resolve(candidate('PH'));
});

test('faster authenticated candidate for the wrong country cannot win', async () => {
  const oldRegion = deferred();
  const newRegion = deferred();
  const rejected = [];
  const racing = raceRecoveryStrategies({
    strategies: [
      { name: 'cached-edge', run: () => oldRegion.promise },
      { name: 'fresh-edge', run: () => newRegion.promise }
    ],
    validate,
    expectedCountry: 'PH',
    timeoutMs: 1000,
    trace: (event, details) => {
      if (event === 'recovery_strategy_rejected') rejected.push(details);
    }
  });

  oldRegion.resolve(candidate('EG'));
  await Promise.resolve();
  await Promise.resolve();
  newRegion.resolve(candidate('PH'));

  const winner = await racing;
  assert.equal(winner.strategy, 'fresh-edge');
  assert.equal(rejected[0].country, 'EG');
});

test('all failed or unverified strategies reject deterministically', async () => {
  await assert.rejects(
    raceRecoveryStrategies({
      strategies: [
        { name: 'reset', run: async () => { throw new Error('ERR_CONNECTION_RESET'); } },
        { name: 'anonymous', run: async () => candidate('EG') }
      ],
      validate,
      expectedCountry: 'PH',
      timeoutMs: 1000
    }),
    /No recovery strategy verified PH/
  );
});

test('parallel winner is not blocked by a slow loser', async () => {
  const startedAt = Date.now();
  const racing = raceRecoveryStrategies({
    strategies: [
      {
        name: 'slow',
        run: ({ signal }) => new Promise(resolve => {
          const timer = setTimeout(() => resolve(candidate('PH')), 250);
          signal.addEventListener('abort', () => clearTimeout(timer), { once: true });
        })
      },
      {
        name: 'fast',
        run: async () => {
          await new Promise(resolve => setTimeout(resolve, 15));
          return candidate('PH');
        }
      }
    ],
    validate,
    expectedCountry: 'PH',
    timeoutMs: 1000
  });

  const winner = await racing;
  const elapsedMs = Date.now() - startedAt;
  assert.equal(winner.strategy, 'fast');
  assert.ok(elapsedMs < 150, `race took ${elapsedMs}ms`);
});
