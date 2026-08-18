'use strict';

const defaultNow = () => Date.now();

/**
 * Run independent recovery attempts and return the first candidate accepted by
 * validate(). Aborting a strategy is cooperative: browser strategies should
 * close their background target from the signal's abort handler.
 */
async function raceRecoveryStrategies({
  strategies,
  validate,
  expectedCountry,
  timeoutMs = 90000,
  trace = () => {},
  now = defaultNow
}) {
  if (!Array.isArray(strategies) || strategies.length === 0) {
    throw new Error('At least one recovery strategy is required');
  }
  const names = strategies.map(strategy => strategy.name);
  if (names.some(name => !name) || new Set(names).size !== names.length) {
    throw new Error('Recovery strategies require unique non-empty names');
  }

  const startedAt = now();
  const controllers = new Map();
  let settled = false;
  let remaining = strategies.length;
  const failures = [];

  trace('recovery_race_started', {
    expectedCountry,
    timeoutMs,
    strategies: strategies.map(strategy => strategy.name)
  });

  return new Promise((resolve, reject) => {
    const finishFailure = error => {
      if (settled) return;
      settled = true;
      clearTimeout(deadline);
      for (const controller of controllers.values()) controller.abort('race-finished');
      trace('recovery_race_failed', {
        expectedCountry,
        elapsedMs: now() - startedAt,
        failures
      });
      reject(error);
    };

    const deadline = setTimeout(() => {
      failures.push({ strategy: 'race', reason: `timeout after ${timeoutMs}ms` });
      finishFailure(new Error(`Recovery race timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    for (const strategy of strategies) {
      const controller = new AbortController();
      controllers.set(strategy.name, controller);
      const strategyStartedAt = now();
      trace('recovery_strategy_started', {
        strategy: strategy.name,
        expectedCountry
      });

      Promise.resolve()
        .then(() => strategy.run({
          signal: controller.signal,
          expectedCountry,
          strategy: strategy.name
        }))
        .then(async candidate => {
          if (settled || controller.signal.aborted) return;
          const verdict = await validate(candidate, {
            expectedCountry,
            strategy: strategy.name,
            signal: controller.signal
          });
          if (settled || controller.signal.aborted) return;

          if (!verdict?.ok) {
            const reason = verdict?.reason || 'candidate did not verify';
            failures.push({ strategy: strategy.name, reason });
            trace('recovery_strategy_rejected', {
              strategy: strategy.name,
              elapsedMs: now() - strategyStartedAt,
              reason,
              country: candidate?.verification?.state?.country ||
                candidate?.state?.country || candidate?.country || null
            });
            return;
          }

          settled = true;
          clearTimeout(deadline);
          for (const [name, loser] of controllers) {
            if (name !== strategy.name) {
              loser.abort(`winner:${strategy.name}`);
              trace('recovery_strategy_cancelled', {
                strategy: name,
                winner: strategy.name,
                reason: `winner:${strategy.name}`
              });
            }
          }
          const result = {
            strategy: strategy.name,
            candidate,
            verification: verdict,
            elapsedMs: now() - startedAt
          };
          trace('recovery_race_winner', {
            strategy: strategy.name,
            expectedCountry,
            elapsedMs: result.elapsedMs,
            country: verdict.country || candidate?.verification?.state?.country ||
              candidate?.state?.country || candidate?.country || null
          });
          resolve(result);
        })
        .catch(error => {
          if (settled) return;
          const cancelled = controller.signal.aborted;
          if (!cancelled) {
            failures.push({ strategy: strategy.name, reason: error?.message || String(error) });
          }
          trace(cancelled ? 'recovery_strategy_cancelled' : 'recovery_strategy_failed', {
            strategy: strategy.name,
            elapsedMs: now() - strategyStartedAt,
            reason: cancelled ? controller.signal.reason : error?.message || String(error)
          });
        })
        .finally(() => {
          remaining--;
          if (!settled && remaining === 0) {
            finishFailure(new Error(`No recovery strategy verified ${expectedCountry || 'the route'}`));
          }
        });
    }
  });
}

module.exports = { raceRecoveryStrategies };
