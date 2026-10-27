const redis = require('redis');
const crypto = require('crypto');

const RENEW_SCRIPT = 'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end';
const RELEASE_SCRIPT = 'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end';

/**
 * Dedicated client for scheduler coordination. Commands fail fast while disconnected
 * (no offline queue), so a Redis outage skips scheduler ticks instead of piling them up.
 */
function createSchedulerRedis(url) {
	const client = redis.createClient(url, {
		enable_offline_queue: false,
		retry_strategy: (options) => Math.min(options.attempt * 500, 5000),
	});
	let lastError = '';
	client.on('error', (err) => {
		if (err.message !== lastError) console.error('[scheduler] redis error:', err.message);
		lastError = err.message;
	});
	client.on('ready', () => {
		lastError = '';
		console.log('[scheduler] redis ready');
	});

	const run = (command, ...args) => new Promise((resolve, reject) => {
		client[command](...args, (err, result) => (err ? reject(err) : resolve(result)));
	});

	return {
		client,
		isReady: () => client.connected === true,
		run,

		/** Returns a lock token when acquired, or null when another holder owns the key. */
		async acquire(key, ttlMs) {
			const token = crypto.randomBytes(16).toString('hex');
			const result = await run('set', key, token, 'NX', 'PX', ttlMs);
			return result === 'OK' ? token : null;
		},

		async renew(key, token, ttlMs) {
			return (await run('eval', RENEW_SCRIPT, 1, key, token, ttlMs)) === 1;
		},

		async release(key, token) {
			return (await run('eval', RELEASE_SCRIPT, 1, key, token)) === 1;
		},

		quit: () => new Promise((resolve) => client.quit(() => resolve())),
	};
}

module.exports = { createSchedulerRedis };
