const os = require('os');
const crypto = require('crypto');
const { callWebService } = require('./webServiceExecutor');

class Semaphore {
	constructor(max) {
		this.max = max;
		this.active = 0;
		this.waiting = [];
	}

	acquire() {
		if (this.active < this.max) {
			this.active += 1;
			return Promise.resolve();
		}
		return new Promise((resolve) => this.waiting.push(resolve));
	}

	release() {
		const next = this.waiting.shift();
		if (next) next();
		else this.active -= 1;
	}
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function runPool(items, limit, fn) {
	let cursor = 0;
	const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
		while (cursor < items.length) {
			const item = items[cursor];
			cursor += 1;
			await fn(item);
		}
	});
	await Promise.all(workers);
}

/** Bounded retry for bookkeeping calls whose repetition is harmless (guarded by lease token and step state in glozic). */
async function withRetry(fn, attempts = 3) {
	let lastError;
	for (let i = 0; i < attempts; i++) {
		try {
			return await fn();
		} catch (err) {
			lastError = err;
			const transient = !err.statusCode || err.statusCode >= 500;
			if (!transient || i === attempts - 1) break;
			await sleep(1000 * (i + 1));
		}
	}
	throw lastError;
}

function createCoordinator({ config, store, glozic, registry }) {
	const workerId = `${os.hostname()}:${process.pid}:${crypto.randomBytes(4).toString('hex')}`;
	const executionSlots = new Semaphore(config.maxConcurrency);
	const state = {
		running: false,
		stopping: false,
		timer: null,
		refreshTimer: null,
		currentTick: null,
		inFlight: 0,
		lastTickAt: null,
		lastTickError: '',
		lastRefreshAt: null,
		lastRefreshError: '',
	};

	async function refreshRegistry() {
		if (!store.isReady()) {
			state.lastRefreshError = 'Redis not ready';
			return;
		}
		try {
			const count = await registry.refresh();
			state.lastRefreshAt = new Date();
			state.lastRefreshError = '';
			console.log(`[scheduler] tenant registry refreshed (${count} tenant(s))`);
		} catch (err) {
			state.lastRefreshError = err.message;
			console.warn(`[scheduler] tenant registry refresh failed: ${err.message}`);
		}
	}

	async function runWebServiceStep(tenant, key, leaseToken, step) {
		const started = await glozic.startStep(tenant, key, step.index, leaseToken);
		if (started.duplicate) return true;
		const result = await callWebService(step.config || {}, config);
		await withRetry(() => glozic.reportStep(tenant, key, step.index, leaseToken, result));
		return result.status === 'SUCCEEDED';
	}

	async function runMessageStep(tenant, key, leaseToken, step) {
		// Not retried here: a lost response means the email/SMS outcome is unknown, and glozic records it as such.
		const result = await glozic.executeMessageStep(tenant, key, step.index, leaseToken, step.type);
		return !!result && result.status === 'SUCCEEDED';
	}

	async function runExecution(tenant, execution) {
		await executionSlots.acquire();
		state.inFlight += 1;
		const key = execution.executionKey;
		const leaseToken = execution.leaseToken;
		let canComplete = true;
		try {
			for (const step of execution.steps || []) {
				if (step.status === 'SUCCEEDED' || step.status === 'SKIPPED') continue;
				if (!(step.status === 'PENDING' || (step.status === 'FAILED' && step.retryable))) break;
				if (state.stopping) break;
				const succeeded = step.type === 'CALL_WEB_SERVICE'
					? await runWebServiceStep(tenant, key, leaseToken, step)
					: await runMessageStep(tenant, key, leaseToken, step);
				if (!succeeded) break;
			}
		} catch (err) {
			console.warn(`[scheduler] ${key}: ${err.message}`);
			if (err.statusCode === 409 || err.statusCode === 404) canComplete = false;
		}
		try {
			if (canComplete) {
				const result = await withRetry(() => glozic.completeExecution(tenant, key, leaseToken));
				console.log(`[scheduler] ${key}: ${result.status}${result.reason ? ` (${result.reason})` : ''}`);
			}
		} catch (err) {
			console.warn(`[scheduler] ${key}: could not complete: ${err.message}`);
		} finally {
			state.inFlight -= 1;
			executionSlots.release();
		}
	}

	async function processTenant(tenant) {
		if (state.stopping) return;
		const lockKey = `scheduler:lock:tenant:${tenant}`;
		let lockToken = null;
		try {
			lockToken = await store.acquire(lockKey, config.lockTtlMs);
		} catch (err) {
			console.warn(`[scheduler] lock for ${tenant} unavailable: ${err.message}`);
			return;
		}
		if (!lockToken) return;

		const renewTimer = setInterval(() => {
			store.renew(lockKey, lockToken, config.lockTtlMs)
				.then((ok) => { if (!ok) console.warn(`[scheduler] lost lock for ${tenant}; leases still protect in-flight executions`); })
				.catch(() => {});
		}, Math.floor(config.lockTtlMs / 3));

		try {
			const executions = await glozic.claimDue(tenant, {
				workerId,
				limit: config.batchSize,
				leaseMs: config.leaseMs,
				graceMs: config.catchupGraceMs,
			});
			if (executions.length) console.log(`[scheduler] ${tenant}: claimed ${executions.length} execution(s)`);
			await runPool(executions, config.maxTenantConcurrency, (execution) => runExecution(tenant, execution));
		} catch (err) {
			console.warn(`[scheduler] ${tenant}: claim failed: ${err.message}`);
			if (err.statusCode === 400 || err.statusCode === 403 || err.code === 'INVALID_TENANT') {
				await registry.setActive(tenant, false).catch(() => {});
			}
		} finally {
			clearInterval(renewTimer);
			await store.release(lockKey, lockToken).catch(() => {});
		}
	}

	async function tick() {
		if (!store.isReady()) {
			state.lastTickError = 'Redis not ready; tick skipped';
			return;
		}
		const tenants = await registry.list();
		await runPool(tenants, config.maxConcurrency, processTenant);
		state.lastTickError = '';
	}

	function scheduleNext(delayMs) {
		if (state.stopping) return;
		state.timer = setTimeout(async () => {
			state.currentTick = tick().catch((err) => {
				state.lastTickError = err.message;
				console.warn(`[scheduler] tick failed: ${err.message}`);
			});
			await state.currentTick;
			state.currentTick = null;
			state.lastTickAt = new Date();
			scheduleNext(config.pollIntervalMs);
		}, delayMs);
	}

	return {
		workerId,

		start() {
			if (state.running) return;
			state.running = true;
			console.log(`[scheduler] started as ${workerId}; polling every ${config.pollIntervalMs}ms`);
			state.refreshTimer = setInterval(refreshRegistry, config.registryRefreshMs);
			// Give the Redis client a moment to connect before the first refresh and tick.
			setTimeout(() => refreshRegistry().finally(() => scheduleNext(0)), 2000);
		},

		/** Stops claiming, lets in-flight executions finish up to the grace period, then returns. */
		async stop() {
			if (state.stopping) return;
			state.stopping = true;
			clearTimeout(state.timer);
			clearInterval(state.refreshTimer);
			if (state.currentTick) await Promise.race([state.currentTick, sleep(config.shutdownGraceMs)]);
			state.running = false;
			if (state.inFlight > 0) console.warn(`[scheduler] stopped with ${state.inFlight} execution(s) in flight; they will be recovered after their lease expires`);
		},

		health() {
			return {
				enabled: true,
				workerId,
				running: state.running,
				stopping: state.stopping,
				redisReady: store.isReady(),
				inFlight: state.inFlight,
				pollIntervalMs: config.pollIntervalMs,
				lastTickAt: state.lastTickAt,
				lastTickError: state.lastTickError,
				lastRegistryRefreshAt: state.lastRefreshAt,
				lastRegistryRefreshError: state.lastRefreshError,
			};
		},
	};
}

module.exports = { createCoordinator, Semaphore, runPool };
