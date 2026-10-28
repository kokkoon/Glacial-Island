const config = require('./config');
const { createSchedulerRedis } = require('./redisLock');
const { createGlozicClient } = require('./glozicClient');
const { createTenantRegistry } = require('./tenantRegistry');
const { createCoordinator } = require('./coordinator');
const schedulerRoutes = require('../../routes/schedulerRoutes');

/**
 * Mounts scheduler routes and starts one independent scheduler loop per configured glozic environment
 * (glozic-stagging and glozic-live-main). Returns a handle with stop(), or null when nothing runs.
 */
function startScheduler(app) {
	if (!config.enabled) {
		console.log('[scheduler] disabled (set SCHEDULER_ENABLED=true to enable)');
		schedulerRoutes(app, { config, runtimes: {} });
		return null;
	}
	const problems = config.problems();
	if (problems.length) {
		console.error(`[scheduler] not started: ${problems.join('; ')}`);
		schedulerRoutes(app, { config, runtimes: {} });
		return null;
	}

	const store = createSchedulerRedis(config.redisUrl);
	const runtimes = {};
	config.environments.forEach((env) => {
		const envProblems = config.environmentProblems(env);
		if (envProblems.length) {
			console.log(`[scheduler:${env.name}] not started: ${envProblems.join('; ')}`);
			return;
		}
		const glozic = createGlozicClient(env, config);
		const registry = createTenantRegistry(store, glozic, env);
		const coordinator = createCoordinator({ env, config, store, glozic, registry });
		runtimes[env.name] = { env, registry, coordinator };
	});

	schedulerRoutes(app, { config, runtimes });
	const running = Object.values(runtimes);
	if (!running.length) {
		console.error('[scheduler] no environment could be started');
		store.quit();
		return null;
	}
	running.forEach((r) => r.coordinator.start());

	return {
		async stop() {
			await Promise.all(running.map((r) => r.coordinator.stop()));
			await store.quit();
		},
	};
}

module.exports = { startScheduler };
