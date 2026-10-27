const config = require('./config');
const { createSchedulerRedis } = require('./redisLock');
const { createGlozicClient } = require('./glozicClient');
const { createTenantRegistry } = require('./tenantRegistry');
const { createCoordinator } = require('./coordinator');
const schedulerRoutes = require('../../routes/schedulerRoutes');

/** Mounts scheduler routes and starts the single scheduler loop. Returns a handle with stop(), or null when disabled. */
function startScheduler(app) {
	if (!config.enabled) {
		console.log('[scheduler] disabled (set SCHEDULER_ENABLED=true to enable)');
		schedulerRoutes(app, { config });
		return null;
	}
	const problems = config.problems();
	if (problems.length) {
		console.error(`[scheduler] not started: ${problems.join('; ')}`);
		schedulerRoutes(app, { config });
		return null;
	}

	const store = createSchedulerRedis(config.redisUrl);
	const glozic = createGlozicClient(config);
	const registry = createTenantRegistry(store, glozic);
	const coordinator = createCoordinator({ config, store, glozic, registry });
	schedulerRoutes(app, { config, registry, coordinator });
	coordinator.start();

	return {
		async stop() {
			await coordinator.stop();
			await store.quit();
		},
	};
}

module.exports = { startScheduler };
