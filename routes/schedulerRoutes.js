const express = require('express');
const crypto = require('crypto');

module.exports = function schedulerRoutes(app, { config, runtimes = {} }) {
	const router = express.Router();
	router.use('/scheduler', express.json({ limit: '10kb' }));

	const requireInternalToken = (env) => (req, res, next) => {
		const expected = Buffer.from(env.internalToken || '');
		const provided = Buffer.from(String(req.headers['x-internal-token'] || ''));
		if (expected.length < 32 || provided.length !== expected.length || !crypto.timingSafeEqual(provided, expected)) {
			return res.status(401).json({ status: false, message: 'Invalid internal token' });
		}
		next();
	};

	// Tenant activity pushes, one route per glozic environment:
	//   glozic-stagging  -> POST /scheduler/tenants        ("<tenant>-dev")
	//   glozic-live-main -> POST /scheduler/live/tenants   (Live tenant)
	const tenantRoute = (env) => async (req, res) => {
		const runtime = runtimes[env.name];
		const tenant = req.headers.tenant;
		if (!env.isTenant(tenant)) return res.status(400).json({ status: false, message: `A valid ${env.tenantRule} tenant header is required` });
		if (!runtime) return res.status(503).json({ status: false, message: `Scheduler for ${env.label} is disabled` });
		try {
			await runtime.registry.setActive(tenant, !!(req.body && req.body.active === true));
			res.json({ status: true });
		} catch (err) {
			res.status(503).json({ status: false, message: `Tenant registry unavailable: ${err.message}` });
		}
	};

	const paths = { staging: '/scheduler/tenants', live: '/scheduler/live/tenants' };
	config.environments.forEach((env) => {
		router.post(paths[env.name], requireInternalToken(env), tenantRoute(env));
	});

	router.get('/scheduler/health', (req, res) => {
		const environments = {};
		config.environments.forEach((env) => {
			const runtime = runtimes[env.name];
			environments[env.name] = runtime
				? runtime.coordinator.health()
				: { enabled: false, glozic: env.label, reason: config.enabled ? config.environmentProblems(env).join('; ') : 'SCHEDULER_ENABLED is off' };
		});
		res.json({ enabled: Object.keys(runtimes).length > 0, environments });
	});

	app.use(router);
};
