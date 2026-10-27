const express = require('express');
const crypto = require('crypto');
const { isDevTenant } = require('../services/scheduler/glozicClient');

module.exports = function schedulerRoutes(app, { config, registry, coordinator }) {
	const router = express.Router();
	router.use('/scheduler', express.json({ limit: '10kb' }));

	const requireInternalToken = (req, res, next) => {
		const expected = Buffer.from(config.internalToken || '');
		const provided = Buffer.from(String(req.headers['x-internal-token'] || ''));
		if (expected.length < 32 || provided.length !== expected.length || !crypto.timingSafeEqual(provided, expected)) {
			return res.status(401).json({ status: false, message: 'Invalid internal token' });
		}
		next();
	};

	// glozic-stagging pushes tenant activity changes here.
	router.post('/scheduler/tenants', requireInternalToken, async (req, res) => {
		const tenant = req.headers.tenant;
		if (!isDevTenant(tenant)) return res.status(400).json({ status: false, message: 'A valid "<tenant>-dev" tenant header is required' });
		if (!registry) return res.status(503).json({ status: false, message: 'Scheduler is disabled' });
		try {
			await registry.setActive(tenant, !!(req.body && req.body.active === true));
			res.json({ status: true });
		} catch (err) {
			res.status(503).json({ status: false, message: `Tenant registry unavailable: ${err.message}` });
		}
	});

	router.get('/scheduler/health', (req, res) => {
		res.json(coordinator ? coordinator.health() : { enabled: false });
	});

	app.use(router);
};
