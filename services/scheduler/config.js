const int = (name, fallback, min, max) => {
	const n = parseInt(process.env[name], 10);
	if (isNaN(n)) return fallback;
	return Math.min(Math.max(n, min), max);
};

const bool = (name, fallback) => {
	const v = process.env[name];
	if (v == null || v === '') return fallback;
	return /^(1|true|yes|on)$/i.test(v);
};

const list = (name) => (process.env[name] || '')
	.split(',')
	.map((s) => s.trim().toLowerCase())
	.filter(Boolean);

const url = (name) => (process.env[name] || '').replace(/\/+$/, '');

const DEV_TENANT = /^[a-z0-9][a-z0-9-]*-dev$/i;
const LIVE_TENANT = /^[a-z0-9][a-z0-9-]*$/i;

const isDevTenant = (tenant) => typeof tenant === 'string' && DEV_TENANT.test(tenant);
const isLiveTenant = (tenant) => typeof tenant === 'string' && LIVE_TENANT.test(tenant) && !/-dev$/i.test(tenant);

const sharedToken = process.env.SCHEDULER_INTERNAL_TOKEN || '';

/**
 * Each glozic environment is scheduled independently: its own glozic base URL and token,
 * tenant rule, Redis registry and lock keys, poll loop and concurrency pool.
 * Staging keeps the original Redis keys so existing deployments carry over unchanged.
 */
const environments = [
	{
		name: 'staging',
		label: 'glozic-stagging',
		baseUrlVar: 'GLOZIC_STAGGING_BASE_URL',
		glozicBaseUrl: url('GLOZIC_STAGGING_BASE_URL'),
		internalToken: process.env.SCHEDULER_STAGING_INTERNAL_TOKEN || sharedToken,
		enabled: bool('SCHEDULER_STAGING_ENABLED', true),
		isTenant: isDevTenant,
		tenantRule: '"<tenant>-dev"',
		registryKey: 'scheduler:tenants',
		lockPrefix: 'scheduler:lock:tenant:',
	},
	{
		name: 'live',
		label: 'glozic-live-main',
		baseUrlVar: 'GLOZIC_LIVE_BASE_URL',
		glozicBaseUrl: url('GLOZIC_LIVE_BASE_URL'),
		internalToken: process.env.SCHEDULER_LIVE_INTERNAL_TOKEN || sharedToken,
		enabled: bool('SCHEDULER_LIVE_ENABLED', true),
		isTenant: isLiveTenant,
		tenantRule: 'Live (without "-dev")',
		registryKey: 'scheduler:live:tenants',
		lockPrefix: 'scheduler:live:lock:tenant:',
	},
];

const config = {
	enabled: bool('SCHEDULER_ENABLED', false),
	port: String(process.env.PORT || '4000'),
	redisUrl: process.env.REDIS_URL || '',
	pollIntervalMs: int('SCHEDULER_POLL_INTERVAL_MS', 15000, 5000, 300000),
	batchSize: int('SCHEDULER_BATCH_SIZE', 10, 1, 25),
	maxConcurrency: int('SCHEDULER_MAX_CONCURRENCY', 5, 1, 20),
	maxTenantConcurrency: int('SCHEDULER_MAX_TENANT_CONCURRENCY', 2, 1, 10),
	lockTtlMs: int('SCHEDULER_LOCK_TTL_MS', 60000, 10000, 600000),
	leaseMs: int('SCHEDULER_LEASE_MS', 300000, 30000, 900000),
	httpTimeoutMs: int('SCHEDULER_HTTP_TIMEOUT_MS', 30000, 1000, 120000),
	webServiceTimeoutMs: int('SCHEDULER_WEBSERVICE_TIMEOUT_MS', 15000, 1000, 60000),
	webServiceMaxResponseBytes: int('SCHEDULER_WEBSERVICE_MAX_RESPONSE_BYTES', 1048576, 1024, 10485760),
	webServiceAllowPrivateHosts: list('SCHEDULER_WEBSERVICE_ALLOW_PRIVATE_HOSTS'),
	catchupGraceMs: int('SCHEDULER_CATCHUP_GRACE_MS', 3600000, 0, 86400000),
	registryRefreshMs: int('SCHEDULER_REGISTRY_REFRESH_MS', 300000, 30000, 3600000),
	shutdownGraceMs: int('SCHEDULER_SHUTDOWN_GRACE_MS', 20000, 1000, 120000),
	environments,
};

/** Configuration problems that prevent the whole scheduler from starting. */
config.problems = () => {
	const problems = [];
	if (!config.redisUrl) problems.push('REDIS_URL is required');
	if (!environments.some((env) => env.enabled && env.glozicBaseUrl)) {
		problems.push('set GLOZIC_STAGGING_BASE_URL and/or GLOZIC_LIVE_BASE_URL (for example http://localhost:5000)');
	}
	return problems;
};

/** Problems that keep one environment from running; other environments still start. Empty when it should run. */
config.environmentProblems = (env) => {
	const problems = [];
	if (!env.enabled) return ['disabled'];
	if (!env.glozicBaseUrl) return [`${env.baseUrlVar} is not set`];
	if (!/^https?:\/\/[^/]+/i.test(env.glozicBaseUrl)) problems.push(`${env.baseUrlVar} must be an http(s) URL (for example http://localhost:5000)`);
	if (env.internalToken.length < 32) problems.push(`SCHEDULER_${env.name.toUpperCase()}_INTERNAL_TOKEN or SCHEDULER_INTERNAL_TOKEN must be at least 32 characters`);
	return problems;
};

config.isDevTenant = isDevTenant;
config.isLiveTenant = isLiveTenant;

module.exports = config;
