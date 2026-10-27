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

const config = {
	enabled: bool('SCHEDULER_ENABLED', false),
	port: String(process.env.PORT || '4000'),
	glozicBaseUrl: (process.env.GLOZIC_STAGGING_BASE_URL || '').replace(/\/+$/, ''),
	internalToken: process.env.SCHEDULER_INTERNAL_TOKEN || '',
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
};

/** Configuration problems that prevent the scheduler from starting. */
config.problems = () => {
	const problems = [];
	if (config.internalToken.length < 32) problems.push('SCHEDULER_INTERNAL_TOKEN must be at least 32 characters');
	if (!/^https?:\/\/[^/]+/i.test(config.glozicBaseUrl)) problems.push('GLOZIC_STAGGING_BASE_URL must be an http(s) URL (for example http://localhost:5000)');
	if (!config.redisUrl) problems.push('REDIS_URL is required');
	return problems;
};

module.exports = config;
