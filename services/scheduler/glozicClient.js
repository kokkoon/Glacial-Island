const http = require('http');
const https = require('https');
const { URL } = require('url');
const { isDevTenant } = require('./config');

// A claim batch carries up to 25 executions x 5 web-service configs (bodies up to 100KB each).
const MAX_RESPONSE_BYTES = 32 * 1024 * 1024;

class GlozicError extends Error {
	constructor(message, { statusCode = 0, code = '', unknown = false } = {}) {
		super(message);
		this.name = 'GlozicError';
		this.statusCode = statusCode;
		this.code = code;
		// The request may have reached glozic; its effect is not known.
		this.unknown = unknown;
	}
}

/**
 * HTTP client for one glozic environment's internal scheduler API (glozic-stagging or glozic-live-main).
 * Every tenant-scoped call carries a tenant header validated against that environment's tenant rule.
 */
function createGlozicClient(env, config) {
	const base = new URL(env.glozicBaseUrl);
	const transport = base.protocol === 'https:' ? https : http;
	const basePath = base.pathname.replace(/\/+$/, '');

	function request(method, path, { tenant, body, headers = {}, timeoutMs = config.httpTimeoutMs } = {}) {
		if (tenant !== undefined && !env.isTenant(tenant)) {
			return Promise.reject(new GlozicError(`Refusing to call ${env.label} with invalid tenant "${tenant}"`, { code: 'INVALID_TENANT' }));
		}
		const payload = body === undefined ? null : Buffer.from(JSON.stringify(body));
		const requestHeaders = {
			accept: 'application/json',
			'x-internal-token': env.internalToken,
			...headers,
		};
		if (tenant !== undefined) requestHeaders.tenant = tenant;
		if (payload) {
			requestHeaders['content-type'] = 'application/json';
			requestHeaders['content-length'] = payload.length;
		}

		return new Promise((resolve, reject) => {
			let settled = false;
			const finish = (fn, value) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				fn(value);
			};

			const req = transport.request({
				protocol: base.protocol,
				hostname: base.hostname,
				port: base.port || undefined,
				method,
				path: `${basePath}${path}`,
				headers: requestHeaders,
			}, (res) => {
				const chunks = [];
				let size = 0;
				res.on('data', (chunk) => {
					size += chunk.length;
					if (size > MAX_RESPONSE_BYTES) {
						req.destroy();
						finish(reject, new GlozicError('glozic response too large', { statusCode: res.statusCode, unknown: true }));
						return;
					}
					chunks.push(chunk);
				});
				res.on('end', () => {
					let data = null;
					try { data = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch (e) { data = null; }
					if (res.statusCode >= 400 || !data || data.status === false) {
						const routePath = path.split('?')[0].replace(/\/executions\/[^/]+/, '/executions/:key');
						const message = (data && data.message)
							|| (res.statusCode === 404
								? `${env.label} has no route ${method} ${routePath} (HTTP 404); ${env.label} and Glacial-Island may be running different versions`
								: `${env.label} responded with HTTP ${res.statusCode} for ${method} ${routePath}`);
						finish(reject, new GlozicError(String(message), { statusCode: res.statusCode }));
						return;
					}
					finish(resolve, data);
				});
				res.on('error', (err) => finish(reject, new GlozicError(err.message, { code: err.code, unknown: true })));
			});

			const timer = setTimeout(() => {
				req.destroy();
				finish(reject, new GlozicError(`glozic request timed out after ${timeoutMs}ms`, { code: 'TIMEOUT', unknown: true }));
			}, timeoutMs);

			req.on('error', (err) => {
				// Connection-level failures before a response: the call may or may not have been processed.
				const notSent = ['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH'].indexOf(err.code) >= 0;
				finish(reject, new GlozicError(err.message, { code: err.code, unknown: !notSent }));
			});
			if (payload) req.write(payload);
			req.end();
		});
	}

	const stepPath = (key, index) => `/api/internal/scheduler/executions/${encodeURIComponent(key)}/steps/${index}`;

	return {
		/** Registry recovery: the one internal call that is not tenant scoped. */
		listTenants: async () => {
			const data = await request('GET', '/api/internal/scheduler/tenants');
			return (data.tenants || []).filter(env.isTenant);
		},

		claimDue: async (tenant, { workerId, limit, leaseMs, graceMs }) => {
			const data = await request('POST', '/api/internal/scheduler/claim-due', { tenant, body: { workerId, limit, leaseMs, graceMs } });
			return data.executions || [];
		},

		startStep: (tenant, key, index, leaseToken) =>
			request('POST', `${stepPath(key, index)}/start`, { tenant, body: { leaseToken } }),

		reportStep: (tenant, key, index, leaseToken, outcome) =>
			request('POST', `${stepPath(key, index)}/result`, { tenant, body: { leaseToken, ...outcome } }),

		/** Runs a SEND_EMAIL or SEND_SMS step through glozic's internal email / SMS execution API. */
		executeMessageStep: async (tenant, key, index, leaseToken, stepType) => {
			const action = { SEND_EMAIL: 'send-email', SEND_SMS: 'send-sms' }[stepType];
			if (!action) throw new GlozicError(`Unsupported message step type ${stepType}`, { code: 'INVALID_STEP' });
			const data = await request('POST', `${stepPath(key, index)}/${action}`, {
				tenant,
				body: { leaseToken },
				headers: { 'idempotency-key': `${key}#${index}` },
			});
			return data.step;
		},

		completeExecution: async (tenant, key, leaseToken) => {
			const data = await request('POST', `/api/internal/scheduler/executions/${encodeURIComponent(key)}/complete`, { tenant, body: { leaseToken } });
			return data.execution;
		},
	};
}

module.exports = { createGlozicClient, GlozicError, isDevTenant };
