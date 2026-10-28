const http = require('http');
const https = require('https');
const { resolveTarget, BlockedTargetError } = require('./ssrfGuard');

const SCHEDULER_ORIGIN_HEADER = 'x-glozic-scheduler-origin';
const MAX_STORED_BODY_CHARS = 64 * 1024;
const DROPPED_HEADERS = ['host', 'content-length', 'transfer-encoding', 'connection', 'x-internal-token', SCHEDULER_ORIGIN_HEADER];
const NOT_SENT_CODES = ['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH'];
const RETRYABLE_STATUS = [429, 502, 503, 504];

const outcome = (status, retryable, error, result = null) => ({ status, retryable, error, result });

const storedBody = (buffer, truncated) => {
	const text = buffer.toString('utf8');
	try {
		if (!truncated) return { body: JSON.parse(text), bodyTruncated: false };
	} catch (e) { /* not JSON */ }
	return { body: text.slice(0, MAX_STORED_BODY_CHARS), bodyTruncated: truncated || text.length > MAX_STORED_BODY_CHARS };
};

/**
 * Executes one CALL_WEB_SERVICE step. Redirects are not followed; responses are size-capped.
 * Returns { status: SUCCEEDED | FAILED | UNKNOWN, retryable, error, result }.
 */
async function callWebService(stepConfig, config) {
	const method = String(stepConfig.method || 'GET').toUpperCase();
	let target;
	try {
		target = await resolveTarget(stepConfig.url, { allowPrivateHosts: config.webServiceAllowPrivateHosts, selfPort: config.port });
	} catch (err) {
		if (err instanceof BlockedTargetError) return outcome('FAILED', false, err.message);
		return outcome('FAILED', NOT_SENT_CODES.indexOf(err.code) >= 0, `DNS lookup failed: ${err.message}`);
	}

	const headers = {};
	Object.keys(stepConfig.headers || {}).forEach((name) => {
		if (DROPPED_HEADERS.indexOf(name.toLowerCase()) < 0) headers[name] = String(stepConfig.headers[name]);
	});
	headers[SCHEDULER_ORIGIN_HEADER] = '1';
	if (!Object.keys(headers).some((h) => h.toLowerCase() === 'user-agent')) headers['user-agent'] = 'Glacial-Island-Scheduler/1.0';

	const body = stepConfig.body && method !== 'GET' ? Buffer.from(String(stepConfig.body)) : null;
	if (body) {
		headers['content-length'] = body.length;
		if (!Object.keys(headers).some((h) => h.toLowerCase() === 'content-type')) {
			headers['content-type'] = /^\s*[{[]/.test(String(stepConfig.body)) ? 'application/json' : 'text/plain';
		}
	}

	const transport = target.url.protocol === 'https:' ? https : http;
	const startedAt = Date.now();

	return new Promise((resolve) => {
		let settled = false;
		let responseStarted = false;
		const finish = (value) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			resolve(value);
		};

		const req = transport.request({
			protocol: target.url.protocol,
			hostname: target.hostname,
			port: target.url.port || undefined,
			path: `${target.url.pathname}${target.url.search}`,
			method,
			headers,
			// Connect to the address that passed validation (prevents DNS rebinding between check and use).
			lookup: (hostname, options, callback) => callback(null, target.address, target.family),
		}, (res) => {
			responseStarted = true;
			const chunks = [];
			let size = 0;
			let truncated = false;
			const done = () => {
				const statusCode = res.statusCode;
				const result = { statusCode, durationMs: Date.now() - startedAt, ...storedBody(Buffer.concat(chunks), truncated) };
				if (statusCode >= 200 && statusCode < 300) return finish(outcome('SUCCEEDED', false, '', result));
				if (statusCode >= 300 && statusCode < 400) return finish(outcome('FAILED', false, `HTTP ${statusCode}: redirects are not followed`, result));
				finish(outcome('FAILED', RETRYABLE_STATUS.indexOf(statusCode) >= 0, `HTTP ${statusCode}`, result));
			};
			res.on('data', (chunk) => {
				if (truncated) return;
				size += chunk.length;
				if (size > config.webServiceMaxResponseBytes) {
					truncated = true;
					chunks.push(chunk.slice(0, chunk.length - (size - config.webServiceMaxResponseBytes)));
					res.destroy();
					done();
					return;
				}
				chunks.push(chunk);
			});
			res.on('end', done);
			res.on('error', () => { if (!truncated) done(); });
		});

		const timer = setTimeout(() => {
			req.destroy();
			const message = `Timed out after ${config.webServiceTimeoutMs}ms`;
			// A GET is safe to repeat; for other methods the remote side may already have acted.
			finish(method === 'GET' ? outcome('FAILED', true, message) : outcome('UNKNOWN', false, `${message}; the remote outcome is unknown`));
		}, config.webServiceTimeoutMs);

		req.on('error', (err) => {
			if (NOT_SENT_CODES.indexOf(err.code) >= 0) return finish(outcome('FAILED', true, `Connection failed: ${err.message}`));
			if (method === 'GET' && !responseStarted) return finish(outcome('FAILED', true, `Request failed: ${err.message}`));
			finish(outcome('UNKNOWN', false, `Request failed after sending: ${err.message}; the remote outcome is unknown`));
		});

		if (body) req.write(body);
		req.end();
	});
}

module.exports = { callWebService, SCHEDULER_ORIGIN_HEADER };
