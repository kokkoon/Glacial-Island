const dns = require('dns');
const net = require('net');
const { URL } = require('url');

class BlockedTargetError extends Error {
	constructor(message) {
		super(message);
		this.name = 'BlockedTargetError';
	}
}

const BLOCKED_IPV4_CIDRS = [
	['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8], ['169.254.0.0', 16],
	['172.16.0.0', 12], ['192.0.0.0', 24], ['192.0.2.0', 24], ['192.168.0.0', 16], ['198.18.0.0', 15],
	['198.51.100.0', 24], ['203.0.113.0', 24], ['224.0.0.0', 4], ['240.0.0.0', 4],
];

const ipv4ToInt = (ip) => ip.split('.').reduce((acc, part) => ((acc << 8) + parseInt(part, 10)) >>> 0, 0);

const inCidr = (ip, [range, bits]) => {
	const mask = bits === 0 ? 0 : (~0 << (32 - bits)) >>> 0;
	return (ipv4ToInt(ip) & mask) === (ipv4ToInt(range) & mask);
};

const isBlockedIPv4 = (ip) => BLOCKED_IPV4_CIDRS.some((cidr) => inCidr(ip, cidr));

const isBlockedIPv6 = (ip) => {
	const addr = ip.toLowerCase().split('%')[0];
	if (addr === '::' || addr === '::1') return true;
	const mapped = addr.match(/^(?:::ffff:|64:ff9b::)(\d+\.\d+\.\d+\.\d+)$/);
	if (mapped) return isBlockedIPv4(mapped[1]);
	if (/^::ffff:[0-9a-f]{1,4}:[0-9a-f]{1,4}$/.test(addr)) return true;
	const first = parseInt(addr.split(':')[0] || '0', 16);
	if ((first & 0xfe00) === 0xfc00) return true; // fc00::/7 unique local
	if ((first & 0xffc0) === 0xfe80) return true; // fe80::/10 link local
	if ((first & 0xff00) === 0xff00) return true; // ff00::/8 multicast
	if (addr.indexOf('2001:db8:') === 0) return true; // documentation
	return false;
};

const isBlockedAddress = (ip) => (net.isIPv4(ip) ? isBlockedIPv4(ip) : isBlockedIPv6(ip));

const lookupAll = (hostname) => new Promise((resolve, reject) => {
	dns.lookup(hostname, { all: true, verbatim: true }, (err, addresses) => (err ? reject(err) : resolve(addresses)));
});

/**
 * Validates an outbound web-service URL and resolves it to a pinned address.
 * Every resolved address must be public unless the hostname is explicitly allowlisted.
 */
async function resolveTarget(rawUrl, { allowPrivateHosts = [], selfPort } = {}) {
	let url;
	try { url = new URL(String(rawUrl)); } catch (e) { throw new BlockedTargetError('URL is not valid'); }
	if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new BlockedTargetError('Only http and https URLs are allowed');
	if (url.username || url.password) throw new BlockedTargetError('Credentials in the URL are not allowed');
	if (/^\/api\/internal\/scheduler(\/|$)/i.test(decodeURIComponent(url.pathname).replace(/\/{2,}/g, '/'))) {
		throw new BlockedTargetError('Scheduler internal endpoints cannot be called from a scheduled action');
	}

	const hostname = url.hostname.replace(/^\[|\]$/g, '').toLowerCase();
	const port = url.port || (url.protocol === 'https:' ? '443' : '80');
	const addresses = net.isIP(hostname)
		? [{ address: hostname, family: net.isIP(hostname) }]
		: await lookupAll(hostname);
	if (!addresses.length) throw new BlockedTargetError(`Host ${hostname} did not resolve`);

	const allowPrivate = allowPrivateHosts.indexOf(hostname) >= 0;
	for (const { address } of addresses) {
		const loopback = address === '::1' || /^127\./.test(address) || address === '0.0.0.0' || address === '::';
		if (loopback && String(port) === String(selfPort)) {
			throw new BlockedTargetError('Scheduled actions cannot call Glacial-Island itself');
		}
		if (!allowPrivate && isBlockedAddress(address)) {
			throw new BlockedTargetError(`Host ${hostname} resolves to a private or reserved address`);
		}
	}
	return { url, hostname, address: addresses[0].address, family: addresses[0].family };
}

module.exports = { resolveTarget, isBlockedAddress, BlockedTargetError };
