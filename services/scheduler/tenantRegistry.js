/**
 * Set of tenants with active schedules for one glozic environment (its own Redis key).
 * glozic pushes changes; a periodic refresh pulls the full list back so the set heals after Redis loss or missed pushes.
 */
function createTenantRegistry(store, glozic, env) {
	const key = env.registryKey;
	return {
		list: () => store.run('smembers', key),

		setActive: (tenant, active) => store.run(active ? 'sadd' : 'srem', key, tenant),

		async refresh() {
			const tenants = await glozic.listTenants();
			const multi = store.client.multi();
			multi.del(key);
			if (tenants.length) multi.sadd(key, ...tenants);
			await new Promise((resolve, reject) => multi.exec((err, result) => (err ? reject(err) : resolve(result))));
			return tenants.length;
		},
	};
}

module.exports = { createTenantRegistry };
