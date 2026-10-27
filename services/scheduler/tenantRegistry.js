const REGISTRY_KEY = 'scheduler:tenants';

/**
 * Set of dev tenants that currently have active schedules. glozic pushes changes;
 * a periodic refresh pulls the full list back so the set heals after Redis loss or missed pushes.
 */
function createTenantRegistry(store, glozic) {
	return {
		list: () => store.run('smembers', REGISTRY_KEY),

		setActive: (tenant, active) => store.run(active ? 'sadd' : 'srem', REGISTRY_KEY, tenant),

		async refresh() {
			const tenants = await glozic.listTenants();
			const multi = store.client.multi();
			multi.del(REGISTRY_KEY);
			if (tenants.length) multi.sadd(REGISTRY_KEY, ...tenants);
			await new Promise((resolve, reject) => multi.exec((err, result) => (err ? reject(err) : resolve(result))));
			return tenants.length;
		},
	};
}

module.exports = { createTenantRegistry, REGISTRY_KEY };
