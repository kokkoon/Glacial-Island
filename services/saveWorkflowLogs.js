const request = require('request-promise');
const utility = require('../utils/utility');
const { flowQueue } = require('../config/bull');

const getTenantHost = (tenant) => {
	const tenantSlug = String(tenant || '').replace(/-dev$/i, '');
	const domain = utility.IsCheckDevTenant(tenant || '') ? 'glozic.dev' : 'glozic.com';
	return `https://${tenantSlug}.${domain}/api`;
};

const parseJobLogEntry = (entry, sequence) => {
	try {
		const parsed = typeof entry === 'string' ? JSON.parse(entry) : entry;
		return {
			sequence,
			actionName: parsed.activity || parsed.actionName || '',
			start: parsed.start || parsed.timestamp || null,
			end: parsed.end || null,
			detail: parsed.log || parsed.detail || '',
			status: parsed.status || null,
			actionId: parsed.actionId || null,
			...parsed,
		};
	} catch (err) {
		return {
			sequence,
			actionName: '',
			start: null,
			end: null,
			detail: String(entry),
		};
	}
};

/**
 * Persist workflow job outcome via tenant saveWorkflowLogs API.
 */
const saveWorkflowLogs = async (job, outcome, error = null) => {
	try {
		const tenant = job?.data?.tenant;
		if (!tenant) {
			console.log('saveWorkflowLogs skipped: tenant missing on job', job?.id);
			return null;
		}

		const tenantSlug = String(tenant).replace(/-dev$/i, '') || tenant;

		const isFailed = outcome === 'failed' || outcome === 'Failed';
		const workflowId =
			job?.data?._id ||
			job?.data?.workflowId ||
			job?.data?.moduleId ||
			(typeof job?.id === 'string' ? job.id.split('-')[0] : '') ||
			'';

		const jobLogsResult = await flowQueue.getJobLogs(job.id);

		const logs = (jobLogsResult?.logs || []).map((entry, index) =>
			parseJobLogEntry(entry, index + 1)
		);

		const logPayload = {
			level: isFailed ? 'error' : 'info',
			message: isFailed
				? (error?.message || job?.failedReason || 'Workflow failed')
				: `Workflow ${job?.data?.name || workflowId || job?.id} completed`,
			optionId: 4,
			module_type: 'workflow',
			module_name: job?.data?.name || job?.data?.workflowName || '',
			id: workflowId || String(job?.id || ''),
			moduleId: workflowId || String(job?.id || ''),
			appId: job?.data?.appId || '',
			userId: job?.data?.userId || '',
			isStreamLog: false,
			status: isFailed ? 'failed' : 'completed',
			status_code: isFailed ? 500 : 200,
			logDetails: isFailed
				? (error?.stack || error?.message || job?.failedReason || '')
				: '',
			log_data: {
				instanceId: job?.id,
				status: job?.data?.state || (isFailed ? 'Failed' : 'Completed'),
				workflowName: job?.data?.name || job?.data?.workflowName || '',
				description: job?.data?.description || '',
				start: job?.data?.start || job?.data?.jobStart || null,
				end: job?.data?.end || job?.data?.jobEnd || null,
				jobId: job?.id,
				state: job?.data?.state,
				result: job?.returnvalue || null,
				error: isFailed ? (error?.message || job?.failedReason || null) : null,
				count: jobLogsResult?.count ?? logs.length,
				logs,
			},
			tenant: tenant,
			metadata: {
				tenant: tenant,
				jobId: job?.id,
				queue: job?.queue?.name,
			},
		};

		const url = `${getTenantHost(tenant)}/saveWorkflowLogs`;
		// const url = `http://constantsys.localhost:5000/api/saveWorkflowLogs`;
		const options = {
			method: 'POST',
			url,
			headers: {
				tenant: tenant,
			},
			body: { logs: [logPayload] },
			json: true,
		};

		const res = await request(options);
		console.log('saveWorkflowLogs ok:', job?.id, outcome, res?.count ?? res);
		return res;
	} catch (err) {
		console.log('saveWorkflowLogs error:', job?.id, err?.message || err);
		return null;
	}
};

module.exports = {
	saveWorkflowLogs,
	getTenantHost,
};
