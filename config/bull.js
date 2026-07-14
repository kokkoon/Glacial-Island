const Bull = require('bull');
const NODE_ENV = process.env.NODE_ENV || 'local';
const REDIS_URL = process.env.REDIS_URL;

// Used to Create Task Bull
const TASK_QUEUE = 'TASK@' + NODE_ENV;
const taskQueue = new Bull(TASK_QUEUE, REDIS_URL);

// Used to Create Workflow / Flow Bull
const WORKFLOW_QUEUE = process.env.WORKFLOW_FLOW_QUEUE || ('WORKFLOW@' + NODE_ENV);
const FLOW_QUEUE = WORKFLOW_QUEUE;
const workflowQueue = new Bull(WORKFLOW_QUEUE, REDIS_URL);
const flowQueue = workflowQueue;

// Used to Create Email Bull
const EMAIL_QUEUE = 'EMAIL@' + NODE_ENV;
const emailQueue = new Bull(EMAIL_QUEUE, REDIS_URL);

// Used to Create Logs Bull
const LOGS_QUEUE = 'Logs@' + NODE_ENV;
const logQueue = new Bull(LOGS_QUEUE, REDIS_URL);

// Used to Create Messenger Bull
const MSG_QUEUE = 'MESSENGER@' + NODE_ENV;
const msgQueue = new Bull(MSG_QUEUE, REDIS_URL);

const connectQueue = (name) => new Bull(name, REDIS_URL);

module.exports = {
	taskQueue,
	workflowQueue,
	flowQueue,
	emailQueue,
	logQueue,
	msgQueue,
	TASK_QUEUE,
	WORKFLOW_QUEUE,
	FLOW_QUEUE,
	EMAIL_QUEUE,
	LOGS_QUEUE,
	MSG_QUEUE,
	connectQueue,
};
