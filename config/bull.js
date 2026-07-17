require('dotenv').config();


const Bull = require('bull');
const NODE_ENV = process.env.NODE_ENV || 'local';
const REDIS_URL = process.env.REDIS_URL;

// Used to Create Task Bull
const TASK_QUEUE = 'TASK@' + NODE_ENV;
const taskQueue = new Bull(TASK_QUEUE, REDIS_URL);

// Used to Create Workflow / Flow Bull
const WORKFLOW_QUEUE = 'WORKFLOW@' + NODE_ENV;
const FLOW_QUEUE = WORKFLOW_QUEUE;
const workflowQueue = new Bull(WORKFLOW_QUEUE, REDIS_URL);
const flowQueue = workflowQueue;

const connectQueue = (name) => new Bull(name, REDIS_URL);

module.exports = {
	taskQueue,
	workflowQueue,
	flowQueue,
	TASK_QUEUE,
	WORKFLOW_QUEUE,
	FLOW_QUEUE,
	connectQueue,
};
