const Bull = require('bull');
const keys = require('../config/keys');
const NODE_ENV = process.env.NODE_ENV || 'local';
const TASK_QUEUE = 'TASK@' + NODE_ENV;
const taskQueue = new Bull(TASK_QUEUE, keys.redisURL);

const createTask = async (taskData, options = {}) => {
  const jobOptions = {
    removeOnComplete: false,
    removeOnFail: false,
    ...options
  };

  return taskQueue.add(taskData, jobOptions);
};

const updateTask = async (taskJob, updates = {}) => {
  if (!taskJob) {
    return null;
  }

  const nextData = {
    ...(taskJob.data || {}),
    ...updates,
    updatedAt: Date.now()
  };

  await taskJob.update(nextData);
  return nextData;
};

const setTaskStatus = async (taskJob, status, extra = {}) => {
  return updateTask(taskJob, { status, ...extra });
};

module.exports = {
  TASK_QUEUE,
  taskQueue,
  createTask,
  updateTask,
  setTaskStatus
};
