const { taskQueue, TASK_QUEUE } = require('../config/bull');

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
