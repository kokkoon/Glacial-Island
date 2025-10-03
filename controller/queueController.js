const Queue = require("bull");
const Redis = require("ioredis");
const keys = require('../config/keys');

// Use redisURL directly
const redis = new Redis(keys.redisURL);

// Helper: Get all unique queue names
async function getAllQueueNames() {
    const keysList = await redis.keys("bull:*:id");
    const queueNames = keysList.map(k => k.split(":")[1]);
    return [...new Set(queueNames)];
}

// ===================
// Controllers
// ===================

// 1. Get all queues with job counts per status
exports.getQueues = async (req, res) => {
    try {
        const queueNames = await getAllQueueNames();
        const countsArr = await Promise.all(
            queueNames.map(async name => {
                const q = new Queue(name, { redis: keys.redisURL });
                const counts = await q.getJobCounts();
                return { name, counts };
            })
        );
        res.json(countsArr);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
};

// 2. Get all jobs of a particular queue
exports.getJobs = async (req, res) => {
    try {
        const { queueName } = req.params;
        const q = new Queue(queueName, { redis: keys.redisURL });

        const jobs = await q.getJobs(
            ["waiting", "active", "completed", "failed", "delayed"],
            0,
            50
        );

        const jobsData = await Promise.all(
            jobs.map(async job => ({
                id: job.id,
                status: await job.getState(),
                attemptsMade: job.attemptsMade,
                cron: job.opts.repeat?.cron || null,
            }))
        );

        res.json({ queueName, jobs: jobsData });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
};

// 3. Get details of a particular job by ID in a queue
exports.getJobById = async (req, res) => {
    try {
        const { queueName, jobId } = req.params;
        const q = new Queue(queueName, { redis: keys.redisURL });
        const job = await q.getJob(jobId);

        if (!job) {
            return res.status(404).json({ error: "Job not found" });
        }

        const jobData = {
            id: job.id,
            status: await job.getState(),
            attemptsMade: job.attemptsMade,
            cron: job.opts.repeat?.cron || null,
            nextRun: job.opts.repeat?.next ? new Date(job.opts.repeat.next).toString() : null,
            finishedOn: job.finishedOn ? new Date(job.finishedOn).toString() : null,
            failedReason: job.failedReason || null,
            data: job.data || null,
        };

        res.json(jobData);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
};
