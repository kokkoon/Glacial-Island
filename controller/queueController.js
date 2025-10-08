const Queue = require("bull");
const Redis = require("ioredis");
const keys = require("../config/keys");

// ===============================
// Redis Connection
// ===============================
const redis = new Redis(keys.redisURL);
// Helper: Get all unique queue names
async function getAllQueueNames() {
    try {
        const keysList = await redis.keys("bull:*:id");
        const queueNames = keysList.map(k => k.split(":")[1]);
        return [...new Set(queueNames)];
    } catch (err) {
        console.error("Error fetching queue names:", err);
        return [];
    }
}

// ===============================
// Controllers
// ===============================

// 1️⃣ Get all queues with job counts per status (only jobs matching tenant)
exports.getQueues = async (req, res) => {
    try {
        const tenant = req.headers.tenant;
        if (!tenant) {
            return res.status(400).json({ error: "Tenant header required" });
        }

        const queueNames = await getAllQueueNames();

        const countsArr = await Promise.all(
            queueNames.map(async (name) => {
                try {
                    const q = new Queue(name, { redis: keys.redisURL });

                    // Get all jobs and remove null entries
                    const allJobs = (await q.getJobs([
                        "waiting",
                        "active",
                        "completed",
                        "failed",
                        "delayed",
                    ])).filter(j => j);

                    const counts = allJobs.reduce(
                        (acc, job) => {
                            const state = job.finishedOn
                                ? "completed"
                                : job.opts.repeat
                                    ? "delayed"
                                    : job.state || "waiting";

                            acc[state] = (acc[state] || 0) + 1;
                            return acc;
                        },
                        {
                            waiting: 0,
                            active: 0,
                            completed: 0,
                            failed: 0,
                            delayed: 0,
                            paused: 0,
                        }
                    );

                    return { name, counts };
                } catch (innerErr) {
                    console.error(`Error processing queue "${name}":`, innerErr);
                    return { name, error: innerErr.message };
                }
            })
        );

        // 🧠 Tenant-based filtering
        let filteredQueues;
        if (tenant.includes("-dev")) {
            // Tenant has "-dev" → show only .dev queues
            filteredQueues = countsArr.filter((q) =>
                ["LOGS@glozic.dev", "SCHEDULE@glozic.dev"].includes(q.name)
            );
        } else {
            // Tenant does NOT have "-dev" → show only .com queues
            filteredQueues = countsArr.filter((q) =>
                ["LOGS@glozic.com", "SCHEDULE@glozic.com"].includes(q.name)
            );
        }

        res.json(filteredQueues);
    } catch (err) {
        console.error("getQueues error:", err);
        res.status(500).json({ error: err.message });
    }
};


exports.getJobs = async (req, res) => {
    try {
        const tenant = req.headers.tenant;
        if (!tenant) return res.status(400).json({ error: "Tenant header required" });
        const { queueName } = req.params;
        const q = new Queue(queueName, { redis: keys.redisURL });

        const jobs = (await q.getJobs(
            ["waiting", "active", "completed", "failed", "delayed"],
            0,
            50
        ))
        const jobsData = await Promise.all(
            jobs
                .filter(job => job.data?.tenant == tenant)
                .map(async job => ({
                    id: job.id,
                    status: await job.getState(),
                    attemptsMade: job.attemptsMade,
                    cron: job.opts.repeat?.cron || null,
                }))
        );

        res.json({ queueName, jobs: jobsData });
    } catch (err) {
        console.error("getJobs error:", err);
        res.status(500).json({ error: err.message });
    }
};

exports.getJobById = async (req, res) => {
    try {
        const tenant = req.headers.tenant;
        if (!tenant) return res.status(400).json({ error: "Tenant header required" });

        const { queueName, jobId } = req.params;
        const q = new Queue(queueName, { redis: keys.redisURL });

        const job = await q.getJob(jobId);
        if (!job) return res.status(404).json({ error: "Job not found or deleted" });

        if (job?.data?.tenant != tenant) {
            return res.status(403).json({ error: "Access denied for this tenant" });
        }

        const jobData = {
            id: job.id,
            status: await job.getState(),
            attemptsMade: job.attemptsMade,
            cron: job.opts.repeat?.cron || null,
            nextRun: job.opts.repeat?.next ? new Date(job.opts.repeat.next).toString() : null,
            finishedOn: job.finishedOn ? new Date(job.finishedOn).toString() : null,
            failedReason: job.failedReason || null,
            data: job?.data || null,
        };

        res.json(jobData);
    } catch (err) {
        console.error("getJobById error:", err);
        res.status(500).json({ error: err.message });
    }
};


exports.getAllRepeatableJobs = async (req, res) => {
    try {
        const queueNames = await getAllQueueNames();
        const repeatableJobs = [];

        for (const name of queueNames) {
            const q = new Queue(name, { redis: keys.redisURL });
            const repeats = await q.getRepeatableJobs();

            for (const r of repeats) {
                // Get all past jobs generated by this repeatable job

                repeatableJobs.push({
                    queue: name,
                    key: r.key,
                    cron: r.cron || null,
                    nextRunAt: r.next ? new Date(r.next).toLocaleString() : null,
                    endDate: r.endDate ? new Date(r.endDate).toLocaleString() : null,
                    tz: r.tz || "UTC",
                    count: r.count || 0,
                });
            }
        }

        res.json({
            total: repeatableJobs.length,
            repeatableJobs,
        });
    } catch (err) {
        console.error("getAllRepeatableJobsWithHistory error:", err);
        res.status(500).json({ error: err.message });
    }
};

exports.getJobsDetails = async (req, res) => {
    try {
        const { queueName, jobId } = req.params;
        if (queueName.endsWith(':')) {
            queueName = queueName.replace(/:$/, '');
        }
        const q = new Queue(queueName, { redis: keys.redisURL });

        // Fetch up to 100 jobs from all states
        const jobs = await q.getJobs(
            ["waiting", "active", "completed", "failed", "delayed"],
            0,
            100
        );
        // Filter jobs by tenant and jobId substring match
        const filteredJobs = jobs.filter(job => (job?.id?.includes(jobId)));


        const jobsData = await Promise.all(
            filteredJobs?.map(async job => ({
                id: job.id,
                status: await job.getState(),
                attemptsMade: job.attemptsMade,
                cron: job.opts.repeat?.cron || null,
                tz: job?.data?.tz || '',
                title: job?.data?.title || '',
                description: job?.data?.description || '',
                scheduleId: job?.data?.scheduleId || '',
                tenant: job?.data?.tenant || '',
                action: job?.data?.eventsConfig[0]?.actions || '',
                opts: job.opts
            }))
        );

        res.json({ queueName, jobId, jobs: jobsData });
    } catch (err) {
        console.error("getJobsV2 error:", err);
        res.status(500).json({ error: err.message });
    }
};
