const throng = require('throng');
const Bull = require('bull');
const keys = require('./config/keys');
const QUEUE_NAME = 'FLOW';
const REDIS_URL = process.env.REDIS_URL || keys.redisURL;
const orchestrator = require('./services/orchstrator');
const moment = require('moment');

const workers = process.env.WEB_CONCURRENCY || 1;

const maxJobsPerWorker = 1;

function start() {
    const flowQueue = new Bull(QUEUE_NAME, REDIS_URL);

    flowQueue.process(maxJobsPerWorker, async (job) => {
        console.log(job.data)
        var log = (moment().format("MMM Do YYYY, h:mm a")) + `: Orchestration job started...`;
        job.log(log);

        // Start orchestration job
        orchestrator.startflow(job)
        return { value: "job done"}
    })
}

throng({ workers, start })