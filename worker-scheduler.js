const throng = require('throng');
const Bull = require('bull');
const keys = require('./config/keys');
const QUEUE_NAME = 'SCHEDULE';
const REDIS_URL = process.env.REDIS_URL || keys.redisURL;
const moment = require('moment');

const workers = process.env.WEB_CONCURRENCY || 2;

const maxJobsPerWorker = 5;

function start() {
    const scheduleQueue = new Bull(QUEUE_NAME, REDIS_URL);

    scheduleQueue.process(maxJobsPerWorker, async (job) => {
        console.log(job.data)
        var log = (moment().format("MMM Do YYYY, h:mm a")) + `: Scheduled job started...`;
        job.log(log);
        return { value: "job done"}
    })
}

throng({ workers, start })
