//const throng = require('throng');
const Bull = require('bull');
const keys = require('./config/keys');
const orchestrator = require('./services/orchestrator');
const moment = require('moment');
const maxJobsPerWorker = 1;
const getEnv = (tenant) => {
	let result = ""
	if (tenant.search("-dev") >= 0) {
		result = `studio.${tenant}`
	} else {
		result = `production.${tenant}`
	}
	return result
}

function start(tenant) {
    const QUEUE_NAME = 'FLOW@' + getEnv(tenant);
    const flowQueue = new Bull(QUEUE_NAME, keys.redisURL);
    flowQueue.process(maxJobsPerWorker, async (job) => {
        //Active
        if (!job.data.state) job.data.state = "Active";

        //Start
        if (!job.data.start) job.data.start = moment();

        job.data.jobStart = moment();
        if (job.data.state !== "Paused") {
            job.data.data = {};
            job.data.definition.variables.forEach(element => {
                job.data.data[element.name] = element.value
            });
        }

        await job.update(job.data);
        // Start orchestration job
        orchestrator.startflow(job)
        return { value: "job done" }
    })

    //throng({ workers, start })
    console.log("Flow worker started for ", QUEUE_NAME);
}

module.exports = {
    start: start
}
