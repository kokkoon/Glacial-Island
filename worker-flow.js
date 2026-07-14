const { flowQueue, FLOW_QUEUE } = require('./config/bull');
const orchestrator = require('./services/orchestrator');
const moment = require('moment');


//const workers = process.env.WEB_CONCURRENCY || 1;

const maxJobsPerWorker = 1;

//function start() {
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
        return { value: "job done"}
    })
//}

//throng({ workers, start })
console.log("Flow worker started for ", FLOW_QUEUE);
