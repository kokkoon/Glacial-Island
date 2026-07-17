const { flowQueue, FLOW_QUEUE } = require('./config/bull');
const orchestrator = require('./services/orchestrator');
const { saveWorkflowLogs } = require('./services/saveWorkflowLogs');
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
        await orchestrator.startflow(job);

        if (job.data.state === "Failed") {
            throw new Error(job.data.error || "Workflow failed");
        }

        return { value: "job done", state: job.data.state };
    })
//}

flowQueue.on('completed', async (job) => {
    // Paused jobs are waiting on a task — not a final complete
    if (job?.data?.state === "Paused") return;
    await saveWorkflowLogs(job, 'completed');
});

flowQueue.on('failed', async (job, err) => {
    await saveWorkflowLogs(job, 'failed', err);
});

//throng({ workers, start })
console.log("Flow worker started for ", FLOW_QUEUE);
