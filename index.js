//replaced server.js

const express = require("express");
const session = require("express-session");
const app = express();
const Bull = require('bull');
const GUI = require('bull-arena');
const keys = require('./config/keys');
const redisqueries = require('./services/redisqueries');
const QUEUE_NAME = 'FLOW';
const REDIS_URL = process.env.REDIS_URL || keys.redisURL;
const orchestrator = require('./services/orchestrator');
const moment = require('moment');
const flowQueue = new Bull(QUEUE_NAME, REDIS_URL);

//const serviceWorker = require('./worker-service');
//const taskWorker = require('./worker-task');
// const flowWorker = require('./worker-flow');
//const emailWorker = require('./worker-email');

const maxJobsPerWorker = 1;
flowQueue.process(maxJobsPerWorker, async (job) => {
	console.log('Workflow worker started.')
	if (!job.data.state) job.data.state = "Active";
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

redisqueries.getAllQueues(resData => {
	resData = resData.length === 0 ? ["FLOW"] : resData
	const qDashboard = GUI({
		Bull,
		queues: resData.map(v => ({name: v, hostId: "flow", url: keys.redisURL}))
	}, {
		basePath: "/",
		disableListen: true
	});
	
	app.use('/queue_dashboard', qDashboard);
})

const dashboard = GUI({
    Bull,
	queues: [
		{
			name: "FLOW",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "SERVICE",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "MESSAGE",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "RESPONSE",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "REPONSE",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "SCHEDULE",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "SCHEDULE@DESKTOP-HO2F260",
			hostId: "flow",
			url: keys.redisURL 
        },
		{
			name: "SCHEDULE@glozic.com",
			hostId: "flow",
			url: keys.redisURL 
        },
		{
			name: "SCHEDULE@flowngin.com",
			hostId: "flow",
			url: keys.redisURL 
        }
	]
}, {
	basePath: "/",
	disableListen: true
});


app.use('/dashboard', dashboard);

app.use(session({
	secret:'anyt-string-but-to-keep-secret',
	name: 'get-approval',
    proxy: true,
    resave: true,
    saveUninitialized: true
}));
require('./routes/jobRoutes')(app);

const PORT = process.env.PORT || '4000';
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`)
})







  