const bodyParser = require("body-parser");
const URL = require('url');
const Bull = require("bull");
const QUEUE_NAME= 'FLOW';
const keys = require('../config/keys');
const sample_flow_definition = require('../config/wf-definition-example.json');
const flowQueue = new Bull(QUEUE_NAME, keys.redisURL);
const resQueue = new Bull('RESPONSE', keys.redisURL);
const MessagingResponse = require('twilio').twiml.MessagingResponse;


module.exports = app => {
  app.use(bodyParser.urlencoded({ extended: false }));
  app.use(bodyParser.json());

  app.post('/orchestration', function(req, res) {
	const url = URL.parse(req.url, true)
	const mode = url.query.mode;
	const jobDefinition = (mode && mode === "test")?sample_flow_definition: req.body;
	console.log("Posting ", (mode && mode === "test")? "sample flow definition": "flow definition");
	jobDefinition.name = jobDefinition.workflowName;
	jobDefinition.state = "Queued";
	flowQueue.add(jobDefinition)
	.then(result => {
		console.log("jobId:", result.id, "jobState:", result.getState())
		res.json({"status": true, "data": result, "status_code": 200})
		}, error => {
		console.log("error:", error)
		res.json({ "status": false, "message": error.message, "status_code": 401 });
		})
	.catch(alert => {
		console.log("alert:", alert)
		res.json({ "status": false, "message": alert.message, "status_code": 401 });
	})

  })

  app.get('/orchestration/:id', function(req, res) {
	console.log(req.params.id)
	flowQueue.getJob(req.params.id)
		.then(job => {
			console.log("result:", job)
			job.getState()
				.then(result => {
					console.log("jobState:", result)
				})
				.catch(alert => {
					console.log("alert:", alert)
				})
				res.send(job)
			}, error => {
				console.log("(ops!)error:", error)
				res.send(error)
			})
		.catch(alert => {
			console.log("alert:", alert)
			res.send(alert)
		})
  })

  app.get('/logs/:jobId', function(req, res) {
	const jobId = req.params.jobId;
	const url = URL.parse(req.url, true);
	const start = url.query.start? url.query.start : 0;
	const end = url.query.end? url.query.end : 20;
	  flowQueue.getJobLogs(jobId, start, end)
	  	.then(logs => {
			  console.log(`jobLogs(${jobId}?${start}&${end}):`, logs)
			  res.json(logs);
		  }, error => {
			console.log("(ops!)error:", error)
			res.send(error)
		  })
		.catch(alert => {
			console.log("(ops!)alert:", alert);
			res.send(alert);
		})
  })

  app.post('/resumejob/:jobId/:outcome', async function(req, res) {
	const jobId = req.params.jobId;
	const job = await flowQueue.getJob(jobId);
	if (job.data.state !== "Paused") {
		res.send("Only a paused job could be resumed");
		return;
	}
	const jobData = {...job.data};
	jobData.definition.actions[0].configuration.properties.outcome = req.params.outcome;
	flowQueue.getJobLogs(jobId)
		.then(logs => {
			const jobLogs = {...logs}
			console.log("jobLogs123:", jobLogs);
			job.remove();
			flowQueue.add(jobData, {jobId: jobId})
				.then(resumedJob => {
					jobLogs.logs.forEach(log => {
						resumedJob.log(log);
					});
				})
				.then(resumedJob => {
					res.send(resumedJob)
				})
		})

  })

  app.get('/instances/:flowId', function(req, res) {
	const flowId = req.params.flowId;
	flowQueue.getJobs(['completed','active','waiting'], 0, 100)
		.then(result => {
				console.log("All instances:", result.length)
				result1 = result.filter(obj => { console.log(obj.data._id); return obj.data._id === flowId });
				console.log(`Instance of ${flowId}`,result1.length)
				res.json({"status": true, "data": result1, "status_code": 200});
			}, error => {
				console.log("error:", error);
				res.json({ "status": false, "message": error.message, "status_code": 401 });
			})
		.catch(alert => {
			console.log("(ops!)alert:", alert);
			res.json({ "status": false, "message": alert.message, "status_code": 401 });
		})
  })

  app.post('/sms/reply', function (req, res) {
	  const twiml = new MessagingResponse();
	  twiml.message('Init!');
	  const smsCount = req.session.counter || 0;
	  const msg = req.body.Body;
	  req.session.counter = smsCount + 1;
	  console.log("BODY: ", req.body)

	  resQueue.getJobs(['waiting'], 0, 100)
	  	.then(result => {
			var waitingJob = result.filter(obj => {return obj.data.to === req.body.From})
			console.log(`# of waiting jobs for ${req.body.From}`, waitingJob.length)
			const outcome = msg.match(/Approve/i) ? 'approved': msg.match(/Reject/i) ? 'rejected':undefined;
			console.log("outcome", outcome)
			if (outcome !== undefined) { 
				resume(waitingJob[0].data.instanceId, outcome)
					.then(ans => {
						if (ans) {waitingJob.moveToCompleted('completed', true, true)
						twiml.message(`Task ${outcome}`);} else {
							twiml.message(`There was no pending task to ${msg}`)
						}
					}).catch(err => {
						twiml.message(`There was no pending task to ${msg}`)
					})
			} else {
				twiml.message(`Could not interprete reply: ${msg}`)
			} 
			res.writeHead(200, {'Content-Type':'text/xml'});
			res.end(twiml.toString());
		})
		.catch(alert => {
			console.log("(ops!alert:", alert);
			twiml.message('Failed!');
			res.writeHead(200, {'Content-Type':'text/xml'});
			res.end(twiml.toString());
		})

		console.log("SESSION: ", req.session)
		//res.set('Content-Type', 'text/xml')
  })

}

const resume = async (jobId, outcome) => {
	const job = await flowQueue.getJob(jobId);
	console.log(jobId, job.data.state)
	if (job.data.state !== "Paused") {
		res.send("Only a paused job could be resumed");
		return false;
	}
	const jobData = {...job.data};
	jobData.definition.actions[0].configuration.properties.outcome = outcome;
	flowQueue.getJobLogs(jobId)
		.then(logs => {
			const jobLogs = {...logs}
			job.remove();
			flowQueue.add(jobData, {jobId: jobId})
				.then(resumedJob => {
					jobLogs.logs.forEach(log => {
						resumedJob.log(log);
					});
				})
				.then(resumedJob => {
					//res.send(resumedJob)
					console.log(`Job ${jobId} resumed`)
					return true
				})
		})

  }

