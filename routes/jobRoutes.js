const bodyParser = require("body-parser");
const URL = require('url');
const Bull = require("bull");
const QUEUE_NAME= 'FLOW';
const keys = require('../config/keys');
const sample_flow_definition = require('../config/wf-definition-example.json');
const flowQueue = new Bull(QUEUE_NAME, keys.redisURL);
const resQueue = new Bull('RESPONSE', keys.redisURL);


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

  app.post('/sms/reply', function(req, res) {
	  const smsCount = req.session.counter || 0;

	  req.session.counter = smsCount + 1;

	  console.log("HEADER: ",req.headers, "BODY: ", req.body, "SESSION: ", req.session)
	  res.json({"status":true, "messag": "Success", "status_code": 200})
  })

}

