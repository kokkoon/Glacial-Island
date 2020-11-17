const { promisify } = require('util');
const bodyParser = require("body-parser");
const URL = require('url');
const keys = require('../config/keys');
const NODE_ENV = process.env.NODE_ENV;
const Bull = require("bull");
const QUEUE_NAME= 'FLOW';
const TASK_QUEUE = 'TASK@' + NODE_ENV;
const flowQueue = new Bull(QUEUE_NAME, keys.redisURL);
const taskQueue = new Bull(TASK_QUEUE, keys.redisURL);
const Auth = require("../services/authentication");
const sample_flow_definition = require('../config/wf-definition-example.json');
const MessagingResponse = require('twilio').twiml.MessagingResponse;
const redis = require('redis');
const async = require('async');
const redisqueries = require('../services/redisqueries');
const { doesNotMatch } = require('assert');


module.exports = app => {
  app.use(bodyParser.urlencoded({ extended: false }));
  app.use(bodyParser.json());

  app.get('/allkeys/:id', async function(req, res) {
	console.log(req.params.id)
	redisqueries.allkeys(`bull:FLOW:${req.params.id}*`)
		.then(keys => {
			res.json({"status": true, "message": keys, "status_code": 200})
		})
		.catch(alert => {
			res.json({ "status": false, "message": alert.message, "status_code": 401})
		})
  })

  app.get('/allIds', async function(req, res) {
	redisqueries.allIds(resData => {
		res.send(resData)
	})
	})

  app.get('/queues', async function(req, res) {
	redisqueries.scan(resData => {
		res.send(resData)
	})
  })

  app.post('/orchestration', Auth.Authenticate, async function(req, res) {
	  console.log(req.headers)
	const url = URL.parse(req.url, true)
	const mode = url.query.mode;
	const jobDefinition = (mode && mode === "test")?sample_flow_definition: req.body; 
	redisqueries.instanceNumber('bull:FLOW:id')
		.then(uniqueId => {
			console.log(uniqueId);
			const JobOpts = {
				... jobDefinition._id && {jobId: jobDefinition._id + "-" + uniqueId}
			};
			console.log("Posting ", (mode && mode === "test")? "sample flow definition": "flow definition", JobOpts);
			jobDefinition.name = jobDefinition.workflowName;
			jobDefinition.tenant = req.headers.tenant;
			jobDefinition.state = "Queued";
			flowQueue.add(jobDefinition, JobOpts)
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
		.catch(alert => {
			res.json({ "status": false, "message": alert.message, "status_code": 401})
		})
  })

  app.get('/orchestration/:id', Auth.Authenticate, function(req, res) {
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

  app.get('/logs/:jobId', Auth.Authenticate, function(req, res) {
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

  app.post('/resumejob/:jobId/:outcome', Auth.Authenticate, async function(req, res) {
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

  app.get('/instances/:flowId', Auth.Authenticate, function(req, res) {
	const flowId = req.params.flowId;

	redisqueries.allkeys(`bull:FLOW:${flowId}-*[^s]`)
		.then(async keys => {
				//console.log(keys);
				const instList = []
				var inst = {}
				var getJobList = new Promise((resolve, reject) => {
					keys.forEach(async (key, i, array) => {
						//console.log(key, i)
						//if (!key.endsWith(":logs")) {
							inst = await flowQueue.getJob(key.match(/bull\:FLOW\:(.*)/)[1])
							instList.push(inst)
						//}
						if (i === array.length -1) resolve();
					})
				})
				
				getJobList.then(() => {
					console.log(`Log instances for ${flowId}:`, instList.length);
					if (instList.length > 0) {
						res.json({"status": true, "data":instList, "status_code": 200})
					} else {
						res.json({"status": false, "data": [], "status_code": 401})
					}
				})
			}, error => {
				console.log("error:", error);
				res.json({ "status": false, "message": "Found no matching keys", "status_code": 401 });
			})
		.catch(alert => {
			console.log("(ops!)alert:", alert);
			res.json({ "status": false, "message": alert.message, "status_code": 401})
		})
	
  })

  app.post('/sms/reply', function (req, res) {
	  const twiml = new MessagingResponse();
	  const smsCount = req.session.counter || 0;
	  const msg = req.body.Body;
	  req.session.counter = smsCount + 1;
	  console.log("BODY: ", req.body)

	  taskQueue.getJobs(['delayed'], 0, 100)
	  	.then(async result => {
			var waitingJob = result.filter(obj => {return obj.data.to === req.body.From})
			console.log(`Total: ${result.length}, # of waiting jobs for ${req.body.From}`, waitingJob.length)
			if (waitingJob.length<1) return `There were no pending task for you`;
			const outcome = msg.match(/Ap/i) ? 'approved': msg.match(/Re/i) ? 'rejected':undefined;
			console.log("User's response:", outcome)
			var replyMsg = "";
			if (outcome === undefined) return `Failed interprete your reply: ${msg}`;
			//Approval criteria check... then resume or not
			return resume(waitingJob[0], outcome)
				.then(async ans => {
					console.log("Resumed message:", ans)
					waitingJob[0].data.status = "Completed";
					waitingJob[0].data.repliedOutcome = outcome;
					await waitingJob[0].update(waitingJob[0].data);
					await waitingJob[0].promote();
					//await waitingJob[0].moveToCompleted('completed', true, true)
					//await waitingJob[0].remove();
					return `${ans}`;
				}).catch(err => {
					console.log(`Error...${err} ${msg}`)
					return `Error... ${err}`
				})
		})
		.then(replyMsg =>{
			console.log(replyMsg)
			twiml.message(replyMsg);
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

function resume(task, outcome) {
	return new Promise(async function(resolve, reject) {
		const jobId = task.data.instanceId
		const job = await flowQueue.getJob(jobId); //get workflow instance by instance id
		console.log(jobId, job.data.state)
		if (job.data.state !== "Paused") {
			console.log("Only a paused job could be resumed");
			reject("Only a paused job could be resumed");
		} else {
			// Check approval criteria here before setting job/workflow's outcome
			// criteria = "Anyone" | "Majority" | "All"
			if (task.data.criteria!="Anyone") {  
				var taskGroupNumber = task.id.match(/(?<=\-).+?(?=\-)/);
				redisqueries.allkeys(`bull:${TASK_QUEUE}:*-${taskGroupNumber}-*`)
					.then(async keys => {
						console.log(`Total task/assignee: ${keys.length}, Task group: ${taskGroupNumber}`)
						if (keys.length > 1)  {
							const taskList = [];
							var taskInst = undefined;
							var getTaskList = new Promise((resolve, reject) => {
								keys.forEach(async (key, i, array) => {
									console.log("Retriving task:", key);
									taskInst = await taskQueue.getJob(key.match(/([^:]+$)/)); //substring after the last colon (i.e. :)
									taskInst && taskInst.status && console.log("Task Inst:", taskInst.status);
									taskInst && taskList.push(taskInst);
									if (i === array.length -1) resolve();
								})
							})

							getTaskList.then(() => {
								console.log("Returned taskList length:", taskList.length)
							})
						}
					})
					.catch(alert => {
						console.log("(ops!)alert:", alert);
					})
			}
			
			// Approval concluded, resume workflow...
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
							resolve(`Workflow instance ${jobId} resumed as "${outcome}"`)
						})
				}).catch(err => {
					reject(err)
				})
		}
	});

  }