const { promisify } = require('util');
const bodyParser = require("body-parser");
const URL = require('url');
const keys = require('../config/keys');
const NODE_ENV = process.env.NODE_ENV;
const Bull = require("bull");
const QUEUE_NAME= 'FLOW';
const TASK_QUEUE = 'TASK@' + NODE_ENV;
const EMAIL_QUEUE = 'EMAIL@' + NODE_ENV;
const flowQueue = new Bull(QUEUE_NAME, keys.redisURL);
const taskQueue = new Bull(TASK_QUEUE, keys.redisURL);
const emailQueue = new Bull(EMAIL_QUEUE, keys.redisURL);
const Auth = require("../services/authentication");
const sample_flow_definition = require('../config/wf-definition-example.json');
const MessagingResponse = require('twilio').twiml.MessagingResponse;
const redis = require('redis');
const async = require('async');
const redisqueries = require('../services/redisqueries');
const taskqueries = require('../services/taskqueries');
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

  app.get('/tasks', function(req, res) {
	var owner = req.headers.owner ? req.headers.owner.split(','): "";
	var getKeys = new Promise((resolve, reject) => {
		var keys = [];
		var keylist = undefined
		owner.forEach(async (key, i, array) => {
			keylist = await redisqueries.allkeys(`bull:${TASK_QUEUE}:${key}-*`)
			keys = keys.concat(keylist) 
			if (i === array.length - 1) resolve(keys)
		});
	});
	getKeys.then((allkeys) => {
		console.log("allkeys", allkeys);
		const taskList = [];
		var taskInst = undefined;
		var getTaskList = new Promise((resolve, reject) => {
			allkeys.forEach(async (key, i, array) => {
				console.log("Retriving task:", key, key.match(/([^:]+$)/)[0]);
				taskInst = await taskQueue.getJob(key.match(/([^:]+$)/)[0]); //substring after the last colon (i.e. :)
				//console.log(taskInst)
				taskInst && taskList.push({id: taskInst.id, timestamp: taskInst.timestamp, key: key, data: taskInst.data, task: taskInst});
				if (i === array.length -1) resolve(taskList);
			})
		})

		getTaskList.then((tl) => {
			res.status(200).send(tl)
		})
	})
	.catch(alert => {
		console.log("(ops!)alert:", alert);
		res.json({ "status": false, "message": alert, "status_code": 401})
	})
  })

  app.patch('/task/:id/:outcome', Auth.Authenticate, async function(req, res) {
	const id = req.params.id;
	var outcome = req.params.outcome;
	var taskInst = undefined;
	console.log("Retriving task:", id, " outcome:", outcome);
	taskInst = await taskQueue.getJob(id);
	outcome = outcome.match(/App/i) ? 'approved': outcome.match(/Rej/i) ? 'rejected':outcome;
	console.log("User's response:", outcome)

	taskqueries.resume(taskInst, outcome)
		.then(async ans => {
			if (ans.resumed) {
				// completion criteria met, update other tasks...
				taskqueries.closePendingTasks(taskInst, outcome)
			}
			console.log("Resumed message:", ans)
			taskInst.data.status = "Completed";
			taskInst.data.response = outcome;
			taskInst.data.updated = Date.now();
			await taskInst.update(taskInst.data);
			res.status(200).send(`${ans}`);
		}).catch(err => {
			console.log(`Error patching task...${err}`)
			res.status(501).send({status: 501, error: err})
		})
  })

  app.post('/email/notify', function(req, res) {
    let message = req.body;
    emailQueue.add(message)
        .then(result => {
            res.status(200).send("Success");
        })
        .catch(alert => {
            res.status(401).send(alert);
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
			const outcome = msg.match(/App/i) ? 'approved': msg.match(/Rej/i) ? 'rejected':undefined;
			console.log("User's response:", outcome);

			var replyMsg = "";
			if (outcome === undefined) return `Failed interprete your reply: ${msg}`;
			
			return taskqueries.resume(waitingJob[0], outcome)
				.then(async ans => {
					console.log(`1. Resumed: ${ans.resumed}, message: ${ans.message}`);
					if (ans.resumed) {
						// completion criteria met, update other tasks...
						taskqueries.closePendingTasks(waitingJob[0], outcome)
					} 
					
					waitingJob[0].data.status = "Completed";
					waitingJob[0].data.response = outcome;
					waitingJob[0].data.updated = Date.now();
					await waitingJob[0].update(waitingJob[0].data);
					//await waitingJob[0].promote();
					//await waitingJob[0].moveToCompleted('completed', true, true)
					//await waitingJob[0].remove();
					return `${ans.message}`;
						
				}).catch(err => {
					console.log(`Error...${err} ${msg}`)
					return `Error... ${err}`
				})
		})
		.then(replyMsg =>{
			console.log(`replyMsg: ${replyMsg}`)
			twiml.message(replyMsg);
			res.writeHead(200, {'Content-Type':'text/xml'});
			res.end(twiml.toString());
		})
		.catch(alert => {
			console.log("ops!alert:", alert);
			twiml.message('Failed!');
			res.writeHead(200, {'Content-Type':'text/xml'});
			res.end(twiml.toString());
		})

		console.log("SESSION: ", req.session)
		//res.set('Content-Type', 'text/xml')
  })

}

function closePendingTasks(task, outcome) {
	var taskGroupNumber = task.id.match(/(?<=\-).+?(?=\-)/);
	redisqueries.allkeys(`bull:${TASK_QUEUE}:*-${taskGroupNumber}-*`)
		.then(async keys => {
			console.log(`3. Total task/assignee: ${keys.length}, Task group: ${taskGroupNumber}`)
			keys.splice(keys.indexOf(task.queue.keys['']+task.id),1);
			
			if (keys.length > 0)  {
				var taskInst = undefined;
				var getTaskList = new Promise((resolve, reject) => {
					keys.forEach(async (key, i, array) => {
						console.log("3. Retriving task:", key, key.match(/([^:]+$)/)[0]);
						taskInst = await taskQueue.getJob(key.match(/([^:]+$)/)[0]); //substring after the last colon (i.e. :)
						taskInst && console.log("3. Task Inst:", taskInst.id, " response:", taskInst.data.response);
						if (taskInst.data.status !== "Completed" && taskInst.data.status !== "Closed") {
							taskInst.data.status = "Closed";
							taskInst.data.response = outcome;
							taskInst.data.updated = Date.now();
							await taskInst.update(taskInst.data);
						}
						if (i === array.length -1) resolve(keys);
					})
				})
			}
		})
}

function resume(task, outcome) {
	return new Promise(async function(resolve, reject) {
		const jobId = task.data.instanceId
		const job = await flowQueue.getJob(jobId); //get workflow instance by instance id
		const jobData = {...job.data};
		console.log(jobId, job.data.state)
		if (job.data.state !== "Paused") {
			console.log("Only a paused job could be resumed");
			reject("Only a paused job could be resumed");
		} else {
			/* Note:
			// Check approval criteria here before setting job/workflow's outcome
			// criteria = "Anyone" | "Majority" | "All"
			// Anyone = First response to complete
			// Majority = highest vote or "Reject" (i.e. equal vote = rejected)
			// All = all must agreed on a decision to complete, or it will be rejected
			*/
			var taskGroupNumber = task.id.match(/(?<=\-).+?(?=\-)/);
			if (task.data.criteria!="Anyone") {  
				redisqueries.allkeys(`bull:${TASK_QUEUE}:*-${taskGroupNumber}-*`)
					.then(async keys => {
						console.log(keys, task.queue.keys['']+task.id)
						console.log(`2. Total task/assignee: ${keys.length}, Task group: ${taskGroupNumber}`)
						keys.splice(keys.indexOf(task.queue.keys['']+task.id),1);
						if (keys.length > 0)  {
							const taskList = [];
							var taskInst = undefined;
							var getTaskList = new Promise((resolve, reject) => {
								keys.forEach(async (key, i, array) => {
									console.log("2. Retriving task:", key, key.match(/([^:]+$)/)[0]);
									taskInst = await taskQueue.getJob(key.match(/([^:]+$)/)[0]); //substring after the last colon (i.e. :)
									taskInst && console.log("Task Inst:", taskInst.data.response);
									taskInst && taskList.push(taskInst.data.response);
									if (i === array.length -1) resolve(taskList);
								})
							})

							getTaskList.then((tl) => {
								console.log(tl)
								tl.push(outcome);
								var allEqual = tl.every(v => v === tl[0]);
								var majority = majWithKKalgorithm(tl);
								var agreed = tl.filter(x => x == "approved").length;
								var disagreed = tl.filter(x => x == "rejected").length;
								var other = tl.filter(x => x.match(/^(approved|rejected)$/)).length;
								var allAgreed = agreed === tl.length;
								var all = allEqual? tl[0] : "rejected";
								console.log("taskList:", tl, "length:", tl.length,"all equals?", allEqual, allEqual? tl[0]: "", "Majority:", majority, "All:", all)

								var outcomeByCriteria = ""
								if (task.data.criteria == "Majority") {
									outcomeByCriteria = majority;
								} else if (task.data.criteria == "All") {
									outcomeByCriteria = tl.includes("")? "" : all;
								}

								if (outcomeByCriteria == "none") {
									resolve({resumed: false, message: `${outcome}, pending completion criteria!`})
								} else {
									// Criteria fulfilled, resume workflow...
									jobData.definition.actions[0].configuration.properties.outcome = outcomeByCriteria;
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
													resolve({resumed: true, message: `Workflow instance ${jobId} resumed as "${outcomeByCriteria}"`})
												})
										}).catch(err => {
											reject(err)
										})
								}
							})
						} else {
							// The only assignee, resume workflow...
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
											resolve({resumed: true, message: `Workflow instance ${jobId} resumed as "${outcome}"`})
										})
								}).catch(err => {
									reject(err)
								})
						}
					})
					.catch(alert => {
						console.log("(ops!)alert:", alert);
					})
			} else {
				// Approval concluded, resume workflow...
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
								resolve({resumed: true, message: `Workflow instance ${jobId} resumed as "${outcome}"`})
							})
					}).catch(err => {
						reject(err)
					})
			}
		}
	});

  }

  function majWithKKalgorithm(nums) {
	let count = {};
  
	for (let elem of nums) { count[elem] = count[elem] ? count[elem] + 1 : 1 }
	
	let candidates = Object.keys(count)
	let votes = candidates.map(k => { return count[k]})
	console.log("candidates:", candidates, "votes:", votes)
	
	let max = Math.max(...votes)  //highest votes
	//let maxCount = votes.map(v => v == max? 1 : 0).reduce((a,b) => a+b, 0)
	//console.log(`highest=${max}, occurs: ${maxCount} times`)
	
	console.log("Total candidates:", candidates.length)
	console.log("Uncountered votes:", count[""])
	console.log("Highest vote:", max)
	console.log("Total votes:", nums.length - count[""])
	
	var winners = candidates.filter(key => {return count[key] === max})
	console.log("winners:", winners)
	
	let theWinner = (winners.length == 1) && (nums.length - max < max) ? winners[0] : count[""] == null ? "rejected": (nums.length - count["rejected"] <= count["rejected"])? "rejected" : "none" 
  
	return theWinner;
  }