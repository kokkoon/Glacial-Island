const { promisify } = require('util');
const bodyParser = require("body-parser");
const URL = require('url');
const keys = require('../config/keys');
const NODE_ENV = process.env.NODE_ENV || "local";
const {
	flowQueue,
	logQueue,
	taskQueue,
	emailQueue,
	FLOW_QUEUE,
	LOGS_QUEUE,
	TASK_QUEUE,
	EMAIL_QUEUE,
	connectQueue,
} = require('../config/bull');
const Auth = require("../services/authentication");
const sample_flow_definition = require('../config/wf-definition-example-1.json');
const MessagingResponse = require('twilio').twiml.MessagingResponse;
const redis = require('redis');
const async = require('async');
const redisqueries = require('../services/redisqueries');
const taskqueries = require('../services/taskqueries');
const { doesNotMatch } = require('assert');
const accountSid = keys.twilioAccountSid;
const authToken = keys.twilioAuthToken;
const client = require('twilio')(accountSid, authToken);
const queueController = require("../controller/queueController");



module.exports = app => {
	app.use(bodyParser.urlencoded({ extended: false }));
	app.use(bodyParser.json());

	/*
	app.get('/allkeys/:id', async function (req, res) {
		console.log(req.params.id)
		redisqueries.allkeys(`bull:${FLOW_QUEUE}:${req.params.id}*`)
			.then(keys => {
				res.json({ "status": true, "message": keys, "status_code": 200 })
			})
			.catch(alert => {
				res.json({ "status": false, "message": alert.message, "status_code": 401 })
			})
	})
			*/

	app.get('/allkeys/:id?', async function (req, res) {
		try {
			var { id } = req.params;

			// 1. Check if ID is provided
			if (!id) {
				id = '*';
			}

			// 2. Optional: Basic sanitization/validation (e.g., alphanumeric only)
			// This prevents directory traversal or Redis pattern injection
			/*if (!/^[a-zA-Z0-9_*1-]+$/.test(id)) {
				return res.status(400).json({ 
					status: false, 
					message: "Invalid format. Use alphanumeric or '*'.", 
					status_code: 400 
				});
			}*/

			console.log(`Searching keys for ID: ${id}`);

			// 3. Perform the query using await
			const keys = await redisqueries.allkeys(id);

			res.json({ 
				status: true, 
				message: keys, 
				status_code: 200 
			});

		} catch (error) {
			// 4. Handle unexpected errors (Redis connection, etc.)
			console.error("Redis Error:", error);
			res.status(500).json({ 
				status: false, 
				message: "Internal Server Error", 
				status_code: 500 
			});
		}
	});

	app.get('/allIds', async function (req, res) {
		redisqueries.allIds(resData => {
			res.send(resData)
		})
	})

	app.get('/queues', async function (req, res) {
		redisqueries.scan(resData => {
			res.send(resData)
		})
	})

		/**
     * API to clean completed and failed jobs from all primary queues
     * Usage: GET /clean-queues?grace=1000 (grace is in milliseconds)
     */
    app.get('/clean-queues', async function (req, res) {
        try {
            // Default grace period of 24 hours (86,400,000 ms) if not specified
            const grace = req.query.grace ? parseInt(req.query.grace) : 24 * 3600 * 1000;
			const queue = req.query.queue ? String(req.query.queue) : FLOW_QUEUE;
            
            const queues = [
                { name: queue, instance: flowQueue },
                //{ name: LOGS_QUEUE, instance: logQueue },
                //{ name: TASK_QUEUE, instance: taskQueue },
                //{ name: EMAIL_QUEUE, instance: emailQueue }
            ];

            const results = {};
			console.log(`Starting cleanup ${queue} with grace period: ${grace} ms`);

            // Loop through each queue and clean both 'completed' and 'failed'
            for (const q of queues) {
                const cleanedCompleted = await q.instance.clean(grace, 'completed');
                const cleanedFailed = await q.instance.clean(grace, 'failed');
                
                results[q.name] = {
                    completed_removed: cleanedCompleted.length,
                    failed_removed: cleanedFailed.length
                };
            }

            console.log("Cleanup Results:", results);
            res.status(200).json({ 
                status: true, 
                message: "Cleanup successful", 
                grace_period_ms: grace,
                data: results 
            });

        } catch (err) {
            console.error("Cleanup Error:", err);
            res.status(500).json({ status: false, message: err.message });
        }
    });

	app.get('/purge-schedule', async function (req, res) {
		try {
			// 1. Define the specific queue causing the bloat
			const scheduleQueueName = req.query.queue ? String(req.query.queue) : 'SCHEDULE@glozic.dev';
			const scheduleQueue = connectQueue(scheduleQueueName);

			// 2. More aggressive cleaning: 0ms grace period
			// This removes ALL completed and failed jobs regardless of age
			const cleanedCompleted = await scheduleQueue.clean(0, 'completed');
			const cleanedFailed = await scheduleQueue.clean(0, 'failed');

			// 3. Clear "Wait" and "Delayed" if needed (Caution: this stops pending jobs)
			// const cleanedWaiting = await scheduleQueue.clean(0, 'wait');

			res.json({
				status: true,
				queue: scheduleQueueName,
				removed: {
					completed: cleanedCompleted.length,
					failed: cleanedFailed.length
				}
			});

			// Close local instance to prevent memory leaks
			await scheduleQueue.close();
			
		} catch (err) {
			res.status(500).json({ status: false, error: err.message });
		}
	});

	const checkAuth = (NODE_ENV === 'test' || NODE_ENV === 'local') 
		? (req, res, next) => next() // Skip middleware
		: Auth.Authenticate;         // Use real middleware

	app.post('/orchestration', checkAuth, async function (req, res) {
		console.log(req.headers)
		const url = URL.parse(req.url, true)
		const mode = url.query.mode;
		const jobDefinition = (mode && mode === "test") ? sample_flow_definition : req.body;
		redisqueries.instanceNumber(`bull:${FLOW_QUEUE}:id`)
			.then(uniqueId => {
				console.log(uniqueId);
				const JobOpts = {
					...jobDefinition._id && { jobId: jobDefinition._id + "-" + uniqueId }
				};
				console.log("Posting ", (mode && mode === "test") ? "sample flow definition" : "flow definition", JobOpts);
				jobDefinition.name = jobDefinition.workflowName;
				jobDefinition.tenant = req.headers.tenant;
				jobDefinition.state = "Queued";
				flowQueue.add(jobDefinition, JobOpts)
					.then(result => {
						console.log("jobId:", result.id, "jobState:", result.getState())
						res.json({ "status": true, "data": result, "status_code": 200 })
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
				res.json({ "status": false, "message": alert.message, "status_code": 401 })
			})
	})

	app.get('/orchestration/:id', Auth.Authenticate, function (req, res) {
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

	app.get('/logs/:jobId', Auth.Authenticate, function (req, res) {
		const jobId = req.params.jobId;
		const url = URL.parse(req.url, true);
		const start = url.query.start ? url.query.start : 0;
		const end = url.query.end ? url.query.end : 20;
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

	const resumePausedJob = async (jobId, outcome, res, asHtml) => {
		let normalizedOutcome = outcome;
		if (outcome.match(/App/i)) normalizedOutcome = 'approved';
		else if (outcome.match(/Rej/i)) normalizedOutcome = 'rejected';

		const job = await flowQueue.getJob(jobId);
		if (!job) {
			return res.status(404).send(asHtml
				? `<html><body><h2>Job not found</h2><p>Job ID: ${jobId}</p></body></html>`
				: "Job not found");
		}
		if (job.data.state !== "Paused") {
			return res.send(asHtml
				? `<html><body><h2>Unable to resume</h2><p>Only a paused job could be resumed.</p></body></html>`
				: "Only a paused job could be resumed");
		}
		if (job.data.hasOwnProperty('current_branch') && job.data.current_branch.length > 0) {
			job.data.definition.actions = [].concat(job.data.current_branch, job.data.definition.actions);
		}
		const jobData = { ...job.data };
		jobData.definition.actions[0].configuration.properties.outcome = normalizedOutcome;
		jobData.outcome = normalizedOutcome;
		flowQueue.getJobLogs(jobId)
			.then(logs => {
				const jobLogs = { ...logs }
				console.log("jobLogs123:", jobLogs);
				job.remove();
				flowQueue.add(jobData, { jobId: jobId })
					.then(resumedJob => {
						jobLogs.logs.forEach(log => {
							resumedJob.log(log);
						});
						return resumedJob;
					})
					.then(resumedJob => {
						if (asHtml) {
							res.send(`<html><body style="font-family:Arial,sans-serif;padding:24px;">
								<h2>Response recorded</h2>
								<p>Job <strong>${jobId}</strong> was marked as <strong>${normalizedOutcome}</strong>.</p>
								<p>You can close this window.</p>
							</body></html>`);
						} else {
							res.send(resumedJob);
						}
					})
			})
			.catch(err => {
				console.log("resumejob error:", err);
				res.status(500).send(asHtml
					? `<html><body><h2>Error</h2><p>${err.message || err}</p></body></html>`
					: err);
			})
	}

	// API / Postman (requires Authorization + tenant headers)
	app.post('/resumejob/:jobId/:outcome', Auth.Authenticate, async function (req, res) {
		await resumePausedJob(req.params.jobId, req.params.outcome, res, false);
	})

	// Email Approve/Reject links (browser GET, no auth headers available)
	app.get('/resumejob/:jobId/:outcome', async function (req, res) {
		await resumePausedJob(req.params.jobId, req.params.outcome, res, true);
	})

	app.get('/instances/:flowId', Auth.Authenticate, function (req, res) {
		const flowId = req.params.flowId;

		redisqueries.allkeys(`bull:${FLOW_QUEUE}:${flowId}-*[^s]`)
			.then(async keys => {
				//console.log(keys);
				const instList = []
				var inst = {}
				var getJobList = new Promise((resolve, reject) => {
					strRegex = new RegExp(`bull\\:${FLOW_QUEUE}\\:(.*)`);
					keys.forEach(async (key, i, array) => {
						//console.log(key, i)
						//if (!key.endsWith(":logs")) {
						//inst = await flowQueue.getJob(key.match(/bull\:FLOW\:(.*)/)[1])
						inst = await flowQueue.getJob(key.match(strRegex)[1])
						if (inst) instList.push(inst)
						//}
						if (i === array.length - 1) resolve();
					})
				})

				getJobList.then(() => {
					console.log(`Log instances for ${flowId}:`, instList.length);
					if (instList.length > 0) {
						let lists = instList.sort(function (a, b) {
							return new Date(b.timestamp) - new Date(a.timestamp);
						}).filter(x => x.data.tenant == req.headers.tenant);
						res.status(200).json({ "status": true, "data": lists })
					} else {
						res.json({ "status": false, "data": [], "status_code": 401 })
					}
				})
			}, error => {
				console.log("error:", error);
				res.json({ "status": false, "message": "Found no matching keys", "status_code": 401 });
			})
			.catch(alert => {
				console.log("(ops!)alert:", alert);
				res.json({ "status": false, "message": alert.message, "status_code": 401 })
			})

	})

	app.get('/task/:id', function (req, res) {
		const id = req.params.id;
		console.log("Retriving task:", id);/*
	var task = await taskQueue.getJob(id); 
		.then(task => {
			console.log(`Found task id: ${id}`, task)
			res.status(200).send(task)
		}).catch(err => {
			console.log(`Error retrieving task...${err}`)
			res.status(501).send({status: 501, error: err})
		}) */
		taskQueue.getJob(id)
			.then(task => {
				console.log(`Found task id: ${id}`, task)
				res.status(200).send(task)
			}).catch(err => {
				console.log(`Error retrieving task...${err}`)
				res.status(501).send({ status: 501, error: err })
			})
		//console.log(`task id: ${id}`, task)
		//res.status(200).send(task)
	})

	app.get('/tasks', function (req, res) {
		var owner = req.headers.owner ? req.headers.owner : "";
		console.log("owner", owner)
		var getKeys = new Promise(async (resolve, reject) => {
			var keys = [];
			var keylist = undefined
			try {
				keylist = await redisqueries.allkeys(`bull:${TASK_QUEUE}:${owner}-*`).catch(e => { reject(e) })
				keys = keys.concat(keylist)
				console.log("key length:", keys.length)
				resolve(keys)
			} catch (err) {
				reject({ message: err.message, status: false })
			}
		});
		getKeys.then((allkeys) => {
			const taskList = [];
			var taskInst = undefined;
			var getTaskList = new Promise((resolve, reject) => {
				try {
					allkeys.forEach(async (key, i, array) => {
						console.log("Retriving task:", key, key.match(/([^:]+$)/)[0]); //substring after the last colon (i.e. :)
						taskInst = await taskQueue.getJob(key.match(/([^:]+$)/)[0]).catch(e => { reject(e) });
						//console.log(taskInst)
						taskInst && taskList.push({ id: taskInst.id, timestamp: taskInst.timestamp, key: key, data: taskInst.data, task: taskInst });
						if (i === array.length - 1) resolve(taskList);
					})
				} catch (err) {
					//reject({ message: err.message, status: false })
					reject(err)
				}
			})

			getTaskList.then((tl) => {
				res.status(200).send(tl)
			})

			getTaskList.catch(err => {
				console.log("getTaskList err", err)
				res.status(401).send({})
			})
		})
			.catch(alert => {
				console.log("(ops!) alert:", alert);
				res.json({ "status": false, "message": alert, "status_code": 401 })
			})
	})

	app.patch('/task/:id/:outcome', Auth.Authenticate, async function (req, res) {
		const id = req.params.id;
		var outcome = req.params.outcome;
		var taskInst = undefined;
		console.log("Retriving task:", id, " outcome:", outcome);
		taskInst = await taskQueue.getJob(id);
		outcome = outcome.match(/App/i) ? 'approved' : outcome.match(/Rej/i) ? 'rejected' : outcome;
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
				res.status(501).send({ status: 501, error: err })
			})
	})

	app.patch('/externaltask/:id/:outcome', async function (req, res) {
		const id = req.params.id;
		var outcome = req.params.outcome;
		var taskInst = undefined;
		console.log("Retriving task:", id, " outcome:", outcome);
		taskInst = await taskQueue.getJob(id);
		outcome = outcome.match(/App/i) ? 'approved' : outcome.match(/Rej/i) ? 'rejected' : outcome;
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
				res.status(501).send({ status: 501, error: err })
			})
	})

	app.post('/email/notify', function (req, res) {
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
		//const smsCount = req.session.counter || 0;
		const msg = req.body.Body;
		//req.session.counter = smsCount + 1;
		console.log("BODY: ", req.body)
		var command = msg.match(/^task|tasks$/i) ? 'task' : msg.match(/^\?$/) ? '?' : msg;
		console.log('Command:', command);

		switch (command) {
			case "?":
				const replyMsg = "?: Command helps \
				\ntask, tasks: Get list of pending tasks \
				\napp[roved]: Approve a task \
				\nrej[ected]: Reject a task \
				\nlocation: Get location \
				\ndemo: Demo buttons \
				\nmore help: Redirect link"
				console.log(`replyMsg: ${replyMsg}`)
				twiml.message(replyMsg);
				res.writeHead(200, { 'Content-Type': 'text/xml' });
				res.end(twiml.toString());
				break
			case "location":
				client.messages.create({
					from: 'whatsapp:+16262473170',
					body: "Here is our office location",
					persistentAction: ['geo: 1.281422489647776,103.84804055799597'],
					to: req.body.From
				})
					.then(message => {
						console.log(message.sid);
						res.send(true)
					})
					.catch(error => console.error('error: ', error.message));
				break
			case "demo":
				client.messages.create({
					from: 'whatsapp:+16262473170',
					body: "Please select an action to be performed on task 10893237",
					to: req.body.From
				})
					.then(message => {
						console.log(message.sid);
						res.send(true)
					})
					.catch(error => console.error('error: ', error.message));
				break
			case "more help":
				client.messages.create({
					from: 'whatsapp:+16262473170',
					body: "For further enquiry, please tap below to call or visit out website.",
					to: req.body.From
				})
					.then(message => {
						res.send(true)
					})
					.catch(error => console.error('error: ', error.message));
				break
			default:
				taskQueue.getJobs(['delayed'], 0, 100)
					.then(async result => {
						const outcome = command.match(/App/i) ? 'approved' : command.match(/Rej/i) ? 'rejected' :
							command.match(/task/i) ? 'task' : undefined;
						console.log("User's response:", outcome);

						var waitingJob = result.filter(obj => { return obj.data.to === req.body.From })
						console.log(`Total: ${result.length}, # of waiting jobs for ${req.body.From}`, waitingJob.length)
						var openJob = waitingJob.filter(obj => { return obj.data.status === 'New' })
						if (outcome == "task") {
							if (waitingJob.length < 1) return 'There were no pending task for you'
							return (openJob.length < 1) ? `There were no pending task for you` : openJob.map(x => `${x.id}, ${x.data.taskName}`).join('\n');
						}

						if (outcome === undefined) return `Failed interprete your reply: ${msg}, reply "?" to get help`;
						if (openJob.length < 1) return `There were no pending task to ${outcome}`;

						var replyMsg = "";

						return taskqueries.resume(openJob[0], outcome)
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
					.then(replyMsg => {
						console.log(`replyMsg: ${replyMsg}`)
						twiml.message(replyMsg);
						res.writeHead(200, { 'Content-Type': 'text/xml' });
						res.end(twiml.toString());
					})
					.catch(alert => {
						console.log("ops!alert:", alert);
						twiml.message('Failed!');
						res.writeHead(200, { 'Content-Type': 'text/xml' });
						res.end(twiml.toString());
					})
				break
		}

		//console.log("SESSION: ", req.session)
		//res.set('Content-Type', 'text/xml')
	})

	// Routes
	app.get("/queues-v2", queueController.getQueues);
	app.get("/queue-v2/:queueName/jobs", queueController.getJobs);
	app.get("/queue-v2/:queueName/:jobId/jobsdetails", queueController.getJobsDetails);
	app.get("/queue-v2/:queueName/job/:jobId", queueController.getJobById);
	app.get("/getAllRepeatableJobs", queueController.getAllRepeatableJobs);
}