const keys = require('../config/keys');
const NODE_ENV = process.env.NODE_ENV;
const Bull = require("bull");
const QUEUE_NAME= 'FLOW';
const TASK_QUEUE = 'TASK@' + NODE_ENV;
const EMAIL_QUEUE = 'EMAIL@' + NODE_ENV;
const flowQueue = new Bull(QUEUE_NAME, keys.redisURL);
const taskQueue = new Bull(TASK_QUEUE, keys.redisURL);
const emailQueue = new Bull(EMAIL_QUEUE, keys.redisURL);
const Auth = require("./authentication");
const redisqueries = require('./redisqueries');

const closePendingTasks = (task, outcome) => {
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

const resume = (task, outcome) => {
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

/**
 * Majority Algorithm by KK Gan
 * 
 * @param {*} nums 
 */

const majWithKKalgorithm = (nums) => {
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

module.exports = {
    closePendingTasks : closePendingTasks,
    resume : resume
}