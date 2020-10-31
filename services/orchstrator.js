// flow actions/activities/functions examples to be implemented in it's own module and imported here
const JSONPath = require('jsonpath');
const jsonLogic = require('json-logic-js');
const Bull = require('bull');
const keys = require('../config/keys');
const QUEUE_NAME = "SERVICE";
const REDIS_URL = keys.redisURL;
const serviceQueue = new Bull(QUEUE_NAME, REDIS_URL);
const moment = require('moment');

const doFunction = (job, node) => {
  new Promise(res => setTimeout(res, 2000))
};

var level = 0;
var currentNode = {};

//exec first node and continue recurse
var exec1 = async (job, actions) => {
  if (actions.length > 0) {
    const first = actions.shift();

    if (first.dataType == "logic") {
      switch (first.name) {
        case "IF_ELSE":
          level=level+1;
          currentNode = {...first};
          var logObj = {timestamp: moment(), status: "Start", activity: first.configuration.actionTitle, log: `Starts ${first.configuration.actionTitle}`};
          job.log(logObj)
          console.log(job.data.definition[first.data]);
          if (jsonLogic.apply(first.rules, job.data.definition[first.data])) {
            await exec1(job, JSONPath.query(first, '$..branches[?(@.condition==true)].actions')[0])
          } else {
            await exec1(job, JSONPath.query(first, '$..branches[?(@.condition==false)].actions')[0])
          }
          break
        case "RUN_IF":
          break
        case "WHILE":
          break
        default:
          break
      }
    } else if (first.dataType =="get-response") {
      // start executing task
      var logObj = {timestamp: moment(), status: "Start", activity: first.configuration.actionTitle, log: `Starts ${first.configuration.actionTitle}`};
      var loginst = (moment()) + `: Started ${first.name}, ${first.title}`;
      console.log(actions.length, loginst);
      job.log(logObj);

      //assign a task and pause..
      job.data.state = "Paused";
      job.update(job.data);
      
      //finish doing task..
      //loginst = (moment()) + `: Waiting ${first.name}, ${first.title}`;
      logObj = {timestamp: moment(), status: "Waiting", activity: first.configuration.actionTitle, log: `Wait for ${first.configuration.actionTitle}`};
      console.log(actions.length, JSON.stringify(logObj));
      job.log(JSON.stringify(logObj));
      return "Paused"
    } else {

      // start executing task
      //var loginst = (moment()) + `: Started ${first.name}, ${first.title}`;
      var loginst = {timestamp: moment(), status: "Start", activity: first.configuration.actionTitle, log: `Starts ${first.configuration.actionTitle}`};
      console.log(actions.length, loginst);
      job.log(JSON.stringify(loginst));

      switch (first.dataType) {
        case "service":
          console.log("Execute service");
          //serviceQueue.add(first)
          //  .then(job => {console.log("jobId:", job.id)})
          //  .catch(alert => {console.log("alert:", alert)})
          let serviceJob = await serviceQueue.add(first);
          let result = await serviceJob.finished();
          console.log(result)

          break
        default:
          console.log("Execute doFunction");
          // do task execution
          await doFunction(job, first)
          break
      }
     
      
      //finish doing task..
      //loginst = (moment()) + `: Ended ${first.name}, ${first.title}`;
      loginst = {timestamp: moment(), status: "End", activity: first.configuration.actionTitle, log: `Exiting ${first.configuration.actionTitle}`};
      console.log(actions.length, JSON.stringify(loginst));
      job.log(JSON.stringify(loginst));

    }
    await exec1(job, actions);
    
    return "active";
  } else {
    if (level < 1) {
      job.data.state = "Completed";
      job.update(job.data);
      logObj = {timestamp: moment(), status: "Completed", activity: first.configuration.actionTitle, log: `Completes ${first.configuration.actionTitle}`};
      job.log("Workflow completed")
    } else {
      logObj = {timestamp: moment(), status: "Exit branch", activity: first.configuration.actionTitle, log: `Exit branch ${first.configuration.actionTitle}`};
      job.log(JSON.stringify(logObj));
      level = level -1;
    };
    return "done";
  }
}

var startflow = async (job) => {
  job.data.state = "Active";
  await job.update(job.data);
  exec1(job, job.data.definition.actions);
}

module.exports = {
  startflow: startflow
}
