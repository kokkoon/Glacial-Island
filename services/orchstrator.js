// flow actions/activities/functions examples to be implemented in it's own module and imported here
const JSONPath = require('jsonpath');
const jsonLogic = require('json-logic-js');
const Bull = require('bull');
const keys = require('../config/keys');
const QUEUE_NAME = "SERVICE";
const REDIS_URL = keys.redisURL;
const serviceQueue = new Bull(QUEUE_NAME, REDIS_URL);
const resQueue = new Bull('REPONSE', REDIS_URL);
const moment = require('moment');
const twilio = require('twilio');
const client = new twilio(keys.twilioAccountSid, keys.twilioAuthToken);

const doFunction = (job, node) => {
  new Promise(res => setTimeout(res, 2000))
};

var level = 0;
var currentNode = {};

//exec first node and continue recurse
var exec1 = async (job, actions) => {
  if (actions.length > 0) {
    const first = actions.shift();
    console.log("line 22", first.taskType, first.configuration.actionName)
    
    var logObj = {timestamp: moment(), status: "Start", activity: first.configuration.actionTitle, log: `Starts ${first.configuration.actionTitle}`};
    console.log(actions.length, JSON.stringify(logObj))
    job.log(JSON.stringify(logObj))

    if (first.taskType == "logic") {
      switch (first.configuration.actionName) {
        case "IF_ELSE":
          level=level+1;
          currentNode = {...first};
          //var logObj = {timestamp: moment(), status: "Start", activity: first.configuration.actionTitle, log: `Starts ${first.configuration.actionTitle}`};
          //job.log(JSON.stringify(logObj))
          //console.log(job.data.definition["variables"]);
          if (first.rules && jsonLogic.apply(first.rules, job.data.definition["variables"])) {
            await exec1(job, JSONPath.query(first, '$..branches[?(@.condition==true)].actions')[0])
          } else {
            await exec1(job, JSONPath.query(first, '$..branches[?(@.condition==false)].actions')[0])
          }
          //log exit if_else branch here..
          var logObj = {timestamp: moment(), status: "Exit branch", activity: first.configuration.actionTitle, log: `Exit branch ${first.configuration.actionTitle}`};
          console.log(actions.length, JSON.stringify(logObj));
          job.log(JSON.stringify(logObj));
          level = level -1;
          break
        case "RUN_IF":
          break
        case "WHILE":
          while (first.rules && jsonLogic.apply(first.rules, job.data.definition["variables"])) {
            await exac1(job, first.branches[0].actions)
          }
          var logObj = {timestamp: moment(), status: "Exit branch", activity: first.configuration.actionTitle, log: `Exit branch ${first.configuration.actionTitle}`};
          console.log(actions.length, JSON.stringify(logObj));
          job.log(JSON.stringify(logObj));
          break
        default:
          break
      }
    } else if (first.taskType =="get-response") {

      //assign a task and pause..
      if (job.data.state !== "Paused" ) {
        var promise = client.messages.create({
            from: 'whatsapp:+14155238886',
            body: 'Please reply approve/reject',
            to: 'whatsapp:+6583327738'
          });

        promise.then(message => {
            console.log(message.sid)
            job.data.messageSID = message.sid;
            job.update(job.data);
          }, error => {
            console.error(error.message)
          });

        promise.then(message => {
          resQueue.add({
            instanceId: job.id,
            state: job.data.state,
            from: 'whatsapp:+14155238886',
            to: 'whatsapp:+6583327738'
          })
          .then(result => {
              console.log(result)
            }, error => {
            //
            })
          .catch(alert => {
            console.log("alert:", alert)
          })
        });

        job.data.state = "Paused";
        actions.unshift(first);
        job.update(job.data);
        
        logObj = {timestamp: moment(), status: "Waiting", activity: first.configuration.actionTitle, log: `Wait for ${first.configuration.actionTitle}`};
        console.log(actions.length, JSON.stringify(logObj));
        job.log(JSON.stringify(logObj));

        return "Paused"
      } else {
        console.log(first.configuration.properties.outcome)
        if (true) {
          await exec1(job, JSONPath.query(first, '$..branches[?(@.condition==true)].actions')[0])
        } else {
          await exec1(job, JSONPath.query(first, '$..branches[?(@.condition==false)].actions')[0])
        }
        
        logObj = {timestamp: moment(), status: "End", activity: first.configuration.actionTitle, log: `Exiting ${first.configuration.actionTitle}`};
        console.log(actions.length, JSON.stringify(logObj));
        job.log(JSON.stringify(logObj));
      }
    } else if (first.taskType == "service") {
      console.log("Execute service");
      //serviceQueue.add(first)
      //  .then(job => {console.log("jobId:", job.id)})
      //  .catch(alert => {console.log("alert:", alert)})

      //let serviceJob = await serviceQueue.add(first);
      //let result = await serviceJob.finished();
      //console.log(result)
      logObj = {timestamp: moment(), status: "End", activity: first.configuration.actionTitle, log: `Exiting ${first.configuration.actionTitle}`};
      console.log(actions.length, JSON.stringify(logObj));
      job.log(JSON.stringify(logObj));
    } else {

      
      console.log("Execute doFunction");
      // do task execution
      await doFunction(job, first);
     
      
      //finish doing task..
      //loginst = (moment()) + `: Ended ${first.name}, ${first.title}`;
      logObj = {timestamp: moment(), status: "End", activity: first.configuration.actionTitle, log: `Exiting ${first.configuration.actionTitle}`};
      console.log(actions.length, JSON.stringify(logObj));
      job.log(JSON.stringify(logObj));

    }
  } else {
    return "Completed";
  }
  return await exec1(job, actions)

}

var startflow = async (job) => {
  // Start executing workflow actions...
  var state = await exec1(job, job.data.definition.actions);

  //Exited execution of workflow actions
  job.data.state = state;
  job.data.jobEnd = moment();
  if (state === "Completed") job.data.end = moment();
  job.update(job.data);
}

module.exports = {
  startflow: startflow
}
