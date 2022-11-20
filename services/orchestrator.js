// flow actions/activities/functions examples to be implemented in it's own module and imported here
const JSONPath = require('jsonpath');
const jsonLogic = require('json-logic-js');
const Bull = require('bull');
const keys = require('../config/keys');
const NODE_ENV = process.env.NODE_ENV || "development";
const MSG_QUEUE = 'MESSENGER@' + NODE_ENV;
//const QUEUE_NAME = "SERVICE";
const REDIS_URL = keys.redisURL;
//const serviceQueue = new Bull(QUEUE_NAME, REDIS_URL);
const msgQueue = new Bull(MSG_QUEUE, REDIS_URL);
const moment = require('moment');
const twilio = require('twilio');
//const client = new twilio(keys.twilioAccountSid, keys.twilioAuthToken);
const math = require('mathjs');
const redisqueries = require('./redisqueries');
const { nanoid } = require('nanoid');
const workflowController = require("../controller/workflow_v3.controller")
const str2Json = str => {
  console.log(str)
  try {
      return JSON.parse(str);
  } catch (e) {
      return {};
  }
}

const parseVariable = (str, data) => {
  var varNames = str.match(/(?<=\<\<).+?(?=\>\>)/g);
  console.log(varNames)
  varNames && varNames.map((varName, i) => {
    var regex = new RegExp("\<\<" + varName + "\>\>");
    str = str.replace(regex, data[varName]);
  })
  return str
}

const doFunction = (job, node) => {
  //new Promise(res => setTimeout(res, 2000))
  return new Promise(async (resolve, reject) => {
    switch (node.datatype.type) { //or node.configuration.actionName
      case "log_message":
        var logMsg = node.configuration.properties.message;
        var logObj = {
          timestamp: moment(),
          actionId: node.actionId,
          status: "Custom",
          activity: node.configuration.actionName,
          log: `${parseVariable(logMsg, job.data.data)}`
        };
        console.log(JSON.stringify(logObj))
        job.log(JSON.stringify(logObj))
        resolve(true)
        break
      case "set_variables":
        var vars = node.configuration.properties;
        var logMsg = "";
        vars.map((v) => {
          var varObj = job.data.definition.variables.find(obj => {return obj.name === v.var});
          var i = job.data.definition.variables.findIndex(obj => {return obj.name === v.var});
          switch (varObj.type) {
            case "number":
              //job.data.definition.variables[i].value = Number(v.val)
              job.data.data[v.var] = Number(v.val)
              logMsg = (logMsg == ""? "": `${logMsg}; `) + v.var + " = " + v.val;
              break
            case "boolean":
              //job.data.definition.variables[i].value = (/^\s*(true|1|on)\s*$/i).test(v.val)
              job.data.data[v.var] = (/^\s*(true|1|on)\s*$/i).test(v.val)
              logMsg = (logMsg == ""? "": `${logMsg}; `) + v.var + " = " + v.val;
              break
            case "array":
              //job.data.definition.variables[i].value = JSON.parse(v.val)
              job.data.data[v.var] = JSON.parse(v.val)
              logMsg = (logMsg == ""? "": `${logMsg}; `) + v.var + " = " + v.val;
              break
            case "object":
              //job.data.definition.variables[i].value = JSON.parse(v.val)
              job.data.data[v.var] = JSON.parse(v.val)
              logMsg = (logMsg == ""? "": `${logMsg}; `) + v.var + " = " + v.val;
              break
            default: //string as default
              //job.data.definition.variables[i].value = v.val
              job.data.data[v.var] = v.val
              logMsg = (logMsg == ""? "": `${logMsg}; `) + v.var + " = " + v.val;
              break
          }
          console.log(v.var, job.data.data[v.var])
        });
        var logObj = {timestamp: moment(), actionId: node.actionId, status: "Custom", activity: node.configuration.actionName, log: `Set ${logMsg}`};
        console.log(JSON.stringify(logObj))
        job.log(JSON.stringify(logObj))
        resolve(true)
        break
      case "create_variable":
        var variable = node.configuration.properties;
        var logMsg = "";
        switch (variable.type) {
          case "number":
            console.log("number variable")
            job.data.data[variable.name] = math.evaluate(parseVariable(variable.value, job.data.data))
            break
          default:
            console.log("default case ")
            var val = await parseVariable(variable.value, job.data.data)
            job.data.data[variable.name] = val
            break
        }
        logMsg = variable.name + " as " + variable.value;
        var logObj = {timestamp: moment(), actionId: node.actionId, status: "Custom", activity: node.configuration.actionName, log: `Set ${logMsg}`};
        console.log(JSON.stringify(logObj))
        job.log(JSON.stringify(logObj))
        resolve(true)
        break
      default:
        break
    }
    resolve(true)
  })
};

var level = 0;
var currentNode = {};

//exec first node and continue recurse
var exec1 = async (job, actions) => {
  console.log("=======actions=========")
  if (actions.length > 0) {

    const first = actions.shift();

    if (!first.hasOwnProperty('actionId')) first.actionId = `${first.number}-${nanoid(6)}`;

    var logObj = { timestamp: moment(), actionId: first.actionId, status: "Start", activity: first.configuration.actionTitle, log: `Starts ${first.configuration.actionTitle}` };
    console.log(actions.length, JSON.stringify(logObj))
    job.log(JSON.stringify(logObj))

    if (first.taskType == "logic") {
      switch (first.configuration.actionName) {
        case "IF_ELSE":
          level=level+1;
          currentNode = {...first};
          var operator = first.configuration.properties.operator;
          var operand1 = first.configuration.properties.operand1;
          var operand2 = first.configuration.properties.operand2;
          var rules = JSON.parse(`{"${operator}": [{"var":"${operand1}"}, ${operand2}]}`);
          var logMsg = `${operand1} ${operator} ${operand2}`;
          var logObj = {timestamp: moment(), actionId: first.actionId, status: "Custom", activity: first.configuration.actionTitle, log: `${logMsg}`};
        
          console.log(JSON.stringify(logObj))
          job.log(JSON.stringify(logObj))
          var j = job.data.state;
         
          if (first.hasOwnProperty('current_branch')) {
            var branchActions = first.current_branch.actions;
          } else {
            if (Object.keys(rules).length !== 0) 
            if (jsonLogic.apply(rules, job.data.data)) {
              var branchActions = JSONPath.query(first, '$..branches[?(@.condition==true)].actions')[0];
            } else {
              var branchActions = JSONPath.query(first, '$..branches[?(@.condition==false)].actions')[0];
            }
          };
       
          j = await exec1(job, branchActions)
       
          console.log("j of IF_ELSE", j)
       
          if (j == "Paused") {
            job.data.state = j;
            first.current_branch = {}
            first.current_branch.actions = branchActions;
            actions.unshift(first);
            job.update(job.data);
            return j;
          }
          //log exit if_else branch here..
          logObj = {timestamp: moment(), actionId: first.actionId, status: "End", activity: first.configuration.actionTitle, log: `Exit branch ${first.configuration.actionTitle}`};
          console.log(actions.length, JSON.stringify(logObj));
          job.log(JSON.stringify(logObj));
          level = level -1;
          break
        case "WHILE":
          var operator = first.configuration.properties.operator;
          var operand1 = first.configuration.properties.operand1;
          var operand2 = first.configuration.properties.operand2;
          var rules = JSON.parse(`{"${operator}": [{"var":"${operand1}"}, ${operand2}]}`);
          var logMsg = `While ${operand1} ${operator} ${operand2}`;
          var logObj = {timestamp: moment(), actionId: first.actionId, status: "Custom", activity: first.configuration.actionTitle, log: `${logMsg}`};
          console.log(JSON.stringify(logObj))
          job.log(JSON.stringify(logObj))
          var branchActions = first.branches[0].hasOwnProperty('current_actions')? first.branches[0].current_actions : JSON.parse(JSON.stringify(first.branches[0].actions));
          var j = job.data.state;
          if (Object.keys(rules).length !== 0) 
          while (jsonLogic.apply(rules, job.data.data)) {
            j = await exec1(job, branchActions)
            console.log("J:", j)
            if (j == "Paused") {
              job.data.state = j
              first.branches[0].current_actions = branchActions;
              actions.unshift(first);
              //job.data.definition.actions = actions;
              job.update(job.data);
              return j;
            }
            branchActions = JSON.parse(JSON.stringify(first.branches[0].actions));
          }
          console.log("job.data.state", job.data.state, " j--->", j);
          if (j !== "Paused") {
            logObj = {timestamp: moment(), actionId: first.actionId, status: "End", activity: first.configuration.actionTitle, log: `Exit branch ${first.configuration.actionTitle}`};
            console.log(actions.length, JSON.stringify(logObj));
            job.log(JSON.stringify(logObj));
          }
          console.log("J", j)
          break
        case "RUN_IF":
          break
        default:
          break
      }
    } else if (first.taskType =="get-response") {

     /* if (first.hasOwnProperty('current_branch') && first.current_branch.length > 0) {
        var j = job.data.state;
        var branchActions = first.current_branch.actions;
        j = await exec1(job, branchActions)
      } else */
      if (job.data.state !== "Paused") {  //assign a task and pause..
        //assign task to first.configuration.properties.assignee.assignee
        var validPhone = /^\+?[1-9]\d{9,14}$/;
        var assigneeList = first.configuration.properties.assignee.assignee.split(/[,;]+/);
        assigneeList = assigneeList.map(e => validPhone.test(e.trim().replace(/[ -]/g, ''))?e.trim().replace(/[ -]/g, ''):e.trim());
        console.log(assigneeList);

        var taskList = [];
        
				var createTaskList = new Promise((resolve, reject) => {
          assigneeList.forEach((assignee, i, arr) => {
            redisqueries.instanceNumber(`bull:${MSG_QUEUE}:id`)
              .then(taskId => {
                console.log(assignee)
                const taskData = {...first.configuration.properties};
                taskData.name = first.configuration.properties.taskName; 
                taskData.owner = assignee.trim();
                taskData.tenant = job.data.tenant;
                taskData.status = "New";
                taskData.response = "";
                taskData.taskDesc = first.configuration.properties.taskDesc;
                taskData.instanceId = job.id;
                taskData.actionId = first.actionId;
                taskData.state = job.data.state;
                taskData.linkedTask = i===0 ? taskId : taskList[0].data.linkedTask;
                taskData.taskId = taskId;
                const JobOpts = {jobId: assignee + "-" + taskData.linkedTask + "-" + taskId, removeOnComplete: true};
                taskList.push({data: taskData, opts: JobOpts})
                msgQueue.add(taskData, JobOpts)
                .then(result => {
                    //console.log(result)
                  }, error => {
                    //
                  })
                .catch(alert => {
                  console.log("alert:", alert)
                })
              })
              .then(() => {
                if (i === arr.length -1) {
                  resolve(taskList)
                };
              })
              .catch(alert => {
                console.log("Oh-o! alert:", alert)
              });
          })
        })

        createTaskList.then(taskList => {
          console.log("taskList:",taskList.length)

          job.data.state = "Paused";
          job.data.waitForResponse = true;
          actions.unshift(first);
          job.update(job.data);

          var tasks = taskList.map(ta => ta.data.owner).join()
          console.log(tasks)
          logObj = {
            timestamp: moment(), actionId: first.actionId, status: "Waiting", activity: first.configuration.actionTitle,
            log: `Task(s) [${taskList.map(ta => ta.data.taskId).join()}] \nassigned to [${tasks}]`
          };
          console.log(actions.length, JSON.stringify(logObj));
          job.log(JSON.stringify(logObj));
        })


        return "Paused"
      } else {
        var outcome = job.data.outcome; //first.configuration.properties.outcome;
        console.log(outcome);
        var j = job.data.state;
        job.data.state = "Active";
        job.update(job.data)
        if (first.hasOwnProperty('current_branch')) {
          var branchActions = first.current_branch.actions;
        } else {
          if (outcome=='approved') {
            var branchActions = JSONPath.query(first, '$..branches[?(@.condition==true)].actions')[0];
            //job.data.state = await exec1(job, JSONPath.query(first, '$..branches[?(@.condition==true)].actions')[0])
          } else {
            var branchActions = JSONPath.query(first, '$..branches[?(@.condition==false)].actions')[0];
            //job.data.state = await exec1(job, JSONPath.query(first, '$..branches[?(@.condition==false)].actions')[0])
          }
        }
        job.data.current_branch = branchActions;
        job.update(job.data);
        j = await exec1(job, branchActions)

      }


      logObj = {timestamp: moment(), actionId: first.actionId, status: "End", activity: first.configuration.actionTitle, log: `Exiting ${first.configuration.actionTitle}`};
      console.log(actions.length, JSON.stringify(logObj));
      job.log(JSON.stringify(logObj));
      //job.data.state = "Active";
      job.update(job.data);

    } else if (first.taskType == "service") {
      console.log("Execute service");
      //serviceQueue.add(first)
      //  .then(job => {console.log("jobId:", job.id)})
      //  .catch(alert => {console.log("alert:", alert)})

      //let serviceJob = await serviceQueue.add(first);
      //let result = await serviceJob.finished();
      //console.log(result)
      logObj = {timestamp: moment(), actionId: first.actionId, status: "End", activity: first.configuration.actionTitle, log: `Exiting ${first.configuration.actionTitle}`};
      console.log(actions.length, JSON.stringify(logObj));
      job.log(JSON.stringify(logObj));
    } else { //do function

      
      console.log("Execute doFunction");
      // do task execution
      await doFunction(job, first);
    
      
      //finish doing task..
      //loginst = (moment()) + `: Ended ${first.name}, ${first.title}`;
      logObj = {timestamp: moment(), actionId: first.actionId, status: "End", activity: first.configuration.actionTitle, log: `Exiting ${first.configuration.actionTitle}`};
      console.log(actions.length, JSON.stringify(logObj));
      job.log(JSON.stringify(logObj));

    }
  } else {
    return "Completed";
  }      
  return await exec1(job, actions);
}

var startflow = async (job) => {
  // Start executing workflow actions...
  console.log("Start executing workflow actions...");
  var state = null;
  debugger
  if (job.data.workflow_definition) {
    state = await workflowController.startExcution(job, job.data.workflow_definition.variables, job.data.workflow_definition.actions[0].actions, true);
  } else {
    state = await exec1(job, job.data.definition.actions);
  }


  console.log("==============Job Completed..=================", state);
  //Exited execution of workflow actions
  job.data.state = state;
  job.data.jobEnd = moment();
  job.data['end'] = (state === "Completed") ? moment() : undefined;
  job.update(job.data);
}

module.exports = {
  startflow: startflow
}
