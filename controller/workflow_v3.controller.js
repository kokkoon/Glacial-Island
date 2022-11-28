
const JSONPath = require('jsonpath');
const jsonLogic = require('json-logic-js');
const Bull = require('bull');
const moment = require('moment');
const twilio = require('twilio');
const math = require('mathjs');
const { nanoid } = require('nanoid');
const replaceall = require("replaceall");
const request = require("request-promise");
const ejs = require('ejs');
const jp = JSONPath;
//ENV config
const keys = require('../config/keys');

//ENV NODE_ENV
const NODE_ENV = process.env.NODE_ENV || "local";
const MSG_QUEUE = 'MESSENGER@' + NODE_ENV;
const msgQueue = new Bull(MSG_QUEUE, keys.redisURL);
console.log('connect', keys.redisURL);

//ENV redisqueries
const redisqueries = require('../services/redisqueries');
const SendMail = require('../services/SendMail');
const { parseVariable } = require('./helper.controller');
const getvariables = require('./getvariables.controller');

//SMS Config
const accountSid = keys.twilioAccountSid;
const authToken = keys.twilioAuthToken;


const startExcution = async (job, variables, actions, intialExcution) => {
    return new Promise(async (resolve, reject) => {
        try {
            let varVault = intialExcution ? {} : variables, edges = actions.edges, nodes = actions.nodes;
            let actionStatus = 'Completed'
            if (intialExcution) {
                let variablesData = await getvariables(variables);
                Object.keys(variablesData).forEach(ele => {
                    varVault[ele] = JSON.stringify(variablesData[ele])
                });
            }

            let actionLists = JSON.parse(JSON.stringify(actions));

            while (actionLists.length > 0) {
                var action = actionLists.shift();

                if (!action.hasOwnProperty('actionId')) action.actionId = `${action.number}-${nanoid(6)}`;
                var logObj = { timestamp: moment(), actionId: action.actionId, status: "Start", activity: action.configuration.actionTitle, log: `Starts ${action.configuration.actionTitle}` };
                console.log(actions.length, JSON.stringify(logObj))
                job.log(JSON.stringify(logObj))
                var j = job.data.state;
                var resData = await callAction(job, varVault, action, actionLists, actionStatus);
                varVault = resData.varVault;
                actionLists = resData.actionLists
                job = resData.job;
                actionStatus = resData.actionStatus
            }

            resolve(actionStatus == 'Active' ? 'Completed' : actionStatus)
        } catch (err) {
            console.log(err.message);
            resolve("Failed")
        }
    });

}

//Action Lists 
const callAction = (job, varVault, action, actionLists, actionStatus) => {
    console.log("===========================================");
    console.log(job.data.state);
    // job.data.data[variable.name]
    console.log("===========================================");
    return new Promise(async (resolve, reject) => {
        try {
            switch (action.configuration.nodeType) {
                case "Query Json":
                    varVault = await queryJson(varVault, action);
                    break
                case "Send Email":
                    const reqEmailData = await sendEmail(job, varVault, action);
                    job = reqEmailData.job;
                    actionStatus = job.data.state;
                    break
                case "Send SMS":
                    const reqSMSData = await sendSMS(job, varVault, action);
                    job = reqSMSData.job;
                    actionStatus = job.data.state;
                    break
                case "Log Message":
                    const properties = (action && action.configuration) ? action.configuration.properties : "";
                    if (properties) {
                        var logMsg = ejsRender(properties.value, varVault)
                        var logObj = {
                            timestamp: moment(),
                            actionId: action.id,
                            status: "Custom",
                            activity: action.text,
                            log: logMsg
                        };
                        job.log(JSON.stringify(logObj))
                        actionStatus = job.data.state;
                    }
                    break
                case "Loop":
                    const reqLoopData = await loopFunction(job, varVault, action, actionLists);
                    job = reqLoopData.job;
                    actionStatus = job.data.state;
                    break
                case "Condition":
                    const reqConditionData = await callCondition(job, varVault, action, actionLists);
                    job = reqConditionData.job;
                    actionStatus = job.data.state;
                    break
                case "Assign Task":
                    //For Testing
                    const reqAssignTaskData = await callAssignTask(job, varVault, action, actionLists);
                    job = reqAssignTaskData.job;
                    actionLists = reqAssignTaskData.actions;
                    actionStatus = reqAssignTaskData.actionStatus;

                    break
                case "BRANCH":
                    //For Testing
                    await callCondition(job, varVault, action, actionLists);
                    break
                case "Call Web Service":
                    const reqWebServiceData = await callWebService(action, varVault);
                    job = reqWebServiceData.job;
                    varVault = reqWebServiceData.varVault;
                    actionStatus = job.data.state;
                    break
                default:
                    console.log("run other actions")
                    break
            }
            resolve({ varVault, action, actionLists, job, actionStatus: actionStatus })
        } catch (err) {
            console.log(err.message);
            reject(false)
        }
    })
}

const callAssignTask = (job, varVault, action, actions, actionStatus) => {
    return new Promise(async (resolve, reject) => {
        try {
            debugger
            let properties = (action && action.configuration) ? action.configuration.properties : "";
            if (properties) {
                properties['nodeType'] = action.nodeType;
                if (job.data.state !== "Paused") {
                    var validPhone = /^\+?[1-9]\d{9,14}$/;
                    var assigneeList = properties.assign.split(/[,;]+/);
                    assigneeList = assigneeList.map(e => validPhone.test(e.trim().replace(/[ -]/g, '')) ? e.trim().replace(/[ -]/g, '') : e.trim());
                    var taskList = [];

                    let count = 0;
                    while (assigneeList.length > count) {
                        let assignee = assigneeList[count];
                        const taskId = await redisqueries.instanceNumber(`bull:${MSG_QUEUE}:id`);
                        const taskData = { ...properties };
                        taskData.name = properties.taskName;
                        taskData.owner = assignee.trim();
                        taskData.tenant = job.data.tenant;
                        taskData.status = "New";
                        taskData.response = "";
                        taskData.taskDesc = properties.taskDesc;
                        taskData.instanceId = job.id;
                        taskData.actionId = action.actionId;
                        taskData.state = job.data.state;
                        taskData.linkedTask = count === 0 ? taskId : taskList[0].data.linkedTask;
                        taskData.taskId = taskId;
                        const JobOpts = { jobId: assignee + "-" + taskData.linkedTask + "-" + taskId, removeOnComplete: true };
                        taskList.push({ data: taskData, opts: JobOpts })
                        msgQueue.add(taskData, JobOpts);
                        count++;
                    }
                    console.log("taskList:", taskList.length)
                    job.data.state = "Paused";

                    job.data.waitForResponse = true;

                    actions.unshift(action);
                    job.update(job.data);
                    var tasks = taskList.map(ta => ta.data.owner).join()

                    let logObj = {
                        timestamp: moment(), actionId: action.actionId, status: "Waiting", activity: action.configuration.actionTitle,
                        log: `Task(s) [${taskList.map(ta => ta.data.taskId).join()}] \nassigned to [${tasks}]`
                    };

                    job.log(JSON.stringify(logObj));
                    actionStatus = "Paused"
                } else {
                    var outcome = job.data.outcome;
                    var j = job.data.state;
                    job.data.state = "Active";
                    job.update(job.data)

                    if (action.hasOwnProperty('current_branch')) {
                        var branchActions = action.current_branch.actions;
                    } else {
                        if (outcome == 'approved') {
                            var branchActions = JSONPath.query(action, '$..branches[?(@.condition==true)].actions')[0];
                        } else {
                            var branchActions = JSONPath.query(action, '$..branches[?(@.condition==false)].actions')[0];
                        }
                    }
                    job.data.current_branch = branchActions;
                    job.update(job.data);
                    const resdata = await startExcution(job, varVault, branchActions);
                    actionStatus = "Active"
                }

                logObj = { timestamp: moment(), actionId: action.actionId, status: "End", activity: action.configuration.actionTitle, log: `Exiting ${action.configuration.actionTitle}` };
                job.log(JSON.stringify(logObj));
                //job.data.state = "Active";
                job.update(job.data);
            }
            resolve({ job, actions, actionStatus })
        } catch (err) {
            resolve({ job })
        }
    })
}

const callCondition = (job, varVault, action, mainAction) => {
    return new Promise(async (resolve, reject) => {
        try {
            let properties = (action && action.configuration) ? action.configuration.properties : "";
            if (properties) {
                properties['nodeType'] = action.nodeType
                let whenValue = "";
                let variableValue = false;
                if (properties.type == "variable") {
                    whenValue = await replaceVariables(properties.whencondition, varVault, true);
                } else {
                    whenValue = properties.whencondition;
                }

                switch (properties.operator) {
                    case 'equals':
                        variableValue = whenValue == properties.value;
                        break;
                    case 'not_equals':
                        variableValue = whenValue != properties.value;
                        break;
                    case 'is_empty':
                        variableValue = (whenValue == null || whenValue == "");
                        break;
                    case 'is_not_empty':
                        variableValue = (whenValue != null || whenValue != "");
                        break;
                }
                varVault[properties.variable] = variableValue;
                if (variableValue) {
                    branchActions = JSONPath.query(action, '$..branches[?(@.condition==true)].actions')[0];
                } else {
                    branchActions = JSONPath.query(action, '$..branches[?(@.condition==false)].actions')[0];
                }

                var logMsg = `${properties.operator} ${whenValue} ${properties.value} ${'Condition : ' + variableValue}`;

                var logObj = { timestamp: moment(), actionId: action.actionId, status: "Custom", activity: action.configuration.actionTitle, log: `${logMsg}` };
                job.log(JSON.stringify(logObj))
                var j = job.data.state;

                console.log(j);
                const resdata = await startExcution(job, varVault, branchActions);

                { job }
            } else {
                { job }
            }
        } catch (err) {
            { job }
        }
    });
}


const loopFunction = async (job, varVault, action, mainAction) => {
    const properties = (action && action.configuration) ? action.configuration.properties : "";
    if (properties) {
        try {
            let loopLength = 0, count = 0;
            if (properties.type == "variable") {
                loopLength = Number(ejsRender(properties.value, varVault))
            } else if (properties.type == "json") {
                loopLength = JSON.parse(properties.value) ? JSON.parse(properties.value).length : 0;
            }

            while (count < loopLength) {
                varVault["loop_index_id"] = JSON.stringify(count)

                if (properties.type == "variable") {
                    varVault[properties.variable] = replaceVariables(properties.selected_variable, varVault, true);
                } else if (properties.type == "json") {
                    varVault[properties.variable] = properties.value
                }

                if (isCheckJSONParse(varVault[properties.variable])) {
                    varVault[properties.variable] = JSON.parse(varVault[properties.variable]);
                    varVault[properties.variable] = (varVault[properties.variable] && varVault[properties.variable].length) ? JSON.stringify(varVault[properties.variable][count]) : {}
                }

                let loopActions = JSON.parse(JSON.stringify(action.branches[0].actions));
                const resdata = await startExcution(job, varVault, loopActions);
                count++
            }
            { job }
        } catch (err) {
            { job }
        }
    } else {
        { job }
    }
}

const convertString = (string, varVault) => {
    try {
        var replaceVariablesStr = "";
        const regex = /\{{([0-9a-zA-Z-_., \/\']+)\}}/gm;
        if (!regex.exec(string)) {
            replaceVariablesStr = string
        } else {
            replaceVariablesStr = replaceVariables(string, varVault, true)
        }
        let tempobjStr = JSON.stringify({ "v1": replaceVariablesStr })
        return tempobjStr.substring(7, tempobjStr.length - 2)
    } catch (err) {
        return string
    }
}

const getObjectKey = (string, index) => {
    return replaceall("}}", "", replaceall("{{", "", string)).split(".")[index];
}

const replaceVariables = (action, varVault, isString) => {
    try {
        var gv = [], s, string = isString ? action : JSON.stringify(action);
        const regex = /\{{([0-9a-zA-Z-_., \/\']+)\}}/gm;
        while ((s = regex.exec(string)) !== null) {
            if (s.index === regex.lastIndex) {
                regex.lastIndex++;
            }
            gv.push(s[0]);
        }
        for (let index = 0; index < gv.length; index++) {
            if (gv[index].search("window.eventForm") == -1) {
                const objectKey = getObjectKey(gv[index], 0);
                if (varVault[objectKey]) {
                    let varData = (JSON.parse(varVault[objectKey]) && JSON.parse(varVault[objectKey]).result) ? JSON.parse(varVault[objectKey]).result : JSON.parse(varVault[objectKey]);
                    if (varData != "") {
                        let objectkeylen = replaceall("}}", "", replaceall("{{", "", gv[index]))
                        const pathExp = "$." + objectkeylen + "";
                        const editDataTableComponents = jp.query({ [objectKey]: varData }, pathExp, 1000);
                        if (editDataTableComponents[0]) {
                            string = replaceall(gv[index], (isCheckString(editDataTableComponents[0]) ? convertString(editDataTableComponents[0], varVault) : JSON.stringify(editDataTableComponents[0])), string)
                        } else {
                            string = replaceall(gv[index], "", string)
                        }
                    }
                }
            }
        }
        return isString ? string : JSON.parse(string)
    } catch (err) {
        return isString ? string : JSON.parse(string)
    }

}

const isCheckString = (string) => {
    try {
        if (typeof string == 'string') {
            return true;
        } else {
            return false;
        }
    } catch (e) {
        return false;
    }
}

const isCheckJSONParse = (string) => {
    try {
        if (JSON.parse(string)) {
            return true;
        } else {
            return false;
        }
    } catch (e) {
        return false;
    }
}

const sendEmail = async (job, varVault, action) => {
    try {
        let properties = (action && action.configuration) ? action.configuration.properties : "";
        if (properties) {
            properties['nodeType'] = action.nodeType
            console.log(varVault);
            const startTime = moment();
            const mailOptions = {
                from: "'Glozic' <workflow@glozic.com>", // sender address
                emailTo: ejsRender(properties.sendTo, varVault), // list of receivers
                emailSubject: (properties.subject ? "Re: " + ejsRender(properties.subject, varVault) : "Glozic workflow"), // Subject line
                emailBody: "parsed reply", // plain text body
            };
            console.log(mailOptions);
            mailOptions.emailBody = ejsRender(properties.messageBody, varVault);
            await SendMail.sendEmail(mailOptions);
            joblogs(job, startTime, properties)
        }
        return { job }
    } catch (err) {
        console.log(err.message);
        console.log("Email service failed...")
        return { job }
    }
}


const sendSMS = async (job, varVault, action) => {
    try {

        let properties = (action && action.configuration) ? action.configuration.properties : "";
        if (properties) {
            properties['nodeType'] = action.nodeType
            const client = new twilio(accountSid, authToken);
            const tempActionDef = { ...properties };
            var str = properties.sendTo
            tempActionDef.sendTo = ejsRender(`${tempActionDef.sendTo}`, varVault);

            str = properties.messageBody
            tempActionDef.messageBody = ejsRender(`${tempActionDef.messageBody}`, varVault);

            var recipientList = tempActionDef.sendTo.split(/[,;]/)

            await recipientList.forEach(async (recipient) => {
                try {
                    const msg = await client.messages.create({ 
                        body: tempActionDef.messageBody, 
                        to: recipient, 
                        from: '+16262473170' 
                    }) //+12062079558
                    console.log(`Twilio message ID: ${msg.sid}`)
                } catch (err) {

                }
            });
            return { job }
        } else {
            return { job }
        }
    }
    catch (err) {
        console.log(err.message);
        console.log("SMS service failed...")
        return { job }
    }

}


const queryJson = (varVault, action) => {
    let properties = (action && action.configuration) ? action.configuration.properties : "";
    if (properties) {
        properties['nodeType'] = action.nodeType
        var jpQuery = (obj, pathExp, count) => {
            if (typeof obj !== 'object' || obj === null) return null
            return count ? JSONPath.query(obj, pathExp, count) : jp.query(obj, pathExp);
        }
        return new Promise(async (resolve, reject) => {
            try {
                if (Object.keys(varVault).length != 0) {
                    const JSONData = properties.formatType == "2" ? varVault[properties.jsonData] : properties.jsonData;
                    var resdata = (typeof JSONData == 'string') ? JSON.parse(JSONData) : JSONData;
                    if (properties.query) {
                        var resdata = await jpQuery((typeof JSONData == 'string') ? JSON.parse(JSONData) : JSONData, properties.query);
                    }
                    varVault[properties.variable] = resdata;
                }
                resolve(varVault)
            } catch (err) {
                console.log(err);
                Toast("Something went wrong action execution", "error")
                resolve(varVault)
            }
        });
    } else {
        resolve(varVault)
    }
}

const joblogs = (job, startTime, { message, id, text, nodeType }) => {
    try {
        var logMsg = nodeType;
        var logObj = {
            timestamp: moment(),
            actionId: id,
            status: "Custom",
            activity: text,
            start: startTime,
            end: moment(),
            log: `Set ${logMsg}`
        };

        job.log(JSON.stringify(logObj));
    } catch (err) {
        console.log(err.message)
    }
}

const ejsRender = (value, varVault) => {
    value = replaceall("}}", "%>", replaceall("{{", "<%=", value));
    value = replaceall("%}", "%>", replaceall("{%", "<%", value));
    let varVaultdata = {};
    Object.keys(varVault).forEach(ele => {
        varVaultdata[ele] = JSON.parse(varVault[ele])
    });
    const outputHtml = ejs.render(value, varVaultdata);
    return outputHtml;
}

const ejsRenderJson = (value, varVault) => {
    value = replaceall("}}", "%>", replaceall("{{", "<%=", value));
    value = replaceall("%}", "%>", replaceall("{%", "<%", value));
    let varVaultdata = {};
    Object.keys(varVault).forEach(ele => {
        varVaultdata[ele] = JSON.parse(varVault[ele])
    });
    const outputHtml = //ejs.render(value, varVaultdata);
        console.log(replaceall("&#34;", "'", outputHtml));
    let JSONData = replaceall("&#34;", "'", outputHtml)
    return JSONData;
}

const callWebService = async (actionDef, varVault) => {
    try {
        const url = actionDef.apiUrl
        const method = actionDef.reqMethod;
        const headers = actionDef.reqHeaders;
        const body = actionDef.reqBody;

        var options = {}
        options.method = method;
        options.url = url;
        options.headers = headers;
        method !== 'GET' ? options.body = body : null;
        //options.body = body;
        options.json = true;
        const resWebrequest = await request(options);
        varVault[actionDef.variable] = JSON.stringify(resWebrequest);
        return varVault
    } catch (err) {
        return varVault
    }

}

module.exports = {
    startExcution: startExcution
}
