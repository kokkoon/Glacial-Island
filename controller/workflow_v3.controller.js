
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
const utility = require("../utils/utility")
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
const { parseVariable, isJSON, isCheckString, ejsRender } = require('./helper.controller');
const getvariables = require('./getvariables.controller');
const S = require('string');

//SMS Config
const accountSid = keys.twilioAccountSid;
const authToken = keys.twilioAuthToken;


const startExcution = async (job, variables, actions, intialExcution) => {
    return new Promise(async (resolve, reject) => {
        try {
            let varVault = intialExcution ? {} : variables, edges = actions.edges, nodes = actions.nodes;
            let actionStatus = 'Completed'
            if (intialExcution) {
                let variablesData = await getvariables(variables, {}, job);
                Object.keys(variablesData).forEach(ele => {
                    varVault[ele] = JSON.stringify(variablesData[ele])
                });
            }

            let actionLists = JSON.parse(JSON.stringify(actions));

            while (actionLists.length > 0) {
                var action = actionLists.shift();
                if (action.configuration.isDisabled === false) {
                    if (!action.hasOwnProperty('actionId')) action.actionId = `${action.number}-${nanoid(6)}`;
                    var logObj = { timestamp: moment(), actionId: action.actionId, status: "Start", activity: action.configuration.actionTitle, log: `Starts ${action.configuration.actionTitle}` };
                    console.log(actions.length, JSON.stringify(logObj))
                    job.log(JSON.stringify(logObj))
                    var j = job.data.state;
                    var resData = await callAction(job, varVault, action, actionLists, actionStatus);
                    varVault = resData.varVault;
                    actionLists = resData.actionLists
                    job = resData.job;

                    if (resData.actionStatus == "Paused") {
                        resolve({ status: resData.actionStatus, actions: actionLists });
                        break;
                    }
                    actionStatus = resData.actionStatus
                }
            }
            resolve({ status: actionStatus == 'Active' ? 'Completed' : actionStatus, actions: actionLists });
        } catch (err) {
            console.log(err.message);
            resolve("Failed")
        }
    });

}

//Action Lists 
const callAction = (job, varVault, action, actionLists, actionStatus) => {
    return new Promise(async (resolve, reject) => {
        try {
            switch (action.configuration.nodeType) {
                case "Query Json":
                    varVault = await queryJson(job, varVault, action);
                    break
                case "JS Editor":
                    reqjsEditorData = await jsEditor(varVault, action);
                    varVault = reqjsEditorData.varVault;
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
                        var logMsg = logMessage(job, varVault, action, properties)
                        var logObj = {
                            timestamp: moment(),
                            actionId: action.actionId,
                            status: "Custom",
                            activity: action.text,
                            log: S(logMsg).unescapeHTML().s
                        };
                        job.log(JSON.stringify(logObj))
                        actionStatus = job.data.state;
                    }
                    break
                case "Loop":
                    const reqLoopData = await loopFunction(job, varVault, action, actionLists);
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
                    const reqWebServiceData = await callWebService(action, varVault, job);
                    varVault = reqWebServiceData.varVault;
                    break
                case "Collection":
                    const reqCollectioneData = await callCollectionOperation(varVault, action, job);
                    varVault = reqCollectioneData.varVault;
                    break;
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
                    whenValue = await replaceVariablesString(properties.whencondition, varVault, true);
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
                        variableValue = (whenValue == null || whenValue == "" || !whenValue || whenValue==undefined)
                        break;
                    case 'is_not_empty':
                        variableValue = !(whenValue == null || whenValue == "" || !whenValue || whenValue==undefined)
                        break;
                    case 'less_then':
                        variableValue = Number(whenValue) < Number(properties.value);
                        break;
                    case 'less_then_equals':
                        variableValue = Number(whenValue) <= Number(properties.value);
                        break;
                    case 'greater_than':
                        variableValue = Number(whenValue) > Number(properties.value);
                        break;
                    case 'greater_than_equals':
                        variableValue = Number(whenValue) >= Number(properties.value);
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
                await startExcution(job, varVault, branchActions);
                job.data.state = "Active";
                resolve({ job })
            } else {
                resolve({ job })
            }
        } catch (err) {
            resolve({ job })
        }
    });
}

const loopFunction = async (job, varVault, action, mainAction) => {
    const properties = (action && action.configuration) ? action.configuration.properties : "";
    if (properties) {
        try {
            let loopLength = 0, count = 0, arrayLists = [];
            if (properties.type == "variable") {
                loopLength = Number(ejsRender(properties.value, varVault))
            } else if (properties.type == "json") {
                loopLength = JSON.parse(properties.value) ? JSON.parse(properties.value).length : 0;
            }

            if (properties.type == "variable") {
                arrayLists = replaceVariables(properties.selected_variable, varVault, true);
            } else if (properties.type == "json") {
                arrayLists = properties.value
            }

            while (count < loopLength) {
                varVault["loop_index_id"] = JSON.stringify(count)

                if (isCheckJSONParse(arrayLists)) {
                    let arrayJsonLists = JSON.parse(arrayLists);
                    varVault[properties.variable] = (arrayJsonLists && arrayJsonLists.length) ? JSON.stringify(arrayJsonLists[count]) : {}
                }

                let loopActions = JSON.parse(JSON.stringify(action.branches[0].actions));
                const resdata = await startExcution(job, varVault, loopActions);
                count++
            }
            job.data.state = "Active";
            return ({ job })
        } catch (err) {
            return ({ job })
        }
    } else {
        return ({ job })
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

const replaceVariableFunction = (string, Variables, options, format) => {
    try {
        var gv = [], s;
        var arr = [];
        string = replaceall("]]", ']]$', replaceall("[[", '$[[', string))
        const regex1 = /\$([.*+?^$(){[\]}:@"0-9a-zA-Z-_.,\/\']+)\$/gm; // "[[{value:{{user.name}}]]"
        const regex2 = /\{{([0-9a-zA-Z-_., \/\']+)\}}/gm; // {{user.firstname}}

        [regex1, regex2].forEach(ele => {
            while ((s = ele.exec(string)) !== null) {
                if (s.index === ele.lastIndex) {
                    ele.lastIndex++;
                }
                console.log(s[0]);
                gv.push(replaceall("]]$", ']]', replaceall("$[[", '[[', s[0])));
            }
        });

        string = replaceall("$[[", '[[', replaceall("]]$", ']]', string))

        for (let index = 0; index < gv.length; index++) {
            const objectKey = getObjectKey(gv[index], 0);
            let variable = isJSON(Variables[objectKey]) ? JSON.parse(Variables[objectKey]) : Variables[objectKey];
            if (variable && typeof variable == "object") {
                const objectKe2 = getObjectKey(gv[index], 1);
                string = replaceall(gv[index], variable[objectKe2], string)
            } else if (variable) {
                if (typeof variable == 'string') {
                    string = replaceall('$', '', replaceall(gv[index], `'${variable}'`, string))
                } else {
                    string = replaceall('$', '', replaceall(gv[index], variable, string))
                }
            } else if (variable == '') {
                string = replaceall('$', '', replaceall(gv[index], `'${variable}'`, string))
            }
        }

        if (isJSON(string)) {
            var tempString = JSON.parse(string)
            if (tempString.length != 0) {
                string = tempString[0][0].value;
            }
        }

        if (options.isExpression) {
            //   string = HotFormulaParser(string)
        }

        return (string)
    } catch (err) {
        return string
    }
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

const replaceVariablesString = (action, varVault, isString) => {
    try {
        console.log("=================");
        var gv = [], s, string = isString ? action : JSON.stringify(action), match;
        // const regex = /\{{([0-9a-zA-Z-_., \/\']+)\}}/gm;
        // while ((s = regex.exec(string)) !== null) {
        //     if (s.index === regex.lastIndex) {
        //         regex.lastIndex++;
        //     }
        //     gv.push(s[0]);
        // }

        const regex = /{{([^{}[\]]*?(?:(?:\[[^\]]*\])[^{}[\]]*?)*)}}/g;
        while (match = regex.exec(string)) {
            console.log(match[1]);
            gv.push(`{{${match[1]}}}`);
        }

        for (let index = 0; index < gv.length; index++) {
            const objectKey = gv[index];
            string = string.replace(gv[index], ejsRender(objectKey, varVault))
        }

        return isString ? string : JSON.parse(string)
    } catch (err) {
        return isString ? string : JSON.parse(string)
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
            var mailOptions = {
                from: "'Glozic' <workflow@glozic.com>", // sender address
                emailTo: ejsRender(properties.sendTo, varVault), // list of receivers
                emailSubject: (properties.subject ? "Re: " + ejsRender(properties.subject, varVault) : "Glozic workflow"), // Subject line
                emailBody: "parsed reply", // plain text body
            };


            if (properties.ccTo && properties.ccTo != "") {
                mailOptions["cc"] = ejsRender(properties.ccTo, varVault)
            }

            if (properties.bccTo && properties.bccTo != "") {
                mailOptions["bcc"] = ejsRender(properties.bccTo, varVault)
            }

            console.log(mailOptions);

            mailOptions.emailBody = ejsRender(properties.messageBody, varVault);
            if (job?.data?.tenant && job?.data?.tenant != "") {
                const options = {
                    method: 'POST',
                    url: `${utility.IsCheckDevTenant(job?.data?.tenant) ? keys.PortalDevHost : keys.PortalLiveHost}/workflow-send-email`,
                    body: mailOptions,
                    headers: {
                        tenant: job?.data?.tenant
                    },
                    json: true
                }
                console.log(options);
                await request(options)
            } else {
                await SendMail.sendEmail(mailOptions);
            }
            joblogs(job, startTime, properties)
        }
        return { job }
    } catch (err) {
        console.log(err.message);
        console.log("Email service failed...")
        var logObj = { timestamp: moment(), actionId: `${action.actionId}-${new Date().getTime()}`, status: "Start", activity: action.configuration.actionTitle, log: `Error : ${err.message} in ${action.nodeType} ` };
        job.log(JSON.stringify(logObj));

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
                    var logObj = { timestamp: moment(), actionId: `${action.actionId}-${new Date().getTime()}`, status: "Start", activity: action.configuration.actionTitle, log: `Error : ${err.message} in ${action.nodeType} ` };
                    job.log(JSON.stringify(logObj));
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
        var logObj = { timestamp: moment(), actionId: `${action.actionId}-${new Date().getTime()}`, status: "Start", activity: action.configuration.actionTitle, log: `Error : ${err.message} in ${action.nodeType} ` };
        job.log(JSON.stringify(logObj));
        return { job }
    }

}

const queryJson = (job, varVault, action) => {
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
                        varVault[properties.variable] = JSON.stringify(resdata)
                    }
                }
                resolve(varVault)
            } catch (err) {
                var logObj = { timestamp: moment(), actionId: `${action.actionId}-${new Date().getTime()}`, status: "Start", activity: action.configuration.actionTitle, log: `Error : ${err.message} in ${action.nodeType} ` };
                job.log(JSON.stringify(logObj));
                console.log(err);
                //Toast("Something went wrong action execution", "error")/
                resolve(varVault)
            }
        });
    } else {
        resolve(varVault)
    }
}

const logMessage = (job, varVault, action, properties) => {
    var value = properties.value;
    try {
        value = value.replaceAll("}}", "%>").replaceAll("{{", "<%=")
        let varVaultdata = {};
        Object.keys(varVault).forEach(ele => {
            varVaultdata[ele] = JSON.parse(varVault[ele])
        });
        let outputHtml = ejs.render(value, varVaultdata);
        return outputHtml.replaceAll("%>", "}}").replaceAll("<%=", "{{");
    } catch (err) {
        console.log(err);
        var logObj = { timestamp: moment(), actionId: `${action.actionId}-${new Date().getTime()}`, status: "Start", activity: action.configuration.actionTitle, log: `Error : ${err.message} in ${action.nodeType} ` };
        job.log(JSON.stringify(logObj));
        return value.replaceAll("%>", "}}").replaceAll("<%=", "{{");
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

const bodyreplaceVariables = (action, varVault, isString, format) => {
    try {
        var gv = [], s, string = isString ? action : JSON.stringify(action), match;
        const regex = /\{{([0-9a-zA-Z-_., \/\']+)\}}/gm;
        while ((s = regex.exec(string)) !== null) {
            if (s.index === regex.lastIndex) {
                regex.lastIndex++;
            }
            gv.push(s[0]);
        }

        const regex2 = /{{([^{}[\]]*?(?:(?:\[[^\]]*\])[^{}[\]]*?)*)}}/g;
        while (match = regex2.exec(string)) {
            if (!gv.includes(`{{${match[1]}}}`)) {
                gv.push(`{{${match[1]}}}`);
            }

        }

        for (let index = 0; index < gv.length; index++) {
            const objectKey = getObjectKey(gv[index], 0);
            if (varVault[objectKey]) {
                let varData = (JSON.parse(varVault[objectKey]) && JSON.parse(varVault[objectKey]).result) ? JSON.parse(varVault[objectKey]).result : JSON.parse(varVault[objectKey]);
                if (varData != "") {
                    let objectkeylen = replaceall("}}", "", replaceall("{{", "", gv[index]))
                    const pathExp = "$." + objectkeylen + "";
                    const editDataTableComponents = jp.query({ [objectKey]: varData }, pathExp, 1000);
                    if (editDataTableComponents[0]) {
                        if (isCheckString(editDataTableComponents[0])) {
                            string = replaceall(gv[index], (convertString(editDataTableComponents[0], varVault)), string)
                        } else {
                            if (typeof editDataTableComponents[0] == "string") {
                                string = replaceall(gv[index], (JSON.stringify(editDataTableComponents[0])), string)
                            } else {
                                string = replaceall(`"${gv[index]}"`, (JSON.stringify(editDataTableComponents[0])), string)
                            }
                        }
                    } else {
                        string = replaceall(gv[index], "", string)
                    }
                }
            } else {
                string = replaceall(gv[index], "", string)
            }

        }
        return (!isString || format == "JSON") ? JSON.parse(string) : string;
    } catch (err) {
        console.log(err);

        return isString ? string : JSON.parse(string)
    }

}

const CheckRequestBody = (body) => {
    try {
        let tempBody = JSON.parse(body)
        return (body && tempBody == "{{request.body}}") ? true : false
    } catch (err) {
        return false;
    }
}

const callCollectionOperation = (varVault, actionData, job) => {
    return new Promise(async (resolve, reject) => {
        try {
            let action = (actionData && actionData.configuration) ? actionData.configuration.properties : "";
            if (action) {
                let count = 0;
                //Variable To Orignal Value 
                if ((action.requestType == 2 || action.requestType == 3) && action.filters && action.filters != "") {
                    let ObjFilter = isJSON(action.filters) ? JSON.parse(action.filters) : action.filters;
                    if (ObjFilter.length != 0) {
                        while ((ObjFilter.length - 1) >= count) {
                            ObjFilter[count].value = replaceVariablesString(ObjFilter[count].value, varVault, true)
                            ObjFilter[count].value = await replaceVariablesString(ObjFilter[count].value, varVault, null);
                            ObjFilter[count].field = ObjFilter[count].field == "documentId" ? "_id" : ObjFilter[count].field;
                            count++;
                        }
                    } ``
                    action.filters = ObjFilter;
                }


                //Variable To Orignal Value 
                if (action.fetchRecordField && action.fetchRecordValue) {
                    action.fetchRecordValue = await replaceVariablesString(action.fetchRecordValue, varVault, true);
                }


                if (action.reqBody) {
                    action.reqBody = await bodyreplaceVariables(action.reqBody, varVault, true);
                    action.reqBody = JSON.parse(action.reqBody)
                }

                const requestData = {
                    ...action
                }

                requestData["tenant"] = job?.data?.tenant || requestData["tenant"]

                var options = {
                    method: 'POST',
                    url: `${utility.IsCheckDevTenant(requestData["tenant"]) ? keys.PortalDevHost : keys.PortalLiveHost}/callCollectionOperation/endpoint`,
                    body: requestData,
                    json: true
                };

                const res = await request(options)
                if (res.status) {
                    varVault[action.variable] = JSON.stringify(res.data)
                    resolve({ job, varVault })
                } else {
                    resolve({ job, varVault })
                }
            } else {
                resolve({ job, varVault })
            }

        } catch (err) {
            console.log(err)
            var logObj = { timestamp: moment(), actionId: `${actionData.actionId}-${new Date().getTime()}`, status: "Start", activity: actionData.configuration.actionTitle, log: `Error : ${err.message} in ${actionData.nodeType} ` };
            job.log(JSON.stringify(logObj));
            resolve({ job, varVault })
        }
    })
}

const callWebService = async (actionData, varVault, job) => {
    try {
        let action = (actionData && actionData.configuration) ? actionData.configuration.properties : "";

        // const url = action.apiUrl
        // const method = actionDef.reqMethod;
        // const headers = actionDef.reqHeaders;
        // const body = actionDef.reqBody;

        let requestUrl = await replaceVariables(action.apiUrl, varVault, true);
        let requestHearder = await replaceVariables(action.reqHeaders, varVault, true);
        let requestBody = await bodyreplaceVariables(action.reqBody, varVault, true);


        var reqDatas = {
            "url": requestUrl,
            "connId": action.connId,
            "dataSrcType": action.type,
            "tenant": action.tenant,
            "method": action.reqMethod,
            "reqHeaders": requestHearder,
            "isRequestHeaders": action.isRequestHeaders,
            "reqBody": requestBody,
            "isRequestBody": requestBody,
            "contentType": action.contentType
        }

        console.log(reqDatas);

        const replaceVariablesData = reqDatas
        const url = replaceVariablesData.url ? replaceVariablesData.url : undefined;
        const method = replaceVariablesData.method;
        const headers = replaceVariablesData.reqHeaders;
        const body = replaceVariablesData.reqBody;


        var options = {}
        options.method = method;
        options.url = url;
        options.headers = (headers != "" && headers) ? JSON.parse(headers) : {};
        method !== 'GET' ? options.body = (isJSON(body) ? JSON.parse(body) : body) : null;
        options.json = true;

        console.log(options);
        const resWebrequest = await request(options);
        varVault[action.variable] = JSON.stringify(resWebrequest);
        return { job, varVault }
    } catch (err) {
        console.log(err);
        var logObj = { timestamp: moment(), actionId: `${actionData.actionId}-${new Date().getTime()}`, status: "Start", activity: actionData.configuration.actionTitle, log: `Error : ${err.message} in ${actionData.nodeType} ` };
        job.log(JSON.stringify(logObj));
        return { job, varVault }
    }
}

const jsEditor = async (varVault, actionData, job) => {
    try {
        let action = (actionData && actionData.configuration) ? actionData.configuration.properties : "";
        let _var = {};
        Object.keys(varVault).forEach(ele => {
            _var[ele] = JSON.parse(varVault[ele])
        });
        const JsExpressionData = jsFunction(_var, action)
        varVault[action.variable] = JSON.stringify(JsExpressionData);
        return { job, varVault }
    } catch (err) {
        return { job, varVault }
    }
}

function jsFunction(varVault, action) {
    var dynamicFunction = new Function(`const _var = ${JSON.stringify(varVault)}; ${action.value}`);
    return dynamicFunction();
}

module.exports = {
    startExcution: startExcution
}
