
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
const NODE_ENV = process.env.NODE_ENV || "development";
const MSG_QUEUE = 'MESSENGER@' + NODE_ENV;

//ENV redisqueries
const redisqueries = require('../services/redisqueries');
const SendMail = require('../services/SendMail');
const { parseVariable } = require('./helper.controller');
const getvariables = require('./getvariables.controller');

const startExcution = async (job, variables, actions, intialExcution) => {
    debugger
    return new Promise(async (resolve, reject) => {
        try {
            let varVault = intialExcution ? {} : variables, edges = actions.edges, nodes = actions.nodes;

            if (intialExcution) {
                let variablesData = await getvariables(variables);
                Object.keys(variablesData).forEach(ele => {
                    varVault[ele] = JSON.stringify(variablesData[ele])
                });
            }


            let count = 0;
            let from = intialExcution ? "node-start" : actions.startNode;
            while (count < edges.length) {
                if (edges.find(x => x.from == from)) {
                    let findNodeId = edges.find(x => x.from == from).to;
                    var action = JSON.parse(JSON.stringify(nodes.find(x => x.id == findNodeId)));
                    if (action) {
                        var resData = await callAction(job, varVault, action, actions);
                        varVault = resData.varVault;
                    }

                    if (action && action.nodeType == "Condition") {
                        const conditonNodeType = varVault[action.variable] ? "conditionTrue" : "conditionFalse"
                        let getEdge = edges.filter(x => x.from == findNodeId).find(y => nodesLists.some(z => (z.id == y.to && z.nodeType == conditonNodeType)))
                        if (getEdge) {
                            findNodeId = edges.find(x => x.from == getEdge.to).from;
                            from = findNodeId;
                        }
                    } else {
                        from = findNodeId;
                    }
                }
                count++;
            }
            resolve("Completed")
        } catch (err) {
            console.log(err.message);
            resolve("Failed")
        }
    });

}

//Action Lists 
const callAction = (job, varVault, action, mainAction) => {
    return new Promise(async (resolve, reject) => {
        try {
            switch (action.nodeType) {
                case "Query Json":
                    varVault = await queryJson(varVault, action);
                    break
                case "Send Email":
                    await sendEmail(job, varVault, action);
                    break
                case "Log Message":
                    var logMsg = ejsRender(action.value, varVault)
                    var logObj = {
                        timestamp: moment(),
                        actionId: action.id,
                        status: "Custom",
                        activity: action.text,
                        log: logMsg
                    };
                    console.log(JSON.stringify(logObj));
                    job.log(JSON.stringify(logObj))
                    break
                case "Loop":
                    await loopFunction(job, varVault, action, mainAction);
                    break
                case "Call Web Service":
                    varVault = await callWebService(action, varVault);
                    break
                default:
                    console.log("run other actions")
                    break
            }
            resolve({ varVault, action })
        } catch (err) {
            console.log(err.message);
            reject(false)
        }
    })
}

const loopFunction = async (job, varVault, action, mainAction) => {
    try {
        let loopLength = 0, count = 0;
        if (action.type == "variable") {
            loopLength = Number(ejsRender(action.value, varVault))
        } else if (action.type == "json") {
            loopLength = JSON.parse(action.value) ? JSON.parse(action.value).length : 0;
        }


        while (count < loopLength) {
            varVault["loop_index_id"] = JSON.stringify(count)

            if (action.type == "variable") {
                varVault[action.variable] = replaceVariables(action.selected_variable, varVault, true);
            } else if (action.type == "json") {
                varVault[action.variable] = action.value
            }

            if (isCheckJSONParse(varVault[action.variable])) {
                varVault[action.variable] = JSON.parse(varVault[action.variable]);
                varVault[action.variable] = (varVault[action.variable] && varVault[action.variable].length) ? JSON.stringify(varVault[action.variable][count]) : {}
            }

            let loopActions = {
                startNode: mainAction.nodes.find(x => x.nodeType == "whileStart" && x.parent == action.id).id,
                actions: mainAction.actions,
                nodes: mainAction.nodes,
                edges: mainAction.edges.filter(x => x.parent == action.id)
            }
            await startExcution(job, varVault, loopActions);
            count++
        }

    } catch (err) {
        console.log(err.message);
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
        console.log(varVault);
        const startTime = moment();
        const mailOptions = {
            from: "'Glozic' <workflow@glozic.com>", // sender address
            emailTo: ejsRender(action.sendTo, varVault), // list of receivers
            emailSubject: (action.subject ? "Re: " + ejsRender(action.subject, varVault) : "Glozic workflow"), // Subject line
            emailBody: "parsed reply", // plain text body
        };
        console.log(mailOptions);
        mailOptions.emailBody = ejsRender(action.messageBody, varVault);
        await SendMail.sendEmail(mailOptions);
        joblogs(job, startTime, action)
        return true;
    } catch (err) {
        console.log(err.message);
        console.log("Email service failed...")
        return false;
    }
}

const queryJson = (varVault, action) => {

    var jpQuery = (obj, pathExp, count) => {
        if (typeof obj !== 'object' || obj === null) return null
        return count ? JSONPath.query(obj, pathExp, count) : jp.query(obj, pathExp);
    }

    return new Promise(async (resolve, reject) => {
        try {
            if (Object.keys(varVault).length != 0) {
                const JSONData = action.formatType == "2" ? varVault[action.jsonData] : action.jsonData;
                var resdata = (typeof JSONData == 'string') ? JSON.parse(JSONData) : JSONData;
                if (action.query) {
                    var resdata = await jpQuery((typeof JSONData == 'string') ? JSON.parse(JSONData) : JSONData, action.query);
                }
                varVault[action.variable] = resdata;
            }
            resolve(varVault)
        } catch (err) {
            console.log(err);
            Toast("Something went wrong action execution", "error")
            resolve(varVault)
        }
    });
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
