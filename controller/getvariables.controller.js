const keys = require('../config/keys');
const request = require("request-promise");
const jp = require('jsonpath');
const URL = require('url')
const replaceall = require("replaceall");
const hotFormula = require("../utils/hotFormula")
const utility = require("../utils/utility")
const HotFormulaParser = hotFormula.HotFormulaParser
const { replaceVariable, togifyTotextvariableFunction, ejsRender } = require('./helper.controller');

const getvariables = async (extSources, queryStringObjet, job) => {
    var extData = {}, count = 0;
    try {
        extSources = await loadExtDataInputVariable(extSources, queryStringObjet);

        extSources.forEach(source => {
            if (source.type == "Input Variable") {
                extData[source.var] = source.defaultValue;
            }

            if (source.var == "document") {
                extData[source.var] = source.value;
            }
        });


        const InputVariable = extSources.filter(x => x.type == "Input Variable")

        //Load Variables
        while ((extSources.length - 1) >= count) {
            try {
                var source = extSources[count];
                var propsUser = {}
                const exraDataRes = await loadExrVariable({ job, extData, source, propsUser, queryStringObjet, InputVariable, extSources, propsVars: [] })
                if (exraDataRes.status) extData = exraDataRes.data;
                count++;
            } catch (err) {
                count++;
            }
        }
        return extData;
    } catch (err) {
        return err.message
    }
}

const loadExrVariable = async (reqPayload) => {
    try {
        let { job, extData, source, propsUser: props, queryStringObjet, InputVariable, ref, extSources, propsVars } = reqPayload;

        if (source.filters && source.filters.length != 0) {
            source.filters.forEach(x => {
                x.filters.forEach(e2 => {
                    let dataField = togifyTotextvariableFunction(e2.filter.dataField)
                    if (dataField.search("{{") >= 0) {
                        const findInput = InputVariable.find(y => y.var == dataField.replaceAll("{{", "").replaceAll("}}", ""));
                        if (findInput) {
                            e2["queryString"] = findInput.queryString;
                            e2["value"] = findInput.defaultValue;
                        }
                    }
                });
            });

            source.filters = await loadFilterDataVariable(reqPayload);
        }

        const loadPayload = { extData, source, props, queryStringObjet, InputVariable, ref, extSources }

        switch (source.type) {
            case 'Json':
                extData[source.var] = source.defaultValue
                break;
            case 'Tenant':
                extData[source.var] = job?.data?.tenant
                break;
            case 'App':
                extData[source.var] = null
                break;
            case 'User':
                extData[source.var] = null
                break;
            case 'Object':
                if (source.var == 'user') {
                    extData[source.var] = null
                } else if (source.var == 'app') {
                    extData[source.var] = null
                } else if (source.var == 'tenant') {
                    extData[source.var] = job?.data?.tenant
                } else if (source.var == 'document') {
                    extData[source.var] = source.value;
                } else {
                    var reqData = {
                        method: 'GET',
                        connId: source.connId,
                        dataSrcType: source.type,
                        tenant: source.tenant,
                        isObject: true,
                        query: generateQuery(source.filters)
                    }

                    reqData["tenant"] = job?.data?.tenant || reqData["tenant"]

                    var options = {
                        method: 'POST',
                        url: `${utility.IsCheckDevTenant(reqData["tenant"]) ? keys.PortalDevHost : keys.PortalLiveHost}$/webRequestCollection/worflow`,
                        body: reqData,
                        json: true
                    };

                    if (Object.keys(reqData.query).length == 0) {
                        extData[source.var] = null;
                        const res1 = await request(options)
                        const result = (res1.status && res1.result && res1.result.length >= 1) ? res1.result[0] : {};
                        [source.var] = { ...result, connId: source.connId, tenant: source.tenant };
                    } else {
                        const res1 = await request(options)
                        const result = (res1.status && res1.result && res1.result.length >= 1) ? res1.result[0] : {};
                        extData[source.var] = { ...result, connId: source.connId, tenant: source.tenant };
                    }
                }
                break;
            case 'Collection':

                var reqData = {
                    method: 'GET',
                    connId: source.connId,
                    dataSrcType: source.type,
                    tenant: source.tenant,
                    query: generateQuery(source.filters)
                }

                reqData["tenant"] = job?.data?.tenant || reqData["tenant"]

                var options = {
                    method: 'POST',
                    url: `${utility.IsCheckDevTenant(reqData["tenant"]) ? keys.PortalDevHost : keys.PortalLiveHost}/webRequestCollection/worflow`,
                    body: reqData,
                    json: true
                };

                const res = await request(options)
                const result = (res.status && res.result && res.result.length != 0) ? res.result : [];
                extData[source.var] = result
                break;
            case 'UsersData':
                var reqData = {
                    method: 'GET',
                    connId: 'user_data',
                    dataSrcType: source.type,
                    tenant: source.tenant,
                    query: generateQuery(source.filters)
                }

                reqData["tenant"] = job?.data?.tenant || reqData["tenant"]

                var options = {
                    method: 'POST',
                    url: `${utility.IsCheckDevTenant(reqData["tenant"]) ? keys.PortalDevHost : keys.PortalLiveHost}/webRequestCollection/worflow`,
                    body: reqData,
                    json: true
                };

                const resUserData = await request(options)
                const resultUserdata = (resUserData.status && resUserData.result && resUserData.result.length != 0) ? resUserData.result : [];
                extData[source.var] = resultUserdata
                break;
            case 'Sharepoint':
            case 'Web request':
                var reqData = {
                    "url": source.url,
                    "method": 'GET',
                    "connId": source.connId,
                    "dataSrcType": source.type,
                    "tenant": source.tenant,
                    "method": source.reqMethod,
                    "reqHeaders": source.reqHeaders,
                    "isRequestHeaders": source.isRequestHeaders,
                    "reqBody": source.reqBody,
                    "isRequestBody": source.isRequestBody,
                    "contentType": source.contentType
                }

                reqData = await replaceVariables(reqData, extData, null, { refence: 'viewPage', source: source, loadPayload, propsVars }, 'variables')

                reqData["tenant"] = job?.data?.tenant || reqData["tenant"]

                var options = {
                    method: 'POST',
                    url: `${utility.IsCheckDevTenant(reqData["tenant"]) ? keys.PortalDevHost : keys.PortalLiveHost}/webrequest`,
                    body: reqData,
                    json: true
                };

                const resWebrequest = await request(options)
                extData[source.var] = resWebrequest
                break;
            case 'Text':
                extData[source.var] = source.defaultValue;
                break;
            case 'Number':
                extData[source.var] = source.defaultValue
                break;
            case 'TextExpression':
                const textExpressionData = await replaceVariableFunction(source.defaultValue, extData, { isExpression: true, refence: 'viewPage', source: source, loadPayload, propsVars })
                extData[source.var] = textExpressionData
                break;
            case 'NumberExpression':
                const numberExpressionData = await replaceVariableFunction(source.defaultValue, extData, { isExpression: true, refence: 'viewPage', source: source, loadPayload, propsVars })
                extData[source.var] = numberExpressionData
                break;
            case 'CheckboxExpression':
                const checkboxExpressionData = await replaceVariableFunction(source.defaultValue, extData, { isExpression: true, refence: 'viewPage', source: source, loadPayload })
                extData[source.var] = checkboxExpressionData;
                break;
            case 'DatetimeExpression':
                const datetimeExpressionData = await replaceVariableFunction(source.defaultValue, extData, { isExpression: true, refence: 'viewPage', source: source, loadPayload })
                extData[source.var] = datetimeExpressionData;
                break;
        }
        return { status: true, data: extData };
    } catch (err) {
        console.log(err);
        return { status: false }
    }
}

const generateQuery = (filtersPayload) => {
    var query = { $or: [] };

    try {
        if (filtersPayload) {
            filtersPayload.forEach(ele => {
                if (ele.condition_type == "$and") {
                    let tempQuery2 = { "$and": [] }
                    let tempQuery3 = { "$or": [] }
                    ele.filters.forEach(ele2 => {
                        if (ele2.condition_type == "$and") {
                            tempQuery2["$and"].push({ [replaceVariable(ele2.filter.field)]: { "$eq": ele2.value || ele2.filter.dataField } })
                        } else {
                            tempQuery3["$or"].push({ [replaceVariable(ele2.filter.field)]: { "$eq": ele2.value || ele2.filter.dataField } })
                        }
                    });

                    if (tempQuery2["$and"].length != 0) {
                        query["$or"].push(tempQuery2);
                    }
                    if (tempQuery3["$or"].length != 0) {
                        query["$or"].push(tempQuery3);
                    }


                } else if (ele.condition_type == "$or") {
                    let tempQuery2 = { "$and": [] }
                    let tempQuery3 = { "$or": [] }
                    ele.filters.forEach(ele2 => {
                        if (ele2.condition_type == "$and") {
                            tempQuery2["$and"].push({ [replaceVariable(ele2.filter.field)]: { "$eq": ele2.value || ele2.filter.dataField } })
                        } else {
                            tempQuery3["$or"].push({ [replaceVariable(ele2.filter.field)]: { "$eq": ele2.value || ele2.filter.dataField } })
                        }
                    });

                    if (tempQuery2["$and"].length != 0) {
                        query["$or"].push(tempQuery2);
                    }
                    if (tempQuery3["$or"].length != 0) {
                        query["$or"].push(tempQuery3);
                    }
                }
            });
        }
        return (query["$or"].length == 0 ? {} : query);
    } catch (err) {
        return {};
    }

    return filtersPayload;
}

async function loadExtDataInputVariable(extSources, queryStringObjet) {
    return new Promise(async (resolve, reject) => {
        try {
            var count = 0;
            while ((extSources.length - 1) >= count) {
                var source = extSources[count];
                if (source.type == "Input Variable") {
                    extSources[count]["queryString"] = extSources[count].defaultValue;
                    extSources[count].defaultValue = queryStringObjet[source.defaultValue] ? queryStringObjet[source.defaultValue] : ""
                }
                count++;
            }
            resolve(extSources)
        } catch (err) {
            cls.log(err);
            cls.log(err.message);
            resolve(extSources)
        }
    });
}

async function loadFilterDataVariable(reqPayload) {
    try {
        let count = 0;
        while (reqPayload.source.filters.length > count) {
            reqPayload.source.filters[count].filters = await filtersFunction(reqPayload, reqPayload.source.filters[count].filters)
            count++;
        }
        return reqPayload.source.filters;
    } catch (err) {
        cls.log(err);
        return reqPayload.source.filters;
    }
}

async function filtersFunction(reqPayload, filters) {
    try {

        let count = 0;
        while (filters.length > count) {
            let e2 = filters[count];
            let dataField = this.togifyTotextvariableFunction(e2.filter.dataField);
            if (dataField.search("{{") >= 0) {
                //const keys = dataField.replaceAll("{{", "").replaceAll("}}", "").split(".");
                const keys = replaceall("}}", "", replaceall("{{", "", dataField)).split(".");
                //e2["field"] = e2.filter.field.replaceAll("{{", "").replaceAll("}}", "");
                e2["field"] = replaceall("}}", "", replaceall("{{", "", e2.filter.field));
                if (!reqPayload.extData[keys[0]]) {
                    const getSource = reqPayload.extSources.find((x) => x.var == keys[0]);
                    const reqPayloaddatas = {
                        InputVariable: reqPayload.InputVariable,
                        extData: reqPayload.extData,
                        extSources: reqPayload.extSources,
                        propsUser: reqPayload.propsUser,
                        propsVars: reqPayload.propsVars,
                        queryStringObjet: reqPayload.queryStringObjet,
                        source: getSource
                    }
                    const loadExrVariableData = await this.loadExrVariable(reqPayloaddatas);
                    reqPayload.extData = loadExrVariableData.status ? loadExrVariableData.data : reqPayload.extData
                }


                let findInput = "";
                const pathExp = "$." + this.replaceVariable(dataField) + "";

                const pathExpData = jp.query(reqPayload.extData, pathExp, 1000);
                if (pathExpData[0]) {
                    findInput = pathExpData[0]
                }


                if (findInput) {
                    e2["valueAssign"] = true;
                    e2["value"] = utility.isJSON(findInput) ? JSON.parse(findInput) : findInput;
                } else {
                    e2["valueAssign"] = true;
                    e2["value"] = this.togifyTotextvariableFunction(e2.filter.dataField);
                }
            }

            filters[count] = e2;
            count++;
        }
        return filters;
    } catch (err) {
        return filters;
    }
}

const getObjectKey = (string, index) => {
    return replaceall("}}", "", replaceall("{{", "", string)).split(".")[index];
}

//Old
// async function replaceVariableFunction(string, Variables, options) {
//     try {
//         var gv = [], s;
//         var arr = [];
//         console.log(string);
//         string = string.replaceAll("[[", '$[[').replaceAll("]]", ']]$')
//         const regex1 = /\$([.*+?^$(){[\]}:@"0-9a-zA-Z-_.,\/\']+)\$/gm; // "[[{value:{{user.name}}]]"
//         const regex2 = /\{{([0-9a-zA-Z-_., \/\']+)\}}/gm; // {{user.firstname}}

//         [regex1, regex2].forEach(ele => {
//             while ((s = ele.exec(string)) !== null) {
//                 if (s.index === ele.lastIndex) {
//                     ele.lastIndex++;
//                 }
//                 console.log(s[0]);
//                 gv.push(s[0].replaceAll("$[[", '[[').replaceAll("]]$", ']]'));
//             }
//         });

//         string = string.replaceAll("$[[", '[[').replaceAll("]]$", ']]')

//         for (let index = 0; index < gv.length; index++) {
//             const objectKey = getObjectKey(gv[index], 0);
//             let variable = Variables[objectKey];
//             if (!variable && variable != "" && options) {
//                 const getSource = options.loadPayload.extSources.find(x => x.var == objectKey);
//                 if (getSource) {
//                     options.loadPayload.source = getSource;
//                     cls.log(options.loadPayload);
//                     if (!options.propsVars.some(x => x === options.source.var)) {
//                         options.propsVars.push(options.source.var)
//                         options.loadPayload["propsVars"] = options.propsVars;
//                         const loadExrVariableData = await loadExrVariable(options.loadPayload);
//                         variable = loadExrVariableData.status ? loadExrVariableData.data[objectKey] : "";
//                     } else {
//                         variable = `<b> Variable  ${options.source.var} is not exist</b>`
//                     }
//                 } else {
//                     variable = "";
//                 }
//             }

//             if (variable && typeof variable == "object") {
//                 const objectKe2 = getObjectKey(gv[index], 1, objectKey);
//                 string = string.replace(gv[index], variable[objectKe2])
//             } else if (variable) {
//                 string = string.replace(gv[index], variable).replaceAll('$', '')
//             }
//         }

//         //  string = HotFormulaParser(string)
//         //return (string.replaceAll(/\n/g, "<br />"))
//         return (string)
//     } catch (err) {
//         cls.log(err);
//         return string
//     }
// }

const replaceVariableFunction = (string, Variables, options, format) => {
    try {
        var gv = [], s;
        var arr = [];
        string = replaceall("]]", ']]$', replaceall("[[", '$[[', string))
        const regex1 = /\$([.*+?^$(){[\]}:@"0-9a-zA-Z-_.,\/\']+)\$/gm; // "[[{value:{{user.name}}]]"
        //const regex2 = /\{{([0-9a-zA-Z-_[\]"., \/\']+)\}}/gm; // {{user.firstname}}

        [regex1].forEach(ele => {
            while ((s = ele.exec(string)) !== null) {
                if (s.index === ele.lastIndex) {
                    ele.lastIndex++;
                }
                console.log(s[0]);
                gv.push(replaceall("]]$", ']]', replaceall("$[[", '[[', s[0])));
            }
        });

        const regex2 = /{{([^{}[\]]*?(?:(?:\[[^\]]*\])[^{}[\]]*?)*)}}/g;
        while (true) {
            const match = regex2.exec(string)
            if (!match) {
                break;
            } else {
                gv.push(`{{${match[1]}}}`);
            }
        }

        string = replaceall("$[[", '[[', replaceall("]]$", ']]', string))

        for (let index = 0; index < gv.length; index++) {
            const objectKey = getObjectKey(gv[index], 0);
            let variable = utility.isJSON(Variables[objectKey]) ? JSON.parse(Variables[objectKey]) : Variables[objectKey];
            if (variable && typeof variable == "object") {
                const objectKe2 = getObjectKey(gv[index], 1);
                string = ejsRender(string, Variables)// replaceall(gv[index], variable[objectKe2], string)
            } else if (variable) {
                if (typeof variable == 'string' || typeof variable == 'number') {
                    string = replaceall('$', '', replaceall(gv[index], `'${variable}'`, string))
                } else {
                    string = replaceall('$', '', replaceall(gv[index], variable, string))
                }
            } else if (variable == '') {
                string = replaceall('$', '', replaceall(gv[index], `'${variable}'`, string))
            }
        }

        if (utility.isJSON(string)) {
            var tempString = JSON.parse(string)
            if (tempString.length != 0 && tempString[0] && tempString[0][0] && tempString[0][0].value) {
                string = tempString[0][0].value;
            }
        }

        if (options.isExpression) {
            string = HotFormulaParser(string)
        }

        //return (string.replaceAll(/\n/g, "<br />"))
        return (string)
    } catch (err) {
        return string
    }
}

const replaceVariablesOld = async (action, varVault, isString, options, type) => {
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
            console.log(match[1]);
            gv.push(`{{${match[1]}}}`);
        }

        for (let index = 0; index < gv.length; index++) {
            const objectKey = getObjectKeys(gv[index], 0);

            let variable = varVault[objectKey] ? (type == "variables" ? JSON.stringify(varVault[objectKey]) : varVault[objectKey]) : varVault[objectKey];
            if (!variable && variable != "" && options) {
                const getSource = options.loadPayload.extSources.find(x => x.var == objectKey);
                if (getSource) {
                    options.loadPayload.source = getSource;
                    cls.log(options.loadPayload);
                    if (!options.propsVars.some(x => x === options.source.var)) {
                        options.propsVars.push(options.source.var)
                        options.loadPayload["propsVars"] = options.propsVars;
                        const loadExrVariableData = await loadExrVariable(options.loadPayload);
                        variable = loadExrVariableData.status ? (type == "variables" ? JSON.stringify(loadExrVariableData.data[objectKey]) : loadExrVariableData.data[objectKey]) : "";
                    } else {
                        variable = `<b> Variable  ${options.source.var} is not exist</b>`
                    }
                } else {
                    variable = "";
                }
            }

            if (variable) {
                let varData = (JSON.parse(variable) && JSON.parse(variable).result) ? JSON.parse(variable).result : JSON.parse(variable);
                if (varData != "") {
                    let objectkeylen = gv[index].replaceAll("{{", "").replaceAll("}}", "")
                    const pathExp = "$." + objectkeylen + "";
                    const editDataTableComponents = jp.query({ [objectKey]: varData }, pathExp, 1000);
                    if (editDataTableComponents[0]) {
                        string = string.replaceAll(gv[index], (isCheckString(editDataTableComponents[0]) ? convertString(editDataTableComponents[0], varVault, options) : editDataTableComponents[0]))
                    }
                }
            } else {
                string = replaceall(gv[index], "", string)
            }
        }
        return isString ? string : JSON.parse(string)
    } catch (err) {
        return action
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

const replaceVariables = async (action, varVault, isString, options, type) => {
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
            console.log(match[1]);
            gv.push(`{{${match[1]}}}`);
        }

        for (let index = 0; index < gv.length; index++) {
            const objectKey = getObjectKey(gv[index], 0);

            let variable = varVault[objectKey] ? (type == "variables" ? JSON.stringify(varVault[objectKey]) : varVault[objectKey]) : varVault[objectKey];

            if (!variable && variable != "" && options) {
                const getSource = options.loadPayload.extSources.find(x => x.var == objectKey);
                if (getSource) {
                    options.loadPayload.source = getSource;
                    if (!options.propsVars.some(x => x === options.source.var)) {
                        options.propsVars.push(options.source.var)
                        options.loadPayload["propsVars"] = options.propsVars;
                        const loadExrVariableData = await loadExrVariable(options.loadPayload);
                        variable = loadExrVariableData.status ? (type == "variables" ? JSON.stringify(loadExrVariableData.data[objectKey]) : loadExrVariableData.data[objectKey]) : "";
                    } else {
                        variable = "";
                    }
                } else {
                    variable = "";
                }
            }

            if (variable) {
                let varData = (JSON.parse(variable) && JSON.parse(variable).result) ? JSON.parse(variable).result : JSON.parse(variable);
                if (varData != "") {
                    let objectkeylen = replaceall("}}", "", replaceall("{{", "", gv[index]))
                    const pathExp = "$." + objectkeylen + "";
                    const editDataTableComponents = jp.query({ [objectKey]: varData }, pathExp, 1000);
                    if (editDataTableComponents[0]) {
                        string = replaceall(gv[index], (utility.isCheckString(editDataTableComponents[0]) ? convertString(editDataTableComponents[0], varVault) : JSON.stringify(editDataTableComponents[0])), string)
                    } else {
                        string = replaceall(gv[index], "", string)
                    }
                }
            } else {
                string = replaceall(gv[index], "", string)
            }

        }
        return isString ? string : JSON.parse(string)
    } catch (err) {
        console.log(err);

        return isString ? string : JSON.parse(string)
    }
}

module.exports = getvariables