const ejs = require('ejs');
const replaceall = require("replaceall");


const parseVariable = (str, data) => {
    var varNames = str.match(/(?<=\<\<).+?(?=\>\>)/g);
    console.log(varNames)
    varNames && varNames.map((varName, i) => {
        var regex = new RegExp("\<\<" + varName + "\>\>");
        str = str.replace(regex, data[varName]);
    })
    return str
}

const replaceVariable = (field) => {
    return field.replaceAll("{{", "").replaceAll("}}", "")
}

function togifyTotextvariableFunction(string) {
    try {
        var gv = [], s;
        var arr = [];
        string = string.replaceAll("[[", '$[[').replaceAll("]]", ']]$')
        const regex2 = /\{{([0-9a-zA-Z-_., \/\']+)\}}/gm; // {{user.firstname}}
        [regex2].forEach(ele => {
            while ((s = ele.exec(string)) !== null) {
                if (s.index === ele.lastIndex) {
                    ele.lastIndex++;
                }
                console.log(s[0]);
                gv.push(s[0].replaceAll("$[[", '[[').replaceAll("]]$", ']]'));
            }
        });
        return gv.length >= 1 ? gv[0] : string;
    } catch (err) {
        cls.log(err);
        return string
    }
}

const isJSON = (str) => {
    try {
        return (JSON.parse(str) && !!str);
    } catch (e) {
        return false;
    }
}

function isCheckString(string) {
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

//New Flow
String.prototype.replaceAll = function (snytxt1, snytxt2) {
    return replaceall(snytxt1, snytxt2, this)
};

const ejsRender = (value, varVault, isVarObjects) => {
    try {

        value = value.replaceAll("}}", "%>").replaceAll("{{", "<%=")
        let varVaultdata = isVarObjects ? varVault : {};


        if (!isVarObjects) {
            Object.keys(varVault).forEach(ele => {
                varVaultdata[ele] = JSON.parse(varVault[ele])
            });
        }

        let outputHtml = ejs.render(value, varVaultdata);
        return outputHtml.replaceAll("%>", "}}").replaceAll("<%=", "{{");
    } catch (err) {
        console.log(err);
        return value.replaceAll("%>", "}}").replaceAll("<%=", "{{");
    }
}

module.exports = {
    parseVariable: parseVariable,
    replaceVariable,
    togifyTotextvariableFunction,
    isJSON,
    isCheckString,
    ejsRender
}

