const keys = require('../config/keys');
const request = require('request');
const NODE_ENV = process.env.NODE_ENV || "local";

const Authenticate = async (req, res, next) => {
    if (req.headers.authorization && req.headers.tenant) {
        const authorization = { authorization: req.headers.authorization, tenant: req.headers.tenant };
        const requestOptions = {
            method: 'GET',
            uri: `${keys.PortalDevHost}/VerifyAuthorizationToken`,
            headers: {
                'Content-Type': 'application/json',
                'authorization': req.headers.authorization,
                'tenant': req.headers.tenant
            },
            body: {},
            json: true
        }
        if (NODE_ENV != "production") {
            console.log("Skipped authentication for non-production environment.")
            next()
        } else {
            request(requestOptions, (error, response, responseBody) => {
                //console.log(responseBody);
                if (error) {
                    console.error(error)
                } else if (responseBody.status) {
                    next();
                } else {
                    res.json({ "res": 1, status: false, message: "Header is not correct. Please try again." });
                }
            })
        }
    } else {
        res.json({ "res": 1, status: false, message: "Header is not correct. Please try again." });
    }
}


module.exports = {
    Authenticate: Authenticate
}