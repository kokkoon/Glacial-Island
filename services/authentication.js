const keys = require('../config/keys');
const request = require('request');

const Authenticate = async (req, res, next) => {
    if (req.headers.authorization && req.headers.tenant) {
        const authorization = { authorization: req.headers.authorization, tenant: req.headers.tenant };
        const requestOptions = {
            method: 'GET',
            uri: `${keys.PortalHost}/VerifyAuthorizationToken`,
            headers: {
                'Content-Type': 'application/json',
                'authorization': req.headers.authorization,
                'tenant': req.headers.tenant
            },
            body: {},
            json: true
        }
        request(requestOptions, (error, response, responseBody) => {
            console.log(responseBody);
            if (error) {
                console.log(error)
                console.error(error)
            } else if (responseBody.status) {
                console.log(responseBody.status)
                next();
            } else {
                res.json({ "res": 1, status: false, message: "Header is not correct. Please try again." });
            }
        })
    } else {
        res.json({ "res": 1, status: false, message: "Header is not correct. Please try again." });
    }
}


module.exports = {
    Authenticate: Authenticate
}