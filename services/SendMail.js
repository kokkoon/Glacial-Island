const keys = require('../config/keys');
const nodemailer = require('nodemailer');
const smtpTransport = require("nodemailer-smtp-transport");
//var path = require('path');
//const fs = require('fs');


const sendEmail = async (payload) => {

    let mailOptions = {
        from: "", // sender address
        to: payload.emailTo, // list of receivers
        subject: payload.emailSubject, // Subject line
        html: payload.emailBody, // plain text body
        host: payload.host ? payload.host : "portal"
    };

    if (payload.cc && payload.cc != "") {
        mailOptions["cc"] = payload.cc
    }

    if (payload.bcc && payload.bcc != "") {
        mailOptions["bcc"] = payload.bcc
    }

    if (mailOptions.html) {
        await nodemailersendMail(mailOptions)
    } else {
        return false;
    }
}

var transporter = function (host) {
    return new Promise((resolve) => {
        resolve({
            smtp_server: keys.smtpServer,
            smtp_port: Number(keys.smtpPort) || 587,
            smtp_auth: {
                user: keys.smtpUser,
                pass: keys.smtpPass
            },
            smtp_fromMail: keys.smtpFrom
        });
    });
}

var nodemailersendMail = function (mailOptions) {
    return new Promise((resolve, reject) => {
        var host = null
        if (mailOptions.host) {
            host = mailOptions.host;
            delete mailOptions.host;
        }

        transporter(host).then((nodemailersConfig) => {
            if (nodemailersConfig) {
                const Transport = nodemailer.createTransport(
                    smtpTransport({
                        service: 'smtp',
                        host: nodemailersConfig.smtp_server,
                        port: nodemailersConfig.smtp_port,
                        secureConnection: true,  //secureConnection: true, // true for 465, false for other ports
                        auth: nodemailersConfig.smtp_auth
                    })
                )
                mailOptions["from"] = `Glozic <` + nodemailersConfig.smtp_fromMail + ">"
                Transport.sendMail(mailOptions, (error, info) => {
                    if (error) {
                        console.log("Email Failed..." + error.message);
                        resolve(false);
                    } else {
                        console.log("Email Sent...");
                        resolve(true)
                    }
                });
            }
        });
    });
}

module.exports = {
    sendEmail: sendEmail
}