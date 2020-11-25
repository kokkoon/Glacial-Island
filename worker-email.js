const keys = require('./config/keys');
const nodemailer = require('nodemailer');
const smtpTransport = require("nodemailer-smtp-transport");
const NODE_ENV = process.env.NODE_ENV;
const Bull = require("bull");
const EMAIL_QUEUE = 'EMAIL@' + NODE_ENV;
const emailQueue = new Bull(EMAIL_QUEUE, keys.redisURL);
const SendMail = require('./services/SendMail');

emailQueue.process(function(job, done) {
  console.log("Processing job id:", job.id)
  var validEmail = /^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+$/;
  var validPhone = /^\+?[1-9]\d{9,14}$/;
  console.log(`Processing email from ${job.data.fromAddress}`);
  var body = job.data.summary;
  console.log(body)

  const mailOptions = {
    from: "'Glozic' <workflow@glozic.com>", // sender address
    emailTo: job.data.fromAddress, // list of receivers
    emailSubject: "Re: " + job.data.subject, // Subject line
    emailBody: "parsed reply", // plain text body
  };
  
  if (job.data.fromAddress.includes('workflow@glozic.com')) {
      done("Ignore email from " + job.data.fromAddress)
  }
  const foundApp = body.search(/(?! )(app.*)/ig)
  const foundRej = body.search(/(?! )(rej.*)/ig)
  console.log(foundApp, foundRej, [foundApp, foundRej].indexOf(-1))
  var outcome;
  if ([foundApp, foundRej].indexOf(-1) == -1) {
    outcome = foundApp > foundRej? "rejected" : "approved"
  } else if (foundApp == -1 && foundRej == -1) {
    console.log("asfd")
    outcome = undefined
  } else if (foundApp == -1) {
    outcome = "rejected"
  } else {
    outcome = "approved"
  }
  mailOptions.emailBody = outcome;
  SendMail.sendEmail(mailOptions);

  done();
});

const sendEmail = async (payload) => {

  let mailOptions = {
      from: "", // sender address
      to: payload.emailTo, // list of receivers
      subject: payload.emailSubject, // Subject line
      html: payload.emailBody, // plain text body
      host: payload.host ? payload.host : "portal"
  };

  if (mailOptions.html) {
      await nodemailersendMail(mailOptions)
  } else {
      return false;
  }
}

var transporter = function (host) {
  var hostname = host;
  return new Promise((resolve, reject) => {
      
      var smtp = {
          smtp_server: "email-smtp.ap-southeast-1.amazonaws.com",
          smtp_port: 587,
          smtp_auth: {user: "AKIA6QCOO42T3OFY2OXZ", pass: "BGHlOJ7bIjIASUCza/2OxIfPheI+UeyW+nA4m1LVIVAi"},
          smtp_fromMail: "workflow@glozic.com"
      }
      resolve(smtp)
                      
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