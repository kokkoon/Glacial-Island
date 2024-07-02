const keys = require('./config/keys');
const nodemailer = require('nodemailer');
const smtpTransport = require("nodemailer-smtp-transport");
const NODE_ENV = process.env.NODE_ENV || "local";
const Bull = require("bull");
const EMAIL_QUEUE = 'EMAIL@' + NODE_ENV;
const TASK_QUEUE = 'TASK@' + NODE_ENV;
const emailQueue = new Bull(EMAIL_QUEUE, keys.redisURL);
const taskQueue = new Bull(TASK_QUEUE,  keys.redisURL);
const SendMail = require('./services/SendMail');
const taskqueries = require('./services/taskqueries');

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

  taskQueue.getJobs(['delayed'], 0, 100)
    .then(async result => {
			var waitingJob = result.filter(obj => {return obj.data.owner.trim() === job.data.fromAddress})
			console.log(`Total: ${result.length}, # of waiting jobs for ${job.data.fromAddress}`, waitingJob.length)
      if (waitingJob.length<1) return `There were no pending task for you`;
      if (outcome === undefined) return `Failed interpreting your reply`;

      return taskqueries.resume(waitingJob[0], outcome)
        .then(async ans => {
					console.log(`1. Resumed: ${ans.resumed}, message: ${ans.message}`);
					if (ans.resumed) {
						// completion criteria met, update other tasks...
						taskqueries.closePendingTasks(waitingJob[0], outcome)
          } 
          
					waitingJob[0].data.status = "Completed";
					waitingJob[0].data.response = outcome;
					waitingJob[0].data.updated = Date.now();
					await waitingJob[0].update(waitingJob[0].data);
					//await waitingJob[0].promote();
					//await waitingJob[0].moveToCompleted('completed', true, true)
					//await waitingJob[0].remove();
          return `${ans.message}`;
          
        }).catch(err => {
          console.log(`Error...${err}`);
          return `Error... ${err}`
        })
    })
    .then(replyMsg => {
      console.log('replyMsg:', replyMsg);
      mailOptions.emailBody = replyMsg;
      SendMail.sendEmail(mailOptions);
    })
    .catch(alert => {
      console.log("Ops! alert:", alert);
      //mailOptions.emailBody = 'Failed processing your reply...';
      //SendMail.sendEmail(mailOptions);
    })

  done();
});