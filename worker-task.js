const keys = require('./config/keys');
const Bull = require("bull");
const QUEUE_NAME= 'TASK';
const taskQueue = new Bull(QUEUE_NAME, { redis: { port: keys.redisPort, host: keys.redisHost, password: keys.redisPWD } });

taskQueue.process(function(job, done) {
  console.log("Processing task id:", job.id)
  var validEmail = /^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+$/;
  var validPhone = /^\+?[1-9]\d{9,14}$/;
  if (validEmail.test(job.owner.trim())) {
    // Check preferred notification mode and send notification
    console.log("Send email notification for " + job.owner)
  } else if (validPhone.test(job.owner.trim().replace(/[ -]/g, ''))) {
    // send notification via sms/Whatsapp
    console.log("Send sms/whatsapp notification for " + job.owner)
  }
  
  done();
});