const keys = require('./config/keys');
const Bull = require("bull");
const QUEUE_NAME= 'SERVICE';
const serviceQueue = new Bull(QUEUE_NAME, { redis: { port: keys.redisPort, host: keys.redisHost, password: keys.redisPWD } });

serviceQueue.process(function(job, done) {
  console.log(job.data)
  //job.log((job.data.state && job.data.state=="Paused")? "Resuming workflow...": "Starting workflow...")
  //job.moveToCompleted("stopped", true)
  done();
});