const keys = require('./config/keys');
const Bull = require("bull");
const QUEUE_NAME= 'SERVICE';
const REDIS_URL= keys.redisURL; //'redis://h:zwWbvx0uyH2ZYceqMAUzeHXm8u90ROnK@redis-13053.c1.asia-northeast1-1.gce.cloud.redislabs.com:13053'
const serviceQueue = new Bull(QUEUE_NAME, REDIS_URL);

serviceQueue.process(function(job, done) {
  console.log(job.data)
  //job.log((job.data.state && job.data.state=="Paused")? "Resuming workflow...": "Starting workflow...")
  //job.moveToCompleted("stopped", true)
  done();
});