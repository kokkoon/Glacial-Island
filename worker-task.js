const keys = require('./config/keys');
const Bull = require("bull");
const QUEUE_NAME= 'TASK';
const REDIS_URL= keys.redisURL; //'redis://h:zwWbvx0uyH2ZYceqMAUzeHXm8u90ROnK@redis-13053.c1.asia-northeast1-1.gce.cloud.redislabs.com:13053'
const taskQueue = new Bull(QUEUE_NAME, REDIS_URL);

taskQueue.process(function(job, done) {
  console.log(job.id, job.data, job.opts)
  //job.log((job.data.state && job.data.state=="Paused")? "Resuming workflow...": "Starting workflow...")
  //job.moveToCompleted("stopped", true)
  done();
});