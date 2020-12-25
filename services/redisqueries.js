const { promisify } = require('util');
const keys = require('../config/keys');
const redis = require('redis');
const redisScan = require('node-redis-scan');
const async = require('async');
const url = require('url');
let client;
if(process.env.REDIS_URL){
    let redisURL = url.parse(process.env.REDIS_URL, {no_ready_check: true});
    client = redis.createClient(redisURL)
} else {
    client = redis.createClient({port:keys.redisPort, host: keys.redisHost, password:keys.redisPWD})
}

//var client = redis.createClient({port:keys.redisPort, host: keys.redisHost, password:keys.redisPWD});
//var client = redis.createClient(keys.redisURL, {no_ready_check: true});
client.on('connect', function(){
  console.log('Redis Connection Successfull');
});
var scanner = new redisScan(client)

var getAllQueues = async function(callback) {
  try {
    const getAsync = promisify(client.keys).bind(client);

    const keys =  await getAsync('bull:*');

    callback(keys.map(key => key.match(/(?<=bull:).+?(?=:)/g)[0]).filter((v,i) => keys.map(key => key.match(/(?<=bull:).+?(?=:)/g)[0]).indexOf(v) === i ))
  } catch (err) {
    console.log(err.message)
  }
}

var scan = (callback) =>{
  scanner.scan('bull:*', {count: 1000}, (err, matchingKeys) => {
    if (err) throw(err);
 
    // matchingKeys will be an array of strings if matches were found
    // otherwise it will be an empty array.
    console.log(matchingKeys)
    callback(matchingKeys.map(key => key.match(/(?<=bull:).+?(?=:)/g)[0]).filter((v,i) => matchingKeys.map(key => key.match(/(?<=bull:).+?(?=:)/g)[0]).indexOf(v) === i));
});
}

var allkeys = function(key) {
  return new Promise((resolve, reject) => {
    client.keys(key, function(err, keys) {
      if (err || keys.length === 0) reject(err)
      resolve(keys)
    }) 
  })
}

var allIds = function(callback) {
  client.keys('*:id', function (err, keys) {
    if (err) return console.log(err);
    if(keys){
      //callback(keys)
        async.map(keys, function(key, cb) {
           client.get(key, function (error, value) {
                if (error) return cb(error);
                var job = {};
                job['jobId']=key;
                job['data']=value;
                cb(null, job);
            }); 
        }, function (error, results) {
           if (error) return console.log(error);
           console.log(results);
           callback({data:results});
        });
    }
});
}

var redisWFInst = function(inst, callback) {
  client.hget(inst, "flowdef", function(err, value) {
    if(err) callback(err)
    callback(null, value)
  })
}

var instanceNumber = function(key) {
  return new Promise((resolve, reject) => {
    client.incr(key, function(err, instNum) {
      if(err) reject(err)
      //console.log("instNum:", instNum)
      resolve(instNum)
    })
  })
}

var newInst = function(key, wfId, wfDef, status, callback) {
  client.hmset(key, "flowdef", JSON.stringify(wfDef), "flowID", wfId, "status", status, function (err, result) {
    if (err) callback(err)
    callback(null, result)
  })
}

var updateInst = function(key, wfDef, status, callback) {
  client.hmset(key, "flowdef", JSON.stringify(wfDef), "status", status, function (err, result) {
    if (err) callback(err)
    callback(null, result)
  })
}

var logInst = function(key, wfDef, log, callback) {
  wfDef.inst.logs.push(log);
  client.hmset(key, "flowdef", JSON.stringify(wfDef), function (err, result) {
    if (err) callback(err)
    callback(null, result)
  })
}

module.exports = {
  getAllQueues: getAllQueues,
  scan: scan,
  allkeys: allkeys,
  allIds: allIds,
  redisWFInst: redisWFInst,
  instanceNumber: instanceNumber,
  newInst: newInst,
  updateInst: updateInst,
  logInst: logInst
}
