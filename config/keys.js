const NODE_ENV = process.env.NODE_ENV || "local";
console.log("NODE_ENV: " + NODE_ENV);
var keys;
switch (NODE_ENV) {
  case 'production':
    keys = require('./prod');
    break;
  case 'development':
    keys = require('./dev');
    break;
  case 'local':
    keys = require('./local');
    break;
}

//export default keys
module.exports = keys