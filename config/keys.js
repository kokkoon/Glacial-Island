const NODE_ENV = process.env.NODE_ENV || "development"
console.log("keys.NODE_ENV: " + NODE_ENV);
switch (process.env.NODE_ENV) {
  case 'production':
    module.exports = require('./prod');
    break;
  case 'development':
    module.exports = require('./dev');
    break;
  case 'local':
    module.exports = require('./local');
    break;
}