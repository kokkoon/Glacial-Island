const moment = require('moment');
const workflowController = require("../controller/workflow_v3.controller")
var startflow = async(job) => {
  console.log("Start executing workflow actions...");
  var state = null;
  let data = await workflowController.startExcution(job, job.data.definition.variables, job.data.definition.actions, true);

  // startExcution may resolve a string "Failed" on error
  if (typeof data === 'string') {
    data = { status: data, actions: job.data.definition.actions };
  }

  data.status = data.status == 'Queued' ? "Completed" : data.status;
  state = data.status;
  job.data.state = data.status;
  if (state === "Failed" && !job.data.error) {
    job.data.error = "Workflow failed";
  }
  job.data.jobEnd = moment();
  job.data['end'] = (state === "Completed") ? moment() : undefined;
  job.data.definition = {
    variables : job.data.definition.variables,
    actions : data.actions
  }
  await job.update(job.data);
  console.log("==============Job Completed..=================", state);
}

module.exports = {
  startflow: startflow
}
