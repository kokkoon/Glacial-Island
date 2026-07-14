const SendMail = require('./services/SendMail');
const taskStore = require('./services/taskStore');
const { taskQueue, TASK_QUEUE } = require('./config/bull');

const validEmail = /^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+$/;
const validPhone = /^\+?[1-9]\d{9,14}$/;

taskQueue.process(async (job) => {
  console.log("Processing task id:", job.id);
  const task = job.data || {};
  const owner = (task.owner || "").trim();

  let notificationSent = false;
  let notificationMessage = "";
  if (validEmail.test(owner)) {
    const mailOptions = {
      from: "'Glozic' <workflow@glozic.com>",
      emailTo: owner,
      emailSubject: task.taskName ? `Task assigned: ${task.taskName}` : 'Workflow task assigned',
      emailBody: `You have a new workflow task:<br/><br/>${task.taskDesc || ''}<br/><br/>Task ID: ${task.taskId}<br/>Please respond with approve/reject at the task portal.`,
    };
    notificationSent = await SendMail.sendEmail(mailOptions);
    notificationMessage = notificationSent ? 'Email notification sent' : 'Email notification failed';
  } else if (validPhone.test(owner.replace(/[ -]/g, ''))) {
    notificationSent = true;
    notificationMessage = `SMS/WhatsApp notification requested for ${owner}`;
    console.log(notificationMessage);
  } else {
    notificationMessage = `Invalid task owner address: ${owner}`;
    console.log(notificationMessage);
  }

  await taskStore.updateTask(job, {
    status: notificationSent ? 'AwaitingResponse' : 'NotificationFailed',
    sentAt: Date.now(),
    notificationMessage,
    updatedAt: Date.now()
  });

  return job.data;
});

//throng({ workers, start })
console.log("Task worker started for ", TASK_QUEUE);