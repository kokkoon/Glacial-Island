const keys = require('./config/keys');
const SendMail = require('./services/SendMail');
const taskStore = require('./services/taskStore');
const { taskQueue, TASK_QUEUE } = require('./config/bull');

const validEmail = /^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+$/;
const validPhone = /^\+?[1-9]\d{9,14}$/;

const buildTaskEmailBody = (task) => {
  const apiBase = (keys.WorkflowApiUrl || '').replace(/\/$/, '');
  const jobId = task.instanceId || task.workflowJobId || '';
  const approveUrl = `${apiBase}/resumejob/${encodeURIComponent(jobId)}/approved`;
  const rejectUrl = `${apiBase}/resumejob/${encodeURIComponent(jobId)}/rejected`;

  return `
    <div style="font-family:Arial,sans-serif;line-height:1.5;color:#222;">
      <p>You have a new workflow task:</p>
      <p>${task.taskDesc || ''}</p>
      <p>Task ID: ${task.taskId || ''}</p>
      <p style="margin:24px 0;">
        <a href="${approveUrl}"
           style="display:inline-block;padding:12px 20px;margin-right:12px;background:#16a34a;color:#ffffff;text-decoration:none;border-radius:4px;font-weight:bold;">
          Approve
        </a>
        <a href="${rejectUrl}"
           style="display:inline-block;padding:12px 20px;background:#dc2626;color:#ffffff;text-decoration:none;border-radius:4px;font-weight:bold;">
          Reject
        </a>
      </p>
      <p style="color:#666;font-size:12px;">Or open the links directly:<br/>
        Approve: ${approveUrl}<br/>
        Reject: ${rejectUrl}
      </p>
    </div>
  `;
};

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
      emailBody: buildTaskEmailBody(task),
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