require('dotenv').config();

module.exports = {
	redisURL: process.env.REDIS_URL,
	redisHost: process.env.REDIS_HOST,
	redisPort: process.env.REDIS_PORT,
	redisPWD: process.env.REDIS_PWD,
	redisUser: process.env.REDIS_USER,
	twilioAccountSid: process.env.TWILIO_ACCOUNT_SID,
	twilioAuthToken: process.env.TWILIO_AUTH_TOKEN,
	PortalDevHost: process.env.PORTAL_DEV_HOST,
	PortalLiveHost: process.env.PORTAL_LIVE_HOST,
	WORKFLOW_FLOW_QUEUE: process.env.WORKFLOW_FLOW_QUEUE,
	WorkflowApiUrl: process.env.NODE_ENV == 'local' ? `http://localhost:4000` : process.env.WORKFLOW_API_URL,
	smtpServer: process.env.SMTP_SERVER,
	smtpPort: process.env.SMTP_PORT || 587,
	smtpUser: process.env.SMTP_USER,
	smtpPass: process.env.SMTP_PASS,
	smtpFrom: process.env.SMTP_FROM || 'workflow@glozic.com',
};
