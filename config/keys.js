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
};
