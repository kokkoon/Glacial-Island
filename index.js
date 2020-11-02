//replaced server.js

const express = require("express");
const session = require("express-session");
const app = express();
const Bull = require('bull');
const GUI = require('bull-arena');
const keys = require('./config/keys');

const serviceWorker = require('./worker-service');

const queueDashboard = GUI({
    Bull,
	queues: [
		{
			name: "FLOW",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "SERVICE",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "MESSAGE",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "SCHEDULE",
			hostId: "flow",
			url: keys.redisURL 
		},
		{
			name: "SCHEDULE@DESKTOP-HO2F260",
			hostId: "flow",
			url: keys.redisURL 
        },
		{
			name: "SCHEDULE-DESKTOP-HO2F260",
			hostId: "flow",
			url: keys.redisURL 
        },
		{
			name: "SCHEDULE@ip-10-0-0-155.ap-southeast-1.compute.internal",
			hostId: "flow",
			url: keys.redisURL 
        }
	]
}, {
	basePath: "/",
	disableListen: true
});

require('./routes/jobRoutes')(app);

app.use(session({secret:'anyt-string-but-to-keep-secret'}));

app.use('/queue_dashboard', queueDashboard);


const PORT = process.env.PORT || '4000';
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`)
})







  