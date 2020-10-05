//replaced server.js

const express = require("express");
const app = express();
const Bull = require('bull');
const GUI = require('bull-arena');
const keys = require('./config/keys');

const REDIS_URL = process.env.REDIS_URL || keys.redisURL

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
		}
	]
}, {
	basePath: "/",
	disableListen: true
});

require('./routes/jobRoutes')(app);

app.use('/queue_dashboard', queueDashboard);


const PORT = process.env.PORT || '4000';
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`)
})







  