const express = require('express');
const Bull = require('bull');
const GUI = require('bull-arena');
const keys = require('./config/keys');
const { connectQueue } = require('./config/bull');

const PORT = process.env.PORT || '6000';

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

let app = express();

app.use('/queue_dashboard', queueDashboard);

let scheduleQueue = connectQueue('SCHEDULE');

scheduleQueue.on('global:completed', (jobId, result) => {
    console.log(`Job completed with result ${result}`);
  });
  
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
