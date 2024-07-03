//replaced server.js
require('dotenv').config()

const express = require("express");
const app = express();
const Bull = require('bull');
const GUI = require('bull-arena');
const keys = require('./config/keys');
const redisqueries = require('./services/redisqueries');
const fs = require('fs');
const path = require('path');
const _ = require('lodash');

const { log } = console;
function proxiedLog(...args) {
	const l1 = ((new Error('log')).stack.split('\n')[2] || '…')
	const line = (l1.match(/[^\/||^\\]*(?=\))/) || ['not found'])[0];
	log.call(console, `../${line}-->`, ...args);
}
console.info = proxiedLog;
console.log = proxiedLog;

//const serviceWorker = require('./worker-service');
//const taskWorker = require('./worker-task');
const flowWorker = require('./worker-flow');
//const emailWorker = require('./worker-email');

console.log(redisqueries.allkeys);
redisqueries.getAllQueues(resData => {
	resData = resData.length === 0 ? ["FLOW", "TEST"] : resData
	const qDashboard = GUI({
		Bull,
		queues: resData.map(v => ({ name: v, hostId: "flow", url: keys.redisURL }))
	}, {
		basePath: "/",
		disableListen: true
	});

	app.use('/queue_dashboard', qDashboard);
})

const dashboard = GUI({
	Bull,
	queues: [
		{
			name: "WORKFLOW_LIVE",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "WORKFLOW_STUDIO",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "WORKFLOW",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "MESSAGE@production",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "MESSAGE@development",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "MESSAGE@local",
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
			name: "RESPONSE",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "REPONSE",
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
			name: "SCHEDULE@glozic.com",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "SCHEDULE@flowngin.com",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "Logs@local",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "Logs@development",
			hostId: "flow",
			url: keys.redisURL
		},
		{
			name: "Logs@production",
			hostId: "flow",
			url: keys.redisURL
		},
	]
}, {
	basePath: "/",
	disableListen: true
});


app.use('/dashboard', dashboard);

app.get('/random-images', (req, res) => {

	// List of image filenames
const images = [
	'image-1.jpg',
	'image-2.jpg',
	'image-3.jpg',
	'image-4.jpg',
	'image-5.jpg',
	'image-6.jpg',
  ];

	// Pick one random image from the list
    const randomImage = _.sample(images);
// Directory where images are stored
const imagesDir = path.join(__dirname, 'images');
    // Path to the random image
    const imagePath = path.join(imagesDir, randomImage);

    // Read the image data as a buffer
    fs.readFile(imagePath, (err, data) => {
      if (err) {
        console.error(err);
        res.status(500).send('Error reading image');
        return;
      }

      // Set the correct content type based on the file extension
      const ext = path.extname(randomImage).toLowerCase();
      let contentType = 'image/jpeg'; // Default content type

      if (ext === '.png') {
        contentType = 'image/png';
      } else if (ext === '.gif') {
        contentType = 'image/gif';
      }

      res.set('Content-Type', contentType);
      res.send(data);
	})

  });

require('./routes/jobRoutes')(app);



const PORT = process.env.PORT || '4000';
app.listen(PORT, '0.0.0.0', () => {
	console.log(`Server is running on port ${PORT}`)
})