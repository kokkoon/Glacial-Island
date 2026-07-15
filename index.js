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
const cors = require('cors');

const { log } = console;
function proxiedLog(...args) {
	const l1 = ((new Error('log')).stack.split('\n')[2] || '…')
	const line = (l1.match(/[^\/||^\\]*(?=\))/) || ['not found'])[0];
	log.call(console, `../${line}-->`, ...args);
}
console.info = proxiedLog;
console.log = proxiedLog;
app.use(cors())
//const serviceWorker = require('./worker-service');
const flowWorker = require('./worker-flow');
const taskWorker = require('./worker-task');
//const emailWorker = require('./worker-email');

console.log(redisqueries.allkeys);
redisqueries.getAllQueues(resData => {
	const queueNames = (resData && resData.length > 0) ? resData : ["FLOW", "TEST"];
	console.log("Arena queues (from Redis):", queueNames);

	const dashboard = GUI({
		Bull,
		queues: queueNames.map(name => ({
			name,
			hostId: "flow",
			url: keys.redisURL
		}))
	}, {
		basePath: "/",
		disableListen: true
	});

	app.use('/dashboard', dashboard);
	app.use('/queue_dashboard', dashboard);
});

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
	console.log(`Start ${process.env.NODE_ENV}. v1`)
	console.log(`Server is running on port ${PORT}`)
})