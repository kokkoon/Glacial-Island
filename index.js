//replaced server.js

const express = require("express");
<<<<<<< HEAD
=======
//const session = require("express-session");
>>>>>>> 02982ad4af4af25887fa859b6751cf70abb95d0b
const app = express();
const Bull = require('bull');
const GUI = require('bull-arena');
const keys = require('./config/keys');
const redisqueries = require('./services/redisqueries');

//const serviceWorker = require('./worker-service');
//const taskWorker = require('./worker-task');
const flowWorker = require('./worker-flow');
//const emailWorker = require('./worker-email');
<<<<<<< HEAD
=======

>>>>>>> 02982ad4af4af25887fa859b6751cf70abb95d0b

redisqueries.getAllQueues(resData => {
	resData = resData.length === 0 ? ["FLOW"] : resData
	const qDashboard = GUI({
		Bull,
		queues: resData.map(v => ({name: v, hostId: "flow", url: keys.redisURL}))
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
        }
	]
}, {
	basePath: "/",
	disableListen: true
});


app.use('/dashboard', dashboard);

<<<<<<< HEAD
=======
/*app.use(session({
	secret:'anyt-string-but-to-keep-secret',
	name: 'get-approval',
    proxy: true,
    resave: true,
    saveUninitialized: true
}));
*/

>>>>>>> 02982ad4af4af25887fa859b6751cf70abb95d0b
require('./routes/jobRoutes')(app);

const PORT = process.env.PORT || '4000';
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`)
})







  