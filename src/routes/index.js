const express = require('express');
const path = require('path');
const fs = require('fs');
const {
    getReportTCO, getReportTCI, getReportDCI, getReportDCO, getReportCA, getReportRU, getReportDBO, getReportDBONA, getReportDBONASUM, getReportMP,
    downloadTCO, downloadTCI, downloadDCI, downloadDCO, downloadCA, downloadRU, downloadDBO, downloadDBONA, downloadDBONASUM, downloadMP,
    checkPendingJobs, getPendingJobs, clean
} = require('../handler/jobHandler');

const router = express.Router();

// Define API endpoint with query parameters
router.get("/getreporttco", getReportTCO);

router.get("/getreporttci", getReportTCI);

router.get("/getreportdci", getReportDCI);

router.get("/getreportdco", getReportDCO);

router.get("/getreportca", getReportCA);

router.get("/getreportru", getReportRU);

router.get("/getreportdbo", getReportDBO);

router.get("/getreportdbona", getReportDBONA);

router.get("/getreportdbonasum", getReportDBONASUM);

router.get("/getreportmp", getReportMP);

router.get('/downloadTCO/:jobId', downloadTCO);

router.get('/downloadTCI/:jobId', downloadTCI);

router.get('/downloadDCI/:jobId', downloadDCI);

router.get('/downloadDCO/:jobId', downloadDCO);

router.get('/downloadCA/:jobId', downloadCA);

router.get("/downloadru/:jobId", downloadRU);

router.get("/downloaddbo/:jobId", downloadDBO);

router.get("/downloaddbona/:jobId", downloadDBONA);

router.get("/downloaddbonasum/:jobId", downloadDBONASUM);

router.get("/downloadmp/:jobId", downloadMP);

router.get('/checkPendingJobs', checkPendingJobs);

router.get('/getPendingJobs', getPendingJobs);

router.get("/clean", clean);

router.use('/file_download', express.static(path.join(__dirname, '../../file_download')));

// Serve the progresstci.html when visiting the /progress URL
router.get('/swaggertco', (req, res) => {
    res.sendFile(__dirname + '/public/progresstco.html');
});

router.get('/progresstci', (req, res) => {
    res.sendFile(__dirname + '/public/progresstci.html');
});

// Serve the progresstci.html when visiting the /progress URL
router.get('/progressdci', (req, res) => {
    res.sendFile(__dirname + '/public/progressdci.html');
});

router.get('/logs', (req, res) => {
    const logPath = `/Users/abiyyirahinda/Documents/testlog.log`;

    fs.readFile(logPath, 'utf8', (err, data) => {
        if (err) {
            console.error(err);
            return res.status(500).send('Log file not found or cannot be read.');
        }

        const lines = data.trim().split('\n');
        const last100Lines = lines.slice(-150).join('\n');

        res.setHeader('Content-Type', 'text/plain');
        res.send(last100Lines || 'Log is empty.');
    });
});


module.exports = router; 