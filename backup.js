const express = require('express');
const oracledb = require('oracledb');  // Import oracledb untuk koneksi ke Oracle
const ExcelJS = require('exceljs');    // Import exceljs untuk ekspor ke Excel
const fs = require('fs');  // Untuk menulis file ke sistem
const path = require('path');  // Untuk memanipulasi path direktori
const archiver = require('archiver');  // Import archiver untuk zip file
const Bull = require('bull'); // Import Bull untuk job queue
const {setQueues, BullAdapter} = require('bull-board');
const app = express();
const port = 3005;  // Port API
const Sentry = require("@sentry/node");
const {nodeProfilingIntegration} = require("@sentry/profiling-node");
const cluster = require('cluster');
const os = require('os');
const Redis = require('ioredis');
const redis = new Redis(); // Koneksi ke Redis server
const JOB_LOCK_KEY = 'job_lock';
const ProgressBar = require('progress');
const { format } = require('date-fns');  // Import the format function from date-fns
const moment = require('moment');  // Import moment.js

// Middleware to parse JSON bodies
app.use(express.json());
// const numCPUs = os.cpus().length;

// Koneksi ke database Oracle
const config = {
    user: 'dbctc_v2',
    password: 'dbctc123',
    connectString: '10.8.2.48:1521/ctcv2db'  // Host, port, dan service name
};
// Membuat Job Queue menggunakan Bull
const reportQueue = new Bull('reportQueue', {
    redis: {host: '127.0.0.1', port: 6379},

    removeOnComplete: true // Job selesai langsung dihapus dari Redis

});
const reportQueueTCI = new Bull('reportQueueTCI', {
    redis: {host: '127.0.0.1', port: 6379},

    removeOnComplete: true // Job selesai langsung dihapus dari Redis

});
const reportQueueDCI = new Bull('reportQueueDCI', {
    redis: {host: '127.0.0.1', port: 6379},

    removeOnComplete: true // Job selesai langsung dihapus dari Redis

});
const reportQueueDCO = new Bull('reportQueueDCO', {
    redis: {host: '127.0.0.1', port: 6379},
    // limiter: {
    //     max: 1, // Hanya satu job yang bisa dijalankan dalam satu waktu
    //     duration: 86400000
    //
    // },
    removeOnComplete: true // Job selesai langsung dihapus dari Redis

});
Sentry.init({
    dsn: "https://911497525f2ba3a60f2ea285b4e82520@o4506467821092864.ingest.us.sentry.io/4509234026184704",
    integrations: [
        nodeProfilingIntegration(),
    ],

    // Tracing
    tracesSampleRate: 1.0, //  Capture 100% of the transactions
    // Set sampling rate for profiling - this is evaluated only once per SDK.init call
    profileSessionSampleRate: 1.0,
    // Trace lifecycle automatically enables profiling during active traces
    profileLifecycle: 'trace',

    // Setting this option to true will send default PII data to Sentry.
    // For example, automatic IP address collection on events
    sendDefaultPii: true,
});
// Mendaftarkan queue untuk memonitor pekerjaan

// if (cluster.isMaster) {
//     // Fork workers for each CPU core
//     for (let i = 0; i < numCPUs; i++) {
//         cluster.fork();
//     }
//
//     cluster.on('exit', (worker, code, signal) => {
//         console.log(`Worker ${worker.process.pid} died`);
//     });
// } else {


setQueues([new BullAdapter(reportQueue)]);
setQueues([new BullAdapter(reportQueueTCI)]);
setQueues([new BullAdapter(reportQueueDCI)]);
setQueues([new BullAdapter(reportQueueDCO)]);

function logErrorToFile(jobId, origin, destination, userId, errorMessage) {
    const logFilePath = path.join(__dirname, 'error_logs.txt');
    const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId} | Origin: ${origin} | Destination: ${destination} | UserID: ${userId} | Error: ${errorMessage}\n`;

    fs.appendFile(logFilePath, logMessage, (err) => {
        if (err) {
            console.error('Error writing to log file:', err);
        }
    });
}

function logErrorToFileTCI(jobId, origin, destination, userId, session, errorMessage) {
    const logFilePath = path.join(__dirname, 'error_logs.txt');
    const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId} | Origin: ${origin} | Destination: ${destination} | UserID: ${userId} | Session : ${session} | Error: ${errorMessage}\n`;

    fs.appendFile(logFilePath, logMessage, (err) => {
        if (err) {
            console.error('Error writing to log file:', err);
        }
    });
}

function logErrorToFileDCI(jobId, origin, destination, userId, errorMessage) {
    const logFilePath = path.join(__dirname, 'error_logs.txt');
    const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId} | Origin: ${origin} | Destination: ${destination} | UserID: ${userId} |  Error: ${errorMessage}\n`;

    fs.appendFile(logFilePath, logMessage, (err) => {
        if (err) {
            console.error('Error writing to log file:', err);
        }
    });
}

function logErrorToFileDCO(jobId, origin, destination, service, userId, errorMessage) {
    const logFilePath = path.join(__dirname, 'error_logs.txt');
    const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId} | Origin: ${origin} | Destination: ${destination} | UserID: ${userId} | Error: ${errorMessage}\n`;

    fs.appendFile(logFilePath, logMessage, (err) => {
        if (err) {
            console.error('Error writing to log file:', err);
        }
    });
}

const acquireLock = async () => {
    // Cobalah untuk mengakuisisi lock
    const lock = await redis.setnx(JOB_LOCK_KEY, 'locked');
    if (lock) {
        // Lock berhasil didapatkan
        return true;
    }
    return false;
};

const releaseLock = async () => {
    // Hapus kunci untuk melepaskan lock
    await redis.del(JOB_LOCK_KEY);
};

const processJob = async (job) => {
    try {
        const jobId = job.id;
        const isJobRunning = await redis.get('currentJobStatus');
        if (isJobRunning === 'running') {
            console.log(`Job ID: ${jobId} is already running. Skipping...`);
            // Jika job sedang berjalan, simpan job di queue pending
            await redis.lpush('pending_jobs', JSON.stringify(job.data));  // Simpan job ke antrian
            return;
        }
        // const lockAcquired = await acquireLock();
        // if (!lockAcquired) {
        //     console.log('Job is already running, skipping...');
        //     return;
        // }
        await redis.set('currentJobStatus', 'running');
        console.log(`Processing Job ID: ${job.id}`);

        // Tentukan fungsi yang digunakan berdasarkan nama queue
        let fetchDataAndExportToExcelFunc;
        if (job.queue.name === 'reportQueue') {
            fetchDataAndExportToExcelFunc = fetchDataAndExportToExcel;
        } else if (job.queue.name === 'reportQueueTCI') {
            fetchDataAndExportToExcelFunc = fetchDataAndExportToExcelTCI;
        } else if (job.queue.name === 'reportQueueDCI') {
            fetchDataAndExportToExcelFunc = fetchDataAndExportToExcelDCI;
        } else if (job.queue.name === 'reportQueueDCO') {
            fetchDataAndExportToExcelFunc = fetchDataAndExportToExcelDCO;
        }

        if(job.queue.name === 'reportQueue') {
            await Sentry.startSpan({name: 'Process Job' + job.id, jobId: job.id}, async (span) => {
                const {origin, destination, froms, thrus, user_id, dateStr} = job.data;

                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();

                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcel({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id,
                        dateStr
                    }).then((result) => {
                        dataCount = result.dataCount; // Assuming the fetchDataAndExportToExcel function returns data count
                        return result.zipFileName;
                    });

                    // Capture the completion time after the job is done
                    const endTime = Date.now();
                    completionTime = new Date(endTime).toISOString(); // Convert to ISO string for consistency

                    // Calculate the elapsed time in minutes
                    elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes
                    const formattedDate = moment().format('MM/DD/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                    const connection = await oracledb.getConnection(config);
                    const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET DOWNLOAD = 0,
                        STATUS = 'Done',
                        NAME_FILE = :filename,
                        UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM')
                    WHERE ID_JOB_REDIS = :jobId
                `;

                    // Prepare the update values
                    const updateValues = {
                        filename: zipFileName.split('\\').pop(),  // Get the zip file name from the generated file path
                        updated_at: formattedDate,  // Use the formatted date here
                        jobId: job.id  // The job ID that we are processing
                    };

                    // Execute the update query
                    await connection.execute(updateQuery, updateValues);
                    await connection.commit();
                    console.log(`Job status updated to 'Done' for job ID: ${job.id}`);
                    await redis.del('currentJobStatus');
                    console.log(`Job ID: ${jobId} is done`);

                    // Cek apakah ada job tertunda yang perlu diproses
                    await processPendingJobs();

                    return {
                        status: 'done',
                        zipFileName: zipFileName, // Add the file name to the return value
                        completionTime: completionTime, // Add the completion time
                        dataCount: dataCount,  // Number of records processed
                        elapsedTimeMinutes: elapsedTimeMinutes  // Processing time in minutes
                    };
                } catch (error) {
                    console.error('Error processing the job:', error);
                    await Sentry.startSpan({name: 'Log Error to File' + job.id, jobId: job.id}, async () => {

                        // Log the error details to file
                        logErrorToFile(job.id, origin, destination, user_id, error.message);

                    });
                    Sentry.captureException(error);

                    return {
                        status: 'failed',
                        error: error.message
                    };
                }
            });
        }else if(job.queue.name === 'reportQueueTCI') {
            await Sentry.startSpan({name: 'Process Report TCI Job' + job.id, jobId: job.id}, async (span) => {
                const {origin, destination, froms, thrus, user_id, TM, user_session, dateStr} = job.data;
                console.log('Processing job with data:', job.data);

                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();

                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcelTCI({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id,
                        TM,
                        user_session,
                        dateStr
                    }).then((result) => {
                        dataCount = result.dataCount; // Assuming the fetchDataAndExportToExcel function returns data count
                        return result.zipFileName;
                    });

                    // Capture the completion time after the job is done
                    const endTime = Date.now();
                    completionTime = new Date(endTime).toISOString(); // Convert to ISO string for consistency

                    // Calculate the elapsed time in minutes
                    elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes
                    const formattedDate = moment().format('MM/DD/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                    const connection = await oracledb.getConnection(config);
                    const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET DOWNLOAD  = 0,
                        STATUS    = 'Done',
                        NAME_FILE = :filename,
                        UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM')
                    WHERE ID_JOB_REDIS = :jobId
                `;

                    // Prepare the update values
                    const updateValues = {
                        filename: zipFileName.split('\\').pop(),  // Get the zip file name from the generated file path
                        updated_at: formattedDate,  // Use the formatted date here
                        jobId: job.id  // The job ID that we are processing
                    };

                    // Execute the update query
                    await connection.execute(updateQuery, updateValues);
                    await connection.commit();
                    console.log(`Job status updated to 'Done' for job ID: ${job.id}`);
                    await redis.del('currentJobStatus');
                    console.log(`Job ID: ${jobId} is done`);

                    // Cek apakah ada job tertunda yang perlu diproses
                    await processPendingJobs();

                    return {
                        status: 'done',
                        zipFileName: zipFileName, // Add the file name to the return value
                        completionTime: completionTime, // Add the completion time
                        dataCount: dataCount,  // Number of records processed
                        elapsedTimeMinutes: elapsedTimeMinutes  // Processing time in minutes
                    };
                } catch (error) {
                    console.error('Error processing the job:', error);
                    await Sentry.startSpan({name: 'Log Error to File' + job.id, jobId: job.id}, async () => {

                        // Log the error details to file
                        logErrorToFileTCI(job.id, origin, destination, user_id, user_session, error.message);

                    });
                    Sentry.captureException(error);

                    return {
                        status: 'failed',
                        error: error.message
                    };
                }
            });
        }else if(job.queue.name === 'reportQueueDCI') {
            await Sentry.startSpan({name: 'Process Report DCI Job' + job.id, jobId: job.id}, async (span) => {
                const {origin, destination, froms, thrus, user_id, service, dateStr} = job.data;
                console.log('Processing job with data:', job.data);

                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();

                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcelDCI({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id,
                        service,
                        dateStr
                    }).then((result) => {
                        dataCount = result.dataCount; // Assuming the fetchDataAndExportToExcel function returns data count
                        return result.zipFileName;
                    });

                    // Capture the completion time after the job is done
                    const endTime = Date.now();
                    completionTime = new Date(endTime).toISOString(); // Convert to ISO string for consistency

                    // Calculate the elapsed time in minutes
                    elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes
                    const formattedDate = moment().format('MM/DD/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                    const connection = await oracledb.getConnection(config);
                    const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET DOWNLOAD  = 0,
                        STATUS    = 'Done',
                        NAME_FILE = :filename,
                        UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM')
                    WHERE ID_JOB_REDIS = :jobId
                `;

                    // Prepare the update values
                    const updateValues = {
                        filename: zipFileName.split('\\').pop(),  // Get the zip file name from the generated file path
                        updated_at: formattedDate,  // Use the formatted date here
                        jobId: job.id  // The job ID that we are processing
                    };

                    // Execute the update query
                    await connection.execute(updateQuery, updateValues);
                    await connection.commit();
                    console.log(`Job status updated to 'Done' for job ID: ${job.id}`);
                    await redis.del('currentJobStatus');
                    console.log(`Job ID: ${jobId} is done`);

                    // Cek apakah ada job tertunda yang perlu diproses
                    await processPendingJobs();

                    return {
                        status: 'done',
                        zipFileName: zipFileName, // Add the file name to the return value
                        completionTime: completionTime, // Add the completion time
                        dataCount: dataCount,  // Number of records processed
                        elapsedTimeMinutes: elapsedTimeMinutes  // Processing time in minutes
                    };
                } catch (error) {
                    console.error('Error processing the job:', error);
                    await Sentry.startSpan({name: 'Log Error to File' + job.id, jobId: job.id}, async () => {

                        // Log the error details to file
                        logErrorToFileDCI(job.id, origin, destination, user_id, error.message);

                    });
                    Sentry.captureException(error);

                    return {
                        status: 'failed',
                        error: error.message
                    };
                }
            });
        }else if(job.queue.name === 'reportQueueDCO') {
            await Sentry.startSpan({name: 'Process Report DCO Job' + job.id, jobId: job.id}, async (span) => {
                const {origin, destination, froms, thrus, service, user_id, dateStr} = job.data;
                console.log('Processing job with data:', job.data);

                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();

                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcelDCO({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id,
                        service,
                        dateStr
                    }).then((result) => {
                        dataCount = result.dataCount; // Assuming the fetchDataAndExportToExcel function returns data count
                        return result.zipFileName;
                    });

                    // Capture the completion time after the job is done
                    const endTime = Date.now();
                    completionTime = new Date(endTime).toISOString(); // Convert to ISO string for consistency
                    const formattedDate = moment().format('MM/DD/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                    // Calculate the elapsed time in minutes
                    elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes

                    const connection = await oracledb.getConnection(config);
                    const updateQuery = `
                UPDATE CMS_COST_TRANSIT_V2_LOG
                SET DOWNLOAD  = 0,
                    STATUS    = 'Done',
                    NAME_FILE = :filename,
                    UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM')
                WHERE ID_JOB_REDIS = :jobId
            `;

                    // Prepare the update values
                    const updateValues = {
                        filename: zipFileName.split('\\').pop(),  // Get the zip file name from the generated file path
                        updated_at: formattedDate,  // Use the formatted date here
                        jobId: job.id  // The job ID that we are processing
                    };

                    // Execute the update query
                    await connection.execute(updateQuery, updateValues);
                    await connection.commit();
                    console.log(`Job status updated to 'Done' for job ID: ${job.id}`);
                    await redis.del('currentJobStatus');
                    console.log(`Job ID: ${jobId} is done`);

                    // Cek apakah ada job tertunda yang perlu diproses
                    await processPendingJobs();

                    return {
                        status: 'done',
                        zipFileName: zipFileName, // Add the file name to the return value
                        completionTime: completionTime, // Add the completion time
                        dataCount: dataCount,  // Number of records processed
                        elapsedTimeMinutes: elapsedTimeMinutes  // Processing time in minutes
                    };
                } catch (error) {
                    console.error('Error processing the job:', error);
                    await Sentry.startSpan({name: 'Log Error to File' + job.id, jobId: job.id}, async () => {

                        // Log the error details to file
                        logErrorToFileDCO(job.id, origin, destination, service, user_id, error.message);

                    });
                    Sentry.captureException(error);

                    return {
                        status: 'failed',
                        error: error.message
                    };
                }
            });
        }
    } catch (error) {
        console.error('Error processing job:', error);
        await redis.del('currentJobStatus');  // Lepaskan lock saat error
        Sentry.captureException(error);
    }
};

const processPendingJobs = async () => {
    const pendingJob = await redis.lpop('pending_jobs');  // Ambil job pertama dari antrian pending
    if (pendingJob) {
        const jobData = JSON.parse(pendingJob);
        console.log('Processing next pending job...');
        // Proses job yang tertunda
        await reportQueue.add(jobData);  // Menambahkan job ke dalam queue untuk diproses
    } else {
        console.log('No pending jobs.');
    }
};


// Tentukan bagaimana job akan diproses dalam queue
reportQueue.process(async (job) => processJob(job));
reportQueueTCI.process(async (job) => processJob(job));
reportQueueDCI.process(async (job) => processJob(job));
reportQueueDCO.process(async (job) => processJob(job));

// Menggunakan Promise untuk estimasi jumlah data
async function estimateDataCount({origin, destination, froms, thrus, user_id}) {
    return new Promise((resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;

                    let whereClause = "WHERE 1 = 1";
                    const bindParams = {};

                    if (origin !== '0') {
                        whereClause += ` AND OUTBOND_MANIFEST_ROUTE LIKE :origin`;
                        bindParams.origin = origin + '%';
                    }

                    if (destination !== '0') {
                        whereClause += ` AND OUTBOND_MANIFEST_ROUTE LIKE :destination`;
                        bindParams.destination = destination + '%';
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }


                    // Query untuk estimasi jumlah data
                    connection.execute(`
                            SELECT COUNT(*) AS DATA_COUNT
                            FROM CMS_COST_TRANSIT_V2 ${whereClause} AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE 
    AND CNOTE_WEIGHT > 0

                        `, bindParams, (err, result) => {
                        if (err) {
                            reject('Error executing query: ' + err.message);
                        } else {
                            resolve(result.rows.length > 0 ? result.rows[0][0] : 0);
                        }
                    });
                }
            });
        } catch (err) {
            reject('Error: ' + err.message);
        }
    });
}

async function estimateDataCountTCI({origin, destination, froms, thrus, user_id, TM, session}) {
    return new Promise((resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;

                    let whereClause = "WHERE 1 = 1";
                    const bindParams = {};

                    if (origin !== '0') {
                        whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`;
                        bindParams.origin = origin + '%';
                    }

                    if (destination !== '0') {
                        whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`;
                        bindParams.destination = destination + '%';
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }

                    if (TM !== '0') {
                        whereClause += ` AND SUBSTR(ORIGIN_TM, 1, 3) = :TM`;
                        bindParams.TM = TM;
                    }

                    // Query untuk estimasi jumlah data
                    connection.execute(`
                            SELECT COUNT(*) AS DATA_COUNT
                            FROM CMS_COST_TRANSIT_V2
                            ${whereClause}
                            AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                            AND CNOTE_WEIGHT > 0
                        `, bindParams, (err, result) => {
                        if (err) {
                            reject('Error executing query: ' + err.message);
                        } else {
                            resolve(result.rows.length > 0 ? result.rows[0][0] : 0);
                        }
                    });
                }
            });
        } catch (err) {
            reject('Error: ' + err.message);
        }
    });
}

async function estimateDataCountDCI({origin, destination, froms, thrus, service, user_id}) {
    return new Promise((resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;

                    let whereClause = "WHERE 1 = 1";
                    const bindParams = {};

                    if (origin !== '0') {
                        whereClause += ` AND SUBSTR(ORIGIN, 1, 3) LIKE SUBSTR(:origin, 1, 3)`;
                        bindParams.origin = origin + '%';
                    }

                    if (destination !== '0') {
                        whereClause += ` AND SUBSTR(DESTINATION, 1, 3) LIKE SUBSTR(:destination, 1, 3)`;
                        bindParams.destination = destination + '%';
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }

                    if (service !== '0') {
                        whereClause += ` AND SERVICE_CODE = :service`;
                        bindParams.service = service;
                    }

                    // Query untuk estimasi jumlah data
                    connection.execute(`
                            SELECT COUNT(*) AS DATA_COUNT
                            FROM CMS_COST_DELIVERY_V2
                                     ${whereClause}
                                AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
                --AND SERVICE_CODE NOT IN ('TRC11','TRC13')  -- remark by ibnu 18 sep 2024 req team ctc 
                AND SERVICES_CODE NOT IN ('CML','CTC_CML','P2P')

                AND CNOTE_NO NOT LIKE 'RT%' --10 OCT 2022 REQ RT TIDAK MASUK REQUEST BY RICKI, BA : YOGA 

                AND CNOTE_NO NOT LIKE 'FW%' --22 NOV 2022 REQ RT TIDAK MASUK REQUEST BY RICKI, BA : YOGA 
                            
                        `, bindParams, (err, result) => {
                        if (err) {
                            reject('Error executing query: ' + err.message);
                        } else {
                            resolve(result.rows.length > 0 ? result.rows[0][0] : 0);
                        }
                    });
                }
            });
        } catch (err) {
            reject('Error: ' + err.message);
        }
    });
}

async function estimateDataCountDCO({origin, destination, froms, thrus, service, user_id}) {
    return new Promise((resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;

                    let whereClause = "WHERE 1 = 1";
                    const bindParams = {};

                    if (origin !== '0') {
                        whereClause += ` AND SUBSTR(ORIGIN, 1, 3) = :origin`;
                        bindParams.origin = origin;
                    }

                    if (destination !== '0') {
                        whereClause += ` AND SUBSTR(DESTINATION, 1, 3) = :destination`;
                        bindParams.destination = destination;
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }

                    if (service !== '0') {
                        whereClause += ` AND SERVICES_CODE = :service`;
                        bindParams.service = service;
                    }

                    // Query untuk estimasi jumlah data
                    connection.execute(`
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM CMS_COST_DELIVERY_V2 ${whereClause}
                            AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
                             AND SERVICES_CODE NOT IN ('CML','CTC_CML','P2P')
                    `, bindParams, (err, result) => {
                        if (err) {
                            reject('Error executing query: ' + err.message);
                        } else {
                            resolve(result.rows.length > 0 ? result.rows[0][0] : 0);
                        }
                    });
                }
            });
        } catch (err) {
            reject('Error: ' + err.message);
        }
    });
}

async function fetchDataAndExportToExcel({origin, destination, froms, thrus, user_id, dateStr}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let userNameQuery = `SELECT USER_NAME
                                 FROM ORA_USER
                                 WHERE USER_ID = :user_id`;
            const userResult = await connection.execute(userNameQuery, [user_id]);
            let userName = userResult.rows.length > 0 ? userResult.rows[0][0] : 'Unknown';

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (origin !== '0') {
                whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`;
                bindParams.origin = origin + '%';
            }

            if (destination !== '0') {
                whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`;
                bindParams.destination = destination + '%';
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += ` AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }


            const result = await connection.execute(`
                SELECT 

    ROWNUM AS NO, 

    ''''|| AWB_NO AS CONNOTE_NUMBER, 

   ''''|| AWB_DATE AS CONNOTE_DATE, 

    SERVICES_CODE AS SERVICE_CONNOTE, 

    OUTBOND_MANIFEST_NO AS OUTBOND_MANIFEST_NUMBER, 

    OUTBOND_MANIFEST_DATE, 

    ORIGIN, 

    DESTINATION, 

    ZONA_DESTINATION, 

    OUTBOND_MANIFEST_ROUTE AS MANIFEST_ROUTE, 

    TRANSIT_MANIFEST_NO AS TRANSIT_MANIFEST_NUMBER, 

    TRANSIT_MANIFEST_DATE AS TRANSIT_MANIFEST_DATE, 

    TRANSIT_MANIFEST_ROUTE, --BAG_ROUTE 

    SMU_NUMBER, 

    FLIGHT_NUMBER, 

    BRANCH_TRANSPORTER, 

    ''''|| BAG_NO AS BAG_NUMBER, 

    SERVICE_BAG, 

    MODA, 

    MODA_TYPE, 

    CNOTE_WEIGHT AS WEIGHT_CONNOTE, 

    ACT_WEIGHT AS WEIGHT_BAG, 

    Round(PRORATED_WEIGHT,3) AS PRORATED_WEIGHT, 

    SUM(TRANSIT_FEE) AS TRANSIT_FEE, -- Gunakan SUM 

    SUM(HANDLING_FEE) AS HANDLING_FEE, -- Gunakan SUM 

    SUM(OTHER_FEE) AS OTHER_FEE, -- Gunakan SUM 

    SUM(NVL(TRANSIT_FEE,0) +  NVL(HANDLING_FEE,0)  + NVL(OTHER_FEE,0)) AS TOTAL, 
    ''''|| SYSDATE AS DOWNLOAD_DATE 
    FROM CMS_COST_TRANSIT_V2
                ${whereClause}
        AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
        AND CNOTE_WEIGHT > 0
                GROUP BY
                    ROWNUM,OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                    BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                    ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                    SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
            `, bindParams);


            dataCount = result.rows.length;

            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }

            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format

            const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', { total: chunks.length, width: 20 });


            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan');

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]);
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', userName]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(10);
                headerRow.values = [
                    'NO',
                    'CONNOTE_NUMBER',
                    'CONNOTE_DATE',
                    'SERVICE_CONNOTE',
                    'OUTBOND_MANIFEST_NUMBER',
                    'OUTBOND_MANIFEST_DATE',
                    'ORIGIN',
                    'DESTINATION',
                    'ZONA_DESTINATION',
                    'MANIFEST_ROUTE',
                    'TRANSIT_MANIFEST_NUMBER',
                    'TRANSIT_MANIFEST_DATE',
                    'TRANSIT_MANIFEST_ROUTE',
                    'SMU_NUMBER',
                    'FLIGHT_NUMBER',
                    'BRANCH_TRANSPORTER',
                    'BAG_NUMBER',
                    'SERVICE_BAG',
                    'MODA',
                    'MODA_TYPE',
                    'WEIGHT_CONNOTE',
                    'WEIGHT_BAG',
                    'PRORATED_WEIGHT',
                    'TRANSIT_FEE',
                    'HANDLING_FEE',
                    'OTHER_FEE',
                    'TOTAL',
                    'DOWNLOAD_DATE'
                ];
// Menambahkan alias ke setiap kolom
                worksheet.getRow(10).columns = [
                    {header: 'NO', key: 'no'},
                    {header: 'CONNOTE NUMBER', key: 'CONNOTE_NUMBER'},
                    {header: 'CONNOTE DATE', key: 'CONNOTE_DATE'},
                    {header: 'SERVICE CONNOTE', key: 'SERVICE_CONNOTE'},
                    {header: 'OUTBOND MANIFEST NUMBER', key: 'OUTBOND_MANIFEST_NUMBER'},
                    {header: 'OUTBOND MANIFEST DATE', key: 'OUTBOND_MANIFEST_DATE'},
                    {header: 'ORIGIN', key: 'ORIGIN'},
                    {header: 'DESTINATION', key: 'DESTINATION'},
                    {header: 'ZONA DESTINATION', key: 'ZONA_DESTINATION'},
                    {header: 'MANIFEST ROUTE', key: 'MANIFEST_ROUTE'},
                    {header: 'TRANSIT MANIFEST NUMBER', key: 'TRANSIT_MANIFEST_NUMBER'},
                    {header: 'TRANSIT MANIFEST DATE', key: 'TRANSIT_MANIFEST_DATE'},
                    {header: 'TRANSIT MANIFEST ROUTE', key: 'TRANSIT_MANIFEST_ROUTE'},
                    {header: 'SMU NUMBER', key: 'SMU_NUMBER'},
                    {header: 'FLIGHT NUMBER', key: 'FLIGHT_NUMBER'},
                    {header: 'BRANCH TRANSPORTER', key: 'BRANCH_TRANSPORTER'},
                    {header: 'BAG NUMBER', key: 'BAG_NUMBER'},
                    {header: 'SERVICE BAG', key: 'SERVICE_BAG'},
                    {header: 'MODA', key: 'MODA'},
                    {header: 'MODA TYPE', key: 'MODA_TYPE'},
                    {header: 'WEIGHT CONNOTE', key: 'WEIGHT_CONNOTE'},
                    {header: 'WEIGHT BAG', key: 'WEIGHT_BAG'},
                    {header: 'PRORATED WEIGHT', key: 'PRORATED_WEIGHT'},
                    {header: 'TRANSIT FEE', key: 'TRANSIT_FEE', style: { numFmt: '#,##0' }},  // Currency format
                    {header: 'HANDLING FEE', key: 'HANDLING_FEE', style: { numFmt: '#,##0' }},  // Currency format
                    {header: 'OTHER FEE', key: 'OTHER_FEE', style: { numFmt: '#,##0' }},  // Currency format
                    {header: 'TOTAL', key: 'TOTAL', style: { numFmt: '#,##0' }},
                    {header: 'DOWNLOAD DATE', key: 'DOWNLOAD_DATE'}
                ];
                chunk.forEach((row) => {
                    worksheet.addRow(row);
                });

                const fileName = path.join(folderPath, `TCOReport_${dateStr}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);
                console.log(`Data berhasil diekspor ke ${fileName}`);
                bar.tick();

            }

            const zipFileName = path.join(__dirname, 'file_download', `TCOReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 5}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmdirSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}

async function fetchDataAndExportToExcelTCI({
                                                origin,
                                                destination,
                                                froms,
                                                thrus,
                                                user_id,
                                                TM,
                                                user_session,
                                                dateStr
                                            }) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let userNameQuery = `SELECT USER_NAME FROM ORA_USER WHERE USER_ID = :user_id`;
            const userResult = await connection.execute(userNameQuery, [user_id]);
            let userName = userResult.rows.length > 0 ? userResult.rows[0][0] : 'Unknown';

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (origin !== '0') {
                whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`;
                bindParams.origin = origin + '%';
            }

            if (destination !== '0') {
                whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`;
                bindParams.destination = destination + '%';
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += ` AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (TM !== '0') {
                whereClause += ` AND SUBSTR(ORIGIN_TM, 1, 3) = :TM`;
                bindParams.TM = TM;
            }

            const result = await connection.execute(`
                SELECT
                    ''''|| AWB_NO AS CONNOTE_NUMBER,
                    ''''|| AWB_DATE AS CONNOTE_DATE,
                    SERVICES_CODE AS SERVICE_CONNOTE,
                    OUTBOND_MANIFEST_NO AS OUTBOND_MANIFEST_NUMBER,
                    OUTBOND_MANIFEST_DATE,
                    ORIGIN,
                    DESTINATION,
                    ZONA_DESTINATION,
                    OUTBOND_MANIFEST_ROUTE AS MANIFEST_ROUTE,
                    TRANSIT_MANIFEST_NO AS TRANSIT_MANIFEST_NUMBER,
                    TRANSIT_MANIFEST_DATE AS TRANSIT_MANIFEST_DATE,
                    TRANSIT_MANIFEST_ROUTE, 
                    SMU_NUMBER,
                    FLIGHT_NUMBER,
                    BRANCH_TRANSPORTER,
                    ''''|| BAG_NO AS BAG_NUMBER,
                    SERVICE_BAG,
                    MODA,
                    MODA_TYPE,
                    round(CNOTE_WEIGHT,3) AS WEIGHT_CONNOTE,
                    round(ACT_WEIGHT,3) AS WEIGHT_BAG,
                    round(PRORATED_WEIGHT,3) AS PRORATED_WEIGHT,
                    SUM(TRANSIT_FEE) AS TRANSIT_FEE, 
                    SUM(HANDLING_FEE) AS HANDLING_FEE, 
                    SUM(OTHER_FEE) AS OTHER_FEE,
                    SUM(NVL(TRANSIT_FEE,0) +  NVL(HANDLING_FEE,0)  + NVL(OTHER_FEE,0)) AS TOTAL,

                    ''''|| SYSDATE AS DOWNLOAD_DATE
                FROM CMS_COST_TRANSIT_V2
                         ${whereClause}
                    AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                    AND CNOTE_WEIGHT > 0
                GROUP BY
                    OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                    BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                    ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                    SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
            `, bindParams);

            dataCount = result.rows.length;

            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }

            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format

            const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }

            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan');

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]);
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
                worksheet.addRow(['Branch TM:', TM]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', userName]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(11);
                headerRow.values = [
                    'NO',
                    'CONNOTE_NUMBER',
                    'CONNOTE_DATE',
                    'SERVICE_CONNOTE',
                    'OUTBOND_MANIFEST_NUMBER',
                    'OUTBOND_MANIFEST_DATE',
                    'ORIGIN',
                    'DESTINATION',
                    'ZONA_DESTINATION',
                    'MANIFEST_ROUTE',
                    'TRANSIT_MANIFEST_NUMBER',
                    'TRANSIT_MANIFEST_DATE',
                    'TRANSIT_MANIFEST_ROUTE',
                    'SMU_NUMBER',
                    'FLIGHT_NUMBER',
                    'BRANCH_TRANSPORTER',
                    'BAG_NUMBER',
                    'SERVICE_BAG',
                    'MODA',
                    'MODA_TYPE',
                    'WEIGHT_CONNOTE',
                    'WEIGHT_BAG',
                    'PRORATED_WEIGHT',
                    'TRANSIT_FEE',
                    'HANDLING_FEE',
                    'OTHER_FEE',
                    'TOTAL',
                    'DOWNLOAD_DATE'
                ];

                worksheet.getRow(10).columns = [
                    {header: 'NO', key: 'NO'},
                    {header: 'CONNOTE NUMBER', key: 'CONNOTE_NUMBER'},
                    {header: 'CONNOTE DATE', key: 'CONNOTE_DATE'},
                    {header: 'SERVICE CONNOTE', key: 'SERVICE_CONNOTE'},
                    {header: 'OUTBOND MANIFEST NUMBER', key: 'OUTBOND_MANIFEST_NUMBER'},
                    {header: 'OUTBOND MANIFEST DATE', key: 'OUTBOND_MANIFEST_DATE'},
                    {header: 'ORIGIN', key: 'ORIGIN'},
                    {header: 'DESTINATION', key: 'DESTINATION'},
                    {header: 'ZONA DESTINATION', key: 'ZONA_DESTINATION'},
                    {header: 'MANIFEST ROUTE', key: 'MANIFEST_ROUTE'},
                    {header: 'TRANSIT MANIFEST NUMBER', key: 'TRANSIT_MANIFEST_NUMBER'},
                    {header: 'TRANSIT MANIFEST DATE', key: 'TRANSIT_MANIFEST_DATE'},
                    {header: 'TRANSIT MANIFEST ROUTE', key: 'TRANSIT_MANIFEST_ROUTE'},
                    {header: 'SMU NUMBER', key: 'SMU_NUMBER'},
                    {header: 'FLIGHT NUMBER', key: 'FLIGHT_NUMBER'},
                    {header: 'BRANCH TRANSPORTER', key: 'BRANCH_TRANSPORTER'},
                    {header: 'BAG NUMBER', key: 'BAG_NUMBER'},
                    {header: 'SERVICE BAG', key: 'SERVICE_BAG'},
                    {header: 'MODA', key: 'MODA'},
                    {header: 'MODA TYPE', key: 'MODA_TYPE'},
                    {header: 'WEIGHT CONNOTE', key: 'WEIGHT_CONNOTE'},
                    {header: 'WEIGHT BAG', key: 'WEIGHT_BAG'},
                    {header: 'PRORATED WEIGHT', key: 'PRORATED_WEIGHT'},
                    {header: 'TRANSIT FEE', key: 'TRANSIT_FEE', style: {numFmt: '#,##0.00'}},
                    {header: 'HANDLING FEE', key: 'HANDLING_FEE', style: {numFmt: '#,##0.00'}},
                    {header: 'OTHER FEE', key: 'OTHER_FEE', style: {numFmt: '#,##0.00'}},
                    {header: 'TOTAL', key: 'TOTAL', style: {numFmt: '#,##0.00'}},
                    {header: 'DOWNLOAD DATE', key: 'DOWNLOAD_DATE'}
                ];

                let rowNumber = 1;
                chunk.forEach((row) => {
                    worksheet.addRow([rowNumber++, ...row]);
                });

                const fileName = path.join(folderPath, `TCIReport_${dateStr}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `TCIReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 5}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmdirSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}


async function fetchDataAndExportToExcelDCI({origin, destination, froms, thrus, service, user_id, dateStr}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let userNameQuery = `SELECT USER_NAME FROM ORA_USER WHERE USER_ID = :user_id`;
            const userResult = await connection.execute(userNameQuery, [user_id]);
            let userName = userResult.rows.length > 0 ? userResult.rows[0][0] : 'Unknown';

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (origin !== '0') {
                whereClause += ` AND SUBSTR(ORIGIN, 1, 3) LIKE SUBSTR(:origin , 1, 3)`;
                bindParams.origin = origin + '%';
            }

            if (destination !== '0') {

                whereClause += ` AND  SUBSTR(DESTINATION,1,3) LIKE SUBSTR(:destination,1,3) `;
                bindParams.destination = destination + '%';
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += ` AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (service !== '0') {
                whereClause += ` AND SERVICES_CODE LIKE :service`;
                bindParams.service = service + '%';
            }

            const result = await connection.execute(`

                SELECT

                    ROWNUM AS NO, 

       MANIFEST_NO, 

       --TO_CHAR(MANIFEST_DATE,'MM-DD-RRRR') MANIFEST_DATE, 

       MANIFEST_DATE, 

       SERVICES_CODE, 

       CASE WHEN SERVICES_CODE LIKE 'JTR%' THEN 'LOG' ELSE 'EXP' END TIPE, 

       ORIGIN, 

       DESTINATION, 

       ZONA_DESTINATION, 

       ''''||CNOTE_NO AS CNOTE_NO, 

       CNOTE_DATE, 

       NVL(QTY,0) QTY, 

       CASE      

             WHEN WEIGHT = 0 THEN 0 

             WHEN WEIGHT < 1 THEN 1 

             WHEN RPAD(REGEXP_SUBSTR(WEIGHT , '[[:digit:]]+$'),3,0) > 300 THEN CEIL(WEIGHT ) 

             ELSE FLOOR(WEIGHT )

                END WEIGHT, 

       NVL(DELIVERY,0) DELIVERY, 

       NVL(DELIVERY_SPS, 0) DELIVERY_SPS, 

       NVL(TRANSIT,0)  AS BIAYA_TRANSIT, 

       NVL(LINEHAUL_FIRST,0) LINEHAUL_FIRST,    -- remark by ibnu 01 oct 2024 di ambil nilai inehaulnya saja  

       nvl(AMOUNT,0) AMOUNT, 

       nvl(LINEHAUL_NEXT,0) LINEHAUL_NEXT 

        FROM CMS_COST_DELIVERY_V2

                ${whereClause}

                AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)

                --AND SERVICE_CODE NOT IN ('TRC11','TRC13')  -- remark by ibnu 18 sep 2024 req team ctc 

                AND SERVICES_CODE NOT IN ('CML','CTC_CML','P2P')

                AND CNOTE_NO NOT LIKE 'RT%' --10 OCT 2022 REQ RT TIDAK MASUK REQUEST BY RICKI, BA : YOGA 

                AND CNOTE_NO NOT LIKE 'FW%' --22 NOV 2022 REQ RT TIDAK MASUK REQUEST BY RICKI, BA : YOGA 
            `, bindParams);

            dataCount = result.rows.length;

            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }
            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format

            const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', { total: chunks.length, width: 20 });

            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan');

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]);
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
                worksheet.addRow(['Service Code:', service === '0' ? 'ALL' : service]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', userName]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(10);
                headerRow.values = [
                    'NO',
                    'CNOTE_NO',
                    'CNOTE_DATE',
                    'ORIGIN',
                    'DESTINATION',
                    'ZONA_DESTINATION',
                    'SERVICES_CODE',
                    'QTY',
                    'WEIGHT',
                    'AMOUNT',
                    'MANIFEST_NO',
                    'MANIFEST_DATE',
                    'DELIVERY',
                    'DELIVERY_SPS',
                    'BIAYA_TRANSIT',
                    'LINEHAUL_FIRST',
                    'LINEHAUL_NEXT',
                ];


                worksheet.columns = [
                    {header: 'NO', key: 'NO'},
                    {header: 'CNOTE NO', key: 'CNOTE_NO'},
                    {header: 'CNOTE DATE', key: 'CNOTE_DATE'},
                    {header: 'ORIGIN', key: 'ORIGIN'},
                    {header: 'DESTINATION', key: 'DESTINATION'},
                    {header: 'ZONA DESTINATION', key: 'ZONA_DESTINATION'},
                    {header: 'SERVICES CODE', key: 'SERVICES_CODE'},
                    {header: 'QTY', key: 'QTY'},
                    {header: 'WEIGHT', key: 'WEIGHT'},
                    {header: 'AMOUNT', key: 'AMOUNT'},
                    {header: 'MANIFEST NO', key: 'MANIFEST_NO'},
                    {header: 'MANIFEST DATE', key: 'MANIFEST_DATE'},
                    {header: 'DELIVERY', key: 'DELIVERY'},
                    {header: 'DELIVERY SPS', key: 'DELIVERY_SPS'},
                    {header: 'BIAYA TRANSIT', key: 'BIAYA_TRANSIT'},
                    {header: 'BIAYA PENERUS', key: 'LINEHAUL_FIRST'},
                    {header: 'BIAYA PENERUS NEXT KG', key: 'LINEHAUL_NEXT'},
                ];
                let rowNumber = 1;
                chunk.forEach((row) => {
                    worksheet.addRow([rowNumber++, ...row]);
                });

                const fileName = path.join(folderPath, `DCIReport_${dateStr}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);
                bar.tick();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `DCIReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 5}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmdirSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}

async function fetchDataAndExportToExcelDCO({origin, destination, froms, thrus, service, user_id, dateStr}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let userNameQuery = `SELECT USER_NAME FROM ORA_USER WHERE USER_ID = :user_id`;
            const userResult = await connection.execute(userNameQuery, [user_id]);
            let userName = userResult.rows.length > 0 ? userResult.rows[0][0] : 'Unknown';

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (origin !== '0') {
                whereClause += ` AND SUBSTR(ORIGIN, 1, 3) = :origin`;
                bindParams.origin = origin;
            }

            if (destination !== '0') {
                whereClause += ` AND SUBSTR(DESTINATION, 1, 3) = :destination`;
                bindParams.destination = destination;
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += ` AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (service !== '0') {
                whereClause += ` AND SERVICES_CODE = :service`;
                bindParams.service = service;
            }


            const result = await connection.execute(`
                SELECT ROWNUM AS NO,
           MANIFEST_NO,
           MANIFEST_DATE,
           SERVICES_CODE,
           CASE 
               WHEN SERVICES_CODE LIKE 'JTR%' THEN 'LOG' 
               ELSE 'EXP'
                END AS TIPE,
           ORIGIN,
           DESTINATION,
           ZONA_DESTINATION,
           '''' || CNOTE_NO AS CNOTE_NO,
           CNOTE_DATE,
           NVL(QTY, 0) AS QTY,
           CASE     
               WHEN WEIGHT = 0 THEN 0
               WHEN WEIGHT < 1 THEN 1
--                WHEN WEIGHT > 300 THEN CEIL(WEIGHT)  -- Rounds up if weight > 300
--                ELSE ROUND(WEIGHT)  -- Rounds to the nearest integer
--                 END AS WEIGHT,
                      WHEN RPAD(REGEXP_SUBSTR(WEIGHT , '[[:digit:]]+$'),3,0) > 300 THEN CEIL(WEIGHT )
             ELSE FLOOR(WEIGHT )
                END WEIGHT,
           NVL(DELIVERY, 0) AS DELIVERY,
           NVL(DELIVERY_SPS, 0) AS DELIVERY_SPS,
           NVL(TRANSIT, 0) AS BIAYA_TRANSIT,
           NVL(LINEHAUL_FIRST, 0) AS LINEHAUL_FIRST,
           NVL(AMOUNT, 0) AS AMOUNT,
           NVL(LINEHAUL_NEXT, 0) AS LINEHAUL_NEXT
    FROM CMS_COST_DELIVERY_V2
     ${whereClause}
                AND SUBSTR(ORIGIN, 1, 3) <> SUBSTR(DESTINATION, 1, 3)
                AND SERVICES_CODE NOT IN ('CML', 'CTC_CML', 'P2P')
                AND CNOTE_NO NOT LIKE 'RT%'  -- Exclude records with CNOTE_NO starting with 'RT'
                AND CNOTE_NO NOT LIKE 'FW%'  -- Exclude records with CNOTE_NO starting with 'FW'
            `, bindParams);


            dataCount = result.rows.length;

            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }

            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format

            const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', { total: chunks.length, width: 20 });

            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan');

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]);
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
                worksheet.addRow(['Service Code:', service === '0' ? 'ALL' : service]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', userName]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(10);
                headerRow.values = [
                    'NO',
                    'CNOTE_NO',
                    'CNOTE_DATE',
                    'ORIGIN',
                    'DESTINATION',
                    'QTY',
                    'ZONA_DESTINATION',
                    'SERVICES_CODE',
                    'WEIGHT',
                    'AMOUNT',
                    'MANIFEST_NO',
                    'MANIFEST_DATE',
                    'DELIVERY',
                    'DELIVERY_SPS',
                    'BIAYA_TRANSIT',
                    'LINEHAUL_FIRST',
                    'LINEHAUL_NEXT',
                ];

                worksheet.columns = [
                    {header: 'NO', key: 'NO', width: 5},
                    {header: 'CNOTE NO', key: 'CNOTE_NO', width: 15},
                    {header: 'CNOTE DATE', key: 'CNOTE_DATE', width: 15},
                    {header: 'ORIGIN', key: 'ORIGIN', width: 15},
                    {header: 'DESTINATION', key: 'DESTINATION', width: 15},
                    {header: 'COLLY', key: 'QTY', width: 10},
                    {header: 'ZONA DESTINATION', key: 'ZONA_DESTINATION', width: 15},
                    {header: 'SERVICES CODE', key: 'SERVICES_CODE', width: 15},
                    {header: 'WEIGHT', key: 'WEIGHT', width: 10},
                    {header: 'AMOUNT', key: 'AMOUNT', width: 10, style: {numFmt: '#,##0.00'}},
                    {header: 'MANIFEST NO', key: 'MANIFEST_NO', width: 15},
                    {header: 'MANIFEST DATE', key: 'MANIFEST_DATE', width: 15},
                    {header: 'DELIVERY', key: 'DELIVERY', width: 10, style: {numFmt: '#,##0.00'}},
                    {header: 'DELIVERY SPS', key: 'DELIVERY_SPS', width: 10, style: {numFmt: '#,##0.00'}},
                    {header: 'BIAYA TRANSIT', key: 'BIAYA_TRANSIT', width: 10, style: {numFmt: '#,##0.00'}},
                    {header: 'BIAYA PENERUS', key: 'LINEHAUL_FIRST', width: 10, style: {numFmt: '#,##0.00'}},
                    {header: 'BIAYA PENERUS NEXT KG', key: 'LINEHAUL_NEXT', width: 10, style: {numFmt: '#,##0.00'}},
                ]

                chunk.forEach((row) => {
                    worksheet.addRow(row);
                });


                const fileName = path.join(folderPath, `DCOReport_${dateStr}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);
                bar.tick();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `DCOReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 5}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmdirSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}

// Define API endpoint with query parameters
app.get('/getreporttco', async (req, res) => {
    try {
        const {origin, destination, froms, thrus, user_id} = req.query;

        if (!origin || !destination || !froms || !thrus || !user_id) {
            return res.status(400).json({success: false, message: 'Missing required parameters'});
        }

        // Get the number of jobs that are waiting or active
        const activeJobs = await reportQueue.getJobs(['waiting', 'active']);

        // Check if the queue has more than 20 jobs
        if (activeJobs.length >= 20) {
            return res.status(503).json({
                success: false,
                message: 'Antrian penuh, coba beberapa saat lagi.'
            });
        }

        // Estimasi jumlah data
        const estimatedDataCount = await estimateDataCount({origin, destination, froms, thrus, user_id});

        // Calculate the estimated time based on the benchmark
        const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
        const estimatedTimeMinutes = (estimatedDataCount / benchmarkRecordsPerMinute) * 2;  // Estimated time in minutes

        const today = new Date();
        const dateStr = today.toISOString().split('T')[0];

        // Add the job to the queue
        const job = await reportQueue.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            dateStr
        });


        const connection = await oracledb.getConnection(config);

        const insertQuery = `
                INSERT INTO CMS_COST_TRANSIT_V2_LOG (USER_NAME, NAME_FILE, DURATION, CATEGORY, PERIODE, STATUS,
                                                     DOWNLOAD, CREATED_AT, ID_JOB_REDIS, DATACOUNT)
                VALUES (:user_name, :name_file, :duration, :category, :periode, :status, :download, :created_at,
                        :id_job, :datacount)
            `;

        // Set values to be inserted
        const insertValues = {
            id_job: job.id,
            user_name: user_id,  // user_id sebagai USER_NAME
            name_file: '',       // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: estimatedTimeMinutes.toFixed(2), // Estimasi waktu
            category: 'TCO',     // Kategori adalah TCI
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: 'Pending',   // Status awal adalah Pending
            download: 0,         // Belum diunduh, set download = 0
            created_at: new Date(), // Timestamp saat data dimasukkan,
            datacount: estimatedDataCount


        };

        await connection.execute(insertQuery, insertValues);
        await connection.commit();
        // res.status(200).json({
        //     success: true,
        //     message: 'Job added successfully, processing in the background.',
        //     jobId: job.id,
        //     estimatedDataCount: estimatedDataCount , // Send the estimated data count
        //     estimatedTimeMinutes: estimatedTimeMinutes.toFixed(2) // Estimated processing time in minutes
        // });
        const logFilePath = path.join(__dirname, 'log_files', `JNE_REPORT_TCO_${job.id}.txt`);
        const logMessage = `
            Job ID: ${job.id}
            Origin: ${origin}
            Destination: ${destination}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), {recursive: true});
        }

        // Write the log message to the file
        fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // Send the log file for download
        res.download(logFilePath, (err) => {
            if (err) {
                console.error('Error downloading the log file:', err);
                res.status(500).send({
                    success: false,
                    message: 'An error occurred while downloading the log file.'
                });
            } else {
                console.log('Log file sent for download');
            }
        });

    } catch (err) {
        console.error('Error adding job to queue:', err);
        res.status(500).send({success: false, message: 'An error occurred while adding the job.'});
    }
});
app.get('/getreporttci', async (req, res) => {
    try {
        const {origin, destination, froms, thrus, user_id, TM, user_session} = req.query;

        if (!origin || !destination || !froms || !thrus || !user_id || !TM || !user_session) {
            return res.status(400).json({success: false, message: 'Missing required parameters'});
        }

        // Get the number of jobs that are waiting or active
        const activeJobs = await reportQueueTCI.getJobs(['waiting', 'active']);

        // Check if the queue has more than 20 jobs
        if (activeJobs.length >= 10) {
            return res.status(503).json({
                success: false,
                message: 'Antrian penuh, coba beberapa saat lagi.'
            });
        }

        // Estimasi jumlah data
        const estimatedDataCount = await estimateDataCountTCI({origin, destination, froms, thrus, user_id, TM});

        // Calculate the estimated time based on the benchmark
        const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
        const estimatedTimeMinutes = (estimatedDataCount / benchmarkRecordsPerMinute) * 2;  // Estimated time in minutes

        const today = new Date();
        const dateStr = today.toISOString().split('T')[0];

        // Add the job to the queue
        const job = await reportQueueTCI.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            TM,
            user_session,
            dateStr
        });


        const connection = await oracledb.getConnection(config);

        const insertQuery = `
            INSERT INTO CMS_COST_TRANSIT_V2_LOG (
              USER_NAME, NAME_FILE, DURATION, CATEGORY, PERIODE, STATUS, DOWNLOAD, CREATED_AT, ID_JOB_REDIS , DATACOUNT
            ) VALUES (
               :user_name, :name_file, :duration, :category, :periode, :status, :download, :created_at, :id_job, :datacount
            )
        `;

        // Set values to be inserted
        const insertValues = {
            id_job: job.id,
            user_name: user_id,  // user_id sebagai USER_NAME
            name_file: '',       // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: estimatedTimeMinutes.toFixed(2), // Estimasi waktu
            category: 'TCI',     // Kategori adalah TCI
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: 'Pending',   // Status awal adalah Pending
            download: 0,         // Belum diunduh, set download = 0
            created_at: new Date(), // Timestamp saat data dimasukkan,
            datacount: estimatedDataCount


        };

        await connection.execute(insertQuery, insertValues);
        await connection.commit();
        // res.status(200).json({
        //     success: true,
        //     message: 'Job added successfully, processing in the background.',
        //     jobId: job.id,
        //     estimatedDataCount: estimatedDataCount , // Send the estimated data count
        //     estimatedTimeMinutes: estimatedTimeMinutes.toFixed(2) // Estimated processing time in minutes
        // });
        const logFilePath = path.join(__dirname, 'log_files', `JNE_REPORT_TCI_${job.id}.txt`);
        const logMessage = `
            Job ID: ${job.id}
            Origin: ${origin}
            Destination: ${destination}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            TM: ${TM}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), {recursive: true});
        }
        // http://10.8.2.48:8080/ords/f?p=101:78:17076041502424::NO::P78_USER:YASIQIN
        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:78:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
        // Write the log message to the file
        // fs.writeFileSync(logFilePath, logMessage, 'utf8');
        //
        // // Send the log file for download
        // res.download(logFilePath, (err) => {
        //     if (err) {
        //         console.error('Error downloading the log file:', err);
        //         res.status(500).send({ success: false, message: 'An error occurred while downloading the log file.' });
        //     } else {
        //         console.log('Log file sent for download');
        //
        //     }
        // });

        //     refresh


    } catch (err) {
        console.error('Error adding job to queue:', err);
        res.status(500).send({success: false, message: 'An error occurred while adding the job.'});
    }
});
app.get('/getreportdci', async (req, res) => {
    try {
        const {origin, destination, froms, thrus, service, user_id} = req.query;

        if (!origin || !destination || !froms || !thrus || !user_id || !service) {
            return res.status(400).json({success: false, message: 'Missing required parameters'});
        }

        // Get the number of jobs that are waiting or active
        const activeJobs = await reportQueueDCI.getJobs(['waiting', 'active']);

        // Check if the queue has more than 20 jobs
        if (activeJobs.length >= 10) {
            return res.status(503).json({
                success: false,
                message: 'Antrian penuh, coba beberapa saat lagi.'
            });
        }
        // Estimasi jumlah data
        const estimatedDataCount = await estimateDataCountDCI({
            origin,
            destination,
            froms,
            thrus,
            service,
            user_id
        });

        // Calculate the estimated time based on the benchmark
        const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
        const estimatedTimeMinutes = (estimatedDataCount / benchmarkRecordsPerMinute) * 2;  // Estimated time in minutes

        const today = new Date();
        const dateStr = today.toISOString().split('T')[0];

        // Add the job to the queue
        const job = await reportQueueDCI.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            service,
            dateStr
        });


        const connection = await oracledb.getConnection(config);

        const insertQuery = `
            INSERT INTO CMS_COST_TRANSIT_V2_LOG (
              USER_NAME, NAME_FILE, DURATION, CATEGORY, PERIODE, STATUS, DOWNLOAD, CREATED_AT, ID_JOB_REDIS , DATACOUNT
            ) VALUES (
               :user_name, :name_file, :duration, :category, :periode, :status, :download, :created_at, :id_job, :datacount
            )
        `;

        // Set values to be inserted
        const insertValues = {
            id_job: job.id,
            user_name: user_id,  // user_id sebagai USER_NAME
            name_file: '',       // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: estimatedTimeMinutes.toFixed(2), // Estimasi waktu
            category: 'DCI',     // Kategori adalah TCI
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: 'Pending',   // Status awal adalah Pending
            download: 0,         // Belum diunduh, set download = 0
            created_at: new Date(), // Timestamp saat data dimasukkan,
            datacount: estimatedDataCount


        };

        await connection.execute(insertQuery, insertValues);
        await connection.commit();
        // res.status(200).json({
        //     success: true,
        //     message: 'Job added successfully, processing in the background.',
        //     jobId: job.id,
        //     estimatedDataCount: estimatedDataCount , // Send the estimated data count
        //     estimatedTimeMinutes: estimatedTimeMinutes.toFixed(2) // Estimated processing time in minutes
        // });
        const logFilePath = path.join(__dirname, 'log_files', `JNE_REPORT_DCI_${job.id}.txt`);
        const logMessage = `
            Job ID: ${job.id}
            Origin: ${origin}
            Destination: ${destination}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            service: ${service}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), {recursive: true});
        }

        // Write the log message to the file
        fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // Send the log file for download
        res.download(logFilePath, (err) => {
            if (err) {
                console.error('Error downloading the log file:', err);
                res.status(500).send({
                    success: false,
                    message: 'An error occurred while downloading the log file.'
                });
            } else {
                console.log('Log file sent for download');
            }
        });

    } catch (err) {
        console.error('Error adding job to queue:', err);
        res.status(500).send({success: false, message: 'An error occurred while adding the job.'});
    }
});
app.get('/getreportdco', async (req, res) => {
    try {
        const {origin, destination, froms, thrus, service, user_id} = req.query;

        if (!origin || !destination || !froms || !thrus || !user_id || !service) {
            return res.status(400).json({success: false, message: 'Missing required parameters'});
        }

        // Get the number of jobs that are waiting or active
        const activeJobs = await reportQueueDCO.getJobs(['waiting', 'active']);

        // Check if the queue has more than 20 jobs
        if (activeJobs.length >= 10) {
            return res.status(503).json({
                success: false,
                message: 'Antrian penuh, coba beberapa saat lagi.'
            });
        }

        // Estimasi jumlah data
        const estimatedDataCount = await estimateDataCountDCO({
            origin,
            destination,
            froms,
            thrus,
            service,
            user_id
        });

        // Calculate the estimated time based on the benchmark
        const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
        const estimatedTimeMinutes = (estimatedDataCount / benchmarkRecordsPerMinute) * 2;  // Estimated time in minutes

        const today = new Date();
        const dateStr = today.toISOString().split('T')[0];

        // Add the job to the queue
        const job = await reportQueueDCO.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            service,
            dateStr
        });


        const connection = await oracledb.getConnection(config);

        const insertQuery = `
            INSERT INTO CMS_COST_TRANSIT_V2_LOG (
                USER_NAME, NAME_FILE, DURATION, CATEGORY, PERIODE, STATUS, DOWNLOAD, CREATED_AT, ID_JOB_REDIS , DATACOUNT
            ) VALUES (
                         :user_name, :name_file, :duration, :category, :periode, :status, :download, :created_at, :id_job, :datacount
                     )
        `;

        // Set values to be inserted
        const insertValues = {
            id_job: job.id,
            user_name: user_id,  // user_id sebagai USER_NAME
            name_file: '',       // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: estimatedTimeMinutes.toFixed(2), // Estimasi waktu
            category: 'DCO',     // Kategori adalah TCI
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: 'Pending',   // Status awal adalah Pending
            download: 0,         // Belum diunduh, set download = 0
            created_at: new Date(), // Timestamp saat data dimasukkan,
            datacount: estimatedDataCount


        };

        await connection.execute(insertQuery, insertValues);
        await connection.commit();
        // res.status(200).json({
        //     success: true,
        //     message: 'Job added successfully, processing in the background.',
        //     jobId: job.id,
        //     estimatedDataCount: estimatedDataCount , // Send the estimated data count
        //     estimatedTimeMinutes: estimatedTimeMinutes.toFixed(2) // Estimated processing time in minutes
        // });
        const logFilePath = path.join(__dirname, 'log_files', `JNE_REPORT_DCO_${job.id}.txt`);
        const logMessage = `
            Job ID: ${job.id}
            Origin: ${origin}
            Destination: ${destination}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            Service: ${service}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), {recursive: true});
        }

        // Write the log message to the file
        fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // Send the log file for download
        res.download(logFilePath, (err) => {
            if (err) {
                console.error('Error downloading the log file:', err);
                res.status(500).send({
                    success: false,
                    message: 'An error occurred while downloading the log file.'
                });
            } else {
                console.log('Log file sent for download');
            }
        });

    } catch (err) {
        console.error('Error adding job to queue:', err);
        res.status(500).send({success: false, message: 'An error occurred while adding the job.'});
    }
});

app.get('/jobstco', async (req, res) => {
    try {
        // Get the list of all jobs in the queue
        const jobs = await reportQueue.getJobs();

        // Count the jobs based on their status
        const succeededJobs = await reportQueue.getJobs(['completed']);
        const waitingJobs = await reportQueue.getJobs(['waiting']);
        const activeJobs = await reportQueue.getJobs(['active']);

        // Prepare table headers
        const headers = ['ID', 'User ID', 'Progress', 'Status', 'Error Reason'];

        // Format job data into a table structure
        const table = jobs.map((job) => {
            return {
                ID: job.id,
                UserID: job.data.user_id,
                Progress: job.progress,
                Status: job.returnvalue ? job.returnvalue : 'Failed',
                ErrorReason: job.failedReason || '-'
            };
        });

        // Respond with table headers, job data, and the count of jobs
        res.json({
            success: true,
            tableHeaders: headers,
            tableData: table,
            totalJobs: jobs.length, // Total number of jobs
            succeededJobs: succeededJobs.length, // Total succeeded jobs
            waitingJobs: waitingJobs.length, // Total waiting jobs
            activeJobs: activeJobs.length // Total active jobs
        });

    } catch (err) {
        console.error('Error getting jobs:', err);
        res.status(500).send({success: false, message: 'Error fetching jobs.'});
    }
});
app.get('/jobstci', async (req, res) => {
    try {
        // Get the list of all jobs in the queue
        const jobs = await reportQueue.getJobs();

        // Count the jobs based on their status
        const succeededJobs = await reportQueueTCI.getJobs(['completed']);
        const waitingJobs = await reportQueueTCI.getJobs(['waiting']);
        const activeJobs = await reportQueueTCI.getJobs(['active']);

        // Prepare table headers
        const headers = ['ID', 'User ID', 'Progress', 'Status', 'Error Reason'];

        // Format job data into a table structure
        const table = jobs.map((job) => {
            return {
                ID: job.id,
                UserID: job.data.user_id,
                Progress: job.progress,
                Status: job.returnvalue ? job.returnvalue : 'Failed',
                ErrorReason: job.failedReason || '-'
            };
        });

        // Respond with table headers, job data, and the count of jobs
        res.json({
            success: true,
            tableHeaders: headers,
            tableData: table,
            totalJobs: jobs.length, // Total number of jobs
            succeededJobs: succeededJobs.length, // Total succeeded jobs
            waitingJobs: waitingJobs.length, // Total waiting jobs
            activeJobs: activeJobs.length // Total active jobs
        });

    } catch (err) {
        console.error('Error getting jobs:', err);
        res.status(500).send({success: false, message: 'Error fetching jobs.'});
    }
});
app.get('/jobsdci', async (req, res) => {
    try {
        // Get the list of all jobs in the queue
        const jobs = await reportQueueDCI.getJobs();

        // Count the jobs based on their status
        const succeededJobs = await reportQueue.getJobs(['completed']);
        const waitingJobs = await reportQueue.getJobs(['waiting']);
        const activeJobs = await reportQueue.getJobs(['active']);

        // Prepare table headers
        const headers = ['ID', 'User ID', 'Progress', 'Status', 'Error Reason'];

        // Format job data into a table structure
        const table = jobs.map((job) => {
            return {
                ID: job.id,
                UserID: job.data.user_id,
                Progress: job.progress,
                Status: job.returnvalue ? job.returnvalue : 'Failed',
                ErrorReason: job.failedReason || '-'
            };
        });

        // Respond with table headers, job data, and the count of jobs
        res.json({
            success: true,
            tableHeaders: headers,
            tableData: table,
            totalJobs: jobs.length, // Total number of jobs
            succeededJobs: succeededJobs.length, // Total succeeded jobs
            waitingJobs: waitingJobs.length, // Total waiting jobs
            activeJobs: activeJobs.length // Total active jobs
        });

    } catch (err) {
        console.error('Error getting jobs:', err);
        res.status(500).send({success: false, message: 'Error fetching jobs.'});
    }
});
app.get('/jobsdco', async (req, res) => {
    try {
        // Get the list of all jobs in the queue
        const jobs = await reportQueueDCO.getJobs();

        // Count the jobs based on their status
        const succeededJobs = await reportQueue.getJobs(['completed']);
        const waitingJobs = await reportQueue.getJobs(['waiting']);
        const activeJobs = await reportQueue.getJobs(['active']);

        // Prepare table headers
        const headers = ['ID', 'User ID', 'Progress', 'Status', 'Error Reason'];

        // Format job data into a table structure
        const table = jobs.map((job) => {
            return {
                ID: job.id,
                UserID: job.data.user_id,
                Progress: job.progress,
                Status: job.returnvalue ? job.returnvalue : 'Failed',
                ErrorReason: job.failedReason || '-'
            };
        });

        // Respond with table headers, job data, and the count of jobs
        res.json({
            success: true,
            tableHeaders: headers,
            tableData: table,
            totalJobs: jobs.length, // Total number of jobs
            succeededJobs: succeededJobs.length, // Total succeeded jobs
            waitingJobs: waitingJobs.length, // Total waiting jobs
            activeJobs: activeJobs.length // Total active jobs
        });

    } catch (err) {
        console.error('Error getting jobs:', err);
        res.status(500).send({success: false, message: 'Error fetching jobs.'});
    }
});

app.get('/jobstco/:id', async (req, res) => {
    try {
        const job = await reportQueue.getJob(req.params.id);
        if (job) {
            let statusfix = 'Failed';  // Default status is Failed
            let errorMessage = '';
            let filenamezip = 'Not Available';

            console.log('test' + JSON.stringify(job.returnvalue));
            // If job is done and no error, status is Success
            if (job.returnvalue) {
                if (job.returnvalue.status === 'done') {
                    statusfix = 'Sukses';
                    filenamezip = job.returnvalue.zipFileName;  // If job is done, attach filename
                } else if (job.returnvalue.error) {
                    statusfix = 'Failed';
                    errorMessage = job.returnvalue.error;  // Include error message if exists
                }
            } else {
                if (job.isWaiting()) {
                    statusfix = 'Pending';  // Job is waiting in the queue
                } else {
                    statusfix = 'Failed';
                }
            }

            res.json({
                success: true,
                id: job.id,
                Origin: job.data.origin,
                Destination: job.data.destination,
                FromDate: job.data.froms,
                ToDate: job.data.thrus,
                UserID: job.data.user_id,
                service: job.data.service,
                Status: statusfix,  // Return statusfix (Sukses, Failed, or Pending)
                filenamezip: filenamezip,
                errorMessage: errorMessage // Include error message if exists
            });
        } else {
            res.status(404).send({success: false, message: 'Job not found.'});
        }
    } catch (err) {
        console.error('Error fetching job:', err);
        res.status(500).send({success: false, message: 'Error fetching job.'});
    }
});
app.get('/jobstci/:id', async (req, res) => {
    try {
        const job = await reportQueueTCI.getJob(req.params.id);
        if (job) {
            let statusfix = 'Failed';  // Default status is Failed
            let errorMessage = '';
            let filenamezip = 'Not Available';

            console.log('test' + JSON.stringify(job.returnvalue));
            // If job is done and no error, status is Success
            if (job.returnvalue) {
                if (job.returnvalue.status === 'done') {
                    statusfix = 'Sukses';
                    filenamezip = job.returnvalue.zipFileName;  // If job is done, attach filename
                } else if (job.returnvalue.error) {
                    statusfix = 'Failed';
                    errorMessage = job.returnvalue.error;  // Include error message if exists
                }
            } else {
                if (job.isWaiting()) {
                    statusfix = 'Pending';  // Job is waiting in the queue
                } else {
                    statusfix = 'Failed';
                }
            }

            res.json({
                success: true,
                id: job.id,
                Origin: job.data.origin,
                Destination: job.data.destination,
                FromDate: job.data.froms,
                ToDate: job.data.thrus,
                UserID: job.data.user_id,
                TM: job.data.TM,
                Status: statusfix,  // Return statusfix (Sukses, Failed, or Pending)
                filenamezip: filenamezip,
                errorMessage: errorMessage // Include error message if exists
            });
        } else {
            res.status(404).send({success: false, message: 'Job not found.'});
        }
    } catch (err) {
        console.error('Error fetching job:', err);
        res.status(500).send({success: false, message: 'Error fetching job.'});
    }
});
app.get('/jobsdci/:id', async (req, res) => {
    try {
        const job = await reportQueueDCI.getJob(req.params.id);
        if (job) {
            let statusfix = 'Failed';  // Default status is Failed
            let errorMessage = '';
            let filenamezip = 'Not Available';

            console.log('test' + JSON.stringify(job.returnvalue));
            // If job is done and no error, status is Success
            if (job.returnvalue) {
                if (job.returnvalue.status === 'done') {
                    statusfix = 'Sukses';
                    filenamezip = job.returnvalue.zipFileName;  // If job is done, attach filename
                } else if (job.returnvalue.error) {
                    statusfix = 'Failed';
                    errorMessage = job.returnvalue.error;  // Include error message if exists
                }
            } else {
                if (job.isWaiting()) {
                    statusfix = 'Pending';  // Job is waiting in the queue
                } else {
                    statusfix = 'Failed';
                }
            }

            res.json({
                success: true,
                id: job.id,
                Origin: job.data.origin,
                Destination: job.data.destination,
                FromDate: job.data.froms,
                ToDate: job.data.thrus,
                UserID: job.data.user_id,
                TM: job.data.TM,
                Status: statusfix,  // Return statusfix (Sukses, Failed, or Pending)
                filenamezip: filenamezip,
                errorMessage: errorMessage // Include error message if exists
            });
        } else {
            res.status(404).send({success: false, message: 'Job not found.'});
        }
    } catch (err) {
        console.error('Error fetching job:', err);
        res.status(500).send({success: false, message: 'Error fetching job.'});
    }
});
app.get('/jobsdco/:id', async (req, res) => {
    try {
        const job = await reportQueueDCO.getJob(req.params.id);
        if (job) {
            let statusfix = 'Failed';  // Default status is Failed
            let errorMessage = '';
            let filenamezip = 'Not Available';

            console.log('test' + JSON.stringify(job.returnvalue));
            // If job is done and no error, status is Success
            if (job.returnvalue) {
                if (job.returnvalue.status === 'done') {
                    statusfix = 'Sukses';
                    filenamezip = job.returnvalue.zipFileName;  // If job is done, attach filename
                } else if (job.returnvalue.error) {
                    statusfix = 'Failed';
                    errorMessage = job.returnvalue.error;  // Include error message if exists
                }
            } else {
                if (job.isWaiting()) {
                    statusfix = 'Pending';  // Job is waiting in the queue
                } else {
                    statusfix = 'Failed';
                }
            }

            res.json({
                success: true,
                id: job.id,
                Origin: job.data.origin,
                Destination: job.data.destination,
                FromDate: job.data.froms,
                ToDate: job.data.thrus,
                UserID: job.data.user_id,
                service: job.data.service,
                Status: statusfix,  // Return statusfix (Sukses, Failed, or Pending)
                filenamezip: filenamezip,
                errorMessage: errorMessage // Include error message if exists
            });
        } else {
            res.status(404).send({success: false, message: 'Job not found.'});
        }
    } catch (err) {
        console.error('Error fetching job:', err);
        res.status(500).send({success: false, message: 'Error fetching job.'});
    }
});

app.use('/file_download', express.static(path.join(__dirname, 'file_download')));
app.get('/downloadtco/:jobId', async (req, res) => {
    const {jobId} = req.params;  // Ambil jobId dari parameter URL
    const category = 'TCO'; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {
        // Cari pekerjaan berdasarkan jobId di reportQueue
        const job = await reportQueue.getJob(jobId);

        if (!job) {
            return res.status(404).send({success: false, message: 'Job not found.'});
        }

        // Koneksi ke database untuk mencari nama file berdasarkan jobId dan category
        const connection = await oracledb.getConnection(config);
        const query = `
            SELECT NAME_FILE
            FROM CMS_COST_TRANSIT_V2_LOG
            WHERE ID_JOB_REDIS = :jobId
            AND CATEGORY = :category
        `;

        const result = await connection.execute(query, {
            jobId: jobId,
            category: category
        });

        if (result.rows.length === 0) {
            return res.status(404).send({success: false, message: 'File not found in the database.'});
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({success: false, message: 'File not found.'});
            }

            // Update status download ke 1 (unduhan selesai) dan status ke Done
            const updateQuery = `
                UPDATE CMS_COST_TRANSIT_V2_LOG
                SET DOWNLOAD = 1, STATUS = 'Download'
                WHERE ID_JOB_REDIS = :jobId
            `;
            await connection.execute(updateQuery, {
                jobId: jobId,
            });
            await connection.commit();

            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({success: false, message: 'Error downloading the file.'});
                }

                // After successful download, delete the file
                fs.unlink(filePath, (unlinkErr) => {
                    if (unlinkErr) {
                        console.error('Error deleting the file:', unlinkErr);
                    } else {
                        console.log(`File ${path.basename(filePath)} deleted after download.`);
                    }
                });
            });
        });
    } catch (err) {
        console.error('Error fetching job data or handling download:', err);
        res.status(500).send({success: false, message: 'An error occurred while processing the download.'});
    }
});
app.get('/downloadtci/:jobId', async (req, res) => {
    const {jobId} = req.params;  // Ambil jobId dari parameter URL
    const category = 'TCI'; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {
        // Cari pekerjaan berdasarkan jobId di reportQueue
        const job = await reportQueueTCI.getJob(jobId);

        if (!job) {
            return res.status(404).send({success: false, message: 'Job not found.'});
        }

        // Koneksi ke database untuk mencari nama file berdasarkan jobId dan category
        const connection = await oracledb.getConnection(config);
        const query = `
            SELECT NAME_FILE
            FROM CMS_COST_TRANSIT_V2_LOG
            WHERE ID_JOB_REDIS = :jobId
            AND CATEGORY = :category
        `;

        const result = await connection.execute(query, {
            jobId: jobId,
            category: category
        });

        if (result.rows.length === 0) {
            return res.status(404).send({success: false, message: 'File not found in the database.'});
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({success: false, message: 'File not found.'});
            }

            // Update status download ke 1 (unduhan selesai) dan status ke Done
            const updateQuery = `
                UPDATE CMS_COST_TRANSIT_V2_LOG
                SET DOWNLOAD = 1, STATUS = 'Download'
                WHERE ID_JOB_REDIS = :jobId
            `;
            await connection.execute(updateQuery, {
                jobId: jobId,
            });
            await connection.commit();

            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({success: false, message: 'Error downloading the file.'});
                }

                // After successful download, delete the file
                fs.unlink(filePath, (unlinkErr) => {
                    if (unlinkErr) {
                        console.error('Error deleting the file:', unlinkErr);
                    } else {
                        console.log(`File ${path.basename(filePath)} deleted after download.`);
                    }
                });
            });
        });
    } catch (err) {
        console.error('Error fetching job data or handling download:', err);
        res.status(500).send({success: false, message: 'An error occurred while processing the download.'});
    }
});

app.get('/downloaddci/:jobId', async (req, res) => {
    const {jobId} = req.params;  // Ambil jobId dari parameter URL
    const category = 'DCI'; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {
        // Cari pekerjaan berdasarkan jobId di reportQueue
        const job = await reportQueueDCI.getJob(jobId);

        if (!job) {
            return res.status(404).send({success: false, message: 'Job not found.'});
        }

        // Koneksi ke database untuk mencari nama file berdasarkan jobId dan category
        const connection = await oracledb.getConnection(config);
        const query = `
            SELECT NAME_FILE
            FROM CMS_COST_TRANSIT_V2_LOG
            WHERE ID_JOB_REDIS = :jobId
            AND CATEGORY = :category
        `;

        const result = await connection.execute(query, {
            jobId: jobId,
            category: category
        });

        if (result.rows.length === 0) {
            return res.status(404).send({success: false, message: 'File not found in the database.'});
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({success: false, message: 'File not found.'});
            }

            // Update status download ke 1 (unduhan selesai) dan status ke Done
            const updateQuery = `
                UPDATE CMS_COST_TRANSIT_V2_LOG
                SET DOWNLOAD = 1, STATUS = 'Download'
                WHERE ID_JOB_REDIS = :jobId
            `;
            await connection.execute(updateQuery, {
                jobId: jobId,
            });
            await connection.commit();

            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({success: false, message: 'Error downloading the file.'});
                }

                // After successful download, delete the file
                fs.unlink(filePath, (unlinkErr) => {
                    if (unlinkErr) {
                        console.error('Error deleting the file:', unlinkErr);
                    } else {
                        console.log(`File ${path.basename(filePath)} deleted after download.`);
                    }
                });
            });
        });
    } catch (err) {
        console.error('Error fetching job data or handling download:', err);
        res.status(500).send({success: false, message: 'An error occurred while processing the download.'});
    }
});
app.get('/downloaddco/:jobId', async (req, res) => {
    const {jobId} = req.params;  // Ambil jobId dari parameter URL
    const category = 'DCO'; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {
        // Cari pekerjaan berdasarkan jobId di reportQueue
        const job = await reportQueueDCO.getJob(jobId);

        if (!job) {
            return res.status(404).send({success: false, message: 'Job not found.'});
        }

        // Koneksi ke database untuk mencari nama file berdasarkan jobId dan category
        const connection = await oracledb.getConnection(config);
        const query = `
            SELECT NAME_FILE
            FROM CMS_COST_TRANSIT_V2_LOG
            WHERE ID_JOB_REDIS = :jobId
            AND CATEGORY = :category
        `;

        const result = await connection.execute(query, {
            jobId: jobId,
            category: category
        });

        if (result.rows.length === 0) {
            return res.status(404).send({success: false, message: 'File not found in the database.'});
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({success: false, message: 'File not found.'});
            }

            // Update status download ke 1 (unduhan selesai) dan status ke Done
            const updateQuery = `
                UPDATE CMS_COST_TRANSIT_V2_LOG
                SET DOWNLOAD = 1, STATUS = 'Download'
                WHERE ID_JOB_REDIS = :jobId
            `;
            await connection.execute(updateQuery, {
                jobId: jobId,
            });
            await connection.commit();

            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({success: false, message: 'Error downloading the file.'});
                }

                // After successful download, delete the file
                fs.unlink(filePath, (unlinkErr) => {
                    if (unlinkErr) {
                        console.error('Error deleting the file:', unlinkErr);
                    } else {
                        console.log(`File ${path.basename(filePath)} deleted after download.`);
                    }
                });
            });
        });
    } catch (err) {
        console.error('Error fetching job data or handling download:', err);
        res.status(500).send({success: false, message: 'An error occurred while processing the download.'});
    }
});

app.use(express.static('public'));
// Serve the progresstci.html when visiting the /progress URL
app.get('/swaggertco', (req, res) => {
    res.sendFile(__dirname + '/public/progresstco.html');
});
app.get('/progresstci', (req, res) => {
    res.sendFile(__dirname + '/public/progresstci.html');
});
// Serve the progresstci.html when visiting the /progress URL
app.get('/progressdci', (req, res) => {
    res.sendFile(__dirname + '/public/progressdci.html');
});

// Start the server
app.listen(port, () => {
    console.log(`Server running at http://0.0.0.0:${port}`);
});


// }