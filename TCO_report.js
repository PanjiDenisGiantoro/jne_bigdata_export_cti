const express = require('express');
const oracledb = require('oracledb');  // Import oracledb untuk koneksi ke Oracle
const path = require('path');  // Untuk memanipulasi path direktori
const Bull = require('bull'); // Import Bull untuk job queue
const {setQueues, BullAdapter} = require('bull-board');
const app = express();
const port = 3010;  // Port API
const Sentry = require("@sentry/node");
const {nodeProfilingIntegration} = require("@sentry/profiling-node");
const cluster = require('cluster');
const os = require('os');
const Redis = require('ioredis');
const redis = new Redis(); // Koneksi ke Redis server
const JOB_LOCK_KEY = 'job_lock';
const ProgressBar = require('progress');
const {format} = require('date-fns');  // Import the format function from date-fns
const moment = require('moment');  // Import moment.js
const { v4: uuidv4 } = require('uuid');
const XlsxStreamReader = require('xlsx-stream-reader'); // Menggunakan xlsx-stream-reader untuk membaca Excel
const ExcelJS = require('exceljs');    // Import exceljs untuk ekspor ke Excel
const archiver = require('archiver');  // Import archiver untuk zip file
const fs = require('fs');  // Untuk menulis file ke sistem
const { pipeline } = require('stream/promises');
const fsPromises = fs.promises;  // Ini bikin error karena dipakai sebelum inisialisasi

// require('dotenv').config();
// Middleware to parse JSON bodies
app.use(express.json());
// const numCPUs = os.cpus().length;

// Koneksi ke database Oracle
const config = {
    user: 'dbctc_v2',
    password: 'dbctc123',
    connectString: '10.8.2.48:1521/ctcv2db'  // Host, port, dan service name
};

const config_jnebill = {
    user: 'JNEBILL',
    password: 'JNE98292092B5494083OK',
    connectString: '10.8.2.219:1521/JNEBILL'  // Host, port, dan service name
};
const config_jnebilltraining = {
    user: 'JNEBILL',
    password: 'JNEBILL',
    connectString: '10.8.2.19:1522/JNEBILL'  // Host, port, dan service name
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
    removeOnComplete: true // Job selesai langsung dihapus dari Redis

});

const reportQueueCA = new Bull('reportQueueCA', {
    redis: {host: '127.0.0.1', port: 6379},
    removeOnComplete: true // Job selesai langsung dihapus dari Redis

});

const reportQueueRU = new Bull('reportQueueRU', {
    redis: {host: '127.0.0.1', port: 6379},
    removeOnComplete: true // Job selesai langsung dihapus dari Redis

});
const reportQueueDBO = new Bull('reportQueueDBO', { //detail biaya operasional
    redis: {host: '127.0.0.1', port: 6379},
    removeOnComplete: true // Job selesai langsung dihapus dari Redis
});
const reportQueueDBONA = new Bull('reportQueueDBONA', { //detail biaya operasional
    redis: {host: '127.0.0.1', port: 6379},
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
    // For example, automatic IP address collection on events
    sendDefaultPii: true,
});
setQueues([new BullAdapter(reportQueue)]);
setQueues([new BullAdapter(reportQueueTCI)]);
setQueues([new BullAdapter(reportQueueDCI)]);
setQueues([new BullAdapter(reportQueueDCO)]);
setQueues([new BullAdapter(reportQueueCA)]);
setQueues([new BullAdapter(reportQueueRU)]);
setQueues([new BullAdapter(reportQueueDBO)]);
setQueues([new BullAdapter(reportQueueDBONA)]);

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

function logErrorToFileCA(jobId, branch_id, userId, errorMessage) {
    const logFilePath = path.join(__dirname, 'error_logs.txt');
    const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId}  | branch_id: ${branch_id}  | Error: ${errorMessage}\n`;

    fs.appendFile(logFilePath, logMessage, (err) => {
        if (err) {
            console.error('Error writing to log file:', err);
        }
    });
}
function logErrorToFileRU(jobId, origin_awal,destination,services_code, userId, errorMessage) {
    const logFilePath = path.join(__dirname, 'error_logs.txt');
    const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId} | origin: ${origin_awal} | Destination: ${destination} | service_code : ${services_code} | UserID: ${userId} | Error: ${errorMessage}\n`;

    fs.appendFile(logFilePath, logMessage, (err) => {
        if (err) {
            console.error('Error writing to log file:', err);
        }
    });
}
function logErrorToFileDBO(jobId, branch_id,currency,services_code, userId, errorMessage) {
    const logFilePath = path.join(__dirname, 'error_logs.txt');
    const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId} | origin: ${branch_id} | Destination: ${currency} | service_code : ${services_code} | UserID: ${userId} | Error: ${errorMessage}\n`;

    fs.appendFile(logFilePath, logMessage, (err) => {
        if (err) {
            console.error('Error writing to log file:', err);
        }
    });
}
function logErrorToFileDBONA(jobId, branch_id,currency,services_code, userId, errorMessage) {
    const logFilePath = path.join(__dirname, 'error_logs.txt');
    const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId} | origin: ${branch_id} | Destination: ${currency} | service_code : ${services_code} | UserID: ${userId} | Error: ${errorMessage}\n`;

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
        console.log('Processing Job ID:', job.id, 'with queue:', job.queue.name);

        const isJobRunning = await redis.get('currentJobStatus' + jobId);
        if (isJobRunning === 'running') {
            console.log(`Job ID: ${jobId} is already running. Skipping...` + job.data);
            // Jika job sedang berjalan, simpan job di queue pending
            await redis.lpush('pending_jobs', JSON.stringify(job.data));  // Simpan job ke antrian
            return;
        }
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
        } else if (job.queue.name === 'reportQueueCA') {
            fetchDataAndExportToExcelFunc = fetchDataAndExportToExcelCA;
        }else if (job.queue.name === 'reportQueueRU') {
            fetchDataAndExportToExcelFunc = fetchDataAndExportToExcelRU;
        }else if (job.queue.name === 'reportQueueDBO') {
            fetchDataAndExportToExcelFunc = fetchDataAndExportToExcelDBO;
        }else if (job.queue.name === 'reportQueueDBONA') {
            fetchDataAndExportToExcelFunc = fetchDataAndExportToExcelDBONA;
        }
        if (!fetchDataAndExportToExcelFunc) {
            console.error('Unknown queue name:', job.queue.name);
            return;
        }

        if (job.queue.name === 'reportQueue') {
            await Sentry.startSpan({name: 'Process Job' + job.id, jobId: job.id}, async (span) => {
                const {origin, destination, froms, thrus, user_id, dateStr, jobId} = job.data;

                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {

                    const estimatedDataCount = await estimateDataCount({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id
                    });

                    const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
                    const estimatedTimeMinutes =
                        (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes

                    const today = new Date();
                    const dateStr = today.toISOString().split("T")[0];
                    const count_per_file = Math.ceil(estimatedDataCount / 50000);

                    const connections = await oracledb.getConnection(config);
                    const estimateQuery = `
                        UPDATE CMS_COST_TRANSIT_V2_LOG
                        SET
                            START_PROCESS = SYSDATE,
                            DURATION   = :duration,
                            DATACOUNT  = :datacount,
                            TOTAL_FILE = :total_file
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'TCO'
                    `;
                    const estimateValues = {
                        duration: estimatedTimeMinutes, // Add the duration
                        datacount: estimatedDataCount,        // Add the data count
                        total_file: count_per_file, // Add the total file count
                        jobId: job.id  // The job ID that we are processing
                    };
                    await connections.execute(estimateQuery, estimateValues);
                    await connections.commit();
                    console.log(`estimate data : ${job.id}`);


                    // Capture the start time
                    const startTime = Date.now();

                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcel({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id,
                        dateStr,
                        jobId: job.id
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
                        SET DOWNLOAD   = 0,
                            STATUS     = 'Zipped',
                            NAME_FILE  = :filename,
                            UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM'),
                            TRANSIT_V2_LOG_FLAG_DELETE = 'N'
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'TCO'
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
        } else if (job.queue.name === 'reportQueueTCI') {
            await Sentry.startSpan({name: 'Process Report TCI Job' + job.id, jobId: job.id}, async (span) => {
                const {origin, destination, froms, thrus, user_id, TM, user_session, dateStr, jobId} = job.data;
                console.log('Processing job with data:', job.data);

                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();

                    const estimatedDataCount = await estimateDataCountTCI({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id,
                        TM,
                    });

                    // Calculate the estimated time based on the benchmark
                    const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
                    const estimatedTimeMinutes = Math.round(
                        (estimatedDataCount / benchmarkRecordsPerMinute) * 2
                    ); // Estimated time in minutes (rounded)

                    const today = new Date();
                    const dateStr = today.toISOString().split("T")[0];
                    const count_per_file = Math.ceil(estimatedDataCount / 50000);

                    const connections = await oracledb.getConnection(config);
                    const estimateQuery = `
                        UPDATE CMS_COST_TRANSIT_V2_LOG
                        SET
                            START_PROCESS = SYSDATE,
                            DURATION   = :duration,
                            DATACOUNT  = :datacount,
                            TOTAL_FILE = :total_file
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'TCI'
                    `;
                    const estimateValues = {
                        duration: estimatedTimeMinutes, // Add the duration
                        datacount: estimatedDataCount,        // Add the data count
                        total_file: count_per_file, // Add the total file count
                        jobId: job.id  // The job ID that we are processing
                    };
                    await connections.execute(estimateQuery, estimateValues);
                    await connections.commit();
                    console.log(`estimate data : ${job.id}`);



                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcelTCI({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id,
                        TM,
                        user_session,
                        dateStr,
                        jobId: job.id
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
                        SET DOWNLOAD   = 0,
                            STATUS     = 'Zipped',
                            NAME_FILE  = :filename,
                            UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM'),
                            TRANSIT_V2_LOG_FLAG_DELETE = 'N'
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'TCI'
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
        } else if (job.queue.name === 'reportQueueDCI') {
            await Sentry.startSpan({name: 'Process Report DCI Job' + job.id, jobId: job.id}, async (span) => {
                const {origin, destination, froms, thrus, user_id, service, dateStr,jobId} = job.data;
                console.log('Processing job with data:', job.data);

                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();

                    const estimatedDataCount = await estimateDataCountDCI({
                        origin,
                        destination,
                        froms,
                        thrus,
                        service,
                        user_id,
                    });

                    // Calculate the estimated time based on the benchmark
                    const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
                    const estimatedTimeMinutes =
                        (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes
                    const today = new Date();
                    const dateStr = today.toISOString().split("T")[0];
                    const count_per_file = Math.ceil(estimatedDataCount / 50000);

                    const connections = await oracledb.getConnection(config);
                    const estimateQuery = `
                        UPDATE CMS_COST_TRANSIT_V2_LOG
                        SET
                            START_PROCESS = SYSDATE,
                            DURATION   = :duration,
                            DATACOUNT  = :datacount,
                            TOTAL_FILE = :total_file
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DCI'
                    `;
                    const estimateValues = {
                        duration: estimatedTimeMinutes, // Add the duration
                        datacount: estimatedDataCount,        // Add the data count
                        total_file: count_per_file, // Add the total file count
                        jobId: job.id  // The job ID that we are processing
                    };
                    await connections.execute(estimateQuery, estimateValues);
                    await connections.commit();
                    console.log(`estimate data : ${job.id}`);





                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcelDCI({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id,
                        service,
                        dateStr,
                        jobId: job.id
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
                        SET DOWNLOAD   = 0,
                            STATUS     = 'Zipped',
                            NAME_FILE  = :filename,
                            UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM'),
                            TRANSIT_V2_LOG_FLAG_DELETE = 'N'
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DCI'
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
        } else if (job.queue.name === 'reportQueueDCO') {
            await Sentry.startSpan({name: 'Process Report DCO Job' + job.id, jobId: job.id}, async (span) => {
                const {origin, destination, froms, thrus, service, user_id, dateStr, jobId} = job.data;
                console.log('Processing job with data:', job.data);

                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();



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
                    const estimatedTimeMinutes =
                        (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes

                    const today = new Date();
                    const dateStr = today.toISOString().split("T")[0];
                    const count_per_file = Math.ceil(estimatedDataCount / 50000);

                    const connections = await oracledb.getConnection(config);
                    const estimateQuery = `
                        UPDATE CMS_COST_TRANSIT_V2_LOG
                        SET
                            START_PROCESS = SYSDATE,
                            DURATION   = :duration,
                            DATACOUNT  = :datacount,
                            TOTAL_FILE = :total_file
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DCO'
                    `;
                    const estimateValues = {
                        duration: estimatedTimeMinutes, // Add the duration
                        datacount: estimatedDataCount,        // Add the data count
                        total_file: count_per_file, // Add the total file count
                        jobId: job.id  // The job ID that we are processing
                    };
                    await connections.execute(estimateQuery, estimateValues);
                    await connections.commit();
                    console.log(`estimate data : ${job.id}`);



                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcelDCO({
                        origin,
                        destination,
                        froms,
                        thrus,
                        user_id,
                        service,
                        dateStr,
                        jobId: job.id
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
                        SET DOWNLOAD   = 0,
                            STATUS     = 'Zipped',
                            NAME_FILE  = :filename,
                            UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM'),
                            TRANSIT_V2_LOG_FLAG_DELETE = 'N'
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DCO'
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
        }else if (job.queue.name === 'reportQueueCA') {
            await Sentry.startSpan({name: 'Process Report CA Job' + job.id, jobId: job.id}, async (span) => {
                const {branch, froms, thrus, user_id, dateStr, jobId} = job.data;
                console.log('Processing job with data:', job.data);

                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();


                    // Estimasi jumlah data
                    const estimatedDataCount = await estimateDataCountCA({
                        branch,
                        froms,
                        thrus,
                        user_id
                    });

                    console.log('tes')
                    // Calculate the estimated time based on the benchmark
                    const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
                    const estimatedTimeMinutes =
                        (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes

                    const today = new Date();
                    const dateStr = today.toISOString().split("T")[0];
                    const count_per_file = Math.ceil(estimatedDataCount / 50000);

                    const connections = await oracledb.getConnection(config);
                    const estimateQuery = `
            UPDATE CMS_COST_TRANSIT_V2_LOG
            SET 
                START_PROCESS = SYSDATE,
                DURATION   = :duration,
                DATACOUNT  = :datacount,
                TOTAL_FILE = :total_file
            WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'CA'
        `;
                    const estimateValues = {
                        duration: estimatedTimeMinutes, // Add the duration
                        datacount: estimatedDataCount,        // Add the data count
                        total_file: count_per_file, // Add the total file count
                        jobId: job.id  // The job ID that we are processing
                    };
                    await connections.execute(estimateQuery, estimateValues);
                    await connections.commit();
                    console.log(`estimate data : ${job.id}`);


                    if (branch === 'BTH000') {
                        zipFileName = await fetchDataAndExportToExcelCABTM({
                            branch,
                            froms,
                            thrus,
                            user_id,
                            dateStr,
                            jobId: job.id
                        }).then((result) => {
                            dataCount = result.dataCount; // Assuming the fetchDataAndExportToExcel function returns data count
                            return result.zipFileName;
                        });
                    }else{
                        zipFileName = await fetchDataAndExportToExcelCA({
                            branch,
                            froms,
                            thrus,
                            user_id,
                            dateStr,
                            jobId: job.id
                        }).then((result) => {
                            dataCount = result.dataCount; // Assuming the fetchDataAndExportToExcel function returns data count
                            return result.zipFileName;
                        });
                    }


                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan


                    // Capture the completion time after the job is done
                    const endTime = Date.now();
                    completionTime = new Date(endTime).toISOString(); // Convert to ISO string for consistency
                    const formattedDate = moment().format('MM/DD/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                    // Calculate the elapsed time in minutes
                    elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes

                    const connection = await oracledb.getConnection(config);
                    const updateQuery = `
                        UPDATE CMS_COST_TRANSIT_V2_LOG
                        SET DOWNLOAD   = 0,
                            STATUS     = 'Zipped',
                            NAME_FILE  = :filename,
                            UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM'),
                            TRANSIT_V2_LOG_FLAG_DELETE = 'N'
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'CA'
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
                        logErrorToFileCA(job.id, origin, destination, service, user_id, error.message);

                    });
                    Sentry.captureException(error);

                    return {
                        status: 'failed',
                        error: error.message
                    };
                }
            });
        } else if (job.queue.name === 'reportQueueRU') {
            await Sentry.startSpan({name: 'Process Report RU Job' + job.id, jobId: job.id}, async (span) => {
                const {origin_awal,destination, services_code, froms, thrus, user_id, dateStr, jobId} = job.data;
                console.log('Processing job with data:', job.data);
                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();


                    // Estimasi jumlah data
                    const estimatedDataCount = await estimateDataCountRU({
                        origin_awal,
                        destination,
                        services_code,
                        froms,
                        thrus,
                        user_id
                    });

                    // Calculate the estimated time based on the benchmark
                    const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
                    const estimatedTimeMinutes =
                        (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes
                    const today = new Date();
                    const dateStr = today.toISOString().split("T")[0];
                    const count_per_file = Math.ceil(estimatedDataCount / 50000);

                    const connections = await oracledb.getConnection(config);
                    const estimateQuery = `
            UPDATE CMS_COST_TRANSIT_V2_LOG
            SET 
                START_PROCESS = SYSDATE,
                DURATION   = :duration,
                DATACOUNT  = :datacount,
                TOTAL_FILE = :total_file
            WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'RU'
        `;
                    const estimateValues = {
                        duration: estimatedTimeMinutes, // Add the duration
                        datacount: estimatedDataCount,        // Add the data count
                        total_file: count_per_file, // Add the total file count
                        jobId: job.id  // The job ID that we are processing
                    };
                    await connections.execute(estimateQuery, estimateValues);
                    await connections.commit();
                    console.log(`estimate data : ${job.id}`);



                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcelRU({
                        origin_awal,
                        destination,
                        services_code,
                        froms,
                        thrus,
                        user_id,
                        dateStr,
                        jobId: job.id
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
                        SET DOWNLOAD   = 0,
                            STATUS     = 'Zipped',
                            NAME_FILE  = :filename,
                            UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM'),
                            TRANSIT_V2_LOG_FLAG_DELETE = 'N'
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'RU'
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
                        logErrorToFileRU(job.id, origin_awal, destination, services_code, user_id, error.message);

                    });
                    Sentry.captureException(error);

                    return {
                        status: 'failed',
                        error: error.message
                    };
                }
            });
        }else if (job.queue.name === 'reportQueueDBO') {
            await Sentry.startSpan({name: 'Process Report DBO Job' + job.id, jobId: job.id}, async (span) => {
                const { branch_id,currency,services_code, froms, thrus, user_id, dateStr, jobId} = job.data;
                console.log('Processing job with data:', job.data);
                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();


                    // Default, pakai fungsi estimateDataCountDBO
                    const estimatedDataCount = await estimateDataCountDBO({
                        branch_id,
                        currency,
                        services_code,
                        froms,
                        thrus,
                        user_id
                    });

                    // Calculate the estimated time based on the benchmark
                    const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
                    const estimatedTimeMinutes =
                        (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes

                    const today = new Date();
                    const dateStr = today.toISOString().split("T")[0];
                    const count_per_file = Math.ceil(estimatedDataCount / 50000);

                    const connections = await oracledb.getConnection(config);
                    const estimateQuery = `
            UPDATE CMS_COST_TRANSIT_V2_LOG
            SET 
                START_PROCESS = SYSDATE,
                DURATION   = :duration,
                DATACOUNT  = :datacount,
                TOTAL_FILE = :total_file
            WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DBO'
        `;
                    const estimateValues = {
                        duration: estimatedTimeMinutes, // Add the duration
                        datacount: estimatedDataCount,        // Add the data count
                        total_file: count_per_file, // Add the total file count
                        jobId: job.id  // The job ID that we are processing
                    };
                    await connections.execute(estimateQuery, estimateValues);
                    await connections.commit();
                    console.log(`estimate data : ${job.id}`);



                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcelDBO({
                        branch_id,
                        currency,
                        services_code,
                        froms,
                        thrus,
                        user_id,
                        dateStr,
                        jobId: job.id
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
                        SET DOWNLOAD   = 0,
                            STATUS     = 'Zipped',
                            NAME_FILE  = :filename,
                            UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM'),
                            TRANSIT_V2_LOG_FLAG_DELETE = 'N'
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DBO'
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
                        logErrorToFileDBO(job.id,  branch_id, currency,services_code, user_id, error.message);

                    });
                    Sentry.captureException(error);

                    return {
                        status: 'failed',
                        error: error.message
                    };
                }
            });
        }else if (job.queue.name === 'reportQueueDBONA') {
            await Sentry.startSpan({name: 'Process Report DBONA Job' + job.id, jobId: job.id}, async (span) => {
                const { branch_id,currency,services_code, froms, thrus, user_id, dateStr, jobId} = job.data;
                console.log('Processing job with data:', job.data);
                let zipFileName = '';
                let completionTime = '';
                let dataCount = 0;  // Variable to store the number of records processed
                let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                try {
                    // Capture the start time
                    const startTime = Date.now();

                    const  estimatedDataCount = await estimateDataCountDBONA({
                        branch_id,
                        currency,
                        services_code,
                        froms,
                        thrus,
                        user_id
                    });


                    // Calculate the estimated time based on the benchmark
                    const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
                    const estimatedTimeMinutes =
                        (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes

                    const today = new Date();
                    const dateStr = today.toISOString().split("T")[0];
                    const count_per_file = Math.ceil(estimatedDataCount / 50000);

                    const connections = await oracledb.getConnection(config);
                    const estimateQuery = `
            UPDATE CMS_COST_TRANSIT_V2_LOG
            SET 
                START_PROCESS = SYSDATE,
                DURATION   = :duration,
                DATACOUNT  = :datacount,
                TOTAL_FILE = :total_file
            WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'TCO'
        `;
                    const estimateValues = {
                        duration: estimatedTimeMinutes, // Add the duration
                        datacount: estimatedDataCount,        // Add the data count
                        total_file: count_per_file, // Add the total file count
                        jobId: job.id  // The job ID that we are processing
                    };
                    await connections.execute(estimateQuery, estimateValues);
                    await connections.commit();
                    console.log(`estimate data : ${job.id}`);


                    // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                    zipFileName = await fetchDataAndExportToExcelDBONA({
                        branch_id,
                        currency,
                        services_code,
                        froms,
                        thrus,
                        user_id,
                        dateStr,
                        jobId: job.id
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
                        SET DOWNLOAD   = 0,
                            STATUS     = 'Zipped',
                            NAME_FILE  = :filename,
                            UPDATED_AT = TO_TIMESTAMP(:updated_at, 'MM/DD/YYYY HH:MI:SS AM'),
                            TRANSIT_V2_LOG_FLAG_DELETE = 'N'
                        WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DBONA'
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
                        logErrorToFileDBONA(job.id,  branch_id, currency,services_code, user_id, error.message);

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
    console.log('Processing next pending jobs...' + pendingJob);

    if (pendingJob) {
        const jobData = JSON.parse(pendingJob);
        console.log('Processing next pending job...' + pendingJob);
        // Menambahkan job ke dalam queue yang sesuai berdasarkan job.queue.name
        if (jobData.queue.name === 'reportQueue') {
            await reportQueue.add(jobData);  // Menambahkan job ke reportQueue
            console.log('Job added to reportQueue');
        } else if (jobData.queue.name === 'reportQueueTCI') {
            await reportQueueTCI.add(jobData);  // Menambahkan job ke reportQueueTCI
            console.log('Job added to reportQueueTCI');
        } else if (jobData.queue.name === 'reportQueueDCI') {
            await reportQueueDCI.add(jobData);  // Menambahkan job ke reportQueueDCI
            console.log('Job added to reportQueueDCI');
        } else if (jobData.queue.name === 'reportQueueDCO') {
            await reportQueueDCO.add(jobData);  // Menambahkan job ke reportQueueDCO
            console.log('Job added to reportQueueDCO');
        } else if (jobData.queue.name === 'reportQueueCA') {
            await reportQueueCA.add(jobData);  // Menambahkan job ke reportQueueDCO
            console.log('Job added to reportQueueCA');
        } else if (jobData.queue.name === 'reportQueueRU') {
            await reportQueueRU.add(jobData);  // Menambahkan job ke reportQueueDCO
            console.log('Job added to reportQueueRU');
        } else if (jobData.queue.name === 'reportQueueDBO') {
            await reportQueueDBO.add(jobData);  // Menambahkan job ke reportQueueDCO
            console.log('Job added to reportQueueDBO');
        } else if (jobData.queue.name === 'reportQueueDBONA') {
            await reportQueueDBO.add(jobData);  // Menambahkan job ke reportQueueDCO
            console.log('Job added to reportQueueDBONA');
        }else {
            console.log(`Unknown queue name: ${jobData.queue.name}`);
        }
    } else {
        console.log('No pending jobs.');
    }
};


// Tentukan bagaimana job akan diproses dalam queue
reportQueue.process(async (job) => processJob(job));
reportQueueTCI.process(async (job) => processJob(job));
reportQueueDCI.process(async (job) => processJob(job));
reportQueueDCO.process(async (job) => processJob(job));
reportQueueCA.process(async (job) => processJob(job));
reportQueueRU.process(async (job) => processJob(job));
reportQueueDBO.process(async (job) => processJob(job));
reportQueueDBONA.process(async (job) => processJob(job));

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
                        whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`;                        bindParams.origin = origin + '%';
                    }
                    if (destination !== '0') {
                        whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`;                        bindParams.destination = destination + '%';
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }


                    // Query untuk estimasi jumlah data
                    connection.execute(`
                        SELECT COUNT(*) as DATA_COUNT FROM (
                                                               SELECT
                                                                   ROWNUM, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                                                                   BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                                                                   ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                                                                   SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                                                               FROM CMS_COST_TRANSIT_V2
                                                                        ${whereClause}
                                                                   AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                                                                 AND CNOTE_WEIGHT > 0
                                                               GROUP BY
                                                                   ROWNUM, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                                                                   BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                                                                   ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                                                                   SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                                                           )
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
                        SELECT COUNT(*) as DATA_COUNT FROM (
                                                               SELECT
                                                                   ROWNUM, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                                                                   BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                                                                   ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                                                                   SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                                                               FROM CMS_COST_TRANSIT_V2
                                                                        ${whereClause}
                                                                   AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                                                                 AND CNOTE_WEIGHT > 0
                                                               GROUP BY
                                                                   ROWNUM, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                                                                   BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                                                                   ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                                                                   SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                                                           )
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
                        FROM CMS_COST_DELIVERY_V2 ${whereClause} AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
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
                        FROM CMS_COST_DELIVERY_V2 ${whereClause} AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
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

async function estimateDataCountCA_({branch, froms, thrus, user_id}) {
    return new Promise((resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config_jnebill, (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;
                    let whereClause = "WHERE 1 = 1";
                    const bindParams = {};

                    if (branch !== '0') {
                        whereClause += ` AND C.CNOTE_BRANCH_ID = :branch`;
                        bindParams.branch = branch ;
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(C.CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-RRRR') AND TO_DATE(:thrus, 'DD-MON-RRRR')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }


                    // Query untuk estimasi jumlah data
                    connection.execute(`
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM CMS_APICUST_HYBRID A
                                 JOIN CMS_CUST B ON A.HYBRID_CUST = B.CUST_ID AND A.HYBRID_BRANCH = B.CUST_BRANCH
                                 JOIN CMS_CNOTE@DBS2 C ON A.APICUST_CNOTE_NO = C.CNOTE_NO
                                 JOIN CMS_CUST D ON D.CUST_BRANCH = A.HYBRID_BRANCH AND D.CUST_ID = A.APICUST_CUST_NO

                            ${whereClause}
                          --  C.CNOTE_BRANCH_ID = :P_BRANCH
                       --   AND TRUNC(C.CNOTE_DATE) BETWEEN TO_DATE(:P_DATE1, 'DD-MON-RRRR') AND TO_DATE(:P_DATE2, 'DD-MON-RRRR')
                          AND B.CUST_TYPE IN ('995','996','997','994')
                          AND NVL(
                                      (SELECT CUST_KP
                                       FROM ECONNOTE_CUST E
                                       WHERE E.CUST_BRANCH = C.CNOTE_BRANCH_ID
                                         AND B.CUST_ID = E.CUST_ID
                                         AND CUST_KP = 'N'),
                                      'N'
                              ) = 'N'
                          AND NVL(C.CNOTE_CANCEL, 'N') = 'N'
                            AND HYBRID_CUST=B.CUST_ID
                            AND HYBRID_BRANCH=B.CUST_BRANCH
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

async function estimateDataCountCA({branch, froms, thrus, user_id}) {
    return new Promise((resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config_jnebill, (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                    return;
                }
                connection = conn;
                let query = '';
                const bindParams = {};
                let whereClause = "WHERE 1 = 1";

                if (branch === 'BTH000') {
                    // Kondisi dinamis untuk branch dan tanggal
                    if (branch !== '0') {
                        whereClause += ` AND C.CNOTE_BRANCH_ID = :branch`;
                        bindParams.branch = branch;
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(C.CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-RRRR') AND TO_DATE(:thrus, 'DD-MON-RRRR')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }

                    query = `
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM (
                                 SELECT 1
                                 FROM
                                     CMS_APICUST_HYBRID A
                                         JOIN CMS_CUST B ON HYBRID_CUST = B.CUST_ID AND HYBRID_BRANCH = B.CUST_BRANCH
                                         JOIN CMS_CNOTE@DBS101 C ON APICUST_CNOTE_NO = CNOTE_NO
                                         LEFT JOIN (
                                         SELECT
                                             CUST_BRANCH AS MARKETPLACE_BRANCH,
                                             CUST_ID AS MARKETPLACE_ID,
                                             CUST_NAME AS MARKETPLACE_NAME
                                         FROM CMS_CUST
                                         WHERE CUST_TYPE NOT IN ('995','996','997','994')
                                     ) D ON MARKETPLACE_BRANCH = HYBRID_BRANCH AND MARKETPLACE_ID = APICUST_CUST_NO
                                         LEFT JOIN (
                                         SELECT
                                             CNOTE_NO AS HAWB
                                         FROM REPJNE.CMS_CNOTE_CN23_HYBRID
                                     ) E ON CNOTE_NO = HAWB
                                     ${whereClause}
                            AND CUST_TYPE IN ('995','996','997','994')
                            AND NVL((SELECT CUST_KP FROM REPJNE.ECONNOTE_CUST E2 WHERE CUST_BRANCH = C.CNOTE_BRANCH_ID AND B.CUST_ID = E2.CUST_ID AND CUST_KP = 'N'), 'N') = 'N'
                            AND NVL(C.CNOTE_CANCEL, 'N') = 'N'
                             )
                    `;
                } else {
                    // Query default jika branch selain 'BTH000'
                    if (branch !== '0') {
                        whereClause += ` AND C.CNOTE_BRANCH_ID = :branch`;
                        bindParams.branch = branch;
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(C.CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-RRRR') AND TO_DATE(:thrus, 'DD-MON-RRRR')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }

                    query = `
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM CMS_APICUST_HYBRID A
                                 JOIN CMS_CUST B ON A.HYBRID_CUST = B.CUST_ID AND A.HYBRID_BRANCH = B.CUST_BRANCH
                                 JOIN CMS_CNOTE@DBS2 C ON A.APICUST_CNOTE_NO = C.CNOTE_NO
                                 JOIN CMS_CUST D ON D.CUST_BRANCH = A.HYBRID_BRANCH AND D.CUST_ID = A.APICUST_CUST_NO
                            ${whereClause}
                        AND B.CUST_TYPE IN ('995','996','997','994')
                        AND NVL(
                            (SELECT CUST_KP
                            FROM ECONNOTE_CUST E
                            WHERE E.CUST_BRANCH = C.CNOTE_BRANCH_ID
                                AND B.CUST_ID = E.CUST_ID
                                AND CUST_KP = 'N'),
                            'N'
                        ) = 'N'
                        AND NVL(C.CNOTE_CANCEL, 'N') = 'N'
                        AND HYBRID_CUST = B.CUST_ID
                        AND HYBRID_BRANCH = B.CUST_BRANCH
                    `;
                }

                connection.execute(query, bindParams, (err, result) => {
                    if (err) {
                        reject('Error executing query: ' + err.message);
                    } else {
                        const count = result.rows.length > 0 ? result.rows[0][0] : 0;
                        resolve(count);
                    }
                });
            });
        } catch (err) {
            reject('Error: ' + err.message);
        }
    });
}
async function estimateDataCountRU({origin_awal, destination, services_code, froms, thrus, user_id}) {
    return new Promise((resolve, reject) => {
        oracledb.getConnection(config, (err, connection) => {
            if (err) {
                return reject('Error connecting to database: ' + err.message);
            }

            let whereClause = "WHERE 1=1 ";
            const bindParams = {};

            if (origin_awal !== '0') {
                whereClause += "AND  RT_CNOTE_ASLI_ORIGIN  like :origin_awal ";
                bindParams.origin_awal = origin_awal + '%';
            }

            if (destination !== '0') {
                whereClause += "and RT_CNOTE_DEST LIKE  :destination ";
                bindParams.destination = destination + '%';
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(RT_CRDATE_RT) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (services_code !== '0') {
                whereClause += "  AND RT_SERVICES_CODE LIKE :services_code ";  // ganti SERVICE_CODES jadi SERVICES_CODE
                bindParams.services_code = services_code + '%';
            }

            const sql = `SELECT COUNT(*) AS DATA_COUNT FROM V_OPS_RETURN_UNPAID ${whereClause}`;

            connection.execute(sql, bindParams, (err, result) => {
                connection.close();
                if (err) {
                    reject('Error executing query: ' + err.message);
                } else {
                    resolve(result.rows.length > 0 ? result.rows[0][0] : 0);
                }
            });
        });
    });
}

async function estimateDataCountDBO({ branch_id, currency, services_code, froms, thrus, user_id }) {
    return new Promise((resolve, reject) => {
        oracledb.getConnection(config, (err, connection) => {
            if (err) {
                return reject('Error connecting to database: ' + err.message);
            }

            let whereClause = "WHERE 1=1 ";
            const bindParams = {};

            // Gunakan branch_id jika tidak '0'
            if (branch_id && branch_id !== '0') {
                whereClause += "AND BRANCH_ID = :branch_id ";
                bindParams.branch_id = branch_id;
            }

            // Gunakan currency jika tidak '0'
            if (currency && currency !== '0') {
                whereClause += "AND CURRENCY LIKE :currency || '%' ";
                bindParams.currency = currency;
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (services_code && services_code !== '0') {
                whereClause += "AND SERVICES_CODE = :services_code ";
                bindParams.services_code = services_code;
            }

            const sql = `SELECT COUNT(*) AS DATA_COUNT FROM CMS_COST_DELIVERY_V2 ${whereClause} and CUST_NA IS NULL AND SUBSTR (CNOTE_NO, 1, 2) NOT IN ('FW', 'RT')`;

            connection.execute(sql, bindParams, (err, result) => {
                connection.close();
                if (err) {
                    reject('Error executing query: ' + err.message);
                } else {
                    // result.rows[0][0] adalah count(*) hasil query
                    resolve(result.rows.length > 0 ? result.rows[0][0] : 0);
                }
            });
        });
    });
}
async function estimateDataCountDBONA({ branch_id, currency, services_code, froms, thrus, user_id }) {
    return new Promise((resolve, reject) => {
        oracledb.getConnection(config, (err, connection) => {
            if (err) {
                return reject('Error connecting to database: ' + err.message);
            }

            let whereClause = "WHERE 1=1 ";
            const bindParams = {};

            // Gunakan branch_id jika tidak '0'
            if (branch_id && branch_id !== '0') {
                whereClause += "AND BRANCH_ID = :branch_id ";
                bindParams.branch_id = branch_id;
            }

            // Gunakan currency jika tidak '0'
            if (currency && currency !== '0') {
                whereClause += "AND CURRENCY LIKE :currency || '%' ";
                bindParams.currency = currency;
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (services_code && services_code !== '0') {
                whereClause += "AND SERVICES_CODE = :services_code ";
                bindParams.services_code = services_code;
            }
            const sql = `SELECT COUNT(*) AS DATA_COUNT FROM CMS_COST_DELIVERY_V2 ${whereClause} and CUST_NA = 'Y' AND SUBSTR (CNOTE_NO, 1, 2) NOT IN ('FW', 'RT')`;

            connection.execute(sql, bindParams, (err, result) => {
                connection.close();
                if (err) {
                    reject('Error executing query: ' + err.message);
                } else {
                    // result.rows[0][0] adalah count(*) hasil query
                    resolve(result.rows.length > 0 ? result.rows[0][0] : 0);
                }
            });
        });
    });
}


// not use func
async function buatZip(folderPath, zipFileName) {
    const output = fs.createWriteStream(zipFileName);
    const archive = archiver('zip', {zlib: {level: 1}});
    // Tangani error supaya tidak crash silent
    archive.on('error', err => {
        throw err;
    });
    // Mulai streaming archive ke file output
    archive.directory(folderPath, false);
    // Panggil finalize untuk memulai kompresi
    archive.finalize();
    // Tunggu sampai pipeline selesai (stream selesai)
    await pipeline(archive, output);
    // Setelah zip selesai, hapus folder sumber
    await fsPromises.rm(folderPath, {recursive: true, force: true});
    console.log(`Folder ${folderPath} telah dihapus setelah file ZIP dibuat.`);
}

async function fetchDataAndExportToExcel({ origin, destination, froms, thrus, user_id , dateStr,jobId}) {
    let connection;
    try {
        connection = await oracledb.getConnection(config);
        console.log("Koneksi berhasil ke database");

        let whereClause = "WHERE 1=1";
        const bindParams = {};

        if (origin !== '0') {
            whereClause += " AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin";
            bindParams.origin = origin + '%';
        }
        if (destination !== '0') {
            whereClause += " AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination";
            bindParams.destination = destination + '%';
        }
        if (froms !== '0' && thrus !== '0') {
            whereClause += " AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')";
            bindParams.froms = froms;
            bindParams.thrus = thrus;
        }

        const result = await connection.execute(`
            SELECT
                '''' || AWB_NO AS CONNOTE_NUMBER,
                TO_CHAR(AWB_DATE, 'MM/DD/YYYY HH:MI:SS AM') AS CONNOTE_DATE,
                SERVICES_CODE AS SERVICE_CONNOTE,
                OUTBOND_MANIFEST_NO AS OUTBOND_MANIFEST_NUMBER,
                OUTBOND_MANIFEST_DATE,
                ORIGIN,
                DESTINATION,
                ZONA_DESTINATION,
                OUTBOND_MANIFEST_ROUTE AS MANIFEST_ROUTE,
                TRANSIT_MANIFEST_NO AS TRANSIT_MANIFEST_NUMBER,
                TO_CHAR(TRANSIT_MANIFEST_DATE, 'MM/DD/YYYY HH:MI:SS AM') AS TRANSIT_MANIFEST_DATE,
                TRANSIT_MANIFEST_ROUTE,
                SMU_NUMBER,
                FLIGHT_NUMBER,
                BRANCH_TRANSPORTER,
                '''' || BAG_NO AS BAG_NUMBER,
                SERVICE_BAG,
                MODA,
                MODA_TYPE,
                CNOTE_WEIGHT AS WEIGHT_CONNOTE,
                ACT_WEIGHT AS WEIGHT_BAG,
                Round(PRORATED_WEIGHT, 3) AS PRORATED_WEIGHT,
                SUM(TRANSIT_FEE) AS TRANSIT_FEE,
                SUM(HANDLING_FEE) AS HANDLING_FEE,
                SUM(OTHER_FEE) AS OTHER_FEE,
                SUM(NVL(TRANSIT_FEE, 0) + NVL(HANDLING_FEE, 0) + NVL(OTHER_FEE, 0)) AS TOTAL,
                '''' || SYSDATE AS DOWNLOAD_DATE
            FROM CMS_COST_TRANSIT_V2
                     ${whereClause}
                AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
      AND CNOTE_WEIGHT > 0
            GROUP BY
                ROWNUM, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
        `, bindParams);

        // Bagi data ke chunk (50.000 baris)
        const chunkSize = 50000;
        const chunks = [];
        for (let i = 0; i < result.rows.length; i += chunkSize) {
            chunks.push(result.rows.slice(i, i + chunkSize));
        }

        // Buat folder unik untuk simpan file Excel
        const folderPath = path.join(__dirname, `./${uuidv4()}`);
        if (!fs.existsSync(folderPath)) {
            fs.mkdirSync(folderPath);
            console.log(`Folder ${folderPath} telah dibuat.`);
        }

        const today = new Date();
        const dateStrLocal = today.toISOString().split('T')[0];
        const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, '');

        // Loop buat file Excel per chunk streaming
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];

            const fileName = path.join(folderPath, `TCOReport_part${i + 1}.xlsx`);
            const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
                filename: fileName,
                useStyles: true,
                useSharedStrings: true
            });

            const worksheet = workbook.addWorksheet('Data Laporan TCO');

            // Header info
            worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]).commit();
            worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]).commit();
            worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]).commit();
            worksheet.addRow(['Download Date:', new Date().toLocaleString()]).commit();
            worksheet.addRow(['User Id:', user_id]).commit();
            worksheet.addRow(['Jumlah Data:', chunk.length]).commit();
            worksheet.addRow([]).commit();

            // Header kolom
            const headers = [
                "NO", "CONNOTE_NUMBER", "CONNOTE_DATE", "SERVICE_CONNOTE", "OUTBOND_MANIFEST_NUMBER",
                "OUTBOND_MANIFEST_DATE", "ORIGIN", "DESTINATION", "ZONA_DESTINATION", "MANIFEST_ROUTE",
                "TRANSIT_MANIFEST_NUMBER", "TRANSIT_MANIFEST_DATE", "TRANSIT_MANIFEST_ROUTE", "SMU_NUMBER",
                "FLIGHT_NUMBER", "BRANCH_TRANSPORTER", "BAG_NUMBER", "SERVICE_BAG", "MODA", "MODA_TYPE",
                "WEIGHT_CONNOTE", "WEIGHT_BAG", "PRORATED_WEIGHT", "TRANSIT_FEE", "HANDLING_FEE",
                "OTHER_FEE", "TOTAL", "DOWNLOAD_DATE"
            ];
            worksheet.addRow(headers).commit();

            // Data rows
            let no = 1 + i * chunkSize;
            for (const row of chunk) {
                worksheet.addRow([no++, ...row]).commit();
            }

            await workbook.commit();
            console.log(`File Excel part ${i + 1} telah dibuat: ${fileName}`);

            const updateQuery = `
                UPDATE CMS_COST_TRANSIT_V2_LOG
                SET SUMMARY_FILE = :summary_file
                WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category
            `;

            const updateValues = {
                summary_file: i + 1,  // nomor file yang sudah selesai dibuat
                jobId: jobId,         // pastikan jobId kamu sudah tersedia di scope fungsi
                category: 'TCO'
            };

            await connection.execute(updateQuery, updateValues);
            await connection.commit();
        }

        const zipFileName = path.join(__dirname, 'file_download', `TCOReport_${user_id}_${dateStrLocal}_${timeStr}.zip`);
        const output = fs.createWriteStream(zipFileName);
        const archive = archiver('zip', { zlib: { level: 1 } });

        return new Promise((resolve, reject) => {
            output.on('close', () => {
                console.log(`Zip file created (${archive.pointer()} total bytes): ${zipFileName}`);

                // Hapus folder setelah zip selesai
                fs.rmSync(folderPath, { recursive: true, force: true });
                console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

                resolve({ zipFileName, totalFiles: chunks.length, totalRows: result.rows.length });
            });

            archive.on('error', (err) => {
                reject(err);
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            archive.finalize();
        });

        // return { folderPath, totalFiles: chunks.length, totalRows: result.rows.length };
    } catch (err) {
        console.error('Terjadi kesalahan:', err);
        throw err;
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch {}
        }
    }
}


async function fetchDataAndExportToExcel_current({origin, destination, froms, thrus, user_id, dateStr, jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

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


            const result = await connection.execute( `
                        SELECT
                            '''' || AWB_NO                                                      AS CONNOTE_NUMBER,
                            TO_CHAR(AWB_DATE, 'MM/DD/YYYY HH:MI:SS AM')                         AS CONNOTE_DATE,          -- Format tanggal

                            SERVICES_CODE                                                       AS SERVICE_CONNOTE,

                            OUTBOND_MANIFEST_NO                                                 AS OUTBOND_MANIFEST_NUMBER,

                            OUTBOND_MANIFEST_DATE,

                            ORIGIN,

                            DESTINATION,

                            ZONA_DESTINATION,

                            OUTBOND_MANIFEST_ROUTE                                              AS MANIFEST_ROUTE,

                            TRANSIT_MANIFEST_NO                                                 AS TRANSIT_MANIFEST_NUMBER,
--F_GET_MANIFEST_OM_V4(BAG_NO) as TRANSIT_MANIFEST_NUMBER,

                            TO_CHAR(TRANSIT_MANIFEST_DATE, 'MM/DD/YYYY HH:MI:SS AM')            AS TRANSIT_MANIFEST_DATE, -- Format tanggal

                            TRANSIT_MANIFEST_ROUTE,                                                                       --BAG_ROUTE 

                            SMU_NUMBER,

                            FLIGHT_NUMBER,

                            BRANCH_TRANSPORTER,

                            '''' || BAG_NO                                                      AS BAG_NUMBER,

                            SERVICE_BAG,

                            MODA,

                            MODA_TYPE,

                            CNOTE_WEIGHT                                                        AS WEIGHT_CONNOTE,

                            ACT_WEIGHT                                                          AS WEIGHT_BAG,

                            Round(PRORATED_WEIGHT, 3)                                           AS PRORATED_WEIGHT,

                            SUM(TRANSIT_FEE)                                                    AS TRANSIT_FEE,           -- Gunakan SUM 

                            SUM(HANDLING_FEE)                                                   AS HANDLING_FEE,          -- Gunakan SUM 

                            SUM(OTHER_FEE)                                                      AS OTHER_FEE,             -- Gunakan SUM 

                            SUM(NVL(TRANSIT_FEE, 0) + NVL(HANDLING_FEE, 0) + NVL(OTHER_FEE, 0)) AS TOTAL,
                            '''' || SYSDATE                                                     AS DOWNLOAD_DATE
                        FROM CMS_COST_TRANSIT_V2 ${whereClause}
                            AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                    AND CNOTE_WEIGHT > 0
                        GROUP BY
                            ROWNUM, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                            BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                            ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                            SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                `,
                bindParams
            );

            let no = 1;  // Initialize counter for 'NO'

            console.log()
            dataCount = result.rows.length;
            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }

            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format
            const folderPath = path.join(__dirname, `./${uuidv4()}`);
            // const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', {total: chunks.length, width: 20});


            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan');

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]);
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', user_id]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(10);
                headerRow.values = [
                    "NO",
                    "CONNOTE_NUMBER",
                    "CONNOTE_DATE",
                    "SERVICE_CONNOTE",
                    "OUTBOND_MANIFEST_NUMBER",
                    "OUTBOND_MANIFEST_DATE",
                    "ORIGIN",
                    "DESTINATION",
                    "ZONA_DESTINATION",
                    "MANIFEST_ROUTE",
                    "TRANSIT_MANIFEST_NUMBER",
                    "TRANSIT_MANIFEST_DATE",
                    "TRANSIT_MANIFEST_ROUTE",
                    "SMU_NUMBER",
                    "FLIGHT_NUMBER",
                    "BRANCH_TRANSPORTER",
                    "BAG_NUMBER",
                    "SERVICE_BAG",
                    "MODA",
                    "MODA_TYPE",
                    "WEIGHT_CONNOTE",
                    "WEIGHT_BAG",
                    "PRORATED_WEIGHT",
                    "TRANSIT_FEE",
                    "HANDLING_FEE",
                    "OTHER_FEE",
                    "TOTAL",
                    "DOWNLOAD_DATE",
                ];
// Menambahkan alias ke setiap kolom
                const headerRowIndex = 10; // Baris 10
                let currentRowIndex = headerRowIndex + 1; // Baris 11 untuk data

                // 1. Tulis header di baris ke-10
                worksheet.getRow(headerRowIndex).values = [
                    "NO",
                    "CONNOTE NUMBER",
                    "CONNOTE DATE",
                    "SERVICE CONNOTE",
                    "OUTBOND MANIFEST NUMBER",
                    "OUTBOND MANIFEST DATE",
                    "ORIGIN",
                    "DESTINATION",
                    "ZONA DESTINATION",
                    "MANIFEST ROUTE",
                    "TRANSIT MANIFEST NUMBER",
                    "TRANSIT MANIFEST DATE",
                    "TRANSIT MANIFEST ROUTE",
                    "SMU NUMBER",
                    "FLIGHT NUMBER",
                    "BRANCH TRANSPORTER",
                    "BAG NUMBER",
                    "SERVICE BAG",
                    "MODA",
                    "MODA TYPE",
                    "WEIGHT CONNOTE",
                    "WEIGHT BAG",
                    "PRORATED WEIGHT",
                    "TRANSIT FEE",
                    "HANDLING FEE",
                    "OTHER FEE",
                    "TOTAL",
                    "DOWNLOAD DATE",
                ];

                worksheet.getColumn(3).numFmt = "m/d/yyyy h:mm:ss AM/PM"; // CONNOTE DATE
                worksheet.getColumn(6).numFmt = "m/d/yyyy h:mm:ss AM/PM"; // OUTBOND MANIFEST DATE
                worksheet.getColumn(12).numFmt = "m/d/yyyy h:mm:ss AM/PM"; // TRANSIT MANIFEST DATE

                worksheet.getColumn(23).numFmt = "#,##0"; // BIAYA PENERUS NEXT KG
                worksheet.getColumn(24).numFmt = "#,##0"; // BIAYA TRANSIT
                worksheet.getColumn(25).numFmt = "#,##0"; // BIAYA PENERUS
                worksheet.getColumn(26).numFmt = "#,##0"; // BIAYA PENERUS NEXT KG

                chunk.forEach((row) => {
                    row[23] = parseFloat(row[23]) || 0; // kolom 15
                    row[24] = parseFloat(row[24]) || 0; // kolom 15
                    row[25] = parseFloat(row[25]) || 0; // kolom 16
                    row[26] = parseFloat(row[26]) || 0; // kolom 17

                    worksheet.getRow(currentRowIndex++).values = [no++, ...row];
                });

                const fileName = path.join(folderPath, `TCOReport_${dateStr}_${user_id}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);
                console.log(`Data berhasil diekspor ke ${fileName}`);
                console.log(`job id ${jobId}`);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId  and CATEGORY = :category
                `;
                const updateValues = {
                    summary_file: i + 1, // Update the summary_file with the number of parts processed
                    jobId: jobId,
                    category: 'TCO'
                };
                await connection.execute(updateQuery, updateValues);
                await connection.commit();
                // bar.tick();

            }

            const zipFileName = path.join(__dirname, 'file_download', `TCOReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 1}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();
            fs.rmSync(folderPath, {recursive: true, force: true});
            // fs.rmdirSync(folderPath, {recursive: true, force: true });
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, 'error_logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir);
                }
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify(params, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
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
                                                dateStr,
                                                jobId
                                            }) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

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

            const result = await connection.execute( `
                        SELECT '''' || AWB_NO                                                      AS CONNOTE_NUMBER,
--                      AWB_DATE AS CONNOTE_DATE,
                               TO_CHAR(AWB_DATE, 'MM/DD/YYYY HH:MI:SS AM')                         AS AWB_DATE,              -- Format tanggal
                               SERVICES_CODE                                                       AS SERVICE_CONNOTE,
                               OUTBOND_MANIFEST_NO                                                 AS OUTBOND_MANIFEST_NUMBER,
                               TO_CHAR(OUTBOND_MANIFEST_DATE, 'MM/DD/YYYY HH:MI:SS AM')            AS OUTBOND_MANIFEST_DATE, -- Format tanggal
                               ORIGIN,
                               DESTINATION,
                               ZONA_DESTINATION,
                               OUTBOND_MANIFEST_ROUTE                                              AS MANIFEST_ROUTE,
                               TRANSIT_MANIFEST_NO                                                 AS TRANSIT_MANIFEST_NUMBER,
--                               F_GET_MANIFEST_OM_V4(BAG_NO) as TRANSIT_MANIFEST_NUMBER,
                               TRANSIT_MANIFEST_DATE            AS TRANSIT_MANIFEST_DATE, -- Format tanggal
                               TRANSIT_MANIFEST_ROUTE,
                               SMU_NUMBER,
                               FLIGHT_NUMBER,
                               BRANCH_TRANSPORTER,
                               '''' || BAG_NO                                                      AS BAG_NUMBER,
                               SERVICE_BAG,
                               MODA,
                               MODA_TYPE,
                               round(CNOTE_WEIGHT, 3)                                              AS WEIGHT_CONNOTE,
                               round(ACT_WEIGHT, 3)                                                AS WEIGHT_BAG,
                               round(PRORATED_WEIGHT, 3)                                           AS PRORATED_WEIGHT,
                               SUM(TRANSIT_FEE)                                                    AS TRANSIT_FEE,
                               SUM(HANDLING_FEE)                                                   AS HANDLING_FEE,
                               SUM(OTHER_FEE)                                                      AS OTHER_FEE,
                               SUM(NVL(TRANSIT_FEE, 0) + NVL(HANDLING_FEE, 0) + NVL(OTHER_FEE, 0)) AS TOTAL,

                               SYSDATE                                                             AS DOWNLOAD_DATE
                        FROM CMS_COST_TRANSIT_V2 ${whereClause} AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                    AND CNOTE_WEIGHT > 0
                        GROUP BY
                            OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                            BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                            ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                            SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                `,
                bindParams
            );
            dataCount = result.rows.length;
            let no = 1;  // Initialize counter for 'NO'
            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }

            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format
            const folderPath = path.join(__dirname, `./${uuidv4()}`);
            // const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }

            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan TCI');

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]);
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
                worksheet.addRow(["Branch:", TM === "0" ? "ALL" : TM]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', user_id]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(11);
                headerRow.values = [
                    "NO",
                    "CONNOTE_NUMBER",
                    "CONNOTE_DATE",
                    "SERVICE_CONNOTE",
                    "OUTBOND_MANIFEST_NUMBER",
                    "OUTBOND_MANIFEST_DATE",
                    "ORIGIN",
                    "DESTINATION",
                    "ZONA_DESTINATION",
                    "MANIFEST_ROUTE",
                    "TRANSIT_MANIFEST_NUMBER",
                    "TRANSIT_MANIFEST_DATE",
                    "TRANSIT_MANIFEST_ROUTE",
                    "SMU_NUMBER",
                    "FLIGHT_NUMBER",
                    "BRANCH_TRANSPORTER",
                    "BAG_NUMBER",
                    "SERVICE_BAG",
                    "MODA",
                    "MODA_TYPE",
                    "WEIGHT_CONNOTE",
                    "WEIGHT_BAG",
                    "PRORATED_WEIGHT",
                    "TRANSIT_FEE",
                    "HANDLING_FEE",
                    "OTHER_FEE",
                    "TOTAL",
                    "DOWNLOAD_DATE",
                ];
                const headerRowIndex = 10; // Baris 10
                let currentRowIndex = headerRowIndex + 1; // Baris 11 untuk data

                // 1. Tulis header di baris ke-10
                worksheet.getRow(headerRowIndex).values = [
                    "NO",
                    "CONNOTE NUMBER",
                    "CONNOTE DATE",
                    "SERVICE CONNOTE",
                    "OUTBOND MANIFEST NUMBER",
                    "OUTBOND MANIFEST DATE",
                    "ORIGIN",
                    "DESTINATION",
                    "ZONA DESTINATION",
                    "MANIFEST ROUTE",
                    "TRANSIT MANIFEST NUMBER",
                    "TRANSIT MANIFEST DATE",
                    "TRANSIT MANIFEST ROUTE",
                    "SMU NUMBER",
                    "FLIGHT NUMBER",
                    "BRANCH TRANSPORTER",
                    "BAG NUMBER",
                    "SERVICE BAG",
                    "MODA",
                    "MODA TYPE",
                    "WEIGHT CONNOTE",
                    "WEIGHT BAG",
                    "PRORATED WEIGHT",
                    "TRANSIT FEE",
                    "HANDLING FEE",
                    "OTHER FEE",
                    "TOTAL",
                    "DOWNLOAD DATE",
                ];

                // 2. Format kolom tanggal dan biaya
                worksheet.getColumn(3).numFmt = "m/d/yyyy h:mm:ss AM/PM"; // CONNOTE DATE
                worksheet.getColumn(6).numFmt = "m/d/yyyy h:mm:ss AM/PM"; // OUTBOND MANIFEST DATE
                worksheet.getColumn(12).numFmt = "m/d/yyyy h:mm:ss AM/PM"; // TRANSIT MANIFEST DATE
                worksheet.getColumn(26).numFmt = "m/d/yyyy h:mm:ss AM/PM"; // DOWNLOAD DATE

                // Format kolom biaya (angka dengan ribuan dan desimal)
                worksheet.getColumn(23).numFmt = "#,##0.00"; // TRANSIT FEE
                worksheet.getColumn(24).numFmt = "#,##0.00"; // HANDLING FEE
                worksheet.getColumn(25).numFmt = "#,##0.00"; // OTHER FEE
                worksheet.getColumn(26).numFmt = "#,##0.00"; // TOTAL
                // 3. Tambahkan data ke worksheet
                chunk.forEach((row) => {
                    worksheet.getRow(currentRowIndex++).values = [no++, ...row];

                });
                const fileName = path.join(folderPath, `TCIReport_${dateStr}_${user_id}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);
                console.log(`Data berhasil diekspor ke ${fileName}`);
                console.log(`job id ${jobId}`);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId  and CATEGORY = :category
                `;
                const updateValues = {
                    summary_file: i + 1, // Update the summary_file with the number of parts processed
                    jobId: jobId,
                    category: 'TCI'
                };
                await connection.execute(updateQuery, updateValues);
                await connection.commit();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `TCIReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 1}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();
            fs.rmSync(folderPath, {recursive: true, force: true});
            // fs.rmdirSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, 'error_logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir);
                }
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify(params, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}


async function fetchDataAndExportToExcelDCI({origin, destination, froms, thrus, service, user_id, dateStr, jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

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
                        SELECT '''' || CNOTE_NO                                 AS CNOTE_NO,
                               TO_CHAR(CNOTE_DATE, 'MM/DD/YYYY HH:MI:SS AM')    AS CNOTE_DATE,     -- Format tanggal
                               ORIGIN,
                               DESTINATION,
                               ZONA_DESTINATION,
                               SERVICES_CODE,
                               NVL(QTY, 0)                                         QTY,
                               CASE
                                   WHEN WEIGHT = 0 THEN 0
                                   WHEN WEIGHT < 1 THEN 1
                                   WHEN RPAD(REGEXP_SUBSTR(WEIGHT, '[[:digit:]]+$'), 3, 0) > 300 THEN CEIL(WEIGHT)
                                   ELSE FLOOR(WEIGHT)
                                   END                                             WEIGHT,
                               nvl(AMOUNT, 0)                                      AMOUNT,
                               MANIFEST_NO,
                               --TO_CHAR(MANIFEST_DATE,'MM-DD-RRRR') MANIFEST_DATE, 
                               TO_CHAR(MANIFEST_DATE, 'MM/DD/YYYY') AS MANIFEST_DATE,  -- Format tanggal
                               NVL(DELIVERY, 0)                                    DELIVERY,
                               NVL(DELIVERY_SPS, 0)                                DELIVERY_SPS,
                               NVL(TRANSIT, 0)                                  AS BIAYA_TRANSIT,
                               NVL(LINEHAUL_FIRST, 0)                              LINEHAUL_FIRST, -- remark by ibnu 01 oct 2024 di ambil nilai inehaulnya saja  
                               nvl(LINEHAUL_NEXT, 0)                               LINEHAUL_NEXT
--                     CASE WHEN SERVICES_CODE LIKE 'JTR%' THEN 'LOG' ELSE 'EXP' END TIPE, 
                        FROM CMS_COST_DELIVERY_V2 ${whereClause} AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
                --AND SERVICE_CODE NOT IN ('TRC11','TRC13')  -- remark by ibnu 18 sep 2024 req team ctc 
                AND SERVICES_CODE NOT IN ('CML','CTC_CML','P2P')
                AND CNOTE_NO NOT LIKE 'RT%' --10 OCT 2022 REQ RT TIDAK MASUK REQUEST BY RICKI, BA : YOGA 
                AND CNOTE_NO NOT LIKE 'FW%' --22 NOV 2022 REQ RT TIDAK MASUK REQUEST BY RICKI, BA : YOGA 
                `,
                bindParams
            );
            dataCount = result.rows.length;
            let no = 1;
            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }
            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format
            const folderPath = path.join(__dirname, `./${uuidv4()}`);

            // const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', {total: chunks.length, width: 20});

            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan DCI');
                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]);
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
                worksheet.addRow(['Service Code:', service === '0' ? 'ALL' : service]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', user_id]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);
                worksheet.addRow([]);
                const headerRow = worksheet.getRow(11);
                headerRow.values = [
                    "NO",
                    "CNOTE_NO",
                    "CNOTE_DATE",
                    "ORIGIN",
                    "DESTINATION",
                    "ZONA_DESTINATION",
                    "SERVICES_CODE",
                    "QTY",
                    "WEIGHT",
                    "AMOUNT",
                    "MANIFEST_NO",
                    "MANIFEST_DATE",
                    "DELIVERY",
                    "DELIVERY_SPS",
                    "BIAYA_TRANSIT",
                    "LINEHAUL_FIRST",
                    "LINEHAUL_NEXT"
                ];

                const headerRowIndex = 10;
                const dataStartRowIndex = headerRowIndex + 1;
                let currentRowIndex = dataStartRowIndex;


                worksheet.getRow(headerRowIndex).values = [
                    "NO",
                    "CNOTE NO",
                    "CNOTE DATE",
                    "ORIGIN",
                    "DESTINATION",
                    "ZONA DESTINATION",
                    "SERVICES CODE",
                    "QTY",
                    "WEIGHT",
                    "AMOUNT",
                    "MANIFEST NO",
                    "MANIFEST DATE",
                    "DELIVERY",
                    "DELIVERY SPS",
                    "BIAYA TRANSIT",
                    "BIAYA PENERUS",
                    "BIAYA PENERUS NEXT KG"
                ];


                worksheet.getColumn(10).numFmt = "#,##0.00"; // BIAYA PENERUS NEXT KG
                worksheet.getColumn(13).numFmt = "#,##0.00"; // BIAYA PENERUS NEXT KG
                worksheet.getColumn(14).numFmt = "#,##0.00"; // BIAYA PENERUS NEXT KG
                worksheet.getColumn(15).numFmt = "#,##0.00"; // BIAYA PENERUS NEXT KG
                worksheet.getColumn(16).numFmt = "#,##0.00"; // BIAYA PENERUS NEXT KG
                worksheet.getColumn(17).numFmt = "#,##0.00"; // BIAYA PENERUS NEXT KG

                chunk.forEach((row) => {
                    worksheet.getRow(currentRowIndex++).values = [no++, ...row];
                });
                const fileName = path.join(folderPath, `DCIReport_${dateStr}_${user_id}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);
                console.log(`job id ${jobId}`);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId and CATEGORY = :category
                `;
                const updateValues = {
                    summary_file: i + 1, // Update the summary_file with the number of parts processed
                    jobId: jobId,
                    category: 'DCI'
                };
                await connection.execute(updateQuery, updateValues);
                await connection.commit();


                // bar.tick();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `DCIReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 1}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, 'error_logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir);
                }
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify(params, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}

async function fetchDataAndExportToExcelDCO({origin, destination, froms, thrus, service, user_id, dateStr,jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

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


            const result = await connection.execute(`SELECT
                                                         '''' || CNOTE_NO                                 AS CNOTE_NO,
                                                         TO_CHAR(CNOTE_DATE, 'MM/DD/YYYY HH:MI:SS AM')    AS CNOTE_DATE,
                                                         ORIGIN,
                                                         DESTINATION,
                                                         NVL(QTY, 0)                                      AS QTY,
                                                         ZONA_DESTINATION,
                                                         SERVICES_CODE,
                                                         CASE
                                                             WHEN WEIGHT = 0 THEN 0
                                                             WHEN WEIGHT < 1 THEN 1
                                                             WHEN RPAD(REGEXP_SUBSTR(WEIGHT, '[[:digit:]]+$'), 3, 0) > 300 THEN CEIL(WEIGHT)
                                                             ELSE FLOOR(WEIGHT)
                                                             END                                             WEIGHT,
                                                         NVL(AMOUNT, 0)                                   AS AMOUNT,
                                                         MANIFEST_NO,
                                                         TO_CHAR(MANIFEST_DATE, 'MM/DD/YYYY HH:MI:SS AM') AS MANIFEST_DATE, -- Format tanggal

                                                         NVL(DELIVERY, 0)                                 AS DELIVERY,
                                                         NVL(DELIVERY_SPS, 0)                             AS DELIVERY_SPS,
                                                         NVL(TRANSIT, 0)                                  AS BIAYA_TRANSIT,
                                                         NVL(LINEHAUL_FIRST, 0)                           AS LINEHAUL_FIRST,
                                                         NVL(LINEHAUL_NEXT, 0)                            AS LINEHAUL_NEXT
                                                     FROM CMS_COST_DELIVERY_V2 ${whereClause} AND SUBSTR(ORIGIN, 1, 3) <> SUBSTR(DESTINATION, 1, 3)
                AND SERVICES_CODE NOT IN ('CML', 'CTC_CML', 'P2P')
                AND CNOTE_NO NOT LIKE 'RT%'  -- Exclude records with CNOTE_NO starting with 'RT'
                AND CNOTE_NO NOT LIKE 'FW%' -- Exclude records with CNOTE_NO starting with 'FW'
                `,
                bindParams
            );

            dataCount = result.rows.length;

            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }

            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format
            const folderPath = path.join(__dirname, `./${uuidv4()}`);
            // const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', {total: chunks.length, width: 20});

            let no = 1;
            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan DCO');

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]);
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
                worksheet.addRow(['Service Code:', service === '0' ? 'ALL' : service]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', user_id]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(11);
                headerRow.values = [
                    "NO",
                    "CNOTE_NO",
                    "CNOTE_DATE",
                    "ORIGIN",
                    "DESTINATION",
                    "QTY",
                    "ZONA_DESTINATION",
                    "SERVICES_CODE",
                    "WEIGHT",
                    "AMOUNT",
                    "MANIFEST_NO",
                    "MANIFEST_DATE",
                    "DELIVERY",
                    "DELIVERY_SPS",
                    "BIAYA_TRANSIT",
                    "LINEHAUL_FIRST",
                    "LINEHAUL_NEXT",
                ];

                const headerRowIndex = 10; // Baris 10
                let currentRowIndex = headerRowIndex + 1; // Baris 11
                worksheet.getRow(headerRowIndex).values = [
                    "NO",
                    "CONNOTE NO",
                    "CONNOTE DATE TIME",
                    "ORIGIN",
                    "DESTINATION",
                    "COLLY",
                    "ZONA",
                    "SERVICES CODE",
                    "WEIGHT",
                    "AMOUNT",
                    "MANIFEST NO",
                    "MANIFEST DATE",
                    "DELIVERY",
                    "DELIVERY SPS",
                    "BIAYA TRANSIT",
                    "PENERUS",
                    "BIAYA PENERUS NEXT KG",
                ];

                worksheet.getColumn(8).numFmt = "#,##0"; // BIAYA PENERUS NEXT KG
                worksheet.getColumn(10).numFmt = "#,##0"; // BIAYA PENERUS NEXT KG
                worksheet.getColumn(12).numFmt = "#,##0"; // BIAYA PENERUS NEXT KG
                worksheet.getColumn(13).numFmt = "#,##0"; // BIAYA TRANSIT
                worksheet.getColumn(14).numFmt = "#,##0"; // BIAYA TRANSIT
                worksheet.getColumn(15).numFmt = "#,##0"; // BIAYA PENERUS
                worksheet.getColumn(16).numFmt = "#,##0"; // BIAYA PENERUS
                worksheet.getColumn(17).numFmt = "#,##0"; // BIAYA PENERUS

                chunk.forEach((row) => {
                    worksheet.getRow(currentRowIndex++).values = [no++, ...row];
                });

                const fileName = path.join(folderPath, `DCOReport_${dateStr}_${user_id}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);

                console.log(`job id ${jobId}`);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId
                `;
                const updateValues = {
                    summary_file: i + 1, // Update the summary_file with the number of parts processed
                    jobId: jobId
                };
                await connection.execute(updateQuery, updateValues);
                await connection.commit();


                // bar.tick();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `DCOReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 1}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, 'error_logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir);
                }
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify(params, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}

async function fetchDataAndExportToExcelCA({branch, froms, thrus, user_id, dateStr,jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        let connectionUpdate;  // koneksi untuk UPDATE (config)
        try {
            connection = await oracledb.getConnection(config_jnebill);
            connectionUpdate = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (branch !== '0') {
                whereClause += ` AND C.CNOTE_BRANCH_ID = :branch`;
                bindParams.branch = branch;
            }
            if (froms !== '0' && thrus !== '0') {
                whereClause += ` AND TRUNC(C.CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-RRRR') AND TO_DATE(:thrus, 'DD-MON-RRRR')`;
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }


            const result = await connection.execute(`
                SELECT
                    'B' AS FLAG,
                    A.HYBRID_CUST,
                    B.CUST_NAME,
                    D.CUST_ID,
                    D.CUST_NAME,
                    C.CNOTE_NO,
                    C.CNOTE_BRANCH_ID,
                    TRUNC(A.CREATE_DATE),
                    C.CNOTE_ORIGIN,
                    C.CNOTE_DESTINATION,
                    C.CNOTE_SERVICES_CODE,
                    A.APICUST_INS_FLAG,
                    C.CNOTE_WEIGHT,
                    C.CNOTE_GOODS_VALUE,
                    NVL(C.CNOTE_INSURANCE_VALUE, 0),
                    NVL(C.CNOTE_AMOUNT, 0),
                    (NVL(C.CNOTE_AMOUNT, 0) - NVL(C.CNOTE_INSURANCE_VALUE, 0)),
                    CASE
                        WHEN A.COMM_PCTG BETWEEN 1 AND 100 THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (A.COMM_ANTAR + A.COMM_PCTG)) / 100)
                        WHEN A.DISC_FLAT = 'Y' THEN A.DISC_AMT
                        ELSE 0
                        END AS COMMISSION,
                    CASE
                        WHEN NVL(B.CUST_PPH_TYPE,1) = 1 AND NVL(B.CUST_PPH23_FLAG,'N') = 'Y' THEN '2.5%'
                        WHEN NVL(B.CUST_PPH_TYPE,1) = 1 AND NVL(B.CUST_PPH23_FLAG,'N') = 'N' THEN '3%'
                        WHEN NVL(B.CUST_PPH_TYPE,1) = 2 AND NVL(B.CUST_PPH23_FLAG,'N') = 'Y' THEN '2%'
                        WHEN NVL(B.CUST_PPH_TYPE,1) = 2 AND NVL(B.CUST_PPH23_FLAG,'N') = 'N' THEN '4%'
                        ELSE '2.5%'
                        END AS PERCENT,
                    CASE
                        WHEN NVL(B.CUST_PPH_TYPE,1) = 1 AND NVL(B.CUST_PPH23_FLAG,'N') = 'Y' THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (A.COMM_ANTAR + A.COMM_PCTG)) / 100) * (2.5/100)
                        WHEN NVL(B.CUST_PPH_TYPE,1) = 1 AND NVL(B.CUST_PPH23_FLAG,'N') = 'N' THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (A.COMM_ANTAR + A.COMM_PCTG)) / 100) * (3/100)
                        WHEN NVL(B.CUST_PPH_TYPE,1) = 2 AND NVL(B.CUST_PPH23_FLAG,'N') = 'Y' THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (A.COMM_ANTAR + A.COMM_PCTG)) / 100) * (2/100)
                        WHEN NVL(B.CUST_PPH_TYPE,1) = 2 AND NVL(B.CUST_PPH23_FLAG,'N') = 'N' THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (A.COMM_ANTAR + A.COMM_PCTG)) / 100) * (4/100)
                        ELSE (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (A.COMM_ANTAR + A.COMM_PCTG)) / 100) * (2.5/100)
                        END AS PPH23,
                    F_GET_INFO_INVOICE(A.APICUST_CNOTE_NO, 1) AS INVOICE,
                    F_GET_INFO_INVOICE(A.APICUST_CNOTE_NO, 2) AS INV_DATE,
                    F_GET_INVOICE_AMOUNT(A.APICUST_CNOTE_NO, 1) AS INV_AMOUNT,
                    F_GET_INVOICE_AMOUNT(A.APICUST_CNOTE_NO, 7) AS DISCOUNT,
                    F_GET_INVOICE_AMOUNT(A.APICUST_CNOTE_NO, 8) AS AFT_DISC,
                    F_GET_INV_RAISE(A.APICUST_CNOTE_NO) AS INV_RAISE,
                    F_GET_INV_RAISE_DT(A.APICUST_CNOTE_NO) AS INV_RAISE_DATE,
                    A.APICUST_MERCHAN_ID AS SELLER_ID,
                    A.APICUST_NAME AS SELLER_NAME
                FROM CMS_APICUST_HYBRID A
                         JOIN CMS_CUST B ON A.HYBRID_CUST = B.CUST_ID AND A.HYBRID_BRANCH = B.CUST_BRANCH
                         JOIN CMS_CNOTE@DBS2 C ON A.APICUST_CNOTE_NO = C.CNOTE_NO
                         JOIN CMS_CUST D ON D.CUST_BRANCH = A.HYBRID_BRANCH AND D.CUST_ID = A.APICUST_CUST_NO
                    ${whereClause}
                   -- C.CNOTE_BRANCH_ID = :P_BRANCH
                --  AND TRUNC(C.CNOTE_DATE) BETWEEN TO_DATE(:P_DATE1, 'DD-MON-RRRR') AND TO_DATE(:P_DATE2, 'DD-MON-RRRR')
                  AND B.CUST_TYPE IN ('995','996','997','994')
                 -- AND D.CUST_TYPE NOT IN ('995','996','997','994')
                 AND HYBRID_CUST=B.CUST_ID
                AND HYBRID_BRANCH=B.CUST_BRANCH
                  AND NVL((SELECT CUST_KP FROM ECONNOTE_CUST E WHERE E.CUST_BRANCH = C.CNOTE_BRANCH_ID AND B.CUST_ID = E.CUST_ID AND CUST_KP = 'N'), 'N') = 'N'
                  AND NVL(C.CNOTE_CANCEL, 'N') = 'N'
                AND HYBRID_CUST=B.CUST_ID
                AND HYBRID_BRANCH=B.CUST_BRANCH
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
            const folderPath = path.join(__dirname, `./${uuidv4()}`);
            // const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', {total: chunks.length, width: 20});

            let no = 1;
            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan CA');

                worksheet.addRow(['Branch:', branch === '0' ? 'ALL' : branch]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', user_id]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(11);
                headerRow.values = [
                    "NO",
                    "HYBRID_CUST",
                    "CUST_NAME",
                    "MARKETPLACE_ID",
                    "MARKETPLACE_NAME",
                    "CNOTE_NO",
                    "CNOTE_BRANCH_ID",
                    "CREATE_DATE",
                    "CNOTE_ORIGIN",
                    "CNOTE_DESTINATION",
                    "CNOTE_SERVICES_CODE",
                    "APICUST_INS_FLAG",
                    "CNOTE_WEIGHT",
                    "CNOTE_GOODS_VALUE",
                    "CNOTE_INSURANCE_VALUE",
                    "CNOTE_AMOUNT",
                    "AMOUNT_EXCLUDE",
                    "COMMISSION",
                    "PERCENT",
                    "PPH23",
                    "INVOICE",
                    "INV_DATE",
                    "INV_AMOUNT",
                    "DISCOUNT",
                    "AFT_DISC",
                    "INV_RAISE",
                    "INV_RAISE_DATE",
                    "SELLER_ID",
                    "SELLER_NAME"
                ];

                const headerRowIndex = 10; // Baris 10
                let currentRowIndex = headerRowIndex + 1; // Baris 11
                worksheet.getRow(headerRowIndex).values = [
                    "NO",
                    "HYBRID_CUST",
                    "CUST_NAME",
                    "MARKETPLACE_ID",
                    "MARKETPLACE_NAME",
                    "CNOTE_NO",
                    "CNOTE_BRANCH_ID",
                    "CREATE_DATE",
                    "CNOTE_ORIGIN",
                    "CNOTE_DESTINATION",
                    "CNOTE_SERVICES_CODE",
                    "APICUST_INS_FLAG",
                    "CNOTE_WEIGHT",
                    "CNOTE_GOODS_VALUE",
                    "CNOTE_INSURANCE_VALUE",
                    "CNOTE_AMOUNT",
                    "AMOUNT_EXCLUDE",
                    "COMMISSION",
                    "PERCENT",
                    "PPH23",
                    "INVOICE",
                    "INV_DATE",
                    "INV_AMOUNT",
                    "DISCOUNT",
                    "AFT_DISC",
                    "INV_RAISE",
                    "INV_RAISE_DATE",
                    "SELLER_ID",
                    "SELLER_NAME"
                ];

                chunk.forEach((row) => {
                    worksheet.getRow(currentRowIndex++).values = [no++, ...row];
                });

                const fileName = path.join(folderPath, `CAReport_${dateStr}_${user_id}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);

                console.log(`job id ${jobId}`);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId
                `;
                const updateValues = {
                    summary_file: i + 1, // Update the summary_file with the number of parts processed
                    jobId: jobId
                };
                const updateResult = await connectionUpdate.execute(updateQuery, updateValues);
                await connectionUpdate.commit();
                console.log("Update berhasil:", updateResult);

                // bar.tick();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `CAReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 1}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);
            console.log(zipFileName)

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, 'error_logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir);
                }
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify(params, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
            if (connectionUpdate) {
                await connectionUpdate.close();
            }
        }
    });
}
async function fetchDataAndExportToExcelCABTM({branch, froms, thrus, user_id, dateStr,jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        let connectionUpdate;  // koneksi untuk UPDATE (config)
        try {
            connection = await oracledb.getConnection(config_jnebill);
            connectionUpdate = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (branch !== '0') {
                whereClause += ` AND C.CNOTE_BRANCH_ID = :branch`;
                bindParams.branch = branch;
            }
            if (froms !== '0' && thrus !== '0') {
                whereClause += ` AND TRUNC(C.CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-RRRR') AND TO_DATE(:thrus, 'DD-MON-RRRR')`;
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }


            const result = await connection.execute(`
                SELECT
                    'B' AS FLAG,
                    HYBRID_CUST,
                    CUST_NAME,
                    MARKETPLACE_ID,
                    MARKETPLACE_NAME,
                    APICUST_MERCHAN_ID,
                    APICUST_NAME,
                    CNOTE_NO,
                    CNOTE_BRANCH_ID,
                    TRUNC(CREATE_DATE) AS CREATE_DATE,
                    CNOTE_ORIGIN,
                    CNOTE_DESTINATION,
                    CNOTE_SERVICES_CODE,
                    APICUST_INS_FLAG,
                    CNOTE_WEIGHT,
                    CNOTE_GOODS_VALUE,
                    NVL(C.CNOTE_INSURANCE_VALUE, 0) AS CNOTE_INSURANCE_VALUE,
                    NVL(C.CNOTE_AMOUNT, 0) AS CNOTE_AMOUNT,
                    NVL(C.CNOTE_AMOUNT, 0) - NVL(C.CNOTE_INSURANCE_VALUE, 0) AS NET_AMOUNT,
                    CASE
                        WHEN COMM_PCTG BETWEEN 1 AND 100 THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (COMM_ANTAR + COMM_PCTG)) / 100)
                        WHEN DISC_FLAT = 'Y' THEN DISC_AMT
                        ELSE 0
                        END AS COMMISSION,
                    CASE
                        WHEN NVL(CUST_PPH_TYPE, 1) = 1 AND NVL(CUST_PPH23_FLAG, 'N') = 'Y' THEN '2.5%'
                        WHEN NVL(CUST_PPH_TYPE, 1) = 1 AND NVL(CUST_PPH23_FLAG, 'N') = 'N' THEN '3%'
                        WHEN NVL(CUST_PPH_TYPE, 1) = 2 AND NVL(CUST_PPH23_FLAG, 'N') = 'Y' THEN '2%'
                        WHEN NVL(CUST_PPH_TYPE, 1) = 2 AND NVL(CUST_PPH23_FLAG, 'N') = 'N' THEN '4%'
                        ELSE '2.5%'
                        END AS PPH_RATE,
                    CASE
                        WHEN NVL(CUST_PPH_TYPE, 1) = 1 AND NVL(CUST_PPH23_FLAG, 'N') = 'Y' THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (COMM_ANTAR + COMM_PCTG)) / 100) * (2.5 / 100)
                        WHEN NVL(CUST_PPH_TYPE, 1) = 1 AND NVL(CUST_PPH23_FLAG, 'N') = 'N' THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (COMM_ANTAR + COMM_PCTG)) / 100) * (3 / 100)
                        WHEN NVL(CUST_PPH_TYPE, 1) = 2 AND NVL(CUST_PPH23_FLAG, 'N') = 'Y' THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (COMM_ANTAR + COMM_PCTG)) / 100) * (2 / 100)
                        WHEN NVL(CUST_PPH_TYPE, 1) = 2 AND NVL(CUST_PPH23_FLAG, 'N') = 'N' THEN (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (COMM_ANTAR + COMM_PCTG)) / 100) * (4 / 100)
                        ELSE (((NVL(C.CNOTE_AMOUNT,0) - NVL(C.CNOTE_INSURANCE_VALUE,0)) * (COMM_ANTAR + COMM_PCTG)) / 100) * (2.5 / 100)
                        END AS PPH_AMOUNT,
                    F_GET_INFO_INVOICE(A.APICUST_CNOTE_NO, 1) AS INFO_INVOICE_1,
                    F_GET_INFO_INVOICE(A.APICUST_CNOTE_NO, 2) AS INFO_INVOICE_2,
                    F_GET_INVOICE_AMOUNT(A.APICUST_CNOTE_NO, 1) AS INVOICE_AMOUNT_1,
                    F_GET_INVOICE_AMOUNT(A.APICUST_CNOTE_NO, 7) AS INVOICE_AMOUNT_7,
                    F_GET_INVOICE_AMOUNT(A.APICUST_CNOTE_NO, 8) AS INVOICE_AMOUNT_8,
                    F_GET_INV_RAISE(A.APICUST_CNOTE_NO) AS INV_RAISE,
                    F_GET_INV_RAISE_DT(A.APICUST_CNOTE_NO) AS INV_RAISE_DATE,
                    APICUST_MERCHAN_ID AS APICUST_MERCHAN_ID2,
                    APICUST_NAME AS APICUST_NAME2,
                    CNOTE_NO
                FROM
                    CMS_APICUST_HYBRID A
                        JOIN CMS_CUST B ON HYBRID_CUST = B.CUST_ID AND HYBRID_BRANCH = B.CUST_BRANCH
                        JOIN CMS_CNOTE@DBS101 C ON APICUST_CNOTE_NO = CNOTE_NO
                        LEFT JOIN (
                        SELECT
                            CUST_BRANCH AS MARKETPLACE_BRANCH,
                            CUST_ID AS MARKETPLACE_ID,
                            CUST_NAME AS MARKETPLACE_NAME
                        FROM CMS_CUST
                        WHERE CUST_TYPE NOT IN ('995','996','997','994')
                    ) D ON MARKETPLACE_BRANCH = HYBRID_BRANCH AND MARKETPLACE_ID = APICUST_CUST_NO
                        LEFT JOIN (
                        SELECT
                            CNOTE_NO AS HAWB,
                            TO_CHAR(
                                    NVL(HS_BM_VAL1, 0) + NVL(HS_PPN_VAL1, 0) + NVL(HS_PPH_VAL1, 0) +
                                    NVL(HS_BM_VAL2, 0) + NVL(HS_PPN_VAL2, 0) + NVL(HS_PPH_VAL2, 0) +
                                    NVL(HS_BM_VAL3, 0) + NVL(HS_PPN_VAL3, 0) + NVL(HS_PPH_VAL3, 0) +
                                    NVL(HS_BM_VAL4, 0) + NVL(HS_PPN_VAL4, 0) + NVL(HS_PPH_VAL4, 0) +
                                    NVL(HS_BM_VAL5, 0) + NVL(HS_PPN_VAL5, 0) + NVL(HS_PPH_VAL5, 0)
                            ) || ';' ||
                            TO_CHAR(
                                    NVL(HS_PPN_VAL1, 0) + NVL(HS_PPN_VAL2, 0) + NVL(HS_PPN_VAL3, 0) +
                                    NVL(HS_PPN_VAL4, 0) + NVL(HS_PPN_VAL5, 0)
                            ) || ';' ||
                            TO_CHAR(
                                    NVL(HS_PPH_VAL1, 0) + NVL(HS_PPH_VAL2, 0) + NVL(HS_PPH_VAL3, 0) +
                                    NVL(HS_PPH_VAL4, 0) + NVL(HS_PPH_VAL5, 0)
                            ) || ';' ||
                            TO_CHAR(
                                    NVL(HS_BM_VAL1, 0) + NVL(HS_BM_VAL2, 0) + NVL(HS_BM_VAL3, 0) +
                                    NVL(HS_BM_VAL4, 0) + NVL(HS_BM_VAL5, 0)
                            ) || ';' ||
                            TO_CHAR(
                                    NVL(HS_BMTP_VALUE1, 0) + NVL(HS_BMTP_VALUE2, 0) + NVL(HS_BMTP_VALUE3, 0) +
                                    NVL(HS_BMTP_VALUE4, 0) + NVL(HS_BMTP_VALUE5, 0)
                            ) AS PAJAK
                        FROM REPJNE.CMS_CNOTE_CN23_HYBRID
                    ) E ON CNOTE_NO = HAWB
                    ${whereClause}
                  --  CNOTE_BRANCH_ID = :P_BRANCH
                 -- AND TRUNC(CNOTE_DATE) BETWEEN TO_DATE(:P_DATE1, 'DD-MON-RRRR') AND TO_DATE(:P_DATE2, 'DD-MON-RRRR')
                  AND CUST_TYPE IN ('995','996','997','994')
                  AND NVL((SELECT CUST_KP FROM REPJNE.ECONNOTE_CUST E2 WHERE CUST_BRANCH = CNOTE_BRANCH_ID AND B.CUST_ID = E2.CUST_ID AND CUST_KP = 'N'), 'N') = 'N'
                  AND NVL(C.CNOTE_CANCEL, 'N') = 'N'
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
            const folderPath = path.join(__dirname, `./${uuidv4()}`);
            // const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', {total: chunks.length, width: 20});

            let no = 1;
            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan CABTM');

                worksheet.addRow(['Branch:', branch === '0' ? 'ALL' : branch]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', user_id]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(11);
                headerRow.values = [
                    "NO",
                    "HYBRID_CUST",
                    "CUST_NAME",
                    "MARKETPLACE_ID",
                    "MARKETPLACE_NAME",
                    "APICUST_MERCHAN_ID",
                    "APICUST_NAME",
                    "CNOTE_NO",
                    "CNOTE_BRANCH_ID",
                    "CREATE_DATE",
                    "CNOTE_ORIGIN",
                    "CNOTE_DESTINATION",
                    "CNOTE_SERVICES_CODE",
                    "APICUST_INS_FLAG",
                    "CNOTE_WEIGHT",
                    "CNOTE_GOODS_VALUE",
                    "CNOTE_INSURANCE_VALUE",
                    "CNOTE_AMOUNT",
                    "NET_AMOUNT",
                    "COMMISSION",
                    "PPH_RATE",
                    "PPH_AMOUNT",
                    "INFO_INVOICE_1",
                    "INFO_INVOICE_2",
                    "INVOICE_AMOUNT_1",
                    "INVOICE_AMOUNT_7",
                    "INVOICE_AMOUNT_8",
                    "INV_RAISE",
                    "INV_RAISE_DATE",
                    "APICUST_MERCHAN_ID2",
                    "APICUST_NAME2",
                    "CNOTE_NO_1"
                ];

                const headerRowIndex = 10; // Baris 10
                let currentRowIndex = headerRowIndex + 1; // Baris 11
                worksheet.getRow(headerRowIndex).values = [
                    "NO",
                    "HYBRID_CUST",
                    "CUST_NAME",
                    "MARKETPLACE_ID",
                    "MARKETPLACE_NAME",
                    "APICUST_MERCHAN_ID",
                    "APICUST_NAME",
                    "CNOTE_NO",
                    "CNOTE_BRANCH_ID",
                    "CREATE_DATE",
                    "CNOTE_ORIGIN",
                    "CNOTE_DESTINATION",
                    "CNOTE_SERVICES_CODE",
                    "APICUST_INS_FLAG",
                    "CNOTE_WEIGHT",
                    "CNOTE_GOODS_VALUE",
                    "CNOTE_INSURANCE_VALUE",
                    "CNOTE_AMOUNT",
                    "NET_AMOUNT",
                    "COMMISSION",
                    "PPH_RATE",
                    "PPH_AMOUNT",
                    "INFO_INVOICE_1",
                    "INFO_INVOICE_2",
                    "INVOICE_AMOUNT_1",
                    "INVOICE_AMOUNT_7",
                    "INVOICE_AMOUNT_8",
                    "INV_RAISE",
                    "INV_RAISE_DATE",
                    "APICUST_MERCHAN_ID2",
                    "APICUST_NAME2",
                    "CNOTE_NO_1"
                ];

                chunk.forEach((row) => {
                    worksheet.getRow(currentRowIndex++).values = [no++, ...row];
                });

                const fileName = path.join(folderPath, `CAReport_${dateStr}_${user_id}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);

                console.log(`job id ${jobId}`);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId
                `;
                const updateValues = {
                    summary_file: i + 1, // Update the summary_file with the number of parts processed
                    jobId: jobId
                };
                const updateResult = await connectionUpdate.execute(updateQuery, updateValues);
                await connectionUpdate.commit();
                console.log("Update berhasil:", updateResult);

                // bar.tick();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `CABTHReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 1}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);
            console.log(zipFileName)

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, 'error_logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir);
                }
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify(params, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
            if (connectionUpdate) {
                await connectionUpdate.close();
            }
        }
    });
}
async function fetchDataAndExportToExcelRU({origin_awal, destination,services_code, froms, thrus, user_id, dateStr,jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (origin_awal !== '0') {
                whereClause += "AND  RT_CNOTE_ASLI_ORIGIN  like :origin_awal ";
                bindParams.origin_awal = origin_awal + '%';
            }

            if (destination !== '0') {
                whereClause += "and RT_CNOTE_DEST LIKE  :destination ";
                bindParams.destination = destination + '%';
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(RT_CRDATE_RT) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (services_code !== '0') {
                whereClause += "  AND RT_SERVICES_CODE LIKE :services_code ";  // ganti SERVICE_CODES jadi SERVICES_CODE
                bindParams.services_code = services_code + '%';
            }

            const result = await connection.execute(`SELECT RT_SEQ,
                                                            RT_CNOTE_NO,
                                                            RT_CRDATE_RT,
                                                            RT_CNOTE_ASLI,
                                                            RT_CNOTE_ASLI_ORIGIN,
                                                            RT_MANIFEST,
                                                            RT_MANIFEST_DATE,
                                                            RT_SERVICES_CODE,
                                                            RT_CNOTE_ORIGIN,
                                                            RT_CNOTE_DEST,
                                                            RT_CNOTE_DEST_NAME,
                                                            RT_CNOTE_QTY,
                                                            RT_CNOTE_WEIGHT,
                                                            RT_INS_DATE,
                                                            RT_INS_MODE
                                                     FROM V_OPS_RETURN_UNPAID   ${whereClause}`,
                bindParams
            );

            dataCount = result.rows.length;

            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }

            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format
            const folderPath = path.join(__dirname, `./${uuidv4()}`);
            // const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', {total: chunks.length, width: 20});

            let no = 1;
            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan RU');

                worksheet.addRow(['Origin:', origin_awal === '0' ? 'ALL' : origin_awal]);
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
                worksheet.addRow(['Service Code:', services_code === '0' ? 'ALL' : services_code]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', user_id]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(11);
                headerRow.values = [

                    "NO",
                    "RT_SEQ",
                    "RT_CNOTE_NO",
                    "RT_CRDATE_RT",
                    "RT_CNOTE_ASLI",
                    "RT_CNOTE_ASLI_ORIGIN",
                    "RT_MANIFEST",
                    "RT_MANIFEST_DATE",
                    "RT_SERVICES_CODE",
                    "RT_CNOTE_ORIGIN",
                    "RT_CNOTE_DEST",
                    "RT_CNOTE_DEST_NAME",
                    "RT_CNOTE_QTY",
                    "RT_CNOTE_WEIGHT",
                    "RT_INS_DATE",
                    "RT_INS_MODE"
                ];

                const headerRowIndex = 10; // Baris 10
                let currentRowIndex = headerRowIndex + 1; // Baris 11
                worksheet.getRow(headerRowIndex).values = [

                    "NO",
                    "RT_SEQ",
                    "RT_CNOTE_NO",
                    "RT_CRDATE_RT",
                    "RT_CNOTE_ASLI",
                    "RT_CNOTE_ASLI_ORIGIN",
                    "RT_MANIFEST",
                    "RT_MANIFEST_DATE",
                    "RT_SERVICES_CODE",
                    "RT_CNOTE_ORIGIN",
                    "RT_CNOTE_DEST",
                    "RT_CNOTE_DEST_NAME",
                    "RT_CNOTE_QTY",
                    "RT_CNOTE_WEIGHT",
                    "RT_INS_DATE",
                    "RT_INS_MODE"
                ];

                // worksheet.getColumn(13).numFmt = "m/d/yyyy h:mm:ss AM/PM";

                chunk.forEach((row) => {
                    console.log(row)
                    worksheet.getRow(currentRowIndex++).values = [no++, ...row];
                });

                const fileName = path.join(folderPath, `RUReport_${dateStr}_${user_id}_part${i + 1}.xlsx`);
                await workbook.xlsx.writeFile(fileName);

                console.log(`job id ${jobId}`);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId
                `;
                const updateValues = {
                    summary_file: i + 1, // Update the summary_file with the number of parts processed
                    jobId: jobId
                };
                await connection.execute(updateQuery, updateValues);
                await connection.commit();


                // bar.tick();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `RUReport_${user_id}_${dateStr}_${timeStr}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 1}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, 'error_logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir);
                }
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify(params, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}

async function fetchDataAndExportToExcelDBO({ branch_id, currency, services_code, froms, thrus, user_id, dateStr,jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};


            // Gunakan branch_id jika tidak '0'
            if (branch_id && branch_id !== '0') {
                whereClause += "AND BRANCH_ID = :branch_id ";
                bindParams.branch_id = branch_id;
            }

            // Gunakan currency jika tidak '0'
            if (currency && currency !== '0') {
                whereClause += "AND CURRENCY LIKE :currency || '%' ";
                bindParams.currency = currency;
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (services_code && services_code !== '0') {
                whereClause += "AND SERVICES_CODE = :services_code ";
                bindParams.services_code = services_code;
            }

            const result = await connection.execute(`
                        SELECT CNOTE_NO,
                               SERVICES_CODE,
                               QTY,
                               CURRENCY,
                               --WEIGHT,
                               CASE
                                   WHEN WEIGHT = 0 THEN 0
                                   WHEN WEIGHT < 1 THEN 1
                                   WHEN RPAD(REGEXP_SUBSTR(WEIGHT , '[[:digit:]]+$'),3,0) > 300 THEN CEIL(WEIGHT )
                                   ELSE FLOOR(WEIGHT )
                                   END WEIGHT,
                               ---DECODE(SERVICES_CODE,'INTL',((AMOUNT*CURRENCY_RATE)*0.7),'INT',((AMOUNT*CURRENCY_RATE)*0.7),MGMNT_FEE) AS MFEE,
                               COST_OPS MFEE,
                               AMOUNT,
                               CURRENCY_RATE
                        from CMS_COST_DELIVERY_V2 ${whereClause} AND CUST_NA IS NULL AND SUBSTR (CNOTE_NO, 1, 2) NOT IN ('FW', 'RT')`,
                bindParams
            );


            dataCount = result.rows.length;

            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }

            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format
            const folderPath = path.join(__dirname, `./${uuidv4()}`);
            // const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', {total: chunks.length, width: 20});

            let no = 1;
            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan DBO');

                worksheet.addRow(['Branch:', branch_id === '0' ? 'ALL' : branch_id]);
                worksheet.addRow(['Currency:', currency === '0' ? 'ALL' : currency]);
                worksheet.addRow(['Service Code:', services_code === '0' ? 'ALL' : services_code]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', user_id]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(11);
                headerRow.values = [
                    "NO",
                    "CNOTE_NO",
                    "SERVICES_CODE",
                    "QTY",
                    "CURRENCY",
                    "WEIGHT",
                    "MFEE",
                    "AMOUNT",
                    "CURRENCY_RATE"
                ];

                const headerRowIndex = 10; // Baris 10
                let currentRowIndex = headerRowIndex + 1; // Baris 11
                worksheet.getRow(headerRowIndex).values = [
                    "NO",
                    "CNOTE_NO",
                    "SERVICES_CODE",
                    "QTY",
                    "CURRENCY",
                    "WEIGHT",
                    "MFEE",
                    "AMOUNT",
                    "CURRENCY_RATE"
                ];

                worksheet.getColumn(7).numFmt = "#,##0"; // BIAYA PENERUS
                worksheet.getColumn(8).numFmt = "#,##0"; // BIAYA PENERUS

                chunk.forEach((row) => {
                    console.log(row)
                    worksheet.getRow(currentRowIndex++).values = [no++, ...row];
                });

                const fileName = path.join(folderPath, `DBOReport_${dateStr}_${user_id}_part${i + 1}.xlsx`);

                await workbook.xlsx.writeFile(fileName);

                console.log(`job id ${jobId}`);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId
                `;
                const updateValues = {
                    summary_file: i + 1, // Update the summary_file with the number of parts processed
                    jobId: jobId
                };
                await connection.execute(updateQuery, updateValues);
                await connection.commit();


                // bar.tick();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `DBOReport_${user_id}_${dateStr}_${timeStr}.zip`);

            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 1}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, 'error_logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir);
                }
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify(params, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}
async function fetchDataAndExportToExcelDBONA({ branch_id, currency, services_code, froms, thrus, user_id, dateStr,jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};


            // Gunakan branch_id jika tidak '0'
            if (branch_id && branch_id !== '0') {
                whereClause += "AND BRANCH_ID = :branch_id ";
                bindParams.branch_id = branch_id;
            }

            // Gunakan currency jika tidak '0'
            if (currency && currency !== '0') {
                whereClause += "AND CURRENCY LIKE :currency || '%' ";
                bindParams.currency = currency;
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (services_code && services_code !== '0') {
                whereClause += "AND SERVICES_CODE = :services_code ";
                bindParams.services_code = services_code;
            }

            const result = await connection.execute(`
                        SELECT CNOTE_NO,
                               SERVICES_CODE,
                               QTY,
                               CURRENCY,
                               --WEIGHT,
                               CASE
                                   WHEN WEIGHT = 0 THEN 0
                                   WHEN WEIGHT < 1 THEN 1
                                   WHEN RPAD(REGEXP_SUBSTR(WEIGHT , '[[:digit:]]+$'),3,0) > 300 THEN CEIL(WEIGHT )
                                   ELSE FLOOR(WEIGHT )
                                   END WEIGHT,
                               ---DECODE(SERVICES_CODE,'INTL',((AMOUNT*CURRENCY_RATE)*0.7),'INT',((AMOUNT*CURRENCY_RATE)*0.7),MGMNT_FEE) AS MFEE,
                               COST_OPS MFEE,
                               AMOUNT,
                               CURRENCY_RATE
                        from CMS_COST_DELIVERY_V2 ${whereClause} AND CUST_NA = 'Y' AND SUBSTR (CNOTE_NO, 1, 2) NOT IN ('FW', 'RT')`,
                bindParams
            );


            dataCount = result.rows.length;

            const chunkSize = 50000;
            const chunks = [];
            for (let i = 0; i < result.rows.length; i += chunkSize) {
                chunks.push(result.rows.slice(i, i + chunkSize));
            }

            const today = new Date();
            const dateStr = today.toISOString().split('T')[0];
            const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format
            const folderPath = path.join(__dirname, `./${uuidv4()}`);
            // const folderPath = path.join(__dirname, timeStr);
            if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath);
                console.log(`Folder ${dateStr} telah dibuat.`);
            }
            const bar = new ProgressBar(':bar :percent', {total: chunks.length, width: 20});

            let no = 1;
            // Loop through each chunk, create an Excel file, and save it
            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];

                const workbook = new ExcelJS.Workbook();
                const worksheet = workbook.addWorksheet('Data Laporan DBONA');

                worksheet.addRow(['Branch:', branch_id === '0' ? 'ALL' : branch_id]);
                worksheet.addRow(['Currency:', currency === '0' ? 'ALL' : currency]);
                worksheet.addRow(['Service Code:', services_code === '0' ? 'ALL' : services_code]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
                worksheet.addRow(['User Id:', user_id]);
                worksheet.addRow(['Jumlah Data:', chunk.length]);

                worksheet.addRow([]);

                const headerRow = worksheet.getRow(11);
                headerRow.values = [
                    "NO",
                    "CNOTE_NO",
                    "SERVICES_CODE",
                    "QTY",
                    "CURRENCY",
                    "WEIGHT",
                    "MFEE",
                    "AMOUNT",
                    "CURRENCY_RATE"
                ];

                const headerRowIndex = 10; // Baris 10
                let currentRowIndex = headerRowIndex + 1; // Baris 11
                worksheet.getRow(headerRowIndex).values = [
                    "NO",
                    "CNOTE_NO",
                    "SERVICES_CODE",
                    "QTY",
                    "CURRENCY",
                    "WEIGHT",
                    "MFEE",
                    "AMOUNT",
                    "CURRENCY_RATE"
                ];

                worksheet.getColumn(7).numFmt = "#,##0"; // BIAYA PENERUS
                worksheet.getColumn(8).numFmt = "#,##0"; // BIAYA PENERUS

                chunk.forEach((row) => {
                    console.log(row)
                    worksheet.getRow(currentRowIndex++).values = [no++, ...row];
                });

                const fileName = path.join(folderPath, `DBONAReport_${dateStr}_${user_id}_part${i + 1}.xlsx`);

                await workbook.xlsx.writeFile(fileName);

                console.log(`job id ${jobId}`);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId
                `;
                const updateValues = {
                    summary_file: i + 1, // Update the summary_file with the number of parts processed
                    jobId: jobId
                };
                await connection.execute(updateQuery, updateValues);
                await connection.commit();


                // bar.tick();

                console.log(`Data berhasil diekspor ke ${fileName}`);
            }

            const zipFileName = path.join(__dirname, 'file_download', `DBONAReport_${user_id}_${dateStr}_${timeStr}.zip`);

            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', {
                zlib: {level: 1}
            });

            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, {recursive: true});
            console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

            resolve({zipFileName, dataCount}); // Resolve with zip file name and data count

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, 'error_logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir);
                }
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify(params, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
            reject(err); // Reject if error occurs
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    });
}

// Define API endpoint with query parameters
app.get("/getreporttco", async (req, res) => {
    try {
        const {
            origin,
            destination,
            froms,
            thrus,
            user_id,
            branch_id,
            user_session,
        } = req.query;

        if (
            !origin ||
            !destination ||
            !froms ||
            !thrus ||
            !user_id ||
            !branch_id ||
            !user_session
        ) {
            return res
                .status(400)
                .json({ success: false, message: "Missing required parameters" });
        }

        // Estimasi jumlah data
        // const estimatedDataCount = await estimateDataCount({
        //     origin,
        //     destination,
        //     froms,
        //     thrus,
        //     user_id
        // });
        //
        // // Calculate the estimated time based on the benchmark
        // const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
        // const estimatedTimeMinutes =
        //     (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        // Add the job to the queue
        const job = await reportQueue.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            dateStr
        });

        const jsonData = {
            origin: origin,
            destination: destination,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            user_session: user_session,
            //estimatedDataCount: estimatedDataCount,
            //estimatedTimeMinutes: estimatedTimeMinutes,
            dateStr: dateStr,
            branch_id: branch_id,
        };
        const clobJson = JSON.stringify(jsonData);

        //estimatedDataCount: estimatedDataCount,
        //estimatedTimeMinutes: estimatedTimeMinutes,
        // const count_per_file = Math.ceil(estimatedDataCount / 50000);
        const connection = await oracledb.getConnection(config);

        const insertProcedure = `
    BEGIN
        DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(
            P_USER_LOGIN        => :user_name,
            P_NAME_FILE         => :name_file,
            P_DURATION          => :duration,
            P_NAMA_MODUL        => :category,
            P_TGL_SETTING       => :periode,
            P_STATUS            => :status,
            P_JOB_SERVER        => :job_server,
            P_COUNT_DATA        => :datacount,
            P_SETTING_PERPAGE   => :count_per_file,
            P_TOTAL_FILE        => :total_file,
            P_BRANCH            => :branch,
            P_LOG_JSON         => :log_json
        );
    END;
`;

        const insertValues = {
            user_name: user_id, // user_id sebagai USER_NAME
            name_file: "", // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: 0, // Estimasi waktu
            category: "TCO", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 50000,
            total_file: 0,
            branch: branch_id, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };

        let generatedQuery = insertProcedure;

        // Replace placeholders with actual values
        // generatedQuery = generatedQuery
        //     .replace(":user_name", `'${insertValues.user_name}'`)
        //     .replace(":name_file", `'${insertValues.name_file}'`)
        //     .replace(":duration", insertValues.duration)
        //     .replace(":category", `'${insertValues.category}'`)
        //     .replace(":periode", `'${insertValues.periode}'`)
        //     .replace(":status", `'${insertValues.status}'`)
        //     .replace(":job_server", `'${insertValues.job_server}'`)
        //     .replace(":datacount", insertValues.datacount)
        //     .replace(":count_per_file", insertValues.count_per_file)
        //     .replace(":total_file", insertValues.total_file)
        //     .replace(":branch", `'${insertValues.branch}'`)
        //     .replace(":log_json", `'${insertValues.log_json}'`);
        //
        // // Log the query to console
        // console.log("Generated SQL Query:");
        // console.log(generatedQuery);
        //
        // console.log(insertValues);
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        //
        // const insertQuery = `
        //     INSERT INTO CMS_COST_TRANSIT_V2_LOG (USER_NAME, NAME_FILE, DURATION, CATEGORY, PERIODE, STATUS,
        //                                          DOWNLOAD, CREATED_AT, ID_JOB_REDIS, DATACOUNT, COUNT_PER_FILE,
        //                                          SUMMARY_FILE, TOTAL_FILE, LOG_JSON)
        //     VALUES (:user_name, :name_file, :duration, :category, :periode, :status, :download, :created_at,
        //             :id_job, :datacount, :count_per_file, :summary_file, :total_file, :log_json)
        // `;
        // // Set values to be inserted
        // const insertValues = {
        //     id_job: job.id,
        //     user_name: user_id,  // user_id sebagai USER_NAME
        //     name_file: '',       // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
        //     duration: estimatedTimeMinutes.toFixed(2), // Estimasi waktu
        //     category: 'TCO',     // Kategori adalah TCI
        //     periode: `${froms} - ${thrus}`, // Rentang periode
        //     status: 'Process',   // Status awal adalah Pending
        //     download: 0,         // Belum diunduh, set download = 0
        //     created_at: new Date(), // Timestamp saat data dimasukkan,
        //     datacount: estimatedDataCount,
        //     total_file: count_per_file,
        //     summary_file: '0',
        //     count_per_file: 50000,
        //     log_json: clobJson
        // };
        // await connection.execute(insertQuery, insertValues);
        // await connection.commit();
        // res.status(200).json({
        //     success: true,
        //     message: 'Job added successfully, processing in the background.',
        //     jobId: job.id,
        //     estimatedDataCount: estimatedDataCount , // Send the estimated data count
        //     estimatedTimeMinutes: estimatedTimeMinutes.toFixed(2) // Estimated processing time in minutes
        // });
        const logFilePath = path.join(
            __dirname,
            "log_files",
            `JNE_REPORT_TCO_${job.id}.txt`
        );
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
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);


    } catch (err) {
        console.error("Error adding job to queue:", err + " " + err.stack + err.line);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
});
app.get("/getreporttci", async (req, res) => {
    try {
        const {
            origin,
            destination,
            froms,
            thrus,
            user_id,
            TM,
            branch_id,
            user_session,
        } = req.query;

        if (
            !origin ||
            !destination ||
            !froms ||
            !thrus ||
            !user_id ||
            !TM ||
            !user_session ||
            !branch_id
        ) {
            return res
                .status(400)
                .json({ success: false, message: "Missing required parameters" });
        }

        // Get the number of jobs that are waiting or active
        // const activeJobs = await reportQueueTCI.getJobs(['waiting', 'active']);
        //
        // // Check if the queue has more than 20 jobs
        // if (activeJobs.length >= 10) {
        //     return res.status(503).json({
        //         success: false,
        //         message: 'Antrian penuh, coba beberapa saat lagi.'
        //     });
        // }

        // Estimasi jumlah data

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        // Add the job to the queue
        const job = await reportQueueTCI.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            TM,
            user_session,
            dateStr,
        });

        const jsonData = {
            origin: origin,
            destination: destination,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            TM: TM,
            user_session: user_session,
            // estimatedDataCount: estimatedDataCount,
            // estimatedTimeMinutes: estimatedTimeMinutes,
            dateStr: dateStr,
            branch_id: branch_id,
        };

        const clobJson = JSON.stringify(jsonData);
        // const count_per_file = Math.ceil(estimatedDataCount / 50000);
        const connection = await oracledb.getConnection(config);

        const insertProcedure = `
    BEGIN
        DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(
            P_USER_LOGIN        => :user_name,
            P_NAME_FILE         => :name_file,
            P_DURATION          => :duration,
            P_NAMA_MODUL        => :category,
            P_TGL_SETTING       => :periode,
            P_STATUS            => :status,
            P_JOB_SERVER        => :job_server,
            P_COUNT_DATA        => :datacount,
            P_SETTING_PERPAGE   => :count_per_file,
            P_TOTAL_FILE        => :total_file,
            P_BRANCH            => :branch,
            P_LOG_JSON         => :log_json
        );
    END;
`;

        const insertValues = {
            user_name: user_id, // user_id sebagai USER_NAME
            name_file: "", // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: 0, // Estimasi waktu
            category: "TCI", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 50000,
            total_file: 0,
            branch: branch_id, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };

        // Generate and log the query

        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "log_files",
            `JNE_REPORT_TCI_${job.id}.txt`
        );
        const logMessage = `
            Job ID: ${job.id}
            Origin: ${origin}
            Destination: ${destination}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            TM: ${TM}
            Branch: ${branch_id}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }
        // http://10.8.2.48:8080/ords/f?p=101:78:17076041502424::NO::P78_USER:YASIQIN
        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
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
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
});



app.get("/getreportdci", async (req, res) => {
    try {
        const {
            origin,
            destination,
            froms,
            thrus,
            service,
            user_id,
            branch_id,
            user_session,
        } = req.query;

        if (
            !origin ||
            !destination ||
            !froms ||
            !thrus ||
            !user_id ||
            !service ||
            !branch_id ||
            !user_session
        ) {
            return res
                .status(400)
                .json({ success: false, message: "Missing required parameters" });
        }

        // Get the number of jobs that are waiting or active
        // const activeJobs = await reportQueueDCI.getJobs(['waiting', 'active']);
        //
        // // Check if the queue has more than 20 jobs
        // if (activeJobs.length >= 10) {
        //     return res.status(503).json({
        //         success: false,
        //         message: 'Antrian penuh, coba beberapa saat lagi.'
        //     });
        // }
        // Estimasi jumlah data

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        // Add the job to the queue
        const job = await reportQueueDCI.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            service,
            dateStr
            // queue: { name: 'reportDCI'}
        });

        const jsonData = {
            origin: origin,
            destination: destination,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            service: service,
            // estimatedDataCount: estimatedDataCount,
            // estimatedTimeMinutes: estimatedTimeMinutes,
            dateStr: dateStr,
            branch_id: branch_id,
            user_session: user_session,
        };

        const clobJson = JSON.stringify(jsonData);
        // const count_per_file = Math.ceil(estimatedDataCount / 50000);

        const connection = await oracledb.getConnection(config);

        const insertProcedure = `
    BEGIN
        DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(
            P_USER_LOGIN        => :user_name,
            P_NAME_FILE         => :name_file,
            P_DURATION          => :duration,
            P_NAMA_MODUL        => :category,
            P_TGL_SETTING       => :periode,
            P_STATUS            => :status,
            P_JOB_SERVER        => :job_server,
            P_COUNT_DATA        => :datacount,
            P_SETTING_PERPAGE   => :count_per_file,
            P_TOTAL_FILE        => :total_file,
            P_BRANCH            => :branch,
            P_LOG_JSON         => :log_json
        );
    END;
`;

        const insertValues = {
            user_name: user_id, // user_id sebagai USER_NAME
            name_file: "", // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: 0, // Estimasi waktu
            category: "DCI", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 50000,
            total_file: 0,
            branch: branch_id, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };

        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        //
        // const insertQuery = `
        //     INSERT INTO CMS_COST_TRANSIT_V2_LOG (USER_NAME, NAME_FILE, DURATION, CATEGORY, PERIODE, STATUS,
        //                                          DOWNLOAD, CREATED_AT, ID_JOB_REDIS, DATACOUNT, COUNT_PER_FILE,
        //                                          SUMMARY_FILE, TOTAL_FILE, LOG_JSON)
        //     VALUES (:user_name, :name_file, :duration, :category, :periode, :status, :download, :created_at,
        //             :id_job, :datacount, :count_per_file, :summary_file, :total_file, :log_json)
        // `;
        //
        // // Set values to be inserted
        // const insertValues = {
        //     id_job: job.id,
        //     user_name: user_id,  // user_id sebagai USER_NAME
        //     name_file: '',       // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
        //     duration: estimatedTimeMinutes.toFixed(2), // Estimasi waktu
        //     category: 'DCI',     // Kategori adalah TCI
        //     periode: `${froms} - ${thrus}`, // Rentang periode
        //     status: 'Process',   // Status awal adalah Pending
        //     download: 0,         // Belum diunduh, set download = 0
        //     created_at: new Date(), // Timestamp saat data dimasukkan,
        //     datacount: estimatedDataCount,
        //     total_file: count_per_file,
        //     summary_file: '0',
        //     count_per_file: 50000,
        //     log_json: clobJson
        //
        // };
        //
        // await connection.execute(insertQuery, insertValues);
        // await connection.commit();
        // res.status(200).json({
        //     success: true,
        //     message: 'Job added successfully, processing in the background.',
        //     jobId: job.id,
        //     estimatedDataCount: estimatedDataCount , // Send the estimated data count
        //     estimatedTimeMinutes: estimatedTimeMinutes.toFixed(2) // Estimated processing time in minutes
        // });
        const logFilePath = path.join(
            __dirname,
            "log_files",
            `JNE_REPORT_DCI_${job.id}.txt`
        );
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
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);

        // Write the log message to the file
        // fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:62:${user_session}::NO::P78_USER:${user_id}`;
        // res.redirect(redirectUrl);
        // Send the log file for download
        // res.download(logFilePath, (err) => {
        //     if (err) {
        //         console.error('Error downloading the log file:', err);
        //         res.status(500).send({
        //             success: false,
        //             message: 'An error occurred while downloading the log file.'
        //         });
        //     } else {
        //         console.log('Log file sent for download');
        //     }
        // });
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
});
app.get("/getreportdco", async (req, res) => {
    try {
        const {
            origin,
            destination,
            froms,
            thrus,
            service,
            user_id,
            branch_id,
            user_session,
        } = req.query;

        if (
            !origin ||
            !destination ||
            !froms ||
            !thrus ||
            !user_id ||
            !service ||
            !branch_id ||
            !user_session
        ) {
            return res
                .status(400)
                .json({ success: false, message: "Missing required parameters" });
        }

        // Estimasi jumlah data

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        // Add the job to the queue
        const job = await reportQueueDCO.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            service,
            dateStr,
        });

        const jsonData = {
            origin: origin,
            destination: destination,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            service: service,
            user_session: user_session,
            // estimatedDataCount: estimatedDataCount,
            // estimatedTimeMinutes: estimatedTimeMinutes,
            dateStr: dateStr,
            branch_id: branch_id,
        };

        const clobJson = JSON.stringify(jsonData);

        // const count_per_file = Math.ceil(estimatedDataCount / 50000);

        const connection = await oracledb.getConnection(config);

        const insertProcedure = `
    BEGIN
        DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(
            P_USER_LOGIN        => :user_name,
            P_NAME_FILE         => :name_file,
            P_DURATION          => :duration,
            P_NAMA_MODUL        => :category,
            P_TGL_SETTING       => :periode,
            P_STATUS            => :status,
            P_JOB_SERVER        => :job_server,
            P_COUNT_DATA        => :datacount,
            P_SETTING_PERPAGE   => :count_per_file,
            P_TOTAL_FILE        => :total_file,
            P_BRANCH            => :branch,
            P_LOG_JSON         => :log_json
        );
    END;
`;

        const insertValues = {
            user_name: user_id, // user_id sebagai USER_NAME
            name_file: "", // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: 0, // Estimasi waktu
            category: "DCO", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 50000,
            total_file: 0,
            branch: branch_id, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };


        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "log_files",
            `JNE_REPORT_DCO_${job.id}.txt`
        );
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
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
        // Write the log message to the file
        // fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:63:${user_session}::NO::P78_USER:${user_id}`;
        // res.redirect(redirectUrl);
        // Send the log file for download
        // res.download(logFilePath, (err) => {
        //     if (err) {
        //         console.error('Error downloading the log file:', err);
        //         res.status(500).send({
        //             success: false,
        //             message: 'An error occurred while downloading the log file.'
        //         });
        //     } else {
        //         console.log('Log file sent for download');
        //     }
        // });
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
});
app.get("/getreportca", async (req, res) => {
    try {
        const {
            branch,
            froms,
            thrus,
            user_id,
            user_session,
        } = req.query;

        if (
            !branch ||
            !froms ||
            !thrus ||
            !user_id ||
            !user_session
        ) {
            return res
                .status(400)
                .json({ success: false, message: "Missing required parameters" });
        }


        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        // Add the job to the queue
        const job = await reportQueueCA.add({
            branch,
            froms,
            thrus,
            user_id,
            dateStr,
        });

        const jsonData = {
            branch: branch,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            user_session: user_session,
            // estimatedDataCount: estimatedDataCount,
            // estimatedTimeMinutes: estimatedTimeMinutes,
            dateStr: dateStr
        };

        const clobJson = JSON.stringify(jsonData);

        // const count_per_file = Math.ceil(estimatedDataCount / 1);

        const connection = await oracledb.getConnection(config);

        const insertProcedure = `
    BEGIN
        DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(
            P_USER_LOGIN        => :user_name,
            P_NAME_FILE         => :name_file,
            P_DURATION          => :duration,
            P_NAMA_MODUL        => :category,
            P_TGL_SETTING       => :periode,
            P_STATUS            => :status,
            P_JOB_SERVER        => :job_server,
            P_COUNT_DATA        => :datacount,
            P_SETTING_PERPAGE   => :count_per_file,
            P_TOTAL_FILE        => :total_file,
            P_BRANCH            => :branch,
            P_LOG_JSON         => :log_json
        );
    END;
`;

        const insertValues = {
            user_name: user_id, // user_id sebagai USER_NAME
            name_file: "", // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: 0, // Estimasi waktu
            category: "CA", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 0,
            total_file: 0,
            branch: "", // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };

        // Generate and log the query

        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "log_files",
            `JNE_REPORT_CA_${job.id}.txt`
        );
        const logMessage = `
            Job ID: ${job.id}
            branch: ${branch}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
        // Write the log message to the file
        // fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:63:${user_session}::NO::P78_USER:${user_id}`;
        // res.redirect(redirectUrl);
        // Send the log file for download
        // res.download(logFilePath, (err) => {
        //     if (err) {
        //         console.error('Error downloading the log file:', err);
        //         res.status(500).send({
        //             success: false,
        //             message: 'An error occurred while downloading the log file.'
        //         });
        //     } else {
        //         console.log('Log file sent for download');
        //     }
        // });
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
});
app.get("/getreportru", async (req, res) => {
    try {
        const {
            origin_awal,
            destination,
            services_code,
            froms,
            thrus,
            user_id,
            branch_id,
            user_session,
        } = req.query;

        if (
            !origin_awal ||
            !destination ||
            !froms ||
            !thrus ||
            !user_id ||
            !services_code ||
            !branch_id ||
            !user_session
        ) {
            return res
                .status(400)
                .json({ success: false, message: "Missing required parameters" });
        }


        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        // Add the job to the queue
        const job = await reportQueueRU.add({
            origin_awal,
            destination,
            services_code,
            froms,
            thrus,
            user_id,
            dateStr,
        });

        const jsonData = {
            origin: origin_awal,
            destination: destination,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            service: services_code,
            user_session: user_session,
            // estimatedDataCount: estimatedDataCount,
            // estimatedTimeMinutes: estimatedTimeMinutes,
            dateStr: dateStr,
            branch_id: branch_id,
        };

        const clobJson = JSON.stringify(jsonData);

        // const count_per_file = Math.ceil(estimatedDataCount / 50000);

        const connection = await oracledb.getConnection(config);

        const insertProcedure = `
    BEGIN
        DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(
            P_USER_LOGIN        => :user_name,
            P_NAME_FILE         => :name_file,
            P_DURATION          => :duration,
            P_NAMA_MODUL        => :category,
            P_TGL_SETTING       => :periode,
            P_STATUS            => :status,
            P_JOB_SERVER        => :job_server,
            P_COUNT_DATA        => :datacount,
            P_SETTING_PERPAGE   => :count_per_file,
            P_TOTAL_FILE        => :total_file,
            P_BRANCH            => :branch,
            P_LOG_JSON         => :log_json
        );
    END;
`;

        const insertValues = {
            user_name: user_id, // user_id sebagai USER_NAME
            name_file: "", // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: 0, // Estimasi waktu
            category: "RU", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 50000,
            total_file: 0,
            branch: branch_id, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };

        // Replace placeholders directly with bind parameters
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "log_files",
            `JNE_REPORT_RU_${job.id}.txt`
        );
        const logMessage = `
            Job ID: ${job.id}
            Origin: ${origin_awal}
            Destination: ${destination}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            Service: ${services_code}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
});
app.get("/getreportdbo", async (req, res) => {
    try {
        const {
            branch_id,
            currency,
            services_code,
            froms,
            thrus,
            user_id,
            branch,
            user_session,
        } = req.query;

        if (
            !branch_id ||
            !currency ||
            !froms ||
            !thrus ||
            !user_id ||
            !services_code ||
            !branch ||
            !user_session
        ) {
            return res
                .status(400)
                .json({ success: false, message: "Missing required parameters" });
        }



        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        // Add the job to the queue
        const job = await reportQueueDBO.add({
            branch_id,
            currency,
            services_code,
            froms,
            thrus,
            user_id,
            dateStr
        });

        const jsonData = {
            branch_id: branch_id,
            currency: currency,
            services_code: services_code,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            user_session: user_session,
            // estimatedDataCount: estimatedDataCount,
            // estimatedTimeMinutes: estimatedTimeMinutes,
            dateStr: dateStr,
            branch: branch,
        };

        const clobJson = JSON.stringify(jsonData);

        // const count_per_file = Math.ceil(estimatedDataCount / 50000);

        const connection = await oracledb.getConnection(config);

        const insertProcedure = `
    BEGIN
        DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(
            P_USER_LOGIN        => :user_name,
            P_NAME_FILE         => :name_file,
            P_DURATION          => :duration,
            P_NAMA_MODUL        => :category,
            P_TGL_SETTING       => :periode,
            P_STATUS            => :status,
            P_JOB_SERVER        => :job_server,
            P_COUNT_DATA        => :datacount,
            P_SETTING_PERPAGE   => :count_per_file,
            P_TOTAL_FILE        => :total_file,
            P_BRANCH            => :branch,
            P_LOG_JSON         => :log_json
        );
    END;
`;

        const insertValues = {
            user_name: user_id, // user_id sebagai USER_NAME
            name_file: "", // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: 0, // Estimasi waktu
            category: "DBO", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 50000,
            total_file: 0,
            branch: branch, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };

        // Replace placeholders directly with bind parameters
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "log_files",
            `JNE_REPORT_DBO_${job.id}.txt`
        );
        const logMessage = `
            Job ID: ${job.id}
            branch_id: ${branch_id}
            currency: ${currency}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            Service: ${services_code}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
});
app.get("/getreportdbona", async (req, res) => {
    try {
        const {
            branch_id,
            currency,
            services_code,
            froms,
            thrus,
            user_id,
            branch,
            user_session,
        } = req.query;

        if (
            !branch_id ||
            !currency ||
            !froms ||
            !thrus ||
            !user_id ||
            !services_code ||
            !branch ||
            !user_session
        ) {
            return res
                .status(400)
                .json({ success: false, message: "Missing required parameters" });
        }


        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        // Add the job to the queue
        const job = await reportQueueDBONA.add({
            branch_id,
            currency,
            services_code,
            froms,
            thrus,
            user_id,
            dateStr
        });

        const jsonData = {
            branch_id: branch_id,
            currency: currency,
            services_code: services_code,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            user_session: user_session,
            // estimatedDataCount: estimatedDataCount,
            // estimatedTimeMinutes: estimatedTimeMinutes,
            dateStr: dateStr,
            branch: branch,
        };

        const clobJson = JSON.stringify(jsonData);

        // const count_per_file = Math.ceil(estimatedDataCount / 50000);

        const connection = await oracledb.getConnection(config);

        const insertProcedure = `
    BEGIN
        DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(
            P_USER_LOGIN        => :user_name,
            P_NAME_FILE         => :name_file,
            P_DURATION          => :duration,
            P_NAMA_MODUL        => :category,
            P_TGL_SETTING       => :periode,
            P_STATUS            => :status,
            P_JOB_SERVER        => :job_server,
            P_COUNT_DATA        => :datacount,
            P_SETTING_PERPAGE   => :count_per_file,
            P_TOTAL_FILE        => :total_file,
            P_BRANCH            => :branch,
            P_LOG_JSON         => :log_json
        );
    END;
`;

        const insertValues = {
            user_name: user_id, // user_id sebagai USER_NAME
            name_file: "", // Kosongkan terlebih dahulu, nanti akan diupdate setelah proses selesai
            duration: 0, // Estimasi waktu
            category: "DBONA", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 50000,
            total_file: 0,
            branch: branch, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };

        // Replace placeholders directly with bind parameters
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "log_files",
            `JNE_REPORT_DBONA_${job.id}.txt`
        );
        const logMessage = `
            Job ID: ${job.id}
            branch_id: ${branch_id}
            currency: ${currency}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            Service: ${services_code}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
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

app.get("/downloadtco/:jobId", async (req, res) => {
    const { jobId } = req.params; // Ambil jobId dari parameter URL
    const category = "TCO"; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {
        // Cari pekerjaan berdasarkan jobId di reportQueue

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
            category: category,
        });

        if (result.rows.length === 0) {
            return res
                .status(404)
                .send({ success: false, message: "File not found in the database." });
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res
                    .status(404)
                    .send({ success: false, message: "File not found." });
            }



            let connection_download;
            try {
                // Establish a connection to the database
                connection_download = await oracledb.getConnection(config);

                // Call the stored procedure P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED
                const result = await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    {
                        P_ID_REDIS: jobId, // Pass the jobId to the stored procedure
                        P_NAME_FILE: zipFileName.split("\\").pop(), // Extract the file name from the full path
                    }
                );

                // Commit the changes
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                // Ensure the connection is closed
                if (connection_download) {
                    await connection_download.close();
                }
            }
            // Update status download ke 1 (unduhan selesai) dan status ke Done
            // const updateQuery = `
            //           UPDATE CMS_COST_TRANSIT_V2_LOG
            //           SET DOWNLOAD = 1,
            //               STATUS   = 'Downloaded'
            //           WHERE ID_JOB_REDIS = :jobId
            //       `;
            // await connection.execute(updateQuery, {
            //   jobId: jobId,
            // });
            // await connection.commit();

            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res
                        .status(500)
                        .send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while processing the download.",
        });
    }
});
app.get("/downloadtci/:jobId", async (req, res) => {
    const { jobId } = req.params; // Ambil jobId dari parameter URL
    const category = "TCI"; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {
        // Cari pekerjaan berdasarkan jobId di reportQueue

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
            category: category,
        });

        if (result.rows.length === 0) {
            return res
                .status(404)
                .send({ success: false, message: "File not found in the database." });
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res
                    .status(404)
                    .send({ success: false, message: "File not found." });
            }


            let connection_download;
            try {
                // Establish a connection to the database
                connection_download = await oracledb.getConnection(config);

                // Call the stored procedure P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED
                const result = await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    {
                        P_ID_REDIS: jobId, // Pass the jobId to the stored procedure
                        P_NAME_FILE: zipFileName.split("\\").pop(), // Extract the file name from the full path
                    }
                );

                // Commit the changes
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                // Ensure the connection is closed
                if (connection_download) {
                    await connection_download.close();
                }
            }
            // Update status download ke 1 (unduhan selesai) dan status ke Done
            // const updateQuery = `
            //           UPDATE CMS_COST_TRANSIT_V2_LOG
            //           SET DOWNLOAD = 1,
            //               STATUS   = 'Downloaded'
            //           WHERE ID_JOB_REDIS = :jobId
            //       `;
            // await connection.execute(updateQuery, {
            //   jobId: jobId,
            // });
            // await connection.commit();

            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res
                        .status(500)
                        .send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while processing the download.",
        });
    }
});

app.get("/downloaddci/:jobId", async (req, res) => {
    const { jobId } = req.params; // Ambil jobId dari parameter URL
    const category = "DCI"; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {
        // Cari pekerjaan berdasarkan jobId di reportQueue

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
            category: category,
        });

        if (result.rows.length === 0) {
            return res
                .status(404)
                .send({ success: false, message: "File not found in the database." });
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res
                    .status(404)
                    .send({ success: false, message: "File not found." });
            }


            let connection_download;
            try {
                // Establish a connection to the database
                connection_download = await oracledb.getConnection(config);

                // Call the stored procedure P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED
                const result = await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    {
                        P_ID_REDIS: jobId, // Pass the jobId to the stored procedure
                        P_NAME_FILE: zipFileName.split("\\").pop(), // Extract the file name from the full path
                    }
                );

                // Commit the changes
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                // Ensure the connection is closed
                if (connection_download) {
                    await connection_download.close();
                }
            }
            // Update status download ke 1 (unduhan selesai) dan status ke Done
            // const updateQuery = `
            //           UPDATE CMS_COST_TRANSIT_V2_LOG
            //           SET DOWNLOAD = 1,
            //               STATUS   = 'Downloaded'
            //           WHERE ID_JOB_REDIS = :jobId
            //       `;
            // await connection.execute(updateQuery, {
            //   jobId: jobId,
            // });
            // await connection.commit();

            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res
                        .status(500)
                        .send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while processing the download.",
        });
    }
});
app.get("/downloaddco/:jobId", async (req, res) => {
    const { jobId } = req.params; // Ambil jobId dari parameter URL
    const category = "DCO"; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {


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
            category: category,
        });

        if (result.rows.length === 0) {
            return res
                .status(404)
                .send({ success: false, message: "File not found in the database." });
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res
                    .status(404)
                    .send({ success: false, message: "File not found." });
            }


            let connection_download;
            try {
                // Establish a connection to the database
                connection_download = await oracledb.getConnection(config);

                // Call the stored procedure P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED
                const result = await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    {
                        P_ID_REDIS: jobId, // Pass the jobId to the stored procedure
                        P_NAME_FILE: zipFileName.split("\\").pop(), // Extract the file name from the full path
                    }
                );

                // Commit the changes
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                // Ensure the connection is closed
                if (connection_download) {
                    await connection_download.close();
                }
            }
            // Update status download ke 1 (unduhan selesai) dan status ke Done
            // const updateQuery = `
            //           UPDATE CMS_COST_TRANSIT_V2_LOG
            //           SET DOWNLOAD = 1,
            //               STATUS   = 'Downloaded'
            //           WHERE ID_JOB_REDIS = :jobId
            //       `;
            // await connection.execute(updateQuery, {
            //   jobId: jobId,
            // });
            // await connection.commit();
            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res
                        .status(500)
                        .send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while processing the download.",
        });
    }
});
app.get("/downloadca/:jobId", async (req, res) => {
    const { jobId } = req.params; // Ambil jobId dari parameter URL
    const category = "CA"; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {
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
            category: category,
        });

        if (result.rows.length === 0) {
            return res
                .status(404)
                .send({ success: false, message: "File not found in the database." });
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res
                    .status(404)
                    .send({ success: false, message: "File not found." });
            }


            let connection_download;
            try {
                // Establish a connection to the database
                connection_download = await oracledb.getConnection(config);

                // Call the stored procedure P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED
                const result = await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    {
                        P_ID_REDIS: jobId, // Pass the jobId to the stored procedure
                        P_NAME_FILE: zipFileName.split("\\").pop(), // Extract the file name from the full path
                    }
                );

                // Commit the changes
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                // Ensure the connection is closed
                if (connection_download) {
                    await connection_download.close();
                }
            }
            // Update status download ke 1 (unduhan selesai) dan status ke Done
            // const updateQuery = `
            //           UPDATE CMS_COST_TRANSIT_V2_LOG
            //           SET DOWNLOAD = 1,
            //               STATUS   = 'Downloaded'
            //           WHERE ID_JOB_REDIS = :jobId
            //       `;
            // await connection.execute(updateQuery, {
            //   jobId: jobId,
            // });
            // await connection.commit();
            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res
                        .status(500)
                        .send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while processing the download.",
        });
    }
});
app.get("/downloadru/:jobId", async (req, res) => {
    const { jobId } = req.params; // Ambil jobId dari parameter URL
    const category = "RU"; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {


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
            category: category,
        });

        if (result.rows.length === 0) {
            return res
                .status(404)
                .send({ success: false, message: "File not found in the database." });
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res
                    .status(404)
                    .send({ success: false, message: "File not found." });
            }



            let connection_download;
            try {
                // Establish a connection to the database
                connection_download = await oracledb.getConnection(config);

                // Call the stored procedure P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED
                const result = await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    {
                        P_ID_REDIS: jobId, // Pass the jobId to the stored procedure
                        P_NAME_FILE: zipFileName.split("\\").pop(), // Extract the file name from the full path
                    }
                );

                // Commit the changes
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                // Ensure the connection is closed
                if (connection_download) {
                    await connection_download.close();
                }
            }
            // Update status download ke 1 (unduhan selesai) dan status ke Done
            // const updateQuery = `
            //           UPDATE CMS_COST_TRANSIT_V2_LOG
            //           SET DOWNLOAD = 1,
            //               STATUS   = 'Downloaded'
            //           WHERE ID_JOB_REDIS = :jobId
            //       `;
            // await connection.execute(updateQuery, {
            //   jobId: jobId,
            // });
            // await connection.commit();

            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res
                        .status(500)
                        .send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while processing the download.",
        });
    }
});
app.get("/downloaddbo/:jobId", async (req, res) => {
    const { jobId } = req.params; // Ambil jobId dari parameter URL
    const category = "DBO"; // Misalnya 'TCO', bisa disesuaikan sesuai kebutuhan

    try {

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
            category: category,
        });

        if (result.rows.length === 0) {
            return res
                .status(404)
                .send({ success: false, message: "File not found in the database." });
        }

        const zipFileName = result.rows[0][0]; // Ambil nama file dari hasil query

        // Tentukan path file zip
        const filePath = path.join(zipFileName);

        // Cek jika file zip sudah ada di direktori
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res
                    .status(404)
                    .send({ success: false, message: "File not found." });
            }



            let connection_download;
            try {
                // Establish a connection to the database
                connection_download = await oracledb.getConnection(config);

                // Call the stored procedure P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED
                const result = await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    {
                        P_ID_REDIS: jobId, // Pass the jobId to the stored procedure
                        P_NAME_FILE: zipFileName.split("\\").pop(), // Extract the file name from the full path
                    }
                );

                // Commit the changes
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                // Ensure the connection is closed
                if (connection_download) {
                    await connection_download.close();
                }
            }
            // Update status download ke 1 (unduhan selesai) dan status ke Done
            // const updateQuery = `
            //           UPDATE CMS_COST_TRANSIT_V2_LOG
            //           SET DOWNLOAD = 1,
            //               STATUS   = 'Downloaded'
            //           WHERE ID_JOB_REDIS = :jobId
            //       `;
            // await connection.execute(updateQuery, {
            //   jobId: jobId,
            // });
            // await connection.commit();

            // Serve the file for download
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res
                        .status(500)
                        .send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while processing the download.",
        });
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


app.get('/checkPendingJobs', async (req, res) => {
    try {
        const length = await redis.llen('pending_jobs');
        res.json({message: `There are ${length} jobs in the pending_jobs queue.`});
    } catch (error) {
        console.error('Error checking pending jobs length:', error);
        res.status(500).json({error: 'Failed to check pending jobs length'});
    }
});

// Route to get all pending jobs from the 'pending_jobs' queue
app.get('/getPendingJobs', async (req, res) => {
    try {
        const job = await redis.lpop('pending_jobs');  // Ambil job pertama dari antrian pending
        const jobData = jobs.map(job => JSON.parse(job));  // Parse the JSON data for each job
        res.json({pendingJobs: jobData});
    } catch (error) {
        console.error('Error retrieving pending jobs:', error);
        res.status(500).json({error: 'Failed to retrieve pending jobs'});
    }
});


app.get("/reruntco/:id", async (req, res) => {
    try {
        const { id } = req.params;  // Mengambil ID langsung dari parameter URL


        if (!id) {
            return res.status(400).json({
                success: false,
                message: "ID is required to rerun the job.",
            });
        }

        const connection = await oracledb.getConnection(config);

        // Ambil log_json dari CMS_COST_TRANSIT_V2_LOG berdasarkan id
        const result = await connection.execute(
            `SELECT log_json FROM CMS_COST_TRANSIT_V2_LOG WHERE id = :id`,
            [id]
        );

        console.log('result'+ result)
        if (result.rows.length === 0) {
            return res.status(404).json({
                success: false,
                message: "Job not found with the provided ID.",
            });
        }

        let logJson = result.rows[0][0];  // Ambil log_json dalam bentuk CLOB

        console.log('log :', logJson);

        // Jika CLOB, Anda perlu mengambil datanya dengan getData() atau menggunakan .toString()
        if (logJson && logJson.getData) {
            logJson = await new Promise((resolve, reject) => {
                logJson.getData((err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data.toString());  // CLOB menjadi string
                    }
                });
            });
        }

        console.log('logJson after getting data:', logJson);  // Cek apakah logJson sudah berisi data string

        // Jika logJson adalah string, lakukan parsing
        if (typeof logJson === 'string') {
            logJson = JSON.parse(logJson);  // Jika berupa string, lakukan parsing
        }

        const { origin, destination, froms, thrus, user_id, user_session, estimatedDataCount, estimatedTimeMinutes, dateStr, branch_id } = logJson;  // Ganti parsedJson menjadi logJson

        // Menambahkan job baru dengan data yang diambil dari log_json
        const job = await reportQueue.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            dateStr
        });

        const jsonData = {
            origin,
            destination,
            froms,
            thrus,
            user_id,
            user_session,
            estimatedDataCount,
            estimatedTimeMinutes,
            dateStr,
            branch_id,
        };
        const clobJson = JSON.stringify(jsonData);

        // Hitung jumlah file yang akan dihasilkan
        const count_per_file = Math.ceil(estimatedDataCount / 50000);
        const updateQuery = `
            UPDATE CMS_COST_TRANSIT_V2_LOG
            SET
                NAME_FILE = :name_file,
                DURATION = :duration,
                CATEGORY = :category,
                PERIODE = :periode,
                STATUS = :status,
                DOWNLOAD = :download,
                CREATED_AT = :created_at,
                ID_JOB_REDIS = :id_job,
                DATACOUNT = :datacount,
                COUNT_PER_FILE = :count_per_file,
                SUMMARY_FILE = :summary_file,
                TOTAL_FILE = :total_file,
                LOG_JSON = :log_json
            WHERE ID = :id
        `;

        const updateValues = {
            id: id,  // ID pekerjaan yang ingin diupdate
            name_file: '',  // Kosongkan nama file terlebih dahulu
            duration: estimatedTimeMinutes.toFixed(2),  // Estimasi waktu
            category: 'TCO',  // Kategori adalah TCO
            periode: `${froms} - ${thrus}`,  // Rentang periode
            status: 'Process',  // Status untuk pekerjaan yang sedang diproses
            download: 0,  // Belum diunduh
            created_at: new Date(),  // Timestamp saat data dimasukkan
            datacount: estimatedDataCount,  // Jumlah data yang diperkirakan
            total_file: count_per_file,  // Total file yang akan dibuat
            summary_file: '0',  // Status sementara untuk summary file
            count_per_file: 50000,  // Jumlah data per file
            log_json: clobJson,  // JSON yang berisi log pekerjaan
            id_job: job.id
        };

        await connection.execute(updateQuery, updateValues);
        await connection.commit();

        // Setelah insert, membuat file log dan menulis log
        const logFilePath = path.join(__dirname, "log_files", `JNE_REPORT_TCO_${job.id}.txt`);
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

        // Membuat direktori jika belum ada
        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        // Menulis log ke file
        fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // Redirect ke URL dengan user session
        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);

    } catch (err) {
        console.error("Error rerunning the job:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while rerunning the job.",
        });
    }
});
app.get("/reruntci/:id", async (req, res) => {
    try {
        const { id } = req.params;  // Mengambil ID langsung dari parameter URL


        if (!id) {
            return res.status(400).json({
                success: false,
                message: "ID is required to rerun the job.",
            });
        }

        const connection = await oracledb.getConnection(config);

        // Ambil log_json dari CMS_COST_TRANSIT_V2_LOG berdasarkan id
        const result = await connection.execute(
            `SELECT log_json FROM CMS_COST_TRANSIT_V2_LOG WHERE id = :id`,
            [id]
        );

        console.log('result'+ result)
        if (result.rows.length === 0) {
            return res.status(404).json({
                success: false,
                message: "Job not found with the provided ID.",
            });
        }

        let logJson = result.rows[0][0];  // Ambil log_json dalam bentuk CLOB

        console.log('log :', logJson);

        // Jika CLOB, Anda perlu mengambil datanya dengan getData() atau menggunakan .toString()
        if (logJson && logJson.getData) {
            logJson = await new Promise((resolve, reject) => {
                logJson.getData((err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data.toString());  // CLOB menjadi string
                    }
                });
            });
        }

        console.log('logJson after getting data:', logJson);  // Cek apakah logJson sudah berisi data string

        // Jika logJson adalah string, lakukan parsing
        if (typeof logJson === 'string') {
            logJson = JSON.parse(logJson);  // Jika berupa string, lakukan parsing
        }

        const { origin, destination, froms, thrus, user_id,TM, user_session, estimatedDataCount, estimatedTimeMinutes, dateStr, branch_id } = logJson;  // Ganti parsedJson menjadi logJson

        // Menambahkan job baru dengan data yang diambil dari log_json
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

        const jsonData = {
            origin,
            destination,
            froms,
            thrus,
            user_id,
            user_session,
            estimatedDataCount,
            estimatedTimeMinutes,
            dateStr,
            branch_id,
        };
        const clobJson = JSON.stringify(jsonData);

        // Hitung jumlah file yang akan dihasilkan
        const count_per_file = Math.ceil(estimatedDataCount / 50000);
        const updateQuery = `
            UPDATE CMS_COST_TRANSIT_V2_LOG
            SET
                NAME_FILE = :name_file,
                DURATION = :duration,
                CATEGORY = :category,
                PERIODE = :periode,
                STATUS = :status,
                DOWNLOAD = :download,
                CREATED_AT = :created_at,
                ID_JOB_REDIS = :id_job,
                DATACOUNT = :datacount,
                COUNT_PER_FILE = :count_per_file,
                SUMMARY_FILE = :summary_file,
                TOTAL_FILE = :total_file,
                LOG_JSON = :log_json
            WHERE ID = :id
        `;

        const updateValues = {
            id: id,  // ID pekerjaan yang ingin diupdate
            name_file: '',  // Kosongkan nama file terlebih dahulu
            duration: estimatedTimeMinutes,  // Estimasi waktu
            category: 'TCI',  // Kategori adalah TCO
            periode: `${froms} - ${thrus}`,  // Rentang periode
            status: 'Process',  // Status untuk pekerjaan yang sedang diproses
            download: 0,  // Belum diunduh
            created_at: new Date(),  // Timestamp saat data dimasukkan
            datacount: estimatedDataCount,  // Jumlah data yang diperkirakan
            total_file: count_per_file,  // Total file yang akan dibuat
            summary_file: '0',  // Status sementara untuk summary file
            count_per_file: 50000,  // Jumlah data per file
            log_json: clobJson,  // JSON yang berisi log pekerjaan
            id_job: job.id
        };

        await connection.execute(updateQuery, updateValues);
        await connection.commit();

        // Setelah insert, membuat file log dan menulis log
        const logFilePath = path.join(__dirname, "log_files", `JNE_REPORT_TCI_${job.id}.txt`);
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

        // Membuat direktori jika belum ada
        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        // Menulis log ke file
        fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // Redirect ke URL dengan user session
        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);

    } catch (err) {
        console.error("Error rerunning the job:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while rerunning the job.",
        });
    }
});
app.get("/rerundci/:id", async (req, res) => {
    try {
        const { id } = req.params;  // Mengambil ID langsung dari parameter URL


        if (!id) {
            return res.status(400).json({
                success: false,
                message: "ID is required to rerun the job.",
            });
        }

        const connection = await oracledb.getConnection(config);

        // Ambil log_json dari CMS_COST_TRANSIT_V2_LOG berdasarkan id
        const result = await connection.execute(
            `SELECT log_json FROM CMS_COST_TRANSIT_V2_LOG WHERE id = :id`,
            [id]
        );

        console.log('result'+ result)
        if (result.rows.length === 0) {
            return res.status(404).json({
                success: false,
                message: "Job not found with the provided ID.",
            });
        }

        let logJson = result.rows[0][0];  // Ambil log_json dalam bentuk CLOB

        console.log('log :', logJson);

        // Jika CLOB, Anda perlu mengambil datanya dengan getData() atau menggunakan .toString()
        if (logJson && logJson.getData) {
            logJson = await new Promise((resolve, reject) => {
                logJson.getData((err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data.toString());  // CLOB menjadi string
                    }
                });
            });
        }

        console.log('logJson after getting data:', logJson);  // Cek apakah logJson sudah berisi data string

        // Jika logJson adalah string, lakukan parsing
        if (typeof logJson === 'string') {
            logJson = JSON.parse(logJson);  // Jika berupa string, lakukan parsing
        }

        const { origin, destination, froms, thrus, user_id, service,user_session, estimatedDataCount, estimatedTimeMinutes, dateStr, branch_id } = logJson;  // Ganti parsedJson menjadi logJson

        // Menambahkan job baru dengan data yang diambil dari log_json
        const job = await reportQueueDCI.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            service,
            dateStr
        });

        const jsonData = {
            origin,
            destination,
            froms,
            thrus,
            user_id,
            user_session,
            estimatedDataCount,
            estimatedTimeMinutes,
            dateStr,
            branch_id,
        };
        const clobJson = JSON.stringify(jsonData);

        // Hitung jumlah file yang akan dihasilkan
        const count_per_file = Math.ceil(estimatedDataCount / 50000);
        const updateQuery = `
            UPDATE CMS_COST_TRANSIT_V2_LOG
            SET
                NAME_FILE = :name_file,
                DURATION = :duration,
                CATEGORY = :category,
                PERIODE = :periode,
                STATUS = :status,
                DOWNLOAD = :download,
                CREATED_AT = :created_at,
                ID_JOB_REDIS = :id_job,
                DATACOUNT = :datacount,
                COUNT_PER_FILE = :count_per_file,
                SUMMARY_FILE = :summary_file,
                TOTAL_FILE = :total_file,
                LOG_JSON = :log_json
            WHERE ID = :id
        `;

        const updateValues = {
            id: id,  // ID pekerjaan yang ingin diupdate
            name_file: '',  // Kosongkan nama file terlebih dahulu
            duration: estimatedTimeMinutes.toFixed(2),  // Estimasi waktu
            category: 'DCI',  // Kategori adalah TCO
            periode: `${froms} - ${thrus}`,  // Rentang periode
            status: 'Process',  // Status untuk pekerjaan yang sedang diproses
            download: 0,  // Belum diunduh
            created_at: new Date(),  // Timestamp saat data dimasukkan
            datacount: estimatedDataCount,  // Jumlah data yang diperkirakan
            total_file: count_per_file,  // Total file yang akan dibuat
            summary_file: '0',  // Status sementara untuk summary file
            count_per_file: 50000,  // Jumlah data per file
            log_json: clobJson,  // JSON yang berisi log pekerjaan
            id_job: job.id
        };

        await connection.execute(updateQuery, updateValues);
        await connection.commit();

        // Setelah insert, membuat file log dan menulis log
        const logFilePath = path.join(__dirname, "log_files", `JNE_REPORT_DCI_${job.id}.txt`);
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

        // Membuat direktori jika belum ada
        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        // Menulis log ke file
        fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // Redirect ke URL dengan user session
        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);

    } catch (err) {
        console.error("Error rerunning the job:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while rerunning the job.",
        });
    }
});
app.get("/rerundco/:id", async (req, res) => {
    try {
        const { id } = req.params;  // Mengambil ID langsung dari parameter URL


        if (!id) {
            return res.status(400).json({
                success: false,
                message: "ID is required to rerun the job.",
            });
        }

        const connection = await oracledb.getConnection(config);

        // Ambil log_json dari CMS_COST_TRANSIT_V2_LOG berdasarkan id
        const result = await connection.execute(
            `SELECT log_json FROM CMS_COST_TRANSIT_V2_LOG WHERE id = :id`,
            [id]
        );

        console.log('result'+ result)
        if (result.rows.length === 0) {
            return res.status(404).json({
                success: false,
                message: "Job not found with the provided ID.",
            });
        }

        let logJson = result.rows[0][0];  // Ambil log_json dalam bentuk CLOB

        console.log('log :', logJson);

        // Jika CLOB, Anda perlu mengambil datanya dengan getData() atau menggunakan .toString()
        if (logJson && logJson.getData) {
            logJson = await new Promise((resolve, reject) => {
                logJson.getData((err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data.toString());  // CLOB menjadi string
                    }
                });
            });
        }

        console.log('logJson after getting data:', logJson);  // Cek apakah logJson sudah berisi data string

        // Jika logJson adalah string, lakukan parsing
        if (typeof logJson === 'string') {
            logJson = JSON.parse(logJson);  // Jika berupa string, lakukan parsing
        }

        const { origin, destination, froms, thrus, user_id,service, user_session, estimatedDataCount, estimatedTimeMinutes, dateStr, branch_id } = logJson;  // Ganti parsedJson menjadi logJson

        // Menambahkan job baru dengan data yang diambil dari log_json
        const job = await reportQueueDCO.add({
            origin,
            destination,
            froms,
            thrus,
            user_id,
            service,
            dateStr
        });

        const jsonData = {
            origin,
            destination,
            froms,
            thrus,
            user_id,
            user_session,
            estimatedDataCount,
            estimatedTimeMinutes,
            dateStr,
            branch_id,
        };
        const clobJson = JSON.stringify(jsonData);

        // Hitung jumlah file yang akan dihasilkan
        const count_per_file = Math.ceil(estimatedDataCount / 50000);
        const updateQuery = `
            UPDATE CMS_COST_TRANSIT_V2_LOG
            SET
                NAME_FILE = :name_file,
                DURATION = :duration,
                CATEGORY = :category,
                PERIODE = :periode,
                STATUS = :status,
                DOWNLOAD = :download,
                CREATED_AT = :created_at,
                ID_JOB_REDIS = :id_job,
                DATACOUNT = :datacount,
                COUNT_PER_FILE = :count_per_file,
                SUMMARY_FILE = :summary_file,
                TOTAL_FILE = :total_file,
                LOG_JSON = :log_json
            WHERE ID = :id
        `;

        const updateValues = {
            id: id,  // ID pekerjaan yang ingin diupdate
            name_file: '',  // Kosongkan nama file terlebih dahulu
            duration: estimatedTimeMinutes.toFixed(2),  // Estimasi waktu
            category: 'DCO',  // Kategori adalah TCO
            periode: `${froms} - ${thrus}`,  // Rentang periode
            status: 'Process',  // Status untuk pekerjaan yang sedang diproses
            download: 0,  // Belum diunduh
            created_at: new Date(),  // Timestamp saat data dimasukkan
            datacount: estimatedDataCount,  // Jumlah data yang diperkirakan
            total_file: count_per_file,  // Total file yang akan dibuat
            summary_file: '0',  // Status sementara untuk summary file
            count_per_file: 50000,  // Jumlah data per file
            log_json: clobJson,  // JSON yang berisi log pekerjaan
            id_job: job.id
        };

        await connection.execute(updateQuery, updateValues);
        await connection.commit();

        // Setelah insert, membuat file log dan menulis log
        const logFilePath = path.join(__dirname, "log_files", `JNE_REPORT_DCO_${job.id}.txt`);
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

        // Membuat direktori jika belum ada
        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        // Menulis log ke file
        fs.writeFileSync(logFilePath, logMessage, 'utf8');

        // Redirect ke URL dengan user session
        const redirectUrl = `http://10.8.2.48:8080/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);

    } catch (err) {
        console.error("Error rerunning the job:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while rerunning the job.",
        });
    }
});
app.get("/clean", async (req, res) => {
    try {
        // Hapus semua job di reportQueue
        await reportQueue.clean(0, 'completed'); // Hapus job yang sudah selesai
        await reportQueue.clean(0, 'failed');    // Hapus job yang gagal
        console.log('Semua job di reportQueue telah dihapus');

        // Hapus semua job di reportQueueTCI
        await reportQueueTCI.clean(0, 'completed');
        await reportQueueTCI.clean(0, 'failed');
        console.log('Semua job di reportQueueTCI telah dihapus');

        // Hapus semua job di reportQueueDCI
        await reportQueueDCI.clean(0, 'completed');
        await reportQueueDCI.clean(0, 'failed');
        console.log('Semua job di reportQueueDCI telah dihapus');

        // Hapus semua job di reportQueueDCO
        await reportQueueDCO.clean(0, 'completed');
        await reportQueueDCO.clean(0, 'failed');
        console.log('Semua job di reportQueueDCO telah dihapus');
        // Hapus semua job di reportQueueDCO
        await reportQueueRU.clean(0, 'completed');
        await reportQueueRU.clean(0, 'failed');
        console.log('Semua job di reportQueueRU telah dihapus');
        // Hapus semua job di reportQueueDCO
        await reportQueueDBO.clean(0, 'completed');
        await reportQueueDBO.clean(0, 'failed');
        console.log('Semua job di reportQueueDBO telah dihapus');
        await reportQueueDBONA.clean(0, 'completed');
        await reportQueueDBONA.clean(0, 'failed');
        console.log('Semua job di reportQueueDBONA telah dihapus');
    } catch (error) {
        console.error('Terjadi kesalahan saat menghapus job:', error);
    }
});
// Start the server
app.listen(port, () => {
    console.log(`Server running at http://0.0.0.0:${port}`);
});


// }