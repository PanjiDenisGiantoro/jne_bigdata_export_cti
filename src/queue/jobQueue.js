const Bull = require('bull');
const { setQueues, BullAdapter } = require('bull-board');
const Sentry = require("@sentry/node");
const { nodeProfilingIntegration } = require("@sentry/profiling-node");
const Redis = require('ioredis');
const redis = new Redis();
const oracledb = require('oracledb');
const config = require('../config/dbConfig').config;
const moment = require('moment');
const { logErrorToFile, logErrorToFileTCI, logErrorToFileDCI, logErrorToFileDCO, logErrorToFileCA, logErrorToFileRU, logErrorToFileDBO, logErrorToFileDBONA, logErrorToFileDBONASUM } = require('../log/errorLogger');
const { estimateDataCount, estimateDataCountTCI, estimateDataCountDCI, estimateDataCountDCO, estimateDataCountCA, estimateDataCountRU, estimateDataCountDBO, estimateDataCountDBONA, estimateDataCountMP } = require('../estimate/estimateDataCount');
const { fetchDataAndExportToExcel, fetchDataAndExportToExcelTCI, fetchDataAndExportToExcelDCI, fetchDataAndExportToExcelDCO, fetchDataAndExportToExcelCA, fetchDataAndExportToExcelCABTM, fetchDataAndExportToExcelRU, fetchDataAndExportToExcelDBO, fetchDataAndExportToExcelDBONA, fetchDataAndExportToExcelDBONASUM, fetchDataAndExportToExcelMP } = require('../export/fetchDataExport');

const reportQueue = new Bull('reportQueue', {
    redis: {host: '127.0.0.1', port: 6379},
    removeOnComplete: true // Job selesai langsung dihapus dari Redis
});
const reportQueue2 = new Bull('reportQueue2', {
    redis: { host: '127.0.0.1', port: 6379 },
    removeOnComplete: true
});
const reportQueue3 = new Bull('reportQueue3', {
    redis: { host: '127.0.0.1', port: 6379 },
    removeOnComplete: true
});
const reportQueue4 = new Bull('reportQueue4', {
    redis: { host: '127.0.0.1', port: 6379 },
    removeOnComplete: true
});
const reportQueue5 = new Bull('reportQueue5', {
    redis: { host: '127.0.0.1', port: 6379 },
    removeOnComplete: true
});
const reportQueue6 = new Bull('reportQueue6', {
    redis: { host: '127.0.0.1', port: 6379 },
    removeOnComplete: true
});
const reportQueue7 = new Bull('reportQueue7', {
    redis: { host: '127.0.0.1', port: 6379 },
    removeOnComplete: true
});
const reportQueue8 = new Bull('reportQueue8', {
    redis: { host: '127.0.0.1', port: 6379 },
    removeOnComplete: true
});
const reportQueue9 = new Bull('reportQueue9', {
    redis: { host: '127.0.0.1', port: 6379 },
    removeOnComplete: true
});
const reportQueue10 = new Bull('reportQueue10', {
    redis: { host: '127.0.0.1', port: 6379 },
    removeOnComplete: true
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
setQueues([new BullAdapter(reportQueue2)]);
setQueues([new BullAdapter(reportQueue3)]);
setQueues([new BullAdapter(reportQueue4)]);
setQueues([new BullAdapter(reportQueue5)]);
setQueues([new BullAdapter(reportQueue6)]);
setQueues([new BullAdapter(reportQueue7)]);
setQueues([new BullAdapter(reportQueue8)]);
setQueues([new BullAdapter(reportQueue9)]);
setQueues([new BullAdapter(reportQueue10)]);

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
        if (
            job.queue.name === 'reportQueue' ||
            job.queue.name === 'reportQueue2' ||
            job.queue.name === 'reportQueue3' ||
            job.queue.name === 'reportQueue4' ||
            job.queue.name === 'reportQueue5' ||
            job.queue.name === 'reportQueue6' ||
            job.queue.name === 'reportQueue7' ||
            job.queue.name === 'reportQueue8' ||
            job.queue.name === 'reportQueue9' ||
            job.queue.name === 'reportQueue10'
        ) {
            const { type } = job.data;
            if(type === 'tco'){
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
                        const count_per_file = Math.ceil(estimatedDataCount / 1000000);

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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
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
            }else if(type === 'tci') {

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
                        const count_per_file = Math.ceil(estimatedDataCount / 1000000);

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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
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

                        // Clear cache and buffer after the job is completed
                        // if (global.gc) {
                        //     global.gc();  // Memicu garbage collection untuk membersihkan cache dan buffer
                        //     console.log('Manual garbage collection triggered TCI');
                        // } else {
                        //     console.log('Garbage collection not exposed. Run Node.js with --expose-gc flag.');
                        // }

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
            }else if(type === 'dci'){
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
                        const count_per_file = Math.ceil(estimatedDataCount / 1000000);

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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
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

                        // if (global.gc) {
                        //     global.gc();  // Memicu garbage collection untuk membersihkan cache dan buffer
                        //     console.log('Manual garbage collection triggered DCI');
                        // } else {
                        //     console.log('Garbage collection not exposed. Run Node.js with --expose-gc flag.');
                        // }
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

            }else if(type ==='dco') {
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
                        const count_per_file = Math.ceil(estimatedDataCount / 1000000);

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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        // Calculate the elapsed time in minutes
                        elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
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


                        // if (global.gc) {
                        //     global.gc();  // Memicu garbage collection untuk membersihkan cache dan buffer
                        //     console.log('Manual garbage collection triggered DCO');
                        // } else {
                        //     console.log('Garbage collection not exposed. Run Node.js with --expose-gc flag.');
                        // }
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
            }else if(type === 'ca') {
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
                        const count_per_file = Math.ceil(estimatedDataCount / 1000000);

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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        // Calculate the elapsed time in minutes
                        elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
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
            }else if(type === 'ru') {
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
                        const count_per_file = Math.ceil(estimatedDataCount / 1000000);

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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        // Calculate the elapsed time in minutes
                        elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
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

                        // if (global.gc) {
                        //     global.gc();  // Memicu garbage collection untuk membersihkan cache dan buffer
                        //     console.log('Manual garbage collection triggered RU');
                        // } else {
                        //     console.log('Garbage collection not exposed. Run Node.js with --expose-gc flag.');
                        // }
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
            }else if(type === 'dbo') {
                await Sentry.startSpan({name: 'Process Report DBO Job' + job.id, jobId: job.id}, async (span) => {
                    const {  branch_id,froms, thrus, user_id, dateStr, jobId} = job.data;
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
                        const count_per_file = Math.ceil(estimatedDataCount / 1000000);

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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        // Calculate the elapsed time in minutes
                        elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
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
                        // if (global.gc) {
                        //     global.gc();  // Memicu garbage collection untuk membersihkan cache dan buffer
                        //     console.log('Manual garbage collection triggered DBO');
                        // } else {
                        //     console.log('Garbage collection not exposed. Run Node.js with --expose-gc flag.');
                        // }
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
                            logErrorToFileDBO(job.id,  branch_id, user_id, error.message);

                        });
                        Sentry.captureException(error);

                        return {
                            status: 'failed',
                            error: error.message
                        };
                    }
                });
            }else if(type === 'dbona') {
                await Sentry.startSpan({name: 'Process Report DBONA Job' + job.id, jobId: job.id}, async (span) => {
                    const { branch_id, froms, thrus, user_id, dateStr, jobId} = job.data;
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
                            froms,
                            thrus,
                            user_id
                        });

                        console.log('estimasi : '+estimatedDataCount)

                        // Calculate the estimated time based on the benchmark
                        const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
                        const estimatedTimeMinutes =
                            (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes

                        const today = new Date();
                        const dateStr = today.toISOString().split("T")[0];
                        const count_per_file = Math.ceil(estimatedDataCount / 1000000);

                        const connections = await oracledb.getConnection(config);
                        const estimateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET
                                START_PROCESS = SYSDATE,
                                DURATION   = :duration,
                                DATACOUNT  = :datacount,
                                TOTAL_FILE = :total_file
                            WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DBONA'
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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        // Calculate the elapsed time in minutes
                        elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
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

                        // if (global.gc) {
                        //     global.gc();  // Memicu garbage collection untuk membersihkan cache dan buffer
                        //     console.log('Manual garbage collection triggered DBONA');
                        // } else {
                        //     console.log('Garbage collection not exposed. Run Node.js with --expose-gc flag.');
                        // }
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
            }else if(type === 'dbonasum') {
                await Sentry.startSpan({name: 'Process Report DBONASUM Summary Job' + job.id, jobId: job.id}, async (span) => {
                    const { branch_id, froms, thrus, user_id, dateStr, jobId} = job.data;
                    console.log('Processing job with data:', job.data);
                    let zipFileName = '';
                    let completionTime = '';
                    let dataCount = 0;  // Variable to store the number of records processed
                    let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                    try {
                        // Capture the start time
                        const startTime = Date.now();

                        // const  estimatedDataCount = await estimateDataCountDBONASUM({
                        //     branch_id,
                        //     froms,
                        //     thrus,
                        //     user_id
                        // });

                        console.log('estimasi : '+ job.id);

                        // Calculate the estimated time based on the benchmark
                        const benchmarkRecordsPerMinute = 30000; // 60,000 records / 2 minutes
                        // const estimatedTimeMinutes =
                        //     (estimatedDataCount / benchmarkRecordsPerMinute) * 2; // Estimated time in minutes

                        const today = new Date();
                        const dateStr = today.toISOString().split("T")[0];
                        // const count_per_file = Math.ceil(estimatedDataCount / 1000000);

                        const connections = await oracledb.getConnection(config);
                        const estimateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET
                                START_PROCESS = SYSDATE,
                                DURATION   = :duration,
                                DATACOUNT  = :datacount,
                                TOTAL_FILE = :total_file
                            WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DBONASUM'
                        `;
                        const estimateValues = {
                            duration: '1', // Add the duration
                            datacount: '1',        // Add the data count
                            total_file: '1', // Add the total file count
                            jobId: job.id  // The job ID that we are processing
                        };
                        await connections.execute(estimateQuery, estimateValues);
                        await connections.commit();
                        console.log(`estimate data : ${job.id}`);

                        // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                        zipFileName = await fetchDataAndExportToExcelDBONASUM({
                            branch_id,
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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        // Calculate the elapsed time in minutes
                        elapsedTimeMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2); // Time in minutes

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
                                TRANSIT_V2_LOG_FLAG_DELETE = 'N',
                                SUMMARY_FILE = '1',
                                DATACOUNT  = '1',
                                TOTAL_FILE = '1'
                            WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'DBONASUM'
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

                        // if (global.gc) {
                        //     global.gc();  // Memicu garbage collection untuk membersihkan cache dan buffer
                        //     console.log('Manual garbage collection triggered DBONA');
                        // } else {
                        //     console.log('Garbage collection not exposed. Run Node.js with --expose-gc flag.');
                        // }
                        return {
                            status: 'done',
                            zipFileName: zipFileName, // Add the file name to the return value
                            completionTime: completionTime, // Add the completion time
                            dataCount: dataCount,  // Number of records processed
                            elapsedTimeMinutes: elapsedTimeMinutes // Processing time in minutes
                        };
                    } catch (error) {
                        console.error('Error processing the job:', error);
                        await Sentry.startSpan({name: 'Log Error to File' + job.id, jobId: job.id}, async () => {

                            // Log the error details to file
                            logErrorToFileDBONASUM(job.id,  branch_id,  user_id, error.message);

                        });
                        Sentry.captureException(error);

                        return {
                            status: 'failed',
                            error: error.message
                        };
                    }
                });
            }else if(type === 'mp') {
                await Sentry.startSpan({name: 'Process Job' + job.id, jobId: job.id}, async (span) => {
                    const {origin, destination, froms, thrus, user_id, dateStr, jobId} = job.data;
                    console.log('Processing job with data:', job.data);
                    let zipFileName = '';
                    let completionTime = '';
                    let dataCount = 0;  // Variable to store the number of records processed
                    let elapsedTimeMinutes = 0;  // Variable to store elapsed time in minutes

                    try {
                        const estimatedDataCount = await estimateDataCountMP({
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
                        const count_per_file = Math.ceil(estimatedDataCount / 1000000);

                        const connections = await oracledb.getConnection(config);
                        const estimateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET
                                START_PROCESS = SYSDATE,
                                DURATION   = :duration,
                                DATACOUNT  = :datacount,
                                TOTAL_FILE = :total_file
                            WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'MP'
                        `;
                        const estimateValues = {
                            duration: estimatedTimeMinutes,
                            datacount: estimatedDataCount,
                            total_file: count_per_file, // Add the total file count
                            jobId: job.id
                        };
                        await connections.execute(estimateQuery, estimateValues);
                        await connections.commit();
                        console.log(`estimate data : ${job.id}`);

                        // Capture the start time
                        const startTime = Date.now();

                        // Panggil fungsi fetchDataAndExportToExcel untuk menghasilkan laporan
                        zipFileName = await fetchDataAndExportToExcelMP({
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
                        const formattedDate = moment().format('DD/MM/YYYY hh:mm:ss A');  // Example: "05/08/2025 03:49:00 PM"

                        const connection = await oracledb.getConnection(config);
                        const updateQuery = `
                            UPDATE CMS_COST_TRANSIT_V2_LOG
                            SET DOWNLOAD   = 0,
                                STATUS     = 'Zipped',
                                NAME_FILE  = :filename,
                                UPDATED_AT = TO_TIMESTAMP(:updated_at, 'DD/MM/YYYY HH:MI:SS AM'),
                                TRANSIT_V2_LOG_FLAG_DELETE = 'N'
                            WHERE ID_JOB_REDIS = :jobId and CATEGORY = 'MP'
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
            }
        }else{
            console.log('error queue')
        }

    } catch (error) {
        console.error('Error processing job:', error);
        await redis.del('currentJobStatus');  // Lepaskan lock saat error
        Sentry.captureException(error);
    }
};

const processPendingJobs = async () => {
    const pendingJob = await redis.lpop('pending_jobs');
    console.log('Processing next pending jobs...' + pendingJob);

    if (pendingJob) {
        const jobData = JSON.parse(pendingJob);
        console.log('Processing next pending job...' + pendingJob);

        const queueToAdd = await getQueueToAddJob();
        await queueToAdd.add(jobData);
        console.log(`Job added to reportQueue with type: ${jobData.type}`);
    } else {
        console.log('No pending jobs.');
    }
};

reportQueue.process(async (job) => processJob(job));
reportQueue2.process(async (job) => processJob(job));
reportQueue3.process(async (job) => processJob(job));
reportQueue4.process(async (job) => processJob(job));
reportQueue5.process(async (job) => processJob(job));
reportQueue6.process(async (job) => processJob(job));
reportQueue7.process(async (job) => processJob(job));
reportQueue8.process(async (job) => processJob(job));
reportQueue9.process(async (job) => processJob(job));
reportQueue10.process(async (job) => processJob(job));

const getQueueToAddJob = async (branch_id) => {
    let selectedQueue;
    let activeJobs1, activeJobs2, activeJobs3, activeJobs4, activeJobs5, activeJobs6, activeJobs7, activeJobs8, activeJobs9, activeJobs10;

    if (branch_id === 'CGK000') {
        // Jika branch_id adalah CGK000, gunakan queue1, queue2, queue3
        activeJobs1 = await reportQueue.getJobs(['waiting', 'active']);
        activeJobs2 = await reportQueue2.getJobs(['waiting', 'active']);
        activeJobs3 = await reportQueue3.getJobs(['waiting', 'active']);
        // Menentukan queue dengan pekerjaan paling sedikit
        if (activeJobs1.length <= activeJobs2.length && activeJobs1.length <= activeJobs3.length) {
            selectedQueue = reportQueue;
        } else if (activeJobs2.length <= activeJobs1.length && activeJobs2.length <= activeJobs3.length) {
            selectedQueue = reportQueue2;
        } else {
            selectedQueue = reportQueue3;
        }
    } else {
        activeJobs4 = await reportQueue4.getJobs(['waiting', 'active']);
        activeJobs5 = await reportQueue5.getJobs(['waiting', 'active']);
        activeJobs6 = await reportQueue6.getJobs(['waiting', 'active']);
        activeJobs7 = await reportQueue7.getJobs(['waiting', 'active']);
        activeJobs8 = await reportQueue8.getJobs(['waiting', 'active']);
        activeJobs9 = await reportQueue9.getJobs(['waiting', 'active']);
        activeJobs10 = await reportQueue10.getJobs(['waiting', 'active']);

        // Menentukan queue dengan pekerjaan paling sedikit
        if (activeJobs4.length <= activeJobs5.length && activeJobs4.length <= activeJobs6.length && activeJobs4.length <= activeJobs7.length && activeJobs4.length <= activeJobs8.length && activeJobs4.length <= activeJobs9.length && activeJobs4.length <= activeJobs10.length) {
            selectedQueue = reportQueue4;
        } else if (activeJobs5.length <= activeJobs4.length && activeJobs5.length <= activeJobs6.length && activeJobs5.length <= activeJobs7.length && activeJobs5.length <= activeJobs8.length && activeJobs5.length <= activeJobs9.length && activeJobs5.length <= activeJobs10.length) {
            selectedQueue = reportQueue5;
        } else if (activeJobs6.length <= activeJobs4.length && activeJobs6.length <= activeJobs5.length && activeJobs6.length <= activeJobs7.length && activeJobs6.length <= activeJobs8.length && activeJobs6.length <= activeJobs9.length && activeJobs6.length <= activeJobs10.length) {
            selectedQueue = reportQueue6;
        } else if (activeJobs7.length <= activeJobs4.length && activeJobs7.length <= activeJobs5.length && activeJobs7.length <= activeJobs6.length && activeJobs7.length <= activeJobs8.length && activeJobs7.length <= activeJobs9.length && activeJobs7.length <= activeJobs10.length) {
            selectedQueue = reportQueue7;
        } else if (activeJobs8.length <= activeJobs4.length && activeJobs8.length <= activeJobs5.length && activeJobs8.length <= activeJobs6.length && activeJobs8.length <= activeJobs7.length && activeJobs8.length <= activeJobs9.length && activeJobs8.length <= activeJobs10.length) {
            selectedQueue = reportQueue8;
        } else if (activeJobs9.length <= activeJobs4.length && activeJobs9.length <= activeJobs5.length && activeJobs9.length <= activeJobs6.length && activeJobs9.length <= activeJobs7.length && activeJobs9.length <= activeJobs8.length && activeJobs9.length <= activeJobs10.length) {
            selectedQueue = reportQueue9;
        } else {
            selectedQueue = reportQueue10;
        }
    }

    // console.log("Selected Queue:", selectedQueue);  // Menambahkan log untuk melihat queue yang dipilih
    return selectedQueue;
};

const reportQueues = [
  reportQueue,
  reportQueue2,
  reportQueue3,
  reportQueue4,
  reportQueue5,
  reportQueue6,
  reportQueue7,
  reportQueue8,
  reportQueue9,
  reportQueue10
];

module.exports = {
  reportQueue,
  reportQueue2,
  reportQueue3,
  reportQueue4,
  reportQueue5,
  reportQueue6,
  reportQueue7,
  reportQueue8,
  reportQueue9,
  reportQueue10,
  reportQueues,
  getQueueToAddJob
};
