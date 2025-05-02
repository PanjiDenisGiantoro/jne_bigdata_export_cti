    const express = require('express');
    const oracledb = require('oracledb');  // Import oracledb untuk koneksi ke Oracle
    const ExcelJS = require('exceljs');    // Import exceljs untuk ekspor ke Excel
    const fs = require('fs');  // Untuk menulis file ke sistem
    const path = require('path');  // Untuk memanipulasi path direktori
    const archiver = require('archiver');  // Import archiver untuk zip file
    const Bull = require('bull'); // Import Bull untuk job queue
    const { setQueues, BullAdapter } = require('bull-board');
    const app = express();
    const port = 3005;  // Port API
    const Sentry = require("@sentry/node");
    const {nodeProfilingIntegration} = require("@sentry/profiling-node");

    // Middleware to parse JSON bodies
    app.use(express.json());
    // Koneksi ke database Oracle
    const config = {
        user: 'dbctc_v2',
        password: 'dbctc123',
        connectString: '10.8.2.48:1521/ctcv2db'  // Host, port, dan service name
    };

    // Membuat Job Queue menggunakan Bull
    const reportQueue = new Bull('reportQueue', {
        redis: { host: '127.0.0.1', port: 6379 }
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
    setQueues([new BullAdapter(reportQueue)]);

    function logErrorToFile(jobId, origin, destination, userId, errorMessage) {
        const logFilePath = path.join(__dirname, 'error_logs.txt');
        const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId} | Origin: ${origin} | Destination: ${destination} | UserID: ${userId} | Error: ${errorMessage}\n`;

        fs.appendFile(logFilePath, logMessage, (err) => {
            if (err) {
                console.error('Error writing to log file:', err);
            }
        });
    }
    // Fungsi untuk memproses job di queue
    reportQueue.process(async (job) => {
        return await Sentry.startSpan({name: 'Process Report TCI Job' + job.id, jobId: job.id}, async (span) => {
            const {origin, destination, froms, thrus, user_id, TM, dateStr} = job.data;
            console.log('Processing job with data:', job.data);

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
                    TM,
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

                const connection = await oracledb.getConnection(config);
                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET DOWNLOAD  = 0,
                        STATUS    = 'Done',
                        NAME_FILE = :filename
                    WHERE ID_JOB_REDIS = :jobId
                `;

                // Prepare the update values
                const updateValues = {
                    filename: zipFileName.split('\\').pop(),  // Get the zip file name from the generated file path
                    jobId: job.id  // The job ID that we are processing
                };

                // Execute the update query
                await connection.execute(updateQuery, updateValues);
                await connection.commit();
                console.log(`Job status updated to 'Done' for job ID: ${job.id}`);

                return {
                    status: 'done',
                    zipFileName: zipFileName, // Add the file name to the return value
                    completionTime: completionTime, // Add the completion time
                    dataCount: dataCount,  // Number of records processed
                    elapsedTimeMinutes: elapsedTimeMinutes  // Processing time in minutes
                };
            } catch (error) {
                console.error('Error processing the job:', error);
                await Sentry.startSpan({name: 'Log Error to File'+ job.id, jobId: job.id}, async () => {

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
    });

    // Menggunakan Promise untuk estimasi jumlah data
    async function estimateDataCount({ origin, destination, froms, thrus, user_id, TM }) {
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

    async function fetchDataAndExportToExcel({ origin, destination, froms, thrus, user_id, TM, dateStr }) {
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
                    AWB_DATE AS CONNOTE_DATE,
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
                    CNOTE_WEIGHT AS WEIGHT_CONNOTE,
                    ACT_WEIGHT AS WEIGHT_BAG,
                    PRORATED_WEIGHT AS PRORATED_WEIGHT,
                    SUM(TRANSIT_FEE) AS TRANSIT_FEE, 
                    SUM(HANDLING_FEE) AS HANDLING_FEE, 
                    SUM(OTHER_FEE) AS OTHER_FEE, 
                    SYSDATE AS DOWNLOAD_DATE
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

                const chunkSize = 100000;
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
                        'DOWNLOAD_DATE'
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
                    zlib: { level: 9 }
                });

                archive.pipe(output);
                archive.directory(folderPath, false);
                await archive.finalize();

                fs.rmdirSync(folderPath, { recursive: true });
                console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

                resolve({ zipFileName, dataCount }); // Resolve with zip file name and data count

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
    app.get('/getreporttci', async (req, res) => {
        try {
            const { origin, destination, froms, thrus, user_id, TM } = req.query;

            if (!origin || !destination || !froms || !thrus || !user_id || !TM) {
                return res.status(400).json({ success: false, message: 'Missing required parameters' });
            }

            // Get the number of jobs that are waiting or active
            const activeJobs = await reportQueue.getJobs(['waiting', 'active']);

            // Check if the queue has more than 20 jobs
            if (activeJobs.length >= 10) {
                return res.status(503).json({
                    success: false,
                    message: 'Antrian penuh, coba beberapa saat lagi.'
                });
            }

            // Estimasi jumlah data
            const estimatedDataCount = await estimateDataCount({ origin, destination, froms, thrus, user_id, TM });

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
                TM,
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
            Estimated Data Count: ${estimatedDataCount}
            Estimated Time: ${estimatedTimeMinutes.toFixed(2)} minutes
            Status: Pending
            created_at: ${new Date()}
            Estimate_date: ${new Date(new Date().getTime() + estimatedTimeMinutes * 60 * 1000)}
        `;

            if (!fs.existsSync(path.dirname(logFilePath))) {
                fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
            }

            // Write the log message to the file
            fs.writeFileSync(logFilePath, logMessage, 'utf8');

            // Send the log file for download
            res.download(logFilePath, (err) => {
                if (err) {
                    console.error('Error downloading the log file:', err);
                    res.status(500).send({ success: false, message: 'An error occurred while downloading the log file.' });
                } else {
                    console.log('Log file sent for download');
                }
            });

        } catch (err) {
            console.error('Error adding job to queue:', err);
            res.status(500).send({ success: false, message: 'An error occurred while adding the job.' });
        }
    });


    app.get('/jobs', async (req, res) => {
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
            res.status(500).send({ success: false, message: 'Error fetching jobs.' });
        }
    });

    app.get('/jobs/:id', async (req, res) => {
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
                }else{
                  if (job.isWaiting()) {
                      statusfix = 'Pending';  // Job is waiting in the queue
                  }else{
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
                res.status(404).send({ success: false, message: 'Job not found.' });
            }
        } catch (err) {
            console.error('Error fetching job:', err);
            res.status(500).send({ success: false, message: 'Error fetching job.' });
        }
    });


    app.use('/file_download', express.static(path.join(__dirname, 'file_download')));
    app.get('/download/:jobId', async (req, res) => {
        const { jobId } = req.params;  // Ambil jobId dari parameter URL
        try {
            // Cari pekerjaan berdasarkan jobId di reportQueue
            const job = await reportQueue.getJob(jobId);

            if (!job) {
                return res.status(404).send({ success: false, message: 'Job not found.' });
            }

            // Ambil nama file zip yang terkait dengan pekerjaan
            const zipFileName = job.returnvalue?.zipFileName;

            if (!zipFileName) {
                return res.status(400).send({ success: false, message: 'File not available, please wait.' });
            }

            // Tentukan path file zip
            const filePath = path.join(__dirname, 'file_download', zipFileName.split('\\').pop());

            // Cek jika file zip sudah ada di direktori
            fs.stat(filePath, async (err, stats) => {
                if (err) {
                    return res.status(404).send({ success: false, message: 'File not found.' });
                }

                // Koneksi ke database untuk update status download
                const connection = await oracledb.getConnection(config);
                const updateQuery = `
                UPDATE CMS_COST_TRANSIT_V2_LOG
                SET DOWNLOAD = 1, STATUS = 'Download'
                WHERE ID_JOB_REDIS = :jobId
            `;

                // Update status download ke 1 (unduhan selesai) dan status ke Done
                await connection.execute(updateQuery, {
                    jobId: jobId,
                });
                await connection.commit();

                // Serve the file for download
                res.download(filePath, zipFileName.split('\\').pop(), (downloadErr) => {
                    if (downloadErr) {
                        return res.status(500).send({ success: false, message: 'Error downloading the file.' });
                    }

                    // After successful download, delete the file
                    fs.unlink(filePath, (unlinkErr) => {
                        if (unlinkErr) {
                            console.error('Error deleting the file:', unlinkErr);
                        } else {
                            console.log(`File ${zipFileName.split('\\').pop()} deleted after download.`);
                        }
                    });
                });
            });
        } catch (err) {
            console.error('Error fetching job data or handling download:', err);
            res.status(500).send({ success: false, message: 'An error occurred while processing the download.' });
        }
    });


    // Start the server
    app.listen(port, () => {
        console.log(`Server running at http://0.0.0.0:${port}`);
    });




