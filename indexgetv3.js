const express = require('express');
const oracledb = require('oracledb');  // Import oracledb untuk koneksi ke Oracle
const ExcelJS = require('exceljs');    // Import exceljs untuk ekspor ke Excel
const fs = require('fs');  // Untuk menulis file ke sistem
const path = require('path');  // Untuk memanipulasi path direktori
const archiver = require('archiver');  // Import archiver untuk zip file
const Bull = require('bull'); // Import Bull untuk job queue
const { setQueues, BullAdapter } = require('bull-board'); // Untuk memonitor job
const app = express();
const port = 3005;  // Port API

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

// Mendaftarkan queue untuk memonitor pekerjaan
setQueues([new BullAdapter(reportQueue)]);

// Fungsi untuk memproses job di queue
reportQueue.process(async (job) => {
    const { origin, destination, froms, thrus, user_id, TM, dateStr } = job.data;
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

        return {
            status: 'done',
            zipFileName: zipFileName, // Add the file name to the return value
            completionTime: completionTime, // Add the completion time
            dataCount: dataCount,  // Number of records processed
            elapsedTimeMinutes: elapsedTimeMinutes  // Processing time in minutes
        };
    } catch (error) {
        console.error('Error processing the job:', error);
        return {
            status: 'failed',
            error: error.message
        };
    }
});

async function estimateDataCount({ origin, destination, froms, thrus, user_id, TM }) {
    let connection;
    try {
        connection = await oracledb.getConnection(config);

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
            whereClause += ` AND AWB_DATE BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
            bindParams.froms = froms;
            bindParams.thrus = thrus;
        }

        if (TM !== '0') {
            whereClause += ` AND SUBSTR(ORIGIN_TM, 1, 3) = :TM`;
            bindParams.TM = TM;
        }

        // Query to get the estimated record count
        const result = await connection.execute(`
            SELECT COUNT(*) AS DATA_COUNT
            FROM CMS_COST_TRANSIT_V2
            ${whereClause}
            AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
            AND CNOTE_WEIGHT > 0
        `, bindParams);

        // Return the estimated data count
        return result.rows.length > 0 ? result.rows[0][0] : 0;

    } catch (err) {
        console.error('Error estimating data count:', err);
        throw err;
    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

async function fetchDataAndExportToExcel({ origin, destination, froms, thrus, user_id, TM, dateStr }) {
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
            whereClause += ` AND AWB_DATE BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
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

        // console.log('Query Result:', result);

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

        const zipFileName = path.join(__dirname, 'file_download', `${user_id}_${dateStr}_${timeStr}.zip`);
        const output = fs.createWriteStream(zipFileName);
        const archive = archiver('zip', {
            zlib: { level: 9 }
        });

        archive.pipe(output);
        archive.directory(folderPath, false);
        await archive.finalize();

        fs.rmdirSync(folderPath, { recursive: true });
        console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

        return { zipFileName, dataCount }; // Return both zipFileName and dataCount

    } catch (err) {
        console.error('Terjadi kesalahan:', err);
        throw err;
    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

// Define API endpoint with query parameters
app.get('/getreporttci', async (req, res) => {
    try {
        const { origin, destination, froms, thrus, user_id, TM } = req.query;

        if (!origin || !destination || !froms || !thrus || !user_id || !TM) {
            return res.status(400).json({ success: false, message: 'Missing required parameters' });
        }

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

        res.status(200).json({
            success: true,
            message: 'Job added successfully, processing in the background.',
            jobId: job.id,
            estimatedDataCount: estimatedDataCount ,// Send the estimated data count
            estimatedTimeMinutes: estimatedTimeMinutes.toFixed(2) // Estimated processing time in minutes

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

        // Prepare table headers
        const headers = ['ID', 'Origin', 'Destination', 'From Date', 'To Date', 'User ID', 'TM', 'Progress', 'Status', 'Error Reason'];

        // Format job data into a table structure
        const table = jobs.map((job) => {
            return {
                ID: job.id,
                Origin: job.data.origin,
                Destination: job.data.destination,
                FromDate: job.data.froms,
                ToDate: job.data.thrus,
                UserID: job.data.user_id,
                TM: job.data.TM,
                Progress: job.progress,
                Status: job.returnvalue ? job.returnvalue : 'Failed',
                ErrorReason: job.failedReason || '-'
            };
        });

        // Respond with table headers and data
        res.json({ success: true, tableHeaders: headers, tableData: table });

    } catch (err) {
        console.error('Error getting jobs:', err);
        res.status(500).send({ success: false, message: 'Error fetching jobs.' });
    }
});

app.get('/jobs/:id', async (req, res) => {
    try {
        const job = await reportQueue.getJob(req.params.id);
        if (job) {
            res.json({ success: true,
                id: job.id,
                Origin: job.data.origin,
                Destination: job.data.destination,
                FromDate: job.data.froms,
                ToDate: job.data.thrus,
                UserID: job.data.user_id,
                TM: job.data.TM,
                Status: job.returnvalue ? job.returnvalue : 'Failed',
                filenamezip : job.returnvalue,

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
// Endpoint to handle file download
app.get('/download/:filename', async (req, res) => {
    const { filename } = req.params;
    const filePath = path.join(__dirname, 'file_download', filename);

    // Check if the file exists
    fs.stat(filePath, (err, stats) => {
        if (err) {
            return res.status(404).send({ success: false, message: 'File not found.' });
        }

        // Serve the file for download
        res.download(filePath, filename, (downloadErr) => {
            if (downloadErr) {
                return res.status(500).send({ success: false, message: 'Error downloading the file.' });
            }

            // After successful download, delete the file
            fs.unlink(filePath, (unlinkErr) => {
                if (unlinkErr) {
                    console.error('Error deleting the file:', unlinkErr);
                } else {
                    console.log(`File ${filename} deleted after download.`);
                }
            });
        });
    });
});


// Start the server
app.listen(port, () => {
    console.log(`Server running at http://0.0.0.0:${port}`);
});
