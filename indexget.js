const express = require('express');
const oracledb = require('oracledb');  // Import oracledb untuk koneksi ke Oracle
const ExcelJS = require('exceljs');    // Import exceljs untuk ekspor ke Excel
const fs = require('fs');  // Untuk menulis file ke sistem
const path = require('path');  // Untuk memanipulasi path direktori
const archiver = require('archiver');  // Import archiver untuk zip file
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

async function fetchDataAndExportToExcel({ origin, destination, froms, thrus, user_id, TM, dateStr }) {
    let connection;

    try {
        // Membuka koneksi ke Oracle Database
        connection = await oracledb.getConnection(config);
        console.log("Koneksi berhasil ke database");

        // Query untuk mendapatkan nama pengguna berdasarkan user_id
        let userNameQuery = `SELECT USER_NAME FROM ORA_USER WHERE USER_ID = :user_id`;
        const userResult = await connection.execute(userNameQuery, [user_id]);
        let userName = userResult.rows.length > 0 ? userResult.rows[0][0] : 'Unknown'; // Jika tidak ada data, set 'Unknown'

        // Dynamically build the WHERE clause based on parameters
        let whereClause = "WHERE 1 = 1";  // Start with a condition that is always true
        const bindParams = {};  // Store bind parameters

        // Add origin filter if provided
        if (origin !== '0') {
            whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`;
            bindParams.origin = origin + '%';  // Concatenate '%' here, outside the SQL
        }

        // Add destination filter if provided
        if (destination !== '0') {
            whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`;
            bindParams.destination = destination + '%';  // Concatenate '%' here, outside the SQL
        }

        // Add date range filter if 'froms' and 'thrus' are provided
        if (froms !== '0' && thrus !== '0') {
            whereClause += ` AND AWB_DATE BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
            bindParams.froms = froms;
            bindParams.thrus = thrus;
        }

        // Add TM filter if provided
        if (TM !== '0') {
            whereClause += ` AND SUBSTR(ORIGIN_TM, 1, 3) = :TM`;
            bindParams.TM = TM;
        }

        // Now run the query with the dynamically built WHERE clause
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
                PRORATED_WEIGHT AS PRORATED_WEIGHT,
                SUM(TRANSIT_FEE) AS TRANSIT_FEE, -- Gunakan SUM
                SUM(HANDLING_FEE) AS HANDLING_FEE, -- Gunakan SUM
                SUM(OTHER_FEE) AS OTHER_FEE, -- Gunakan SUM
                SYSDATE AS DOWNLOAD_DATE
            FROM CMS_COST_TRANSIT_V2
                     ${whereClause}  -- Dynamically generated WHERE clause
                AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                AND CNOTE_WEIGHT > 0
            GROUP BY
                OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
        `, bindParams);

        console.log('Query Result:', result);

        // Chunking data into 1,000 rows per chunk
        const chunkSize = 100000;
        const chunks = [];
        for (let i = 0; i < result.rows.length; i += chunkSize) {
            chunks.push(result.rows.slice(i, i + chunkSize));
        }

        // Create folder with today's date
        const today = new Date();
        const timeStr = today.toISOString().split('T')[1].split('.')[0].replace(/:/g, ''); // Time in HHMMSS format
        const folderPath = path.join(__dirname, dateStr);

        // Cek jika folder tidak ada, maka buat folder baru
        if (!fs.existsSync(folderPath)) {
            fs.mkdirSync(folderPath);
            console.log(`Folder ${dateStr} telah dibuat.`);
        }

        // Loop through each chunk, create an Excel file, and save it
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];

            // Membuat workbook dan worksheet baru untuk setiap chunk
            const workbook = new ExcelJS.Workbook();
            const worksheet = workbook.addWorksheet('Data Laporan');

            // Add the header with additional information
            worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]);
            worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]);
            worksheet.addRow(['Branch TM:', TM]);  // Example static data
            worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
            worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
            worksheet.addRow(['User Id:', userName]);  // Display User Name based on user_id
            worksheet.addRow(['Jumlah Data:', chunk.length]);  // Add number of data rows

            worksheet.addRow([]);  // Empty row before the actual data

            const headerRow = worksheet.getRow(10);
            // Set the header row values
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

            // Menambahkan data ke worksheet dengan nomor urut
            let rowNumber = 1; // Start the row number at 1
            chunk.forEach((row) => {
                // Insert row number as the first column
                worksheet.addRow([rowNumber++, ...row]);  // Increment rowNumber for each row
            });

            // Menyimpan file Excel untuk setiap chunk ke dalam folder baru
            const fileName = path.join(folderPath, `TCIReport_${dateStr}_part${i + 1}.xlsx`);
            await workbook.xlsx.writeFile(fileName);
            console.log(`Data berhasil diekspor ke ${fileName}`);
        }

        // Membuat file zip dari folder yang baru dibuat
        const zipFileName = path.join(__dirname, `TCI_${dateStr}_${timeStr}.zip`);
        const output = fs.createWriteStream(zipFileName);
        const archive = archiver('zip', {
            zlib: { level: 9 } // Maksimal kompresi
        });

        archive.pipe(output);

        // Menambahkan folder yang berisi file Excel ke dalam zip
        archive.directory(folderPath, false);

        // Finalize dan simpan zip file
        await archive.finalize();
        console.log(`File berhasil di-zip menjadi ${zipFileName}`);

        // Menghapus folder setelah proses zip selesai
        fs.rmdirSync(folderPath, { recursive: true });
        console.log(`Folder ${folderPath} telah dihapus setelah di-zip`);

        return zipFileName;

    } catch (err) {
        console.error('Terjadi kesalahan:', err);
        throw err;
    } finally {
        if (connection) {
            // Menutup koneksi
            await connection.close();
        }
    }
}

// Define API endpoint with query parameters
app.get('/getreporttci', async (req, res) => {
    try {
        // Get parameters from request body
        const { origin, destination, froms, thrus,user_id, TM } = req.query;

        // Log the parameters to the console
        console.log('Request Parameters:', req.query);  // Logs all request body parameters
        console.log('Origin:', origin);  // Logs individual parameter
        console.log('Destination:', destination);  // Logs individual parameter
        console.log('TM:', TM);  // Logs individual parameter
        console.log('user:', user_id);  // Logs individual parameter

        // Validate parameters
        if (!origin || !destination || !froms || !thrus || !user_id || !TM) {
            return res.status(400).json({ success: false, message: 'Missing required parameters' });
        }

        const today = new Date();
        const dateStr = today.toISOString().split('T')[0];  // Get today's date in YYYY-MM-DD format
        const zipFilePath = await fetchDataAndExportToExcel({
            origin, destination, froms, thrus, user_id,TM, dateStr
        });

        // Send the generated zip file as a download response
        res.download(zipFilePath, (err) => {
            if (err) {
                console.error('Error downloading the file:', err);
            } else {
                // Delete the file after download
                fs.unlinkSync(zipFilePath);
            }
        });

    } catch (err) {
        console.error('Error generating report:', err);
        res.status(500).send({ success: false, message: 'An error occurred while generating the report.' });
    }
});

// Start the server
app.listen(port, '0.0.0.0', () => {
    console.log(`Server running at http://0.0.0.0:${port}`);
});
