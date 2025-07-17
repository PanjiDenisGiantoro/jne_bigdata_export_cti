const oracledb = require('oracledb');
const { config } = require('../config/dbConfig');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const ExcelJS = require('exceljs');
const archiver = require('archiver');
const { buildWhereClause } = require('../helper/whereClause');

function normalizeName(val) {
    if (!val || val === '0' || val === '%' || val === '') return 'All';
    return val;
}

async function fetchDataAndExportToExcel({origin, destination, froms, thrus, user_id, dateStr, jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            console.log('Menghubungkan ke database...');
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            // const { whereClause, bindParams } = await buildWhereClause(
            //     { origin, destination, froms, thrus }, 'TCO'
            // )

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

            console.log('Menjalankan query data...');

            const result = await connection.execute( `
                SELECT
                    '''' || AWB_NO AS CONNOTE_NUMBER,
                    TO_CHAR(AWB_DATE, 'DD/MM/YYYY') AS CONNOTE_DATE, -- Format tanggal
                    TO_CHAR(AWB_DATE, 'HH:MI:SS AM') AS TIME_CONNOTE_DATE, -- Format tanggal
                    CUST_ID,
                    CASE
                        WHEN CUST_ID = '80514305' THEN 'SHOPEE'
                        WHEN CUST_ID IN ('11666700', '80561600', '80561601') THEN 'TOKOPEDIA'
                        ELSE 'NON MP'
                    END AS MARKETPLACE,
                    SERVICES_CODE AS SERVICE_CONNOTE,
                    OUTBOND_MANIFEST_NO AS OUTBOND_MANIFEST_NUMBER,
                    TO_CHAR(OUTBOND_MANIFEST_DATE, 'DD/MM/YYYY') AS OUTBOND_MANIFEST_DATE, -- Format tanggal
                    TO_CHAR(OUTBOND_MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_OUTBOND_MANIFEST_DATE, -- Format tanggal
                    ORIGIN,
                    DESTINATION,
                    ZONA_DESTINATION,
                    OUTBOND_MANIFEST_ROUTE AS MANIFEST_ROUTE,
                    TRANSIT_MANIFEST_NO AS TRANSIT_MANIFEST_NUMBER, --F_GET_MANIFEST_OM_V4(BAG_NO) as TRANSIT_MANIFEST_NUMBER,
                    TO_CHAR(TRANSIT_MANIFEST_DATE, 'DD/MM/YYYY') AS TRANSIT_MANIFEST_DATE, -- Format tanggal
                    TO_CHAR(TRANSIT_MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_TRANSIT_MANIFEST_DATE, -- Format tanggal
                    TRANSIT_MANIFEST_ROUTE, --BAG_ROUTE 
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
                FROM CMS_COST_TRANSIT_V2 ${whereClause}
                    AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                AND CNOTE_WEIGHT > 0
                GROUP BY
                    ROWNUM, CUST_ID, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                    BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                    ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                    SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                `,
                bindParams
            );
            console.log('Query selesai, memproses data...');
            const rows = result.rows;
            const chunkSize = 1000000;
            const chunks = [];
            for (let i = 0; i < rows.length; i += chunkSize) {
                chunks.push(rows.slice(i, i + chunkSize));
            }
            console.log(`Data dibagi menjadi ${chunks.length} chunk.`);

            const dateNow = new Date();
            const dateString = dateNow.toISOString().split('T')[0];
            const folderPath = path.join(__dirname, uuidv4());
            if (!fs.existsSync(folderPath)) fs.mkdirSync(folderPath);

            console.log('Memulai proses ekspor Excel...');
            // Normalisasi origin dan destination untuk penamaan file
            const normOrigin = normalizeName(origin);
            const normDestination = normalizeName(destination);

            const baseFileNameTCO = `TCO Report - ${user_id} [${normOrigin} to ${normDestination}] [${froms} - ${thrus}]`;

            let no = 1;
            for (let i = 0; i < chunks.length; i++) {
                console.log(`Membuat file Excel untuk chunk ${i + 1}...`);
                const fileName = path.join(folderPath, `${baseFileNameTCO} Part ${i + 1}.xlsx`);
                const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: fileName });
                const worksheet = workbook.addWorksheet('Data Laporan TCO');

                const headers = [
                    "NO",
                    "CONNOTE NUMBER",
                    "CONNOTE DATE",
                    "TIME CONNOTE DATE",
                    "CUST ID",
                    "MARKETPLACE",
                    "SERVICE CONNOTE",
                    "OUTBOND MANIFEST NUMBER",
                    "OUTBOND MANIFEST DATE",
                    "TIME OUTBOND MANIFEST DATE",
                    "ORIGIN",
                    "DESTINATION",
                    "ZONA DESTINATION",
                    "MANIFEST ROUTE",
                    "TRANSIT MANIFEST NUMBER",
                    "TRANSIT MANIFEST DATE",
                    "TIME TRANSIT MANIFEST DATE",
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
                    "DOWNLOAD DATE"  ];

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]).commit();
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]).commit();
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]).commit();
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]).commit();
                worksheet.addRow(['User Id:', user_id]).commit();
                worksheet.addRow(['Jumlah Data:', chunks[i].length]).commit();
                worksheet.addRow([]).commit();
                worksheet.addRow(headers).commit();

                for (const row of chunks[i]) {
                    worksheet.addRow([no++, ...row]).commit();
                }

                await workbook.commit();
                console.log(`File berhasil dibuat: ${fileName}`);

                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category
                `;
                await connection.execute(updateQuery, {
                    summary_file: i + 1,
                    jobId: jobId,
                    category: 'TCO'
                });
                await connection.commit();
                console.log(`Database log diupdate untuk chunk ${i + 1}`);
            }

            console.log('Memulai proses zip file...');
            const zipFileName = path.join(__dirname, '../../file_download', `${baseFileNameTCO} ${jobId.substring(0, 5)}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', { zlib: { level: 1 } });
            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, { recursive: true });
            console.log(`Folder ${folderPath} telah dihapus setelah proses zip.`);
            console.log(`File zip berhasil dibuat: ${zipFileName}`);

            resolve({ zipFileName, dataCount: rows.length });
        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err);
        } finally {
            if (connection) await connection.close();
            console.log('Koneksi database ditutup.');
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
            console.log('Menghubungkan ke database...');
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            // const { whereClause, bindParams } = await buildWhereClause(
            //     { origin, destination, froms, thrus, TM }, 'TCI'
            // )

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (origin !== '0' ) {
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
            console.log('Menjalankan query data...');
            const result = await connection.execute( `
                SELECT '''' || AWB_NO AS CONNOTE_NUMBER,
                    -- AWB_DATE AS CONNOTE_DATE,
                    TO_CHAR(AWB_DATE, 'DD/MM/YYYY') AS AWB_DATE, -- Format tanggal
                    TO_CHAR(AWB_DATE, 'HH:MI:SS AM') AS TIME_AWB_DATE, -- Format tanggal
                    CUST_ID,
                    CASE
                        WHEN CUST_ID = '80514305' THEN 'SHOPEE'
                        WHEN CUST_ID IN ('11666700', '80561600', '80561601') THEN 'TOKOPEDIA'
                        ELSE 'NON MP'
                    END AS MARKETPLACE,
                    SERVICES_CODE AS SERVICE_CONNOTE,
                    OUTBOND_MANIFEST_NO AS OUTBOND_MANIFEST_NUMBER,
                    TO_CHAR(OUTBOND_MANIFEST_DATE, 'DD/MM/YYYY') AS OUTBOND_MANIFEST_DATE, -- Format tanggal
                    TO_CHAR(OUTBOND_MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_OUTBOND_MANIFEST_DATE, -- Format tanggal
                    ORIGIN,
                    DESTINATION,
                    ZONA_DESTINATION,
                    OUTBOND_MANIFEST_ROUTE AS MANIFEST_ROUTE,
                    TRANSIT_MANIFEST_NO AS TRANSIT_MANIFEST_NUMBER,
                    -- F_GET_MANIFEST_OM_V4(BAG_NO) as TRANSIT_MANIFEST_NUMBER,
                    TO_CHAR(TRANSIT_MANIFEST_DATE, 'DD/MM/YYYY') AS TRANSIT_MANIFEST_DATE, -- Format tanggal
                    TO_CHAR(TRANSIT_MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_TRANSIT_MANIFEST_DATE, -- Format tanggal
                    TRANSIT_MANIFEST_ROUTE,
                    SMU_NUMBER,
                    FLIGHT_NUMBER,
                    BRANCH_TRANSPORTER,
                    '''' || BAG_NO AS BAG_NUMBER,
                    SERVICE_BAG,
                    MODA,
                    MODA_TYPE,
                    round(CNOTE_WEIGHT, 3) AS WEIGHT_CONNOTE,
                    round(ACT_WEIGHT, 3) AS WEIGHT_BAG,
                    round(PRORATED_WEIGHT, 3) AS PRORATED_WEIGHT,
                    SUM(TRANSIT_FEE) AS TRANSIT_FEE,
                    SUM(HANDLING_FEE) AS HANDLING_FEE,
                    SUM(OTHER_FEE) AS OTHER_FEE,
                    SUM(NVL(TRANSIT_FEE, 0) + NVL(HANDLING_FEE, 0) + NVL(OTHER_FEE, 0)) AS TOTAL,
                    TO_CHAR(SYSDATE  , 'DD/MM/YYYY') AS DOWNLOAD_DATE,
                    TO_CHAR(SYSDATE  , 'HH:MI:SS AM') AS TIME_DOWNLOAD_DATE
                FROM CMS_COST_TRANSIT_V2 ${whereClause} AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                    AND CNOTE_WEIGHT > 0
                        GROUP BY CUST_ID,
                        OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                        BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                        ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                        SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                `,
                bindParams
            );
            console.log('Query selesai, memproses data...');
            const rows = result.rows;
            const chunkSize = 1000000;
            const chunks = [];
            for (let i = 0; i < rows.length; i += chunkSize) {
                chunks.push(rows.slice(i, i + chunkSize));
            }
            console.log(`Data dibagi menjadi ${chunks.length} chunk.`);

            const dateNow = new Date();
            const dateString = dateNow.toISOString().split('T')[0];
            const folderPath = path.join(__dirname, uuidv4());
            if (!fs.existsSync(folderPath)) fs.mkdirSync(folderPath);

            console.log('Memulai proses ekspor Excel...');
            // Normalisasi origin dan destination untuk penamaan file
            const normOrigin = normalizeName(origin);
            const normDestination = normalizeName(destination);

            const baseFileNameTCI = `TCI Report - ${user_id} [${normOrigin} to ${normDestination}] [${froms} - ${thrus}]`;

            let no = 1;
            for (let i = 0; i < chunks.length; i++) {
                console.log(`Membuat file Excel untuk chunk ${i + 1}...`);
                const fileName = path.join(folderPath, `${baseFileNameTCI} Part ${i + 1}.xlsx`);
                const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: fileName });
                const worksheet = workbook.addWorksheet('Data Laporan TCI');

                const headers = [
                    "NO",
                    "CONNOTE NUMBER",
                    "CONNOTE DATE",
                    "TIME CONNOTE DATE",
                    "CUST ID",
                    "MARKETPLACE",
                    "SERVICE CONNOTE",
                    "OUTBOND MANIFEST NUMBER",
                    "OUTBOND MANIFEST DATE",
                    "TIME OUTBOND MANIFEST DATE",
                    "ORIGIN",
                    "DESTINATION",
                    "ZONA DESTINATION",
                    "MANIFEST ROUTE",
                    "TRANSIT MANIFEST NUMBER",
                    "TRANSIT MANIFEST DATE",
                    "TIME TRANSIT MANIFEST DATE",
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
                    "TIME DOWNLOAD DATE"
                ];

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]).commit();
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]).commit();
                worksheet.addRow(["Branch:", TM === "0" ? "ALL" : TM]);
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]).commit();
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]).commit();
                worksheet.addRow(['User Id:', user_id]).commit();
                worksheet.addRow(['Jumlah Data:', chunks[i].length]).commit();
                worksheet.addRow([]).commit();
                worksheet.addRow(headers).commit();

                for (const row of chunks[i]) {
                    worksheet.addRow([no++, ...row]).commit();
                }

                await workbook.commit();
                console.log(`File berhasil dibuat: ${fileName}`);

                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category
                `;
                await connection.execute(updateQuery, {
                    summary_file: i + 1,
                    jobId: jobId,
                    category: 'TCI'
                });
                await connection.commit();
                console.log(`Database log diupdate untuk chunk ${i + 1}`);
            }

            console.log('Memulai proses zip file...');
            const zipFileName = path.join(__dirname, '../../file_download', `${baseFileNameTCI} ${jobId.substring(0, 5)}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', { zlib: { level: 1 } });
            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, { recursive: true });
            console.log(`Folder ${folderPath} telah dihapus setelah proses zip.`);
            console.log(`File zip berhasil dibuat: ${zipFileName}`);

            resolve({ zipFileName, dataCount: rows.length });
        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err);
        } finally {
            if (connection) await connection.close();
            console.log('Koneksi database ditutup.');
        }
    });
}

async function fetchDataAndExportToExcelDCI({ origin, destination, froms, thrus, service, user_id, dateStr, jobId }) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            console.log('Menghubungkan ke database...');
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            // const { whereClause, bindParams } = await buildWhereClause(
            //     { origin, destination, froms, thrus, service }, 'DCI'
            // )

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (origin !== '0') {
                whereClause += ` AND SUBSTR(ORIGIN, 1, 3) LIKE SUBSTR(:origin , 1, 3)`;
                bindParams.origin = origin + '%';
            }

            if (destination !== '0') {
                whereClause += ` AND SUBSTR(DESTINATION,1,3) LIKE SUBSTR(:destination,1,3)`;
                bindParams.destination = destination + '%';
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += ` AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            if (service !== '0' ) {
                whereClause += ` AND SERVICES_CODE = :service`;
                bindParams.service = service ;
            }

            console.log('Menjalankan query data...');
            const result = await connection.execute(`
                SELECT '''' || CNOTE_NO AS CNOTE_NO,
                    TO_CHAR(CNOTE_DATE, 'DD/MM/YYYY') AS CNOTE_DATE,
                    TO_CHAR(CNOTE_DATE, 'HH:MI:SS AM') AS TIME_CNOTE_DATE,
                    ORIGIN,
                    DESTINATION,
                    ZONA_DESTINATION,
                    SERVICES_CODE,
                    NVL(QTY, 0) AS QTY,
                       CASE
                           WHEN WEIGHT = 0 THEN 0
                           WHEN WEIGHT < 1 THEN 1
                           WHEN RPAD(REGEXP_SUBSTR(WEIGHT, '[[:digit:]]+$'), 3, 0) > 300 THEN CEIL(WEIGHT)
                           ELSE FLOOR(WEIGHT)
                           END                                             WEIGHT,
                    NVL(AMOUNT, 0) AS AMOUNT,
                    MANIFEST_NO,
                    TO_CHAR(MANIFEST_DATE, 'DD/MM/YYYY') AS MANIFEST_DATE,
                    TO_CHAR(MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_MANIFEST_DATE,
                    NVL(DELIVERY, 0) AS DELIVERY,
                    NVL(DELIVERY_SPS, 0) AS DELIVERY_SPS,
                    NVL(TRANSIT, 0) AS BIAYA_TRANSIT,
                    NVL(LINEHAUL_FIRST, 0) AS LINEHAUL_FIRST,
                    NVL(LINEHAUL_NEXT, 0) AS LINEHAUL_NEXT,
                    FLAG_MULTI,
                    FLAG_CANCEL
                FROM CMS_COST_DELIVERY_V2 ${whereClause} AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
                    AND SERVICES_CODE NOT IN ('CML','CTC_CML','P2P')
                    AND CNOTE_NO NOT LIKE 'RT%'
                    AND CNOTE_NO NOT LIKE 'FW%'
                    AND SERVICES_CODE NOT IN  ('@BOX3KG','@BOX5KG','CCINTL','CCINTL2','CML_CTC','CTC','CTC-YES','CTC05','CTC08','CTC11',
                    'CTC12','CTC13','CTC15','CTC19','CTC23','CTCOKE','CTCOKE08','CTCOKE11','CTCOKE12',
                    'CTCOKE13','CTCOKE15','CTCREG','CTCREG08','CTCREG11','CTCREG13','CTCREG15','CTCSPS08',
                    'CTCSPS1','CTCSPS11','CTCSPS12','CTCSPS13','CTCSPS15','CTCSPS19','CTCSPS2','CTCSPS23',
                    'CTCTRC08','CTCTRC11','CTCVIP','CTCVVIP','CTCYES','CTCYES08','CTCYES11','CTCYES12',
                    'CTCYES13','CTCYES15','CTCYES19','CTCYES23','INT','INTL','INTL10','INTL15',
                    'INTL16','INTL19','INTL20','JKT','JKTSS','JKTYES')
            `, bindParams);

            console.log('Query selesai, memproses data...');
            const rows = result.rows;
            const chunkSize = 1000000;
            const chunks = [];
            for (let i = 0; i < rows.length; i += chunkSize) {
                chunks.push(rows.slice(i, i + chunkSize));
            }
            console.log(`Data dibagi menjadi ${chunks.length} chunk.`);

            const dateNow = new Date();
            const dateString = dateNow.toISOString().split('T')[0];
            const timeString = dateNow.toISOString().split('T')[1].split('.')[0].replace(/:/g, '');
            const folderPath = path.join(__dirname, uuidv4());
            if (!fs.existsSync(folderPath)) fs.mkdirSync(folderPath);

            console.log('Memulai proses ekspor Excel...');
            // Normalisasi origin dan destination untuk penamaan file
            const normOrigin = normalizeName(origin);
            const normDestination = normalizeName(destination);

            const baseFileNameDCI = `DCI Report - ${user_id} [${normOrigin} to ${normDestination}] [${froms} - ${thrus}]`;

            let no = 1;
            for (let i = 0; i < chunks.length; i++) {
                console.log(`Membuat file Excel untuk chunk ${i + 1}...`);
                const fileName = path.join(folderPath, `${baseFileNameDCI} Part ${i + 1}.xlsx`);
                const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: fileName });
                const worksheet = workbook.addWorksheet('Data Laporan DCI');

                const headers = [
                    "NO", "CNOTE NO", "CNOTE DATE", "TIME CNOTE DATE", "ORIGIN", "DESTINATION", "ZONA DESTINATION",
                    "SERVICES CODE", "QTY", "WEIGHT", "AMOUNT", "MANIFEST NO", "MANIFEST DATE", "TIME MANIFEST DATE",
                    "DELIVERY", "DELIVERY SPS", "BIAYA TRANSIT", "BIAYA PENERUS", "BIAYA PENERUS NEXT KG",
                    "FLAG MULTI", "FLAG CANCEL"
                ];

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]).commit();
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]).commit();
                worksheet.addRow(['Service Code:', service === '0' ? 'ALL' : service]).commit();
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]).commit();
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]).commit();
                worksheet.addRow(['User Id:', user_id]).commit();
                worksheet.addRow(['Jumlah Data:', chunks[i].length]).commit();
                worksheet.addRow([]).commit();
                worksheet.addRow(headers).commit();

                for (const row of chunks[i]) {
                    worksheet.addRow([no++, ...row]).commit();
                }

                await workbook.commit();
                console.log(`File berhasil dibuat: ${fileName}`);

                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category
                `;
                await connection.execute(updateQuery, {
                    summary_file: i + 1,
                    jobId: jobId,
                    category: 'DCI'
                });
                await connection.commit();
                console.log(`Database log diupdate untuk chunk ${i + 1}`);
            }

            console.log('Memulai proses zip file...');
            const zipFileName = path.join(__dirname, '../../file_download', `${baseFileNameDCI} ${jobId.substring(0, 5)}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', { zlib: { level: 1 } });
            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, { recursive: true });
            console.log(`Folder ${folderPath} telah dihapus setelah proses zip.`);
            console.log(`File zip berhasil dibuat: ${zipFileName}`);

            resolve({ zipFileName, dataCount: rows.length });
        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err);
        } finally {
            if (connection) await connection.close();
            console.log('Koneksi database ditutup.');
        }
    });
}

async function fetchDataAndExportToExcelDCO({origin, destination, froms, thrus, service, user_id, dateStr,jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            console.log('Menghubungkan ke database...');
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");
            //
            // const { whereClause, bindParams } = await buildWhereClause(
            //     { origin, destination, froms, thrus, service, }, 'DCO'
            // )

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (origin !== '0' ) {
                whereClause += ` AND SUBSTR(ORIGIN, 1, 3) LIKE :origin`;
                bindParams.origin = origin + '%';
            }

            if (destination !== '0') {
                whereClause += ` AND SUBSTR(DESTINATION,1,3) LIKE :destination`;
                bindParams.destination = destination + '%';
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

            console.log('Menjalankan query data...');
            const result = await connection.execute(`
                SELECT
                    '''' || CNOTE_NO AS CNOTE_NO,
                    TO_CHAR(CNOTE_DATE, 'DD/MM/YYYY')    AS CNOTE_DATE,
                    TO_CHAR(CNOTE_DATE, 'HH:MI:SS AM')    AS TIME_CNOTE_DATE,
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
                    NVL(AMOUNT, 0) AS AMOUNT,
                    MANIFEST_NO,
                    TO_CHAR(MANIFEST_DATE, 'DD/MM/YYYY') AS MANIFEST_DATE, -- Format tanggal
                    TO_CHAR(MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_MANIFEST_DATE, -- Format tanggal
                    NVL(DELIVERY, 0) AS DELIVERY,
                    NVL(DELIVERY_SPS, 0) AS DELIVERY_SPS,
                    NVL(TRANSIT, 0) AS BIAYA_TRANSIT,
                    NVL(LINEHAUL_FIRST, 0) AS LINEHAUL_FIRST,
                    NVL(LINEHAUL_NEXT, 0) AS LINEHAUL_NEXT,
                    FLAG_MULTI,
                    FLAG_CANCEL
                FROM CMS_COST_DELIVERY_V2 ${whereClause} AND SUBSTR(ORIGIN, 1, 3) <> SUBSTR(DESTINATION, 1, 3)
                    AND SERVICES_CODE NOT IN ('CML', 'CTC_CML', 'P2P')
                    AND CNOTE_NO NOT LIKE 'RT%'  -- Exclude records with CNOTE_NO starting with 'RT'
                    AND CNOTE_NO NOT LIKE 'FW%' -- Exclude records with CNOTE_NO starting with 'FW'
                    AND SERVICES_CODE NOT IN ('@BOX3KG','@BOX5KG','CCINTL','CCINTL2','CML_CTC','CTC','CTC-YES','CTC05','CTC08','CTC11',
                    'CTC12','CTC13','CTC15','CTC19','CTC23','CTCOKE','CTCOKE08','CTCOKE11','CTCOKE12',
                    'CTCOKE13','CTCOKE15','CTCREG','CTCREG08','CTCREG11','CTCREG13','CTCREG15','CTCSPS08',
                    'CTCSPS1','CTCSPS11','CTCSPS12','CTCSPS13','CTCSPS15','CTCSPS19','CTCSPS2','CTCSPS23',
                    'CTCTRC08','CTCTRC11','CTCVIP','CTCVVIP','CTCYES','CTCYES08','CTCYES11','CTCYES12',
                    'CTCYES13','CTCYES15','CTCYES19','CTCYES23','INT','INTL','INTL10','INTL15',
                    'INTL16','INTL19','INTL20','JKT','JKTSS','JKTYES')
                `,
                bindParams
            );
            console.log('Query selesai, memproses data...');
            const rows = result.rows;
            const chunkSize = 1000000;
            const chunks = [];
            for (let i = 0; i < rows.length; i += chunkSize) {
                chunks.push(rows.slice(i, i + chunkSize));
            }
            console.log(`Data dibagi menjadi ${chunks.length} chunk.`);

            const dateNow = new Date();
            const dateString = dateNow.toISOString().split('T')[0];
            const timeString = dateNow.toISOString().split('T')[1].split('.')[0].replace(/:/g, '');
            const folderPath = path.join(__dirname, uuidv4());
            if (!fs.existsSync(folderPath)) fs.mkdirSync(folderPath);

            console.log('Memulai proses ekspor Excel...');
            // Normalisasi origin dan destination untuk penamaan file
            const normOrigin = normalizeName(origin);
            const normDestination = normalizeName(destination);
            const baseFileNameDCO = `DCO Report - ${user_id} [${normOrigin} to ${normDestination}] [${froms} - ${thrus}]`;

            let no = 1;
            for (let i = 0; i < chunks.length; i++) {
                console.log(`Membuat file Excel untuk chunk ${i + 1}...`);
                const fileName = path.join(folderPath, `${baseFileNameDCO} Part ${i + 1}.xlsx`);
                const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: fileName });
                const worksheet = workbook.addWorksheet('Data Laporan DCO');

                const headers = [
                    "NO",
                    "CONNOTE NO",
                    "CONNOTE DATE ",
                    "TIME CONNOTE DATE ",
                    "ORIGIN",
                    "DESTINATION",
                    "COLLY",
                    "ZONA",
                    "SERVICES CODE",
                    "WEIGHT",
                    "AMOUNT",
                    "MANIFEST NO",
                    "MANIFEST DATE",
                    "TIME MANIFEST DATE",
                    "DELIVERY",
                    "DELIVERY SPS",
                    "BIAYA TRANSIT",
                    "PENERUS",
                    "BIAYA PENERUS NEXT KG",
                    "FLAG_MULTI",
                    "FLAG_CANCEL"
                ];

                worksheet.addRow(['Origin:', origin === '0' ? 'ALL' : origin]).commit();
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]).commit();
                worksheet.addRow(['Service Code:', service === '0' ? 'ALL' : service]).commit();
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]).commit();
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]).commit();
                worksheet.addRow(['User Id:', user_id]).commit();
                worksheet.addRow(['Jumlah Data:', chunks[i].length]).commit();
                worksheet.addRow([]).commit();
                worksheet.addRow(headers).commit();

                for (const row of chunks[i]) {
                    worksheet.addRow([no++, ...row]).commit();
                }

                await workbook.commit();
                console.log(`File berhasil dibuat: ${fileName}`);

                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category
                `;
                await connection.execute(updateQuery, {
                    summary_file: i + 1,
                    jobId: jobId,
                    category: 'DCO'
                });
                await connection.commit();
                console.log(`Database log diupdate untuk chunk ${i + 1}`);
            }

            console.log('Memulai proses zip file...');
            const zipFileName = path.join(__dirname, '../../file_download', `${baseFileNameDCO} ${jobId.substring(0, 5)}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', { zlib: { level: 1 } });
            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, { recursive: true });
            console.log(`Folder ${folderPath} telah dihapus setelah proses zip.`);
            console.log(`File zip berhasil dibuat: ${zipFileName}`);

            resolve({ zipFileName, dataCount: rows.length });
        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err);
        } finally {
            if (connection) await connection.close();
            console.log('Koneksi database ditutup.');
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

            // const { whereClause, bindParams } = await buildWhereClause(
            //     { branch, froms, thrus }, 'CA'
            // )

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

            const chunkSize = 1000000;
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

            const zipFileName = path.join(__dirname, '../../file_download', `CAReport_${user_id}_${dateStr}_${timeStr}.zip`);
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
                const logDir = path.join(__dirname, '../log/log_files');
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

            // const { whereClause, bindParams } = await buildWhereClause(
            //     { branch, froms, thrus }, 'CABTM'
            // )

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

            const chunkSize = 1000000;
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

            const zipFileName = path.join(__dirname, '../../file_download', `CABTHReport_${user_id}_${dateStr}_${timeStr}.zip`);
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
                const logDir = path.join(__dirname, '../log/log_files');
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
            console.log('Menghubungkan ke database...');
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            // const { whereClause, bindParams } = await buildWhereClause(
            //     { origin_awal, destination, froms, thrus, services_code }, 'RU'
            // )

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (origin_awal !== '0' ) {
                whereClause += "AND  SUBSTR(RT_CNOTE_ORIGIN,1,3)  like :origin_awal ";
                bindParams.origin_awal = origin_awal + '%';
            }

            if (destination !== '0' ) {
                whereClause += "and SUBSTR(RT_CNOTE_DEST,1,3) LIKE  :destination ";
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

            console.log('Menjalankan query data...');
            const result = await connection.execute(`SELECT
                    RT_CNOTE_NO,
                    TO_CHAR(RT_CRDATE_RT, 'DD/MM/YYYY HH:MI:SS AM')    AS RT_CRDATE_RT,
                    TO_CHAR(RT_CRDATE_RT, 'HH:MI:SS AM')    AS TIME_RT_CRDATE_RT,
                    RT_CNOTE_ASLI,
                    RT_CNOTE_ASLI_ORIGIN,
                    RT_MANIFEST,
                    TO_CHAR(RT_MANIFEST_DATE, 'DD/MM/YYYY HH:MI:SS AM')    AS RT_MANIFEST_DATE,
                    TO_CHAR(RT_MANIFEST_DATE, 'HH:MI:SS AM')    AS TIME_RT_MANIFEST_DATE,
                    RT_SERVICES_CODE,
                    RT_CNOTE_ORIGIN,
                    RT_CNOTE_DEST,
                    RT_CNOTE_DEST_NAME,
                    RT_CNOTE_QTY,
                    RT_CNOTE_WEIGHT
                FROM V_OPS_RETURN_UNPAID ${whereClause}`,
                bindParams
            );

            console.log('Query selesai, memproses data...');
            const rows = result.rows;
            const chunkSize = 1000000;
            const chunks = [];
            for (let i = 0; i < rows.length; i += chunkSize) {
                chunks.push(rows.slice(i, i + chunkSize));
            }
            console.log(`Data dibagi menjadi ${chunks.length} chunk.`);

            const dateNow = new Date();
            const dateString = dateNow.toISOString().split('T')[0];
            const folderPath = path.join(__dirname, uuidv4());
            if (!fs.existsSync(folderPath)) fs.mkdirSync(folderPath);

            console.log('Memulai proses ekspor Excel...');
            const normOrigin = normalizeName(origin_awal);
            const normDestination = normalizeName(destination);
            const baseFileNameRU = `RU Report - ${user_id} [${normOrigin} to ${normDestination}] [${froms} - ${thrus}]`;

            let no = 1;
            for (let i = 0; i < chunks.length; i++) {
                console.log(`Membuat file Excel untuk chunk ${i + 1}...`);
                const fileName = path.join(folderPath, `${baseFileNameRU} Part ${i + 1}.xlsx`);
                const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: fileName });
                const worksheet = workbook.addWorksheet('Data Laporan RU');

                const headers = [
                    "NO",
                    "RESI RETURN",
                    "TANGGAL RESI RETURN",
                    "TIME RESI RETURN",
                    "RESI ASLI",
                    "ORIGIN RESI ASLI",
                    "MANIFEST RETURN",
                    "TANGGAL MANIFEST RETURN",
                    "TIME MANIFEST RETURN",
                    "SERVICES RETURN",
                    "ORIGIN RESI RETURN",
                    "DESTINASI RESI RETURN",
                    "DESTINATION NAME RETURN",
                    "QTY",
                    "WEIGHT"  ];

                worksheet.addRow(['Origin:', origin_awal === '0' ? 'ALL' : origin_awal]).commit();
                worksheet.addRow(['Destination:', destination === '0' ? 'ALL' : destination]).commit();
                worksheet.addRow(['Service Code:',(services_code === '0' || services_code === '%') ? 'ALL' : services_code ]).commit();
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]).commit();
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]).commit();
                worksheet.addRow(['User Id:', user_id]).commit();
                worksheet.addRow(['Jumlah Data:', chunks[i].length]).commit();
                worksheet.addRow([]).commit();
                worksheet.addRow(headers).commit();

                for (const row of chunks[i]) {
                    worksheet.addRow([no++, ...row]).commit();
                }

                await workbook.commit();
                console.log(`File berhasil dibuat: ${fileName}`);

                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category
                `;
                await connection.execute(updateQuery, {
                    summary_file: i + 1,
                    jobId: jobId,
                    category: 'RU'
                });
                await connection.commit();
                console.log(`Database log diupdate untuk chunk ${i + 1}`);
            }

            console.log('Memulai proses zip file...');
            const zipFileName = path.join(__dirname, '../../file_download', `${baseFileNameRU} ${jobId.substring(0, 5)}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', { zlib: { level: 1 } });
            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, { recursive: true });
            console.log(`Folder ${folderPath} telah dihapus setelah proses zip.`);
            console.log(`File zip berhasil dibuat: ${zipFileName}`);

            resolve({ zipFileName, dataCount: rows.length });
        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err);
        } finally {
            if (connection) await connection.close();
            console.log('Koneksi database ditutup.');
        }
    });
}

async function fetchDataAndExportToExcelDBO({ branch_id, froms, thrus, user_id, dateStr,jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            console.log('Menghubungkan ke database...');
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};
            if (branch_id !== '0' ) {
                whereClause += "AND  SUBSTR(BRANCH_ID,1,3) = :branch_id ";
                bindParams.branch_id = branch_id ;
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            console.log('Menjalankan query data...');
            const result = await connection.execute(`
                SELECT CNOTE_NO,
                    SERVICES_CODE,
                    QTY,
                    CURRENCY,
                    CASE
                        WHEN WEIGHT = 0 THEN 0
                        WHEN WEIGHT < 1 THEN 1
                        WHEN RPAD(REGEXP_SUBSTR(WEIGHT , '[[:digit:]]+$'),3,0) > 300 THEN CEIL(WEIGHT )
                        ELSE FLOOR(WEIGHT )
                        END WEIGHT,
                    NVL(COST_OPS, 0) AS MFEE,
                    NVL(AMOUNT, 0) AS AMOUNT,
                    case when CURRENCY = 'IDR' then 1 ELSE 2 END AS CURRENCY_RATE
                from CMS_COST_DELIVERY_V2 ${whereClause} AND CUST_NA IS NULL AND SUBSTR (CNOTE_NO, 1, 2) NOT IN ('FW', 'RT')`,
                bindParams
            );
            console.log('Query selesai, memproses data...');
            const rows = result.rows;
            const chunkSize = 1000000;
            const chunks = [];
            for (let i = 0; i < rows.length; i += chunkSize) {
                chunks.push(rows.slice(i, i + chunkSize));
            }
            console.log(`Data dibagi menjadi ${chunks.length} chunk.`);

            const dateNow = new Date();
            const folderPath = path.join(__dirname, uuidv4());
            if (!fs.existsSync(folderPath)) fs.mkdirSync(folderPath);

            console.log('Memulai proses ekspor Excel...');
            let no = 1;
            for (let i = 0; i < chunks.length; i++) {
                console.log(`Membuat file Excel untuk chunk ${i + 1}...`);
                const fileName = path.join(folderPath, `DBO Report - ${user_id} [${branch_id}] [${froms} - ${thrus}] Part ${i + 1}.xlsx`);
                const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: fileName });
                const worksheet = workbook.addWorksheet('Data Laporan DBO');

                const headers = [
                    "NO",
                    "NO CNOTE",
                    "SERVICES CODE",
                    "QTY",
                    "CURRENCY",
                    "WEIGHT",
                    "MFEE",
                    "AMOUNT",
                    "CURRENCY RATE"
                ];

                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]).commit();
                if (branch_id !== '%') {
                    worksheet.addRow(['Branch:', branch_id]).commit();
                }else{
                    worksheet.addRow(['Branch:', 'ALL']).commit();
                }
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]).commit();
                worksheet.addRow(['User Id:', user_id]).commit();
                worksheet.addRow(['Jumlah Data:', chunks[i].length]).commit();
                worksheet.addRow([]).commit();
                worksheet.addRow(headers).commit();

                for (const row of chunks[i]) {
                    worksheet.addRow([no++, ...row]).commit();
                }

                await workbook.commit();
                console.log(`File berhasil dibuat: ${fileName}`);

                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category
                `;
                await connection.execute(updateQuery, {
                    summary_file: i + 1,
                    jobId: jobId,
                    category: 'DBO'
                });
                await connection.commit();
                console.log(`Database log diupdate untuk chunk ${i + 1}`);
            }

            console.log('Memulai proses zip file...');
            const zipFileName = path.join(__dirname, '../../file_download', `DBO Report - ${user_id} [${normalizeName(branch_id)}] [${froms} - ${thrus}] ${jobId.substring(0, 5)}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', { zlib: { level: 1 } });
            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, { recursive: true });
            console.log(`Folder ${folderPath} telah dihapus setelah proses zip.`);
            console.log(`File zip berhasil dibuat: ${zipFileName}`);

            resolve({ zipFileName, dataCount: rows.length });
        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err);
        } finally {
            if (connection) await connection.close();
            console.log('Koneksi database ditutup.');
        }
    });
}

async function fetchDataAndExportToExcelDBONA({ branch_id, froms, thrus, user_id, dateStr,jobId}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            console.log('Menghubungkan ke database...');
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};

            if (branch_id !== '0' ) {
                whereClause += "AND  SUBSTR(BRANCH_ID,1,3) = :branch_id ";
                bindParams.branch_id = branch_id ;
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            console.log('Menjalankan query data...');
            const result = await connection.execute(`
                SELECT CNOTE_NO,
                    SERVICES_CODE,
                    QTY,
                    CURRENCY,
                    CASE
                        WHEN WEIGHT = 0 THEN 0
                        WHEN WEIGHT < 1 THEN 1
                        WHEN RPAD(REGEXP_SUBSTR(WEIGHT , '[[:digit:]]+$'),3,0) > 300 THEN CEIL(WEIGHT )
                        ELSE FLOOR(WEIGHT )
                        END WEIGHT,
                    NVL(COST_OPS, 0) AS MFEE,
                    TO_CHAR(NVL(AMOUNT, 0), '999G999G999', 'NLS_NUMERIC_CHARACTERS = ''.,''') AS AMOUNT,
                    case when CURRENCY = 'IDR' then 1 ELSE 2 END AS CURRENCY_RATE
                from CMS_COST_DELIVERY_V2 ${whereClause} AND CUST_NA = 'Y' AND SUBSTR (CNOTE_NO, 1, 2) NOT IN ('FW', 'RT')`,
                bindParams
            );

            console.log('Query selesai, memproses data...');
            const rows = result.rows;
            const chunkSize = 1000000;
            const chunks = [];
            for (let i = 0; i < rows.length; i += chunkSize) {
                chunks.push(rows.slice(i, i + chunkSize));
            }
            console.log(`Data dibagi menjadi ${chunks.length} chunk.`);

            const dateNow = new Date();
            const folderPath = path.join(__dirname, uuidv4());
            if (!fs.existsSync(folderPath)) fs.mkdirSync(folderPath);

            console.log('Memulai proses ekspor Excel...');
            let no = 1;
            for (let i = 0; i < chunks.length; i++) {
                console.log(`Membuat file Excel untuk chunk ${i + 1}...`);
                const fileName = path.join(folderPath, `DBONA Report - ${user_id} [${branch_id}] [${froms} - ${thrus}] Part ${i + 1}.xlsx`);
                const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: fileName });
                const worksheet = workbook.addWorksheet('Data Laporan DBONA');

                const headers = [
                    "NO",
                    "CNOTE NO",
                    "SERVICES CODE",
                    "QTY",
                    "CURRENCY",
                    "WEIGHT",
                    "MFEE",
                    "AMOUNT",
                    "CURRENCY RATE"
                ];

                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]).commit();
                if (branch_id !== '%') {
                    worksheet.addRow(['Branch:', branch_id]).commit();
                }else{
                    worksheet.addRow(['Branch:', 'ALL']).commit();
                }
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]).commit();
                worksheet.addRow(['User Id:', user_id]).commit();
                worksheet.addRow(['Jumlah Datas:', chunks[i].length]).commit();
                worksheet.addRow([]).commit();
                worksheet.addRow(headers).commit();

                for (const row of chunks[i]) {
                    const numericIndices = [6, 7];
                    numericIndices.forEach(idx => {
                        const value = row[idx];
                        if (typeof value === 'string') {
                            row[idx] = parseFloat(value.replace(/[.,]/g, '')) || 0;
                        }
                    });

                    const newRow = worksheet.addRow([no++, ...row]);

                    // Format agar tampil 150,000
                    newRow.getCell(7).numFmt = '#,##0'; // MFEE
                    newRow.getCell(8).numFmt = '#,##0'; // AMOUNT

                    newRow.commit();
                }
                await workbook.commit();
                console.log(`File berhasil dibuat: ${fileName}`);

                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category
                `;
                await connection.execute(updateQuery, {
                    summary_file: i + 1,
                    jobId: jobId,
                    category: 'DBONA'
                });
                await connection.commit();
                console.log(`Database log diupdate untuk chunk ${i + 1}`);
            }

            console.log('Memulai proses zip file...');
            const zipFileName = path.join(__dirname, '../../file_download', `DBONA Report - ${user_id} [${normalizeName(branch_id)}] [${froms} - ${thrus}] ${jobId.substring(0, 5)}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', { zlib: { level: 1 } });
            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, { recursive: true });
            console.log(`Folder ${folderPath} telah dihapus setelah proses zip.`);
            console.log(`File zip berhasil dibuat: ${zipFileName}`);

            resolve({ zipFileName, dataCount: rows.length });
        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err);
        } finally {
            if (connection) await connection.close();
            console.log('Koneksi database ditutup.');
        }
    });
}

async function fetchDataAndExportToExcelDBONASUM({ branch_id, froms, thrus, user_id, dateStr, jobId }) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            //
            // const { whereClause, bindParams } = await buildWhereClause(
            //     { branch_id, froms, thrus }, 'DBONASUM'
            // )

            let whereClause = "WHERE 1 = 1";
            const bindParams = {};


            // Kondisi untuk branch_id
            if (branch_id !== '0' ) {
                whereClause += " AND SUBSTR(BRANCH_ID, 1, 3) = :branch_id ";
                bindParams.branch_id = branch_id;
            }
            // Kondisi untuk periode tanggal
            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND TRUNC(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            function calculateTotal(rows, indices) {
                const totals = new Array(indices.length).fill(0);
                for (const row of rows) {
                    indices.forEach((idx, i) => {
                        const val = parseFloat(row[idx]) || 0;
                        totals[i] += val;
                    });
                }
                return totals.map(num => Math.round(num));
            }

            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            const workbook = new ExcelJS.Workbook();
            const worksheet = workbook.addWorksheet('Data Laporan Biaya Operasional');
            let rowIndex = 1;

            worksheet.addRow(['Branch:', branch_id === '0' ? 'ALL' : branch_id]);
            worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]);
            worksheet.addRow(['Download Date:', new Date().toLocaleString()]);
            worksheet.addRow(['User Id:', user_id]);
            worksheet.addRow([]);
            rowIndex = worksheet.lastRow.number + 2;

            // === TABEL SUMMARY OPS ===
            worksheet.getRow(rowIndex++).values = ['Biaya Operasional Non NA'];
            worksheet.getRow(rowIndex++).values = [
                'Services Code', 'QTY', 'Currency', 'Weight', 'Awb',
                'Amount', 'Nett Amount', 'Currency Rate', 'Tipe', 'Biaya Ops'
            ];
            const summaryOps = await connection.execute(`
                SELECT
                    SERVICES_CODE,
                    SUM(QTY),
                    CURRENCY,
                    SUM(WEIGHT),
                    COUNT(CNOTE_NO),
                    SUM(AMOUNT),
                    SUM(AMOUNT)/1.011 AS NETT_AMOUNT,
--                     SUM(COST_OPS),
                    CASE WHEN CURRENCY = 'IDR' THEN 1 ELSE 2 END CURRENCY_RATE,
                    CASE
                        WHEN SERVICES_CODE LIKE 'JTR%' THEN 'LOG'
                        WHEN SERVICES_CODE LIKE 'CTCJTR%' THEN 'LOG'
                        WHEN SERVICES_CODE IN ('CRGMB24','CRGTK','CTCCARGO23','CTCTRC11') THEN 'LOG'
                        ELSE 'EXP'
                        END TIPE,
                    CASE
                        WHEN SERVICES_CODE LIKE 'JTR%' THEN (SUM(AMOUNT)/1.011) * 0.05
                        WHEN SERVICES_CODE LIKE 'CTCJTR%' THEN (SUM(AMOUNT)/1.011) * 0.05
                        WHEN SERVICES_CODE IN ('CRGMB24','CRGTK','CTCCARGO23') THEN (SUM(AMOUNT)/1.011) * 0.05
                        WHEN SERVICES_CODE LIKE '%TRC%' THEN 0
                        ELSE (SUM(AMOUNT)/1.011) * 0.1
                        END BIAYA_OPS_NEW
                FROM CMS_COST_DELIVERY_v2
                    ${whereClause}
                    AND COST_OPS IS NOT NULL
             AND SERVICES_CODE IN (
                    SELECT SERVICE_CODE
                    FROM CMS_SERVICE@DB219
                    WHERE SERVICE_CATEGORY = 'EXP'   OR SERVICE_CATEGORY IS NULL
                    AND SERVICE_ACTIVE ='Y'
                    )
                    and services_code not in ('CTCTRC11')
                AND SERVICES_CODE NOT LIKE '%INT%'
                AND CNOTE_NO NOT LIKE 'RT%'
                AND CNOTE_NO NOT LIKE 'FW%'
                GROUP BY SERVICES_CODE, CURRENCY_RATE, CURRENCY
            `, bindParams);

            summaryOps.rows.forEach(row => {
                const r = worksheet.getRow(rowIndex++);
                r.values = row;
                [2, 4, 5, 6, 7, 10].forEach(col => r.getCell(col).numFmt = '#,##0');
            });

            const totalOps = calculateTotal(summaryOps.rows, [1, 3, 4, 5, 6, 9]);
            const totalRowOps = worksheet.getRow(rowIndex++);
            totalRowOps.values = [
                'TOTAL', totalOps[0], '', totalOps[1], totalOps[2],
                totalOps[3], totalOps[4], '', '', totalOps[5]
            ];
            [2, 4, 5, 6, 7, 10].forEach(col => totalRowOps.getCell(col).numFmt = '#,##0');

            // jtr
            rowIndex += 1;
            const summaryOpsJTR = await connection.execute(`
                SELECT
                    SERVICES_CODE,
                    SUM(QTY),
                    CURRENCY,
                    SUM(WEIGHT),
                    COUNT(CNOTE_NO),
                    SUM(AMOUNT),
                    SUM(AMOUNT)/1.011 AS NETT_AMOUNT,
--                     SUM(COST_OPS),
                    CASE WHEN CURRENCY = 'IDR' THEN 1 ELSE 2 END CURRENCY_RATE,
                    CASE
                        WHEN SERVICES_CODE LIKE 'JTR%' THEN 'LOG'
                        WHEN SERVICES_CODE LIKE 'CTCJTR%' THEN 'LOG'
                        WHEN SERVICES_CODE IN ('CRGMB24','CRGTK','CTCCARGO23','CTCTRC11') THEN 'LOG'
                        ELSE 'EXP'
                        END TIPE,
                    CASE
                        WHEN SERVICES_CODE LIKE 'JTR%' THEN (SUM(AMOUNT)/1.011) * 0.05
                        WHEN SERVICES_CODE LIKE 'CTCJTR%' THEN (SUM(AMOUNT)/1.011) * 0.05
                        WHEN SERVICES_CODE IN ('CRGMB24','CRGTK','CTCCARGO23') THEN (SUM(AMOUNT)/1.011) * 0.05
                        WHEN SERVICES_CODE LIKE '%TRC%' THEN 0
                        ELSE (SUM(AMOUNT)/1.011) * 0.1
                        END BIAYA_OPS_NEW
                FROM CMS_COST_DELIVERY_v2
                    ${whereClause}
                    AND COST_OPS IS NOT NULL
--                         AND SERVICES_CODE IN ('JTR<130',
--                         'JTR>130',
--                         'JTR23',
--                         'JTR>200',
--                         'CTCJTR23',
--                         'CTCTRC11',
--                         'CTCJTR5_23',
--                         'JTR5_23',
--                         'CRGTK')
                    AND SERVICES_CODE NOT LIKE '%INT%'
                     AND SERVICES_CODE IN (
                    SELECT SERVICE_CODE
                    FROM CMS_SERVICE@DB219
                    WHERE SERVICE_CATEGORY = 'LOG'  
                    AND SERVICE_ACTIVE ='Y'
                    )
                    AND CNOTE_NO NOT LIKE 'RT%'
                    AND CNOTE_NO NOT LIKE 'FW%'
                    GROUP BY SERVICES_CODE, CURRENCY_RATE, CURRENCY
            `, bindParams);

            summaryOpsJTR.rows.forEach(row => {
                const rjtr = worksheet.getRow(rowIndex++);
                rjtr.values = row;
                [2, 4, 5, 6, 7,8, 10].forEach(col => rjtr.getCell(col).numFmt = '#,##0');
            });

            const totalOpsJTR = calculateTotal(summaryOpsJTR.rows, [1, 3, 4, 5, 6, 9]);
            const totalRowOpsJTR = worksheet.getRow(rowIndex++);
            totalRowOpsJTR.values = [
                'TOTAL', totalOpsJTR[0], '', totalOpsJTR[1], totalOpsJTR[2],
                totalOpsJTR[3], totalOpsJTR[4], '', '', totalOpsJTR[5]
            ];
            [2, 4, 5, 6, 7,10].forEach(col => totalRowOpsJTR.getCell(col).numFmt = '#,##0');

            // === TABEL SUMMARY NO OPS ===
            rowIndex += 2;
            worksheet.getRow(rowIndex++).values = ['Biaya Operasional NA'];
            worksheet.getRow(rowIndex++).values = [
                'Currency', 'Services Code', 'Awb', 'Qty', 'Weight',
                'Amount', 'Tipe', 'Currency Rate'
            ];

            const summaryNoOps = await connection.execute(`
                SELECT
                    SERVICES_CODE,
                    SUM(QTY),
                    CURRENCY,
                    SUM(WEIGHT),
                    COUNT(CNOTE_NO),
                    SUM(AMOUNT),
                    CASE WHEN CURRENCY = 'IDR' THEN 1 ELSE 2 END CURRENCY_RATE,
                    CASE
                        WHEN SERVICES_CODE LIKE 'JTR%' THEN 'LOG'
                        WHEN SERVICES_CODE LIKE 'CTCJTR%' THEN 'LOG'
                        WHEN SERVICES_CODE IN ('CRGMB24','CARGO23','CRGTK','CTCCARGO23','CTCTRC11') THEN 'LOG'
                        WHEN SERVICES_CODE LIKE 'INTL%' THEN 'INTL'
                        ELSE 'EXP'
                        END TIPE
                FROM CMS_COST_DELIVERY_V2
                    ${whereClause}
                    AND COST_OPS IS NULL
--                     AND SERVICES_CODE NOT IN ('JTR<130',
--                         'JTR>130',
--                         'JTR23',
--                         'CTCJTR23',
--                         'CTCTRC11',
--                         'CTCJTR5_23',
--                         'JTR5_23',
--                         'CRGTK')
                         AND SERVICES_CODE IN (
                    SELECT SERVICE_CODE
                    FROM CMS_SERVICE@DB219
                    WHERE SERVICE_CATEGORY = 'EXP'   OR SERVICE_CATEGORY IS NULL
                    AND SERVICE_ACTIVE ='Y'
                    )
                    and services_code not in ('CTCTRC11')
                    AND SERVICES_CODE NOT LIKE '%INT%'
                    AND CNOTE_NO NOT LIKE 'RT%'
                    AND CNOTE_NO NOT LIKE 'FW%'
                    GROUP BY SERVICES_CODE, CURRENCY_RATE, CURRENCY
            `, bindParams);

            summaryNoOps.rows = summaryNoOps.rows.map(row => [
                row[2], row[0], row[4], row[1], row[3], row[5], row[7], row[6]
            ]);

            summaryNoOps.rows.forEach(row => {
                const r = worksheet.getRow(rowIndex++);
                r.values = row;
                [3,4, 5, 6].forEach(col => r.getCell(col).numFmt = '#,##0');
            });

            const totalNoOps = calculateTotal(summaryNoOps.rows, [2,3, 4, 5]);
            const totalRowNoOps = worksheet.getRow(rowIndex++);
            totalRowNoOps.values = [
                '', 'TOTAL', totalNoOps[0], totalNoOps[1], totalNoOps[2], totalNoOps[3], '', ''

            ];
            [3,4, 5, 6].forEach(col => totalRowNoOps.getCell(col).numFmt = '#,##0');

            // NO JTR
            rowIndex += 1;

            const summaryNoOpsJTR = await connection.execute(`
                SELECT
                    SERVICES_CODE,
                    SUM(QTY),
                    CURRENCY,
                    SUM(WEIGHT),
                    COUNT(CNOTE_NO),
                    SUM(AMOUNT),
                    CASE WHEN CURRENCY = 'IDR' THEN 1 ELSE 2 END CURRENCY_RATE,
                    CASE
                        WHEN SERVICES_CODE LIKE 'JTR%' THEN 'LOG'
                        WHEN SERVICES_CODE LIKE 'CTCJTR%' THEN 'LOG'
                        WHEN SERVICES_CODE IN ('CRGMB24','CARGO23','CRGTK','CTCCARGO23','CTCTRC11') THEN 'LOG'
                        WHEN SERVICES_CODE LIKE 'INTL%' THEN 'INTL'
                        ELSE 'EXP'
                        END TIPE
                FROM CMS_COST_DELIVERY_V2
                    ${whereClause}
                    AND COST_OPS IS NULL
                    AND SERVICES_CODE NOT LIKE '%INT%'
--                     AND SERVICES_CODE  IN ('JTR<130',
--                         'JTR>130',
--                         'JTR23',
--                         'CTCJTR23',
--                         'CTCTRC11',
--                         'CTCJTR5_23',
--                         'JTR5_23',
--                         'CRGTK')
                             AND SERVICES_CODE IN (
                    SELECT SERVICE_CODE
                    FROM CMS_SERVICE@DB219
                    WHERE SERVICE_CATEGORY = 'LOG'
                    AND SERVICE_ACTIVE ='Y'
                    )
                    AND CNOTE_NO NOT LIKE 'RT%'
                    AND CNOTE_NO NOT LIKE 'FW%'
                GROUP BY SERVICES_CODE, CURRENCY_RATE, CURRENCY
            `, bindParams);

            summaryNoOpsJTR.rows = summaryNoOpsJTR.rows.map(row => [
                row[2], row[0], row[4], row[1], row[3], row[5], row[7], row[6]
            ]);

            summaryNoOpsJTR.rows.forEach(row => {
                const rnojtr = worksheet.getRow(rowIndex++);
                rnojtr.values = row;
                [3,4, 5, 6].forEach(col => rnojtr.getCell(col).numFmt = '#,##0');
            });

            const totalNoOpsJTR = calculateTotal(summaryNoOpsJTR.rows, [2,3, 4, 5]);
            const totalRowNoOpsJTR = worksheet.getRow(rowIndex++);
            totalRowNoOpsJTR.values = [
                '', 'TOTAL', totalNoOpsJTR[0], totalNoOpsJTR[1], totalNoOpsJTR[2], totalNoOpsJTR[3], '', ''

            ];
            [3,4, 5, 6].forEach(col => totalRowNoOpsJTR.getCell(col).numFmt = '#,##0');


            rowIndex += 2;
            worksheet.getRow(rowIndex++).values = ['Penjualan Internasional'];
            worksheet.getRow(rowIndex++).values = [
                'Currency', 'Services Code', 'Awb', 'Qty', 'Weight', 'Tipe', 'Amount'
            ];

            const summaryIntl = await connection.execute(`
                SELECT
                    SERVICES_CODE AS SERVICES_CODE4,
                    SUM(QTY) AS SUM_QTY4,
                    CURRENCY AS CURRENCY4,
                    SUM(WEIGHT) AS SUM_WEIGHT4,
                    COUNT(CNOTE_NO) AS COUNT_CNOTE4,
                    SUM(AMOUNT) AS AMOUNT4,
                    CASE
                        WHEN SERVICES_CODE LIKE 'INTL%' THEN 'INTL'
                        ELSE 'EXP'
                        END TIPE
                FROM CMS_COST_DELIVERY_V2
                    ${whereClause}
                    AND COST_OPS IS NOT NULL
                    AND SERVICES_CODE LIKE '%INT%'
                    AND SERVICES_CODE NOT LIKE '%TRC%'
                    AND CNOTE_NO NOT LIKE 'RT%'
                    AND CNOTE_NO NOT LIKE 'FW%'
                GROUP BY SERVICES_CODE, CURRENCY
            `, bindParams);

            summaryIntl.rows = summaryIntl.rows.map(row => [
                row[2], row[0], row[4], row[1], row[3], row[6], row[5]
            ]);

            summaryIntl.rows.forEach(row => {
                const r = worksheet.getRow(rowIndex++);
                r.values = row;
                [3, 4, 5, 7].forEach(col => r.getCell(col).numFmt = '#,##0');
            });

            const totalIntl = calculateTotal(summaryIntl.rows, [2, 3, 4, 6]);
            const totalRowIntl = worksheet.getRow(rowIndex++);
            totalRowIntl.values = [
                '', 'TOTAL', totalIntl[0], totalIntl[1], totalIntl[2], '', totalIntl[3]
            ];
            [3, 4, 5, 7].forEach(col => totalRowIntl.getCell(col).numFmt = '#,##0');



            const dateNow = new Date();
            const dateString = dateNow.toISOString().split('T')[0];
            const timeString = dateNow.toISOString().split('T')[1].split('.')[0].replace(/:/g, '');

            const zipFileName = path.join(__dirname, '../../file_download', `Biaya Operasional Report - ${user_id} [${normalizeName(branch_id)}] [${froms} - ${thrus}] ${jobId.substring(0, 5)}.xlsx`);
            await workbook.xlsx.writeFile(zipFileName);



            resolve({ zipFileName, dataCount: 1 });

        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            try {
                const logDir = path.join(__dirname, '../log/log_files');
                if (!fs.existsSync(logDir)) fs.mkdirSync(logDir);
                const logFile = path.join(logDir, `error_${Date.now()}.log`);
                const logContent = `Error Message: ${err.message}\n` +
                    `Stack Trace: ${err.stack}\n` +
                    `Input Params: ${JSON.stringify({ branch_id, froms, thrus, user_id, dateStr, jobId }, null, 2)}\n`;
                fs.writeFileSync(logFile, logContent, 'utf-8');
                console.log(`Parameter input dan error telah ditulis ke ${logFile}`);
            } catch (writeErr) {
                console.error('Gagal menulis file log:', writeErr);
            }
            reject(err);
        } finally {
            if (connection) await connection.close();
        }
    });
}

async function fetchDataAndExportToExcelMP({ origin, destination, froms, thrus, user_id, dateStr, jobId }) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            console.log('Menghubungkan ke database...');
            connection = await oracledb.getConnection(config);
            console.log("Koneksi berhasil ke database");

            // const { whereClause, bindParams } = await buildWhereClause(
            //     { origin, destination, froms, thrus }, 'MP'
            // )

            let whereClause = `WHERE 1 = 1`;
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
                whereClause += ` AND TRUNC(CONNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                bindParams.froms = froms;
                bindParams.thrus = thrus;
            }

            console.log('Menjalankan query data...');
            // const resultOld = await connection.execute(`
            //     SELECT
            //         '''' || AWB_NO AS AWB,
            //         TO_CHAR(AWB_DATE, 'DD/MM/YYYY') AS AWB_DATE,
            //         TO_CHAR(AWB_DATE, 'HH:MI:SS AM') AS AWB_TIME,
            //         SERVICES_CODE,
            //         CNOTE_WEIGHT,
            //         ORIGIN,
            //         DESTINATION,
            //         CUST_ID,
            //         CASE
            //             WHEN CUST_ID = '80514305' THEN 'SHOPEE'
            //             WHEN CUST_ID IN ('11666700','80561600','80561601') THEN 'TOKOPEDIA'
            //             ELSE 'OTHER'
            //             END AS MARKETPLACE,
            //         '''' || BAG_NO AS BAG_NO,
            //         PRORATED_WEIGHT,
            //         OUTBOND_MANIFEST_NO,
            //         TO_CHAR(OUTBOND_MANIFEST_DATE, 'DD/MM/YYYY') AS OUTBOND_MANIFEST_DATE,
            //         OUTBOND_MANIFEST_ROUTE,
            //         TRANSIT_MANIFEST_NO,
            //         TO_CHAR(TRANSIT_MANIFEST_DATE, 'DD/MM/YYYY') AS TRANSIT_MANIFEST_DATE,
            //         TO_CHAR(TRANSIT_MANIFEST_DATE, 'HH:MI:SS AM') AS TRANSIT_MANIFEST_TIME,
            //         TRANSIT_MANIFEST_ROUTE,
            //         MODA,
            //         MODA_TYPE
            //     FROM CMS_COST_TRANSIT_V2
            //     ${whereClause}
            //         AND CUST_ID IN ('11666700','80561600','80561601','80514305')
            // `, bindParams);

            const result = await connection.execute(`
                SELECT
                BRANCH_ID,
                ''''||CONNOTE_NO as CONNOTE_NO,
                TO_CHAR(CONNOTE_DATE, 'DD/MM/YYYY') AS CONNOTE_DATE,
                TO_CHAR(CONNOTE_DATE, 'HH:MI:SS AM') AS CONNOTE_TIME,
                CONNOTE_ROUTE_CODE,
                SERVICE,
                OUTBOND_MANIFEST,
                TO_CHAR(OUTBOND_MANIFEST_DATE, 'DD/MM/YYYY') AS OUTBOND_MANIFEST_DATE,
                TO_CHAR(OUTBOND_MANIFEST_DATE, 'HH:MI:SS AM') AS OUTBOND_MANIFEST_TIME,
                OUTBOND_MANIFEST_ROUTE,
                TRANSIT_MANIFEST,
                TRANSIT_MANIFEST_ROUTE,
                TO_CHAR(TRANSIT_MANIFEST_DATE, 'DD/MM/YYYY') AS TRANSIT_MANIFEST_DATE,
                TO_CHAR(TRANSIT_MANIFEST_DATE, 'HH:MI:SS AM') AS TRANSIT_MANIFEST_TIME,
                SMU_NO,
                SMU_ROUTE,
                BRANCH_TRANSPORTER,
                SMU_MODA,
                SMU_MODA_TYPE,
                ''''||BAG_NO AS BAG_NO,
                ACTUAL_BAG_WEIGHT,
                PRORATED_WEIGHT,
                CONNOTE_CUST_ID,
                CASE 
                    WHEN CONNOTE_CUST_ID = '80514305' THEN 'SHOPEE'
                    WHEN CONNOTE_CUST_ID IN ('11666700','80561600','80561601') THEN 'TOKOPEDIA'
                    ELSE 'OTHER'
                END MARKETPLACE,
                PRORATED_WEIGHT * NVL(DBCTC_V2.F_GET_BIAYA_ANGKUT_MP (CONNOTE_CUST_ID, CONNOTE_ROUTE_CODE,SERVICE),0) AS BIAYA_ANGKUT
                FROM DBCTC_V2.REP_BIAYA_ANGKUT       
                ${whereClause}
            `, bindParams)

            console.log('Query selesai, memproses data...');

            const rows = result.rows;
            const chunkSize = 1000000;
            const chunks = [];
            for (let i = 0; i < rows.length; i += chunkSize) {
                chunks.push(rows.slice(i, i + chunkSize));
            }
            console.log(`Data dibagi menjadi ${chunks.length} chunk.`);

            const dateNow = new Date();
            const dateString = dateNow.toISOString().split('T')[0];
            const timeString = dateNow.toISOString().split('T')[1].split('.')[0].replace(/:/g, '');
            const folderPath = path.join(__dirname, uuidv4());
            if (!fs.existsSync(folderPath)) fs.mkdirSync(folderPath);

            console.log('Memulai proses ekspor Excel...');
            // Normalisasi origin dan destination untuk penamaan file
            const normOrigin = normalizeName(origin);
            const normDestination = normalizeName(destination);

            // Penamaan file: BiayaAngkut_Marketplace_Report_{origin}-to-{destination}_{user_id}_{dateString}
            const baseFileNameMP = `BA_Marketplace Report - ${user_id} [${normOrigin} to ${normDestination}] [${froms} - ${thrus}]`;

            let no = 1;
            for (let i = 0; i < chunks.length; i++) {
                console.log(`Membuat file Excel untuk chunk ${i + 1}...`);
                const fileName = path.join(folderPath, `${baseFileNameMP} Part ${i + 1}.xlsx`);
                const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: fileName });
                const worksheet = workbook.addWorksheet('Marketplace Report');

                // Header sesuai dengan label tampilan website
                const headers = [
                    "NO",
                    "BRANCH ID",
                    "CONNOTE NO",
                    "CONNOTE DATE",
                    "CONNOTE TIME",
                    "CONNOTE ROUTE CODE",
                    "SERVICES CODE",
                    "OUTBOND MANIFEST NO",
                    "OUTBOND MANIFEST DATE",
                    "OUTBOND MANIFEST TIME",
                    "OUTBOND MANIFEST ROUTE",
                    "TRANSIT MANIFEST NO",
                    "TRANSIT MANIFEST ROUTE",
                    "TRANSIT MANIFEST DATE",
                    "TRANSIT MANIFEST TIME",
                    "SMU NO",
                    "SMU ROUTE",
                    "BRANCH TRANSPORTER",
                    "MODA",
                    "MODA TYPE",
                    "BAG NO",
                    "BAG WEIGHT",
                    "PRORATED WEIGHT",
                    "CUST ID",
                    "MARKETPLACE",
                    "BIAYA ANGKUT"
                ];

                // Metadata laporan
                worksheet.addRow(['Origin:', origin === '0' || origin === '%' ? 'ALL' : origin]).commit();
                worksheet.addRow(['Destination:', destination === '0' || destination === '%' ? 'ALL' : destination]).commit();
                worksheet.addRow(['Period:', `${froms} s/d ${thrus}`]).commit();
                worksheet.addRow(['Download Date:', new Date().toLocaleString()]).commit();
                worksheet.addRow(['User Id:', user_id]).commit();
                worksheet.addRow(['Jumlah Data:', chunks[i].length]).commit();
                worksheet.addRow([]).commit();
                worksheet.addRow(headers).commit();

                for (const row of chunks[i]) {
                    worksheet.addRow([no++, ...row]).commit();
                }

                await workbook.commit();
                console.log(`File berhasil dibuat: ${fileName}`);

                const updateQuery = `
                    UPDATE CMS_COST_TRANSIT_V2_LOG
                    SET SUMMARY_FILE = :summary_file
                    WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category
                `;
                await connection.execute(updateQuery, {
                    summary_file: i + 1,
                    jobId: jobId,
                    category: 'MP'
                });
                await connection.commit();
                console.log(`Database log diupdate untuk chunk ${i + 1}`);
            }

            console.log('Memulai proses zip file...');
            const zipFileName = path.join(__dirname, '../../file_download', `${baseFileNameMP} ${jobId.substring(0, 5)}.zip`);
            const output = fs.createWriteStream(zipFileName);
            const archive = archiver('zip', { zlib: { level: 1 } });
            archive.pipe(output);
            archive.directory(folderPath, false);
            await archive.finalize();

            fs.rmSync(folderPath, { recursive: true });
            console.log(`Folder ${folderPath} telah dihapus setelah proses zip.`);
            console.log(`File zip berhasil dibuat: ${zipFileName}`);

            resolve({ zipFileName, dataCount: rows.length });
        } catch (err) {
            console.error('Terjadi kesalahan:', err);
            reject(err);
        } finally {
            if (connection) await connection.close();
        }
    });
}

module.exports = {
    fetchDataAndExportToExcel,
    fetchDataAndExportToExcelTCI,
    fetchDataAndExportToExcelDCI,
    fetchDataAndExportToExcelDCO,
    fetchDataAndExportToExcelCA,
    fetchDataAndExportToExcelCABTM,
    fetchDataAndExportToExcelRU,
    fetchDataAndExportToExcelDBO,
    fetchDataAndExportToExcelDBONA,
    fetchDataAndExportToExcelDBONASUM,
    fetchDataAndExportToExcelMP,
};
