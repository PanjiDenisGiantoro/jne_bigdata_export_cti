const oracledb = require('oracledb');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');

const config = {
    user: 'dbctc_v2',
    password: 'dbctc123',
    connectString: '10.8.2.48:1521/ctcv2db'
};

const logSuccessPath = path.join(__dirname, 'success.log');
const logErrorPath = path.join(__dirname, 'error.log');

function logToFile(filePath, message) {
    const logMessage = `[${new Date().toISOString()}] ${message}\n`;
    return fs.promises.appendFile(filePath, logMessage);
}
function flushLinuxCache() {
    return new Promise((resolve, reject) => {
        exec("sync; echo 3 > /proc/sys/vm/drop_caches", (error, stdout, stderr) => {
            if (error) {
                console.error(`Gagal flush cache: ${error.message}`);
                return reject(error);
            }
            if (stderr) {
                console.warn(`Peringatan saat flush cache: ${stderr}`);
            }
            console.log("✅ Cache Linux berhasil di-flush.");
            resolve();
        });
    });
}

async function cleanupOldRecords() {
    let connection;
    let updateCount = 0;

    try {
        connection = await oracledb.getConnection(config);

        const fileQuery = `
            SELECT NAME_FILE
            FROM CMS_COST_TRANSIT_V2_LOG
            WHERE download = 0
              AND TRANSIT_V2_LOG_FLAG_DELETE = 'N'
              AND TRANSIT_V2_LOG_DATE_DELETE < SYSDATE - INTERVAL '1' DAY
        `;

        console.log('Executing query:', fileQuery);

        const filesToDelete = await connection.execute(fileQuery);

        const folderPath = path.join(__dirname, 'file_download');
        console.log("Folder Path:", folderPath);

        await Promise.all(filesToDelete.rows.map(async (row) => {
            const fileName = row[0];

            if (fileName && fileName.endsWith('.zip')) {
                const filePath = path.join(folderPath, fileName);
                console.log("File Path:", filePath);

                try {
                    await fs.promises.stat(filePath);
                    await fs.promises.unlink(filePath);

                    const successMsg = `Deleted old zip file: ${fileName}`;
                    console.log(successMsg);
                    await logToFile(logSuccessPath, successMsg);

                    updateCount++;
                } catch (err) {
                    const errorMsg = `Error deleting file ${fileName}: ${err.message}`;
                    console.error(errorMsg);
                    await logToFile(logErrorPath, errorMsg);
                }
            } else {
                const warnMsg = `File name is null or does not end with '.zip': ${fileName}`;
                console.log(warnMsg);
                await logToFile(logErrorPath, warnMsg);
            }
        }));

        const updateQuery = `
            UPDATE CMS_COST_TRANSIT_V2_LOG
            SET TRANSIT_V2_LOG_FLAG_DELETE = 'Y'
            WHERE download = 0
              AND TRANSIT_V2_LOG_FLAG_DELETE = 'N'
              AND TRANSIT_V2_LOG_DATE_DELETE < SYSDATE - INTERVAL '1' DAY
        `;

        const result = await connection.execute(updateQuery, [], { autoCommit: true });
        const updateMsg = `Rows updated: ${result.rowsAffected}`;
        console.log(updateMsg);
        await logToFile(logSuccessPath, updateMsg);

    } catch (error) {
        const errMsg = `Error during cleanup: ${error.message}`;
        console.error(errMsg);
        await logToFile(logErrorPath, errMsg);
    } finally {
        if (connection) {
            await connection.close();

            await flushLinuxCache();
        }

    }
}

cleanupOldRecords();
