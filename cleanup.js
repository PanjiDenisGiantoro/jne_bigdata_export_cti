const oracledb = require('oracledb');
const fs = require('fs');
const path = require('path');
const Sentry = require("@sentry/node");
const {nodeProfilingIntegration} = require("@sentry/profiling-node");

const config = {
    user: 'dbctc_v2',
    password: 'dbctc123',
    connectString: '10.8.2.48:1521/ctcv2db'
};


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
async function cleanupOldRecords() {
    let connection;

    try {
        connection = await oracledb.getConnection(config);

        // Now, fetch the filenames from the database
        const fileQuery = `
            SELECT NAME_FILE
            FROM CMS_COST_TRANSIT_V2_LOG
            WHERE download = 0
              AND created_at < SYSDATE - INTERVAL '1' DAY
        `;

        const filesToDelete = await connection.execute(fileQuery);

        // Define folder path
        const folderPath = path.join(__dirname, 'file_download');

        // Loop through the fetched filenames and delete them from the folder
        for (const row of filesToDelete.rows) {
            const fileName = row[0];

            // Check if the file name is not null and ends with '.zip'
            if (fileName && fileName.endsWith('.zip')) {
                const filePath = path.join(folderPath, fileName);

                // Check if the file exists
                fs.stat(filePath, (err, stats) => {
                    if (err) {
                        console.error('Error checking file stats:', err);
                        return;
                    }

                    // Calculate the age of the file in days
                    const fileAgeInDays = (Date.now() - stats.mtimeMs) / (1000 * 3600 * 24); // in days

                    // if (fileAgeInDays > 1) {
                        // If the file is older than 5 days, delete it
                        fs.unlink(filePath, (unlinkErr) => {
                            if (unlinkErr) {
                                console.error('Error deleting file:', unlinkErr);
                                Sentry.captureException(err); // Capture error with Sentry
                                return;

                            } else {
                                console.log(`Deleted old zip file: ${fileName}`);
                            }
                        });
                    // }
                });
            }else{
                console.log(`File name is null or does not end with '.zip': ${fileName}`);
            }
        }

        // After deleting the files, delete the corresponding records from the database
        const deleteQuery = `
            DELETE FROM CMS_COST_TRANSIT_V2_LOG
            WHERE download = 0
              AND created_at < SYSDATE - INTERVAL '1' DAY
        `;

        // Execute the delete query to remove records from the database
        const result = await connection.execute(deleteQuery);
        await connection.commit();

        console.log(`Deleted ${result.rowsAffected} old records from CMS_COST_TRANSIT_V2_LOG.`);

    } catch (error) {
        console.error('Error during cleanup:', error);
        Sentry.captureException(error); // Capture error with Sentry

    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

cleanupOldRecords();
