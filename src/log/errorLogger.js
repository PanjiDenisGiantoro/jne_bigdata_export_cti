const path = require('path');
const fs = require('fs');
const { format } = require('date-fns');

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

function logErrorToFileDBO(jobId, branch_id, userId, errorMessage) {
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

function logErrorToFileDBONASUM(jobId, branch_id, userId, errorMessage) {
    const logFilePath = path.join(__dirname, 'error_logs.txt');
    const logMessage = `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')} | JobID: ${jobId} | origin: ${branch_id} | Destination: ${currency} | service_code : ${services_code} | UserID: ${userId} | Error: ${errorMessage}\n`;

    fs.appendFile(logFilePath, logMessage, (err) => {
        if (err) {
            console.error('Error writing to log file:', err);
        }
    });
}

module.exports = {
    logErrorToFile,
    logErrorToFileTCI,
    logErrorToFileDCI,
    logErrorToFileDCO,
    logErrorToFileCA,
    logErrorToFileRU,
    logErrorToFileDBO,
    logErrorToFileDBONA,
    logErrorToFileDBONASUM
};