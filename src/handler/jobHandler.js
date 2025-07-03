const path = require('path');
const fs = require('fs');
const { config, redis } = require('../config/dbConfig');
const oracledb = require('oracledb');
const { getQueueToAddJob } = require('../queue/jobQueue');
const { reportQueues } = require('../queue/jobQueue');

async function getReportTCO(req, res) {
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
        if (!origin || !destination || !froms || !thrus || !user_id || !branch_id || !user_session) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }
        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];
        const queueToAdd = await getQueueToAddJob(branch_id);
        const job = await queueToAdd.add({ type: 'tco', origin, destination, froms, thrus, user_id, dateStr });
        const jsonData = { origin, destination, froms, thrus, user_id, user_session, dateStr, branch_id };
        const clobJson = JSON.stringify(jsonData);
        const connection = await oracledb.getConnection(config);
        const insertProcedure = `BEGIN DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(P_USER_LOGIN => :user_name, P_NAME_FILE => :name_file, P_DURATION => :duration, P_NAMA_MODUL => :category, P_TGL_SETTING => :periode, P_STATUS => :status, P_JOB_SERVER => :job_server, P_COUNT_DATA => :datacount, P_SETTING_PERPAGE => :count_per_file, P_TOTAL_FILE => :total_file, P_BRANCH => :branch, P_LOG_JSON => :log_json); END;`;
        const insertValues = { user_name: user_id, name_file: "", duration: 0, category: "TCO", periode: `${froms} - ${thrus}`, status: "Process", job_server: job.id, datacount: 0, count_per_file: 1000000, total_file: 0, branch: branch_id, log_json: clobJson };
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();
        const logFilePath = path.join(__dirname, "../log/log_files", `JNE_REPORT_TCO_${job.id}.txt`);
        if (!fs.existsSync(path.dirname(logFilePath))) fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err + " " + err.stack + err.line);
        res.status(500).send({ success: false, message: "An error occurred while adding the job." });
    }
}

async function getReportTCI(req, res) {
    try {
        const { origin, destination, froms, thrus, user_id, TM, branch_id, user_session } = req.query;
        if (!origin || !destination || !froms || !thrus || !user_id || !TM || !user_session || !branch_id) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }
        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];
        const queueToAdd = await getQueueToAddJob(branch_id);
        const job = await queueToAdd.add({ type: 'tci', origin, destination, froms, thrus, user_id, TM, user_session, dateStr });
        const jsonData = { origin, destination, froms, thrus, user_id, TM, user_session, dateStr, branch_id };
        const clobJson = JSON.stringify(jsonData);
        const connection = await oracledb.getConnection(config);
        const insertProcedure = `BEGIN DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(P_USER_LOGIN => :user_name, P_NAME_FILE => :name_file, P_DURATION => :duration, P_NAMA_MODUL => :category, P_TGL_SETTING => :periode, P_STATUS => :status, P_JOB_SERVER => :job_server, P_COUNT_DATA => :datacount, P_SETTING_PERPAGE => :count_per_file, P_TOTAL_FILE => :total_file, P_BRANCH => :branch, P_LOG_JSON => :log_json); END;`;
        const insertValues = { user_name: user_id, name_file: "", duration: 0, category: "TCI", periode: `${froms} - ${thrus}`, status: "Process", job_server: job.id, datacount: 0, count_per_file: 1000000, total_file: 0, branch: branch_id, log_json: clobJson };
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();
        const logFilePath = path.join(__dirname, "../log/log_files", `JNE_REPORT_TCI_${job.id}.txt`);
        if (!fs.existsSync(path.dirname(logFilePath))) fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({ success: false, message: "An error occurred while adding the job." });
    }
}

async function getReportDCI(req, res) {
    try {
        const { origin, destination, froms, thrus, service, user_id, branch_id, user_session } = req.query;
        if (!origin || !destination || !froms || !thrus || !user_id || !service || !branch_id || !user_session) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }
        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];
        const queueToAdd = await getQueueToAddJob(branch_id);
        const job = await queueToAdd.add({ type: 'dci', origin, destination, froms, thrus, user_id, service, dateStr });
        const jsonData = { origin, destination, froms, thrus, user_id, service, dateStr, branch_id, user_session };
        const clobJson = JSON.stringify(jsonData);
        const connection = await oracledb.getConnection(config);
        const insertProcedure = `BEGIN DBCTC_V2.P_INS_LOG_MONITORING_EXPORT(P_USER_LOGIN => :user_name, P_NAME_FILE => :name_file, P_DURATION => :duration, P_NAMA_MODUL => :category, P_TGL_SETTING => :periode, P_STATUS => :status, P_JOB_SERVER => :job_server, P_COUNT_DATA => :datacount, P_SETTING_PERPAGE => :count_per_file, P_TOTAL_FILE => :total_file, P_BRANCH => :branch, P_LOG_JSON => :log_json); END;`;
        const insertValues = { user_name: user_id, name_file: "", duration: 0, category: "DCI", periode: `${froms} - ${thrus}`, status: "Process", job_server: job.id, datacount: 0, count_per_file: 1000000, total_file: 0, branch: branch_id, log_json: clobJson };
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();
        const logFilePath = path.join(__dirname, "../log/log_files", `JNE_REPORT_DCI_${job.id}.txt`);
        if (!fs.existsSync(path.dirname(logFilePath))) fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({ success: false, message: "An error occurred while adding the job." });
    }
}

async function getReportDCO(req, res) {
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

        if (!origin || !destination || !froms || !thrus || !user_id || !service || !branch_id || !user_session) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        const queueToAdd = await getQueueToAddJob(branch_id);

        // Menambahkan pekerjaan ke queue yang dipilih
        const job = await queueToAdd.add({
            type: 'dco',
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
            dateStr: dateStr,
            branch_id: branch_id,
        };

        const clobJson = JSON.stringify(jsonData);
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
            count_per_file: 1000000,
            total_file: 0,
            branch: branch_id, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };

        console.log("insert data dco :" +insertValues)

        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "../log/log_files",
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

        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
}

async function getReportCA(req, res) {
    try {
        const {
            branch,
            froms,
            thrus,
            user_id,
            user_session,
        } = req.query;

        if (!branch || !froms || !thrus || !user_id || !user_session) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }
        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        const queueToAdd = await getQueueToAddJob(branch);

        // Menambahkan pekerjaan ke queue yang dipilih
        const job = await queueToAdd.add({
            type: 'ca',
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
            dateStr: dateStr
        };

        const clobJson = JSON.stringify(jsonData);

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
            "../log/log_files",
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

        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
}

async function getReportRU(req, res) {
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

        if (!origin_awal || !destination || !froms || !thrus || !user_id || !services_code || !branch_id || !user_session) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        const queueToAdd = await getQueueToAddJob(branch_id);

        // Menambahkan pekerjaan ke queue yang dipilih
        const job = await queueToAdd.add({
            type: 'ru',
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
            dateStr: dateStr,
            branch_id: branch_id,
        };

        const clobJson = JSON.stringify(jsonData);

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
            count_per_file: 1000000,
            total_file: 0,
            branch: branch_id, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };
        console.log("insert data ru :" +insertValues)

        // Replace placeholders directly with bind parameters
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "../log/log_files",
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

        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
}

async function getReportDBO(req, res) {
    try {
        const {
            branch_id,
            froms,
            thrus,
            user_id,
            branch,
            user_session,
        } = req.query;

        if (!branch_id || !froms || !thrus || !user_id || !branch || !user_session) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        const queueToAdd = await getQueueToAddJob(branch_id);

        // Menambahkan pekerjaan ke queue yang dipilih
        const job = await queueToAdd.add({
            type: 'dbo',
            branch_id,
            froms,
            thrus,
            user_id,
            dateStr
        });

        const jsonData = {
            branch_id: branch_id,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            user_session: user_session,
            dateStr: dateStr,
            branch: branch,
        };

        const clobJson = JSON.stringify(jsonData);

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
            count_per_file: 1000000,
            total_file: 0,
            branch: branch, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };
        console.log("insert data DBO :" +insertValues)

        // Replace placeholders directly with bind parameters
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "../log/log_files",
            `JNE_REPORT_DBO_${job.id}.txt`
        );

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
}

async function getReportDBONA(req, res) {
    try {
        const {
            branch_id,
            froms,
            thrus,
            user_id,
            branch,
            user_session,
        } = req.query;

        if (!branch_id || !froms || !thrus || !user_id || !branch || !user_session) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        const queueToAdd = await getQueueToAddJob(branch_id);

        // Menambahkan pekerjaan ke queue yang dipilih
        const job = await queueToAdd.add({
            type: 'dbona',
            branch_id,
            froms,
            thrus,
            user_id,
            dateStr
        });

        const jsonData = {
            branch_id: branch_id,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            user_session: user_session,
            dateStr: dateStr,
            branch: branch,
        };

        const clobJson = JSON.stringify(jsonData);

        // const count_per_file = Math.ceil(estimatedDataCount / 1000000);

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
            count_per_file: 1000000,
            total_file: 0,
            branch: branch, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };
        console.log("insert data DBONA :" +insertValues)

        // Replace placeholders directly with bind parameters
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "../log/log_files",
            `JNE_REPORT_DBONA_${job.id}.txt`
        );
        const logMessage = `
            Job ID: ${job.id}
            branch_id: ${branch_id}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
}

async function getReportDBONASUM(req, res) {
    try {
        const {
            branch_id,
            froms,
            thrus,
            user_id,
            branch,
            user_session,
        } = req.query;

        if (!branch_id || !froms || !thrus || !user_id || !branch || !user_session) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        const queueToAdd = await getQueueToAddJob(branch_id);

        // Menambahkan pekerjaan ke queue yang dipilih
        const job = await queueToAdd.add({
            type: 'dbonasum',
            branch_id,
            froms,
            thrus,
            user_id,
            dateStr
        });

        const jsonData = {
            branch_id: branch_id,
            froms: froms,
            thrus: thrus,
            user_id: user_id,
            user_session: user_session,
            dateStr: dateStr,
            branch: branch,
        };

        const clobJson = JSON.stringify(jsonData);

        // const count_per_file = Math.ceil(estimatedDataCount / 1000000);

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
            category: "DBONASUM", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 1000000,
            total_file: 0,
            branch: branch, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };
        console.log("insert data DBONASUM :" +insertValues)

        // Replace placeholders directly with bind parameters
        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "../log/log_files",
            `JNE_REPORT_DBONA_${job.id}.txt`
        );
        const logMessage = `
            Job ID: ${job.id}
            branch_id: ${branch_id}
            From Date: ${froms}
            To Date: ${thrus}
            User ID: ${user_id}
            Status: Pending
            created_at: ${new Date()}
        `;

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:55:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
}

async function getReportMP(req, res) {
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

        if (!origin || !destination || !froms || !thrus || !user_id || !branch_id || !user_session) {
            return res.status(400).json({ success: false, message: "Missing required parameters" });
        }

        const today = new Date();
        const dateStr = today.toISOString().split("T")[0];

        const queueToAdd = await getQueueToAddJob(branch_id);

        // Menambahkan pekerjaan ke queue yang dipilih
        const job = await queueToAdd.add({
            // Add the job to the queue
            type: 'mp',
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
            dateStr: dateStr,
            branch_id: branch_id,
        };
        const clobJson = JSON.stringify(jsonData);

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
            category: "MP", // Kategori adalah TCO
            periode: `${froms} - ${thrus}`, // Rentang periode
            status: "Process", // Status awal adalah Pending
            job_server: job.id, // ID job
            datacount: 0,
            count_per_file: 1000000,
            total_file: 0,
            branch: branch_id, // Ganti sesuai nama cabang yang sesuai
            log_json: clobJson,
        };

        console.log("Insert data MP :" +insertValues)

        await connection.execute(insertProcedure, insertValues);
        await connection.commit();

        const logFilePath = path.join(
            __dirname,
            "../log/log_files",
            `JNE_REPORT_MP_${job.id}.txt`
        );

        if (!fs.existsSync(path.dirname(logFilePath))) {
            fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        }

        const redirectUrl = `https://dash-ctc.jne.co.id:8443/ords/f?p=101:67:${user_session}::NO::P78_USER:${user_id}`;
        res.redirect(redirectUrl);
    } catch (err) {
        console.error("Error adding job to queue:", err + " " + err.stack + err.line);
        res.status(500).send({
            success: false,
            message: "An error occurred while adding the job.",
        });
    }
}

async function downloadTCO(req, res) {
    const { jobId } = req.params;
    const category = "TCO";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function downloadTCI(req, res) {
    const { jobId } = req.params;
    const category = "TCI";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function downloadDCI(req, res) {
    const { jobId } = req.params;
    const category = "DCI";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function downloadDCO(req, res) {
    const { jobId } = req.params;
    const category = "DCO";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function downloadCA(req, res) {
    const { jobId } = req.params;
    const category = "CA";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function downloadRU(req, res) {
    const { jobId } = req.params;
    const category = "RU";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function downloadDBO(req, res) {
    const { jobId } = req.params;
    const category = "DBO";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function downloadDBONA(req, res) {
    const { jobId } = req.params;
    const category = "DBONA";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function downloadDBONASUM(req, res) {
    const { jobId } = req.params;
    const category = "DBONASUM";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function downloadMP(req, res) {
    const { jobId } = req.params;
    const category = "MP";
    try {
        const connection = await oracledb.getConnection(config);
        const query = `SELECT NAME_FILE FROM CMS_COST_TRANSIT_V2_LOG WHERE ID_JOB_REDIS = :jobId AND CATEGORY = :category`;
        const result = await connection.execute(query, { jobId: jobId, category: category });
        if (result.rows.length === 0) {
            return res.status(404).send({ success: false, message: "File not found in the database." });
        }
        const zipFileName = result.rows[0][0];
        const filePath = path.join(zipFileName);
        fs.stat(filePath, async (err, stats) => {
            if (err) {
                return res.status(404).send({ success: false, message: "File not found." });
            }
            let connection_download;
            try {
                connection_download = await oracledb.getConnection(config);
                await connection_download.execute(
                    `BEGIN DBCTC_V2.P_UPD_LOG_EXPORT_ZIPPEDDOWNLOADED(:P_ID_REDIS, :P_NAME_FILE); END;`,
                    { P_ID_REDIS: jobId, P_NAME_FILE: zipFileName.split("\\").pop() }
                );
                await connection_download.commit();
                console.log('Export updated to "Downloaded" for job ID:', jobId);
            } catch (err) {
                console.error('Error updating export process to "Downloaded":', err);
            } finally {
                if (connection_download) {
                    await connection_download.close();
                }
            }
            res.download(filePath, path.basename(filePath), (downloadErr) => {
                if (downloadErr) {
                    return res.status(500).send({ success: false, message: "Error downloading the file." });
                }
            });
        });
    } catch (err) {
        console.error("Error fetching job data or handling download:", err);
        res.status(500).send({ success: false, message: "An error occurred while processing the download." });
    }
}

async function checkPendingJobs(req, res) {
    try {
        const length = await redis.llen('pending_jobs');
        res.json({message: `There are ${length} jobs in the pending_jobs queue.`});
    } catch (error) {
        console.error('Error checking pending jobs length:', error);
        res.status(500).json({error: 'Failed to check pending jobs length'});
    }
}

async function getPendingJobs(req, res) {
    try {
        const jobs = await redis.lrange('pending_jobs', 0, -1); // Ambil semua job dari antrian pending
        const jobData = jobs.map(job => JSON.parse(job));  // Parse the JSON data for each job
        res.json({pendingJobs: jobData});
    } catch (error) {
        console.error('Error retrieving pending jobs:', error);
        res.status(500).json({error: 'Failed to retrieve pending jobs'});
    }
}

async function clean(req, res) {
    try {
        for (const queue of reportQueues) {
            await queue.clean(0, 'completed');
            await queue.clean(0, 'failed');
            await queue.clean(0, 'wait');
            await queue.clean(0, 'active');
            await queue.clean(0, 'delayed');
        }
        res.json({ success: true, message: 'All queues cleaned.' });
    } catch (error) {
        console.error('Error cleaning queues:', error);
        res.status(500).json({ success: false, message: 'Failed to clean queues.' });
    }
}

module.exports = {
    getReportTCO,
    getReportTCI,
    getReportDCI,
    getReportDCO,
    getReportCA,
    getReportRU,
    getReportDBO,
    getReportDBONA,
    getReportDBONASUM,
    getReportMP,
    downloadTCO,
    downloadTCI,
    downloadDCI,
    downloadDCO,
    downloadCA,
    downloadRU,
    downloadDBO,
    downloadDBONA,
    downloadDBONASUM,
    downloadMP,
    checkPendingJobs,
    getPendingJobs,
    clean,
};
