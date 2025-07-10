const oracledb = require('oracledb');
const { config, config_jnebill } = require('../config/dbConfig');

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
                    if (origin !== '0' ) {
                        whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`;                        bindParams.origin = origin + '%';
                    }
                    if (destination !== '0' ) {
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

                    if (origin !== '0' ) {
                        whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`;
                        bindParams.origin = origin + '%';
                    }

                    if (destination !== '0' ) {
                        whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`;
                        bindParams.destination = destination + '%';
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }

                    if (TM !== '0' ) {
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
                            )`, bindParams, (err, result) => {
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

                    if (origin !== '0' ) {
                        whereClause += ` AND SUBSTR(ORIGIN, 1, 3) like :origin`;
                        bindParams.origin = origin + '%' ;
                    }

                    if (destination !== '0' ) {
                        whereClause += ` AND SUBSTR(DESTINATION, 1, 3) like :destination`;
                        bindParams.destination = destination + '%' ;
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }

                    if (service !== '0' ) {
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

                AND SERVICES_CODE NOT IN  ('@BOX3KG','@BOX5KG','CCINTL','CCINTL2','CML_CTC','CTC','CTC-YES','CTC05','CTC08','CTC11',
                    'CTC12','CTC13','CTC15','CTC19','CTC23','CTCOKE','CTCOKE08','CTCOKE11','CTCOKE12',
                    'CTCOKE13','CTCOKE15','CTCREG','CTCREG08','CTCREG11','CTCREG13','CTCREG15','CTCSPS08',
                    'CTCSPS1','CTCSPS11','CTCSPS12','CTCSPS13','CTCSPS15','CTCSPS19','CTCSPS2','CTCSPS23',
                    'CTCTRC08','CTCTRC11','CTCVIP','CTCVVIP','CTCYES','CTCYES08','CTCYES11','CTCYES12',
                    'CTCYES13','CTCYES15','CTCYES19','CTCYES23','INT','INTL','INTL10','INTL15',
                    'INTL16','INTL19','INTL20','JKT','JKTSS','JKTYES')

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

                    if (origin !== '0' ) {
                        whereClause += ` AND SUBSTR(ORIGIN, 1, 3) like :origin`;
                        bindParams.origin = origin + '%';
                    }

                    if (destination !== '0' ) {
                        whereClause += ` AND SUBSTR(DESTINATION, 1, 3) like :destination`;
                        bindParams.destination = destination + '%';
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }

                    if (service !== '0' ) {
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
                            AND SERVICES_CODE NOT IN ('@BOX3KG','@BOX5KG','CCINTL','CCINTL2','CML_CTC','CTC','CTC-YES','CTC05','CTC08','CTC11',
                            'CTC12','CTC13','CTC15','CTC19','CTC23','CTCOKE','CTCOKE08','CTCOKE11','CTCOKE12',
                            'CTCOKE13','CTCOKE15','CTCREG','CTCREG08','CTCREG11','CTCREG13','CTCREG15','CTCSPS08',
                            'CTCSPS1','CTCSPS11','CTCSPS12','CTCSPS13','CTCSPS15','CTCSPS19','CTCSPS2','CTCSPS23',
                            'CTCTRC08','CTCTRC11','CTCVIP','CTCVVIP','CTCYES','CTCYES08','CTCYES11','CTCYES12',
                            'CTCYES13','CTCYES15','CTCYES19','CTCYES23','INT','INTL','INTL10','INTL15',
                            'INTL16','INTL19','INTL20','JKT','JKTSS','JKTYES')
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

                    if (branch !== '0' ) {
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

            if (destination !== '0' ) {
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

async function estimateDataCountDBO({branch_id, froms, thrus, user_id }) {
    return new Promise((resolve, reject) => {
        oracledb.getConnection(config, (err, connection) => {
            if (err) {
                return reject('Error connecting to database: ' + err.message);
            }

            let whereClause = "WHERE 1=1 ";
            const bindParams = {};

            // Gunakan branch_id jika tidak '0'
            if (branch_id !== '0') {
                //     like SUBSTR(BRANCH_ID,1,3)
                whereClause += "AND  SUBSTR(BRANCH_ID,1,3) = :branch_id ";
                bindParams.branch_id = branch_id ;
            }


            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
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

async function estimateDataCountDBONA({  branch_id, froms, thrus, user_id }) {
    return new Promise((resolve, reject) => {
        oracledb.getConnection(config, (err, connection) => {
            if (err) {
                return reject('Error connecting to database: ' + err.message);
            }

            let whereClause = "WHERE 1=1 ";
            const bindParams = {};


            if (branch_id !== '0' ) {
                //     like SUBSTR(BRANCH_ID,1,3)
                whereClause += "AND  SUBSTR(BRANCH_ID,1,3) = :branch_id ";
                bindParams.branch_id = branch_id ;
            }

            if (froms !== '0' && thrus !== '0') {
                whereClause += "AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY') ";
                bindParams.froms = froms;
                bindParams.thrus = thrus;
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

async function estimateDataCountMP({origin, destination, froms, thrus, user_id}) {
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
                    if (destination !== '0' ) {
                        whereClause += ` AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`;
                        bindParams.destination = destination + '%';
                    }

                    if (froms !== '0' && thrus !== '0') {
                        whereClause += ` AND TRUNC(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`;
                        bindParams.froms = froms;
                        bindParams.thrus = thrus;
                    }

                    // Filter khusus marketplace
                    whereClause += ` AND CUST_ID IN ('11666700', '80561600', '80561601', '80514305')`;

                    // Final query
                    const sql = `
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM CMS_COST_TRANSIT_V2
                        ${whereClause}
                    `;

                    connection.execute(sql, bindParams, (err, result) => {
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

module.exports = {
    estimateDataCount,
    estimateDataCountTCI,
    estimateDataCountDCI,
    estimateDataCountDCO,
    estimateDataCountCA_,
    estimateDataCountCA,
    estimateDataCountRU,
    estimateDataCountDBO,
    estimateDataCountDBONA,
    estimateDataCountMP
}; 