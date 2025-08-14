const oracledb = require('oracledb');
const { config, config_jnebill } = require('../config/dbConfig');
const { buildWhereClause } = require('../helper/whereClause');

async function estimateDataCount({origin, destination, froms, thrus, user_id}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, async (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;
                    // Build where clause using the helper
                    const { whereClause, bindParams } = await buildWhereClause(
                        { origin, destination, froms, thrus },
                        'TCO'  // Using TCO mapping for this function
                    );
                    
                    // Add additional conditions specific to this query
                    const additionalConditions = `
                        AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                        AND CNOTE_WEIGHT > 0
                    `;

                    // Query untuk estimasi jumlah data
                    connection.execute(`
                        SELECT COUNT(*) as DATA_COUNT FROM (
                            SELECT
                                ROWNUM, CUST_ID, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                                BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                                ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                                SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                            FROM CMS_COST_TRANSIT_V2
                                    ${whereClause}
                                    ${additionalConditions}
                            GROUP BY
                                ROWNUM, CUST_ID, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
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
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, async (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;
                    
                    // Build where clause using the helper
                    const { whereClause, bindParams } = await buildWhereClause(
                        { origin, destination, froms, thrus, TM },
                        'TCI'  // Using TCI mapping for this function
                    );
                    
                    // Add additional conditions specific to this query
                    const additionalConditions = `
                        AND OUTBOND_MANIFEST_ROUTE <> TRANSIT_MANIFEST_ROUTE
                        AND CNOTE_WEIGHT > 0
                    `;

                    // Query untuk estimasi jumlah data
                    connection.execute(`
                        SELECT COUNT(*) as DATA_COUNT FROM (
                            SELECT
                                ROWNUM, CUST_ID, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                                BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                                ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                                SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                            FROM CMS_COST_TRANSIT_V2
                            ${whereClause}
                            ${additionalConditions}
                            GROUP BY
                                ROWNUM, CUST_ID, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
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
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, async (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;
                    
                    // Build where clause using the helper
                    const { whereClause, bindParams } = await buildWhereClause(
                        { origin, destination, froms, thrus, service },
                        'DCI'  // Using DCI mapping for this function
                    );
                    
                    // Add additional conditions specific to this query
                    const additionalConditions = `
                        AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
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
                    `;

                    // Query untuk estimasi jumlah data
                    connection.execute(`
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM CMS_COST_DELIVERY_V2 
                        ${whereClause}
                        ${additionalConditions}
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
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, async (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;
                    
                    // Build where clause using the helper
                    const { whereClause, bindParams } = await buildWhereClause(
                        { origin, destination, froms, thrus, service },
                        'DCO'  // Using DCO mapping for this function
                    );
                    
                    // Add additional conditions specific to this query
                    const additionalConditions = `
                        AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
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
                    `;

                    // Query untuk estimasi jumlah data
                    connection.execute(`
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM CMS_COST_DELIVERY_V2 
                        ${whereClause}
                        ${additionalConditions}
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

async function estimateDataCountDCIV3({origin, destination, froms, thrus, service, user_id}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, async (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;
                    
                    // Build where clause using the helper
                    const { whereClause, bindParams } = await buildWhereClause(
                        { origin, destination, froms, thrus, service },
                        'DCI'  // Using DCI mapping for this function
                    );
                    
                    // Add additional conditions specific to this query
                    const additionalConditions = `
                        AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
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
                    `;

                    // Query untuk estimasi jumlah data
                    connection.execute(`
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM CMS_COST_DELIVERY_V3 
                        ${whereClause}
                        ${additionalConditions}
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

async function estimateDataCountDCOV3({origin, destination, froms, thrus, service, user_id}) {
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config, async (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                } else {
                    connection = conn;
                    
                    // Build where clause using the helper
                    const { whereClause, bindParams } = await buildWhereClause(
                        { origin, destination, froms, thrus, service },
                        'DCO'  // Using DCO mapping for this function
                    );
                    
                    // Add additional conditions specific to this query
                    const additionalConditions = `
                        AND SUBSTR(ORIGIN,1,3) <> SUBSTR(DESTINATION,1,3)
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
                    `;

                    // Query untuk estimasi jumlah data
                    connection.execute(`
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM CMS_COST_DELIVERY_V3 
                        ${whereClause}
                        ${additionalConditions}
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
    return new Promise(async (resolve, reject) => {
        let connection;
        try {
            oracledb.getConnection(config_jnebill, async (err, conn) => {
                if (err) {
                    reject('Error connecting to database: ' + err.message);
                    return;
                }
                connection = conn;
                let query = '';
                const bindParams = {};
                
                // Build where clause using the helper for BTH000 branch
                if (branch === 'BTH000') {
                    const { whereClause, bindParams: clauseBindParams } = await buildWhereClause(
                        { branch, froms, thrus },
                        'CABTM'  // Using CABTM mapping for BTH000 branch
                    );
                    
                    // Copy bind parameters
                    Object.assign(bindParams, clauseBindParams);
                    
                    // Add additional conditions specific to this query
                    const additionalConditions = `
                        AND CUST_TYPE IN ('995','996','997','994')
                        AND NVL((SELECT CUST_KP FROM REPJNE.ECONNOTE_CUST E2 
                               WHERE CUST_BRANCH = C.CNOTE_BRANCH_ID 
                               AND B.CUST_ID = E2.CUST_ID 
                               AND CUST_KP = 'N'), 'N') = 'N'
                        AND NVL(C.CNOTE_CANCEL, 'N') = 'N'
                    `;

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
                                    SELECT CNOTE_NO AS HAWB
                                    FROM REPJNE.CMS_CNOTE_CN23_HYBRID
                                ) E ON CNOTE_NO = HAWB
                            ${whereClause}
                            ${additionalConditions}
                        )
                    `;
                } else {
                    // For non-BTH000 branches, use the CA mapping
                    const { whereClause, bindParams: clauseBindParams } = await buildWhereClause(
                        { branch, froms, thrus },
                        'CA'  // Using CA mapping for other branches
                    );
                    
                    // Copy bind parameters
                    Object.assign(bindParams, clauseBindParams);
                    
                    // Add additional conditions specific to this query
                    const additionalConditions = `
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

                    query = `
                        SELECT COUNT(*) AS DATA_COUNT
                        FROM CMS_APICUST_HYBRID A
                            JOIN CMS_CUST B ON A.HYBRID_CUST = B.CUST_ID AND A.HYBRID_BRANCH = B.CUST_BRANCH
                            JOIN CMS_CNOTE@DBS2 C ON A.APICUST_CNOTE_NO = C.CNOTE_NO
                            JOIN CMS_CUST D ON D.CUST_BRANCH = A.HYBRID_BRANCH AND D.CUST_ID = A.APICUST_CUST_NO
                            ${whereClause}
                            ${additionalConditions}
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
    return new Promise(async (resolve, reject) => {
        oracledb.getConnection(config, async (err, connection) => {
            if (err) {
                return reject('Error connecting to database: ' + err.message);
            }

            // Build where clause using the helper
            const { whereClause, bindParams } = await buildWhereClause(
                { origin_awal, destination, froms, thrus, services_code },
                'RU'  // Using RU mapping for this function
            );
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
    return new Promise(async (resolve, reject) => {
        oracledb.getConnection(config, async (err, connection) => {
            if (err) {
                return reject('Error connecting to database: ' + err.message);
            }

            // Build where clause using the helper
            const { whereClause, bindParams } = await buildWhereClause(
                { branch_id, froms, thrus },
                'DBO'  // Using DBO mapping for this function
            );
            
            // Add additional conditions specific to this query
            const additionalConditions = "AND CUST_NA IS NULL AND SUBSTR(CNOTE_NO, 1, 2) NOT IN ('FW', 'RT')";

            const sql = `SELECT COUNT(*) AS DATA_COUNT FROM CMS_COST_DELIVERY_V2 ${whereClause} ${additionalConditions}`;

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
        oracledb.getConnection(config, async (err, connection) => {
            if (err) {
                return reject('Error connecting to database: ' + err.message);
            }
            // Build where clause using the helper
            const {whereClause, bindParams} = await buildWhereClause(
                {branch_id, froms, thrus},
                'DBO'  // Using DBO mapping for this function
            );

            // Add additional conditions specific to this query
            const additionalConditions = "AND CUST_NA IS NULL AND SUBSTR(CNOTE_NO, 1, 2) NOT IN ('FW', 'RT')";

            const sql = `SELECT COUNT(*) AS DATA_COUNT
                         FROM CMS_COST_DELIVERY_V2 ${whereClause} ${additionalConditions}`;

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


async function estimateDataCountMP({ origin, destination, froms, thrus, user_id }) {
    return new Promise((resolve, reject) => {
        oracledb.getConnection(config, async (err, connection) => {
            if (err) {
                return reject('Error connecting to database: ' + err.message);
            }
            // Build where clause using the helper
            const {whereClause, bindParams} = await buildWhereClause(
                {branch_id, froms, thrus},
                'MP'  // Using DBO mapping for this function
            );
            const sql = `SELECT COUNT(*) AS DATA_COUNT
                         FROM DBCTC_V2.REP_BIAYA_ANGKUT
                                  ${whereClause}`;

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


module.exports = {
    estimateDataCount,
    estimateDataCountTCI,
    estimateDataCountDCI,
    estimateDataCountDCO,
    estimateDataCountDCIV3,
    estimateDataCountDCOV3,
    estimateDataCountCA,
    estimateDataCountRU,
    estimateDataCountDBO,
    estimateDataCountDBONA,
    estimateDataCountMP,
}; 