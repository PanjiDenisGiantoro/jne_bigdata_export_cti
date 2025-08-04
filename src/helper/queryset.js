// In helper/queryset.js
const querysets = {
    getTCOReportQuery(whereClause = '', additionalConditions = '') {
        return `
            SELECT
                '''' || AWB_NO AS CONNOTE_NUMBER,
                TO_CHAR(AWB_DATE, 'DD/MM/YYYY') AS CONNOTE_DATE,
                TO_CHAR(AWB_DATE, 'HH:MI:SS AM') AS TIME_CONNOTE_DATE,
                CUST_ID,
                SERVICES_CODE AS SERVICE_CONNOTE,
                OUTBOND_MANIFEST_NO AS OUTBOND_MANIFEST_NUMBER,
                TO_CHAR(OUTBOND_MANIFEST_DATE, 'DD/MM/YYYY') AS OUTBOND_MANIFEST_DATE,
                TO_CHAR(OUTBOND_MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_OUTBOND_MANIFEST_DATE,
                ORIGIN,
                DESTINATION,
                ZONA_DESTINATION,
                OUTBOND_MANIFEST_ROUTE AS MANIFEST_ROUTE,
                TRANSIT_MANIFEST_NO AS TRANSIT_MANIFEST_NUMBER,
                TO_CHAR(TRANSIT_MANIFEST_DATE, 'DD/MM/YYYY') AS TRANSIT_MANIFEST_DATE,
                TO_CHAR(TRANSIT_MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_TRANSIT_MANIFEST_DATE,
                TRANSIT_MANIFEST_ROUTE,
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
            FROM CMS_COST_TRANSIT_V2 CT
            ${whereClause} 
            ${additionalConditions}
            GROUP BY
                ROWNUM, CUST_ID, OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, 
                TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE, BAG_NO, AWB_NO, 
                SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, 
                TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE, SMU_NUMBER, 
                FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
            ORDER BY AWB_NO ASC
        `;
    },
    getTCIReportQuery(whereClause = '', additionalConditions = '') {
        return `
        SELECT '''' || AWB_NO AS CONNOTE_NUMBER,
                    -- AWB_DATE AS CONNOTE_DATE,
                    TO_CHAR(AWB_DATE, 'DD/MM/YYYY') AS AWB_DATE, -- Format tanggal
                    TO_CHAR(AWB_DATE, 'HH:MI:SS AM') AS TIME_AWB_DATE, -- Format tanggal
                    CUST_ID,
                    SERVICES_CODE AS SERVICE_CONNOTE,
                    OUTBOND_MANIFEST_NO AS OUTBOND_MANIFEST_NUMBER,
                    TO_CHAR(OUTBOND_MANIFEST_DATE, 'DD/MM/YYYY') AS OUTBOND_MANIFEST_DATE, -- Format tanggal
                    TO_CHAR(OUTBOND_MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_OUTBOND_MANIFEST_DATE, -- Format tanggal
                    ORIGIN,
                    DESTINATION,
                       ZONA_DESTINATION,
--                        (SELECT CITY_ZONA_CTC FROM CMS_CITY_CTC@DB219 WHERE CITY_CODE_CTC = CT.DESTINATION) AS ZONA_DESTINATION,  -- Subquery untuk ZONA_DESTINATION
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
                FROM CMS_COST_TRANSIT_V2 CT ${whereClause} ${additionalConditions}
                        GROUP BY CUST_ID,
                        OUTBOND_MANIFEST_ROUTE, OUTBOND_MANIFEST_NO, TRANSIT_MANIFEST_ROUTE, MODA, MODA_TYPE,
                        BAG_NO, AWB_NO, SERVICES_CODE, OUTBOND_MANIFEST_DATE, ACT_WEIGHT, CNOTE_WEIGHT,
                        ORIGIN, DESTINATION, PRORATED_WEIGHT, AWB_DATE, TRANSIT_MANIFEST_NO, TRANSIT_MANIFEST_DATE,
                        SMU_NUMBER, FLIGHT_NUMBER, BRANCH_TRANSPORTER, SERVICE_BAG, ZONA_DESTINATION
                order by CONNOTE_NUMBER ASC`;
    },
    getDCIReportQuery(whereClause = '', additionalConditions = '') {
        return `
            SELECT '''' || CNOTE_NO                      AS CNOTE_NO,
                   TO_CHAR(CNOTE_DATE, 'DD/MM/YYYY')     AS CNOTE_DATE,
                   TO_CHAR(CNOTE_DATE, 'HH:MI:SS AM')    AS TIME_CNOTE_DATE,
                   ORIGIN,
                   DESTINATION,
                   ZONA_DESTINATION,
                   SERVICES_CODE,
                   NVL(QTY, 0)                           AS QTY,
                   CASE
                       WHEN WEIGHT = 0 THEN 0
                       WHEN WEIGHT < 1 THEN 1
                       WHEN RPAD(REGEXP_SUBSTR(WEIGHT, '[[:digit:]]+$'), 3, 0) > 300 THEN CEIL(WEIGHT)
                       ELSE FLOOR(WEIGHT)
                       END                                  WEIGHT,
                   NVL(AMOUNT, 0)                        AS AMOUNT,
                   MANIFEST_NO,
                   TO_CHAR(MANIFEST_DATE, 'DD/MM/YYYY')  AS MANIFEST_DATE,
                   TO_CHAR(MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_MANIFEST_DATE,
                   NVL(DELIVERY, 0)                      AS DELIVERY,
                   NVL(DELIVERY_SPS, 0)                  AS DELIVERY_SPS,
                   NVL(TRANSIT, 0)                       AS BIAYA_TRANSIT,
                   NVL(LINEHAUL_FIRST, 0)                AS LINEHAUL_FIRST,
                   NVL(LINEHAUL_NEXT, 0)                 AS LINEHAUL_NEXT,
                   FLAG_MULTI,
                   FLAG_CANCEL
            FROM CMS_COST_DELIVERY_V2 ${whereClause} ${additionalConditions}`;
    },
    getDCOReportQuery(whereClause = '', additionalConditions = '') {
        return `
            SELECT '''' || CNOTE_NO                      AS CNOTE_NO,
                   TO_CHAR(CNOTE_DATE, 'DD/MM/YYYY')     AS CNOTE_DATE,
                   TO_CHAR(CNOTE_DATE, 'HH:MI:SS AM')    AS TIME_CNOTE_DATE,
                   ORIGIN,
                   DESTINATION,
                   NVL(QTY, 0)                           AS QTY,
                   ZONA_DESTINATION,
                   SERVICES_CODE,
                   CASE
                       WHEN WEIGHT = 0 THEN 0
                       WHEN WEIGHT < 1 THEN 1
                       WHEN RPAD(REGEXP_SUBSTR(WEIGHT, '[[:digit:]]+$'), 3, 0) > 300 THEN CEIL(WEIGHT)
                       ELSE FLOOR(WEIGHT)
                       END                                  WEIGHT,
                   NVL(AMOUNT, 0)                        AS AMOUNT,
                   MANIFEST_NO,
                   TO_CHAR(MANIFEST_DATE, 'DD/MM/YYYY')  AS MANIFEST_DATE,      -- Format tanggal
                   TO_CHAR(MANIFEST_DATE, 'HH:MI:SS AM') AS TIME_MANIFEST_DATE, -- Format tanggal
                   NVL(DELIVERY, 0)                      AS DELIVERY,
                   NVL(DELIVERY_SPS, 0)                  AS DELIVERY_SPS,
                   NVL(TRANSIT, 0)                       AS BIAYA_TRANSIT,
                   NVL(LINEHAUL_FIRST, 0)                AS LINEHAUL_FIRST,
                   NVL(LINEHAUL_NEXT, 0)                 AS LINEHAUL_NEXT,
                   FLAG_MULTI,
                   FLAG_CANCEL
            FROM CMS_COST_DELIVERY_V2 ${whereClause} ${additionalConditions}`;
    },
    getRUReportQuery(whereClause = '', additionalConditions = '') {
        return `
            SELECT RT_CNOTE_NO,
                   TO_CHAR(RT_CRDATE_RT, 'DD/MM/YYYY HH:MI:SS AM')     AS RT_CRDATE_RT,
                   TO_CHAR(RT_CRDATE_RT, 'HH:MI:SS AM')                AS TIME_RT_CRDATE_RT,
                   RT_CNOTE_ASLI,
                   RT_CNOTE_ASLI_ORIGIN,
                   RT_MANIFEST,
                   TO_CHAR(RT_MANIFEST_DATE, 'DD/MM/YYYY HH:MI:SS AM') AS RT_MANIFEST_DATE,
                   TO_CHAR(RT_MANIFEST_DATE, 'HH:MI:SS AM')            AS TIME_RT_MANIFEST_DATE,
                   RT_SERVICES_CODE,
                   RT_CNOTE_ORIGIN,
                   RT_CNOTE_DEST,
                   RT_CNOTE_DEST_NAME,
                   RT_CNOTE_QTY,
                   RT_CNOTE_WEIGHT
            FROM V_OPS_RETURN_UNPAID ${whereClause} ${additionalConditions}`;
    },
    getDBOReportQuery(whereClause = '', additionalConditions = '') {
        return `
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
                from CMS_COST_DELIVERY_V2 ${whereClause} ${additionalConditions}
                `
    },
    getDBONAReportQuery(whereClause = '', additionalConditions = '') {
        return `
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
                from CMS_COST_DELIVERY_V2 ${whereClause} ${additionalConditions}
        `;
    }
};

module.exports = querysets;