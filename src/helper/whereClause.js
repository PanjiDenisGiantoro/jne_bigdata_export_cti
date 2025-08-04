const whereClauseMappings = {
    DCO: {
        origin: {
            condition: ` AND SUBSTR(ORIGIN, 1, 3) LIKE :origin`,
            bindKey: 'origin',
            format: value => value + '%'
        },
        destination: {
            condition: `AND SUBSTR(DESTINATION,1,3) LIKE :destination`,
            bindKey: 'destination',
            format: value => value + '%'
        },
        froms: {
            condition: `AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'thrus',
            format: value => value
        },
        service: {
            condition: `AND SERVICES_CODE = :service`,
            bindKey: 'service',
            format: value => value
        }
    },
    DCI: {
        origin: {
            condition: `AND SUBSTR(ORIGIN, 1, 3) LIKE SUBSTR(:origin , 1, 3)`,
            bindKey: 'origin',
            format: value => value + '%'
        },
        destination: {
            condition: `AND SUBSTR(DESTINATION,1,3) LIKE SUBSTR(:destination,1,3)`,
            bindKey: 'destination',
            format: value => value + '%'
        },
        froms: {
            condition: `AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'thrus',
            format: value => value
        },
        service: {
            condition: `AND SERVICES_CODE = :service`,
            bindKey: 'service',
            format: value => value
        }
    },
    TCI: {
        origin: {
            condition: `AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`,
            bindKey: 'origin',
            format: value => value + '%'
        },
        destination: {
            condition: `AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`,
            bindKey: 'destination',
            format: value => value + '%'
        },
        froms: {
            condition: `AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'thrus',
            format: value => value
        },
        TM: {
            condition: `AND SUBSTR(ORIGIN_TM, 1, 3) LIKE :TM`,
            bindKey: 'TM',
            format: value => value + '%'
        }
    },
    TCO: {
        origin: {
            condition: `AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`,
            bindKey: 'origin',
            format: value => value + '%'
        },
        destination: {
            condition: `AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`,
            bindKey: 'destination',
            format: value => value + '%'
        },
        froms: {
            condition: `AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND trunc(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'thrus',
            format: value => value
        },
    },
    CA: {
        branch: {
            condition: `AND C.CNOTE_BRANCH_ID = :branch`,
            bindKey: 'branch',
            format: value => value
        },
        froms: {
            condition: `AND TRUNC(C.CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-RRRR') AND TO_DATE(:thrus, 'DD-MON-RRRR')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND TRUNC(C.CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-RRRR') AND TO_DATE(:thrus, 'DD-MON-RRRR')`,
            bindKey: 'thrus',
            format: value => value
        }
    },
    CABTM: {
        branch: {
            condition: `AND C.CNOTE_BRANCH_ID = :branch`,
            bindKey: 'branch',
            format: value => value
        },
        froms: {
            condition: `AND TRUNC(C.CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-RRRR') AND TO_DATE(:thrus, 'DD-MON-RRRR')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND TRUNC(C.CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-RRRR') AND TO_DATE(:thrus, 'DD-MON-RRRR')`,
            bindKey: 'thrus',
            format: value => value
        }
    },
    RU: {
        origin_awal: {
            condition: `AND  RT_CNOTE_ASLI_ORIGIN  like :origin_awal`,
            bindKey: 'origin_awal',
            format: value => value + '%'
        },
        destination: {
            condition: `and RT_CNOTE_DEST LIKE  :destination`,
            bindKey: 'destination',
            format: value => value + '%'
        },
        froms: {
            condition: `AND trunc(RT_CRDATE_RT) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND trunc(RT_CRDATE_RT) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'thrus',
            format: value => value
        },
        service_code: {
            condition: `AND RT_SERVICES_CODE LIKE :services_code`,
            bindKey: 'services_code',
            format: value => value + '%'
        }
    },
    DBO: {
        branch_id: {
            condition: `AND  SUBSTR(BRANCH_ID,1,3) = :branch_id`,
            bindKey: 'branch_id',
            format: value => value
        },
        froms: {
            condition: `AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'thrus',
            format: value => value
        }
    },
    DBONA: {
        branch_id: {
            condition: `AND  SUBSTR(BRANCH_ID,1,3) = :branch_id`,
            bindKey: 'branch_id',
            format: value => value
        },
        froms: {
            condition: `AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND trunc(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'thrus',
            format: value => value
        }
    },
    DBONASUM: {
        branch_id: {
            condition: `AND SUBSTR(BRANCH_ID, 1, 3) = :branch_id`,
            bindKey: 'branch_id',
            format: value => value
        },
        froms: {
            condition: `AND TRUNC(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND TRUNC(CNOTE_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'thrus',
            format: value => value
        }
    },
    MP: {
        origin: {
            condition: `AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 1, 3) LIKE :origin`,
            bindKey: 'origin',
            format: value => value + '%'
        },

        destination: {
            condition: `AND SUBSTR(OUTBOND_MANIFEST_ROUTE, 9, 3) LIKE :destination`,
            bindKey: 'destination',
            format: value => value + '%'
        },
        froms: {
            condition: `AND TRUNC(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'froms',
            format: value => value
        },
        thrus: {
            condition: `AND TRUNC(AWB_DATE) BETWEEN TO_DATE(:froms, 'DD-MON-YYYY') AND TO_DATE(:thrus, 'DD-MON-YYYY')`,
            bindKey: 'thrus',
            format: value => value
        }
    }


}
async function buildWhereClause(params, mappingKey) {
    const mapping = whereClauseMappings[mappingKey];
    let whereClause = 'WHERE 1=1';
    const bindParams = {};

    for (const key in mapping) {
        const rule = mapping[key];
        const value = params[key];

        if (value && value !== '0') {
            if (rule.condition) whereClause += ` ${rule.condition}`;
            bindParams[rule.bindKey] = rule.format ? rule.format(value) : value;
        } 
    }

    return { whereClause, bindParams };
}
module.exports = {
    buildWhereClause
}