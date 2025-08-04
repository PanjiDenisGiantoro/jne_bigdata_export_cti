const oracledb = require('oracledb');
const ExcelJS = require('exceljs');
const path = require('path');
const fs = require('fs');
const { config_jnebill } = require('../src/config/dbConfig');

// Configuration
const BATCH_SIZE = 500;
const EXCEL_PATH = path.join(__dirname, '../public/cnote/insert_cnote_1.xlsx');

// Helper function to chunk array
function chunkArray(array, size) {
    const chunks = [];
    for (let i = 0; i < array.length; i += size) {
        chunks.push(array.slice(i, i + size));
    }
    return chunks;
}

// Read Excel data
async function readExcelData() {
    try {
        // Check if Excel file exists
        if (!fs.existsSync(EXCEL_PATH)) {
            throw new Error(`Excel file not found at: ${EXCEL_PATH}`);
        }

        console.log(`Reading Excel file from: ${EXCEL_PATH}`);
        
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.readFile(EXCEL_PATH);
        
        const worksheet = workbook.getWorksheet(1); // Get first worksheet
        if (!worksheet) {
            throw new Error('No worksheet found in Excel file');
        }

        const awbNumbers = [];
        
        // Read column B (AWB NUMBER) starting from row 2 (skip header)
        worksheet.eachRow((row, rowNumber) => {
            if (rowNumber > 1) { // Skip header row
                const awbNumber = row.getCell(2).value; // Column B - AWB NUMBER
                if (awbNumber && awbNumber.toString().trim() !== '') {
                    // Clean data: remove leading single quote and trim
                    let cleanAwb = awbNumber.toString().trim();
                    if (cleanAwb.startsWith("'")) {
                        cleanAwb = cleanAwb.substring(1); // Remove leading single quote
                    }
                    if (cleanAwb !== '') {
                        awbNumbers.push(cleanAwb);
                    }
                }
            }
        });

        console.log(`Total AWB numbers found: ${awbNumbers.length}`);
        console.log(`Sample cleaned AWB numbers: ${awbNumbers.slice(0, 5).join(', ')}`);
        return awbNumbers;
    } catch (error) {
        console.error('Error reading Excel file:', error);
        throw error;
    }
}

// Filter existing AWB numbers
async function filterExistingAWB(connection, awbNumbers) {
    try {
        console.log('Checking existing AWB numbers in database...');
        const batches = chunkArray(awbNumbers, BATCH_SIZE);
        const existingAwbs = new Set();
        
        for (let i = 0; i < batches.length; i++) {
            const batch = batches[i];
            console.log(`Processing batch ${i + 1}/${batches.length} (${batch.length} items)`);
            
            // Create placeholders for IN clause
            const placeholders = batch.map((_, index) => `:${index + 1}`).join(',');
            const query = `SELECT cnote_no FROM cms_cnote WHERE cnote_no IN (${placeholders})`;
            
            // Create bind variables object
            const bindVars = {};
            batch.forEach((value, index) => {
                bindVars[index + 1] = value;
            });
            
            const result = await connection.execute(query, bindVars);
            
            if (result.rows) {
                result.rows.forEach(row => {
                    existingAwbs.add(row[0]);
                });
            }
        }

        // Filter out existing AWB numbers
        const filteredAwbs = awbNumbers.filter(awb => !existingAwbs.has(awb));
        
        console.log(`Original count: ${awbNumbers.length}`);
        console.log(`Existing count: ${existingAwbs.size}`);
        console.log(`Filtered count (to be inserted): ${filteredAwbs.length}`);
        
        return filteredAwbs;
    } catch (error) {
        console.error('Error filtering existing AWB numbers:', error);
        throw error;
    }
}

// Insert AWB numbers
async function insertAWB(connection, filteredAwbs) {
    try {
        if (filteredAwbs.length === 0) {
            console.log('No new AWB numbers to insert');
            return 0;
        }

        console.log(`Starting insert process for ${filteredAwbs.length} AWB numbers...`);
        const batches = chunkArray(filteredAwbs, BATCH_SIZE);
        let totalInserted = 0;
        
        for (let i = 0; i < batches.length; i++) {
            const batch = batches[i];
            console.log(`Processing insert batch ${i + 1}/${batches.length} (${batch.length} items)`);
            
            // Create placeholders for IN clause
            const placeholders = batch.map((_, index) => `:${index + 1}`).join(',');
            const insertQuery = `
                INSERT INTO cms_cnote 
                SELECT * FROM cms_cnote@to141 
                WHERE cnote_no IN (${placeholders})
            `;
            
            // Create bind variables object
            const bindVars = {};
            batch.forEach((value, index) => {
                bindVars[index + 1] = value;
            });
            
            const result = await connection.execute(insertQuery, bindVars);
            await connection.commit();
            
            totalInserted += result.rowsAffected || 0;
            console.log(`Batch ${i + 1} completed. Rows affected: ${result.rowsAffected || 0}`);
        }
        
        console.log(`Insert process completed. Total rows inserted: ${totalInserted}`);
        return totalInserted;
    } catch (error) {
        console.error('Error inserting AWB numbers:', error);
        await connection.rollback();
        throw error;
    }
}

// Main process function
async function processAWBInsert() {
    let connection = null;
    
    try {
        console.log('=== Starting AWB Insert Process ===');
        
        // Step 1: Initialize database connection
        oracledb.initOracleClient();
        connection = await oracledb.getConnection(config_jnebill);
        console.log('Database connection established successfully');
        
        // Step 2: Read Excel data
        const awbNumbers = await readExcelData();
        
        if (awbNumbers.length === 0) {
            console.log('No AWB numbers found in Excel file');
            return;
        }
        
        // Step 3: Filter existing AWB numbers
        const filteredAwbs = await filterExistingAWB(connection, awbNumbers);
        
        // Step 4: Insert new AWB numbers
        const insertedCount = await insertAWB(connection, filteredAwbs);
        
        console.log('=== Process Completed Successfully ===');
        console.log(`Summary:`);
        console.log(`- Total AWB numbers from Excel: ${awbNumbers.length}`);
        console.log(`- New AWB numbers to insert: ${filteredAwbs.length}`);
        console.log(`- Successfully inserted: ${insertedCount || 0}`);
        
    } catch (error) {
        console.error('Process failed:', error);
        throw error;
    } finally {
        if (connection) {
            try {
                await connection.close();
                console.log('Database connection closed');
            } catch (error) {
                console.error('Error closing database connection:', error);
            }
        }
    }
}

// Main execution
async function main() {
    try {
        await processAWBInsert();
    } catch (error) {
        console.error('Main process failed:', error);
        process.exit(1);
    }
}

// Run the process if this file is executed directly
if (require.main === module) {
    main();
}

module.exports = { processAWBInsert }; 
