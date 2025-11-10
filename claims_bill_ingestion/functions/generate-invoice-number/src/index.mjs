import {initDbPool} from './db-config.mjs';
 
let connection;
const setupConnection = async () => {
    console.log(connection,'connection');
 
    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
    }
};
 
// Lambda handler
export const handler = async (event, context) => {
    console.log('In Generate-Invoice-Number');
    console.log('Event', event);
    console.log('context.aws_request_id', context.awsRequestId);
    await setupConnection();
 
    // Check if the request includes query string parameters, which should not be accepted
    if (event.queryStringParameters) {
        return generateErrorResponse(400, 'Bad Request: Query string parameters are not allowed', context.awsRequestId);
    }
    if (event.body) {
        return generateErrorResponse(400, 'Bad Request: Body not allowed', context.awsRequestId);
    }
 
    // Set numIncrements to 1 by default if not provided
    let numIncrements = event.numIncrements ?? 1;
 
    // Validate numIncrements
    if (isNaN(numIncrements)) {
        return generateErrorResponse(400, 'Bad Request: Invalid value, must be numeric', context.awsRequestId);
    }
 
    numIncrements = Number(numIncrements);
 
    if (numIncrements <= 0) {
        return generateErrorResponse(400, 'Bad Request: Invalid parameter value, must be greater than 0', context.awsRequestId);
    }
 
    console.log('numIncrements', numIncrements);
 
    try {
        const result = await bulkInsertInvoices(numIncrements);
        if (result.statusCode === 200) {
            const responseData = numIncrements === 1 ?
                { invoiceNumber: result.body[0].invoice_number.toString() } :
                { invoiceNumber: result.body };
 
            if (numIncrements == 1){
                return {
                    statusCode: 200,
                    body: JSON.stringify({
                        status: 'success',
                        message: "Request processed successfully",
                        data: responseData,
                        transactionId: context.awsRequestId.toString()
                    })
                };
            }
            else if (numIncrements > 1){
                return {
                    statusCode: 200,
                    body: {
                        status: 'success',
                        message: "Request processed successfully",
                        data: responseData,
                        transactionId: context.awsRequestId.toString()
                    }
                };
            }
           
        } else {
            return generateErrorResponse(result.statusCode, 'Internal Server Error', context.awsRequestId, result.body.message);
        }
    } catch (error) {
        console.error('Error in handler:', error);
        return generateErrorResponse(500, 'Internal Server Error', context.awsRequestId, error.message);
    }
};
 
// Function to insert bulk invoices
async function bulkInsertInvoices(numInvoices) {
    try {
        const timestamps = await generateTimestamps(numInvoices);
 
        const sql = `INSERT INTO ${process.env.DB}.${process.env.TABLE_NAME} (created_at) VALUES ?`;
        const values = timestamps.map(timestamp => [timestamp]);
 
        const [result] = await connection.query(sql, [values]);
        console.log(`${result.affectedRows} rows inserted.`);
       
        const [rows] = await connection.query(`SELECT invoice_number, created_at FROM ${process.env.DB}.${process.env.TABLE_NAME} ORDER BY invoice_number DESC LIMIT ?`, [numInvoices]);
        console.log('rows', rows);
 
        return {
            statusCode: 200,
            body: rows
        };
    } catch (error) {
        console.error('Error during generating invoices:', error);
        return generateErrorResponse(500, 'Failed to generate invoices.', '', error.message);
    } finally {
        connection.release();
    }
}
 
// Function to generate timestamps
async function generateTimestamps(numInvoices) {
    const timestamps = [];
    const currentDate = new Date().toISOString().slice(0, 19).replace('T', ' ');
    for (let i = 1; i <= numInvoices; i++) {
        timestamps.push(currentDate);
    }
    return timestamps;
}
 
// Function to generate error response
function generateErrorResponse(statusCode, errorMessage, transactionId, errorDescription = errorMessage) {
    return {
        statusCode: statusCode,
        body: JSON.stringify({
            status: "error",
            errorMessage: errorMessage.toString(),
            errorDescription: errorDescription.toString(),
            transactionId: transactionId.toString()
        })
    };
}