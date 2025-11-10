import { initDbPool } from './db-config.mjs';
 
let connection;
 
const setupConnection = async () => {
    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
    }
};
 
// Lambda function handler
export const handler = async (event, context) => {
  try {
    const requestBody = JSON.parse(event.body);
    console.log('requestBody',requestBody);
    // const requestBody = event;
    const allowedFields = ['invoiceNumber', 'claimNumber', 'dateCreatedFrom', 'dateCreatedTo', 'dateUpdatedFrom', 'dateUpdatedTo'];
    const requestFields = Object.keys(requestBody);
    for (const field of requestFields) {
      if (!allowedFields.includes(field)) {
        return {
            statusCode: 400, // Bad Request
            body: JSON.stringify({
                status: 'error',
                errorMessage: "Invalid request body",
                errorDescription: JSON.stringify({ message: `Invalid field: ${field}` }),
                transactionId: context.awsRequestId
            }),
        };
      }
    }
 
    await setupConnection();
    const {
      invoiceNumber,
      claimNumber,
      dateCreatedFrom,
      dateCreatedTo,
      dateUpdatedFrom,
      dateUpdatedTo,
    } = requestBody;
 
    console.log(invoiceNumber, claimNumber, dateCreatedFrom, dateCreatedTo, dateUpdatedFrom, dateUpdatedTo);
    let whereConditions = [];
    let queryParams = [];
 
    if (invoiceNumber) {
      whereConditions.push('t1.txm_invoice_number = ?');
      queryParams.push(invoiceNumber);
    }
 
    if (claimNumber) {
      whereConditions.push('t1.txm_claim_number = ?');
      queryParams.push(claimNumber);
    }
 
    if (dateCreatedFrom && dateCreatedTo) {
      whereConditions.push('t2.create_timestamp BETWEEN ? AND ?');
      queryParams.push(dateCreatedFrom);
      queryParams.push(dateCreatedTo);
    }
 
    if (dateUpdatedFrom && dateUpdatedTo) {
      whereConditions.push('t2.update_timestamp BETWEEN ? AND ?');
      queryParams.push(dateUpdatedFrom);
      queryParams.push(dateUpdatedTo);
    }
 
    console.log('whereConditions', whereConditions);
    console.log('queryParams', queryParams);

    if (queryParams.length == 0){
      return {
        statusCode: 400, // Internal server error
        body: JSON.stringify({
            status: 'error',
            errorMessage: 'Invalid request',
            errorDescription: 'Please send valid request body',
            transactionId: context.awsRequestId
        }),
    };
    }
 
    const whereClause = whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : '';
 
    console.log('whereClause', whereClause);
    const query = `
      SELECT t2.create_timestamp, t1.txm_invoice_number, t2.update_timestamp, t1.txm_claim_number, t1.form_type, t1.trading_partner, t1.source, t1.unique_bill_id, t2.status, t2.status_type, t4.provider_number, t4.vendor_number FROM ${process.env.DB_1}.${process.env.TABLE_1} AS t1 INNER JOIN ${process.env.DB_1}.${process.env.TABLE_2} AS t2 ON t1.txm_invoice_number = t2.txm_invoice_number LEFT OUTER JOIN ${process.env.DB_1}.${process.env.TABLE_3} t3 ON t1.bill_id = t3.bill_id LEFT OUTER JOIN ${process.env.DB_2}.${process.env.TABLE_3} t4 on t3.provider_number = t4.provider_number ${whereClause}
    `;
 
    console.log('query', query);
 
    const [table1Records] = await connection.query(query, queryParams);
    console.log('table1Records', table1Records);
    let finalResults = [];
    for (let i = 0; i<table1Records.length; i++){
        if (table1Records[i].trading_partner == 'jopari'){
            let finalObject = {};
            finalObject.invoiceNumber = table1Records[i].txm_invoice_number ? table1Records[i].txm_invoice_number.toString(): '';
            finalObject.claimNumber = table1Records[i].txm_claim_number;
            finalObject.source = table1Records[i].trading_partner;
            finalObject.sourceID = table1Records[i].unique_bill_id ? table1Records[i].unique_bill_id.toString(): '';
            finalObject.billType = table1Records[i].form_type;
            finalObject.vendorNumber = table1Records[i].vendor_number;
            finalObject.providerNumber = table1Records[i].provider_number;
            finalObject.status = table1Records[i].status;
            finalObject.statusType = table1Records[i].status_type;
            finalObject.dateCreated = table1Records[i].create_timestamp;
            finalObject.dateUpdated = table1Records[i].update_timestamp;
            
            
            // console.log('finalObject', finalObject);
            finalResults.push(finalObject);
        }
        else if (table1Records[i].trading_partner == 'optum'){
            let finalObject = {};
            finalObject.invoiceNumber = table1Records[i].txm_invoice_number ? table1Records[i].txm_invoice_number.toString(): '';
            finalObject.claimNumber = table1Records[i].txm_claim_number;
            finalObject.source = table1Records[i].trading_partner;
            finalObject.sourceID = table1Records[i].unique_bill_id ? table1Records[i].unique_bill_id.toString(): '';
            finalObject.billType = table1Records[i].form_type;
            finalObject.vendorNumber = table1Records[i].vendor_number;
            finalObject.providerNumber = table1Records[i].provider_number;
            finalObject.status = table1Records[i].status;
            finalObject.statusType = table1Records[i].status_type;
            finalObject.dateCreated = table1Records[i].create_timestamp;
            finalObject.dateUpdated = table1Records[i].update_timestamp;
            
            // console.log('finalObject', finalObject);
            finalResults.push(finalObject);
        }
        else if (table1Records[i].trading_partner == 'align'){
            let finalObject = {};
            finalObject.invoiceNumber = table1Records[i].txm_invoice_number ? table1Records[i].txm_invoice_number.toString(): '';
            finalObject.claimNumber = table1Records[i].txm_claim_number;
            finalObject.source = table1Records[i].trading_partner;
            finalObject.sourceID = table1Records[i].unique_bill_id ? table1Records[i].unique_bill_id.toString(): '';
            finalObject.billType = table1Records[i].form_type;
            finalObject.vendorNumber = table1Records[i].vendor_number;
            finalObject.providerNumber = table1Records[i].provider_number;
            finalObject.status = table1Records[i].status;
            finalObject.statusType = table1Records[i].status_type;
            finalObject.dateCreated = table1Records[i].create_timestamp;
            finalObject.dateUpdated = table1Records[i].update_timestamp;
            // console.log('finalObject', finalObject);
            finalResults.push(finalObject);
        }
        else if (table1Records[i].trading_partner == 'techhealth'){
            let finalObject = {};
            finalObject.invoiceNumber = table1Records[i].txm_invoice_number ? table1Records[i].txm_invoice_number.toString(): '';
            finalObject.claimNumber = table1Records[i].txm_claim_number;
            finalObject.source = table1Records[i].trading_partner;
            finalObject.sourceID = table1Records[i].unique_bill_id ? table1Records[i].unique_bill_id.toString(): '';
            finalObject.billType = table1Records[i].form_type;
            finalObject.vendorNumber = table1Records[i].vendor_number;
            finalObject.providerNumber = table1Records[i].provider_number;
            finalObject.status = table1Records[i].status;
            finalObject.statusType = table1Records[i].status_type;
            finalObject.dateCreated = table1Records[i].create_timestamp;
            finalObject.dateUpdated = table1Records[i].update_timestamp;
            
            // console.log('finalObject', finalObject);
            finalResults.push(finalObject);
        }
        else if (table1Records[i].source == 'eci'){
            let finalObject = {};
            finalObject.invoiceNumber = table1Records[i].txm_invoice_number ? table1Records[i].txm_invoice_number.toString(): '';
            finalObject.claimNumber = table1Records[i].txm_claim_number;
            finalObject.source = table1Records[i].source;
            finalObject.sourceID = table1Records[i].txm_invoice_number ? table1Records[i].txm_invoice_number.toString(): '';
            finalObject.billType = table1Records[i].form_type;
            finalObject.vendorNumber = table1Records[i].vendor_number;
            finalObject.providerNumber = table1Records[i].provider_number;
            finalObject.status = table1Records[i].status;
            finalObject.statusType = table1Records[i].status_type;
            finalObject.dateCreated = table1Records[i].create_timestamp;
            finalObject.dateUpdated = table1Records[i].update_timestamp;
            
            // console.log('finalObject', finalObject);
            finalResults.push(finalObject);
        }
        else if (table1Records[i].source == 'kofax'){
            let finalObject = {};
            finalObject.invoiceNumber = table1Records[i].txm_invoice_number ? table1Records[i].txm_invoice_number.toString(): '';
            finalObject.claimNumber = table1Records[i].txm_claim_number;
            finalObject.source = table1Records[i].source;
            finalObject.sourceID = table1Records[i].txm_invoice_number ? table1Records[i].txm_invoice_number.toString(): '';
            finalObject.billType = table1Records[i].form_type;
            finalObject.vendorNumber = table1Records[i].vendor_number;
            finalObject.providerNumber = table1Records[i].provider_number;
            finalObject.status = table1Records[i].status;
            finalObject.statusType = table1Records[i].status_type;
            finalObject.dateCreated = table1Records[i].create_timestamp;
            finalObject.dateUpdated = table1Records[i].update_timestamp;
            
            // console.log('finalObject', finalObject);
            finalResults.push(finalObject);
        }
    }
 
 
    console.log('finalResults', finalResults);
    let responseRecord;
    if (finalResults.length > 0){
        responseRecord = `Total ${finalResults.length} records found`;
    }
    else{
        responseRecord = 'No records found for given search criteria';
    }
    return {
        statusCode: 200,
        body: JSON.stringify({
            status: 'success',
            message: responseRecord,
            data: finalResults,
            transactionId: context.awsRequestId
        }),
    };
   
  } catch (error) {
    console.error('Error: ', error);
        return {
            statusCode: 500, // Internal server error
            body: JSON.stringify({
                status: 'error',
                errorMessage: 'Internal server error',
                errorDescription: error.message,
                transactionId: context.awsRequestId
            }),
        };
    }
};