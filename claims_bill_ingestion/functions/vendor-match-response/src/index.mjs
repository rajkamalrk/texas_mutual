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
 
const generateErrorResponse = (transactionId, errorMessage, errorDescription, statusCode = 500) => ({
  statusCode,
  body: JSON.stringify({
    status: 'error',
    errorMessage,
    errorDescription,
    transactionId,
  }),
});
 
const generateInvoiceNotFoundResponse = (transactionId, errorMessage, errorDescription, statusCode = 404) => ({
  statusCode,
  body: JSON.stringify({
    status: 'error',
    errorMessage,
    errorDescription,
    transactionId,
  }),
});
 
const generateSuccessResponse = (transactionId, message, data) => ({
  statusCode: 200,
  body: JSON.stringify({
    status: 'success',
    message,
    data,
    transactionId,
  }),
});
 
const validateRequestFields = (requestFields, context) => {
  const { claimNumber, providerNumber } = requestFields;
  if (claimNumber && (typeof claimNumber !== 'string' || claimNumber.trim() === '')) {
    return generateErrorResponse(context.awsRequestId, 'Invalid Payload', 'Claim Number is missing or it is invalid', 400);
  }
  if (providerNumber && (typeof providerNumber !== 'string' || providerNumber.trim() === '')) {
    return generateErrorResponse(context.awsRequestId, 'Invalid Payload', 'Provider Number is missing or it is invalid', 400);
  }
  if (!claimNumber && !providerNumber) {
    return generateErrorResponse(context.awsRequestId, 'Invalid Payload', 'Either claimNumber or providerNumber is missing or it is invalid', 400);
  }
  return null;
};
 
const updateMatchResponse = async (tableName, invoiceNumber, matchFound, statusType, responseValue, context) => {
  console.log('updateMatchResponse invoice number', `${invoiceNumber}`);
  let invoiceRes = `${invoiceNumber}`;
  console.log('updateMatchResponse invoice number2', invoiceRes);
  const now = new Date();
  const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
  const updatedOn = createdOn;
 
  try {
    let query = `SELECT txm_invoice_number, bill_id FROM ${process.env.DB_1}.${process.env.TABLE_1} WHERE txm_invoice_number = ${invoiceRes}`;
    console.log('query', query);
    const [rowsValidate] = await connection.query(`SELECT txm_invoice_number, bill_id FROM ${process.env.DB_1}.${process.env.TABLE_1} WHERE txm_invoice_number = ${invoiceRes}`);
    console.log('rowsValidate', rowsValidate);
    if (rowsValidate.length < 1) {
      console.log('rowsValidate', rowsValidate);
      return { statusCode: 500, body: 'Invoice number not available.' };
    }
   
    let queryCheck = `SELECT * FROM ${process.env.DB_1}.${process.env.TABLE_2} WHERE txm_invoice_number = ${invoiceRes}`;
    console.log('queryCheck', queryCheck);
    const [rowsValidateCheck] = await connection.query(`SELECT * FROM ${process.env.DB_1}.${process.env.TABLE_2} WHERE txm_invoice_number = ${invoiceRes}`);
    console.log('rowsValidateCheck', rowsValidateCheck[0].status_type);
   
    if (rowsValidateCheck[0].status_type == 'VENDOR_MATCH_MANUAL') {
      statusType = 'VENDOR_MATCH_MANUAL';
      console.log('statusType', statusType);
    }
   
    console.log('statusType', statusType);
    console.log('responseValue', responseValue);
 
    let responseTable = '';
    let dbField = '';
    let updateQuery = '';
    let responseField = '';
    let queryValue = '';
    if (statusType === 'CLAIM_MATCH_MANUAL' && responseValue !== 'DELETE'){
      responseTable = process.env.TABLE_1;
      dbField = 'reported_claim_number';
      responseField = responseValue;
      updateQuery = `UPDATE ${process.env.DB_1}.${responseTable} SET ${dbField} = ? WHERE txm_invoice_number = ?`
      queryValue = [responseField, invoiceNumber];
    }
    else if (statusType === 'CLAIM_MATCH_MANUAL' && responseValue === 'DELETE'){
      responseTable = process.env.TABLE_1;
      dbField = 'txm_invoice_number';
      responseField = invoiceNumber;
      updateQuery = `UPDATE ${process.env.DB_1}.${responseTable} SET ${dbField} = ? WHERE txm_invoice_number = ?`
      queryValue = [responseField, invoiceNumber];
    }
    else if (statusType === 'VENDOR_MATCH_MANUAL' && responseValue !== 'DELETE'){
      responseTable = process.env.TABLE_4;
      dbField = 'provider_number';
      responseField = responseValue;
      updateQuery = `UPDATE ${process.env.DB_1}.${responseTable} SET ${dbField} = ? WHERE bill_id = ?`;
      queryValue = [responseField, rowsValidate[0].bill_id];
      await connection.query(updateQuery, queryValue);

 
      let updateQuery2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_2} SET status = 'READY_FOR_SUBMISSION', status_type = 'ADJUDICATION', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
      let queryValue2 = [updatedOn, invoiceNumber];
      await connection.query(updateQuery2, queryValue2);
 
      const [rowsAF] = await connection.query(`SELECT status_id FROM ${process.env.DB_1}.${process.env.TABLE_2} WHERE txm_invoice_number = ?`, [invoiceNumber]);
      await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_3} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [rowsAF[0].status_id, 'ADJUDICATION', `${statusType}_FROM_ECI`, 'READY_FOR_SUBMISSION', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
 
      return { statusCode: 200, body: 'Updated' };
 
    }
    else if (statusType === 'VENDOR_MATCH_MANUAL' && responseValue === 'DELETE'){
      responseTable = process.env.TABLE_4;
      dbField = 'bill_id';
      responseField = rowsValidate[0].bill_id;
      updateQuery = `UPDATE ${process.env.DB_1}.${responseTable} SET ${dbField} = ? WHERE bill_id = ?`;
      queryValue = [responseField, rowsValidate[0].bill_id];
    }
    console.log('updateQuery', updateQuery);
    console.log('queryValue', queryValue);
 
    await connection.beginTransaction();
 
    await connection.query(updateQuery, queryValue);
   
    if (statusType === 'CLAIM_MATCH_MANUAL' && responseValue !== 'DELETE'){
      await connection.query(`UPDATE ${process.env.DB_1}.${process.env.TABLE_2} SET status = 'REPROCESS', status_type = 'CLAIM_MATCH_AUTO', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`, [ updatedOn, invoiceNumber]);
   
      const [rows2] = await connection.query(`SELECT status_id FROM ${process.env.DB_1}.${process.env.TABLE_2} WHERE txm_invoice_number = ?`, [invoiceNumber]);
   
      await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_3} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [rows2[0].status_id, statusType, `${statusType}_FROM_ECI`, matchFound, 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
      await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_3} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [rows2[0].status_id, 'CLAIM_MATCH_AUTO', `${statusType}_FROM_ECI`, 'REPROCESS', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
 
    } else {
      await connection.query(`UPDATE ${process.env.DB_1}.${process.env.TABLE_2} SET status = ?, update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`, [matchFound, updatedOn, invoiceNumber]);
   
      const [rows2] = await connection.query(`SELECT status_id FROM ${process.env.DB_1}.${process.env.TABLE_2} WHERE txm_invoice_number = ?`, [invoiceNumber]);
   
      await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_3} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [rows2[0].status_id, statusType, `${statusType}_FROM_ECI`, matchFound, 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
 
  }
  await connection.commit();
    return { statusCode: 200, body: 'Updated' };
  } catch (error) {
    await connection.rollback();
    console.error(`Error updating ${statusType} response:`, error);
    if (error.errno == 1054){
      console.error(`error.errno:`, error.errno);
      return { statusCode: 1054, body: 'Invoice number not available.' };
    }
    return { statusCode: 500, body: JSON.stringify({ message: `Error updating ${statusType} response.`, error: error.message }) };
  } finally {
    if (connection) connection.release();
  }
};
 
export const handler = async (event, context) => {
  console.log('In Claim-Match-Response');
  console.log('Event', event);
 
  const parts = event.path.split('/');
  const invoiceNumber = parts[2];
  let matchFound = '';
  let result = '';
  let responseType = '';
 
  await setupConnection();
  try {
    if (event.httpMethod === 'PUT') {
      matchFound = 'FOUND';
    } else if (event.httpMethod === 'DELETE') {
      // if (event.body && event.body.trim() !== '') {
      //   return generateErrorResponse(context.awsRequestId, 'Bad Request', 'DELETE requests should not have a body', 400);
      // }
      matchFound = 'MARKED_FOR_DELETION';
      result = await updateMatchResponse(process.env.Table2, invoiceNumber, matchFound, 'CLAIM_MATCH_MANUAL', 'DELETE', context);
      console.log('result', result);
      console.log('result.body', result.body);
      responseType = 'marked for deletion';
     
      if (result.statusCode === 200) {
        return generateSuccessResponse(context.awsRequestId, `Invoice marked for deletion successfully`, { invoiceNumber });
      }
      else if(result.body === 'Invoice number not available.'){
        // return generateSuccessResponse(context.awsRequestId, `No record found for given invoice number`, { invoiceNumber });
        return generateInvoiceNotFoundResponse(context.awsRequestId, `No record found for given invoice number`, 'Error occurred while processing request');
      }else {
        const errorResponse = JSON.parse(result.body);
        return generateErrorResponse(context.awsRequestId, 'Internal server error', errorResponse.message || 'Error occurred while processing request');
      }
    }
 
    const requestFields = JSON.parse(event.body);
    const validationError = validateRequestFields(requestFields, context);
    if (validationError) return validationError;
 
    const { claimNumber, providerNumber } = requestFields;
 
    if (claimNumber && !providerNumber) {
      result = await updateMatchResponse(process.env.TABLE_2, invoiceNumber, matchFound, 'CLAIM_MATCH_MANUAL', claimNumber, context);
      responseType = 'claim match';
    } else if (!claimNumber && providerNumber) {
      result = await updateMatchResponse(process.env.TABLE_2, invoiceNumber, matchFound, 'VENDOR_MATCH_MANUAL', providerNumber, context);
      responseType = 'provider match';
    }
 
    if (result.statusCode === 200) {
      return generateSuccessResponse(context.awsRequestId, `Updated ${responseType} response successfully`, { invoiceNumber });
    }
    else if(result.body === 'Invoice number not available.'){
        return generateInvoiceNotFoundResponse(context.awsRequestId, `No record found for given invoice number`, 'Error occurred while processing request');
        // return generateSuccessResponse(context.awsRequestId, `No record found for given invoice number`, { invoiceNumber });
    }
    else {
      const errorResponse = JSON.parse(result.body);
      return generateErrorResponse(context.awsRequestId, 'Internal server error', errorResponse.message || 'Error occurred while processing request');
    }
  } catch (error) {
    console.error('Error in handler:', error);
    return generateErrorResponse(context.awsRequestId, 'Internal server error', error.message);
  }
};