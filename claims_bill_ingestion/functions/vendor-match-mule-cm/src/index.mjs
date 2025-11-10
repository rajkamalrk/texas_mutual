import request from 'request-promise-native';
import https from 'https';
import querystring from 'querystring';
import AWS from 'aws-sdk';
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { initDbPool } from './db-config.mjs';


import NodeCache from 'node-cache';

const secretsManager = new AWS.SecretsManager();
const cache = new NodeCache({ stdTTL: 1800 });  // TTL of 30 minutes (1800 seconds)

const retryDelay = 1000;  // Initial delay of 1 second
const maxRetries = 5;

const lambda = new AWS.Lambda();
const sns = new AWS.SNS();

let connection;
const setupConnection = async () => {
    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
    }
};

export const handler = async (event, context) => {

  try {
    await setupConnection();

    const id = event.billId;
    const logId = event.batchId;
    const source = event.source;
    const tp = event.tradingPartner;

    console.log('id', id);
    console.log('logId', logId);
    console.log('source', source);
    console.log('tp', tp);


    // Fetch  required Fields
    const [result] = await connection.query(`
      SELECT 
        bh.unique_bill_id,
        bh.date_bill_received_by_payer,
        bh.invoice_date,
        bh.txm_invoice_number,
        bh.txm_claim_number,
        bh.manual_review_flag,
        bh.manual_review_reason,
        p.vendor_tax_id,
        p.vendor_address_1,
        p.vendor_address_2,
        p.vendor_city,
        p.vendor_state,
        p.vendor_zip_code,
        p.vendor_name
      FROM 
        ${process.env.DB_1}.${process.env.TABLE_1} bh
      INNER JOIN 
        ${process.env.DB_1}.${process.env.TABLE_2} p ON bh.bill_id = p.bill_id
      WHERE 
        bh.bill_id = ?
    `, [id]);
    if (result.length === 0) {
    return {
        statusCode: 400,
        body: JSON.stringify({
            status: 'success',
            errorMessage: 'No records found given bill id',
            errorDescription: 'No records found given bill id',
            transactionId: context.awsRequestId
        }),
    };
      
    }
    const now = new Date();
    const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
    const updatedOn = createdOn;


    const billHeaderResult = {
      unique_bill_id: result[0].unique_bill_id,
      date_bill_received_by_payer: result[0].date_bill_received_by_payer,
      invoice_date: result[0].invoice_date,
      invoice_number: result[0].txm_invoice_number,
      claim_number: result[0].txm_claim_number,
    };

    const providerResult = {
      vendor_tax_id: result[0].vendor_tax_id,
      vendor_address_1: result[0].vendor_address_1,
      vendor_address_2: result[0].vendor_address_2,
      vendor_city: result[0].vendor_city,
      vendor_state: result[0].vendor_state,
      vendor_zip_code: result[0].vendor_zip_code,
      vendor_name: result[0].vendor_name
    };

    let licenseValidation = result[0].manual_review_flag;
    let licenseValidationStatus = result[0].manual_review_reason;
    console.log('licenseValidation', licenseValidation);
    console.log('licenseValidationStatus', licenseValidationStatus);
    console.log('Jopari_Address_Validation', process.env.Jopari_Address_Validation);
    console.log('Optum_Address_Validation', process.env.Optum_Address_Validation);
    console.log('TechHealth_Address_Validation', process.env.TechHealth_Address_Validation);
    console.log('Align_Address_Validation', process.env.Align_Address_Validation);

    console.log('Jopari_NPI_Validation', process.env.Jopari_NPI_Validation);
    console.log('Optum_NPI_Validation', process.env.Optum_NPI_Validation);
    console.log('TechHealth_NPI_Validation', process.env.TechHealth_NPI_Validation);
    console.log('Align_NPI_Validation', process.env.Align_NPI_Validation);

    if (licenseValidation == 'N'){
      licenseValidation = 'PASS';
    }
    else if ((licenseValidation == 'Y') && ((licenseValidationStatus != 'Address Failed') && (licenseValidationStatus != 'NPI Validation Failed'))){
      licenseValidation = 'FAIL';
    }
    else if ((tp == 'jopari') && (licenseValidation == 'Y') && (licenseValidationStatus == 'Address Failed') && (process.env.Jopari_Address_Validation == 'Y')){
      licenseValidation = 'FAIL';
    }
    else if ((tp == 'jopari') && (licenseValidation == 'Y') && (licenseValidationStatus == 'Address Failed') && (process.env.Jopari_Address_Validation == 'N')){
          licenseValidation = 'PASS';
        }
    else if ((tp == 'jopari') && (licenseValidation == 'Y') && (licenseValidationStatus == 'NPI Validation Failed') && (process.env.Jopari_NPI_Validation == 'Y')){
          licenseValidation = 'FAIL';
        }
    else if ((tp == 'jopari') && (licenseValidation == 'Y') && (licenseValidationStatus == 'NPI Validation Failed') && (process.env.Jopari_NPI_Validation == 'N')){
          licenseValidation = 'PASS';
        }
    else if ((tp == 'optum') && (licenseValidation == 'Y') && (licenseValidationStatus == 'Address Failed') && (process.env.Optum_Address_Validation == 'Y')){
          licenseValidation = 'FAIL';
        }
    else if ((tp == 'optum') && (licenseValidation == 'Y') && (licenseValidationStatus == 'Address Failed') && (process.env.Optum_Address_Validation == 'N')){
          licenseValidation = 'PASS';
        }
    else if ((tp == 'optum') && (licenseValidation == 'Y') && (licenseValidationStatus == 'NPI Validation Failed') && (process.env.Optum_NPI_Validation == 'Y')){
          licenseValidation = 'FAIL';
        }
    else if ((tp == 'optum') && (licenseValidation == 'Y') && (licenseValidationStatus == 'NPI Validation Failed') && (process.env.Optum_NPI_Validation == 'N')){
          licenseValidation = 'PASS';
        }
    else if ((tp == 'align') && (licenseValidation == 'Y') && (licenseValidationStatus == 'Address Failed') && (process.env.Align_Address_Validation == 'Y')){
          licenseValidation = 'FAIL';
        }
    else if ((tp == 'align') && (licenseValidation == 'Y') && (licenseValidationStatus == 'Address Failed') && (process.env.Align_Address_Validation == 'N')){
          licenseValidation = 'PASS';
        }
    else if ((tp == 'align') && (licenseValidation == 'Y') && (licenseValidationStatus == 'NPI Validation Failed') && (process.env.Align_NPI_Validation == 'Y')){
          licenseValidation = 'FAIL';
        }
    else if ((tp == 'align') && (licenseValidation == 'Y') && (licenseValidationStatus == 'NPI Validation Failed') && (process.env.Align_NPI_Validation == 'N')){
          licenseValidation = 'PASS';
        }
    else if ((tp == 'techhealth') && (licenseValidation == 'Y') && (licenseValidationStatus == 'Address Failed') && (process.env.TechHealth_Address_Validation == 'Y')){
          licenseValidation = 'FAIL';
        }
    else if ((tp == 'techhealth') && (licenseValidation == 'Y') && (licenseValidationStatus == 'Address Failed') && (process.env.TechHealth_Address_Validation == 'N')){
          licenseValidation = 'PASS';
        }
    else if ((tp == 'techhealth') && (licenseValidation == 'Y') && (licenseValidationStatus == 'NPI Validation Failed') && (process.env.TechHealth_NPI_Validation == 'Y')){
          licenseValidation = 'FAIL';
        }
    else if ((tp == 'techhealth') && (licenseValidation == 'Y') && (licenseValidationStatus == 'NPI Validation Failed') && (process.env.TechHealth_NPI_Validation == 'N')){
          licenseValidation = 'PASS';
        }
    else if ((source == 'kofax') || source == 'eci'){
      console.log("((source == 'kofax') || source == 'eci')")
        licenseValidation = 'PASS';
    }    
    console.log('billHeaderResult', billHeaderResult);
    console.log('providerResult', providerResult);

    const [resultA] = await connection.query(`
      SELECT 
        *
      FROM 
        ${process.env.DB_1}.${process.env.TABLE_1} bh
      INNER JOIN  
        ${process.env.DB_1}.${process.env.TABLE_A} bd ON bh.bill_id = bd.bill_id
      WHERE 
        bh.bill_id = ?
    `, [id]);

    console.log('resultA', resultA);

    let attachments = [];
    if (resultA.length > 0){
      if (tp == 'jopari'){
        for(let i = 0; i < resultA.length; i++){
          attachments.push({
            documentPath: resultA[i].document_path,
            sourceId: resultA[i].unique_bill_id,
            itemType: resultA[i].txm_doc_type
          })
        }
      }
      else if(source == 'kofax'){
        for(let i = 0; i < resultA.length; i++){
          attachments.push( {
            documentPath: `urn:ecbrowser:${resultA[i].document_serial_number}`,
            sourceId: resultA[i].txm_claim_number,
            itemType: resultA[i].txm_doc_type
          })
        }
      }
      
    }
    console.log('attachments', attachments);


    // Update invoice status table as vendor match in progress
    
    const updateQuery = `UPDATE ${process.env.DB_1}.${process.env.TABLE_4} SET status_type = 'VENDOR_MATCH_AUTO', status = 'IN_PROGRESS', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
    const updateValues = [createdOn, billHeaderResult.invoice_number];
    await connection.query(updateQuery, updateValues);
    
    // Fetch Status Id from invoice status table
    const [result2] = await connection.query(`select status_id from ${process.env.DB_1}.${process.env.TABLE_4} where txm_invoice_number = ?`,[billHeaderResult.invoice_number]);
    
    console.log('result2', result2);
    console.log('status_id', result2[0].status_id);
    const statusId = result2[0].status_id;
    let response;
    console.log('licenseValidation', licenseValidation);

    if (licenseValidation == 'PASS'){
      response = await callMulesoftApi(billHeaderResult, providerResult, connection, logId, id, statusId, context, tp);
      console.log('response.body', response.body);
      console.log('response.statusCode', response.statusCode);
    }else if(licenseValidation == 'FAIL'){
      response = {
        body: [],
        statusCode: 200
      }
      console.log('FAIL License validation response created', response);
    }

    if (response && response.statusCode === 200) {
      let responseObject = response.body;
      responseObject = responseObject[0];
      console.log('responseObject', responseObject);
      console.log('response.body.length', response.body.length);
      if ((response.body.length === 0) && (source == 'kofax'|| tp == 'jopari')){
        console.log("if ((response.body.length === 0) && source == 'kofax'|| tp == 'jopari')");
        console.log('responseObject', responseObject);
        let data = {};
        if (tp == 'jopari'){
          data = {
            sourceSystem: tp,
            ingestionSource: "ProviderMatch",
            documents: attachments,
            receiveDate: new Date(billHeaderResult.date_bill_received_by_payer).toISOString(),
            envelopeAttributes: {
              "Invoice Number": billHeaderResult.invoice_number,
              "Notify Bitx EBill Processed": "true"
            },
            workItemAttributes: {
              "Invoice Number": billHeaderResult.invoice_number 
            }
          };
        }
        if (source == 'kofax'){
          data = {
            sourceSystem: 'kofax',
            ingestionSource: "ProviderMatch",
            documents: attachments,
            receiveDate: new Date(billHeaderResult.date_bill_received_by_payer).toISOString(),
            envelopeAttributes: {
              "Invoice Number": billHeaderResult.invoice_number,
              "Notify Bitx EBill Processed": "true"
            },
            workItemAttributes: {
              "Invoice Number": billHeaderResult.invoice_number 
            }
          };
        }

        let lambdaInvokeResponse = await invokeLambda(data, context);
        console.log('lambdaInvokeResponse', lambdaInvokeResponse);
        let payload = JSON.parse(lambdaInvokeResponse.Payload);
        console.log('payload', payload);
        
        let resBody = JSON.parse(payload.body);
        console.log('resBody', resBody)

        if(payload.statusCode == 200){
          console.log('resBody?.response', resBody?.response);
          const updateQueryA = `UPDATE ${process.env.DB_1}.${process.env.TABLE_1} SET manual_vendormatch_request_response = ? WHERE bill_id = ?`;
          const updateValuesA = [JSON.stringify(resBody?.response), id];
          await connection.query(updateQueryA, updateValuesA);
        }

        if (payload.statusCode != 200){
            let errorResBody = JSON.parse(resBody.error);
            console.log('errorrResBody', errorResBody);
            const { status, errorType: errorCode } = errorResBody

            const userMessage = errorResBody.message.error;
  
            console.log('status', status)
            console.log('errorCode', errorCode)
            console.log('userMessage', userMessage)
            
            //Add transaction
            
            try {
              await connection.beginTransaction();
              
              const updateQueryECI = `UPDATE ${process.env.DB_1}.${process.env.TABLE_4} SET status_type = 'VENDOR_MATCH_MANUAL', status = 'FAILED', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
              const updateValuesECI = [createdOn, billHeaderResult.invoice_number];
              await connection.query(updateQueryECI, updateValuesECI);
  
              // Insert into batch vendor match error table
              const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_3} 
                  (batch_id, bill_id, txm_invoice_number, source, target, match_type, error_code, error_type, error_message, create_timestamp, update_timestamp, create_user, update_user) 
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
              const insertValues = [logId, id, billHeaderResult.invoice_number, 'BITX', 'ECI', 'VENDOR_MATCH_MANUAL', status, errorCode, userMessage, createdOn, updatedOn, 'LAMBDA', 'LAMBDA'];
  
              console.log('insertQuery', insertQuery);
              console.log('insertValues', insertValues);
              
  
              await connection.query(insertQuery, insertValues);
              
              // Insert into step status table
              await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_5} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'VENDOR_MATCH_AUTO', `VENDOR_MATCH_FROM_ECI`, 'FAILED', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
              await connection.commit();

              let flow;
              if (tp == 'null' || tp == '' || tp == 'undefined' ){
                flow = source;
              }
              else{
                flow = tp;
              }
              console.log('flow', flow);
              const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
              const subject = `${process.env.ENVIRONMENT} - ${flow.toUpperCase()} - Error occurred for Vendor Match Manual - FATAL`;
              const message = `  Process: ${flow.toUpperCase()} - Vendor Match Manual
  Error: Error occurred in Vendor Match Manual
  Claim Number: ${billHeaderResult.claim_number}
  Invoice Number: ${billHeaderResult.invoice_number} 
  Invoice Status type: VENDOR_MATCH_MANUAL
  Invoice Status: FAILED 
  Timestamp: ${ts}`;
              const sendEmailResponse = await sendEmail(subject, message);
              console.log('sendEmailResponse', sendEmailResponse);
      
            return{
                statusCode: status || 'UNKNOWN',
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: 'Error occurred while invoking ECI api manual vendor match',
                    errorDescription: errorResBody,
                    transactionId: context.awsRequestId
                }),
            }  
            
          } catch (err) {
            await connection.rollback();
            console.error('Error logging error response:', err);

            return{
                statusCode: 500,
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: 'Error logging error response',
                    errorDescription: err,
                    transactionId: context.awsRequestId
                }),
            }
          }
        }
        await connection.beginTransaction();
              
        const updateQueryECI = `UPDATE ${process.env.DB_1}.${process.env.TABLE_4} SET status_type = 'VENDOR_MATCH_MANUAL', status = 'IN_PROGRESS', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
        const updateValuesECI = [createdOn, billHeaderResult.invoice_number];
        await connection.query(updateQueryECI, updateValuesECI);
        
        await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_5} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'VENDOR_MATCH_MANUAL', `VENDOR_MATCH_FROM_ECI`, 'IN_PROGRESS', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
        await connection.commit();
        return {
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Auto Vendor Match is not found, Sent Request to EC Indexer Via Mule Proxy',
                data: 'Auto Vendor Match is not found, Sent Request to EC Indexer Via Mule Proxy',
                transactionId: context.awsRequestId
            }),
        };
      } else if(response.body.length > 0 && Object.keys(responseObject).length > 0){
          console.log("In else if(response.body.length > 0 && Object.keys(responseObject).length > 0){");
          const now = new Date();
          const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
          
          try{
            await connection.beginTransaction();
            
            const query = `UPDATE ${process.env.DB_1}.${process.env.TABLE_4} SET status = 'FOUND', update_timestamp = ? WHERE txm_invoice_number = ? and status_type = 'VENDOR_MATCH_AUTO'`;
            const values = [createdOn, billHeaderResult.invoice_number];
            await connection.query(query, values);
            let resBody = response.body;
            console.log('resBody', resBody);
            console.log("resBody[0].vendorName", resBody[0].vendorName);

            let phoneNumber = null;
            if (resBody[0].contact && resBody[0].contact.phone) {
              phoneNumber = resBody[0].contact.phone;
              // Clean the phone number by removing any hyphens
              phoneNumber = phoneNumber.replace(/-/g, '')
            }
            console.log('phoneNumber', phoneNumber);

            const query2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_2} SET vendor_name = ?,vendor_address_1 = ?,vendor_city = ?,vendor_state = ?,vendor_zip_code = ?,vendor_tax_id = ?,vendor_number = ?,vendor_dba = ?,additional_name = ?,vendor_other_id = ?,vendor_address_2 = ?,federal_military_treatment_facility = ?,vendor_phone_number = ?,update_user=?,update_timestamp=? WHERE bill_id = ?`;
            const values2 = [resBody[0].vendorName, resBody[0].address.line1, resBody[0].address.city, resBody[0].address.state, resBody[0].address.zipCode, resBody[0].taxID, resBody[0].vendorNumber, resBody[0].dba, resBody[0].additionalName, resBody[0].license, resBody[0].address.line2, resBody[0].federalMilitaryTreatmentFacility, phoneNumber, 'LAMBDA', createdOn, id];
            await connection.query(query2, values2);

            // Update step status
            await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_5} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'VENDOR_MATCH_AUTO', `VENDOR_MATCH_FROM_GW`, 'FOUND', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
  
            await connection.commit();
          } catch (err) {
            await connection.rollback();
            console.error('Error logging error response:', err);
            return{
                statusCode: 500,
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: 'Error logging error response:',
                    errorDescription: err,
                    transactionId: context.awsRequestId
                }),
            }          
        }
        return {
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Vendor matched',
                data: billHeaderResult.invoice_number,
                transactionId: context.awsRequestId
            }),
        };
      }
      else if((response.body.length === 0) && source == 'eci' || source == 'kofax' || tp == 'align' || tp == 'techhealth' || tp == 'optum'){
        console.log("else if((response.body.length === 0) && source == 'eci' || source == 'kofax' || tp == 'align || tp == 'techhealth || tp == 'optum'){");
        const now = new Date();
        const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
        
        // Add Transaction
        try {
          await connection.beginTransaction();
          
          if (source == 'eci'){
            const query = `UPDATE ${process.env.DB_1}.${process.env.TABLE_4} SET status = 'NOT_FOUND', status_type = 'VENDOR_MATCH_MANUAL', update_timestamp = ?, update_user = 'LAMBDA' WHERE txm_invoice_number = ?`;
            const values = [createdOn, billHeaderResult.invoice_number];
            await connection.query(query, values);
          }else {
            const query = `UPDATE ${process.env.DB_1}.${process.env.TABLE_4} SET status = 'PENDING', status_type = 'VENDOR_MATCH_MANUAL', update_timestamp = ?, update_user = 'LAMBDA' WHERE txm_invoice_number = ?`;
            const values = [createdOn, billHeaderResult.invoice_number];
            await connection.query(query, values);
          }
          
          
          
          // Update step status
          await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_5} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'VENDOR_MATCH_AUTO', `VENDOR_MATCH_FROM_GW`, 'NOT_FOUND', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
          await connection.commit();
          
          return {
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Vendor not matched',
                data: billHeaderResult.invoice_number,
                transactionId: context.awsRequestId
            }),
          };
        } catch (err) {
          await connection.rollback();
          console.error('Error logging error response:', err);
          return {
                statusCode: 500,
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: 'Error logging error response:',
                    errorDescription: err,
                    transactionId: context.awsRequestId
                }),
            }
        }
      }
    } else {
        console.log(' In else return response');
        return {
          statusCode: 400,
          body: JSON.stringify({
              status: 'error',
              errorMessage: 'license validation is null or empty',
              errorDescription: 'license validation is null or empty',
              transactionId: context.awsRequestId
          }),
      }
    }
  } catch (error) {
    if (connection) await connection.rollback();
    console.error('Error:', error);
    return{
        statusCode: 500,
        body: JSON.stringify({
            status: 'error',
            errorMessage: 'Error occurred while invoking GW api auto vendor match',
            errorDescription: error,
            transactionId: context.awsRequestId
        }),
    }
  } finally {
    if (connection) connection.release();
  }
};

const invokeLambda = async (data, context) => {
  const params = {
    FunctionName: process.env.INVOKE_LAMBDA,
    Payload: JSON.stringify(data)
  };
  
  console.log('data', data);
  console.log('params', params);

  try {
    return await lambda.invoke(params).promise();;
  } catch (e) {
    console.log('invokeLambda :: Error: ' + e);
    return{
        statusCode: 500,
        body: JSON.stringify({
            status: 'error',
            errorMessage: 'invokeLambda :: Error',
            errorDescription: e,
            transactionId: context.awsRequestId
        }),
    }
  }
};

const getSecretDetails = async (secretName, context) => {
  const client = new SecretsManagerClient({ region: process.env.REGION });

  try {
    const response = await client.send(
      new GetSecretValueCommand({
        SecretId: secretName,
        VersionStage: "AWSCURRENT",
      })
    );
    return JSON.parse(response.SecretString);
  } catch (error) {
    console.error('Error in getSecretDetails:', JSON.stringify(error, Object.getOwnPropertyNames(error)));
    return{
        statusCode: 500,
        body: JSON.stringify({
            status: 'error',
            errorMessage: 'error occurred while fetching secret details',
            errorDescription: error,
            transactionId: context.awsRequestId
        }),
    }
    
  }
};

const callMulesoftApi = async (billHeaderResult, providerResult,connection, logId, id, statusId, context, tp) => {
  const token = await getOAuthToken(context);
  const auth = `Bearer ${token}`;
  let requestBody = {};
  let finalRequestBody = {};
  console.log('tp', tp);

  
  requestBody = {
    taxID: providerResult.vendor_tax_id?.toString() || '',
    vendorAddress1: providerResult.vendor_address_1 || '',
    vendorAddress2: providerResult.vendor_address_2 || '',
    city: providerResult.vendor_city || '',
    state: providerResult.vendor_state || '',
    zipCode: providerResult.vendor_zip_code || '',
    vendorName: providerResult.vendor_name || '',
  };
    
  
  for (const [key, value] of Object.entries(requestBody)) {
    if (value !== undefined && value !== null && value !== "") {
      finalRequestBody[key] = value;
    }
  }


  const options = {
    method: process.env.METHOD,
    uri: `${process.env.MULESOFT_API}`,
    headers: {
      'Accept': 'application/json',
      'Authorization': auth,
      'X-GW-Environment': process.env.X_GW_ENVIRONMENT
    },

    body: finalRequestBody,
    json: true,
    resolveWithFullResponse: true
    
  };
  console.log('options', options);


  try {
    const response = await request(options);
    if (response.statusCode !== 200) {
    return{
        statusCode: response.statusCode,
        body: JSON.stringify({
            status: 'error',
            errorMessage: 'error occurred while calling GW api',
            errorDescription: 'error occurred while calling GW api',
            transactionId: context.awsRequestId
        }),
    }
    
    }
    return response;
  } catch (error) {
    
    await logErrorResponse(error, billHeaderResult.invoice_number, connection, logId, id, statusId, context)
    let { errorCode } = error.error;
    return {
        statusCode: errorCode || 'UNKNOWN', 
        body: {
            status: 'error',
            errorMessage: 'Error occurred while invoking GW api auto vendor match',
            errorDescription: error,
            transactionId: context.awsRequestId
        },
    };
  }
};

const logErrorResponse = async (error, invoiceNumber, connection, logId, id, statusId, context) => {
    console.log('error', error);
    const errorDetails = error.error;
    console.log('errorDetails', errorDetails);
    let { status, errorCode, userMessage } = errorDetails;
    const now = new Date();
    const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
    
    if (status == undefined && errorCode == undefined && userMessage == undefined){
      status = 'UNKNOWN';
      errorCode = 'UNKNOWN_ERROR';
      userMessage = JSON.stringify(error);
    }
    console.log('status', status);
    console.log('errorCode', errorCode);
    console.log('userMessage', userMessage);
      try {
      await connection.beginTransaction();
      
      // Insert into error table
  
      const query = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_3} (batch_id, bill_id, txm_invoice_number, source, target, match_type, error_code, error_type, error_message, create_timestamp, update_timestamp, create_user, update_user) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
      const values = [logId, id, invoiceNumber, 'BITX', 'CONTACT_MANAGER', 'VENDOR_MATCH_AUTO', status, errorCode, userMessage, createdOn, createdOn, 'LAMBDA', 'LAMBDA'];
  
      await connection.query(query, values);
  
      // Update invoice status table
      const query2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_4} SET status = 'FAILED', update_timestamp = ? WHERE txm_invoice_number = ? and status_type = 'VENDOR_MATCH_AUTO'`;
      const values2 = [createdOn, invoiceNumber];
  
      await connection.query(query2, values2);
      
      // Insert into step status table
      await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_5} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [statusId, 'VENDOR_MATCH_AUTO', `VENDOR_MATCH_FROM_GW`, 'FAILED', 'LAMBDA', 'LAMBDA', createdOn, createdOn]);
  
      await connection.commit();
    } catch (err) {
      await connection.rollback();
      console.error('Error logging error response:', err);
    return{
        statusCode: 500,
        body: JSON.stringify({
            status: 'error',
            errorMessage: 'Error logging error response:',
            errorDescription: err,
            transactionId: context.awsRequestId
        }),
    }    
    }  
};
  
const getOAuthToken = async (context) => {

  const cachedSecret = cache.get(process.env.TOKEN_SECRET_NAME);
  console.log('cachedSecret', cachedSecret);

  if (cachedSecret) {
    console.log('typeOFcachedSecret.token', typeof(cachedSecret));
    console.log('Token retrieved from cache', JSON.parse(cachedSecret).token);
    return JSON.parse(cachedSecret).token;
  }

  const generateTokenResult = await generateToken(context);
  console.log('generateTokenResult1', generateTokenResult);

  const secretValue = generateTokenResult;
  console.log('secretValue', secretValue);


  const newSecretValue = JSON.stringify({
    ['token']: secretValue, // dynamically add the secret key and value
  });

  console.log('newSecretValue', newSecretValue);

  let attempt = 0;
  let success = false;

  while (attempt < maxRetries && !success) {
    try {
      const response = await secretsManager.putSecretValue({
        SecretId: process.env.TOKEN_SECRET_NAME,
        SecretString: newSecretValue,
      }).promise();
      console.log('response', response);
      cache.set(process.env.TOKEN_SECRET_NAME, newSecretValue);
      console.log("Token updated in Secrets Manager");
      success = true; // Exit loop if successful
    } catch (error) {
      if (error.code === 'ThrottlingException') {
        attempt++;
        console.log(`Throttling exception: Retry attempt ${attempt}/${maxRetries}`);
        await new Promise(resolve => setTimeout(resolve, retryDelay * attempt)); // Exponential backoff
      } else {
        console.error('Error updating secret:', error);
        break;
      }
    }
  }

  if (!success) {
    console.error('Failed to update secret after max retries');
  }
  return generateTokenResult;

};

const generateToken = async (context) => {

  const tokenEndpoint = process.env.TOKEN_ENDPOINT;

  const secretDetails = await getSecretDetails(process.env.CLIENT_DETAILS_SECRET, context);

  const clientId = secretDetails.client_id;
  const clientSecret = secretDetails.client_secret;

  const postData = querystring.stringify({
    grant_type: 'client_credentials',
    scope: process.env.SCOPE,
  });

  const base64Auth = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');

  const options = {
    hostname: new URL(tokenEndpoint).hostname,
    port: 443,
    path: new URL(tokenEndpoint).pathname,
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': `Basic ${base64Auth}`,
      'Content-Length': postData.length,

    },
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        if (res.statusCode === 200) {
          const response = JSON.parse(data);
          resolve(response.access_token);
        } else {
          reject(new Error(`Failed to get OAuth token: ${res.statusCode} - ${data}`));
        }
      });
    });

    req.on('error', (e) => {
      reject(e);
    });

    req.write(postData);
    req.end();
  });
};

async function sendEmail(subject, message) {
    const params = {
        Subject: subject,
        Message: message, 
        TopicArn: `${process.env.TOPIC_ARN}`
    };
 
    try {
        // Publish the message to SNS
        const data = await sns.publish(params).promise();
        console.log(`Message sent to the topic ${params.TopicArn}`);
        console.log("MessageID is " + data.MessageId);
    } catch (err) {
        console.error(err, err.stack);
    }
 
    return {
        statusCode: 200,
        body: JSON.stringify('SNS message sent successfully!'),
    };
  }