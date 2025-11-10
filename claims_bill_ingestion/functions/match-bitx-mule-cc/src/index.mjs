import request from 'request-promise-native';
import https from 'https';
import querystring from 'querystring';
import AWS from 'aws-sdk';
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import {initDbPool} from './db-config.mjs';
// import { SecretsManagerCache } from 'aws-sdk/lib/secretsmanager/SecretsManagerCache';
// const secretsManagerCache = new SecretsManagerCache({ maxAge: 1800 * 1000 }); // 30 minutes TTL

import NodeCache from 'node-cache';

const secretsManager = new AWS.SecretsManager();
const cache = new NodeCache({ stdTTL: 1800 });  // TTL of 30 minutes (1800 seconds)

const retryDelay = 1000;  // Initial delay of 1 second
const maxRetries = 5;

const lambda = new AWS.Lambda();
const sns = new AWS.SNS();
let connection;

const setupConnection = async () => {
    console.log(connection,'connection');

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
    
    console.log('event', event);
    // Fetch  required Fields
    const [result] = await connection.query(`
      SELECT 
        bh.unique_bill_id,
        bh.date_bill_received_by_payer,
        bh.invoice_date,
        bh.txm_invoice_number,
        bh.reported_claim_number,
        p.social_security_number,
        p.date_of_injury,
        p.patient_name,
        s.insured_id_number
      FROM 
        ${process.env.DB_1}.${process.env.TABLE_1} bh
      LEFT JOIN 
        ${process.env.DB_1}.${process.env.TABLE_2} p ON bh.bill_id = p.bill_id
      LEFT JOIN 
        ${process.env.DB_1}.${process.env.TABLE_3} s ON bh.bill_id = s.bill_id
      WHERE 
        bh.bill_id = ?
    `, [id]);

    console.log('result', result);

    if (result.length === 0) {
      return {
        statusCode: 200,
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
      reported_claim_number: result[0].reported_claim_number,
    };

    const patientResult = {
      social_security_number: result[0].social_security_number,
      date_of_injury: result[0].date_of_injury,
      patient_name: result[0].patient_name,
    };

    const subscriberResult = {
      subscriber_id: result[0].insured_id_number,
    };

    console.log('billHeaderResult', billHeaderResult);
    console.log('patientResult', patientResult);
    console.log('subscriberResult', subscriberResult);


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
    // Update invoice status table as claim match in progress
    
    const updateQuery = `UPDATE ${process.env.DB_1}.${process.env.TABLE_5} SET status_type = 'CLAIM_MATCH_AUTO', status = 'IN_PROGRESS', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
    const updateValues = [createdOn, billHeaderResult.invoice_number];
    await connection.query(updateQuery, updateValues);
    
    console.log('billHeaderResult.invoice_number', billHeaderResult.invoice_number);
    // Fetch Status Id from invoice status table
    const [result2] = await connection.query(`select status_id from ${process.env.DB_1}.${process.env.TABLE_5} where txm_invoice_number = ?`,[billHeaderResult.invoice_number])
    console.log('result2', result2);
    console.log('status_id', result2[0].status_id);
    const statusId = result2[0].status_id;
    
    // Invoke GW api via mule
    const response = await callMulesoftApi(billHeaderResult, patientResult, subscriberResult, connection, logId, id, statusId, context, source);
    console.log('response', response);

    if (response && response.statusCode === 200) {
      let responseObject = response.body;
      responseObject = responseObject[0];
      console.log('source', source);
      console.log('tp', tp);

      console.log('responseObject', responseObject);
      console.log('Object.keys(responseObject).length == 0', Object.keys(responseObject).length);
      if((Object.keys(responseObject).length === 0) && ((source == 'kofax') || (tp == 'jopari'))){
        console.log("if((Object.keys(responseObject).length === 0) && ((source == 'kofax') || (tp == 'jopari'))){");
        console.log('responseObject', responseObject);
        let data = {};
        if (tp == 'jopari'){
          data = {
            sourceSystem: tp,
            ingestionSource: "ClaimMatch",
            documents: attachments,
            receiveDate: new Date(billHeaderResult.date_bill_received_by_payer).toISOString(),
            envelopeAttributes: {
              "Invoice Number": billHeaderResult.invoice_number,
              "Notify Bitx EBill Processed": "true"
            }
          };
        }
        if (source == 'kofax'){
          data = {
            sourceSystem: 'kofax',
            ingestionSource: "ClaimMatch",
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

        // Invoke ECI api via mule
        
        let lambdaInvokeResponse = await invokeLambda(data, context);
        console.log('lambdaInvokeResponse', lambdaInvokeResponse);
        let payload = JSON.parse(lambdaInvokeResponse.Payload);
        console.log('payload', payload);
        
        let resBody = JSON.parse(payload.body);
        console.log('resBody', resBody);
        if(payload.statusCode == 200){
          console.log('resBody?.response', resBody?.response);
          const updateQueryA = `UPDATE ${process.env.DB_1}.${process.env.TABLE_1} SET manual_claimmatch_request_response = ? WHERE bill_id = ?`;
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
              
              const updateQueryECI = `UPDATE ${process.env.DB_1}.${process.env.TABLE_5} SET status_type = 'CLAIM_MATCH_MANUAL', status = 'FAILED', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
              const updateValuesECI = [createdOn, billHeaderResult.invoice_number];
              await connection.query(updateQueryECI, updateValuesECI);
  
              // Insert into batch claim match error table
              const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_4} 
                  (batch_id, bill_id, txm_invoice_number, source, target, match_type, error_code, error_type, error_message, create_timestamp, update_timestamp, create_user, update_user) 
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
              const insertValues = [logId, id, billHeaderResult.invoice_number, 'BITX', 'ECI', 'CLAIM_MATCH_MANUAL', status, errorCode, userMessage, createdOn, updatedOn, 'LAMBDA', 'LAMBDA'];
  
              console.log('insertQuery', insertQuery);
              console.log('insertValues', insertValues);
              
  
              await connection.query(insertQuery, insertValues);
              
              // Insert into step status table
              await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_6} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'CLAIM_MATCH_MANUAL', `CLAIM_MATCH_FROM_ECI`, 'FAILED', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
              await connection.commit();
  
  
              // return { status: 'error', message: 'Error occurred while invoking ECI api manual claim match' };
              return{
                statusCode: status || 'UNKNOWN',
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: 'Error occurred while invoking ECI api manual claim match',
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
        
        // return { status: 'success', message: 'Auto Claim Match is not found, Sent Request to EC Indexer Via Mule Proxy' };
        
        await connection.beginTransaction();
              
        const updateQueryECI = `UPDATE ${process.env.DB_1}.${process.env.TABLE_5} SET status_type = 'CLAIM_MATCH_MANUAL', status = 'IN_PROGRESS', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
        const updateValuesECI = [createdOn, billHeaderResult.invoice_number];
        await connection.query(updateQueryECI, updateValuesECI);
        
        await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_6} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'CLAIM_MATCH_MANUAL', `CLAIM_MATCH_FROM_ECI`, 'IN_PROGRESS', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
        await connection.commit();
  
        return {
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Auto Claim Match is not found, Sent Request to EC Indexer Via Mule Proxy',
                data: 'Auto Claim Match is not found, Sent Request to EC Indexer Via Mule Proxy',
                transactionId: context.awsRequestId
            }),
        };
      } else if(response.body.length > 0 && Object.keys(responseObject).length > 0){
        console.log('In else if(response.body.length > 0 && Object.keys(responseObject).length > 0)');
        console.log('responseObject', responseObject)
        const claimNumberReceived = responseObject.claimNumber;
        const claimSecure = responseObject.claimSecure;
        const jurisdictionCode = responseObject.jurisdiction?.code;
        const jurisdictionDescription = responseObject.jurisdiction?.description;
        let dateOfInjury = responseObject.claimant?.dateOfInjury;
        console.log('dateOfInjury', dateOfInjury);


        const now = new Date();
        const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
        
        // Add Transaction
        try {
          await connection.beginTransaction();
          
          
          // Update invocie status table
          const query = `UPDATE ${process.env.DB_1}.${process.env.TABLE_5} SET status = 'FOUND', update_timestamp = ? WHERE txm_invoice_number = ? and status_type = 'CLAIM_MATCH_AUTO'`;
          const values = [createdOn, billHeaderResult.invoice_number];
          await connection.query(query, values);
          
          // Update Received claim number
          const query2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_1} SET txm_claim_number = ?, txm_claim_secure = ?, txm_jurisdiction_code = ?, txm_jurisdiction_description = ?, update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
          const values2 = [claimNumberReceived, claimSecure, jurisdictionCode, jurisdictionDescription, createdOn, billHeaderResult.invoice_number];
          await connection.query(query2, values2);
          
          // Update Received DOI
          const query3 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_2} SET date_of_injury = ?, update_user = 'LAMBDA', update_timestamp = ? WHERE bill_id = ?`;
          const values3 = [dateOfInjury, createdOn, id];
          await connection.query(query3, values3);

          // Update step status
          await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_6} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'CLAIM_MATCH_AUTO', `CLAIM_MATCH_FROM_GW`, 'FOUND', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
          await connection.commit();
          // return { status: 'success', message: 'Claim Matched', data: claimNumberReceived };
          return {
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Claim matched',
                data: billHeaderResult.invoice_number,
                transactionId: context.awsRequestId
            }),
          };
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
      }
      else if((Object.keys(responseObject).length == 0) && source == 'eci'){
        console.log("else if((Object.keys(responseObject).length == 0) && source == 'eci'");
        const now = new Date();
        const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
        
        // Add Transaction
        try {
          await connection.beginTransaction();
          
          
          // Update invocie status table
          const query = `UPDATE ${process.env.DB_1}.${process.env.TABLE_5} SET status = 'NOT_FOUND', update_timestamp = ? WHERE txm_invoice_number = ? and status_type = 'CLAIM_MATCH_AUTO'`;
          const values = [createdOn, billHeaderResult.invoice_number];
          await connection.query(query, values);
          
          
          // Update step status
          await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_6} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'CLAIM_MATCH_AUTO', `CLAIM_MATCH_FROM_GW`, 'NOT_FOUND', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
          await connection.commit();
          // return { status: 'success', message: 'Claim Matched', data: claimNumberReceived };
          
          const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
          let envValue = process.env.ENVIRONMENT;
          envValue = envValue.toUpperCase();
          console.log('envValue', envValue);
          const subject = `${envValue} - ${source.toUpperCase()} - Claim Number Provided Does Not Exist - FATAL`;
          const message = `Process: ${source} - Claim Match Auto \n Error: Claim Number Does Not Exist \n Batch ID: ${logId} \n Timestamp: ${ts} \n Bill ID: ${id} \n TMI Invoice Number: ${billHeaderResult.invoice_number}`;
          const sendEmailResponse = await sendEmail(subject, message);
          console.log('sendEmailResponse', sendEmailResponse);
          return {
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Claim not matched',
                data: billHeaderResult.invoice_number,
                transactionId: context.awsRequestId
            }),
          };
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
      }
    } else {
      return response;
    }
  } catch (error) {
    if (connection) await connection.rollback();
    // Need to update in error table through store proc
    console.error('Error:', error);
    return{
        statusCode: 500,
        body: JSON.stringify({
            status: 'error',
            errorMessage: 'Error occurred while invoking GW api auto claim match',
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
    // await lambda.invoke(params).promise();
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

const callMulesoftApi = async (billHeaderResult, patientResult, subscriberResult, connection, logId, id, statusId, context, source) => {
  
  const token = await getOAuthToken(context);
  const auth = `Bearer ${token}`;

  
  const options = {
    method: process.env.METHOD,
    uri: `${process.env.MULESOFT_API}`,
    headers: {
      'Accept': 'application/json',
      'Authorization': auth,
      'X-GW-Environment': process.env.X_GW_ENVIRONMENT
    },
    json: true,
    resolveWithFullResponse: true
  };

  try {
    // Utility function to check if a value is null or empty
    const isNullOrEmpty = (value) => value === null || value === undefined || value === '';

    const date = new Date(patientResult.date_of_injury);
    const formattedDate = `${date.getUTCFullYear()}${String(date.getUTCMonth() + 1).padStart(2, '0')}${String(date.getUTCDate()).padStart(2, '0')}`;

    // 1. Make the first API call with claim number
    if (!isNullOrEmpty(billHeaderResult.reported_claim_number)) {
        options.body = {
          claimNumber: billHeaderResult.reported_claim_number,
          dateOfInjury: formattedDate
        };
        
        const response1 = await request(options);
        console.log('options1', options);
        console.log('response1', response1);
        let responseObject1 = response1.body[0];
        
        if (Array.isArray(response1.body) && Object.keys(responseObject1).length > 0) {
          return response1; // Return response if it's not an empty array
        }
        else if(source == 'eci'){
          return response1;
        }
    }
    


    
    // 2. Make the second API call with socialSecurityNumber
    if (!isNullOrEmpty(patientResult.social_security_number)) {
        options.body = {
          socialSecurityNumber: patientResult.social_security_number,
          dateOfInjury: formattedDate
        };
        
        const response2 = await request(options);
        console.log('options2', options);
        console.log('response2', response2);
        let responseObject2 = response2.body[0];
        
        if (Array.isArray(response2.body) && Object.keys(responseObject2).length > 0) {
          return response2; // Return response if it's not an empty array
        }
    }
    
    // 3. Make the third API call with insuredID
    if (!isNullOrEmpty(subscriberResult.subscriber_id)) {
        options.body = {
          insuredID: subscriberResult.subscriber_id,
          dateOfInjury: formattedDate
        };
        
        const response3 = await request(options);
        console.log('options3', options);
        console.log('response3', response3);
        let responseObject3 = response3.body[0];
        
        if (Array.isArray(response3.body) && Object.keys(responseObject3).length > 0) {
          return response3; // Return response if it's not an empty array
        }
    }
    
    // 4. Make the fourth API call with claimantName
    if (!isNullOrEmpty(patientResult.patient_name)) {
        options.body = {
          claimantName: patientResult.patient_name,
          dateOfInjury: formattedDate
        };
        
        const response4 = await request(options);
        console.log('options4', options);
        console.log('response4', response4);
        return response4; // Return the response regardless of the content
    }


  } catch (error) {
    console.error('API call failed:', error);
    await logErrorResponse(error, billHeaderResult.invoice_number, connection, logId, id, statusId, context);
    let { errorCode } = error.error;
    return {
      statusCode: errorCode || 'UNKNOWN',
      body: {
        status: 'error',
        errorMessage: 'Error occurred while invoking GW API auto claim match',
        errorDescription: error,
        transactionId: context.awsRequestId
      },
    };
  }
}

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

    const query = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_4} (batch_id, bill_id, txm_invoice_number, source, target, match_type, error_code, error_type, error_message, create_timestamp, update_timestamp, create_user, update_user) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    const values = [logId, id, invoiceNumber, 'BITX', 'CLAIM_CENTER', 'CLAIM_MATCH_AUTO', status, errorCode, userMessage, createdOn, createdOn, 'LAMBDA', 'LAMBDA'];

    await connection.query(query, values);

    // Update invoice status table
    const query2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_5} SET status = 'FAILED', update_timestamp = ? WHERE txm_invoice_number = ? and status_type = 'CLAIM_MATCH_AUTO'`;
    const values2 = [createdOn, invoiceNumber];

    await connection.query(query2, values2);
    
    // Insert into step status ta
    await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_6} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [statusId, 'CLAIM_MATCH_AUTO', `CLAIM_MATCH_FROM_GW`, 'FAILED', 'LAMBDA', 'LAMBDA', createdOn, createdOn]);

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

  // const secret = await secretsManagerCache.getSecretValue(TOKEN_SECRET_NAME);
  const cachedSecret = cache.get(process.env.TOKEN_SECRET_NAME);
  console.log('cachedSecret', cachedSecret);
  // console.log('cachedSecret.token', cachedSecret.token);


  if (cachedSecret) {
    console.log('typeOFcachedSecret.token', typeof(cachedSecret));
    console.log('Token retrieved from cache', JSON.parse(cachedSecret).token);
    return JSON.parse(cachedSecret).token;
  }

  const generateTokenResult = await generateToken(context);
  console.log('generateTokenResult1', generateTokenResult);

  // const secretValue = JSON.stringify({ generateTokenResult });
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
}