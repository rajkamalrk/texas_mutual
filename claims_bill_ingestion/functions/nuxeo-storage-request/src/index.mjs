import https from 'https';
import querystring from 'querystring';
import AWS from 'aws-sdk';
import {
    SecretsManagerClient,
    GetSecretValueCommand,
  } from "@aws-sdk/client-secrets-manager";

import {initDbPool} from './db-config.mjs';

import NodeCache from 'node-cache';

const secretsManager = new AWS.SecretsManager();
const cache = new NodeCache({ stdTTL: 1800 });  // TTL of 30 minutes (1800 seconds)

const retryDelay = 1000;  // Initial delay of 1 second
const maxRetries = 5;

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
export const handler = async (event) => {
    let createdOn;
    let result;
    let result2;
    let id;
    let batchId;
    let tp;
    let billingDocumentsId;
    let atLeastOneStored = false;
    try {
        console.log('In Main');
        await setupConnection();
    
        id = event.billId;
        batchId = event.batchId;
    
        [result] = await connection.query(`
          SELECT 
            bh.txm_invoice_number,
            bh.unique_bill_id,
            bh.date_bill_received_by_payer,
            bh.trading_partner,
            bh.txm_claim_number,
            p.billing_documents_id,
            p.document_serial_number,
            p.document_path,
            p.txm_doc_type
          FROM 
            ${process.env.DB_1}.${process.env.TABLE_1} bh
          INNER JOIN 
            ${process.env.DB_1}.${process.env.TABLE_2} p ON bh.bill_id = p.bill_id
          WHERE 
            bh.bill_id = ?
        `, [id]);
        
        tp = result[0]?.trading_partner;
        const now = new Date();
        createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
        const updatedOn = createdOn;
        console.log('result', result);

        // Fetch invoice status type
        let [resultC] = await connection.query(`select status_type from ${process.env.DB_1}.${process.env.TABLE_3} where txm_invoice_number = ?`,[result[0].txm_invoice_number])
        console.log('resultC', resultC);
        

        if (result.length > 0){
            // Update invoice status table as storage  in progress
    
            // Update status type only if it is claim match
            if ((resultC[0].status_type == 'CLAIM_MATCH_AUTO') || (resultC[0].status_type == 'CLAIM_MATCH_MANUAL')){
                const updateQuery = `UPDATE ${process.env.DB_1}.${process.env.TABLE_3} SET status_type = 'STORAGE_REQUEST', status = 'IN_PROGRESS', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
                const updateValues = [createdOn, result[0].txm_invoice_number];
                await connection.query(updateQuery, updateValues);
            }
            
            // Fetch Status Id from invoice status table
            [result2] = await connection.query(`select status_id from ${process.env.DB_1}.${process.env.TABLE_3} where txm_invoice_number = ?`,[result[0].txm_invoice_number])
            console.log('status_id', result2[0].status_id);
            const statusId = result2[0].status_id;
            console.log('statusId', statusId);
            for (let i = 0; i < result.length; i++){
                billingDocumentsId = result[i].billing_documents_id;
                try{
                    //check for the doc serial number process only for null

                    if (!result[i].document_serial_number){
                        const response = await sendAttachmentToNuxeo(result[i], id, connection);
                        console.log('Main Response', response);
                        if (response.documentSerialNumber){
                            await connection.beginTransaction();
                            console.log("update bill document serial number");
                            const updateQueryBT = `UPDATE ${process.env.DB_1}.${process.env.TABLE_2} SET  update_user = 'LAMBDA', update_timestamp = ?, document_serial_number  = ? WHERE billing_documents_id = ?`;
                            const updateValuesBT = [updatedOn, response.documentSerialNumber, result[i].billing_documents_id];
                            await connection.query(updateQueryBT, updateValuesBT);
                
                            // Insert into step status table
                            await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_4} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'STORAGE_REQUEST', `COMPLETED`, 'COMPLETED', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
                            await connection.commit();
    
                            atLeastOneStored = true;
                        }
                    }
                    
                } catch(error){
                    console.log("error while sendAttachmentToNuxeo", error);

                    const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_5} 
                    (batch_id, bill_id, billing_documents_id, txm_invoice_number, source, target, storage_type, error_code, error_type, error_message, create_timestamp, update_timestamp, create_user, update_user) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                    const insertValues = [batchId, id, billingDocumentsId, result[0].txm_invoice_number, 'BITX', 'NUXEO', 'STORAGE_REQUEST', JSON.parse(error).status, JSON.parse(error).errorType, JSON.stringify(JSON.parse(error).message), createdOn, createdOn, 'LAMBDA', 'LAMBDA'];
      
                    console.log('insertQuery in catch block', insertQuery);
                    console.log('insertValues in catch block', insertValues);
      
                    await connection.query(insertQuery, insertValues);
          
                    // Insert into step status table
                    await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_4} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'STORAGE_REQUEST', `FAILED`, 'FAILED', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
                    await connection.commit();
                    
                }
            }

            // Update into status table

            // Update status type only if it is claim match
            if ((resultC[0].status_type == 'CLAIM_MATCH_AUTO') || (resultC[0].status_type == 'CLAIM_MATCH_MANUAL')){
                const updateQuery2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_3} SET status = 'COMPLETED', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
                const updateValues2 = [createdOn, result[0].txm_invoice_number];
                await connection.query(updateQuery2, updateValues2);
            }

            return {
                statusCode: atLeastOneStored ? 200 : 500,
                body: { message: atLeastOneStored ? "Attachment stored and Nuxeo updated successfully" : "Failed to store Attachment and Nuxeo not updated successfully" },
            }
            
            
        }else{
            return {
                statusCode: 200,
                body: { message: "No record found for given bill id"},
            };
        }
        
    } catch (error) {
        console.error('Error in Lambda handler:', error);
        console.error('error.Error', JSON.parse(error).status);
        try {
            await connection.beginTransaction()
            const updateQuery2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_3} SET status = 'FAILED', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
            const updateValues2 = [createdOn, result[0].txm_invoice_number];
            await connection.query(updateQuery2, updateValues2);
            await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_4} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'STORAGE_REQUEST', `FAILED`, 'FAILED', 'LAMBDA', 'LAMBDA', createdOn, createdOn]);
            const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_5} 
                      (batch_id, bill_id, billing_documents_id, txm_invoice_number, source, target, storage_type, error_code, error_type, error_message, create_timestamp, update_timestamp, create_user, update_user) 
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
            const insertValues = [batchId, id, billingDocumentsId, result[0].txm_invoice_number, 'BITX', 'NUXEO', 'STORAGE_REQUEST', JSON.parse(error).status, JSON.parse(error).errorType, JSON.stringify(JSON.parse(error).message), createdOn, createdOn, 'LAMBDA', 'LAMBDA'];
        
            console.log('insertQuery', insertQuery);
            console.log('insertValues', insertValues);
              
        
            await connection.query(insertQuery, insertValues);
            await connection.commit();

            // const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
            // const subject = `${process.env.ENVIRONMENT} - ${tp} - Failed to send storage request to NUXEO - FATAL`;
            // const message = `Process: ${tp} - Send attachment storage request to NUXEO \n Error: ${error} \n Timestamp: ${ts}` ;
            // await sendEmail(subject, message);
            return {
                statusCode: JSON.parse(error).status || 'UNKNOWN',
                body: { message: "Internal server error", error: error.message },
            };
        } catch (err) {
            await connection.rollback();
            console.error('Error logging error response:', err);
            throw err;
        }
    }
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


const getSecretDetails = async (secretName) => {
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
    throw error;
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
  
const generateToken = async (context) => {

const tokenEndpoint = process.env.TOKEN_ENDPOINT;

const secretDetails = await getSecretDetails(process.env.SECRET_NAME, context);

const clientId = secretDetails.client_id;
const clientSecret = secretDetails.client_secret;

const postData = querystring.stringify({
    grant_type: process.env.GRANT_TYPE,
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
  
 
async function sendAttachmentToNuxeo(result, id) {
    
    console.log('In sendAttachmentToNuxeo', result);

    // Date formating to CST and itemType changes for align and techhealth Align = PTNOTES, techhealth = DIAG
    
    let utcDate = result.date_bill_received_by_payer;
    let date = new Date(utcDate);
    date.setHours(date.getHours() + 6);
    let cstDate = date.toISOString();

    let docItemType;
    if (result.trading_partner == 'align'){
        docItemType = 'PTNOTES';
    }
    else if (result.trading_partner == 'techhealth'){
        docItemType = 'DIAG';
    }
    else {
        docItemType = result.txm_doc_type;
    }
    console.log(cstDate);
    let nuxeoPayload = {
        "invoiceNumber": result.txm_invoice_number.toString(),
        "receiveDate": cstDate,
        "filePath": result.document_path,
        "itemType": docItemType,
        "sourceId": result.unique_bill_id,
        "sourceSystem": result.trading_partner,
        "claimNumber": result.txm_claim_number,
    }
    console.log('nuxeoPayload', nuxeoPayload);
    try {
        const token = await getOAuthToken();
        console.log('Received OAuth token:', token);
 
        const nuxeoEndpoint = process.env.NUXEO_ENDPOINT;
        const nuxeoRequestData = JSON.stringify(nuxeoPayload);
 
        const options = {
            hostname: new URL(nuxeoEndpoint).hostname,
            port: 443,
            path: new URL(nuxeoEndpoint).pathname,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`,
                'Content-Length': nuxeoRequestData.length,
            },
        };
 
        return new Promise((resolve, reject) => {
            const req = https.request(options, (res) => {
                let data = '';
 
                res.on('data', (chunk) => {
                    data += chunk;
                });
 
                res.on('end', () => {
                    if (res.statusCode === 200 || res.statusCode === 202) {
                        const response = JSON.parse(data);
                        resolve(response);
                    } else {
                        console.log('error in 123', data);
                        console.log('res.statusCode', res.statusCode);
                        reject(data);
                    }
                })
            });
 
            req.on('error', (e) => {
                reject(e);
            });
 
            req.write(nuxeoRequestData);
            console.log('req data', req);
            req.end();
        });
    } catch (error) {
        console.error('Error in sendAttachmentToNuxeo:', error.message);
        throw error;
    }
}