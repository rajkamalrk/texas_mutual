import https from 'https';
import querystring from 'querystring';
import AWS from 'aws-sdk';
import {
    SecretsManagerClient,
    GetSecretValueCommand,
  } from "@aws-sdk/client-secrets-manager";

import {initDbPool} from './db-config.mjs';

// import { SecretsManagerCache } from 'aws-sdk/lib/secretsmanager/SecretsManagerCache';
// const secretsManagerCache = new SecretsManagerCache({ maxAge: 1800 * 1000 }); // 30 minutes TTL

import NodeCache from 'node-cache';

const secretsManager = new AWS.SecretsManager();
const cache = new NodeCache({ stdTTL: 1800 });  // TTL of 30 minutes (1800 seconds)

const retryDelay = 1000;  // Initial delay of 1 second
const maxRetries = 5;


let connection;
const setupConnection = async () => {
    console.log(connection,'connection');
  
    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
      }
};
  
const sns = new AWS.SNS();
const lambda = new AWS.Lambda();

export const handler = async (event) => {
   
    console.log("event in Docdroid-Image-Create", event);
    let createdOn;
    let result;
    let result2;
    let id;
    let batchId;
    let tp;
    let documentType = event.tp === 'optum'? 'PharmacyStatement': 'PhysicianStatement';
    const tradingPartner = event.tp;
    const invoiceHeaderId = event.invoiceHeaderId;
    const billId = event.billId;
    
    try {
        console.log('In Main');
        await setupConnection();
    
        // await connection.beginTransaction();
    
        id = event.billId;
        batchId = event.batchId;

        console.log('billId', id);
        console.log('batchId', batchId);

        let [resultV] = await connection.query(`select * from ${process.env.DB_1}.${process.env.TABLE_1} where bill_id = ?`,[id]);
        console.log('resultV', resultV);
        let [resultV2] = await connection.query(`select * from ${process.env.DB_1}.${process.env.TABLE_2} where bill_id = ? and file_type = 'XML'`,[id]);
        console.log('resultV2', resultV2);
        let [resultV3] = await connection.query(`select * from ${process.env.DB_1}.${process.env.TABLE_3} where txm_invoice_number = ? and status_type = 'BILL_IMAGE_CREATION'`,[resultV[0].txm_invoice_number]);
        console.log('resultV3', resultV3);
        if ((resultV.length > 0) && (resultV2.length > 0) && (resultV3.length > 0)){
            if ((resultV2[0].bill_xml_document_path !== null) && (resultV2[0].image_creation_response !== null) && (resultV3[0].status === 'COMPLETED')){
                console.log('Response to glue', {
                    statusCode: 200,
                    body: { message: "Attachment stored and Docdroid updated successfully"},
                });
                return {
                statusCode: 200,
                body: { message: "Attachment stored and Docdroid updated successfully" },
                }
            }else if((resultV2[0].bill_xml_document_path !== null) && (resultV2[0].image_creation_response !== null) && (resultV3[0].status === 'FAILED')){
                let now3 = new Date();
                let createdOn3 = now3.toISOString().slice(0, 19).replace('T', ' ');
                const updateQueryV = `UPDATE ${process.env.DB_1}.${process.env.TABLE_3} SET status_type = 'BILL_IMAGE_CREATION', status = 'COMPLETED', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
                const updateValuesV = [createdOn3, resultV[0].txm_invoice_number];
                await connection.query(updateQueryV, updateValuesV);
                console.log('Response to glue', {
                    statusCode: 200,
                    body: { message: "Attachment stored and Docdroid updated successfully"},
                });
                return {
                    statusCode: 200,
                    body: { message: "Attachment stored and Docdroid updated successfully" },
                }
            }
            else if((resultV3[0].status_type === 'BILL_IMAGE_CREATION') && (resultV3[0].status === 'IN_PROGRESS')){
                console.log('Response to glue', {
                    statusCode: 200,
                    body: { message: "Attachment storage request is currently in progress" },
                });
                return {
                    statusCode: 200,
                    body: { message: "Attachment storage request is currently in progress" },
                }
            }
            
        }
        
        const xmlLambdaPayload = {
              "tp": tradingPartner,
              "billId": billId,
              "invoiceHeaderId": invoiceHeaderId
            };
            
            const resp = await invokeLambda(xmlLambdaPayload);
            console.log('response xml invoke....', resp);
            if(JSON.parse(resp)?.status !== 200) {
                try {

                        await connection.beginTransaction();

                        let [resultB] = await connection.query(`select * from ${process.env.DB_1}.${process.env.TABLE_1} where bill_id = ?`,[billId]);
                        console.log('resultB', resultB[0]);

                        let [resultA] = await connection.query(`select status_id from ${process.env.DB_1}.${process.env.TABLE_3} where txm_invoice_number = ?`,[resultB[0].txm_invoice_number]);
                        console.log('resultA', resultA[0]);
                        
                        let now2 = new Date();
                        let createdOn2 = now2.toISOString().slice(0, 19).replace('T', ' ');
                        let errorValue = JSON.parse(resp).message;
                        const updateQuery2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_3} SET status_type = 'BILL_IMAGE_CREATION', status = 'FAILED', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
                        const updateValues2 = [createdOn2, resultB[0].txm_invoice_number];
                        await connection.query(updateQuery2, updateValues2);
                        await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_4} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [resultA[0].status_id, 'BILL_IMAGE_CREATION', `FAILED`, 'FAILED', 'LAMBDA', 'LAMBDA', createdOn2, createdOn2]);
                        const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_5} 
                                (batch_id, bill_id, txm_invoice_number, source, target, image_type, error_code, error_type, error_message, create_timestamp, update_timestamp, create_user, update_user) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                        const insertValues = [batchId, id, resultB[0].txm_invoice_number, 'BITX', 'DOCDROID', 'BILL_IMAGE_CREATION', JSON.parse(resp).status, JSON.parse(resp).errorType, JSON.stringify(errorValue), createdOn2, createdOn2, 'LAMBDA', 'LAMBDA'];
                    
                        console.log('insertQuery', insertQuery);
                        console.log('insertValues', insertValues);
                        
                    
                        await connection.query(insertQuery, insertValues);
                        await connection.commit();

                        const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
                        let envValue = process.env.ENVIRONMENT;
                        envValue = envValue.toUpperCase();
                        console.log('envValue', envValue);

                        const subject = `${envValue} - ${tp} - Failed to send image creation request to DOCDROID - FATAL`;
                        const message = `Process: ${tp} - Send attachment storage request to DOCDROID \n Error: ${JSON.stringify(resp)} \n Timestamp: ${ts}` ;
                        await sendEmail(subject, message);
                        console.log('In Send email');
                        console.log("Response to glue", {
                            statusCode: JSON.parse(resp).status || 'UNKNOWN',
                            body: { message: "Internal server error", error: JSON.parse(resp).message },
                        });
                        return {
                            statusCode: JSON.parse(resp).status || 'UNKNOWN',
                            body: { message: "Internal server error", error: JSON.parse(resp).message },
                        };
                    } catch (err) {
                        await connection.rollback();
                        console.error('Error logging error response:', err);
                        throw err;
                    }
            }
        
        [result] = await connection.query(`
          SELECT 
            bh.txm_invoice_number,
            bh.date_bill_received_by_payer,
            bh.trading_partner,
            bh.txm_claim_number,
            p.document_path,
            p.txm_doc_type
          FROM 
            ${process.env.DB_1}.${process.env.TABLE_1} bh
          INNER JOIN 
            ${process.env.DB_1}.${process.env.TABLE_2} p ON bh.bill_id = p.bill_id
          WHERE 
            bh.bill_id = ?
        `, [id]);    
        
        console.log('result bh bd', result);
        
        tp = result[0]?.trading_partner;
        const now = new Date();
        createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
        const updatedOn = createdOn;
        
        console.log('result', result);
        if (result.length > 0){
            
            
            // Update invoice status table as storage  in progress
    
            const updateQuery = `UPDATE ${process.env.DB_1}.${process.env.TABLE_3} SET status_type = 'BILL_IMAGE_CREATION', status = 'IN_PROGRESS', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
            const updateValues = [createdOn, result[0].txm_invoice_number];
            await connection.query(updateQuery, updateValues);
            
            // Fetch File Path from billing_documents_table
            const[res] = await connection.query(`select bill_xml_document_path from ${process.env.DB_1}.${process.env.TABLE_2} where bill_id = ${id} and file_type = 'XML'`);
            const filePath = res[0].bill_xml_document_path;
            const xmlFileName = filePath.split('/').pop();
            
            // Fetch Status Id from invoice status table
            [result2] = await connection.query(`select status_id from ${process.env.DB_1}.${process.env.TABLE_3} where txm_invoice_number = ?`,[result[0].txm_invoice_number]);
            console.log('result2', result2[0]);
            console.log('status_id', result2[0].status_id);
            const statusId = result2[0].status_id;
            console.log('statusId', statusId);
            
            // console.log('response.status', resp.status);
            let payload = {
                filePath,
                documentType,
                xmlFileName
            };
            const response = await sendAttachmentToDocDroid(payload);
            console.log('Main Response from docdroid api', response);
            if (response.documentBatchId){
                try {
                    await connection.beginTransaction();
                    const updateQueryBT = `UPDATE ${process.env.DB_1}.${process.env.TABLE_2} SET image_creation_response  = ?, update_timestamp = ? WHERE bill_id = ? and file_type = 'XML'`;
                    const updateValuesBT = [response.documentBatchId, createdOn, id];
                    await connection.query(updateQueryBT, updateValuesBT);
                    
                    // Update into status table
                    const updateQuery2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_3} SET status = 'COMPLETED', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
                    const updateValues2 = [createdOn, result[0].txm_invoice_number];
                    await connection.query(updateQuery2, updateValues2);
                    // Insert into step status table
                    await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_4} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'BILL_IMAGE_CREATION', `COMPLETED`, 'COMPLETED', 'LAMBDA', 'LAMBDA', createdOn, updatedOn]);
                    await connection.commit();
                    console.log('Response to glue', {
                        statusCode: 200,
                        body: { message: "Attachment stored and Docdroid updated successfully", response },
                    });
                    return {
                    statusCode: 200,
                    body: { message: "Attachment stored and Docdroid updated successfully", response },
                    };
                } catch (err) {
                    await connection.rollback();
                    console.error('Error logging error response:', err);
                    throw err;
                }
            }
            
        }else{
            console.log('Response to glue', {
                statusCode: 200,
                body: { message: "No record found for given bill id"},
            });
            return {
                statusCode: 200,
                body: { message: "No record found for given bill id"},
            };
        }
        
    } catch (error) {
         console.log('error in catch without parse', error);
        console.log('error.status', JSON.parse(error)?.status);
        console.log('error.errorType', JSON.parse(error)?.errorType);
        console.log('error.message', JSON.parse(error)?.message);
        try {
            await connection.beginTransaction();
            let errorValue = JSON.parse(error).message;
            const updateQuery2 = `UPDATE ${process.env.DB_1}.${process.env.TABLE_3} SET  status_type = 'BILL_IMAGE_CREATION', status = 'FAILED', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
            const updateValues2 = [createdOn, result[0].txm_invoice_number];
            await connection.query(updateQuery2, updateValues2);
            await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_4} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [result2[0].status_id, 'BILL_IMAGE_CREATION', `FAILED`, 'FAILED', 'LAMBDA', 'LAMBDA', createdOn, createdOn]);
            const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_5} 
                      (batch_id, bill_id, txm_invoice_number, source, target, image_type, error_code, error_type, error_message, create_timestamp, update_timestamp, create_user, update_user) 
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
            const insertValues = [batchId, id, result[0].txm_invoice_number, 'BITX', 'DOCDROID', 'BILL_IMAGE_CREATION', JSON.parse(error).status, JSON.parse(error).errorType, JSON.stringify(errorValue), createdOn, createdOn, 'LAMBDA', 'LAMBDA'];
        
            console.log('insertQuery', insertQuery);
            console.log('insertValues', insertValues);
              
        
            await connection.query(insertQuery, insertValues);
            await connection.commit();

            let envValue2 = process.env.ENVIRONMENT;
            envValue2 = envValue2.toUpperCase();
            console.log('envValue2', envValue2);
            const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
            const subject = `${envValue2} - ${tp} - Failed to send image creation request to DOCDROID - FATAL`;
            const message = `Process: ${tp} - Send attachment storage request to DOCDROID \n Error: ${JSON.stringify(error)} \n Timestamp: ${ts}` ;
            await sendEmail(subject, message);
            console.log('Response to glue', {
                statusCode: 500,
                body: { message: "Internal server error", error: error.message },
            });
            return {
                statusCode: 500,
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

const secretDetails = await getSecretDetails(process.env.SECRET_NAME2, context);

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

async function sendAttachmentToDocDroid(payload) {
    
    
    console.log('In sendAttachmentToDocDroid', payload);
    console.log('In sendAttachmentToDocDroid payload.xmlFileName', payload.xmlFileName.replace(/\.xml$/, ""));

    let docdroidPayload = {
      "application": process.env.APP,
      "userId": process.env.USER_ID,
      "userFullName": process.env.USER_NAME,
      "documentBatchId": "",
      "documents": [
        {
          "businessObjectType": process.env.BUSINESS_OBJECT_TYPE,
          "sourceDocumentId": payload.xmlFileName.replace(/\.xml$/, ""),
          "documentType": payload.documentType,
          "filePath": payload.filePath
        }
      ]
    };
    console.log('docdroidPayload', docdroidPayload);
    try {
        const token = await getOAuthToken();
        console.log('Received OAuth token:', token);
 
        const docdroidEndpoint = process.env.DOCDROID_ENDPOINT;
        const docdroidRequestData = JSON.stringify(docdroidPayload);
 
        const options = {
            hostname: new URL(docdroidEndpoint).hostname,
            port: 443,
            path: new URL(docdroidEndpoint).pathname,
            method: process.env.REQUEST_TYPE,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`,
                'Content-Length': docdroidRequestData.length,
            },
        };
 
        return new Promise((resolve, reject) => {
            const req = https.request(options, (res) => {
                let data = '';
 
                res.on('data', (chunk) => {
                    data += chunk;
                });
 
                res.on('end', () => {
                    if (res.statusCode === 200 || res.statusCode === 202 || res.statusCode === 201) {
                        const response = JSON.parse(data);
                        resolve(response);
                    } else {
                        console.log('error in 123', data);
                        console.log('res.statusCode', res.statusCode);
                        console.log('data......',data);
                        reject(data);
                    }
                })
            });
 
            req.on('error', (e) => {
                reject(e);
            });
 
            req.write(docdroidRequestData);
            console.log('req data', req);
            req.end();
        });
    } catch (error) {
        console.error('Error in sendAttachmentToDocDroid:', error.message);
        throw error;
    }
}

const invokeLambda = async (data) => {
    console.log('data in invoke lambda', data);
  let params = {
    FunctionName: process.env.INVOKE_LAMBDA,
    Payload: JSON.stringify(data)
  };
 console.log('params', params);
  try {
      let response = await lambda.invoke(params).promise();
      console.log(response, 'In invokelambda');
      const responsePayload = JSON.parse(response.Payload);
      console.log('response payload in invoke lambda', responsePayload);
      return responsePayload;
    
  } catch (e) {
    console.log('invokeLambda :: Error: ' + e);
    throw e;
  }
};