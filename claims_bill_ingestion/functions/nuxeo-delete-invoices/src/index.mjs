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
    let invoiceNumber;
    let batchId;
    let billId;
    let stepId;
    let atLeastOneStored = false;
    try {
        console.log('In Main');
        await setupConnection();
    
        invoiceNumber = event.invoiceNumber;
        batchId = event.batchId;
        billId = event.billId;
        stepId = event.stepId;
    
        [result] = await connection.query(`
          SELECT 
            *
          FROM 
            ${process.env.DB_1}.${process.env.TABLE_1}
          WHERE 
            bill_id = ?
        `, [billId]);
        
        const now = new Date();
        createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
        const updatedOn = createdOn;
        console.log('result', result);

        if (result.length > 0){
            try{
                const response = await sendDeleteRequestToNuxeo(invoiceNumber);
                console.log('Main Response', response);
                console.log('billId', billId);
                // if (response){
                    const updateQueryBT = `UPDATE ${process.env.DB_1}.${process.env.TABLE_1} SET  update_user = 'LAMBDA', update_timestamp = ?, is_active  = 'N' WHERE bill_id = ?`;
                    const updateValuesBT = [updatedOn,billId];
                    await connection.beginTransaction();
                    await connection.query(updateQueryBT, updateValuesBT);
                    await connection.commit();    
                    atLeastOneStored = true;
                // }                
                
            } catch(error){
                console.log("error while sendDeleteRequestToNuxeo", error);
                const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_2} 
                (batch_id, step_id, txm_invoice_number, error_code, error_info, error_message, create_timestamp, update_timestamp, create_user, update_user) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                const insertValues = [batchId, stepId, invoiceNumber, "LAMBDA FAILED", "Failed while sending request to NUXEO", JSON.stringify(error), createdOn, createdOn, 'LAMBDA', 'LAMBDA'];
                await connection.beginTransaction();
                await connection.query(insertQuery, insertValues);
                await connection.commit();
            }
        return {
            statusCode: atLeastOneStored ? 200 : 500,
            body: { message: atLeastOneStored ? "Attachment deleted and Nuxeo updated successfully" : "Failed to delete Attachment and Nuxeo not updated" },
        }
            
            
        }else{
            return {
                statusCode: 200,
                body: { message: "No record found for given combination"},
            };
        }
        
    } catch (error) {
        console.error('Error in Lambda handler:', error);
        console.error('error.Error', JSON.parse(error).status);
        try {
            await connection.beginTransaction()
            const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_2} 
            (batch_id, step_id, txm_invoice_number, error_code, error_info, error_message, create_timestamp, update_timestamp, create_user, update_user) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
            const insertValues = [batchId, stepId, invoiceNumber, "LAMBDA FAILED", "Failed while sending request to NUXEO", JSON.stringify(error), createdOn, createdOn, 'LAMBDA', 'LAMBDA'];
            await connection.query(insertQuery, insertValues);
            await connection.commit()
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
  
 
async function sendDeleteRequestToNuxeo(invoiceNumber) {
    
    console.log('In sendDeleteRequestToNuxeo', invoiceNumber);
    
    let nuxeoPayload = {
        "invoiceNumber": invoiceNumber
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
                        // const response = JSON.parse(data);
                        resolve(res);
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
        console.error('Error in sendDeleteRequestToNuxeo:', error.message);
        throw error;
    }
}