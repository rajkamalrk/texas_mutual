import https from 'https';
import querystring from 'querystring';
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import AWS from 'aws-sdk';

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

export const handler = async (event, context) => {
    try {
        console.log('In Main');
        const response = await sendMatchRequestWithRetries(event, context);
        console.log('Main Response', response);
        return {
            statusCode: 200,
            body: JSON.stringify({ message: "sendMatchRequest success", response }),
        };
    } catch (error) {
        console.error('Error in Lambda handler:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Internal server error sendMatchRequest", error: error.message }),
        };
    }
};

// async function secretDetails(secretName) {
//     const client = new SecretsManagerClient({ region: process.env.REGION });
//     try {
//         const response = await client.send(new GetSecretValueCommand({ SecretId: secretName, VersionStage: "AWSCURRENT" }));
//         return JSON.parse(response.SecretString);
//     } catch (error) {
//         console.error('Error retrieving secret:', error);
//         throw new Error('Failed to retrieve secret');
//     }
// }

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


async function sendMatchRequest(event, context) {
    const token = await getOAuthToken(context);
    const matchRequestData = typeof event === 'object' ? JSON.stringify(event) : event;

    const matchRequestEndpoint = process.env.MATCH_REQUEST_ENDPOINT;
    const options = {
        hostname: new URL(matchRequestEndpoint).hostname,
        port: 443,
        path: new URL(matchRequestEndpoint).pathname,
        method: process.env.METHOD,
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`,
            'Content-Length': matchRequestData.length,
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
                    resolve(res.rawHeaders);
                } else {
                    reject(new Error(data));
                }
            });
        });

        req.on('error', (e) => {
            reject(e);
        });

        req.write(matchRequestData);
        req.end();
    });
}

async function sendMatchRequestWithRetries(event, context, retries = process.env.RETRY, delay = 30000) {
    try {
        return await sendMatchRequest(event, context);
    } catch (error) {
        const errorRes = error.message;
        console.log('errorRes', errorRes);

        await setupConnection();

        try {
            const now = new Date();
            const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
            const updatedOn = createdOn;

            const { ingestionSource, envelopeAttributes } = event;
            const statusType = ingestionSource === 'ClaimMatch' ? 'CLAIM_MATCH_MANUAL' : 'VENDOR_MATCH_MANUAL'
            const invoiceNumber = envelopeAttributes["Invoice Number"];
            console.log('errorRestype', typeof(errorRes));
            
            const updateQuery = `UPDATE ${process.env.DB_2}.${process.env.TABLE_3} 
                SET status = 'FAILED', update_timestamp = ? 
                WHERE txm_invoice_number = ? and status_type = ?`;
            const updateValues = [updatedOn, invoiceNumber, statusType];

            console.log('updateQuery', updateQuery);
            console.log('updateValues', updateValues);

            await connection.query(updateQuery, updateValues);
            await connection.commit();

        } catch (err) {
            console.error('Error during transaction:', err);
        } finally {
            connection.release();
        }

        if (retries === 0) {
            throw error;
        }
        console.log(`Retrying sendMatchRequest in ${delay / 1000} seconds... (${retries} retries left)`);
        await new Promise(resolve => setTimeout(resolve, delay));
        const nextDelay = delay === 30000 ? 120000 : 300000;
        return sendMatchRequestWithRetries(event, context, retries - 1, nextDelay);
    }
}

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
    // return { status: 'success', message: 'error occurred while fetching secret details'};
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
