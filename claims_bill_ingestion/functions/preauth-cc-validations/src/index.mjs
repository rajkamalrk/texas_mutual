import request from 'request-promise-native';
import https from 'https';
import querystring from 'querystring';
import AWS from 'aws-sdk';
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import {initDbPool} from './db-config.mjs';

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
    const claimNumber = event.claimNumber;
    const dateOfInjury = event.dateOfInjury;
    const batchId = event.batchId;
    const stepId = event.stepId;

    console.log('claimNumber', claimNumber);
    console.log('dateOfInjury', dateOfInjury);
    console.log('batchId', batchId);
    console.log('stepId', stepId);
    
    const response = await callMulesoftApi(claimNumber, dateOfInjury, batchId, stepId, context);
    console.log('response', response);

    if (response && response.statusCode === 200) {
      let responseObject = response.body;
      responseObject = responseObject[0];
      console.log('responseObject', responseObject);
      console.log('Object.keys(responseObject).length == 0', Object.keys(responseObject).length);
      if((Object.keys(responseObject).length === 0)){
        console.log('responseObject', responseObject);
          return {
          statusCode: 200,
          body: JSON.stringify({ message: 'No data found' }),
          };
      }else if(response.body.length > 0 && Object.keys(responseObject).length > 0){
        const claimNumberReceived = responseObject.claimId;
        console.log('claimNumberReceived', claimNumberReceived);
        const now = new Date();
        const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
        const updateQuery2 = `UPDATE ${process.env.DB_2}.${process.env.TABLE_2} SET claim_id = ?, update_user = 'LAMBDA', update_timestamp = ? WHERE claim_number = ?`;
        const updateValues2 = [claimNumberReceived, createdOn, claimNumber];
        await connection.query(updateQuery2, updateValues2);
        return {
          statusCode: 200,
          body: JSON.stringify({ message: 'claim data found', claimNumber:  claimNumber}),
        };
      }
    }  
    else {
      return response;
    }
  } catch (error) {
    console.error('Error:', error);
    return{
        statusCode: 500,
        body: JSON.stringify({
            status: 'error',
            errorMessage: 'Error occurred while invoking GW api',
            errorDescription: error,
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

const callMulesoftApi = async (claimNumber, dateOfInjury, batchId, stepId, context) => {
  
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

  console.log('dateOfInjury', dateOfInjury);
  try {
    const date = new Date(dateOfInjury);
    console.log('date', date);

    // const formattedDate = `${dateOfInjury.getUTCFullYear()}${String(dateOfInjury.getUTCMonth() + 1).padStart(2, '0')}${String(dateOfInjury.getUTCDate()).padStart(2, '0')}`;
    // console.log('formattedDate', formattedDate);

    options.body = {
      claimNumber: claimNumber,
      dateOfInjury: dateOfInjury
    };
    console.log('options', options);

    const response = await request(options);
    console.log('options', options);
    console.log('response', response);
    return response;

  } catch (error) {
    console.error('API call failed:', error);
    await logErrorResponse(error,claimNumber, dateOfInjury, batchId, stepId, context);
    let { errorCode } = error.error;
    return {
      statusCode: errorCode || 'UNKNOWN',
      body: {
        status: 'error',
        errorMessage: 'Error occurred while invoking GW API',
        errorDescription: error,
        transactionId: context.awsRequestId
      },
    };
  }
}

const logErrorResponse = async (error, claimNumber, dateOfInjury, batchId, stepId, context) => {
  console.log('error', error);
  
  const now = new Date();
  const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
    try {
    await connection.beginTransaction();
    
    // Insert into error table

    const insertQuery = `INSERT INTO ${process.env.DB_1}.${process.env.TABLE_1} 
                    (batch_id, step_id, error_code, error_info, error_message, create_timestamp, update_timestamp, create_user, update_user) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    const insertValues = [batchId, stepId, "LAMBDA FAILED", "Failed while sending request to CC", JSON.stringify(error), createdOn, createdOn, 'LAMBDA', 'LAMBDA'];
    await connection.query(insertQuery, insertValues);
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