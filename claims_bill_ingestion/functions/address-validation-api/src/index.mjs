import https from 'https';
import request from 'request-promise-native';
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

export const handler = async (event) => {
  // let connection;
  const now = new Date();
  const updatedOn = now.toISOString().slice(0, 19).replace('T', ' ');
  console.log(updatedOn,"updatedOn...");

  try {
    await setupConnection();
    const { billId } = event;
    console.log('billId', billId);
    const envConfidence = parseInt(process.env.confidence, 10);
    const envMatchScore = parseInt(process.env.matchScore, 10);

    const [res] = await connection.query(
      `SELECT * FROM ${process.env.DB}.${process.env.TABLE} WHERE bill_id = ?`, [billId]
    );
    console.log('DB query result:', res);
 
      const record = res[0];
      try {
        console.log('Processing record:', record);
 
        const vendorReq = {
          addressLine1: record.vendor_address_1 || '',
          addressLine2: record.vendor_address_2 || '',
          city: record.vendor_city || '',
          stateProvince: record.vendor_state || '',
          postalCode: record.vendor_zip_code || ''
        };
 
        const facilityReq = {
          addressLine1: record.facility_address_1 || '',
          addressLine2: record.facility_address_2 || '',
          city: record.facility_city || '',
          stateProvince: record.facility_state || '',
          postalCode: record.facility_zip_code || ''
        };
 
        // Build query params
        const vendorQryParams = new URLSearchParams(vendorReq).toString();
        const facilityQryParams = new URLSearchParams(facilityReq).toString();
 
        // Get OAuth token
        const token = await getOAuthToken();
 
        // Prepare URLs and options for vendor and facility API calls
        const baseURL = process.env.API_ENDPOINT;
   
        const optionsV = {
            method: 'GET',
            uri: `${baseURL}?${vendorQryParams}`,
            headers: {
               'Accept': 'application/json',
               'Authorization': `Bearer ${token}`,
            },
            json: true,
            resolveWithFullResponse: true
        };
 
        const optionsF = {
          method: 'GET',
            uri: `${baseURL}?${facilityQryParams}`,
            headers: {
               'Accept': 'application/json',
                'Authorization': `Bearer ${token}`,
            },
            json: true,
            resolveWithFullResponse: true
        };

        // Execute vendor and facility API calls in parallel
        const [responseV, responseF] = await Promise.all([
          request(optionsV),
          request(optionsF),
        ]);
 
        // Check vendor response and update DB if necessary
        if (responseV?.body[0]?.hasOwnProperty('status') && responseV?.body[0].status === 'F') {
          console.log(`VENDOR Status FAILED`,responseV?.body[0].status);
          await connection.query(
            `UPDATE ${process.env.DB}.${process.env.TABLE_2} SET manual_review_flag ='Y', manual_review_reason ='Address Failed', update_user = 'LAMBDA', update_timestamp = ? WHERE bill_id = ?`,
            [ updatedOn, billId]
          );
        } else if (parseInt(responseV?.body[0].confidence, 10) >= envConfidence && parseInt(responseV?.body[0].matchScore, 10) <= process.env.matchScore) {
          console.log('inside db fields update vendor');
          await connection.query(
            `UPDATE ${process.env.DB}.${process.env.TABLE} SET vendor_address_1 = ?, vendor_address_2 = ?, vendor_city = ?, vendor_state = ?, vendor_zip_code = ?, update_user = 'LAMBDA', update_timestamp = ? WHERE provider_id = ?`,
            [responseV?.body[0]?.addressLine1, responseV?.body[0]?.addressLine2, responseV?.body[0].city, responseV?.body[0].stateProvince, responseV?.body[0].postalCode, updatedOn, record.provider_id]
          );
        } else {
          await connection.query(
            `UPDATE ${process.env.DB}.${process.env.TABLE_2} SET manual_review_flag ='Y', manual_review_reason ='Address Failed', update_user = 'LAMBDA', update_timestamp = ? WHERE bill_id = ?`,
            [ updatedOn, billId]
          );
        }

        // Check facility response and update DB if necessary
        if (responseF?.body[0]?.hasOwnProperty('status') && responseF?.body[0].status === 'F') {
         console.log(` FACILITY Status failed`,responseF?.body[0].status);
          await connection.query(
            `UPDATE ${process.env.DB}.${process.env.TABLE_2} SET manual_review_flag ='Y', manual_review_reason ='Address Failed', update_user = 'LAMBDA', update_timestamp = ? WHERE bill_id = ?`,
            [ updatedOn, billId]
          );
        } else if (parseInt(responseF?.body[0].confidence, 10) >= envConfidence && parseInt(responseF?.body[0].matchScore, 10) <= envMatchScore) {
          console.log('inside db fields update facility');
          await connection.query(
            `UPDATE ${process.env.DB}.${process.env.TABLE} SET facility_address_1 = ?, facility_address_2 = ?, facility_city = ?, facility_state = ?, facility_zip_code = ?, update_user = 'LAMBDA', update_timestamp = ? WHERE provider_id = ?`,
            [responseF?.body[0]?.addressLine1, responseF?.body[0]?.addressLine2, responseF?.body[0].city, responseF?.body[0].stateProvince, responseF?.body[0].postalCode, updatedOn, record.provider_id]
          );
        } else {
          await connection.query(
            `UPDATE ${process.env.DB}.${process.env.TABLE_2} SET manual_review_flag ='Y', manual_review_reason ='Address Failed', update_user = 'LAMBDA', update_timestamp = ? WHERE bill_id = ?`,
            [ updatedOn, billId]
          );
        }
 
      } catch (error) {
        console.error('Error processing record:', error);
        return `Request failed`;
      }
    return `Sucessfully processed request`;
 
  } catch (error) {
    console.error('Error occurred:', error);
    return `Request failed`;
  } finally {
    // Close the connection
    if (connection) {
      // await connection.end();
      await connection.release();
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