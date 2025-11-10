import AWS from 'aws-sdk';
import {initDbPool} from './db-config.mjs';
import request from 'request-promise-native';
import https from 'https';
import querystring from 'querystring';
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";

import NodeCache from 'node-cache';

const secretsManager = new AWS.SecretsManager();
const cache = new NodeCache({ stdTTL: 1800 });  // TTL of 30 minutes (1800 seconds)

const retryDelay = 1000;  // Initial delay of 1 second
const maxRetries = 5;


const lambda = new AWS.Lambda();
let connection;

const setupConnection = async () => {
    console.log(connection,'connection');

    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
    }
};

const validate = (val) => {
    return val === undefined || val === null || val.trim() === '';
};

export const handler = async (event, context) => {
    
try {
  await setupConnection();

  const data = JSON.parse(event.body);
  console.log('data', data);

  const { claimNumber, itemType, receiveDate } = data;
  
  if(validate(claimNumber) || validate(itemType) || validate(receiveDate)) {
      return {
            statusCode: 400,
            body: JSON.stringify({
            "status": "error",
            "errorMessage": "Invalid payload",
            "errorDescription": "Required fields not provided or invalid",
            "transactionId": context.awsRequestId
            })
        };
    }
    
  const itemTypes = ["TWCC-70", "DENTEBIL", "DENT3RD", "DNTLREIM", "HCFA", "HCFAEBIL", "HCFA3RD", "HCFAREIM", "TWCC-66", "EDIRXINV", "RX3RD", "RXREIM", "UB", "UBEBILL", "UB3RD", "UBREIM", "RFND", "RSBILL"];
   if (!itemTypes.includes(itemType)) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                "status": "error",
                "errorMessage": "Invalid item type submitted",
                "errorDescription": "Item type not recognized",
                "transactionId": context.awsRequestId
            })
        };
    }
  const invokeLambdaResponse = await invokeLambda({"numIncrements": 1});
  const invoiceNumber = invokeLambdaResponse.data.invoiceNumber;
  const response = await callMulesoftApi(claimNumber, context);
  console.log('response', response);
  let responseObject = response.body;
  responseObject = responseObject[0];
  console.log('responseObject', responseObject);
  console.log('Object.keys(responseObject).length', Object.keys(responseObject).length);
        if (response && response.statusCode === 200) {
            if((Object.keys(responseObject).length == 0)){
                return {
                    statusCode: 400,
                    body: JSON.stringify({
                        "status": "error",
                        "errorMessage": "Claim Number Does Not Exists in ClaimCenter",
                        "errorDescription": "Claim Number Does Not Exists in ClaimCenter",
                        "transactionId": context.awsRequestId
                    })
                };       
            }else if((Object.keys(responseObject).length > 0)){
                    const now = new Date();
                    const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
                    const updatedOn = createdOn;
                        
                    const billHeaderQuery = `INSERT INTO ${process.env.DB}.${process.env.TABLE} (txm_invoice_number, reported_claim_number, date_bill_received_by_payer, txm_doc_type, create_user, create_timestamp, update_timestamp) VALUES(?, ?, ?, ?, ?, ?, ?)`;
                    console.log('values', invoiceNumber, claimNumber, receiveDate, itemType);
                    console.log('billHeaderQuery',billHeaderQuery);

                    try {
                        await connection.beginTransaction();
                        const [results] = await connection.query(billHeaderQuery, [invoiceNumber, claimNumber, receiveDate, itemType, 'LAMBDA', createdOn, updatedOn]);
                            const billId = results.insertId;
                            console.log('billId', billId);
                
                            await connection.commit();
                            // await connection.end();
                            await connection.release();
                            return {
                                statusCode: 201,
                                body: JSON.stringify({
                                    status: "success",
                                    message: "Request processed successfully",
                                    data: { invoiceNumber: invoiceNumber },
                                    transactionId: context.awsRequestId
                                })
                            };
                    } catch(queryError) {
                        await connection.rollback();
                        // await connection.end();
                        await connection.release();
                        console.error('Transaction error:', queryError);
                        return {
                            statusCode: 500,
                            body: JSON.stringify({
                                status: "error",
                                errorMessage: "Internal server error",
                                errorDescription: "Error occured while processing the request",
                                transactionId: context.awsRequestId
                            })
                        };
                    } 
            }
        }else{
            return {
                statusCode: 500,
                body: JSON.stringify({
                    status: "error",
                    errorMessage: "Error occured while processing the request",
                    errorDescription: response,
                    transactionId: context.awsRequestId
                })
            }           
        }  
  
} catch(error) {
    console.log("Error occured", error);
    return {
            statusCode: 500,
            body: JSON.stringify({
                status: "error",
                errorMessage: "Internal server error",
                errorDescription: "Error occured while processing the request",
                transactionId: context.awsRequestId
            })
    };
}
}
  
const invokeLambda = async (data) => {
  let params = {
    FunctionName: process.env.GENERATE_NUMBER,
    Payload: JSON.stringify(data)
  };
 
  try {
    let response = await lambda.invoke(params).promise();
    const responsePayload = JSON.parse(response.Payload);
    return JSON.parse(responsePayload.body);
  } catch (e) {
    console.log('invokeLambda :: Error: ' + e);
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
  
const callMulesoftApi = async (claimNumber, context) => {
  
    console.log('In callMulesoftApi');

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
    
    options.body = {
        claimNumber: claimNumber
    };
    
    const response1 = await request(options);
    console.log('options1', options);
    console.log('response1', response1);
    return response1;

  } catch (error) {
    console.error('API call failed:', error);
    let { errorCode } = error.error;
    return {
      statusCode: errorCode || 'UNKNOWN',
      body: {
        status: 'error',
        errorMessage: 'Error occurred while invoking GW API claim match',
        errorDescription: error,
        transactionId: context.awsRequestId
      },
    };
  }
}


const getOAuthToken = async (context) => {

    console.log('In getOAuthToken');
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