import https from 'https';
import querystring from 'querystring';
import request from 'request-promise-native';
import AWS from 'aws-sdk';
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { initDbPool } from './db-config.mjs';

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

// Set up DB connection once for the Lambda lifecycle
const setupConnection = async () => {
  if (!connection) {
    const pool = await initDbPool();
    connection = await pool.getConnection();
    console.log("DB connection initialized.");
  }
};

// Process a batch of records concurrently
const processBatchRecords = async (batch) => {
  console.log('batch....',batch);
  const promises = batch.map(async (record) => {
    console.log('record!!!!1', record);
    // Process each record
    const [result2] = await connection.query(
      `SELECT bill_id, date_bill_received_by_payer, trading_partner, txm_claim_number 
      FROM ${process.env.DB_1}.${process.env.TABLE_2} WHERE txm_invoice_number = ?`, 
      [record.txm_invoice_number]
    );

    console.log('result2:', result2);

    const itemType = result2[0].trading_partner === 'optum' ? 'TWCC-66' : 'HCFAEBIL';
    const response = await getDocSerailNumber(record, result2[0].txm_claim_number, itemType);
    console.log('response',response);

    if (response && JSON.parse(response).length > 0) {
      let res = JSON.parse(response);
      let documentSerialNumber = res[res.length-1].serialNumber;
      console.log('documentSerialNumber', documentSerialNumber);

      // Update document serial number
      const updateQueryA = `UPDATE ${process.env.DB_1}.${process.env.TABLE_6} SET document_serial_number = ? WHERE bill_id = ? and file_type = 'XML'`;
      await connection.query(updateQueryA, [documentSerialNumber, result2[0].bill_id]);

      const [resultA] = await connection.query(`
      SELECT 
        *
      FROM 
        ${process.env.DB_1}.${process.env.TABLE_2} bh
      INNER JOIN  
        ${process.env.DB_1}.${process.env.TABLE_6} bd ON bh.bill_id = bd.bill_id
      WHERE 
        bh.bill_id = ?
      `, [result2[0].bill_id]);
      let attachments = [];
      let itemTypeA;

      console.log('resultA',resultA);
      console.log('result2',result2);
  
      for(let i = 0; i < resultA.length; i++){
        if (result2[0].trading_partner == 'optum'){
          attachments.push( {
            documentPath: `urn:ecbrowser:${resultA[i].document_serial_number}`,
            sourceId: resultA[i].txm_claim_number,
            itemType: 'TWCC-66'
          })
        }
        else if(result2[0].trading_partner == 'align'){
          console.log('filetype....align',resultA[i].file_type)
          if(resultA[i].file_type === 'ATTACHMENT'){
            itemTypeA = resultA[i].txm_doc_type
          }else if(resultA[i].file_type === 'XML') {
            itemTypeA =  'HCFAEBIL'
          }
          attachments.push( {
            documentPath: `urn:ecbrowser:${resultA[i].document_serial_number}`,
            sourceId: resultA[i].invoice_id,
            itemType: itemTypeA
          })
        }
        else if(result2[0].trading_partner == 'techhealth'){
          console.log('filetype....techhealth',resultA[i].file_type)
          if(resultA[i].file_type === 'ATTACHMENT'){
            itemTypeA = resultA[i].txm_doc_type
          }else if(resultA[i].file_type === 'XML') {
            itemTypeA =  'HCFAEBIL'
          }
          attachments.push( {
            documentPath: `urn:ecbrowser:${resultA[i].document_serial_number}`,
            sourceId: resultA[i].invoice_id,
            itemType: itemTypeA
          })
        }
        
      }
  
  
      console.log('resultA', resultA);
      console.log("attachments",attachments);

      const data = {
        sourceSystem: result2[0].trading_partner,
        ingestionSource: "ProviderMatch",
        documents: attachments,
        receiveDate: new Date(result2[0].date_bill_received_by_payer).toISOString(),
        envelopeAttributes: {
          "Invoice Number": record.txm_invoice_number,
          "Notify Bitx EBill Processed": "true"
        },
        workItemAttributes:
        {
          "Invoice Number": record.txm_invoice_number
        }
 
      };

      const lambdaInvokeResponse = await invokeLambda(data);
      const payload = JSON.parse(lambdaInvokeResponse.Payload);
      const resBody = JSON.parse(payload.body);

      if (payload.statusCode === 200) {
        // Update database with vendor match response
        const updateQueryA = `UPDATE ${process.env.DB_1}.${process.env.TABLE_2} SET manual_vendormatch_request_response = ? WHERE bill_id = ?`;
        await connection.query(updateQueryA, [JSON.stringify(resBody?.response), result2[0].bill_id]);
      }

      // Handle error response
      if (payload.statusCode !== 200) {
        const errorResBody = JSON.parse(resBody.error);
        const { status, errorType: errorCode } = errorResBody;
        const userMessage = errorResBody.message.error;

        await connection.beginTransaction();

        const updateQueryECI = `UPDATE ${process.env.DB_1}.${process.env.TABLE_1} SET status_type = 'VENDOR_MATCH_MANUAL', status = 'FAILED', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
        await connection.query(updateQueryECI, [new Date().toISOString(), record.txm_invoice_number]);

        const insertQueryMaster = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_5} 
          (source, trading_partner, status, start_datetime, update_timestamp, create_user, update_user) 
          VALUES (?, ?, ?, ?, ?, ?, ?)`;
        let [insertResult] = await connection.query(insertQueryMaster, ['LAMBDA', result2[0].trading_partner, 'VENDOR_MATCH_MANUAL', new Date().toISOString(), new Date().toISOString(), 'LAMBDA', 'LAMBDA']);

        const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_3} 
          (batch_id, bill_id, txm_invoice_number, source, target, match_type, error_code, error_type, error_message, create_timestamp, update_timestamp, create_user, update_user) 
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
        const insertValues = [
          insertResult.insertId, result2[0].bill_id, record.txm_invoice_number, 'BITX', 'ECI', 'VENDOR_MATCH_MANUAL', status, errorCode, userMessage, 
          new Date().toISOString(), new Date().toISOString(), 'LAMBDA', 'LAMBDA'
        ];

        await connection.query(insertQuery, insertValues);
        await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_4} 
          (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) 
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [record.status_id, 'VENDOR_MATCH_AUTO', 'VENDOR_MATCH_FROM_ECI', 'FAILED', 'LAMBDA', 'LAMBDA', new Date().toISOString(), new Date().toISOString()]);
        
        await connection.commit();

        const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
        const subject = `${process.env.ENVIRONMENT} - ${result2[0].trading_partner.toUpperCase()} - Error occurred for Vendor Match Manual - FATAL`;
        const message = `  Process: ${result2[0].trading_partner.toUpperCase()} - Vendor Match Manual \n 
  Error: Error occurred in Vendor Match Manual
  Claim Number: ${result2[0].txm_claim_number} 
  Invoice Number: ${record.txm_invoice_number} 
  Invoice Status type: 'VENDOR_MATCH_MANUAL' 
  Invoice Status: 'FAILED' 
  Timestamp: ${ts}`;
        const sendEmailResponse = await sendEmail(subject, message);
        console.log('sendEmailResponse', sendEmailResponse);

        // Need to update environment in the lambda.tf file
        // Add SNS topic
      } else {
        await connection.beginTransaction();
        const updateQueryECI = `UPDATE ${process.env.DB_1}.${process.env.TABLE_1} 
          SET status_type = 'VENDOR_MATCH_MANUAL', status = 'IN_PROGRESS', update_user = 'LAMBDA', update_timestamp = ? WHERE txm_invoice_number = ?`;
        await connection.query(updateQueryECI, [new Date().toISOString(), record.txm_invoice_number]);

        await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_4} 
          (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp) 
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [record.status_id, 'VENDOR_MATCH_MANUAL', 'VENDOR_MATCH_FROM_ECI', 'IN_PROGRESS', 'LAMBDA', 'LAMBDA', new Date().toISOString(), new Date().toISOString()]);

        await connection.commit();
      }
    }
  });

  // Wait for all promises to complete concurrently
  await Promise.all(promises);
};

// Main Lambda handler
export const handler = async () => {
  await setupConnection();

  // Fetch records in batches
  const batchSize = 100;  // Adjust this as needed based on your Lambda limits
  let offset = 0;
  const records = [];

  // Fetch all records in batches from DB
  while (true) {
    const [result] = await connection.query(
      `SELECT * FROM ${process.env.DB_1}.${process.env.TABLE_1} WHERE status = 'PENDING' and update_timestamp >= NOW() - INTERVAL 7 DAY LIMIT ? OFFSET ?`, 
      [batchSize, offset]
    );

    console.log('result', result);
    if (result.length === 0) break;  // Exit when no more records are found
    records.push(...result);
    offset += batchSize;
  }

  // Process records in batches
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    console.log('batch from handler function',batch);
    await processBatchRecords(batch);
  }

  return 'Successfully processed pending vendor match manual records';
};

// Helper function to get OAuth token
const getSecretDetails = async (secretName) => {
  const client = new SecretsManagerClient({ region: process.env.REGION });
  try {
    const response = await client.send(new GetSecretValueCommand({ SecretId: secretName, VersionStage: "AWSCURRENT" }));
    return JSON.parse(response.SecretString);
  } catch (error) {
    console.error('Error in getSecretDetails:', JSON.stringify(error, Object.getOwnPropertyNames(error)));
    throw error;
  }
};

// Function to get OAuth token
const getOAuthToken = async () => {

  // const secret = await secretsManagerCache.getSecretValue(TOKEN_SECRET_NAME);
  const cachedSecret = cache.get(process.env.TOKEN_SECRET_NAME);
  console.log('cachedSecret', cachedSecret);
  // console.log('cachedSecret.token', cachedSecret.token);


  if (cachedSecret) {
    console.log('typeOFcachedSecret.token', typeof(cachedSecret));
    console.log('Token retrieved from cache', JSON.parse(cachedSecret).token);
    return JSON.parse(cachedSecret).token;
  }

  const generateTokenResult = await generateToken();
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

const generateToken = async () => {

  const tokenEndpoint = process.env.TOKEN_ENDPOINT;

  const secretDetails = await getSecretDetails(process.env.SECRET_NAME);

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

// Function to fetch document serial number
async function getDocSerailNumber(result, txmClaimNumber, itemType) {
  const token = await getOAuthToken();
  const options = {
    method: process.env.METHOD,
    uri: `${process.env.MULESOFT_API}`,
    headers: {
      'Accept': 'application/json',
      'Authorization': `Bearer ${token}`,
    },
    qs: { invoiceNumbers: result.txm_invoice_number, itemTypes: itemType },
  };
  console.log('options', options);
  let getDocSerailNumberResult = await request(options);
  console.log('getDocSerailNumberResult', getDocSerailNumberResult);
  return getDocSerailNumberResult;
}

// Function to invoke a Lambda function
const invokeLambda = async (data) => {
  const params = { FunctionName: process.env.INVOKE_LAMBDA, Payload: JSON.stringify(data) };
  return await lambda.invoke(params).promise();
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