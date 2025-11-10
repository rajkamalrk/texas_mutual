import request from 'request-promise-native';
import https from 'https';
import querystring from 'querystring';
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
let connection;

const setupConnection = async () => {
  console.log(connection, 'connection');

  if (!connection) {
    const pool = await initDbPool();
    connection = await pool.getConnection();
    console.log("DB connection initialized.");
  }
};

export const handler = async (event) => {
  try {
    await setupConnection();
    const billHeaderId = event.billHeaderId;
    const fileHeaderId = event.fileHeaderId;
    const tradingPartner = event.tradingPartner;
    let result = '';
    let postData;
    console.log('event.....',event);

    if (tradingPartner == 'optum') {
      [result] = await connection.query(`SELECT * FROM ${process.env.DB_2}.${process.env.TABLE_3} WHERE invoice_header_id = ?`, [billHeaderId]);
      postData = {
        claimNumber: result[0].claim_number
      };

    } else if (tradingPartner == 'align') {
      [result] = await connection.query(`SELECT * FROM ${process.env.DB_2}.${process.env.TABLE_4} WHERE invoice_header_id = ?`, [billHeaderId]);
      postData = {
        claimNumber: result[0].claim_number
      };
    } else if (tradingPartner == 'techhealth') {
      [result] = await connection.query(`SELECT * FROM ${process.env.DB_2}.${process.env.TABLE_5} WHERE invoice_header_id = ?`, [billHeaderId]);
      postData = {
        claimNumber: result[0].claim_number
      };
    }

    // Fetch  required Fields
    console.log('result', result);


    if (result.length === 0) {
      return 'No records found'
    }

    const now = new Date();
    const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
    const updatedOn = createdOn;


    // Invoke GW api via mule
    const callMulesoftApiResponse = await callMulesoftApi(result, connection, postData);
    console.log('callMulesoftApiResponse', callMulesoftApiResponse);
    // console.log('callMulesoftApiResponse[0]', JSON.parse(callMulesoftApiResponse)[0]);
    if (Object.keys(JSON.parse(callMulesoftApiResponse)[0]).length > 0) {
      console.log('object exists');
      let retrievedObject = JSON.parse(callMulesoftApiResponse)[0];
      console.log('retrievedObject.........', retrievedObject);
      console.log('txm-claimNumber', retrievedObject.claimNumber);
      console.log('retrievedObject.denial[0].type.code', retrievedObject.denial?.[0]?.type?.code);
      console.log('retrievedObject.jurisdiction.code', retrievedObject.jurisdiction?.code);
      console.log('retrievedObject.denial[0].category.code', retrievedObject.denial?.[0]?.category?.code);
      console.log('retrievedObject.denial[0].status.code', retrievedObject.denial?.[0]?.status?.code);
      console.log('retrievedObject.denial[1].reverseStatus.code', retrievedObject.denial?.[1]?.reverseStatus?.code);
      console.log('retrievedObject.condition.type.code', retrievedObject.condition?.[0]?.type?.code);
      console.log('retrievedObject.claimant.gender.code', retrievedObject.claimant?.gender?.code);

      const denial = retrievedObject?.denial;
      const condition = retrievedObject?.condition;

      const insertRecord = async (query, values) => {
        console.log('insertQuery', query);
        console.log('insertValues', values);
        const [result] = await connection.query(query, values);
        console.log('result.....',result);
        return result.insertId;
      };

      const processDenial = async (denial, claimDataId) => {
        const query = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_6} ( 
        claim_data_id,
        denial_type_code, 
        denial_type_description, 
        denial_category_code, 
        denial_category_description, 
        denial_status_code, 
        denial_status_description, 
        denial_effective_date, 
        denial_effectivefrom_date, 
        denial_effectiveto_date, 
        reverse_date, 
        reverse_status_code, 
        reverse_status_description, 
        create_user, 
        create_timestamp, 
        update_user, 
        update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

        for (const item of denial) {
          if (
            item?.category?.code === 'subrofuturecredmedindem' ||
            item?.category?.code === 'subrofuturecredmed'
          ) {
            console.log('item....denial', item);
            const values = [
              claimDataId,
              item?.type?.code,
              item?.type?.description,
              item?.category?.code,
              item?.category?.description,
              item?.status?.code,
              item?.status?.description,
              item?.effectiveDate,
              item?.effectiveFromDate,
              item?.effectiveToDate,
              item?.reverseDate,
              item?.reverseStatus?.code,
              item?.reverseStatus?.description,
              'LAMBDA',
              createdOn,
              'LAMBDA',
              updatedOn,
            ];
            await insertRecord(query, values);
          }
        };
      };

      const processCondition = async (condition, claimDataId) => {
        const query = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_7} ( 
        claim_data_id,
        conditional_effective_date, 
        conditional_expiry_date, 
        conditional_type_code, 
        conditional_type_description, 
        create_user, 
        create_timestamp, 
        update_user, 
        update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

        for (const item of condition) {
          if (item?.type?.code === 'subrofuturecredmedindem' || item?.type?.code === 'subrofuturecredmed') {
              console.log('item....condition', item);
              const values = [
                  claimDataId,
                  item?.effectiveDate,
                  item?.expiryDate,
                  item?.type?.code,
                  item?.type?.description,
                  'LAMBDA',
                  createdOn,
                  'LAMBDA',
                  updatedOn
              ];
              await insertRecord(query, values);
          }
      }
      };

      try {

      const insertQuery = `INSERT INTO ${process.env.DB_2}.${process.env.TABLE_2} (
        trading_partner, 
        file_header_id, 
        bill_header_id, 
        txm_claim_number, 
        txm_claim_secure, 
        txm_jurisdiction_code, 
        txm_jurisdiction_description, 
        patient_first_name, 
        patient_last_name, 
        patient_gender, 
        patient_gender_description, 
        date_of_birth, 
        date_of_injury, 
        patient_address_line_1, 
        patient_address_line_2, 
        patient_city, 
        patient_state_code, 
        patient_zip_code, 
        patient_phone_number, 
        subscriber_name, 
        subscriber_tax_id, 
        insured_policy_number, 
        subscriber_address_line_1, 
        subscriber_address_line_2, 
        subscriber_city, 
        subscriber_state, 
        subscriber_zip_code, 
        subscriber_telephone_number, 
        social_security_number, 
        create_user, 
        create_timestamp, 
        update_user, 
        update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    const insertValues = [
        tradingPartner,
        fileHeaderId,
        billHeaderId,
        retrievedObject.claimNumber,
        retrievedObject.claimSecure,
        retrievedObject.jurisdiction?.code,
        retrievedObject.jurisdiction?.description,
        retrievedObject.claimant?.firstName,
        retrievedObject.claimant?.lastName,
        retrievedObject.claimant?.gender?.code,
        retrievedObject.claimant?.gender?.description,
        retrievedObject.claimant?.dateOfBirth,
        retrievedObject.claimant?.dateOfInjury,
        retrievedObject.claimant?.address?.line1,
        retrievedObject.claimant?.address?.line2,
        retrievedObject.claimant?.address?.city,
        retrievedObject.claimant?.address?.state,
        retrievedObject.claimant?.address?.zipCode,
        retrievedObject.claimant?.contact?.homePhone,
        retrievedObject.insured?.insuredName,
        retrievedObject.insured?.taxID,
        retrievedObject.insured?.policyNumber,
        retrievedObject.insured?.address?.line1,
        retrievedObject.insured?.address?.line2,
        retrievedObject.insured?.address?.city,
        retrievedObject.insured?.address?.state,
        retrievedObject.insured?.address?.zipCode,
        retrievedObject.insured?.contact?.phone,
        retrievedObject.claimant?.socialSecurityNumber,
        'LAMBDA',
        createdOn,
        'LAMBDA',
        updatedOn
    ];
      

        const claimDataId = await insertRecord(insertQuery, insertValues);
        console.log('claimDataId.....', claimDataId)

        if (denial) {
          console.log('denial', denial);
          await processDenial(denial, claimDataId);
        }

        if (condition) {
          console.log('condition', condition);
          await processCondition(condition, claimDataId);
        }

        return 'Inserted record successfully';

      } catch (err) {
        console.log('err', err);
        return err;
      }

    }
    else if (Object.keys(JSON.parse(callMulesoftApiResponse)[0]).length === 0) {
      console.log('object does not exists');
      return 'Data not received from GW for provided claim';

    }

  } catch (error) {
    return error;
  }
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

const callMulesoftApi = async (result, connection, postData) => {
  console.log('In callMulesoftApi');
  const token = await getOAuthToken();
  const auth = `Bearer ${token}`;
  console.log('auth', auth);


  const options = {
    method: process.env.METHOD,
    url: process.env.MULESOFT_API,
    headers: {
      'Accept': 'application/json',
      'Authorization': auth,
      'Content-Type': 'application/json'

      // 'X-GW-Environment': process.env.X_GW_ENVIRONMENT,
    },
    body: JSON.stringify(postData),

  };
  console.log('options', options);


  try {
    const response = await request(options);
    if (response.statusCode !== 200) {
      return response;
    }
    return response;
  } catch (error) {
    console.error('API call failed:', error);
    return error;
  }
};

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

  const secretDetails = await getSecretDetails(process.env.CLIENT_DETAILS_SECRET);

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
