import mysql from "mysql2/promise";
import request from "request-promise-native";
import https from "https";
import querystring from "querystring";
import AWS from "aws-sdk";
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";
import Joi from "joi";
import { initDbPool } from "./db-config.mjs";
import NodeCache from "node-cache";
import csv from "csv-parser";
import fs from "fs";
import { Readable } from "stream";

// AWS clients
const secretsManager = new AWS.SecretsManager();
const cache = new NodeCache({ stdTTL: 1800 }); // 30 min
const s3 = new AWS.S3();
const lambda = new AWS.Lambda();

// Constants
let connection;
const retryDelay = 1000;
const maxRetries = 5;
const BILL_TYPE_CACHE_TTL = 1800; // 30 min
const S3_BUCKET_MAP = {
  dev: "txm-claimant-billingestion-dev",
  qa: "txm-claimant-billingestion-qa",
  stg: "txm-claimant-billingestion-stg",
  prod: "txm-claimant-billingestion-prod",
};
const S3_CONFIG_KEY =
  "config/bitx_static_config/bill_item_type/bill_item_type.csv";

// =================== ENV & DB UTILS ===========================

function getEnvStage() {
  // assumes process.env.ENV is set to dev/qa/stg/prod
  return process.env.ENVIRONMENT || "dev";
}

async function setupConnection() {
  if (!connection) {
    const pool = await initDbPool();
    connection = await pool.getConnection();
    console.log("DB connection initialized.");
  }
}

// =================== LOAD BILL ITEM MAP FROM S3 ===========================

async function loadBillItemTypeMap() {
  let cached = cache.get("billItemTypeMap");
  if (cached) return cached;

  const envStage = getEnvStage();
  const bucket = S3_BUCKET_MAP[envStage] || S3_BUCKET_MAP.dev;
  const s3Key = S3_CONFIG_KEY;

  try {
    const s3Res = await s3.getObject({ Bucket: bucket, Key: s3Key }).promise();

    if (!s3Res.Body) {
      throw new Error(
        `bill_item_type.csv missing or empty in bucket ${bucket}`
      );
    }

    const results = {};
    const stream =
      s3Res.Body instanceof Buffer
        ? Readable.from(s3Res.Body.toString("utf-8"))
        : s3Res.Body;

    await new Promise((resolve, reject) => {
      stream
        .pipe(csv())
        .on("data", (data) => {
          // Keys match CSV headers (case-sensitive)!
          let key = (
            data.billItemType ||
            data.bill_item_type ||
            data[Object.keys(data)[0]]
          ).trim();
          results[key] = {
            RequireRenderingNPI:
              (data.RequireRenderingNPI || "").toLowerCase() === "true",
            isLicenseRequired:
              (data.isLicenseRequired || "").toLowerCase() === "true",
            ValidateRenderingProviderLicense:
              (data.ValidateRenderingProviderLicense || "").toLowerCase() ===
              "true",
          };
        })
        .on("end", resolve)
        .on("error", reject);
    });

    if (Object.keys(results).length === 0) {
      throw new Error("bill_item_type.csv contains no records");
    }
    cache.set("billItemTypeMap", results, BILL_TYPE_CACHE_TTL);
    console.log("Loaded billItemTypeMap from S3:", results);
    return results;
  } catch (e) {
    console.error("Could not load bill_item_type config from S3: ", e);
    throw new Error("Failed to read bill_item_type config from S3");
  }
}

// =================== MAIN HANDLER ===========================

export const handler = async (event, context) => {
  try {
    await setupConnection();

    const { body } = event;
    let parsedBody = body && typeof body === "string" ? JSON.parse(body) : body;
    console.log("parsedBody", parsedBody);

    const billItemType = parsedBody.billItemType;

    // Load bill type mapping from S3
    const billItemTypeMap = await loadBillItemTypeMap();
    const allowedBillItemTypes = Object.keys(billItemTypeMap);
    const billItemTypeMapKeysString = allowedBillItemTypes.join(", ");
    return {
      statusCode: 200,
      body: JSON.stringify({
        status: "success",
        billItemTypeMapKeys: billItemType,
        transactionId: context.awsRequestId,
      }),
    };
    if (!billItemType || !(billItemType in billItemTypeMap)) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          status: "error",
          errorMessage: "Invalid billItemType",
          errorDescription: `Allowed types: ${Object.keys(billItemTypeMap).join(
            ", "
          )}`,
          transactionId: context.awsRequestId,
        }),
      };
    }

    // Select rules according to S3 config for this billItemType
    const isLicenseRequired = billItemTypeMap[billItemType].isLicenseRequired;
    const requireRenderingNPI =
      billItemTypeMap[billItemType].RequireRenderingNPI;
    const validateRenderingProviderLicense =
      billItemTypeMap[billItemType].ValidateRenderingProviderLicense;

    // Dynamic JOI schema
    const schema = Joi.object({
      billItemType: Joi.string().required(),
      vendorNumber: Joi.string().required(),
      renderingProviderNpi: Joi.string().when("$isLicenseRequired", {
        is: true,
        then: Joi.string().required(),
        otherwise: Joi.string().allow(""),
      }),
      renderingProviderLicense: Joi.string().when("$isLicenseRequired", {
        is: true,
        then: Joi.string().required(),
        otherwise: Joi.string().allow(""),
      }),
      renderingProviderName: Joi.string().required(),
      facilityName: Joi.string().allow("", null),
      facilityAddress1: Joi.string().required(),
      facilityAddress2: Joi.string().allow(""),
      facilityCity: Joi.string().required(),
      facilityState: Joi.string().required(),
      facilityZipCode: Joi.string().required(),
      facilityNpi: Joi.string().allow(""),
    });

    // Run validation
    const { error: schemaError } = schema.validate(parsedBody, {
      context: { isLicenseRequired },
    });
    if (schemaError) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          status: "error",
          errorMessage: "Invalid Bill Item Type &BillItemType",
          errorDescription: schemaError.details[0].message,
          transactionId: context.awsRequestId,
        }),
      };
    }

    // ASSIGN input
    const renderingProviderLicense = parsedBody.renderingProviderLicense;
    const renderingProviderName = parsedBody.renderingProviderName;
    const renderingProviderNpi = parsedBody.renderingProviderNpi;
    const vendorNumber = parsedBody.vendorNumber;
    let facilityName = parsedBody.facilityName;
    const facilityAddress1 = parsedBody.facilityAddress1;
    const facilityAddress2 = parsedBody.facilityAddress2;
    const facilityCity = parsedBody.facilityCity;
    let facilityState = parsedBody.facilityState;
    if (facilityState === "Texas") facilityState = "TX";
    const facilityZipCode = parsedBody.facilityZipCode;
    const facilityNpi = parsedBody.facilityNpi;

    // Zip logic
    const zip5 = facilityZipCode ? facilityZipCode.slice(0, 5) : "";

    let duplicateId = null;
    let duplicateQuery,
      duplicateValues = [];

    // ======= DYNAMIC DUPLICATE LOGIC ==========
    if (isLicenseRequired) {
      duplicateQuery = `
        SELECT provider_number
        FROM ${process.env.DB_2}.${process.env.TABLE_2}
        WHERE vendor_number = ?
          AND rendering_provider_npi = ?
          AND rendering_provider_license = ?
          AND facility_address_1 = ?
          AND facility_city = ?
          AND facility_state = ?
          AND SUBSTR(facility_zip_code,1,5) = ?
          AND Vendor_Status = 'Active'
        LIMIT 1
      `;
      duplicateValues = [
        vendorNumber,
        renderingProviderNpi,
        renderingProviderLicense,
        facilityAddress1,
        facilityCity,
        facilityState,
        zip5,
      ];
    } else {
      duplicateQuery = `
        SELECT provider_number
        FROM ${process.env.DB_2}.${process.env.TABLE_2}
        WHERE vendor_number = ?
          AND rendering_provider_npi = ?
          AND facility_address_1 = ?
          AND facility_city = ?
          AND facility_state = ?
          AND SUBSTR(facility_zip_code,1,5) = ?
          AND Vendor_Status = 'Active'
        LIMIT 1
      `;
      duplicateValues = [
        vendorNumber,
        renderingProviderNpi,
        facilityAddress1,
        facilityCity,
        facilityState,
        zip5,
      ];
    }

    // Run duplicate check
    const [duplicateRows] = await connection.query(
      duplicateQuery,
      duplicateValues
    );
    if (duplicateRows.length > 0) {
      duplicateId = duplicateRows[0].provider_number;
    }
    if (duplicateId) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          status: "success",
          message: "Provider already exists.",
          data: { providerNumber: duplicateId },
          transactionId: context.awsRequestId,
        }),
      };
    }

    // =========== PARALLEL API CALLS ============
    const apiCalls = [callMulesoftApi(vendorNumber, context)];

    if (isLicenseRequired) {
      if (!renderingProviderLicense || renderingProviderLicense.trim() === "") {
        return {
          statusCode: 400,
          body: JSON.stringify({
            status: "error",
            errorMessage:
              "renderingProviderLicense is required for this billItemType.",
            errorDescription:
              "Missing renderingProviderLicense for billItemType requiring license.",
            transactionId: context.awsRequestId,
          }),
        };
      }
      apiCalls.push(callMulesoftApi(renderingProviderLicense, context));
    }

    const [vendorResponse, licenseResponse] = await Promise.all(apiCalls);

    // Validate license if required
    if (isLicenseRequired) {
      if (
        !licenseResponse ||
        licenseResponse.statusCode !== 200 ||
        !Array.isArray(licenseResponse.body) ||
        licenseResponse.body.length === 0
      ) {
        return {
          statusCode: 400,
          body: JSON.stringify({
            status: "error",
            errorMessage:
              "Provider record can't be created because of an Invalid rendering_provider_license",
            errorDescription:
              "Provider record can't be created because of an Invalid rendering_provider_license",
            transactionId: context.awsRequestId,
          }),
        };
      }
    }

    // ====== PROCESS VENDOR RESPONSE =========
    let vendorTaxId = "";
    let vendorName = "";
    let vendorAdditonalName = "";
    let vendorAddress1 = "";
    let vendorAddress2 = "";
    let vendorCity = "";
    let vendorState = "";
    let vendorZipCode = "";
    let vendorPhoneNumber = "";

    const response = vendorResponse;
    if (!response || response.statusCode !== 200) {
      return (
        response || {
          statusCode: 500,
          body: JSON.stringify({
            status: "error",
            errorMessage: "Failed to retrieve vendor information",
            errorDescription: "Vendor API returned invalid response",
            transactionId: context.awsRequestId,
          }),
        }
      );
    }

    if (!Array.isArray(response.body) || response.body.length === 0) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          status: "error",
          errorMessage:
            "Provider record can't be created because of an invalid vendor number",
          errorDescription:
            "Provider record can't be created because of an invalid vendor number",
          transactionId: context.awsRequestId,
        }),
      };
    }

    const vendorData = response.body[0];
    if (!vendorData) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          status: "error",
          errorMessage: "Invalid vendor data received",
          errorDescription: "Vendor API returned empty data",
          transactionId: context.awsRequestId,
        }),
      };
    }
    vendorTaxId = vendorData.taxID || "";
    vendorName = vendorData.vendorName || "";
    vendorAdditonalName = vendorData.vendorName || "";
    vendorAddress1 = vendorData.address?.line1 || "";
    vendorAddress2 = vendorData.address?.line2 || "";
    vendorCity = vendorData.address?.city || "";
    vendorState = vendorData.address?.state || "";
    vendorZipCode = vendorData.address?.zipCode || "";
    vendorPhoneNumber = vendorData.contact?.phone || "";

    // Lambda for provider number
    const invokeLambdaResponse = await invokeLambda();
    console.log("invokeLambdaResponse", invokeLambdaResponse);
    const providerNumber =
      invokeLambdaResponse &&
      invokeLambdaResponse.data &&
      invokeLambdaResponse.data.providerNumber
        ? invokeLambdaResponse.data.providerNumber
        : null;

    console.log("providerNumber", providerNumber);

    if (!providerNumber) {
      return {
        statusCode: 500,
        body: JSON.stringify({
          status: "error",
          errorMessage: "Failed to generate provider number",
          errorDescription:
            "Provider number generation service returned invalid response",
          transactionId: context.awsRequestId,
        }),
      };
    }

    // NPI validation
    if (renderingProviderNpi && !/^\d{10}$/.test(renderingProviderNpi)) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          status: "error",
          errorMessage: "Invalid value passed in the request",
          errorDescription:
            "Invalid renderingProviderNpi. It must be a numeric value with a length of 10.",
          transactionId: context.awsRequestId,
        }),
      };
    }
    if (facilityNpi && !/^\d{10}$/.test(facilityNpi)) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          status: "error",
          errorMessage: "Invalid value passed in the request",
          errorDescription:
            "Invalid facilityNpi. It must be a numeric value with a length of 10.",
          transactionId: context.awsRequestId,
        }),
      };
    }

    // Audit fields
    const now = new Date();
    const createdOn = now.toISOString().slice(0, 19).replace("T", " ");
    const updatedOn = createdOn;
    const createdBy = "LAMBDA";
    const updatedBy = "LAMBDA";

    if (!facilityName) {
      facilityName = renderingProviderName;
      console.log("facilityName is null or blank", facilityName);
    }

    // Need to fill with correct data if required
    let ssnTaxIdIndicator = "";
    let providerEntityType = "";
    let licenseType = "";
    let providerJurisdiction = "";

    // INSERT SQL
    let query = `
      INSERT INTO ${process.env.DB_2}.${process.env.TABLE_2}
      (bill_item_type, rendering_provider_license, rendering_provider_name, vendor_number, facility_name, facility_address_1, facility_address_2,
      facility_city, facility_state, facility_zip_code, rendering_provider_npi, facility_npi, provider_number, vendor_tax_id_indicator, provider_entity_type,
      vendor_license_type, rendering_provider_jurisdiction, vendor_tax_id, vendor_name, vendor_address1, vendor_address2, vendor_city, vendor_state,
      vendor_zip_code, vendor_phone, create_user, update_user, create_timestamp, update_timestamp)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    let queryValues = [
      billItemType,
      renderingProviderLicense,
      renderingProviderName,
      vendorNumber,
      facilityName,
      facilityAddress1,
      facilityAddress2,
      facilityCity,
      facilityState,
      facilityZipCode,
      renderingProviderNpi,
      facilityNpi,
      providerNumber,
      ssnTaxIdIndicator,
      providerEntityType,
      licenseType,
      providerJurisdiction,
      vendorTaxId,
      vendorName,
      vendorAddress1,
      vendorAddress2,
      vendorCity,
      vendorState,
      vendorZipCode,
      vendorPhoneNumber,
      createdBy,
      updatedBy,
      createdOn,
      updatedOn,
    ];
    console.log("query", query);
    console.log("queryValues", queryValues);

    await connection.query(query, queryValues);

    return {
      statusCode: 200,
      body: JSON.stringify({
        status: "success",
        message: "Provider created successfully.",
        data: { providerNumber },
        transactionId: context.awsRequestId,
      }),
    };
  } catch (error) {
    console.log("Error occured", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        status: "error",
        errorMessage: "Internal server error",
        errorDescription: error.message,
        transactionId: context.awsRequestId,
      }),
    };
  }
};

// =================== EXTERNAL/API UTILS ===========================
// Get AWS Secret field (JSON)
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
    console.error(
      "Error in getSecretDetails:",
      JSON.stringify(error, Object.getOwnPropertyNames(error))
    );
    throw error;
  }
};

// Invoke Lambda for provider number
const invokeLambda = async () => {
  let params = {
    FunctionName: process.env.INVOKE_LAMBDA,
    Payload: JSON.stringify({}),
  };
  try {
    let response = await lambda.invoke(params).promise();
    console.log("response", response);
    const responsePayload = response.Payload;
    const responseObj = JSON.parse(responsePayload);
    // Defensive: payload is likely stringified
    if (typeof responseObj.body === "string") {
      return JSON.parse(responseObj.body);
    }
    return responseObj.body;
  } catch (e) {
    console.log("invokeLambda :: Error: " + e);
    throw e;
  }
};

// Call MuleSoft API. GW API call.
const callMulesoftApi = async (vendorNumber, context) => {
  const apiUrl = process.env.MULESOFT_API;

  try {
    const parsedUrl = new URL(apiUrl);
    if (parsedUrl.protocol !== "https:") {
      throw new Error("MuleSoft API must use HTTPS protocol");
    }
  } catch (error) {
    throw new Error(`Invalid MuleSoft API URL: ${error.message}`);
  }

  const token = await getOAuthToken(context);
  const auth = `Bearer ${token}`;
  let finalRequestBody = {
    vendorNumber: vendorNumber?.toString() || "",
  };
  const options = {
    method: process.env.METHOD,
    uri: apiUrl,
    headers: {
      Accept: "application/json",
      Authorization: auth,
      "X-GW-Environment": process.env.X_GW_ENVIRONMENT,
    },
    body: finalRequestBody,
    json: true,
    resolveWithFullResponse: true,
  };
  console.log("options", options);
  try {
    const response = await request(options);
    if (response.statusCode !== 200) {
      return {
        statusCode: response.statusCode,
        body: JSON.stringify({
          status: "error",
          errorMessage: "error occurred while calling GW api",
          errorDescription: "error occurred while calling GW api",
          transactionId: context.awsRequestId,
        }),
      };
    }
    return response;
  } catch (error) {
    return {
      statusCode: 500,
      body: {
        status: "error",
        errorMessage: "Error occurred while invoking GW vendor search api",
        errorDescription: error,
        transactionId: context.awsRequestId,
      },
    };
  }
};

// Get OAuth Token (cached, updated in secrets manager if not present)
const getOAuthToken = async (context) => {
  const cachedSecret = cache.get(process.env.TOKEN_SECRET_NAME);
  console.log("cachedSecret", cachedSecret);
  if (cachedSecret) {
    if (typeof cachedSecret === "string") {
      console.log("Token retrieved from cache", JSON.parse(cachedSecret).token);
      return JSON.parse(cachedSecret).token;
    }
    console.log("Token retrieved from cache", cachedSecret.token);
    return cachedSecret.token;
  }
  const generateTokenResult = await generateToken(context);
  console.log("generateTokenResult1", generateTokenResult);
  const secretValue = generateTokenResult;
  console.log("secretValue", secretValue);
  const newSecretValue = JSON.stringify({ token: secretValue });
  let attempt = 0;
  let success = false;
  while (attempt < maxRetries && !success) {
    try {
      const response = await secretsManager
        .putSecretValue({
          SecretId: process.env.TOKEN_SECRET_NAME,
          SecretString: newSecretValue,
        })
        .promise();
      console.log("response", response);
      cache.set(process.env.TOKEN_SECRET_NAME, newSecretValue);
      console.log("Token updated in Secrets Manager");
      success = true;
    } catch (error) {
      if (error.code === "ThrottlingException") {
        attempt++;
        console.log(
          `Throttling exception: Retry attempt ${attempt}/${maxRetries}`
        );
        await new Promise((resolve) =>
          setTimeout(resolve, retryDelay * attempt)
        );
      } else {
        console.error("Error updating secret:", error);
        break;
      }
    }
  }
  if (!success) {
    console.error("Failed to update secret after max retries");
  }
  return generateTokenResult;
};

// Generate OAuth token using client credentials
const generateToken = async (context) => {
  const tokenEndpoint = process.env.TOKEN_ENDPOINT;

  let parsedUrl;
  try {
    parsedUrl = new URL(tokenEndpoint);
    if (parsedUrl.protocol !== "https:") {
      throw new Error("Token endpoint must use HTTPS protocol");
    }
  } catch (error) {
    throw new Error(`Invalid token endpoint URL: ${error.message}`);
  }

  const secretDetails = await getSecretDetails(
    process.env.CLIENT_DETAILS_SECRET,
    context
  );
  const clientId = secretDetails.client_id;
  const clientSecret = secretDetails.client_secret;
  const postData = querystring.stringify({
    grant_type: "client_credentials",
    scope: process.env.SCOPE,
  });
  const base64Auth = Buffer.from(`${clientId}:${clientSecret}`).toString(
    "base64"
  );
  const options = {
    hostname: parsedUrl.hostname,
    port: 443,
    path: parsedUrl.pathname,
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: `Basic ${base64Auth}`,
      "Content-Length": postData.length,
    },
  };
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => {
        data += chunk;
      });
      res.on("end", () => {
        if (res.statusCode === 200) {
          const response = JSON.parse(data);
          resolve(response.access_token);
        } else {
          reject(
            new Error(`Failed to get OAuth token: ${res.statusCode} - ${data}`)
          );
        }
      });
    });
    req.on("error", (e) => {
      reject(e);
    });
    req.write(postData);
    req.end();
  });
};
