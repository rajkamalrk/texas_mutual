import mysql from 'mysql';
import AWS from 'aws-sdk';
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";

const sqs = new AWS.SQS();
const sns = new AWS.SNS();
const QUEUE_URL = process.env.QUEUE_URL;
const QUEUE_URL1 = process.env.QUEUE_URL1;
let env = process.env.ENVIRONMENT;
env = env.toUpperCase();
const tp = process.env.TRADING_PARTNER;
const topic_arn = process.env.TOPIC_ARN;

export const handler = async (event) => {

const secretDetails = await secretDetailsFetch();

    const dbHost = secretDetails.host;
    const dbUser = secretDetails.username;
    const dbPassword = secretDetails.password;

const dbConfig = {
    host: dbHost,
    user: dbUser,
    password: dbPassword,
    database: process.env.DB
};

const connection = mysql.createConnection(dbConfig);
console.log("event",event);

for (const record of event.Records) {
    console.log('record body', record.body);
    const body = JSON.parse(record.body);
    console.log('body', body);

    console.log('event', event);
    const kofaxBillData = body;
    console.log('kofaxBillData', kofaxBillData);
    console.log('connection', connection);
    const formType = kofaxBillData.formType;
    console.log('formType', formType);

    let query = '';
    let values = [];
    let lineItems = [];
    let lineItemsQuery = '';
    let documentItemsQuery = '';
    let documentItems = [];

    const now = new Date();
    const createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
    const updatedOn = createdOn;

    switch (formType) {
        case 'pharmacy':
            query = `
                INSERT INTO ${process.env.TABLE1} (
                    invoice_number, claim_number, receive_date, special_handling, provider_id, vendor_name, vendor_address_1, vendor_address_2, vendor_city, vendor_state, vendor_zip_code, vendor_federal_employer_id_number, invoice_date, facility_name, facility_address_1, facility_address_2, facility_city, facility_state, facility_zip_code, facility_npi, patient_first_name, patient_middle_name, patient_last_name, patient_social_security_number, patient_dob, patient_date_of_injury, prescribingdoctor_firstname, prescribingdoctor_lastname, prescribingdoctor_nationalproviderid, create_user, create_timestamp, update_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;
            lineItemsQuery = `
                INSERT INTO ${process.env.TABLE2} (
                    dwc_id, sequence_number, dispensed_aswritten_code, date_filled, generic_ndc, name_brand_ndc, quantity, days_supply, drugname_and_strength, prescription_number, amount_billed, create_user, create_timestamp, update_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;
            documentItemsQuery = `
            INSERT INTO ${process.env.TABLE7} (
                dwc_id, document_item_type, serial_number, create_user, update_user, create_timestamp, update_timestamp 
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            `;
            values = [
                kofaxBillData.invoiceNumber || '',
                kofaxBillData.claimNumber || '',
                kofaxBillData.receiveDate || null,
                kofaxBillData.specialHandling || '',
                kofaxBillData.providerId || '',
                kofaxBillData.vendor.name || '',
                kofaxBillData.vendor.address1 || '',
                kofaxBillData.vendor.address2 || '',
                kofaxBillData.vendor.city || '',
                kofaxBillData.vendor.state || '',
                kofaxBillData.vendor.zipCode || '',
                kofaxBillData.vendor.federalEmployerIdNumber || '',
                kofaxBillData.vendor.invoiceDate || null,
                kofaxBillData.facility.name || '',
                kofaxBillData.facility.address1 || '',
                kofaxBillData.facility.address2 || '',
                kofaxBillData.facility.city || '',
                kofaxBillData.facility.state || '',
                kofaxBillData.facility.zipCode || '',
                kofaxBillData.facility.nationalProviderId || '',
                kofaxBillData.patient.firstName || '',
                kofaxBillData.patient.middleName || '',
                kofaxBillData.patient.lastName || '',
                kofaxBillData.patient.socialSecurityNumber || '',
                kofaxBillData.patient.dateOfBirth || null,
                kofaxBillData.patient.dateOfInjury || null,
                kofaxBillData.prescribingDoctor.firstName || '',
                kofaxBillData.prescribingDoctor.lastName || '',
                kofaxBillData.prescribingDoctor.nationalProviderId || '',
                'LAMBDA',
                createdOn,
                updatedOn
            ];
            lineItems = kofaxBillData.lineItems.map(item => [
                item.sequenceNumber || '',
                item.dispensedAsWrittenCode || '',
                item.dateFilled || null,
                item.genericNationalDrugCode || '',
                item.nameBrandNationalDrugCode || '',
                item.quantity || '',
                item.daysSupply || '',
                item.drugNameAndStrength || '',
                item.prescriptionNumber || '',
                item.amountBilled || '',
                'LAMBDA',
                createdOn,
                updatedOn
            ]);
            documentItems = kofaxBillData.documents.map(item => [
                item.itemType || '',
                item.serialNumber || '',
                'LAMBDA',
                '',
                createdOn,
                updatedOn
            ]);
            break;
        case 'hospital':
            query = `
                INSERT INTO ${process.env.TABLE3} (
                    invoice_number, claim_number, receive_date, special_handling, type_of_bill, statement_from, statement_to, pps_code, provider_id, vendor_name, vendor_address_1, vendor_address_2, vendor_city, vendor_state, vendor_zip_code, vendor_federal_employer_id_number, vendor_npi, vendor_other_id, facility_name, facility_address_1, facility_address_2, facility_city, facility_state, facility_zip_code, patient_first_name, patient_middle_name, patient_last_name, patient_social_security_number, patient_dob, patient_date_of_injury, control_number, discharge_status, admission_date, admission_hour, admission_type, diagnosis_icd_indicator, diagnosis_principal_code, diagnosis_visit_reason_code, diagnosis_admitting_code, diagnosis_code_A, diagnosis_code_B, diagnosis_code_C, diagnosis_code_D, diagnosis_code_E, diagnosis_code_F, diagnosis_code_G, diagnosis_code_H, diagnosis_code_I, diagnosis_code_J, diagnosis_code_K, diagnosis_code_L, diagnosis_code_M, diagnosis_code_N, diagnosis_code_O, diagnosis_code_P, diagnosis_code_Q, procedures_principal_code, procedures_principal_date, procedures_other_code_a, procedures_other_date_a, procedures_other_code_b, procedures_other_date_b, procedures_other_code_c, procedures_other_date_c, procedures_other_code_d, procedures_other_date_d, procedures_other_code_e, procedures_other_date_e, attending_provider_first_name, attending_provider_last_name, attending_provider_national_provider_id, attending_provider_other_id, operating_provider_first_name, operating_provider_last_name, operating_provider_national_provider_id, operating_provider_other_id, total_covered_charges, create_user, create_timestamp, update_timestamp, creation_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;
            lineItemsQuery = `
                INSERT INTO ${process.env.TABLE4} (
                    ub_id, sequence_number, revenue_code, description, hcpcs_code, service_date, units_service, total_charge, modifier1, modifier2, modifier3, modifier4, create_user, create_timestamp, update_timestamp 
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;
            documentItemsQuery = `
            INSERT INTO ${process.env.TABLE8} (
                ub_id, document_item_type, serial_number, create_user, update_user, create_timestamp, update_timestamp 
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            `;
            values = [
                    kofaxBillData.invoiceNumber || '',
                    kofaxBillData.claimNumber || '',
                    kofaxBillData.receiveDate || null,
                    kofaxBillData.specialHandling || '',
                    kofaxBillData.typeOfBill || '',
                    kofaxBillData.statementFrom || '',
                    kofaxBillData.statementTo || '',
                    kofaxBillData.ppsCode || '',
                    kofaxBillData.providerId || '',
                    kofaxBillData.vendor.name || '',
                    kofaxBillData.vendor.address1 || '',
                    kofaxBillData.vendor.address2 || '',
                    kofaxBillData.vendor.city || '',
                    kofaxBillData.vendor.state || '',
                    kofaxBillData.vendor.zipCode || '',
                    kofaxBillData.vendor.federalEmployerIdNumber || '',
                    kofaxBillData.vendor.nationalProviderId || '',
                    kofaxBillData.vendor.otherId || '',
                    kofaxBillData.facility.name || '',
                    kofaxBillData.facility.address1 || '',
                    kofaxBillData.facility.address2 || '',
                    kofaxBillData.facility.city || '',
                    kofaxBillData.facility.state || '',
                    kofaxBillData.facility.zipCode || '',
                    kofaxBillData.patient.firstName || '',
                    kofaxBillData.patient.middleName || '',
                    kofaxBillData.patient.lastName || '',
                    kofaxBillData.patient.socialSecurityNumber || '',
                    kofaxBillData.patient.dateOfBirth || null,
                    kofaxBillData.patient.dateOfInjury || null,
                    kofaxBillData.patient.controlNumber || '',
                    kofaxBillData.patient.dischargeStatus || '',
                    kofaxBillData.admission.date || null,
                    kofaxBillData.admission.hour || '',
                    kofaxBillData.admission.type || '',
                    kofaxBillData.diagnosis.icdIndicator || '',
                    kofaxBillData.diagnosis.principalCode || '',
                    kofaxBillData.diagnosis.visitReasonCode || '',
                    kofaxBillData.diagnosis.admittingCode || '',
                    kofaxBillData.diagnosis.codeA || '',
                    kofaxBillData.diagnosis.codeB || '',
                    kofaxBillData.diagnosis.codeC || '',
                    kofaxBillData.diagnosis.codeD || '',
                    kofaxBillData.diagnosis.codeE || '',
                    kofaxBillData.diagnosis.codeF || '',
                    kofaxBillData.diagnosis.codeG || '',
                    kofaxBillData.diagnosis.codeH || '',
                    kofaxBillData.diagnosis.codeI || '',
                    kofaxBillData.diagnosis.codeJ || '',
                    kofaxBillData.diagnosis.codeK || '',
                    kofaxBillData.diagnosis.codeL || '',
                    kofaxBillData.diagnosis.codeM || '',
                    kofaxBillData.diagnosis.codeN || '',
                    kofaxBillData.diagnosis.codeO || '',
                    kofaxBillData.diagnosis.codeP || '',
                    kofaxBillData.diagnosis.codeQ || '',
                    kofaxBillData.procedures.principal.code || '',
                    kofaxBillData.procedures.principal.date || null,
                    kofaxBillData.procedures.other[0]?.code || null,
                    kofaxBillData.procedures.other[0]?.date || null,
                    kofaxBillData.procedures.other[1]?.code || null,
                    kofaxBillData.procedures.other[1]?.date || null,
                    kofaxBillData.procedures.other[2]?.code || null,
                    kofaxBillData.procedures.other[2]?.date || null,
                    kofaxBillData.procedures.other[3]?.code || null,
                    kofaxBillData.procedures.other[3]?.date || null,
                    kofaxBillData.procedures.other[4]?.code || null,
                    kofaxBillData.procedures.other[4]?.date || null,
                    kofaxBillData.attendingProvider.firstName || '',
                    kofaxBillData.attendingProvider.lastName || '',
                    kofaxBillData.attendingProvider.nationalProviderId || '',
                    kofaxBillData.attendingProvider.otherId || '',
                    kofaxBillData.operatingProvider.firstName || '',
                    kofaxBillData.operatingProvider.lastName || '',
                    kofaxBillData.operatingProvider.nationalProviderId || '',
                    kofaxBillData.operatingProvider.otherId || '',
                    kofaxBillData.totalCoveredCharges || '',
                    'LAMBDA',
                    createdOn,
                    updatedOn,
                    kofaxBillData.creationDate || null,
                ]

            lineItems = kofaxBillData.lineItems.map(item => [
                item.sequenceNumber || '',
                item.revenueCode || '',
                item.description || '',
                item.hcpcsOrRateOrHippsCode || '',
                item.serviceDate || null,
                item.unitsOfService || '',
                item.totalCharge || '',
                item.modifier1 || '',
                item.modifier2 || '',
                item.modifier3 || '',
                item.modifier4 || '',
                'LAMBDA',
                createdOn,
                updatedOn
            ]);

            documentItems = kofaxBillData.documents.map(item => [
                item.itemType || '',
                item.serialNumber || '',
                'LAMBDA',
                '',
                createdOn,
                updatedOn
            ]);
            break;
        case 'professional':
            query = `
                INSERT INTO ${process.env.TABLE5} (
                    invoice_number, claim_number, receive_date, special_handling, provider_id, vendor_name, vendor_address_1, vendor_address_2, vendor_city, vendor_state, vendor_zip_code, vendor_federal_employer_id_number, vendor_npi, vendor_other_id, facility_name, facility_address_1, facility_address_2, facility_city, facility_state, facility_zip_code, facility_npi, facility_other_id, patient_first_name, patient_middle_name, patient_last_name, patient_social_security_number, patient_dob, patient_date_of_injury, patient_account_number, referring_provider_fullname, referring_provider_other_id, referrring_provider_npi, signing_provider_full_name, signing_provider_date, diagnosis_icd_indicator, diagnosis_code_A, diagnosis_code_B, diagnosis_code_C, diagnosis_code_D, diagnosis_code_E, diagnosis_code_F, diagnosis_code_G, diagnosis_code_H, diagnosis_code_I, diagnosis_code_J, diagnosis_code_K, diagnosis_code_L, total_charge, create_user, create_timestamp, update_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;
            lineItemsQuery = `
                INSERT INTO ${process.env.TABLE6} (
                    cms_id, sequence_number, date_from, date_to, place_of_service_code, cptor_hcpcs, modifier_1, modifier_2, modifier_3, modifier_4, diagnosis_pointer_1, diagnosis_pointer_2, diagnosis_pointer_3, diagnosis_pointer_4, charges, days_or_units, national_provider_id, other_id, create_user, create_timestamp, update_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;
            documentItemsQuery = `
            INSERT INTO ${process.env.TABLE9} (
                cms_id, document_item_type, serial_number, create_user, update_user, create_timestamp, update_timestamp 
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            `;
            values = [
                    kofaxBillData.invoiceNumber || '',
                    kofaxBillData.claimNumber || '',
                    kofaxBillData.receiveDate || null,
                    kofaxBillData.specialHandling || '',
                    kofaxBillData.providerId || '',
                    kofaxBillData.vendor.name || '',
                    kofaxBillData.vendor.address1 || '',
                    kofaxBillData.vendor.address2 || '',
                    kofaxBillData.vendor.city || '',
                    kofaxBillData.vendor.state || '',
                    kofaxBillData.vendor.zipCode || '',
                    kofaxBillData.vendor.federalEmployerIdNumber || '',
                    kofaxBillData.vendor.nationalProviderId || '',
                    kofaxBillData.vendor.otherId || '',
                    kofaxBillData.facility.name || '',
                    kofaxBillData.facility.address1 || '',
                    kofaxBillData.facility.address2 || '',
                    kofaxBillData.facility.city || '',
                    kofaxBillData.facility.state || '',
                    kofaxBillData.facility.zipCode || '',
                    kofaxBillData.facility.nationalProviderId || '',
                    kofaxBillData.facility.otherId || '',
                    kofaxBillData.patient.firstName || '',
                    kofaxBillData.patient.middleName || '',
                    kofaxBillData.patient.lastName || '',
                    kofaxBillData.patient.socialSecurityNumber || '',
                    kofaxBillData.patient.dateOfBirth || null,
                    kofaxBillData.patient.dateOfInjury || null,
                    kofaxBillData.patient.accountNumber || '',
                    kofaxBillData.referringProvider.fullName || '',
                    kofaxBillData.referringProvider.otherId || '',
                    kofaxBillData.referringProvider.nationalProviderId || '',
                    kofaxBillData.signingProvider.fullName || '',
                    kofaxBillData.signingProvider.date || null,
                    kofaxBillData.diagnosis.icdIndicator || '',
                    kofaxBillData.diagnosis.codeA || '',
                    kofaxBillData.diagnosis.codeB || '',
                    kofaxBillData.diagnosis.codeC || '',
                    kofaxBillData.diagnosis.codeD || '',
                    kofaxBillData.diagnosis.codeE || '',
                    kofaxBillData.diagnosis.codeF || '',
                    kofaxBillData.diagnosis.codeG || '',
                    kofaxBillData.diagnosis.codeH || '',
                    kofaxBillData.diagnosis.codeI || '',
                    kofaxBillData.diagnosis.codeJ || '',
                    kofaxBillData.diagnosis.codeK || '',
                    kofaxBillData.diagnosis.codeL || '',
                    kofaxBillData.totalCharge || '',
                    'LAMBDA',
                    createdOn,
                    updatedOn
];

            lineItems = kofaxBillData.lineItems.map(item => [
                item.sequenceNumber || '',
                item.dateFrom || null,
                item.dateTo || null,
                item.placeOfServiceCode || '',
                item.cptOrHcpcs || '',
                item.modifier1 || '',
                item.modifier2 || '',
                item.modifier3 || '',
                item.modifier4 || '',
                item.diagnosisPointer1 || '',
                item.diagnosisPointer2 || '',
                item.diagnosisPointer3 || '',
                item.diagnosisPointer4 || '',
                item.charges || '',
                item.daysOrUnits || '',
                item.nationalProviderId || '',
                item.otherId || '',
                'LAMBDA',
                createdOn,
                updatedOn
            ]);

            documentItems = kofaxBillData.documents.map(item => [
                item.itemType || '',
                item.serialNumber || '',
                'LAMBDA',
                '',
                createdOn,
                updatedOn
            ]);
            break;
        default:
            const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
            const subject = `${env} - ${tp} - Invalid Document Type - FATAL`;
            const message = `Process: ${tp} - Extract ebill data to staging tables \n Error: Unsupported Document ItemType  \n Timestamp: ${ts} \n TMI Invoice Number: ${kofaxBillData.invoiceNumber? kofaxBillData.invoiceNumber: ''} \n Claim Number: ${kofaxBillData.claimNumber? kofaxBillData.claimNumber: ''} ` ;
            await sendEmail(subject, message);
            throw new Error('Unsupported Document ItemType');
    }
    console.log('values', values);
    console.log('lineItems', lineItems);
    console.log('documentItems', documentItems);
 
     return new Promise((resolve, reject) => {
        connection.beginTransaction(async err => {
            if (err) {
                console.error(err);
                const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
                const subject = `${env} - ${tp} - Failed to start transaction - FATAL`;
                const message = `Process: ${tp} - Extract ebill data to staging tables \n Error: ${err} \n Timestamp: ${ts} \n TMI Invoice Number: ${kofaxBillData.invoiceNumber? kofaxBillData.invoiceNumber: ''} \n Claim Number: ${kofaxBillData.claimNumber? kofaxBillData.claimNumber: ''} ` ;
                await sendEmail(subject, message);
                return reject(new Error('Failed to start transaction'));
            }
            connection.query(query, values, async (error, results) => {
                if (error) {
                    console.error(error);
                    const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
                    const subject = `${env} - ${tp} - Failed to insert kofax bill data - FATAL`;
                    const message = `Process: ${tp} - Extract ebill data to staging tables \n Error: ${error} \n Timestamp: ${ts} \n TMI Invoice Number: ${kofaxBillData.invoiceNumber? kofaxBillData.invoiceNumber: ''} \n Claim Number: ${kofaxBillData.claimNumber? kofaxBillData.claimNumber: ''} ` ;
                    await sendEmail(subject, message);
                    return connection.rollback(() => {
                        reject(new Error('Failed to insert kofax bill data'));
                    });
                }
                const foreignKeyValue = results.insertId;
                const lineItemPromises = lineItems.map(lineItemValues => {
                    return new Promise((resolve, reject) => {
                        connection.query(lineItemsQuery, [foreignKeyValue, ...lineItemValues], (error, results) => {
                            if (error) {
                                return reject(error);
                            }
                            resolve(results);
                        });
                    });
                });

                const documentItemPromises = documentItems.map(itemValues => {
                    return new Promise((resolve, reject) => {
                        connection.query(documentItemsQuery, [foreignKeyValue, ...itemValues], (error, results) => {
                            if (error) {
                                return reject(error);
                            }
                            resolve(results);
                        });
                    });
                });

                Promise.all([...lineItemPromises, ...documentItemPromises])
                    .then(results => {
                        connection.commit(async err => {
                            if (err) {
                                const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
                                const subject = `${env} - ${tp} - Failed to commit transaction- FATAL`;
                                const message = `Process: ${tp} - Extract ebill data to staging tables \n Error: ${err} \n Timestamp: ${ts} \n TMI Invoice Number: ${kofaxBillData.invoiceNumber? kofaxBillData.invoiceNumber: ''} \n Claim Number: ${kofaxBillData.claimNumber? kofaxBillData.claimNumber: ''} ` ;
                                await sendEmail(subject, message);
                                return connection.rollback(() => {
                                    reject(new Error('Failed to commit transaction'));
                                    console.log('Failed to commit transaction');
                                });
                            }
                            const deleteParams = {
                                  QueueUrl: QUEUE_URL,
                                  ReceiptHandle: record.receiptHandle
                            };
                            sqs.deleteMessage(deleteParams).promise()
                                .then(() => {
                                    console.log('Deleted message from SQS');
                                })
                                .catch((error) => {
                                    console.error('Error deleting message from SQS:', error);
                                });
                            console.log('Kofax bill data and line items inserted successfully');
                            resolve({
                                statusCode: 200,
                                body: JSON.stringify({
                                    message: 'Kofax bill data and line items inserted successfully',
                                    results
                                })
                            });
                        });
                    })
                    .catch(async error => {
                         console.log('error', error);
                         const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
                         const subject = `${env} - ${tp} - Failed to insert data - FATAL`;
                         const message = `Process: ${tp} - Extract ebill data to staging tables \n Error: ${error} \n Timestamp: ${ts} \n TMI Invoice Number: ${kofaxBillData.invoiceNumber? kofaxBillData.invoiceNumber: ''} \n Claim Number: ${kofaxBillData.claimNumber? kofaxBillData.claimNumber: ''} ` ;
                         await sendEmail(subject, message);

                        const deleteParams2 = {
                            QueueUrl: QUEUE_URL,
                            ReceiptHandle: record.receiptHandle
                            };
                        sqs.deleteMessage(deleteParams2).promise()
                            .then(() => {
                                console.log('Deleted message from SQS2');
                            })
                            .catch((error) => {
                                console.error('Error deleting message from SQS2:', error);
                            });
                        const sqsDlqParams = {
                            QueueUrl: QUEUE_URL1,
                            MessageBody: JSON.stringify(body),
                            };
                            console.log('sqsDlqParams',sqsDlqParams)
                        sqs.sendMessage(sqsDlqParams).promise()
                            .then(() => {
                                console.log('Pushed message to DLQ_SQS');
                            })
                            .catch((error) => {
                                console.error('Error pushing message to DLQ_SQS:', error);
                            });
                        console.log('message sent to DLQ_SQS');
                        connection.rollback(() => {
                            reject(new Error('Failed to insert line items'));
                            console.log('Failed to insert line items')
                        });
                    });
            });
        });
    });
}
};


async function secretDetailsFetch() {
    console.log('In secretDetailsFetch');
    const secretName = process.env.SECRET_NAME;
    const client = new SecretsManagerClient({ region: process.env.REGION });
  
    try {
      const response = await client.send(
        new GetSecretValueCommand({
          SecretId: secretName,
          VersionStage: "AWSCURRENT",
        })
      );
      const secret = JSON.parse(response.SecretString);
      return secret;
    } catch (error) {
      console.error('Error in secretDetailsFetch:', error);
      const ts = new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
      const subject = `${env} - ${tp} - Failed to insert data - FATAL`;
      const message = `Process: ${tp} - Extract ebill data to staging tables \n Error: ${error} \n Timestamp: ${ts}` ;
      await sendEmail(subject, message);
      throw error; 
    }
  }
  
async function sendEmail(subject, message) {
    const params = {
        Subject: subject,
        Message: message, 
        TopicArn: `${topic_arn}`
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