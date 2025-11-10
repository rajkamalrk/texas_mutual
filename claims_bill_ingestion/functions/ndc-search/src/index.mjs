import { initDbPool } from './db-config.mjs'; 
 
let connection;
 
const setupConnection = async () => {
    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
    }
};
 
// Lambda function handler
export const handler = async (event, context) => {
  try {
    const queryStringParameters = event.queryStringParameters;
    const ndcCode = queryStringParameters.ndc;
    const dateOfService = queryStringParameters.dateOfService;
    if (!ndcCode || !dateOfService) {
        return {
            statusCode: 400, 
            body: JSON.stringify({
                status: 'error',
                errorMessage: "Invalid query parameters",
                errorDescription: "Invalid query parameters send correct parameters",
                transactionId: context.awsRequestId
            }),
        };
    }
 
    await setupConnection();
    const query = `
      SELECT * FROM ${process.env.DB_1}.${process.env.TABLE_1} WHERE ndc_code = ${ndcCode}
    `;
 
    console.log('query', query);
 
    const [table1Records] = await connection.query(query);
    console.log('table1Records', table1Records);
    let nameDerived;
    if (table1Records.length === 0){
        return {
            statusCode: 404, // 
            body: JSON.stringify({
                status: 'error',
                message: `Code ${ndcCode} not found`,
                data: null,
                transactionId: context.awsRequestId
            }),
        };
    }

    if (table1Records[0].ndc_name && table1Records[0].ndc_name_extension){
        nameDerived = table1Records[0].ndc_name + table1Records[0].ndc_name_extension;
    }
    else{
        nameDerived = table1Records[0].ndc_name;
    }

    let statusDerived;
    let formatedDate;
    if (/^\d{8}$/.test(dateOfService)) {
        formatedDate = `${dateOfService.slice(0, 4)}-${dateOfService.slice(4, 6)}-${dateOfService.slice(6)}`;
    } else if (/^\d{4}-\d{2}-\d{2}$/.test(dateOfService)) {
        formatedDate = dateOfService;
    }
    console.log('formatedDate', formatedDate);
    console.log('table1Records[0].ndc_item_status', table1Records[0].ndc_item_status);
    
    const isoDate = table1Records[0].ndc_status_change_date;
    // ndc_status_change_date can be null
    let formattedDate2 = null;
    if (isoDate) {
      formattedDate2 = new Date(isoDate).toISOString().split('T')[0];
    }

    statusDerived = table1Records[0].ndc_item_status;
    statusDerived === 'A' ? statusDerived = 'Active' : statusDerived = 'Inactive';

    // NEW LOGIC for Valid flag based on new requirements
    let validStatus = false;
    let validStatusTo = null;

    if (!formattedDate2) {
      // Scenario 1: NDC Status Change Date = null
      if (statusDerived === 'Active') {
        validStatus = true;
      } else {
        validStatus = false;
      }
    } else if (formatedDate === formattedDate2) {
      // Scenario 2: DOS == NDC Status Change Date
      if (statusDerived === 'Active') {
        validStatus = true;
      } else {
        validStatus = false;
      }
    } else {
      // Scenario 3: Based on DOS and NDC Status Change Date
      if (statusDerived === 'Active') {
        if (formatedDate < formattedDate2) {
          validStatus = false;
        } else if (formatedDate > formattedDate2) {
          validStatus = true;
        }
      } else { // statusDerived === 'Inactive'
        if (formatedDate < formattedDate2) {
          validStatus = true;
        } else if (formatedDate > formattedDate2) {
          validStatus = false;
        }
      }
    }

    // Set validStatusTo only if status is not Active and NDC status change date exists
    if (statusDerived !== 'Active' && formattedDate2) {
      validStatusTo = formattedDate2;
    }

    let data = {
        ndc: table1Records[0].ndc_code,
        name: nameDerived,
        status: statusDerived,
        strength: table1Records[0].ndc_strength,
        unitOfMeasure: table1Records[0].ndc_unit_of_measure,
        valid: validStatus,
        validTo: validStatusTo
    }
 
    console.log('data', data);
    let responseRecord;
    if (table1Records.length > 0){
        responseRecord = `Total ${table1Records.length} record found`;
    }
    else{
        responseRecord = 'No records found for given search criteria';
    }
    return {
        statusCode: 200,
        body: JSON.stringify({
            status: 'success',
            message: responseRecord,
            data: data,
            transactionId: context.awsRequestId
        }),
    };
   
  } catch (error) {
    console.error('Error: ', error);
        return {
            statusCode: 500, // Internal server error
            body: JSON.stringify({
                status: 'error',
                errorMessage: 'Internal server error',
                errorDescription: error.message,
                transactionId: context.awsRequestId
            }),
        };
    }
};
