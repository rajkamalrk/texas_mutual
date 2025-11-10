import { initDbPool } from './db-config.mjs';

let connection;

const setupConnection = async () => {
    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
    }
};

export const handler = async (event, context) => {
  // TODO implement
    try{
        await setupConnection();
        const allowedParams = ['providerNumber', 'billItemType', 'renderingProviderLicense', 'renderingProviderNpi', 'vendorTaxId', 'vendorNumber', 'facilityName', 'facilityAddress', 'facilityCity', 'facilityState', 'facilityZipCode', 'facilityNpi', 'renderingProviderName', 'dateCreatedFrom', 'dateCreatedTo', 'dateUpdatedFrom', 'dateUpdatedTo']; // List of allowed query parameters
        const queryStringParameters = event.queryStringParameters; // Extract query string parameters
        console.log('queryStringParameters', queryStringParameters);
        if (queryStringParameters == null){
            return {
                statusCode: 400, // Bad Request
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: "Invalid query string parameters",
                    errorDescription: "Please send atleast 1 or more query parameters",
                    transactionId: context.awsRequestId
                }),
            };
        }
        const providerNumber = queryStringParameters.providerNumber;
        const billItemType = queryStringParameters.billItemType;
        const renderingProviderLicense = queryStringParameters.renderingProviderLicense;
        const renderingProviderNpi = queryStringParameters.renderingProviderNpi;
        const vendorTaxId = queryStringParameters.vendorTaxId;
        const vendorNumber = queryStringParameters.vendorNumber;
        const facilityName = queryStringParameters.facilityName;
        const facilityAddress = queryStringParameters.facilityAddress;
        const facilityCity = queryStringParameters.facilityCity;
        const facilityState = queryStringParameters.facilityState;
        const facilityZipCode = queryStringParameters.facilityZipCode;
        const facilityNpi = queryStringParameters.facilityNpi;
        const renderingProviderName = queryStringParameters.renderingProviderName;
    
        console.log('facilityZipCode', facilityZipCode);
        if ((facilityZipCode && facilityZipCode.length < 5) || (facilityZipCode && facilityZipCode.length > 9)){
            return {
                statusCode: 400, // Bad Request
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: "Invalid query string parameter",
                    errorDescription: "Please send the facilityZipCode with at least 5 characters and not more than 9 characters.",
                    transactionId: context.awsRequestId
                }),
            };
            
        }
        // Function to check if all query parameters are allowed
        const validateQueryParams = (params) => {
            for (const key in params) {
                if (!allowedParams.includes(key)) {
                    return false; // Found an unauthorized parameter
                }
            }
            return true; // All parameters are allowed
        };
    
        // Validate the query string parameters
        if (!validateQueryParams(queryStringParameters)) {
            return {
                statusCode: 400, // Bad Request
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: "Invalid query string parameters",
                    errorDescription: "Please send correct query parameter",
                    transactionId: context.awsRequestId
                }),
            };
        }
    
        if (vendorTaxId){
            if (/^\d{9}$/.test(vendorTaxId)) {
            console.log('Valid vendorTaxId')
            } else {
                return {
                    statusCode: 400, // Bad Request
                    body: JSON.stringify({
                        status: 'error',
                        errorMessage: "Invalid query string parameters",
                        errorDescription: "Invalid vendorTaxId. It must be a numeric value with a length of 9.",
                        transactionId: context.awsRequestId
                    }),
                };
            }
        }
        
        if (renderingProviderNpi){
            if (/^\d{10}$/.test(renderingProviderNpi)) {
            console.log('Valid renderingProviderNpi')
            } else {
                return {
                    statusCode: 400, // Bad Request
                    body: JSON.stringify({
                        status: 'error',
                        errorMessage: "Invalid query string parameters",
                        errorDescription: "Invalid renderingProviderNpi. It must be a numeric value with a length of 10.",
                        transactionId: context.awsRequestId
                    }),
                };
            }
        }
        
        if (facilityNpi){
            if (/^\d{10}$/.test(facilityNpi)) {
            console.log('Valid facilityNpi')
            } else {
                return {
                    statusCode: 400, // Bad Request
                    body: JSON.stringify({
                        status: 'error',
                        errorMessage: "Invalid query string parameters",
                        errorDescription: "Invalid facilityNpi. It must be a numeric value with a length of 10.",
                        transactionId: context.awsRequestId
                    }),
                };
            }
        }
        

        // If validation passes, proceed with your logic
        // You can process query parameters here if needed
        
        const paramToFieldMap = {
            providerNumber: 'provider_number',
            billItemType: 'bill_item_type',
            renderingProviderLicense: 'rendering_provider_license',
            renderingProviderNpi: 'rendering_provider_npi',
            vendorTaxId: 'vendor_tax_id',
            vendorNumber: 'vendor_number',
            facilityName: 'facility_name',
            facilityAddress: 'facility_address_1',
            facilityCity: 'facility_city',
            facilityState: 'facility_state',
            facilityZipCode: 'facility_zip_code',
            facilityNpi: 'facility_npi',
            renderingProviderName: 'rendering_provider_name'
        };
    
        let query = `SELECT * FROM ${process.env.DB_1}.${process.env.TABLE_1} WHERE 1=1`; // Base query
        const queryValues = [];
    
        for (const [param, value] of Object.entries(queryStringParameters)) {
            const field = paramToFieldMap[param];
            if (field) {
                console.log('field', field);
                
                // Corrected condition checks
                if (field === 'facility_zip_code' || 
                    field === 'facility_address_1' || 
                    field === 'facility_name' || 
                    field === 'facility_city') {
                    
                    query += ` AND LOWER(${field}) LIKE ?`; // Append conditions dynamically
                    queryValues.push(`${value}%`);
                    
                } else if (field === 'rendering_provider_name') {
                    query += ` AND LOWER(${field}) LIKE ?`; // Append conditions dynamically
                    queryValues.push(`%${value}%`);
                    
                } else {
                    query += ` AND ${field} = ?`; // Append conditions dynamically
                    queryValues.push(value);
                }
            }

        }
        query +=  ` AND (vendor_status = 'Active'`
        query +=  ` OR vendor_status = ''`
        query +=  ` OR vendor_status is null)`

        const dateCreatedFrom = queryStringParameters.dateCreatedFrom;
        const dateCreatedTo = queryStringParameters.dateCreatedTo;
        const dateUpdatedFrom = queryStringParameters.dateUpdatedFrom;
        const dateUpdatedTo = queryStringParameters.dateUpdatedTo;

        if (dateCreatedFrom && dateCreatedTo){
            query += ` AND create_timestamp BETWEEN ? AND ?`;
            queryValues.push(dateCreatedFrom, dateCreatedTo);
        }
        if (dateUpdatedFrom && dateUpdatedTo){
            query += ` AND update_timestamp BETWEEN ? AND ?`;
            queryValues.push(dateUpdatedFrom, dateUpdatedTo);
        }
        if (dateCreatedFrom && !dateCreatedTo){
            return {
                statusCode: 400, // Bad Request
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: "dateCreatedTo is required",
                    errorDescription: "dateCreatedTo is required",
                    transactionId: context.awsRequestId
                }),
            };
        }
        if (!dateCreatedFrom && dateCreatedTo){
            return {
                statusCode: 400, // Bad Request
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: "dateCreatedFrom is required",
                    errorDescription: "dateCreatedFrom is required",
                    transactionId: context.awsRequestId
                }),
            };
        }
        if (dateUpdatedFrom && !dateUpdatedTo){
            return {
                statusCode: 400, // Bad Request
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: "dateUpdatedTo is required",
                    errorDescription: "dateUpdatedTo is required",
                    transactionId: context.awsRequestId
                }),
            };
        }
        if (!dateUpdatedFrom && dateUpdatedTo){
            return {
                statusCode: 400, // Bad Request
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: "dateUpdatedFrom is required",
                    errorDescription: "dateUpdatedFrom is required",
                    transactionId: context.awsRequestId
                }),
            };
        }

        query +=  ` ORDER BY update_timestamp DESC`
        
        console.log('query', query);
        console.log('queryValues', queryValues);
        console.log('queryValues[0]', queryValues[0])
        
        if (queryValues == '%%'){
             return {
                statusCode: 200,
                body: JSON.stringify({
                    status: 'success',
                    message: 'No records found for given search criteria',
                    data: [],
                    transactionId: context.awsRequestId
                }),
            };
            
        }
        let [result] = await connection.query(query, queryValues);
        console.log('result', result);
        let response = [];
        let recordValue = result.length;;
        // if (result.length >= 25){
        //     recordValue = 25;
        // }
        // else if(result.length < 25){
        //     recordValue = result.length;
        // }
        for (let i = 0; i < recordValue; i++){
            console.log(`result[${i}]`, result[i]);
            let resultRecord = {
                "providerNumber": result[i].provider_number,
                "providerJurisdiction": result[i].rendering_provider_jurisdiction,
                "providerEntityType": result[i].provider_entity_type ? result[i].provider_entity_type.toString() : '',
                "providerTypeCode": result[i].provider_type_code ? result[i].provider_type_code.toString() : '',
                    "lastModifiedDate": result[i].update_timestamp,
                    "createdDate": result[i].create_timestamp,
                    "renderingProvider":{
                    "license": result[i].rendering_provider_license,
                    "npi": result[i].rendering_provider_npi,
                    "name": result[i].rendering_provider_name
                    },
                "facility": {
                    "name": result[i].facility_name,
                    "npi": result[i].facility_npi,
                    "address": {
                    "line1": result[i].facility_address_1,
                    "line2": result[i].facility_address_2 ? result[i].facility_address_2.toString() : '',
                    "city": result[i].facility_city,
                    "state": result[i].facility_state,
                    "zipCode": result[i].facility_zip_code
                    }
                },
                "vendor": {
                    "taxID": result[i].vendor_tax_id ? result[i].vendor_tax_id.toString() : '',
                    "number": result[i].vendor_number,
                    "name": result[i].vendor_name,
                    "additionalName": "",
                    "address":{
                    "line1": result[i].vendor_address1,
                    "line2": result[i].vendor_address2,
                    "city": result[i].vendor_city,
                    "state": result[i].vendor_state,
                    "zipCode": result[i].vendor_zip_code
                    }
            
                }
            }
        response.push(resultRecord);
        }
        let responseRecord = '';
        if (result.length == 1){
            responseRecord = `Total ${result.length} record found out of ${recordValue} record is displayed.`
        }else if(result.length == 0){
            responseRecord = 'No records found for given search criteria'
        }else if(result.length > 1){
            responseRecord = `Total ${result.length} records found out of ${recordValue} records are displayed.`
        }
        return {
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: responseRecord,
                data: response,
                transactionId: context.awsRequestId
            }),
        };
    }catch(error){
        console.log('Error occured', error);
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