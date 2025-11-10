
import AWS from 'aws-sdk';
const lambda = new AWS.Lambda();

export const handler = async (event, context) => {

  try {
    const queryStringParameters = event.queryStringParameters;
    const licenseNumber = queryStringParameters.licenseNumber;
    ;
    console.log('licenseNumber', licenseNumber);

    
    let lambdaInvokeResponse = await invokeLambda(licenseNumber, context);
    console.log('lambdaInvokeResponse', lambdaInvokeResponse);
    let payload = JSON.parse(lambdaInvokeResponse.Payload);
    console.log('payload', payload);

    let statusCode = 200;
    let status = 'SUCCESS';
    let message;
    let data = {
      licenseNumber: licenseNumber,
      valid: true,
    }
    if (payload && payload.validation_result === "FAIL") {
      statusCode = 400;
      status = 'FAILURE';
      message = payload.failure_reason;
      data = {
        licenseNumber: licenseNumber,
        valid: false
      }
    }
    else{
      message = payload.success_reason;
    }
    return {
      statusCode: statusCode,
      body: JSON.stringify({
          status: status,
          message: message,
          data: data,
          transactionId: context.awsRequestId
      }),
    };
    

    } catch (error) {
        console.log('Error occurred', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                status: 'error',
                errorMessage: 'Internal server error',
                errorDescription: error.message,
                transactionId: context.awsRequestId
            }),
        };
    }
};

const invokeLambda = async (licenseNumber, context) => {
  let data = { "license_number": licenseNumber }
  const params = {
    FunctionName: process.env.INVOKE_LAMBDA,
    Payload: JSON.stringify(data)
  };
  
  console.log('data', data);
  console.log('params', params);

  try {
    return await lambda.invoke(params).promise();;
  } catch (e) {
    console.log('invokeLambda :: Error: ' + e);
    return{
        statusCode: 500,
        body: JSON.stringify({
            status: 'error',
            errorMessage: 'invokeLambda :: Error',
            errorDescription: e,
            transactionId: context.awsRequestId
        }),
    }
  }
};