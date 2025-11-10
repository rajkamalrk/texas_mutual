export const handler = async (event, context) => {
  console.log('event', event);
  const httpMethod = event.httpMethod;
  let resource = event.path;
  console.log('httpMethod', httpMethod);
  
  const parts = resource.split('/');
  console.log('parts', parts);
  resource = parts[1];
  console.log('resource', resource);
  resource = `/${resource}`;

  
  // Check if the resource exists
  if (!resourceExists(resource)) {
      return {
          statusCode: 404,
          body: JSON.stringify({
          status: "error",
          errorMessage: 'Resource not found',
          errorDescription: 'Resource not found',
          transactionId: context.awsRequestId,
          })
      };
  }
  
  // Check if the method is allowed
  if (!methodAllowed(resource, httpMethod)) {
      return {
          statusCode: 405,
          body: JSON.stringify({
          status: "error",
          errorMessage: 'Method not allowed',
          errorDescription: 'Method not allowed',
          transactionId: context.awsRequestId,
          })
      };
  }
  
  // Handle valid requests (this is just a placeholder)
  return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Success' })
  };
};

const resourceExists = (resource) => {
  console.log('In resourceExists', resource);
  // Implement logic to check if the resource exists
  const validResources = ['/invoices/generate-number','/invoices','/providers','/invoke-glue-job', '/get-glue-job-status'];
  return validResources.includes(resource);
};

const methodAllowed = (resource, method) => {
  console.log('In methodAllowed', resource, method);
  // Implement logic to check if the method is allowed for the resource
  const validMethods = {
      '/invoices/generate-number': ['GET'],
      '/invoices/': ['PUT'],
      '/providers/': ['GET'],
      '/providers/': ['POST'],
      '/invoke-glue-job': ['POST'],
      '/get-glue-job-status': ['POST'],
  };
  return (validMethods[resource] || []).includes(method);
};
