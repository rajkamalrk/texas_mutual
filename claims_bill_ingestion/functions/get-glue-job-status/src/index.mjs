import AWS from 'aws-sdk';
const glue = new AWS.Glue();

export const handler = async (event, context) => {
    try {
        let jobName;
        
        console.log('event', event);
        if (JSON.parse(event.body).feed === 'Provider') {
            jobName = process.env.PROVIDER;
        } else if (JSON.parse(event.body).feed === 'Bill') {
            jobName = process.env.BILL;
        } else if (JSON.parse(event.body).feed === 'Jopari Reject') {
            jobName = process.env.JOPARI_REJECT;
        } else {
            return {
            statusCode: 400,
            body: JSON.stringify({
                status: 'error',
                errorMessage: 'Invalid feed',
                errorDescription: 'Invalid feed',
                transactionId: context.awsRequestId,
            }),
        };
        }
        
        const params = {
            JobName: jobName,
            RunId: JSON.parse(event.body).jobRunId
        };
        console.log('params', params);
        
        const result = await glue.getJobRun(params).promise();
        return {
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Glue Job status fetched successfully',
                data: {
                  jobStatus: result.JobRun.JobRunState  
                },
                transactionId: context.awsRequestId,
            }),
        };
    } catch (error) {
        console.log("Error fetching Glue Job status: ", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                status: 'error',
                errorMessage: 'Failed to fetch glue job status',
                errorDescription: 'Failed to fetch glue job status',
                transactionId: context.awsRequestId,
            }),
        };
    }
};