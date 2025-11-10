import AWS from 'aws-sdk';
const glue = new AWS.Glue();

async function checkAndStartJob(params, context) {
    const runningJobs = await glue.getJobRuns(params).promise();
    const runningJobCount = runningJobs.JobRuns.filter(job => job.JobRunState === 'RUNNING').length;
    if (runningJobCount > 0) {
        console.log(`There are ${runningJobCount} concurrent runs for the job ${params.JobName}`);
        return {
            statusCode: 500,
            body: JSON.stringify({
                status: 'error',
                errorMessage: 'Concurrent runs exceeded, please try again later.',
                errorDescription: 'Concurrent runs exceeded, please try again later.',
                transactionId: context.awsRequestId,
            }),
        };
    } else {
        const result = await glue.startJobRun(params).promise();
        console.log("Glue job Started Successfully: ", result.JobRunId);
        return {
            statusCode: 200,
            body: JSON.stringify({
                status: 'success',
                message: 'Glue job triggered',
                data:{
                  jobRunId: result.JobRunId  
                },
                transactionId: context.awsRequestId
            }),
        };
    }
}

export const handler = async (event, context) => {
    try {
        let jobName;
        if (JSON.parse(event.body).feed === 'Provider') {  
            jobName = process.env.PROVIDER;
        } else if (JSON.parse(event.body).feed === 'Bill') {
            jobName = process.env.BILL;
        } else if (JSON.parse(event.body).feed === 'Jopari Reject') {
            jobName = process.env.JOPARI_REJECT;
            
        }
         else {
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
        };

        return await checkAndStartJob(params, context);
    } catch (error) {
        console.log("Error in triggering glue job: ", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                status: 'error',
                errorMessage: 'Failed to trigger glue job',
                errorDescription: 'Failed to trigger glue job',
                transactionId: context.awsRequestId,
            }),
        };
    }
}