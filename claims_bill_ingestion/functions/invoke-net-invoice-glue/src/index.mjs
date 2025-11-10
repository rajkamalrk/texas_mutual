import AWS from 'aws-sdk';
const s3 = new AWS.S3();
const glue = new AWS.Glue();
const sqs = new AWS.SQS();
const QUEUE_URL = process.env.QUEUE_URL;

export const handler = async (event) => {
    
    console.log('In Invoke-Glue-Job Lambda Event', event);

    // // Process each SQS message
    for (const record of event.Records) {
        const body = JSON.parse(record.body);
        const s3Event = body.Records[0].s3;
        const bucket = s3Event.bucket.name;
        const key = decodeURIComponent(s3Event.object.key.replace(/\+/g, ' '));
        console.log(`Processing file from S3 bucket: ${bucket}, key: ${key}`);

        let trading_partner = key.split('/');
        trading_partner = trading_partner[3];

        let folder = key.split('/');
        folder = folder[4];
        console.log('folder', folder);

        if (folder === "invoice_files"){
        
            // Trigger the Glue job
            const params = {
                JobName: process.env.GLUE_JOB,
                Arguments: {
                    '--s3_bucket': process.env.S3_BUCKET,
                    '--s3_input_files_prefix': key,
                    '--trading_partner': trading_partner,
                }
            };
            console.log('params', params);
    
            try {
                const data = await glue.startJobRun(params).promise();
                console.log(`Started Glue job with JobRunId: ${data.JobRunId}`);
            } catch (error) {
                console.error(`Error starting Glue job: ${error.message}`);
            }
        
        }
        const deleteParams = {
            QueueUrl: QUEUE_URL,
            ReceiptHandle: record.receiptHandle
        };
        await sqs.deleteMessage(deleteParams).promise();
        console.log('Deleted message from SQS');

    }

    return `Successfully processed ${event.Records.length} messages.`;
};