import AWS from 'aws-sdk';
import nodemailer from 'nodemailer';

export const handler = async (event) => {
    const s3 = new AWS.S3();
    const { to, subject, html, s3Bucket, s3Keys } = event; 

    const options = {
        from: 'NoReplyO365TXM@texasmutual.onmicrosoft.com',
        to: to,
        subject: subject,
        html: html,
        attachments: [] // initialize attachments array
    };

    const SecretsManagerClient = new AWS.SecretsManager({ region: 'us-east-1' });

    try {
        // Fetch email credentials
        const SecretsManagerResult = await SecretsManagerClient.getSecretValue({
            SecretId: 'prod/noreply_email',
        }).promise();

        const { email, password } = JSON.parse(SecretsManagerResult.SecretString);

        // Retrieve all S3 objects concurrently
        const s3Objects = await Promise.all(
            s3Keys.map(async (key) => {
                const s3Object = await s3.getObject({ Bucket: s3Bucket, Key: key }).promise();
                return {
                    filename: key.split('/').pop(),
                    content: s3Object.Body,
                };
            })
        );

        // Add attachments
        options.attachments = s3Objects;

        // Configure mail transport
        const transporter = nodemailer.createTransport({
            host: 'smtp.office365.com',
            port: 587,
            secure: false,
            auth: { user: email, pass: password },
        });

        // Send the email 
        const res = await transporter.sendMail(options);
        console.log('Email sent:', res);

        return {
            statusCode: 200,
            body: JSON.stringify('Email sent successfully with multiple attachments!'),
        };
    } catch (error) {
        console.error('ERROR:', error);
        return {
            statusCode: 500,
            body: JSON.stringify('Error sending email.'),
        };
    }
};
