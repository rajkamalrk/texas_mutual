import AWS from 'aws-sdk';
import AdmZip from 'adm-zip';

const s3 = new AWS.S3();
const sns = new AWS.SNS();

export const handler = async (event, context) => {
    console.log('Inside handler');
    console.log('Received event:', JSON.stringify(event));

    const awsRequestId = context.awsRequestId;
    const lambdaName = context.functionName;

    let imageFileLoc = null;
    let billFileLoc = null;

    try {
        // Retrieve the uploaded file details from the event
        console.log('Received event:', JSON.stringify(event));

        const bucketName = event.parsedBody.s3_bucket;
        const source = event.parsedBody.source;
        const tradingPartner = event.parsedBody.trading_partner;
        const reprocessFlag = event.parsedBody.reprocess_flag;
        const sfBatchId = event.parsedBody.sf_batch_id;
        const stepFunctionInfo = event.parsedBody.step_function_info;
        const stepFunctionExecutionId = event.parsedBody.step_function_execution_id;

        console.log('source value :', source);
        console.log('trading partner value :', tradingPartner);
        console.log('this bucket name : ', bucketName);
        console.log('reprocess flag value :', reprocessFlag);
        console.log('sf batch id value :', sfBatchId);
        console.log('awsRequestId value :', awsRequestId);
        console.log('lambdaName value :', lambdaName);

        if (tradingPartner == 'jopari') {
            billFileLoc = event.parsedBody.decrypted_bill_file.message;
            if (event.parsedBody.decrypted_image_file != null) {
                imageFileLoc = event.parsedBody.decrypted_image_file.decryptedImageFile.message;
            }
        } else {
            billFileLoc = event.parsedBody.bill_file.file_name;

            if (event.parsedBody.image_file != null) {
                imageFileLoc = event.parsedBody.image_file.file_name;
            }
        }
        console.log('this bucket name : ', bucketName);
        console.log('this is bill file name : ', billFileLoc);
        console.log('this is image file name : ', imageFileLoc);

        if (reprocessFlag === 'Y') {
            console.log('Reprocess Flow started...');
            let billFileArchiveLoc = null;
            let billFileErrorLoc = null;
            let billFileProcessed = false;

            // Check if the bill file exists
            const billRes1 = await isS3FileExists(bucketName, billFileLoc);
            if (!billRes1) {
                console.log('bill file NOT exists in inbound');
                billFileArchiveLoc = billFileLoc.replace("inbound", "archive");
                const billRes2 = await isS3FileExists(bucketName, billFileArchiveLoc);
                if (billRes2) {
                    billFileProcessed = true;
                    console.log('bill file exists in archive');
                    billFileLoc = billFileArchiveLoc;
                } else {
                    console.log('bill file NOT exists in archive');
                    billFileErrorLoc = billFileLoc.replace("inbound", "error");
                    const billRes3 = await isS3FileExists(bucketName, billFileErrorLoc);
                    if (billRes3) {
                        console.log('bill file exists in error');
                        billFileLoc = billFileErrorLoc;
                    } else {
                        console.log('bill file NOT exists in error');
                    }
                }
            }

            // Check if the image file exists
            if (imageFileLoc) {
                let imageFileArchiveLoc = null;
                let imageFileErrorLoc = null;
                let imageFileProcessed = false;

                const imageRes1 = await isS3FileExists(bucketName, imageFileLoc);
                if (!imageRes1) {
                    console.log('image file NOT exists in inbound');
                    imageFileArchiveLoc = imageFileLoc.replace("inbound", "archive");
                    const imageRes2 = await isS3FileExists(bucketName, imageFileArchiveLoc);
                    if (imageRes2) {
                        imageFileProcessed = true;
                        console.log('image file exists in archive');
                        imageFileLoc = imageFileArchiveLoc;
                    } else {
                        console.log('image file NOT exists in archive');
                        imageFileErrorLoc = imageFileLoc.replace("inbound", "error");
                        const imageRes3 = await isS3FileExists(bucketName, imageFileErrorLoc);
                        if (imageRes3) {
                            console.log('image file exists in error');
                            imageFileLoc = imageFileErrorLoc;
                        } else {
                            console.log('image file NOT exists in error');
                        }
                    }
                }
            }

            console.log('this is new bill file name : ', billFileLoc);
            console.log('this is new image file name : ', imageFileLoc);
        }

        console.log('unzipping bill file');
        const unzipBillFile = await getUnzipFile(bucketName, billFileLoc);
        console.log('unzipped bill file :', unzipBillFile);
        let unzipImageFile = null;

        if (imageFileLoc) {
            console.log('unzipping image file');
            unzipImageFile = await getUnzipFile(bucketName, imageFileLoc);
            if (unzipImageFile.statusCode === 500) {
                event.errorInfo = 'Error occurred while unzipping image file';
                event.errorMessage = unzipImageFile.message;
                return {
                    statusCode: 500,
                    requestId: awsRequestId,
                    lambdaName: lambdaName,
                    body: JSON.stringify(event)
                };
            }
        } else {
            console.log('No image file provided');
        }

        if (unzipBillFile.statusCode === 500) {
            event.errorInfo = 'Error occurred while unzipping bill file';
            event.errorMessage = unzipBillFile.message;
            return {
                statusCode: 500,
                requestId: awsRequestId,
                lambdaName: lambdaName,
                body: JSON.stringify(event)
            };
        } else {
            return {
                statusCode: 200,
                requestId: awsRequestId,
                lambdaName: lambdaName,
                body: JSON.stringify({
                    message: 'File processed successfully',
                    source: source,
                    trading_partner: tradingPartner,
                    s3_bucket: bucketName,
                    s3_input_files_prefix: unzipBillFile.message,
                    reprocess_flag: reprocessFlag,
                    sf_batch_id: sfBatchId,
                    step_function_info: stepFunctionInfo,
                    step_function_execution_id: stepFunctionExecutionId
                })
            };
        }

    } catch (error) {
        console.log('Exception Error:');
        console.log(error);
        event.errorInfo = 'Error processing the uploaded file';
        event.errorMessage = '' + error;
        return {
            statusCode: 500,
            requestId: awsRequestId,
            lambdaName: lambdaName,
            body: JSON.stringify(event)
        };
    }
};

// Check if a file exists in S3
async function isS3FileExists(bucketName, objectKey) {
    const params = { Bucket: bucketName, Key: objectKey };
    try {
        await s3.headObject(params).promise();
        return true;  // File exists
    } catch (error) {
        if (error.code === 'NotFound') {
            return false;  // File doesn't exist
        }
        throw error;  // Some other error occurred
    }
}

// Function to move files between folders (inbound, archive, error)
async function moveInputFile(folderType, folderPath, bucketName, objectKey, s3Source) {
    const destinationKey = objectKey.replace('inbound', folderType);
    const params = { Bucket: bucketName, Key: destinationKey, CopySource: `${bucketName}/${objectKey}` };

    try {
        // Copy file to destination folder
        await s3Source.copyObject(params).promise();
        // Delete the original file
        await s3Source.deleteObject({ Bucket: bucketName, Key: objectKey }).promise();
    } catch (error) {
        console.error('Error moving file:', error);
        throw error;
    }
}

// Send notification to SNS topic
async function snstopicnotification(tradingPartner, errorInfo, errorMessage, fileName, environment, severity, sns_topic_arn, s3Destination, bucketName) {
    const snsParams = {
        Message: JSON.stringify({
            tradingPartner,
            errorInfo,
            errorMessage,
            fileName,
            environment,
            severity,
            bucketName
        }),
        TopicArn: sns_topic_arn
    };

    try {
        await sns.publish(snsParams).promise();
    } catch (error) {
        console.error('Error sending SNS notification:', error);
        throw error;
    }
}

// Refactor the `getUnzipFile` to handle large files concurrently using Promise.all
async function getUnzipFile(bucketName, objectKey) {
    const s3Source = new AWS.S3({ params: { Bucket: bucketName } });
    const s3Destination = new AWS.S3({ params: { Bucket: bucketName } });

    let bill_folder = null;
    let errorInfo = '';
    let errorMessage = '';

    const folderPath = objectKey.split('/');
    const tradingPartner = folderPath[folderPath.length - 2].toLowerCase();
    const fileName = folderPath[folderPath.length - 1];
    const fileNameWoExt = fileName.split('.').shift();

    const destination = process.env.Destination_folder;
    const environmentMap = JSON.parse(process.env.TP_config);
    const configValues = environmentMap[tradingPartner];
    const sns_topic_arn = process.env.sns_topic_arn;
    const fileExt = objectKey.split('.').pop().toLowerCase();

    try {
        const inputData = await s3Source.getObject({ Key: objectKey }).promise();
        const fileData = inputData.Body;
        console.log('fileData', fileData.length);

        if (configValues.FileExtn.includes(fileExt)) {
            console.log('In if (configValues.FileExtn.includes(fileExt)) {');
            if (configValues.inboundFileExtn.includes(fileExt)) {
                console.log('In if (configValues.inboundFileExtn.includes(fileExt)) {');
                if (fileData.length === 0) {
                    errorInfo = 'Input file is Empty';
                    errorMessage = 'Input file is Empty - EBill/Image file is empty';
                    await moveInputFile('error', folderPath, bucketName, objectKey, s3Source);
                    await snstopicnotification(tradingPartner, errorInfo, errorMessage, fileName, environmentMap.env, 'FATAL', sns_topic_arn, s3Destination, bucketName);
                    return { statusCode: 500, message: errorMessage };
                } else {
                    const rawDesFolder = `${destination}/${tradingPartner}/bill_files`;
                    console.log('rawDesFolder', rawDesFolder);
                    bill_folder = `${rawDesFolder}/${fileName}`;
                    console.log('bill_folder', bill_folder);
                    await s3Destination.upload({ Bucket: bucketName, Key: bill_folder, Body: fileData }).promise();
                    await moveInputFile('archive', folderPath, bucketName, objectKey, s3Source);
                }
            } else if (environmentMap.unzipExtn.includes(fileExt)) {
                console.log('In else if (environmentMap.unzipExtn.includes(fileExt)')
                const zip = new AdmZip(inputData.Body);
                const zipEntries = zip.getEntries();

                if (zipEntries.length === 0) {
                    errorInfo = 'Input Zip file is Empty';
                    errorMessage = 'Input file is Empty - EBill/Image Zip file is empty';
                    await moveInputFile('error', folderPath, bucketName, objectKey, s3Source);
                    await snstopicnotification(tradingPartner, errorInfo, errorMessage, fileName, environmentMap.env, 'FATAL', sns_topic_arn, s3Destination, bucketName);
                    return { statusCode: 500, message: errorMessage };
                }

                // Process files concurrently
                const uploadPromises = zipEntries.map(async (entry) => {
                    if (!entry.isDirectory) {
                        const fileData = entry.getData();
                        const entryDataName = entry.entryName;
                        const fileExtension = entryDataName.split('.').pop().toLowerCase();

                        let desFolder;
                        if (configValues.inboundFileExtn.includes(fileExtension)) {
                            desFolder = `${destination}/${tradingPartner}/bill_files`;
                            bill_folder = `${desFolder}/${entryDataName}`;
                        } else if (configValues.attachExtn.includes(fileExtension)) {
                            desFolder = `${destination}/${tradingPartner}/bill_attachments/${fileNameWoExt}`;
                        } else {
                            return; // Skip invalid files
                        }

                        await s3Destination.upload({ Bucket: bucketName, Key: `${desFolder}/${entryDataName}`, Body: fileData }).promise();
                    }
                });

                // Wait for all upload operations to finish
                await Promise.all(uploadPromises);

                // Call function to move to archive folder and delete
                await moveInputFile('archive', folderPath, bucketName, objectKey, s3Source);
            }
        } else {
            errorInfo = 'Unsupported File Type';
            errorMessage = 'Unsupported File Type - EBill/Image file is Unsupported';
            await moveInputFile('error', folderPath, bucketName, objectKey, s3Source);
            await snstopicnotification(tradingPartner, errorInfo, errorMessage, fileName, environmentMap.env, 'FATAL', sns_topic_arn, s3Destination, bucketName);
            return { statusCode: 500, message: errorMessage };
        }
    } catch (e) {
        console.log('Exception Error:', e);
        errorInfo = 'Error in unzipping process';
        errorMessage = 'Error in unzipping process:' + e;
        await moveInputFile('error', folderPath, bucketName, objectKey, s3Source);

        await snstopicnotification(tradingPartner, errorInfo, errorMessage, fileName, environmentMap.env, 'FATAL', sns_topic_arn, s3Destination, bucketName);
        return { statusCode: 500, message: errorMessage };
    }
    console.log('bill_folder', bill_folder);

    return { statusCode: 200, message: bill_folder };
}
