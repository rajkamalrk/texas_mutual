import AWS from 'aws-sdk';
import openpgp from 'openpgp';
const s3 = new AWS.S3();
const sns = new AWS.SNS();

export const handler = async (event, context) => {
            console.log('inside handler');

            const awsRequestId = context.awsRequestId;
            const lambdaName = context.functionName;
            
            try{
            // Retrieve the uploaded file details from the event
            console.log('Received  event:', JSON.stringify(event));
            
           // const result=JSON.parse(event.body);
            let imageFileLoc=null;
		
            const bucketName = event.parsedBody.s3_bucket;
            let billFileLoc=event.parsedBody.bill_file.file_name;
            if(event.parsedBody.image_file!=null){
            imageFileLoc=event.parsedBody.image_file.file_name;
            }
            const source=event.parsedBody.source;
            const tradingPartner=event.parsedBody.trading_partner;
            const reprocessFlag=event.parsedBody.reprocess_flag;
            const sfBatchId=event.parsedBody.sf_batch_id;
            const stepFunctionInfo=event.parsedBody.step_function_info;
            const stepFunctionExecutionId=event.parsedBody.step_function_execution_id;
            console.log('source value :',source);
            console.log('trading partner value :',tradingPartner);
            console.log('this bucket name : ',bucketName);
            console.log('this is bill file name : ',billFileLoc);
            console.log('this is image file name : ',imageFileLoc);
            console.log('reprocess flag value :',reprocessFlag);
            console.log('sf batch id value :',sfBatchId);
            console.log('awsRequestId value :',awsRequestId);
            console.log('lambdaName value :',lambdaName);

            if (reprocessFlag === 'Y'){
              console.log('Reprocess Flow started...');
              let billFileArchiveLoc=null;
              let billFileErrorLoc=null;
              let billFileProcessed=false;
              const billRes1=await isS3FileExists(bucketName,billFileLoc);
              if (billRes1){
                console.log('bill file exists in inbound');
              }else{
                console.log('bill file NOT exists in inbound');
                billFileArchiveLoc=billFileLoc.replace("inbound","archive")
                const billRes2=await isS3FileExists(bucketName,billFileArchiveLoc);
                if (billRes2){
                  billFileProcessed=true;
                  console.log('bill file exists in archive');
                  billFileLoc=billFileArchiveLoc
                }else{
                  console.log('bill file NOT exists in archive');
                  billFileErrorLoc=billFileLoc.replace("inbound","error")
                  const billRes3=await isS3FileExists(bucketName,billFileErrorLoc);
                  if (billRes3){
                    console.log('bill file exists in error');
                    billFileLoc=billFileErrorLoc;
                  }else{
                    console.log('bill file NOT exists in error');
                  }
                }
              }
              if(imageFileLoc){
                let imageFileArchiveLoc=null;
                let imageFileErrorLoc=null;
                let imageFileProcessed=false;
                const imageRes1=await isS3FileExists(bucketName,imageFileLoc);
                if (imageRes1){
                  console.log('image file exists in inbound');
                }else{
                  console.log('image file NOT exists in inbound');
                  imageFileArchiveLoc=imageFileLoc.replace("inbound","archive")
                  const imageRes2=await isS3FileExists(bucketName,imageFileArchiveLoc);
                  if (imageRes2){
                    imageFileProcessed=true;
                    console.log('image file exists in archive');
                    imageFileLoc=imageFileArchiveLoc
                  }else{
                    console.log('image file NOT exists in archive');
                    imageFileErrorLoc=imageFileLoc.replace("inbound","error")
                    const imageRes3=await isS3FileExists(bucketName,imageFileErrorLoc);
                    if (imageRes3){
                      console.log('image file exists in error');
                      imageFileLoc=imageFileErrorLoc;
                    }else{
                      console.log('image file NOT exists in error');
                    }
                  }
                }
              }
              console.log('this is new bill file name : ',billFileLoc);
              console.log('this is new image file name : ',imageFileLoc);
            }

            console.log('decrypting bill file');
            const decryptedBillFile=await getDecryptedFileName(billFileLoc,bucketName);
            console.log('decrypted bill file :',decryptedBillFile);
            let decryptedImageFile=null;
            if(imageFileLoc){
              console.log('decrypting image file');
              decryptedImageFile=await getDecryptedFileName(imageFileLoc,bucketName);
               if(decryptedImageFile.statusCode===500){
                event.errorInfo = 'Error occured while decrypting image file' ;
                event.errorMessage = decryptedImageFile.message ;
                return {
                statusCode: 500,
                requestId: awsRequestId,
                lambdaName: lambdaName,
                body: JSON.stringify(event)
            }; 

            }
            }else{
                console.log('No image file provided');
            }
            
             //prepare json params and sending as a input to unzip lambda
             if(decryptedBillFile.statusCode===500){
                event.errorInfo = 'Error occured while decrypting bill file' ;
                event.errorMessage = decryptedBillFile.message ;
                return {
                statusCode: 500,
                requestId: awsRequestId,
                lambdaName: lambdaName,
                body: JSON.stringify(event)
            };
            }else{
                 const paramsOutput = {
                     "source" : source,
                     "trading_partner" : tradingPartner,	   
                     "s3_bucket" : bucketName,
                     "decrypted_bill_file" : decryptedBillFile,
                     "decrypted_image_file":decryptedImageFile?{decryptedImageFile}:null,
                     "reprocess_flag" : reprocessFlag,
                     "sf_batch_id" : sfBatchId,
                     "step_function_info": stepFunctionInfo,
                     "step_function_execution_id": stepFunctionExecutionId
                      };
                       
                    console.log(`Uploaded  file to pass unzip lambda`,paramsOutput);
                    return {
                            statusCode: 200,
                            requestId: awsRequestId,
                            lambdaName: lambdaName,
                            body: JSON.stringify(paramsOutput),
                        };
            }
            } catch (error) {
                console.log('Exception Error:');
                console.log(error);
                event.errorInfo = 'Error processing the uploaded file' ;
                event.errorMessage = '' + error ;
                return {
                     statusCode: 500,
                     requestId: awsRequestId,
                     lambdaName: lambdaName,
                     body: JSON.stringify(event)
                };
    }    
 };
        async function getDecryptedFileName(objectKey,bucketName){
             //reading dependent values from lambda environment variable
            const environment=process.env.environment;
            const sns_topic_arn = process.env.sns_topic_arn;
            const destination_path=process.env.destination_path;
            const trading_partner_validate = process.env.trading_partner;
            const s3Destination = new AWS.S3({params:{Bucket:bucketName}});
            
            //getting trading partner name and file name
            const folderPath=objectKey.split('/');
		        console.log('this folderPath',folderPath);
            const tradingPartner=folderPath[folderPath.length-2].toLowerCase();
            console.log('Trading partner value:',tradingPartner);
            const fileName=folderPath[folderPath.length-1];
            console.log('input file name:',fileName);
            
            let outFile;
           //getting file directory to put file 
           var fileDirectory = objectKey.substring(0, objectKey.lastIndexOf("/") + 1);
           console.log('this is directory path : ',fileDirectory);
		  
            var fileNameAndExtension  = objectKey.split("/").pop();
            var fileNameAndExtensionArray = fileNameAndExtension.split(/\.(?=[^\.]+$)/);
            const fileNameWithOutPGPExtention = fileNameAndExtensionArray[0];
            console.log('file name : ',fileNameAndExtensionArray[0]);
            console.log('file extension : ',fileNameAndExtensionArray[1]);
            const decryptedFileName= fileDirectory+fileName;
            var errorInfo='';
            var errorMessage='';
           
            try{
                if(trading_partner_validate.includes(tradingPartner)){
                   console.log('valid trading partner file');
                   const pemSecretKeys=JSON.parse(process.env.pemSecretKeys);
                   const configValues=pemSecretKeys[tradingPartner];
            
                    const pemSecretPrivateName = configValues.privateKey;
                    const pemSecretPassPharaseName = configValues.passphrase;
                    const pemKeys= await getSecreteValues(pemSecretPrivateName);
                    //console.log('this is secret testing key  ',pemKeys);
                    const secretsManager = new AWS.SecretsManager();
                    const SecretsManagerResult = await secretsManager.getSecretValue({SecretId: pemSecretPassPharaseName,
                                     }).promise();
                    const SecretsManagerResponse = JSON.parse(SecretsManagerResult.SecretString);
                    const {passPharaseKey} = SecretsManagerResponse;
                    
                    if(fileNameAndExtensionArray[1] ==='pgp'){ 
                       console.log('getting pgp extension encrypted file');
                       const decryptedPGPFileName= destination_path+'decrypt'+'/'+tradingPartner+'/'+fileNameAndExtensionArray[0];
                       
                       //geting private key and passphrase 
                       //const {keys:[privateKey]}= await openpgp.key.readArmored(pemKeys);
                       //const {keys:[privateKey]}= await openpgp.key.readPrivateKey(pemKeys);
                      //  const privateKey= await openpgp.readPrivateKey({ armoredKey: pemKeys }); //privateKeyArmored
                      
                      const { keys: [privateKey] } = await openpgp.key.readArmored(pemKeys);

                      // Decrypt the private key using the passphrase
                      await privateKey.decrypt(passPharaseKey);
                  
                       //console.log('this is passphrase key ',passPharaseKey);
                       //console.log('this is type of phass phrase ',typeof passPharaseKey);
                       if(passPharaseKey == (null || "") || privateKey == (null || "")){
                         errorInfo='Invalid or Unsupported PGP keys';
                         console.log('errorInfo:',errorInfo);
                         errorMessage='Invalid or Unsupported PGP keys - Unable to extract Pass Phrase or Private Key';
                         console.log('errorMessage:',errorMessage);
                         await moveInputFile('error',destination_path,tradingPartner,bucketName,objectKey,fileName,s3Destination);
                         await snsTopicNotificationAlert(environment,errorInfo,errorMessage,tradingPartner,fileName,sns_topic_arn,s3Destination,bucketName).then(messageId => console.log('Returned SNS MessageId:', messageId))
                     .catch(err => console.error('Invocation error:', err));;
                         return {
                            statusCode: 500,
                            message: errorMessage 
                         };
                     
                        
                       }
                       //getting SFTP sended encrypt file from s3 bucket
                       const encryptedFile=await s3.getObject({
                        Bucket:bucketName,
                        Key: objectKey,
                        ResponseContentType:'application/pgp-encrypted'
                        }).promise();
                        //console.log(encryptedFile)
                       //let encryptedData=encryptedFile.Body
                       //let encryptedData=encryptedFile.Body.toString('utf-8');
                       var encryptedData = new Uint8Array(encryptedFile.Body); 
                       //console.log(encryptedData); 
                       //console.log('this is type of encryptedData ',typeof encryptedData);
                       //await privateKey.decrypt(passPharaseKey); 
                      //  var privateKeyDecrypt = await openpgp.decryptKey({privateKey: privateKey, passphrase: passPharaseKey});
                       try{
                       //const Encmessage=await openpgp.message.readArmored(encryptedData);
                      //  const Encmessage=await openpgp.readMessage({ binaryMessage: encryptedData });
                       console.log('Starting decryption...')
                       //decrypting SFTP sended encrypted file by using secret of private key and passpharas key
                       const { data: decrypted } = await openpgp.decrypt({
                             message:await openpgp.message.read(encryptedData),
                             privateKeys:[privateKey],
                             format:'binary'
                             });
                       console.log('decrypted data',decrypted);
                       if(decrypted){
                       console.log('this is decryptedFileName :',decryptedFileName);
                       const splitFileName = fileNameAndExtensionArray[0].split(/\.(?=[^\.]+$)/);
                       console.log('this is file split by dot ',splitFileName);
                       // putting decrypted file back in S3 decrypt folder
                      //const contentType = 'binary';
                      console.log('this  is split file ',splitFileName[1]);
                      if(splitFileName[1] === 'txt'){
                          await s3.putObject({
                            Bucket: bucketName,
                            Key:decryptedPGPFileName,
                            Body:Buffer.from(decrypted),
                            ContentType:'binary'
                          }).promise();
                      }else {
                          console.log('this is else block');
                          await s3.putObject({
                            Bucket: bucketName,
                            Key:decryptedPGPFileName,
                            Body:Buffer.from(decrypted),
                            ContentType:'application/zip'
                          }).promise();
                      }
                      //console.log('this is finaly constent type  ',contentType);
                      
                      //sending iniate trading partner pgp file to archive trading partner folder 
                      await moveInputFile('archive',destination_path,tradingPartner,bucketName,objectKey,fileName,s3Destination);
                     
                     }else {
                     errorInfo='PGP file cannot be decrypted';
                     console.log('errorInfo:',errorInfo);
                     errorMessage='PGP file cannot be decrypted - Decryted data is empty';
                     console.log('errorMessage:',errorMessage);
                     //file can not be decrypted, sending file to error folder and sending mail 
                      await moveInputFile('error',destination_path,tradingPartner,bucketName,objectKey,fileName,s3Destination);
                     
                      await snsTopicNotificationAlert(environment,errorInfo,errorMessage,tradingPartner,fileName,sns_topic_arn,s3Destination,bucketName).then(messageId => console.log('Returned SNS MessageId:', messageId))
                     .catch(err => console.error('Invocation error:', err));;
                        return {
                            statusCode: 500,
                            message: errorMessage
                         }; 
                     }
                       }
                       catch(error){
                          console.log('Exception Error:');
                          console.log(error);
                          errorInfo='Error decrypting file';
                          console.log('errorInfo:',errorInfo);
                          errorMessage='Error decrypting file: ' + error;
                          console.log('errorMessage:',errorMessage);
                           await moveInputFile('error',destination_path,tradingPartner,bucketName,objectKey,fileName,s3Destination);
                       
                       await  snsTopicNotificationAlert(environment,errorInfo,errorMessage,tradingPartner,fileName,sns_topic_arn,s3Destination,bucketName).then(messageId => console.log('Returned SNS MessageId:', messageId))
                     .catch(err => console.error('Invocation error:', err));;
                       return {
                            statusCode: 500,
                            message: errorMessage
                         };
                           
                       }
                    outFile= decryptedPGPFileName
                         }//pgpif
                
                
                    else{
                        console.log('this is non pgp extention file');
                       outFile= decryptedFileName
          
         
                 }//else
                          
            }
                   //any error raising in try block will throw catch block and sending mail with error details
                }  catch (error) {
                  console.log('Exception Error:');
                  console.log(error);
                  errorInfo='Error in decrypting process';
                  console.log('errorInfo:',errorInfo);
                  errorMessage='Error in decrypting process: ' + error;
                  console.log('errorMessage:',errorMessage);
                //file will move to error folder and sending error details through sns mail
                await moveInputFile('error',destination_path,tradingPartner,bucketName,objectKey,fileName,s3Destination);
                await snsTopicNotificationAlert(environment,errorInfo,errorMessage,tradingPartner,fileName,sns_topic_arn,s3Destination,bucketName).then(messageId => console.log('Returned SNS MessageId:', messageId))
                                                 .catch(err => console.error('Invocation error:', err));;
                    return {
                     statusCode: 500,
                     message:errorMessage
                     };
                    
                 }//catch
                return {
                          statusCode: 200,
                            message: outFile
                         }; 
      
        }
        
        async function getSecreteValues(pemSecretPrivateName){
            return new Promise((resolve) =>{
           const secretsManager = new AWS.SecretsManager();
           //const secretName = 'decrypt-pem-keys';
          secretsManager.getSecretValue({ SecretId: pemSecretPrivateName }, (err, data) => {
            if (err) {
              console.log(`Error retrieving secret: ${err}`);
              return;
            }
          
            let secret;
            if ('SecretString' in data) {
              secret = data.SecretString;
              resolve(secret);
            } else {
              // Handle binary secrets if needed
              secret = Buffer.from(data.SecretBinary, 'base64');
              resolve(secret);
              
            }
  
            // Use your secret here
            //console.log(`Retrieved secret: ${secret}`);
          });
        });
        }

        async function moveInputFile(folderName,destination_path,tradingPartner,bucketName,objectKey,fileName,s3Destination){
        	const copy=bucketName+"/"+objectKey;
        	console.log('copying folder : ',copy);
            const destKey=destination_path+folderName+"/"+tradingPartner+"/"+fileName;
            console.log('destination key',destKey);
          
            await s3Destination.copyObject({Bucket:bucketName,CopySource:copy,Key:destKey}).promise();
            console.log('archived');
        
           await s3Destination.deleteObject({Bucket:bucketName,Key:objectKey}).promise();
            console.log('File moved successfully');
        }

        async function snsTopicNotificationAlert(environment,errorInfo,errorMSG,tradingPartner,fileName,sns_topic_arn,s3Destination,bucketName) {
            errorMsgLog(tradingPartner,fileName,errorMSG,s3Destination,bucketName); 
            console.log('New SNS method calling');
            const process = `${tradingPartner.toUpperCase()} - Decrypt Batch Files`;
            const ts=new Date().toLocaleString('en-US',{timeZone:'America/Chicago'});
            const params = {
                Subject: `${environment} - ${tradingPartner.toUpperCase()} SOLUTIONS INC - ${errorInfo} - FATAL`,
                Message: `Process: ${process}\nFileName: ${fileName}\nTimestamp:  ${ts}.CST\nError Info: ${errorInfo}\nError: ${errorMSG}`,
                TopicArn: sns_topic_arn
            };
        
            try {
                console.log('Params:', params);
                const result = await sns.publish(params).promise();
                console.log('Publish result:', result);
                const { MessageId } = result;
                console.log('MessageId:', MessageId);
                return MessageId;
            } catch (e) {
                console.error('Error publishing to SNS:', e);
            }
          
        
        }
        async function errorMsgLog(tradingPartner,fileName,errorMSG,s3Destination,bucketName){
             const tradingPartnerName=tradingPartner.toUpperCase();
        var datetime = new Date();   
        const year = datetime.getFullYear();
        const month = datetime.getMonth() + 1;
        const day = datetime.getDate();
        const formattedDate = datetime.toISOString();
         console.log('this is formattedDate ',formattedDate)
           const desFolder='logs'+'/'+'decrypt_lambda'+'/'+year+'-'+month+'-'+day+'/'+tradingPartnerName+'_error_log_decrypt_lambda_'+formattedDate+'.json';
           const fileData = {
           "TradingPartnerName" : tradingPartner,	   
          "FileName" : fileName,
          "errorMsg" : errorMSG
        } 
        
        const jsonString = JSON.stringify(fileData);
        try{
        
          await s3Destination.upload({Bucket:bucketName,Key:desFolder,Body:jsonString}).promise();
        }catch(e){
            console.log('this is catch throwing ',e);
        }
        }
        async function isS3FileExists(bucketName,objectKey){
          const params = {
            Bucket: bucketName,
            Key: objectKey
          }; 
          // Using async/await
          try {
            await s3.headObject(params).promise();
            console.log('File exists:', objectKey);
            return true;
          } catch (error) {
            if (error.code === 'NotFound') { // Note with v2 AWS-SDK use error.code, v3 uses error.name
              console.log('File does not exist ', objectKey);
              return false;
            } else {
              console.log('Error occurred:', error);
              throw new Error('Error occurred:', error);
            }
          }
        }