import AWS from 'aws-sdk';
const s3 = new AWS.S3();
const sqs = new AWS.SQS();

export const handler = async (event) => {
            console.log('inside handler');
            // Retrieve the uploaded file details from the sqs event
            console.log('Received SQS event:', JSON.stringify(event));
            let event_msg;
            
            if (Array.isArray(event)) {
                event_msg = event[0];
            } else {
                event_msg = event;
            }
	   for (const record of JSON.parse(event_msg.body).Records) {
		    const s3Event = record.s3;
            const bucketName = s3Event.bucket.name;
            const objectKey = decodeURIComponent(s3Event.object.key.replace(/\+/g, ' '));
            console.log('this bucket name : ',bucketName);
            console.log('this is object name : ',objectKey);
            // Initialize the S3 client
            const s3Source = new AWS.S3({ params: { Bucket: bucketName } });
            //getting trading partner name and file name
            const folderPath=objectKey.split('/');
		        console.log('this folderPath',folderPath);
            const tradingPartner=folderPath[folderPath.length-2].toLowerCase();
            console.log('Trading partner value:',tradingPartner);
            const fileName=folderPath[folderPath.length-1];
            console.log('input file name:',fileName);
          	const fileNameWoExt=fileName.split('.').shift();
            console.log('Input file name wo extension:',fileNameWoExt);
            const fileExt = objectKey.split('.').pop().toLowerCase();
            console.log('File extension:', fileExt);
            //get the folder name
            const folder=objectKey.replace(fileName,"");
            //const baseFileName=objectKey.replace('.completed',"");
            try{
              //List all objects in the folder
              const listParams={
                Bucket:bucketName,
                Prefix:folder
              };
              console.log(listParams);
              const fileList=await s3.listObjectsV2(listParams).promise();
              console.log(fileList);
              //filter to find base file and image file
              const billFile=fileList.Contents.find(file=>file.Key.includes(fileNameWoExt) && !file.Key.endsWith('.completed'));
              if(!billFile){
                  return {
                            statusCode: 500,
                            body: 'No bill file present in s3 bucket'
                        }; 
              }
              console.log(billFile.Key);
              var imageFile=null;
              if(tradingPartner=="jopari"){
                imageFile=fileList.Contents.find(file=>file.Key.includes(fileNameWoExt.replace('Bills','Images')) && !file.Key.endsWith('.completed'));
              }
              console.log(imageFile);
              const billFileExt=billFile.Key.split('.').pop();
              const imagefileExt=imageFile?imageFile.Key.split('.').pop():null;
              
               //delete .completed file
              const deleteCompletedFileParams={
                Bucket:bucketName,
                Key:objectKey
              };
              await s3.deleteObject(deleteCompletedFileParams).promise();
             
              //moving files which are not bill files or image files to error folder
              for(let file of fileList.Contents){
                if(file.key!==billFile.Key && (!imageFile || file.key!==imageFile.Key )&& file.Key!==objectKey){
                  //await moveInputFile('error', folderPath, bucketName, file.Key, s3Source);
    
                }
              }
              
              //prepare step function params
              const paramsOutput = {
                          "TradingPartnerName" : tradingPartner,	   
                          "BucketName" : bucketName,
                          "billfile" :{"fileName": billFile.Key,
                                       "extension":billFileExt
                          },
                          "imageFile":imageFile?{
                                     "fileName": imageFile.Key,
                                      "extension":imagefileExt
                          }:null
                            
                          };
                         
                           
                console.log(paramsOutput)
                           //delete sqs queue
                           const QUEUE_URL = process.env.Queue_URL;
                		 const deleteParams = {
                        QueueUrl: QUEUE_URL,
                        ReceiptHandle: event_msg.receiptHandle
                      };
                
                      await sqs.deleteMessage(deleteParams).promise();
                      console.log('Deleted SQS Message Successfully');
                        return {
                            statusCode: 200,
                            body: JSON.stringify(paramsOutput),
                        };           
          
              
              }catch(error){
              console.error(error);
              return{
                statusCode:500,
                body:JSON.stringify('Error processing the file:',error)
              };
              }
	   }
};