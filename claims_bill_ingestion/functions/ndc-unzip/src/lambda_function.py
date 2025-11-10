import os
import json
import logging
import zipfile
from datetime import datetime
import io
import boto3
import pytz

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO) 
    
def move_input_file(folderType, bucket_name, file_prefix, s3_client): 
    destination_key = file_prefix.replace('inbound', folderType)

    try:
        # Copy file to destination folder
        s3_client.copy_object(Bucket=bucket_name,Key=destination_key,CopySource={'Bucket': bucket_name, 'Key': file_prefix})
        logger.info("File copied successfully.")

        # Delete the original file
        s3_client.delete_object(Bucket=bucket_name,Key=file_prefix)
        logger.info("Original file deleted successfully.")

    except Exception as e:
        logger.error(f"Error moving file: {e}")
        raise

def snstopicnotification(provider,error_info,error_message,file_name,environment,severity,sns_topic_arn):

    logger.info(f"Attempting to send SNS notification to {sns_topic_arn}")
    cst=pytz.timezone('America/Chicago')
    timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
    try:
        sns_client = boto3.client('sns')
        message_subject = f"{environment} - Unsupported File Type - {provider} file is Unsupported - {severity}"
        message_body = (
            f"Process: {provider}\n"
            f"File Name: {file_name}\n"
            f"Environment: {environment}\n"
            f"Error Info: {error_info}\n"
            f"Error Message: {error_message}"
        )
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=message_subject,
            Message=message_body
        )
        logger.info(f"Successfully sent SNS notification for {file_name}")
    except Exception as e:
        logger.error(f"Error sending SNS notification for {file_name}: {e}", exc_info=True)

def get_unzip_file(provider, bucket_name, zip_files_list, destination_folder, initiate_file_prefix, environment, environment_map, config_values, sns_topic_arn,s3_client):
    error_info = ''
    error_message = ''
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=initiate_file_prefix)
        file_data_bytes = response['Body'].read()
        logger.info(f"Downloaded {len(file_data_bytes)} bytes for {initiate_file_prefix}")

        folder_path_parts = initiate_file_prefix.split('/')
        provider = folder_path_parts[-2].lower()  #medispan
        file_name = folder_path_parts[-1]  #MDDBV25.00.20130529.zip
        file_name_wo_ext = os.path.splitext(file_name)[0]  # MDDBV25.00.20130529
        file_ext = os.path.splitext(file_name)[1][1:].lower()  #zip

        if not file_data_bytes:
            error_info = 'Input file is empty'
            error_message = 'zip file is empty.'
            logger.error(f"{error_info}: {initiate_file_prefix}")
            move_input_file('error', bucket_name, initiate_file_prefix, s3_client)
            snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)
            return {"statusCode": 500, "message": error_message}
    except s3_client.exceptions.NoSuchKey:
        error_info = 'File not found in S3'
        error_message = f'The specified object s3://{bucket_name}/{initiate_file_prefix} does not exist.'
        logger.error(error_message)
        snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)
        return {"statusCode": 404, "message": error_message}
    except Exception as e:
        logger.error(f"Error fetching object {initiate_file_prefix} from S3: {e}", exc_info=True)
        error_info = 'S3 Download Error'
        error_message = f'Failed to download file from S3: {e}'
        move_input_file('error', bucket_name, initiate_file_prefix, s3_client)
        snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)
        return {"statusCode": 500, "message": error_message}
    
    if file_ext in environment_map.get('unzipExtn', []):
        logger.info(f"Processing ZIP file: {initiate_file_prefix}")
        try:
            with zipfile.ZipFile(io.BytesIO(file_data_bytes), 'r') as zf:
                zip_entries = zf.infolist()

                if not zip_entries:
                    error_info = 'Input Zip file is Empty'
                    error_message = 'The provided ZIP file contains no entries.'
                    logger.error(f"{error_info}: {initiate_file_prefix}")
                    move_input_file('error', bucket_name, initiate_file_prefix, s3_client)
                    snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)
                    return {"statusCode": 500, "message": error_message}
                
                for entry in zip_entries:
                    if not entry.is_dir():
                        entry_data_name = entry.filename
                        logger.info(f"entry_data_name: {entry_data_name}")
                        if entry_data_name in config_values.get('files', []):
                            destination_path = (f"{destination_folder}/{provider}/ndc_code_files/{file_name_wo_ext}/")
                            destination_path_in_s3 = (f"{destination_folder}/{provider}/ndc_code_files/{file_name_wo_ext}/{entry_data_name}")

                            extracted_file_data = zf.read(entry)
                            logger.info(f"Uploading extracted file to: s3://{bucket_name}/{destination_path_in_s3}")
                            s3_client.put_object(
                                Bucket=bucket_name,
                                Key=destination_path_in_s3,
                                Body=extracted_file_data
                            )
                logger.info("All extracted files uploaded successfully.")
                for file_prefix in zip_files_list:
                    move_input_file('archive', bucket_name, file_prefix, s3_client)

        except zipfile.BadZipFile as e:
            error_info = 'Invalid/corrupt zip file.'
            error_message = f'The input file is a corrupted or invalid ZIP file: {e}'
            logger.error(f"{error_info}: {initiate_file_prefix}, Error: {e}", exc_info=True)
            move_input_file('error', bucket_name, initiate_file_prefix, s3_client)
            snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)
            return {"statusCode": 500, "message": error_message}
        except Exception as e:
            logger.error(f"Error during ZIP file processing for {initiate_file_prefix}: {e}", exc_info=True)
            error_info = 'ZIP Processing Error'
            error_message = f'An unexpected error occurred during ZIP processing: {e}'
            move_input_file('error', bucket_name, initiate_file_prefix, s3_client)
            snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)
            return {"statusCode": 500, "message": error_message}

    else:
        error_info = 'Unsupported File Type'
        error_message = f"Unsupported file type '{file_ext}'. Only ZIP files are expected for this process."
        logger.error(f"{error_info}: {initiate_file_prefix}")
        move_input_file('error', bucket_name, initiate_file_prefix, s3_client)
        snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)
        return {"statusCode": 500, "message": error_message}
    
    logger.info(f"Function completed successfully. Main provider folder key: {destination_path}")
    return {"statusCode": 200, "message": destination_path}

def return_response(error_info,error_message,provider,s3_bucket,step_function_info,reprocess_flag,batch_id,step_function_execution_id,status_code,request_id,lambda_name):
    logger.info(f"stoping the process: {error_message}")
    body_content = {
        "lambdaName": lambda_name,
        "parsedBody": {
            "step_function_info": step_function_info,
            "reprocess_flag": reprocess_flag,
            "provider": provider,
            "batch_id": batch_id,
            "step_function_execution_id": step_function_execution_id,
            "s3_bucket": s3_bucket,
        },
        "errorInfo": error_info,
        "errorMessage": error_message
    }


    return {
        "statusCode": status_code,
        "requestId": request_id,
        "lambdaName": lambda_name,
        "body": json.dumps(body_content)
    }


def lambda_handler(event, context):
    logger.info('Inside handler')
    logger.info(f"Received event: {json.dumps(event)}")

    aws_request_id = context.aws_request_id
    lambda_name = context.function_name
    initiate_folder = os.environ.get('initiate_folder')
    destination_folder = os.environ.get('Destination_folder')
    ndc_config_str = os.environ.get('ndc_config')
    sns_topic_arn = os.environ.get('sns_topic_arn')

    try:
        # Extract details from the event body
        parsed_body = event.get('parsedBody', {})

        provider = parsed_body.get('provider')
        reprocess_flag = parsed_body.get('reprocess_flag')
        batch_id = parsed_body.get('batch_id')
        step_function_info = parsed_body.get('step_function_info')
        step_function_execution_id = parsed_body.get('step_function_execution_id')
        bucket_name = parsed_body.get('s3_bucket')

        logger.info(f"provider value: {provider}")
        logger.info(f"Bucket name: {bucket_name}")
        logger.info(f"AWS Request ID value: {aws_request_id}")
        logger.info(f"Lambda Name value: {lambda_name}")
        logger.info(f"Reprocess flag value: {reprocess_flag}")
        logger.info(f"SF Batch ID value: {batch_id}")

        file_prefix = f"{initiate_folder}/{provider}/"
        logger.info(f"Initiate folder path: {file_prefix}")
        correct_formate = {}
        correct_file_paths = []
        zip_files_list = []
        non_zip_files_list = []
        formatted_date = None
        s3_client = boto3.client('s3') 

        try:
            environment_map = json.loads(ndc_config_str)
            environment = environment_map.get('env')
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing ndc_config JSON: {e}")
            error_info = 'Configuration Error'
            error_message = f'ndc_config environment variable is invalid JSON: {e}'
            snstopicnotification(provider, error_info, error_message, 'N/A', 'unknown', 'FATAL', sns_topic_arn)
            return {"statusCode": 500, "message": error_message}
        
        if not destination_folder:
            logger.error("Environment variable 'Destination_folder' is not set.")
            error_info = 'Configuration Error'
            error_message = 'Destination_folder environment variable is missing.'
            snstopicnotification(provider, error_info, error_message, 'N/A', environment, 'FATAL', sns_topic_arn)
            status_code = 500
            return return_response(error_info,error_message,provider,bucket_name,step_function_info,reprocess_flag,batch_id,step_function_execution_id,status_code,aws_request_id,lambda_name)
        
        if not ndc_config_str:
            logger.error("Environment variable 'ndc_config' is not set.")
            error_info = 'Configuration Error'
            error_message = 'ndc_config environment variable is missing.'
            snstopicnotification(provider, error_info, error_message, 'N/A', environment, 'FATAL', sns_topic_arn)
            status_code = 500
            return return_response(error_info,error_message,provider,bucket_name,step_function_info,reprocess_flag,batch_id,step_function_execution_id,status_code,aws_request_id,lambda_name)
        
        config_values = environment_map.get(provider)
        if not config_values:
            error_info = f'Configuration not found for trading partner: {provider}'
            error_message = f'ndc_config missing entry for {provider}'
            logger.error(error_message)
            snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)
            status_code = 500
            return return_response(error_info,error_message,provider,bucket_name,step_function_info,reprocess_flag,batch_id,step_function_execution_id,status_code,aws_request_id,lambda_name)

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_prefix)

        # Extract only .zip file paths
        for obj in response.get("Contents", []):
            if obj and "Key" in obj:
                file_key = obj["Key"]
                cleaned_file_key = file_key.strip()
                cleaned_file_key = cleaned_file_key.replace('\u200b', '')
                if not cleaned_file_key or cleaned_file_key.endswith('/'): # Check if the key is empty or just a path after cleaning
                    if not cleaned_file_key:
                        logger.warning(f"Skipping key that became empty after cleaning: '{file_key}'")
                    else: # It ends with a slash, so it's a directory
                        logger.info(f"Skipping directory key: '{file_key}'")
                    continue # Skip this file completely
                if cleaned_file_key.endswith(".zip"):
                    zip_files_list.append(cleaned_file_key)
                else:
                    non_zip_files_list.append(cleaned_file_key)

        print("ZIP Files:", zip_files_list)
        print("Non-ZIP Files:", non_zip_files_list)

        #Moving non zip files to error folder
        if non_zip_files_list:
            logger.info(f"not a zip files, moving to error folder : {non_zip_files_list}")
            for file_path in non_zip_files_list:
                file_name = file_path.split("/")[-1]
                error_info = 'Unable to unzip, invalid file'
                error_message = 'The input file is not a zip file'
                logger.info(f"Attempting to move file: {file_name}")
                move_input_file('error', bucket_name, file_path, s3_client)
                snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)

        #If Zip file list is empty, raising empty folder
        if not zip_files_list:
            error_info = 'Empty folder'
            error_message = 'The input list for Provider files is empty.'
            logger.error(error_message)
            snstopicnotification(provider, error_info, error_message, 'N/A', environment, 'FATAL', sns_topic_arn)
            status_code = 404
            return return_response(error_info,error_message,provider,bucket_name,step_function_info,reprocess_flag,batch_id,step_function_execution_id,status_code,aws_request_id,lambda_name)
        
        for file_path in zip_files_list:
            file_name = file_path.split("/")[-1]
            parts = file_name.split('.')
            concat_parts = (f"{parts[0]}.{parts[1]}")
            logger.info(f"concat parts : {concat_parts}")

            # Checks the lenght of file name and constant value (MDDBV25.00)
            if len(parts) == 4 and concat_parts == config_values.get('file_starts'):  
                date_str = parts[-2]  
                logger.info(f"date_str : {date_str}")
                date_obj = datetime.strptime(date_str, "%Y%m%d") 
                formatted_date = date_obj.strftime("%Y-%m-%d")
                correct_formate[file_path] = date_obj
                logger.info(f"correct zip files : {correct_formate}")
                correct_file_paths.append(file_path)
                logger.info(f"correct zip files : {correct_file_paths}")
            else:
                error_info = 'Unsupported file type'
                error_message = 'The input file name formate is invalid'
                logger.error(error_message)
                logger.info(f"file prefix : {file_path}")
                move_input_file('error', bucket_name, file_path, s3_client)
                snstopicnotification(provider, error_info, error_message, file_name, environment, 'FATAL', sns_topic_arn)

        if not correct_file_paths:
            error_info = 'Unsupported file type'
            error_message = 'The input file name formate is invalid'
            logger.error(error_message)
            snstopicnotification(provider, error_info, error_message, 'N/A', environment, 'FATAL', sns_topic_arn)
            status_code = 500
            return return_response(error_info,error_message,provider,bucket_name,step_function_info,reprocess_flag,batch_id,step_function_execution_id,status_code,aws_request_id,lambda_name)

        # Find the latest file
        initiate_file_prefix = max(correct_formate, key=correct_formate.get) if correct_formate else None
        logger.info(f"Latest file for Unzip : {initiate_file_prefix}")
        remaining_files = [f for f in correct_file_paths if f != initiate_file_prefix]
        logger.info(f"Moving the old files to archive : {remaining_files}")

        folder_path_parts = initiate_file_prefix.split('/')
        provider = folder_path_parts[-2].lower()  #medispan
        file_name = folder_path_parts[-1]  #MDDBV25.00.20130529.zip

        # Unzip and process provider file
        logger.info('Unzipping provider file...')
        unzip_bill_file_result = get_unzip_file(provider,bucket_name, correct_file_paths, destination_folder, initiate_file_prefix, environment, environment_map, config_values, sns_topic_arn,s3_client)
        logger.info(f"Unzipped provider file result: {unzip_bill_file_result}")

        # Check provider file unzipping result
        if unzip_bill_file_result['statusCode'] == 500:
            event['errorInfo'] = 'Error occurred while unzipping provider file'
            event['errorMessage'] = unzip_bill_file_result['message']
            return {
                "statusCode": 500,
                "requestId": aws_request_id,
                "lambdaName": lambda_name,
                "body": json.dumps(event)
            }
        elif unzip_bill_file_result['statusCode'] == 404:
            event['errorInfo'] = 'File not found in S3'
            event['errorMessage'] = unzip_bill_file_result['message']
            return {
                "statusCode": 404,
                "requestId": aws_request_id,
                "lambdaName": lambda_name,
                "body": json.dumps(event)
            }
        else:
            # Success response
            return {
                "statusCode": 200,
                "requestId": aws_request_id,
                "lambdaName": lambda_name,
                "body": json.dumps({
                    "message": 'File processed successfully',
                    "provider": provider,
                    "s3_bucket": bucket_name,
                    "s3_input_files_prefix": unzip_bill_file_result['message'],
                    "step_function_info": step_function_info,
                    "reprocess_flag": reprocess_flag,
                    "batch_id": batch_id,
                    "step_function_execution_id": step_function_execution_id,
                    "create_date": formatted_date
                })
            }

    except Exception as error:
        logger.error(f"Exception Error in handler: {error}", exc_info=True)
        event['errorInfo'] = 'Error processing the uploaded file'
        event['errorMessage'] = str(error)
        snstopicnotification(provider, event['errorInfo'], event['errorMessage'], 'N/A', environment, 'FATAL', sns_topic_arn)
        return {
            "statusCode": 500,
            "requestId": aws_request_id,
            "lambdaName": lambda_name,
            "body": json.dumps(event)
        }