import os
import json
import logging
from datetime import datetime
import pymysql
import boto3
from botocore.exceptions import ClientError
import base64
import pytz

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name, region_name):
    """
    Retrieves secrets from AWS Secrets Manager.
    
    Args:
        secret_name (str): Name of the secret in AWS Secrets Manager
        region_name (str): AWS region where the secret is stored
        
    Returns:
        dict: Parsed secret values as a dictionary
        
    Raises:
        ClientError: If secret retrieval fails
        json.JSONDecodeError: If secret cannot be parsed as JSON
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId = secret_name)
    except ClientError as e:
        logger.info("Error retrieving secret '%s': %s", secret_name, str(e))
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response.get('SecretString', '')
    if not secret: 
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    return json.loads(secret)

def move_input_file(folderType, bucket_name, file_prefix, s3_client):
    destination_key = file_prefix.replace('inbound', folderType)

    try:
        s3_client.copy_object(Bucket=bucket_name, Key=destination_key, CopySource={'Bucket': bucket_name, 'Key': file_prefix})
        logger.info("File copied successfully.")

        s3_client.delete_object(Bucket=bucket_name, Key=file_prefix)
        logger.info("Original file deleted successfully.")

    except Exception as e:
        logger.error(f"Error moving file: {e}")
        raise

def run_insert_command(secrets, insert_sql, insert_values):
    """
    Execute an INSERT command against the database.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        insert_sql (str): SQL INSERT statement
        insert_values (tuple or list): Single tuple for one row or list of tuples for multiple rows
        
    Returns:
        int: Auto-increment ID of the last inserted row
        
    Raises:
        ValueError: If SQL query is not a valid INSERT statement
        Exception: If database operation fails
    """
    # Validate SQL query
    if not insert_sql.strip().upper().startswith("INSERT"):
        raise ValueError("Invalid INSERT query!!!")
    
    # Establish database connection
    connection = pymysql.connect( 
        host = secrets["host"],
        database = secrets["dbClusterIdentifier"],
        port = secrets["port"],
        user = secrets["username"],
        password = secrets["password"],
        connect_timeout = 300
    )

    auto_increment_id = None
    
    try:
        with connection.cursor() as cursor:
            # Handle both single tuple and list of tuples dynamically
            if isinstance(insert_values, tuple):
                # Single row insert
                cursor.execute(insert_sql, insert_values)
                auto_increment_id = cursor.lastrowid
                logger.info(f"Number of rows inserted: 1")
            elif isinstance(insert_values, list) and len(insert_values) > 0:
                # Multiple rows insert
                cursor.executemany(insert_sql, insert_values)
                auto_increment_id = cursor.lastrowid
                logger.info(f"Number of rows inserted: {cursor.rowcount}")
            else:
                raise ValueError("insert_values must be a tuple or non-empty list of tuples")

            # Commit the transaction
            connection.commit()
    except Exception as e:
        logger.info(f"Insertion Failed in DB: {str(e)}")
        raise e
    finally:
        connection.close()
    return auto_increment_id

def run_update_command(secrets, update_sql, update_values):
    """
    Execute an UPDATE command against the database.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        update_sql (str): SQL UPDATE statement
        update_values (tuple): Values to bind to the UPDATE statement
        
    Raises:
        ValueError: If SQL query is not a valid UPDATE statement
        Exception: If database operation fails
    """
    # Validate SQL query
    if not update_sql.strip().upper().startswith("UPDATE"):
        raise ValueError("Invalid UPDATE statement!!!")
    
    # Establish database connection
    connection = pymysql.connect( 
        host = secrets["host"],
        database = secrets["dbClusterIdentifier"],
        port = secrets["port"],
        user = secrets["username"],
        password = secrets["password"],
        connect_timeout = 300
    )
    
    try:
        with connection.cursor() as cursor:
            # Execute the update query
            cursor.execute(update_sql, update_values)

            # Log the number of rows updated
            logger.info(f"Number of rows updated: {cursor.rowcount}")

            # Commit the transaction
            connection.commit()
    except Exception as e:
        logger.info(f"Update Failed in DB: {str(e)}")
        raise e
    finally:
        connection.close()

def update_feeds_batch_status(secrets, batch_id, status, create_update_user):
    """
    Update batch status in the database for feeds processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID to associate with the batch
        status (str): Status of the batch
        create_update_user (str): User name for Create/Update
    """

    update_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    update_sql = f"""UPDATE txm_inbound_feeds_logs.batch_inbound_feeds_master
                        SET status = %s,
                            end_datetime = %s,
                            update_user = %s,
                            update_timestamp = %s
                        WHERE batch_id = %s"""
    update_values = (status, update_timestamp, create_update_user, update_timestamp, batch_id)

    run_update_command(secrets, update_sql, update_values)
    logger.info(f"Successfully updated batch_id {batch_id} to status {status}.")

def feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, status, create_update_user):
    """
    Log step details for batch processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID to associate with the step
        step_id (int, optional): Existing step ID for updates, None for new steps
        job_name (str): Name of the job/step
        job_id (str): Unique identifier for the job
        status (str): Status of the step
        create_update_user (str): User name for Create/Update
        
    Returns:
        int: Step ID (for new steps) or None (for updates)
    """
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    
    if step_id:
        update_sql = f"""UPDATE txm_inbound_feeds_logs.batch_inbound_feeds_step_details
                         SET status = %s,
                             end_datetime = %s,
                             update_user = %s,
                             update_timestamp = %s
                         WHERE batch_id = %s AND step_id = %s"""
        update_values = (status, v_load_timestamp, create_update_user, v_load_timestamp, batch_id, step_id)

        run_update_command(secrets, update_sql, update_values)
        logger.info(f"Successfully updated step status {status} for step_id: {step_id}")

    else:
        insert_sql = f"""INSERT INTO txm_inbound_feeds_logs.batch_inbound_feeds_step_details 
                            (batch_id, job_type, job_name, job_id, status, start_datetime, end_datetime, create_user, create_timestamp, update_user, update_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        insert_values = (batch_id, 'LAMBDA', job_name, job_id, status, v_load_timestamp, None, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

        step_id = run_insert_command(secrets, insert_sql, insert_values)
        logger.info(f"Successfully inserted log data with step_id: {step_id}")

        return step_id

def feeds_error_logging(secrets, batch_id, step_id, status, error_code, error_info, error_message, create_update_user):
    """
    Log error details for batch processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID associated with the error
        step_id (int, optional): Step ID associated with the error
        status (str): Status of the batch/step
        error_code (str): Error code for categorization
        error_info (str): Brief error description
        error_message (str): Detailed error message
        create_update_user (str): User name for Create/Update
    """
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    
    insert_sql = f"""INSERT INTO txm_inbound_feeds_logs.batch_inbound_feeds_error_details 
                        (batch_id, step_id, error_code, error_info, error_message, create_user, create_timestamp, update_user, update_timestamp)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    insert_values = (batch_id, step_id, error_code, error_info, error_message, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

    error_id = run_insert_command(secrets, insert_sql, insert_values)
    logger.info(f"Successfully inserted log data with error_id: {error_id}")

    feeds_step_logging(secrets, batch_id, step_id, None, None, status, create_update_user)

    update_feeds_batch_status(secrets, batch_id, status, create_update_user)

def snstopicnotification(batch_id, provider, feed_type, data_pipeline_name, data_pipeline_id, error_info, error_message, file_name, env_profile, reprocess_flag, severity, sns_topic_arn):
    logger.info(f"Attempting to send SNS notification to {sns_topic_arn}")
    cst = pytz.timezone('America/Chicago')
    timestamp = datetime.now(cst).strftime('%m/%d/%Y %H:%M:%S%Z')

    try:
        sns_client = boto3.client('sns')
        message_subject = f"{env_profile} - Unsupported File Type - {provider}-{feed_type} file is Unsupported - {severity}"
        message_body = (
            f"Process: {provider} - Inbound Feed {feed_type} file \n"
            f"Batch ID: {batch_id}\n"
            f"File Name: {file_name}\n"
            f"Step Function Name: {data_pipeline_name}\n"
            f"Step Function ID: {data_pipeline_id}\n"
            f"Timestamp: {timestamp}\n"
            f"Environment: {env_profile}\n" 
            f"Reprocess Flag: {reprocess_flag}\n" 
            f"Error Info: {error_info}\n"
            f"Fields & Content from Error log : {error_message[:500]}"
        )
        logger.info(message_body)
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=message_subject,
            Message=message_body
        )
        logger.info(f"Successfully sent SNS notification for {file_name}")
    except Exception as e:
        logger.error(f"Error sending SNS notification for {file_name}: {e}", exc_info=True)

def process_non_zip_file(batch_id, step_id, provider, feed_type, bucket_name, inbound_feed_file_path, destination_folder, step_function_info, step_function_execution_id, environment, environment_map, config_values, reprocess_flag, sns_topic_arn, s3_client, secrets):
    error_info = ''
    error_message = ''

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=inbound_feed_file_path)
        file_data_bytes = response['Body'].read()
        logger.info(f"Downloaded {len(file_data_bytes)} bytes for {inbound_feed_file_path}")

        file_name = os.path.basename(inbound_feed_file_path)
        file_name_wo_ext = os.path.splitext(file_name)[0]
        file_ext = os.path.splitext(file_name)[1][1:].lower()

        if not file_data_bytes:
            status = 'FAILED'
            error_info = 'Input file is empty'
            error_message = 'file is empty.'
            logger.error(f"{error_info}: {inbound_feed_file_path}")
            move_input_file('error', bucket_name, inbound_feed_file_path, s3_client)
            snstopicnotification(batch_id, provider, feed_type, step_function_info, step_function_execution_id, error_info, error_message, inbound_feed_file_path, environment, reprocess_flag, 'FATAL', sns_topic_arn)
            feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Feeds Unzip Lambda')
            logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
            return {"statusCode": 500, "message": error_message}
    except s3_client.exceptions.NoSuchKey:
        status = 'FAILED'
        error_info = 'File not found in S3'
        error_message = f'The specified object s3://{bucket_name}/{inbound_feed_file_path} does not exist.'
        logger.error(error_message)
        snstopicnotification(batch_id, provider, feed_type, step_function_info, step_function_execution_id, error_info, error_message, inbound_feed_file_path, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Feeds Unzip Lambda')
        logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
        return {"statusCode": 404, "message": error_message}
    except Exception as e:
        logger.error(f"Error fetching object {inbound_feed_file_path} from S3: {e}", exc_info=True)
        status = 'FAILED'
        error_info = 'S3 Download Error'
        error_message = f'Failed to download file from S3: {e}'
        move_input_file('error', bucket_name, inbound_feed_file_path, s3_client)
        snstopicnotification(batch_id, provider, feed_type, step_function_info, step_function_execution_id, error_info, error_message, inbound_feed_file_path, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Feeds Unzip Lambda')
        logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
        return {"statusCode": 500, "message": error_message}
    
    if file_ext in config_values.get('FileExtn', []):
        logger.info(f"Processing {config_values.get('FileExtn', [])} file: {inbound_feed_file_path}")
        destination_path = (f"{destination_folder}/{provider}/{feed_type.lower()}/")
        final_dest_path = f"{destination_path}{file_name}"

        logger.info(f"Uploading extracted file to: s3://{bucket_name}/{destination_path}{file_name}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=final_dest_path,
            Body=file_data_bytes
        )

        logger.info("All extracted files uploaded successfully.")
        move_input_file('archive', bucket_name, inbound_feed_file_path, s3_client)

    else:
        status = 'FAILED'
        error_info = 'Unsupported File Type'
        error_message = f"Unsupported file type '{file_ext}'. Only {config_values.get('FileExtn', [])} files are expected for this process."
        logger.error(f"{error_info}: {inbound_feed_file_path}")
        move_input_file('error', bucket_name, inbound_feed_file_path, s3_client)
        snstopicnotification(batch_id, provider, feed_type, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Feeds Unzip Lambda')
        logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
        return {"statusCode": 500, "message": error_message}

    logger.info(f"Function completed successfully for {inbound_feed_file_path}. Main provider folder key: {destination_path}")
    return {"statusCode": 200, "message": destination_path}

def return_response(error_info, error_message, provider, feed_type, s3_bucket, file_name, step_function_info, reprocess_flag, batch_id, step_function_execution_id, status_code, request_id, lambda_name):
    logger.info(f"Stopping the process: {error_message}")
    body_content = {
        "lambdaName": lambda_name,
        "parsedBody": {
            "step_function_info": step_function_info,
            "reprocess_flag": reprocess_flag,
            "provider": provider,
            'feed_type' : feed_type,
            "batch_id": batch_id,
            "step_function_execution_id": step_function_execution_id,
            "s3_bucket": s3_bucket,
            'file_name': file_name
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

def run_sql_query(db_sql,secrets):
    output = None
    connection = pymysql.connect( 
        host = secrets["host"],
        database = secrets["dbClusterIdentifier"],
        port = secrets["port"],
        user = secrets["username"],
        password = secrets["password"],
        connect_timeout = 300,
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(db_sql)
            if db_sql.strip().upper().startswith("SELECT") or db_sql.strip().upper().startswith("WITH"):
                output = cursor.fetchall()  # Return results for SELECT queries
            connection.commit()
            print('Query executed successfully')
            return output
    finally:
        connection.close()


def lambda_handler(event, context):
    logger.info('Inside handler')
    logger.info(f"Received event: {json.dumps(event)}")
    logger.info(f"Reeived context : {context}")
    aws_request_id = context.aws_request_id
    lambda_name = context.function_name
    initiate_folder = os.environ.get('initiate_folder')
    destination_folder = os.environ.get('Destination_folder')
    feeds_config_str = os.environ.get('feeds_config')  
    sns_topic_arn = os.environ.get('sns_topic_arn')
    db_secret_key = os.environ.get('db_secret_key')

    batch_id = None
    step_id = None
    failed_step_id = None
    status = ""
    provider = ""
    feed_type = ""
    step_function_info = ""
    step_function_execution_id = ""
    reprocess_flag = ""
    file_name_reprocess= ""

    try:
        status = "COMPLETED"

        # Extract details from the event body
        parsed_body = event.get('parsedBody', {})

        provider = parsed_body.get('provider')
        reprocess_flag = parsed_body.get('reprocess_flag')
        feed_type = parsed_body.get('feed_type')
        batch_id = parsed_body.get('batch_id')
        step_function_info = parsed_body.get('step_function_info')
        step_function_execution_id = parsed_body.get('step_function_execution_id')
        bucket_name = parsed_body.get('s3_bucket')
        file_name = parsed_body.get('file_name')
        failed_batch_id = parsed_body.get('failed_batch_id')
        is_execution=True
        is_reprocess_exec=False 
        reprocess_valid_list=['COMPLETED','COMPLETED_IN_REPROCESS']
        
        logger.info(f"db_secret_key => {db_secret_key}")

        logger.info(f"provider value: {provider}")
        logger.info(f"Bucket name: {bucket_name}")
        logger.info(f"AWS Request ID value: {aws_request_id}")
        logger.info(f"Lambda Name value: {lambda_name}")
        logger.info(f"Reprocess flag value: {reprocess_flag}")
        logger.info(f"failed_batch_id : {failed_batch_id}")
        logger.info(f"Batch ID value: {batch_id}")
        logger.info(f"feed type value: {feed_type}")
        logger.info(f"Inbound Feed File Name: {file_name}")
        
        s3_client = boto3.client('s3')

        secrets = get_secret(db_secret_key, 'us-east-1')
        #logger.info(f'secrets=>{secrets}')
        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        if batch_id:
            step_id = feeds_step_logging(secrets, batch_id, step_id, lambda_name, aws_request_id, 'IN PROGRESS', 'Lambda - Feeds Unzip Lambda')
            logger.info(f"step_id => {step_id}")
        
        else:
            raise ValueError(f"Batch ID {batch_id} is empty!!!")
        
        if reprocess_flag.upper().strip()=='N' and batch_id:
            dup_chk=f"""SELECT count(*) as cnt FROM txm_inbound_feeds_logs.batch_inbound_feeds_step_details st
                        INNER JOIN txm_inbound_feeds_logs.batch_inbound_feeds_master m ON m.batch_id=st.batch_id
                        WHERE m.batch_id={batch_id} AND st.step_id <> {step_id} AND st.job_name='{lambda_name}'""" 
            dup_chkstatus=run_sql_query(dup_chk, secrets)
            dup_chk_cnt= int(dup_chkstatus[0]['cnt'])
            if(dup_chk_cnt) > 0:
                is_execution=False
                raise Exception("Duplicate Batch!.Batch is already Processed.") 
        
        file_name_reprocess=''
        if reprocess_flag.upper().strip()=='Y' :
            validate_unzip_sql=f"""SELECT m.batch_id,st.status,st.step_id,m.file_name FROM txm_inbound_feeds_logs.batch_inbound_feeds_step_details st
                        INNER JOIN txm_inbound_feeds_logs.batch_inbound_feeds_master m ON m.batch_id=st.batch_id
                        WHERE m.batch_id={failed_batch_id} AND st.job_name='{lambda_name}'"""
            validation_unzip_status=run_sql_query(validate_unzip_sql, secrets)
            logger.info(f"validation_unzip_status  => {validation_unzip_status}")

            if not validate_unzip_sql:
                failed_step_id = feeds_step_logging(secrets, failed_batch_id, failed_step_id, lambda_name, aws_request_id, status, 'Lambda - Feeds Unzip Lambda')
                logger.info(f"Step record inserted with failed_step_id => {failed_step_id}")
                is_reprocess_exec=True

            else:
                file_name_reprocess=validation_unzip_status[0]['file_name']
                validation_status=validation_unzip_status[0]['status']
                failed_step_id=validation_unzip_status[0]['step_id']
                logger.info(f"file_name_reprocess  => {file_name_reprocess}")
                if feed_type == 'DELETED_INVOICE': 
                    if isinstance(validation_unzip_status, list):
                        if validation_status in reprocess_valid_list:
                            print(f"Batch=> {failed_batch_id} with file name =>{file_name_reprocess} is already processed succesfully in the last run. No further processing required, skipping rest of the flow!!!")
                            is_execution=False
                            status='SKIP_EXECUTION'
                            
                        else:
                            error_path = file_name.replace("inbound","error")
                            s3_input_files_prefix=f"{error_path}" 
                            destination_prefix=f"{file_name}"
                            s3_client = boto3.client("s3")
                            response=s3_client.copy_object(Bucket=bucket_name, Key=destination_prefix, CopySource={'Bucket': bucket_name, 'Key': s3_input_files_prefix})
                            logger.info("File copied successfully.")
                            logger.info(f"File Moved from Error to letters Folder=>{response}")
                            is_execution=True
                            is_reprocess_exec=True

        try:
            environment_map = json.loads(feeds_config_str)
            environment = environment_map.get('env')
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing feeds_config JSON: {e}")
            status = 'FAILED'
            error_info = 'Configuration Error'
            error_message = f'feeds_config environment variable is invalid JSON: {e}'
            snstopicnotification(batch_id, provider, feed_type, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
            feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Feeds Unzip Lambda')
            logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
            return {"statusCode": 500, "message": error_message}
        if is_execution:
            if not destination_folder:
                logger.error("Environment variable 'Destination_folder' is not set.")
                status = 'FAILED'
                error_info = 'Configuration Error'
                error_message = 'Destination_folder environment variable is missing.'
                snstopicnotification(batch_id, provider, feed_type, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
                feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Feeds Unzip Lambda')
                logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
                status_code = 500
                return return_response(error_info, error_message, provider, feed_type, bucket_name, file_name, step_function_info, reprocess_flag, batch_id, step_function_execution_id, status_code, aws_request_id, lambda_name)
            
            if not feeds_config_str:
                logger.error("Environment variable 'feeds_config' is not set.")
                status = 'FAILED'
                error_info = 'Configuration Error'
                error_message = 'feeds_config environment variable is missing.'
                snstopicnotification(batch_id, provider, feed_type, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
                feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Feeds Unzip Lambda')
                logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
                status_code = 500
                return return_response(error_info, error_message, provider, feed_type, bucket_name, file_name, step_function_info, reprocess_flag, batch_id, step_function_execution_id, status_code, aws_request_id, lambda_name)
            
            config_values = environment_map.get(feed_type.strip().upper())
            logger.info(f"config_values => {config_values}")
            if not config_values:
                status = 'FAILED'
                error_info = f'Configuration not found for trading partner: {provider}'
                error_message = f'feeds_config missing entry for {provider}'
                logger.error(error_message)
                snstopicnotification(batch_id, provider, feed_type, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
                feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Feeds Unzip Lambda')
                logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
                status_code = 500
                return return_response(error_info, error_message, provider, feed_type, bucket_name, file_name, step_function_info, reprocess_flag, batch_id, step_function_execution_id, status_code, aws_request_id, lambda_name)

            if feed_type == 'DELETED_INVOICE':
                logger.info(f'processing file: {file_name} for feed_type {feed_type}')
                logger.info(f'processing file: {file_name}')
                process_file_result = process_non_zip_file(batch_id, step_id, provider, feed_type, bucket_name, file_name, destination_folder, step_function_info, step_function_execution_id, environment, environment_map, config_values, reprocess_flag, sns_topic_arn, s3_client, secrets)
                logger.info(f"Unzipped file result for {file_name}: {process_file_result}")

            if process_file_result['statusCode'] == 500:
                status = 'FAILED'
                event['errorInfo'] = process_file_result.get('errorInfo', 'Error during file processing')
                event['errorMessage'] = process_file_result['message']
                return {
                    "statusCode": 500,
                    "requestId": aws_request_id,
                    "lambdaName": lambda_name,
                    "body": json.dumps(event)
                }
            # Continue processing other files even if one fails
            elif process_file_result['statusCode'] == 404:
                status = 'FAILED'
                event['errorInfo'] = 'File not found in S3'
                event['errorMessage'] = process_file_result['message']
                return {
                    "statusCode": 404,
                    "requestId": aws_request_id,
                    "lambdaName": lambda_name,
                    "body": json.dumps(event)
                }
            else:
                feeds_step_logging(secrets, batch_id, step_id, lambda_name, aws_request_id, status, 'Lambda - Feeds Unzip Lambda')
                logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
                s3_input_files_prefix =  process_file_result['message']
                if is_reprocess_exec:
                    logger.info(f"Reprocessing file is_reprocess_exec status: {is_reprocess_exec}")
                    status = "COMPLETED_IN_REPROCESS"
                    feeds_step_logging(secrets, failed_batch_id, failed_step_id, lambda_name, aws_request_id, status, 'Lambda - Feeds Unzip Lambda')

        else: 
            feeds_step_logging(secrets, batch_id, step_id, lambda_name, aws_request_id, status, 'Lambda - Feeds Unzip Lambda')
            # Success response
            s3_input_files_prefix=f'data/source/feeds/{provider}/{feed_type.lower()}/'

        return {
            "statusCode": 200,
            "requestId": aws_request_id,
            "lambdaName": lambda_name,
            "body": json.dumps({
                "message": 'All valid files processed successfully',
                "provider": provider,
                "feed_type": feed_type,
                "s3_bucket": bucket_name,
                "input_file_name": os.path.basename(file_name),
                "s3_input_files_prefix": s3_input_files_prefix, 
                "step_function_info": step_function_info,
                "reprocess_flag": reprocess_flag,
                "batch_id": batch_id,
                "failed_batch_id": failed_batch_id,
                "step_function_execution_id": step_function_execution_id
            })
        }

    except Exception as error:
        logger.error(f"Exception Error in handler: {error}", exc_info=True)
        status = 'FAILED'
        event['error_info'] = 'Error processing the uploaded file'
        event['error_message'] = str(error)
        snstopicnotification(batch_id, provider, feed_type, step_function_info, step_function_execution_id, event['error_info'], event['error_message'], file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        if batch_id:
            feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS UNZIP LAMBDA FAILED', event['error_info'], event['error_message'], 'Lambda - Feeds Unzip Lambda')
            logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
        return {
            "statusCode": 500,
            "requestId": aws_request_id,
            "lambdaName": lambda_name,
            "body": json.dumps(event)
        }