import json
import boto3
import logging
import os
import dateutil.tz
import base64
from datetime import datetime
from botocore.exceptions import ClientError
import time
import pymysql

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
        print("Error retrieving secret '%s': %s", secret_name, str(e))
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response.get('SecretString', '')
    if not secret: 
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    return json.loads(secret)

def delete_s3_file(bucket_name, key):
    """
    Delete an S3 file from the specified bucket.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        key (str): S3 object key to delete
        
    Raises:
        Exception: If file deletion fails
    """
    try:
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket_name, key).delete()
    except Exception as e:
        print("Error deleting file: %s", str(e))
        raise e

def get_s3_objects(bucket, prefix, suffix):
    """
    Fetches all S3 objects matching the given prefix and suffix.
    
    :param bucket: Name of the S3 bucket.
    :param prefix: Prefix to filter S3 objects.
    :param suffix: Suffix to filter S3 objects.
    :return: List of S3 object keys that match the criteria.
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
    s3_files = []
    for response in response_iterator:
        if 'Contents' in response:
            for object_data in response['Contents']:
                key = object_data['Key']
                if not suffix or key.endswith(suffix):
                    s3_files.append(key)
        else:
            print(f"S3 response doesn't have Key Contents. response => {response}")
    if s3_files:
        print(f"List of files available for processing with prefix => {prefix} and suffix => {suffix}")
        print(f"s3_files => {s3_files}")
    else:
        print(f"No files available for processing with prefix => {prefix} and suffix => {suffix}")
    return s3_files

def move_s3_file(destination_prefix, bucket_name, source_prefix, s3_client=None):
    """
    Move a file from source to destination in S3.
    
    Args:
        destination_prefix (str): Destination S3 key
        bucket_name (str): S3 bucket name
        source_prefix (str): Source S3 key
        s3_client: Optional S3 client, will create one if not provided
        
    Returns:
        str: Success message
    """
    if s3_client is None:
        s3_client = boto3.client('s3')
    
    try:
        # Copy file to destination folder
        s3_client.copy_object(Bucket=bucket_name, Key=destination_prefix, CopySource={'Bucket': bucket_name, 'Key': source_prefix})
        logger.info("File copied successfully.")

        # Delete the original file
        s3_client.delete_object(Bucket=bucket_name, Key=source_prefix)
        logger.info("Original file deleted successfully.")
        return "File moved successfully"

    except Exception as e:
        logger.error(f"Error moving file: {e}")
        raise  

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

def create_preauth_batch(secrets, file_type, provider, file_name, file_path, create_update_user):
    """
    Create a new batch record in the database for preauth processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        file_type (str): Type of feed being processed
        provider (str): Provider name
        file_name (str, optional): File name
        file_path (str, optional): File prefix path
        create_update_user (str): User name for Create/Update
        
    Returns:
        int: Batch ID of the created record
    """

    v_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    create_timestamp = v_start_time
    update_timestamp = v_start_time

    insert_sql = """
        INSERT INTO txm_preauth_logs.batch_preauth_master 
            (provider, file_type, file_name, file_path, start_datetime, status, create_user, create_timestamp, update_user, update_timestamp) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_values = (
        provider,
        file_type,
        file_name, 
        file_path,
        v_start_time,
        'IN PROGRESS',
        create_update_user,
        create_timestamp,
        create_update_user,
        update_timestamp
    )
    batch_id = run_insert_command(secrets, insert_sql, insert_values)
    print(f"Successfully inserted log data with batch_id: {batch_id}")
    
    return batch_id

def update_preauth_batch_status(secrets, batch_id, status, create_update_user):
    """
    Update batch status in the database for preauth processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID to associate with the batch
        status (str): Status of the batch
        create_update_user (str): User name for Create/Update
    """

    update_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    update_sql = f"""UPDATE txm_preauth_logs.batch_preauth_master
                        SET status = %s,
                            update_user = %s,
                            update_timestamp = %s
                        WHERE batch_id = %s"""
    update_values = (status, create_update_user, update_timestamp, batch_id)

    run_update_command(secrets, update_sql, update_values)
    print(f"Successfully updated batch_id {batch_id} to status {status}.")

def preauth_step_logging(secrets, batch_id, step_id, job_name, job_id, status, create_update_user):
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
        update_sql = f"""UPDATE txm_preauth_logs.batch_preauth_step_details
                         SET status = %s,
                             end_datetime = %s,
                             update_user = %s,
                             update_timestamp = %s
                         WHERE batch_id = %s AND step_id = %s"""
        update_values = (status, v_load_timestamp, create_update_user, v_load_timestamp, batch_id, step_id)

        run_update_command(secrets, update_sql, update_values)
        logger.info(f"Successfully updated step status {status} for step_id: {step_id}")

    else:
        insert_sql = f"""INSERT INTO txm_preauth_logs.batch_preauth_step_details 
                            (batch_id, job_type, job_name, job_id, status, start_datetime, end_datetime, create_user, create_timestamp, update_user, update_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        insert_values = (batch_id, 'LAMBDA', job_name, job_id, status, v_load_timestamp, None, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

        step_id = run_insert_command(secrets, insert_sql, insert_values)
        logger.info(f"Successfully inserted log data with step_id: {step_id}")

        return step_id

def preauth_error_logging(secrets, batch_id, step_id, status, error_code, error_info, error_message, create_update_user):
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
    
    insert_sql = f"""INSERT INTO txm_preauth_logs.batch_preauth_error_details 
                        (batch_id, step_id, error_code, error_info, error_message, create_user, create_timestamp, update_user, update_timestamp)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    insert_values = (batch_id, step_id, error_code, error_info, error_message, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

    error_id = run_insert_command(secrets, insert_sql, insert_values)
    print(f"Successfully inserted log data with error_id: {error_id}")

    preauth_step_logging(secrets, batch_id, step_id, None, None, status, create_update_user)

    update_preauth_batch_status(secrets, batch_id, status, create_update_user)

def log_and_email(env_profile, provider, load_category, file_name, reprocess_flag, batch_id, error_info, error_message, severity, sns_topic_arn):
    """
    Log error and send notification email via SNS.
    
    Args:
        env_profile (str): Environment profile (DEV, QA, STG, PROD)
        provider (str): Provider name
        load_category (str): Category of load
        file_name (str): Name of the file being processed
        reprocess_flag (str): Reprocess flag value
        batch_id (int, optional): Batch ID associated with the error
        error_info (str): Brief error information
        error_message (str): Detailed error message
        severity (str): Error severity level
        sns_topic_arn (str): SNS topic ARN for notifications
        
    Returns:
        dict: SNS publish response
    """
    print("Inside log email method")
    sns_client=boto3.client('sns',region_name='us-east-1')
    cst=dateutil.tz.gettz('America/Chicago')
    timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
    provider = provider.upper()

    #create email message
    message_subject = f"{env_profile} - {provider} - {load_category} - {error_info} - {severity}"
    message_body = (
        f"Process : {provider}-{load_category} - Invoke PreAuth Load Step Function \n"
        f"Batch ID: {batch_id}\n"
        f"File Name: {file_name}\n"
        f"Timestamp: {timestamp}\n"
        f"Environment: {env_profile}\n" 
        f"Reprocess Flag: {reprocess_flag}\n" 
        f"Error Info: {error_info}\n"
        f"Fields & Content from Error log : {error_message[:500]}"
    )
    print(message_body)
    
    response=sns_client.publish(
       TopicArn=sns_topic_arn,
       Subject=message_subject,
       Message=message_body
       )
    return response

def delete_sqs_message (sqs_queue_url, receipt_handle):
    sqs_client = boto3.client('sqs')
    delete_response = sqs_client.delete_message(QueueUrl= sqs_queue_url,ReceiptHandle=receipt_handle)
    print(f'delete_response => {delete_response}')

def lambda_handler(event, context):

    # Initialize all variables that will be used in the except block with default values
    env_profile = ""
    sqs_queue_url = ""
    step_function_arn = ""
    sns_topic_arn = ""
    db_secret_key = ""
    aws_request_id = ""
    lambda_name = ""
    secrets = dict()
    provider = ""
    load_category = ""
    file_name = ""
    reprocess_flag = "N"
    batch_id = None
    step_id = None
    status = ""

    try:
        print("Event => ", event)

        payload = dict()
        env_profile = os.environ['env_profile']
        print(f'env_profile => {env_profile}')
        sqs_queue_url = os.environ['sqs_queue_url']
        print(f'sqs_queue_url => {sqs_queue_url}')
        step_function_arn = os.environ['step_function_arn']
        print(f'step_function_arn => {step_function_arn}')
        sns_topic_arn = os.environ['sns_topic_arn']
        print(f'sns_topic_arn => {sns_topic_arn}')
        db_secret_key = os.environ['db_secret_key']
        print(f"db_secret_key => {db_secret_key}")
        aws_request_id = context.aws_request_id
        print(f"Lambda Request ID: {aws_request_id}")
        lambda_name = context.function_name
        print(f"Lambda Function Name: {lambda_name}")

        secrets = get_secret(db_secret_key, 'us-east-1')
        #print(f'secrets=>{secrets}')
        
        # Extract SQS records from event
        event_records = []
        if isinstance(event, list):
            # Direct list of records
            for record in event:
                event_records.append(record)
        elif isinstance(event, dict) and 'Records' in event:
            # Records nested in 'Records' key
            for record in event['Records']:
                event_records.append(record)
        else:
            # Invalid event format
            error_message = f"Unexpected event format: {event}"
            print(f"error_message => {error_message}")
            raise Exception(error_message)

        for record in event_records:
            # Get the message body
            message_body = record['body']
            
            # Process the message (this is where you would add your custom processing logic)
            print(f"Message Body: {message_body}")

            receipt_handle = record['receiptHandle'] 
            print(f"receipt_handle => {receipt_handle}")

            message_body_json = json.loads(message_body)
            print(f"Parsed Message: {message_body_json}")

            # Extract S3 event records from message body
            messages = []
            if isinstance(message_body_json, list):
                # Direct list of S3 records
                for message in message_body_json:
                    messages.append(message)
            elif isinstance(message_body_json, dict) and 'Records' in message_body_json:
                # S3 records nested in 'Records' key
                for message in message_body_json['Records']:
                    messages.append(message)
            else:
                # Invalid message body format
                error_message = f"Unexpected message body format: {message_body_json}"
                print(f"error_message => {error_message}")
                print(f"Delete the message from SQS queue...")
                delete_sqs_message (sqs_queue_url, receipt_handle)
                raise Exception(error_message)
            
            for message in messages:
                status = "COMPLETED"

                # Extract S3 bucket and object information
                s3_bucket = message['s3']['bucket']['name']
                print(f"s3_bucket => {s3_bucket}")

                s3_object_key = message['s3']['object']['key']
                print(f"s3_object_key => {s3_object_key}")

                # Parse S3 object key to extract metadata
                folder_path_list = s3_object_key.split('/')
                print(f"folder_path_list => {folder_path_list}")

                print(f"s3_bucket => {s3_bucket}")
                print(f"s3_object_key => {s3_object_key}")

                load_category = folder_path_list[-2].upper()
                print(f"load_category => {load_category}")

                # Extract provider from folder structure
                provider = folder_path_list[-3].lower()
                print(f"provider => {provider}")

                # Extract file information
                file_name = folder_path_list[-1]
                print(f"file_name => {file_name}")

                file_name_wo_ext = file_name.rsplit('.', 1)[0]
                print(f"file_name_wo_ext => {file_name_wo_ext}")

                file_ext = file_name.split('.')[-1]
                print(f"file_ext => {file_ext}")
                
                # Extract folder prefix for additional context
                folder_prefix = s3_object_key.replace(file_name, "")
                print(f"folder_prefix => {folder_prefix}")

                batch_id = create_preauth_batch(secrets, load_category, provider, file_name, folder_prefix, 'Lambda - Invoke Preauth')
                print(f"batch_id => {batch_id}")

                step_id = preauth_step_logging(secrets, batch_id, step_id, lambda_name, aws_request_id, 'IN PROGRESS', 'Lambda - Invoke Preauth')
                print(f"step_id => {step_id}")

                res_s3_file_list = get_s3_objects(bucket = s3_bucket, prefix = folder_prefix + file_name_wo_ext, suffix = None)
                print(f"res_s3_file_list => {res_s3_file_list}")

                s3_file_list = [i for i in res_s3_file_list if i != s3_object_key]
                print(f"s3_file_list => {s3_file_list}")

                if s3_file_list:
                    file_name = s3_file_list[0]
                    print(f"file_name => {file_name}")

                    # Update Log Master with File Name
                    update_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    update_sql = f"""UPDATE txm_preauth_logs.batch_preauth_master
                                    SET file_name = %s,
                                        update_user = %s,
                                        update_timestamp = %s
                                    WHERE batch_id = %s"""
                    print(f"[SUCCESS] update_sql => {update_sql}")
                    update_values = (os.path.basename(file_name), 'Lambda - Invoke Preauth', update_timestamp, batch_id)

                    run_update_command(secrets, update_sql, update_values)
                    print(f"[SUCCESS] updated file_name => {file_name}")

                    ## Validate Duplicate Batch
                    read_db_sql = f"""
                    SELECT COUNT(*) AS file_processed_count
                    FROM txm_preauth_logs.batch_preauth_master b
                    WHERE b.file_type = '{load_category}' 
                    AND b.file_name = '{os.path.basename(file_name)}' 
                    AND b.batch_id <> {batch_id}
                    """
                    
                    # Look for any existing entries for input filename
                    query_res = run_sql_query(read_db_sql, secrets)
                    print(f'query_res => {query_res}')

                    if query_res:
                        validate_count = int(query_res[0]['file_processed_count'])
                        print(f'validate_count => {validate_count}')

                        if validate_count > 0:
                            error_message = f"The processing of {load_category} File failed as duplicate to already processed file with File Name: {file_name}"
                            print(f"error_message => {error_message}")
                            print(f"Delete the message from SQS queue...")
                            delete_sqs_message (sqs_queue_url, receipt_handle)
                            print(f"Deleting .completed file {s3_object_key}...")
                            delete_s3_file(bucket_name=s3_bucket, key=s3_object_key)
                            error_prefix=file_name.replace("/inbound/","/error/")
                            print(f"Move input file {file_name} to Error prefix {error_prefix}...")
                            response=move_s3_file(error_prefix, s3_bucket, file_name)
                            print(f"File Moved from to Error Folder=>{response}")
                            raise Exception(error_message)
                        else:
                            print(f"No duplicate entries found for Input file {file_name}...")
                else:
                    error_message = f"No file present in s3 bucket: {s3_bucket} and prefix: {folder_prefix + file_name_wo_ext}"
                    print(f"error_message => {error_message}")
                    print(f"Delete the message from SQS queue...")
                    delete_sqs_message (sqs_queue_url, receipt_handle)
                    print(f"Deleting .completed file {s3_object_key}...")
                    delete_s3_file(bucket_name=s3_bucket, key=s3_object_key)
                    raise Exception(error_message)

                payload = {
                    'provider' : provider,
                    'load_category' : load_category.lower(),
                    's3_bucket': s3_bucket,
                    'reprocess_flag': 'N',
                    'batch_id': batch_id,
                    'file_name': file_name,
                    'failed_batch_id': -1
                }

                print(f"payload => {payload}")

                sf_client = boto3.client('stepfunctions')
                invoke_response = sf_client.start_execution(
                    stateMachineArn=step_function_arn,
                    input=json.dumps(payload)
                )
                print(f'invoke_response => {invoke_response}')

                # Update Log Master with Step Function Execution ID
                update_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                update_sql = f"""UPDATE txm_preauth_logs.batch_preauth_master
                                 SET data_pipeline_id = %s,
                                     update_user = %s,
                                     update_timestamp = %s
                                 WHERE batch_id = %s"""
                print(f"[SUCCESS] update_sql => {update_sql}")
                update_values = (invoke_response['executionArn'], 'Lambda - Invoke Preauth', update_timestamp, batch_id)

                run_update_command(secrets, update_sql, update_values)
                print(f"[SUCCESS] updated data_pipeline_id => {invoke_response['executionArn']}")

                print(f"Deleting .completed file {s3_object_key}...")
                delete_s3_file(bucket_name=s3_bucket, key=s3_object_key)

                preauth_step_logging(secrets, batch_id, step_id, None, None, status, 'Lambda - Invoke Preauth')
                print(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")

                if len(messages) > 1:
                    # Delaying multiple batches together to avoid conflicts.
                    print("Delaying multiple batches together to avoid conflicts...Sleep for 15 seconds")
                    time.sleep(15)
            
            # Optionally, delete the message from the queue if processing is successful
            delete_sqs_message (sqs_queue_url, receipt_handle)
            
            if len(event_records) > 1:
                # Delaying multiple batches together to avoid conflicts.
                print("Delaying multiple batches together to avoid conflicts...Sleep for 15 seconds")
                time.sleep(15)
        
    except json.JSONDecodeError as e:
        print(f"Message is not in JSON format. {str(e)}")
        raise e
    except Exception as e:
        print(f"Exception Block Error dump: {str(e)}")
        status = "FAILED"
        severity="FATAL"
        error_info="Unable to invoke Staging Load Step Function"
        error_message=str(e)
        response=log_and_email(env_profile,provider,load_category,file_name,reprocess_flag,batch_id,error_info,error_message,severity,sns_topic_arn)
        print(f"Email sent successfully!message_id:{response['MessageId']}")
        if batch_id:
            preauth_error_logging(secrets, batch_id, step_id, status, 'INVOKE PREAUTH FAILED', error_info, error_message, 'Lambda - Invoke Preauth')
            print(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
        raise e