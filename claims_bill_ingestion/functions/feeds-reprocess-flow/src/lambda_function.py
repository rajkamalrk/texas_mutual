
import json
import boto3
import os
import dateutil.tz
from datetime import datetime
import base64
from botocore.exceptions import ClientError
import time
import pymysql

def get_secret(secret_name, region_name):
    """
    Retrives secrets from AWS secrets Manager,
    Raises exceptions for any errors encountered during the retrieval process.
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
    print(f"Successfully updated batch_id {batch_id} to status {status}.")

def snstopicnotification(batch_id, provider, feed_type, error_info, error_message, file_name, env_profile, reprocess_flag, severity, sns_topic_arn):
    print(f"Attempting to send SNS notification to {sns_topic_arn}")
    #cst = pytz.timezone('America/Chicago')
    timestamp = datetime.now().strftime('%m/%d/%Y %H:%M:%S%Z')

    try:
        sns_client = boto3.client('sns')
        message_subject = f"{env_profile} - Reprocess - {provider} - {feed_type} file is Unsupported - {severity}"
        message_body = (
            f"Process:{provider} - {feed_type} file \n"
            f"Batch ID: {batch_id}\n"
            f"File Name: {file_name}\n"
            f"Timestamp: {timestamp}\n"
            f"Environment: {env_profile}\n" 
            f"Reprocess Flag: {reprocess_flag}\n" 
            f"Error Info: {error_info}\n"
            f"Fields & Content from Error log : {error_message[:500]}"
        )
        print(message_body)
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=message_subject,
            Message=message_body
        )
        print(f"Successfully sent SNS notification for {file_name}")
    except Exception as e:
        print(f"Error sending SNS notification for {file_name}: {e}", exc_info=True)

def create_feeds_batch(secrets, feed_type, provider, file_name, file_path, create_update_user):
    """
    Create a new batch record in the database for feeds processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        feed_type (str): Type of feed being processed
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
        INSERT INTO txm_inbound_feeds_logs.batch_inbound_feeds_master 
            (feed_type, provider, file_name, file_path, start_datetime, status, create_user, create_timestamp, update_user, update_timestamp) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_values = (
        feed_type,
        provider,
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
                print(f"Number of rows inserted: 1")
            elif isinstance(insert_values, list) and len(insert_values) > 0:
                # Multiple rows insert
                cursor.executemany(insert_sql, insert_values)
                auto_increment_id = cursor.lastrowid
                print(f"Number of rows inserted: {cursor.rowcount}")
            else:
                raise ValueError("insert_values must be a tuple or non-empty list of tuples")

            # Commit the transaction
            connection.commit()
    except Exception as e:
        print(f"Insertion Failed in DB: {str(e)}")
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
            print(f"Number of rows updated: {cursor.rowcount}")

            # Commit the transaction
            connection.commit()
    except Exception as e:
        print(f"Update Failed in DB: {str(e)}")
        raise e
    finally:
        connection.close()

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
        print(f"Successfully updated step status {status} for step_id: {step_id}")

    else:
        insert_sql = f"""INSERT INTO txm_inbound_feeds_logs.batch_inbound_feeds_step_details 
                            (batch_id, job_type, job_name, job_id, status, start_datetime, end_datetime, create_user, create_timestamp, update_user, update_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        insert_values = (batch_id, 'LAMBDA', job_name, job_id, status, v_load_timestamp, None, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

        step_id = run_insert_command(secrets, insert_sql, insert_values)
        print(f"Successfully inserted log data with step_id: {step_id}")

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
    print(f"Successfully inserted log data with error_id: {error_id}")

    feeds_step_logging(secrets, batch_id, step_id, None, None, status, create_update_user)

    update_feeds_batch_status(secrets, batch_id, status, create_update_user)

def lambda_handler(event, context):

    env_profile = ""
    sqs_queue_url = ""
    step_function_arn = ""
    sns_topic_arn = ""
    db_secret_key = ""
    aws_request_id = ""
    lambda_name = ""
    provider = "mitchell"
    feed_type = ""
    batch_id = None
    step_id = None
    status = ""
    failed_batch_id = -1
    file_path = None
    file_name = ""
    reprocess_valid_list=['COMPLETED','COMPLETED_IN_REPROCESS']
    try:
        print("Event => ", event)

        reprocess_flag = "Y"
        env_profile = os.environ['env_profile']
        print(f'env_profile => {env_profile}')
        sqs_queue_url = os.environ['sqs_queue_url']
        print(f'sqs_queue_url => {sqs_queue_url}')
        sns_topic_arn = os.environ['sns_topic_arn']
        print(f'sns_topic_arn => {sns_topic_arn}')
        db_secret_key = os.environ['db_secret_key']
        print(f"db_secret_key => {db_secret_key}")
        s3_bucket = os.environ['s3_bucket']
        print(f"s3_bucket => {s3_bucket}")
        step_function_arn = os.environ['step_function_arn']
        print(f"step_function_arn => {step_function_arn}")
        glue_job_name = os.environ['glue_job_name']
        print(f"glue_job_name => {glue_job_name}")
        aws_request_id = context.aws_request_id
        print(f"Lambda Request ID: {aws_request_id}")
        lambda_name = context.function_name
        print(f"Lambda Function Name: {lambda_name}")
        secrets = get_secret(db_secret_key, 'us-east-1')
        event_records = []
        # Check if 'Records' is inside a list
        if isinstance(event, list):
            for record in event:
                event_records.append(record)
        elif isinstance(event, dict) and 'Records' in event:
            for record in event['Records']:
                event_records.append(record)
        else:
            print("Unexpected event format:", event)

        for record in event_records:
            status = "COMPLETED"
            # Get the message body
            message_body = record['body']
            
            # Process the message (this is where you would add your custom processing logic)
            print(f"Message Body: {message_body}")

            message = json.loads(message_body)
            print(f"Parsed Message: {message}")
            
            # Extract required fields
            failed_batch_id = message.get('failed_batch_id')
            print(f"failed_batch_id: '{failed_batch_id}'")
            feed_type = message.get('feed_type') 
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            batch_id = create_feeds_batch(secrets, feed_type, provider, file_name, file_path, 'Lambda-Invoke feeds-Reprocess')
            print(f"batch_id => {batch_id}")
            step_id = feeds_step_logging(secrets, batch_id, step_id, lambda_name, aws_request_id, 'IN PROGRESS', 'Lambda-Invoke feeds-Reprocess')
            print(f"step_id => {step_id}")

            if failed_batch_id is not None:
                #Validate the status of Previous Batch
                validate_sql=f"""SELECT batch_id,reprocess_flag,file_name,status,provider,file_path
                                FROM txm_inbound_feeds_logs.batch_inbound_feeds_master 
                                WHERE batch_id='{failed_batch_id}'"""
                validation_repro_status=run_sql_query(validate_sql, secrets)
                print(f"validation_status => {validation_repro_status}")
                if validation_repro_status:
                    if isinstance(validation_repro_status, list):
                        provider = validation_repro_status[0]['provider']
                        file_path = validation_repro_status[0]['file_path']
                        file_name = validation_repro_status[0]['file_name']
                        validation_status=validation_repro_status[0]['status']
                        reprocess_flag = validation_repro_status[0]['reprocess_flag']
                        if validation_status in reprocess_valid_list and reprocess_flag == 'N':
                            print(f"Batch=> {failed_batch_id} with file name =>{file_name} is already processed succesfully in the last run.Terminating Reprocess!")
                            raise Exception(f"Batch=> {failed_batch_id} with file name =>{file_name} is already processed succesfully in the last run.Terminating Reprocess!")
                        else:
                            print(f"Batch=> {failed_batch_id} with file name =>{file_name} is not processed succesfully in the last run,initiating re-process!") 
                            if reprocess_flag == 'N':
                                sf_client = boto3.client('stepfunctions')
                                sf_payload = {
                                            'provider' : provider,
                                            'feed_type' : feed_type,
                                            's3_bucket': s3_bucket,
                                            'reprocess_flag': 'Y',
                                            'batch_id': batch_id,
                                            'file_name':f"{file_path}{file_name}",
                                            'failed_batch_id':failed_batch_id
                                            }
                                print(f'sf_payload==>{sf_payload}')            
                                invoke_sf_response = sf_client.start_execution(stateMachineArn=step_function_arn,input=json.dumps(sf_payload))
                                print(f"Step Function Invoke Response : {invoke_sf_response}")
                                create_timestamp= v_load_timestamp
                                update_timestamp= v_load_timestamp
                                update_sql = f""" UPDATE txm_inbound_feeds_logs.batch_inbound_feeds_master
                                                SET failed_batch_id = %s,
                                                    provider = %s,
                                                    reprocess_flag= %s, 
                                                    file_name = %s,
                                                    file_path = %s,
                                                    data_pipeline_id= %s, 
                                                    update_user = %s, 
                                                    update_timestamp = %s
                                                WHERE batch_id = %s"""
                                update_values = (failed_batch_id,provider,'Y',file_name,file_path,invoke_sf_response['executionArn'],'Lambda-Invoke feeds-Reprocess',update_timestamp,batch_id)
                                print(f"[SUCCESS] update_sql => {update_sql}")
                                run_update_command(secrets, update_sql, update_values)
                                print(f"[SUCCESS] Successfully updated batch_status_id {batch_id} to SUCCESS.")
                                feeds_step_logging(secrets, batch_id, step_id, lambda_name, aws_request_id, status, 'Lambda-Invoke feeds-Reprocess')
                                print(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
                            else:
                                print(f"reprocess_flag = 'Y', Re Process flag should be N. Stoping future re processing...")
                                raise Exception(f"Batch=> {failed_batch_id} with reprocess_flag = 'Y', Re Process flag should be N.")

                else:
                    raise Exception(f"Invalid failed_batch_id=> {failed_batch_id} !")
            else:
                print(f'Input Batch ID is empty. Invoking the {feed_type} load glue job')
                if feed_type == "DELETED_INVOICE":
                    provider = 'mitchell'
                    glue_payload = {
                                '--provider' : provider,
                                '--batch_id' : str(batch_id),
                                '--feed_type' : feed_type,
                                '--reprocess_flag': 'Y',
                                }
                    print(f'glue_payload==>{glue_payload}') 
                    glue_client = boto3.client('glue')
                    glue_response = glue_client.start_job_run(
                        JobName=glue_job_name,
                        Arguments=glue_payload
                    )
                    print(f"Glue job started successfully. RunId: {glue_response['JobRunId']}")
                    update_sql = f""" UPDATE txm_inbound_feeds_logs.batch_inbound_feeds_master
                                        SET failed_batch_id = %s,
                                            provider = %s,
                                            reprocess_flag= %s, 
                                            update_user = %s, 
                                            update_timestamp = %s
                                    WHERE batch_id = %s"""
                    update_values = (failed_batch_id,provider,'Y','Lambda-Invoke feeds-Reprocess',v_load_timestamp,batch_id)
                    print(f"[SUCCESS] update_sql => {update_sql}")
                    run_update_command(secrets, update_sql, update_values)
                    print(f"[SUCCESS] Successfully updated batch_status_id {batch_id} to SUCCESS.")
                else:
                    raise Exception(f"Feed Type is invalid => {feed_type} !")

    except Exception as e:
        print(f"Exception Block Error dump: {str(e)}")
        event['error_info'] = 'Error processing the uploaded file'
        event['error_message'] = str(e)
        status = "FAILED"
        severity="FATAL"
        error_info="Unable to invoke Reprocess Step Function"
        error_message=str(e)
        snstopicnotification(batch_id, provider, feed_type, event['error_info'], event['error_message'], file_path, env_profile, reprocess_flag, severity, sns_topic_arn)
        if batch_id:
            feeds_error_logging(secrets, batch_id, step_id, status, 'INVOKE FEEDS FAILED', error_info, error_message, 'Lambda - Invoke Feeds Inbound')
            print(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
        raise e
    finally:
        print(f"finally Block")
        sqs_client = boto3.client('sqs')
        receipt_handle = record['receiptHandle'] 
        print(f"receipt_handle => {receipt_handle}")
        delete_sqs_response = sqs_client.delete_message(QueueUrl= sqs_queue_url,ReceiptHandle=receipt_handle)
        print(f'delete_sqs_response => {delete_sqs_response}')
