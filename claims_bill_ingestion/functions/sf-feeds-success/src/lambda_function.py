import os
import logging
import json
import dateutil.tz
import base64
import boto3
from botocore.exceptions import ClientError
import pymysql
from datetime import datetime


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

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

def log_and_email(env_profile, provider, feed_type, reprocess_flag, batch_id, data_pipeline_name, data_pipeline_id, error_info, error_message, severity, sns_topic_arn):
    """
    Log error and send notification email via SNS.
    
    Args:
        env_profile (str): Environment profile (DEV, QA, STG, PROD)
        provider (str): Provider name
        feed_type (str): Type of feed
        reprocess_flag (str): Reprocess flag value
        batch_id (int, optional): Batch ID associated with the error
        data_pipeline_name (str): Step Function Name
        data_pipeline_id (str): Step Function Execution ID
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
    message_subject = f"{env_profile} - {provider} - {feed_type} - {error_info} - {severity}"
    message_body = (
        f"Process : {provider}-{feed_type} - Step Function Success Lambda \n"
        f"Batch ID: {batch_id}\n"
        f"Step Function Name: {data_pipeline_name}\n"
        f"Step Function ID: {data_pipeline_id}\n"
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

def lambda_handler(event, context):
    print("Processing STARTED...")
    print("event", event)
    
    status_code = 200
    status = ""
    result_body = dict()
    batch_id = None
    step_id = None
    provider = ""
    feed_type = ""
    data_pipeline_name = ""
    data_pipeline_id = ""
    reprocess_flag= ""

    env_profile = os.environ['env_profile']
    print(f'env_profile => {env_profile}')
    db_secret_key = os.environ['db_secret_key']
    print(f"db_secret_key => {db_secret_key}")
    sns_topic_arn = os.environ['sns_topic_arn']
    print(f'sns_topic_arn => {sns_topic_arn}')
    aws_request_id = context.aws_request_id
    print(f"Lambda Request ID: {aws_request_id}")
    lambda_name = context.function_name
    print(f"Lambda Function Name: {lambda_name}")
    step_function_arn = os.environ['step_function_arn']
    print(f'step_function_arn => {step_function_arn}')

    try:

        if isinstance(event, dict):
            secrets = get_secret(db_secret_key, 'us-east-1')
            print(f'event=>{event}')
            # Get the payload
            if 'parsedBody' in event:
                payload=event['parsedBody']
                
                batch_id = payload.get('batch_id')
                print(f'batch_id=>{batch_id}')
                provider = payload.get('provider')
                print(f'provider=>{provider}')
                feed_type = payload.get('feed_type')
                print(f'feed_type=>{feed_type}')
                reprocess_flag = payload.get('reprocess_flag')
                print(f'reprocess_flag=>{reprocess_flag}')
                data_pipeline_name = payload.get('step_function_info')
                print(f'data_pipeline_name=>{data_pipeline_name}')
                data_pipeline_id = payload.get('step_function_execution_id')
                print(f'data_pipeline_id=>{data_pipeline_id}')   
                failed_batch_id = payload.get('failed_batch_id')  
                print(f'failed_batch_id=>{failed_batch_id}')  
            
            if batch_id:
                step_id = feeds_step_logging(secrets, batch_id, step_id, lambda_name, aws_request_id, 'IN PROGRESS', 'Lambda - Feeds Success')
                print(f"step_id => {step_id}")

                if reprocess_flag == 'Y':
                    status = "COMPLETED_IN_REPROCESS"
                    update_feeds_batch_status(secrets, failed_batch_id, status, 'Lambda - Feeds Success')
                    print(f"[SUCCESS] Successfully updated batch_status_id {batch_id} to status {status}.")

                status = "COMPLETED"
                update_feeds_batch_status(secrets, batch_id, status, 'Lambda - Feeds Success')
                print(f"[SUCCESS] Successfully updated batch_status_id {batch_id} to status {status}.")

                feeds_step_logging(secrets, batch_id, step_id, lambda_name, aws_request_id, status, 'Lambda - Feeds Success')
                print(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
            
            else:
                raise ValueError(f"Batch ID {batch_id} is empty!!!")


            result_body = {
                'batch_id': batch_id,
                'status': status,
                'Comment': "All Process Completed Sucessfully ....",
            }
            print(f"result_body => {result_body}")

        else:
            print("Unexpected event format:", event)
            raise Exception("Unexpected event format:")
            
    except json.JSONDecodeError as e:
        print(f"Message is not in JSON format. {str(e)}")
        raise e
    except Exception as error:
        logger.error(f"Exception Error in handler: {error}", exc_info=True)
        status_code = 500
        result_body = event
        result_body['error_info'] = 'Error in Success Flow'
        result_body['error_message'] = str(error)
        #Trigger Error notification
        severity="FATAL"
        response=log_and_email(env_profile,provider,feed_type,reprocess_flag,batch_id,data_pipeline_name,data_pipeline_id,result_body['error_info'],result_body['error_message'],severity,sns_topic_arn)
        print(f"Email sent successfully!message_id:{response['MessageId']}")
        if batch_id:
            feeds_error_logging(secrets, batch_id, step_id, status, 'FEEDS SUCCESS FLOW FAILED', result_body['error_info'], result_body['error_message'], 'Lambda - Feeds Success')
            print(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")

    return {
        'statusCode': status_code,
        "requestId": aws_request_id,
        "lambdaName": lambda_name,
        'body': json.dumps(result_body)
    }