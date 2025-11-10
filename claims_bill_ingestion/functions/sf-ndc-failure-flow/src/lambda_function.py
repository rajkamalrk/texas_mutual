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
            cursor.execute(db_sql)
            if db_sql.strip().upper().startswith("SELECT"):
                return cursor.fetchall()  # Return results for SELECT queries
            connection.commit()
    finally:
        connection.close()

def log_and_email(env_profile,provider,reprocess_flag,batch_id,data_pipeline_name,data_pipeline_id,error_info,error_message,severity,sns_topic_arn):
    """logging error and sending error email"""
    print("Inside log email method")
    sns_client=boto3.client('sns',region_name='us-east-1')
    cst=dateutil.tz.gettz('America/Chicago')
    timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
    provider = provider.upper()
    #create log message
    process=f"Process : {provider} - Failure Flow Lambda"
    errInfo=f"Error  : {error_info}"
    reprocessflag=f"Reprocess Flag  : {reprocess_flag}"
    batchid=f"Step Function Batch ID: {batch_id}"
    stepFuncName=f"Step Function Name: {data_pipeline_name}"
    stepFuncId=f"Step Function ID: {data_pipeline_id}"
    time=f"Timestamp : {timestamp}"
    errLog=f"Fields & Content from Error log : {error_message}"
    message_body=f"{process}\n{errInfo}\n{reprocessflag}\n{batchid}\n{stepFuncName}\n{stepFuncId}\n{time}\n{errLog}"
    print(message_body)
    subject=f"{env_profile} - {provider} - {error_info} -{severity}"
    
    response=sns_client.publish(
       TopicArn=sns_topic_arn,
       Subject=subject,
       Message=message_body
       )
    return response
   

def log_ndc_failure(batch_id,error_job_type,job_name, errorInfo,job_id,error_code, errorMessage, secrets):
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    end_datetime = v_load_timestamp
    # Update batch status to 'FAILED'
    update_sql = f"""
        UPDATE txm_ndc_logs.batch_ndc_master
        SET status = 'FAILED',
			end_datetime= '{v_load_timestamp}',
            update_user = 'Lambda - Failure Flow',
            update_timestamp = '{end_datetime}'
        WHERE batch_id = '{batch_id}'
    """
    print(f"[FAILED] update_sql => {update_sql}")
    run_sql_query(update_sql, secrets)
    print(f"[FAILED] Successfully updated batch_status_id {batch_id} to FAILED.")	
	
	# Insert  step details
    insert_step_sql = f"""
        INSERT INTO txm_ndc_logs.batch_ndc_step_details (
            batch_id,job_type, job_name, job_id, status,end_datetime,
            create_user, create_timestamp, update_user, update_timestamp
        ) VALUES (
            '{batch_id}', '{error_job_type}','{job_name}','{job_id}','FAILED','{end_datetime}',
            'Lambda - Failure Flow', '{v_load_timestamp}', 'Lambda - Failure Flow', '{v_load_timestamp}'
        )
    """
    print(f"[FAILURE] insert_error_sql => {insert_step_sql}")
    run_sql_query(insert_step_sql, secrets)
    print(f"[FAILURE] Logged failure for batch_status_id {batch_id}")
	
	# Retrieving_step_id 
    run_db_sql = f"""SELECT step_id FROM txm_ndc_logs.batch_ndc_step_details WHERE batch_id = '{batch_id}' AND end_datetime = '{end_datetime}'"""
    print(f"run_db_sql=>{run_db_sql}")
    step_id=run_sql_query(run_db_sql, secrets)
    step_id=step_id[0][0]
    print(f'step_id=>{step_id}')
    
    error_info_db = escape_sql_string(errorInfo)
    error_message_db = escape_sql_string(errorMessage )

    # Insert error details
    insert_error_sql = f"""
        INSERT INTO txm_ndc_logs.batch_ndc_error_details (
            batch_id,step_id, error_code, error_info, error_message,
            create_user, create_timestamp, update_user, update_timestamp
        ) VALUES (
            '{batch_id}', '{step_id}','{error_code}','{error_info_db}','{error_message_db}', 
            'Lambda - Failure Flow', '{v_load_timestamp}', 'Lambda - Failure Flow', '{v_load_timestamp}'
        )
    """
    print(f"[FAILURE] insert_error_sql => {insert_error_sql}")
    run_sql_query(insert_error_sql, secrets)
    print(f"[FAILURE] Logged failure for batch_status_id {batch_id}")

def escape_sql_string(input_string):
    """
    Escapes single quotes in a string by replacing them with double single quotes.
    This is necessary for proper SQL query construction.
    """
    if isinstance(input_string, str):
        return input_string.replace("'", "''")
    elif isinstance(input_string, dict):
        # If input is a dictionary, serialize it and escape single quotes in the JSON string
        return json.dumps(input_string).replace("'", "''")
    elif input_string is None:
        return "NULL"
    else:
        return str(input_string)

def lambda_handler(event, context):
    print("Processing STARTED...")
    print("event", event)
    
    # List to hold extracted values from the messages
    result = dict()

    error_job_type = ""
    status_code = 500
    status = "FAILED"
    provider = ""
    reprocess_flag = ""
    batch_id = ""
    error_info_subj=""

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

    # If the message is in JSON format, you can parse it
    try:
        if isinstance(event, dict):
            secrets = get_secret(db_secret_key, 'us-east-1')

            # Get the payload
            if 'parsedBody' in event:
                payload = event['parsedBody']
            elif 'body' in event:
                # Lambda only Flow
                event_body = json.loads(event['body'])
                if 'parsedBody' in event_body:
                    payload = event_body['parsedBody']
                else:
                    payload = event_body
            
            # Process the message (this is where you would add your custom processing logic)
            print(f"Payload: {payload}")

            # Extract required fields
            # file_header_id = payload.get('file_header_id')
            provider = payload.get('provider')
            reprocess_flag = payload.get('reprocess_flag')
            data_pipeline_name = payload.get('step_function_info')
            data_pipeline_id = payload.get('step_function_execution_id')
            batch_id = payload.get('batch_id')

            if 'GlueJobInput' in event:
                print(f"Extract for Glue Job Details")
                error_job_type = "GLUE"
                # Get the parsed body
                glue_job_details = event['GlueJobInput']
                
                # Process the message (this is where you would add your custom processing logic)
                print(f"Glue Job Input: {glue_job_details}")

                # Extract required fields
                job_id = ""
                job_name = glue_job_details.get('JobName')
                print(f'job_name=>{job_name}')
                if not data_pipeline_name or not data_pipeline_id:
                    glue_arguments = glue_job_details.get('Arguments')
                    data_pipeline_name = glue_arguments.get('--step_function_info')
                    data_pipeline_id = glue_arguments.get('--step_function_execution_id')
                    print(f'data_pipeline_name=>{data_pipeline_name}')
                    print(f'data_pipeline_id=>{data_pipeline_id}')
                error_code = "GLUE FAILED"
            else:
                print(f"Extract for Lambda Details")
                error_job_type = "LAMBDA"
                # Extract required fields
                job_id = event.get('requestId')
                job_name = event.get('lambdaName')
                error_code = "LAMBDA FAILED"
                print(f'job_id=>{job_id}')
                print(f'job_name=>{job_name}')

            # Get the Error Details
            if 'error' in event:
                job_error = event['error']
                try:
                    job_error_details = json.loads(job_error.get('Cause'))
                
                    # Process the message (this is where you would add your custom processing logic)
                    print(f"job_error_details: {job_error_details}")
                    print(f"error_job_type: {error_job_type}")
                    if error_job_type == "GLUE":
                        # Extract required fields
                        job_id = job_error_details.get('Id')
                        print(f"job_id: {job_id}")
                        #status = job_error_details.get('JobRunState', "FAILED")
                        error_info = job_error_details.get('ErrorMessage')
                        print(f"error_info: {error_info}")
                        error_message = job_error
                    else:
                        # Extract required fields
                        job_id = job_error_details.get('requestId')
                        #status = "FAILED"
                        error_info = job_error_details.get('errorMessage')
                        error_message = job_error
                except Exception as e:
                    print(f"Error decoding JSON: {str(e)}")
                    # Extract required fields
                    #status = "FAILED"
                    error_info = job_error.get('Error')
                    error_message = job_error
            else:
                if 'errorInfo' in event_body:
                    error_info = event_body.get('errorInfo')
                    print(f'error_info=>{error_info}')
                else:
                    error_info = payload.get('errorInfo','Failed in job Execution')
                    print(f'error_info=>{error_info}')
                
                if 'errorMessage' in event_body:
                    error_message = event_body.get('errorMessage')
                    print(f'errorMessage=>{error_message}')
                else:
                    error_message = payload.get('errorMessage')
                    print(f'errorMessage=>{errorMessage}')
        
            
            # Capture error details in error log table
            #updated log table
            print('Starting all log tables .....')
            log_ndc_failure(batch_id,error_job_type,job_name, error_info,job_id,error_code, error_message, secrets)
            print('updated all log tables sucessfully.....')

            #Trigger Error notification
            severity="FATAL"
            error_info_subj = f"Failed in execution of Step Function - {data_pipeline_name}"
            response=log_and_email(env_profile,provider,reprocess_flag,batch_id,data_pipeline_name,data_pipeline_id,error_info_subj,json.dumps(error_message),severity,sns_topic_arn)
            print(f"Email sent successfully!message_id:{response['MessageId']}")
            
            # Return the extracted values
            result = {
                'provider' : provider,
                'reprocess_flag': reprocess_flag,
                'batch_id': batch_id,
                'job_type': error_job_type,
                'job_id': job_id,
                'job_name': job_name,
                'status': status,
                'error_info': error_info,
                'error_message': error_message
            }
            print(f"result => {result}")

            
            
        else:
            print("Unexpected event format:", event)
            raise Exception("Unexpected event format:")
        
    except json.JSONDecodeError as e:
        print(f"Message is not in JSON format. {str(e)}")
        raise e
    except Exception as e:
        print(f"Exception Block Error dump: {str(e)}")
        severity="FATAL"
        errorInfo="Unable to process Failure Flow"
        errorMessage=str(e)[:500]
        response=log_and_email(env_profile,provider,reprocess_flag,batch_id,data_pipeline_name,data_pipeline_id,error_info_subj,json.dumps(error_message),severity,sns_topic_arn)
        print(f"Email sent successfully!message_id:{response['MessageId']}")
        if batch_id:
            #updated log table
            log_ndc_failure(batch_id,error_job_type,job_name, errorInfo,job_id,error_code, errorMessage, secrets)

        raise e

    return {
        'statusCode': status_code,
        'body': result
    }