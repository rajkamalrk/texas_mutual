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
            connection.commit()
    finally:
        connection.close()

def call_db_stored_procedure(secrets, db_stored_procedure, db_sp_params, return_out_param_position):
    """
    Executes MySQL Stored Procedure using the provided secret key.
    """
    conn = pymysql.connect( 
        host = secrets["host"],
        database = secrets["dbClusterIdentifier"],
        port = secrets["port"],
        user = secrets["username"],
        password = secrets["password"],
        connect_timeout = 300
    )
    try:
        cursor = conn.cursor()
        out_param_value = None
        
        print(f"Calling DB Stored Procedure {db_stored_procedure}...")
        print(f"db_sp_params => {db_sp_params}")
        #cursor.execute('SET SESSION max_execution_time = 3600')
        cursor.execute('SET LOCAL net_read_timeout = 300')
        #cursor.execute('SET LOCAL net_read_timeout = 31536000')
        #cursor.execute('SET LOCAL wait_timeout = 28800')
        #cursor.execute('SET LOCAL interactive_timeout = 28800')
        cur_res = cursor.callproc(db_stored_procedure, db_sp_params)
        print(f"cur_res => {cur_res}")
        print(f"Stored Procedure Executed!!!")
        print(f"Stored Procedure execution output:")
        print(cursor.fetchall()) ###First resultset
        while cursor.nextset(): ###Remaining resultset
            for res in cursor.fetchall():
                print(res)
                if any(str.startswith('ProcedureExecutionError:') for str in res):
                    raise Exception(f"Error while executing Stored Procedure {db_stored_procedure}!!!")
        if return_out_param_position:
            out_param_query = f'SELECT @_{db_stored_procedure}_{return_out_param_position}'
            print(f"out_param_query => {out_param_query}")
            cur_res = cursor.execute(out_param_query)
            print(f"cur_res => {cur_res}")
            out_res = cursor.fetchone() ###First resultset
            print(f"out_res => {out_res}")
            if out_res:
                out_param_value = out_res[0]
        print(f"out_param_value => {out_param_value}")
        conn.commit()
    except Exception as e:
        print("Error executing DB Stored Procedure: %s", str(e))
        raise e
    finally:
        conn.close()
    return out_param_value  

def log_and_email(env_profile,source,trading_partner,file_header_id,reprocess_flag,sf_batch_id,data_pipeline_name,data_pipeline_id,error_info,error_message,severity,sns_topic_arn):
    """logging error and sending error email"""
    print("Inside log email method")
    sns_client=boto3.client('sns',region_name='us-east-1')
    cst=dateutil.tz.gettz('America/Chicago')
    timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
    if trading_partner:
       source_tp_str = trading_partner
    else:
       source_tp_str = source
    source_tp_str = source_tp_str.upper()
    #create log message
    process=f"Process : {source_tp_str} - Failure Flow Lambda"
    errInfo=f"Error  : {error_info}"
    fileid=f"File Header ID (TxM Staging) : {file_header_id}"
    reprocessflag=f"Reprocess Flag  : {reprocess_flag}"
    batchid=f"Step Function Batch ID: {sf_batch_id}"
    stepFuncName=f"Step Function Name: {data_pipeline_name}"
    stepFuncId=f"Step Function ID: {data_pipeline_id}"
    time=f"Timestamp : {timestamp}"
    errLog=f"Fields & Content from Error log : {error_message}"
    message_body=f"{process}\n{errInfo}\n{fileid}\n{reprocessflag}\n{batchid}\n{stepFuncName}\n{stepFuncId}\n{time}\n{errLog}"
    print(message_body)
    subject=f"{env_profile} - {source_tp_str} - {error_info} -{severity}"
    
    response=sns_client.publish(
       TopicArn=sns_topic_arn,
       Subject=subject,
       Message=message_body
       )
    return response

def lambda_handler(event, context):
    print("Processing STARTED...")
    print("event", event)
    
    # List to hold extracted values from the messages
    result = dict()

    error_job_type = ""

    status_code = 500
    status = "FAILED"
    file_header_id = ""
    source = ""
    trading_partner = ""
    reprocess_flag = ""
    sf_batch_id = ""
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
            file_header_id = payload.get('file_header_id')
            source = payload.get('source')
            trading_partner = payload.get('trading_partner')
            reprocess_flag = payload.get('reprocess_flag')
            data_pipeline_name = payload.get('step_function_info')
            data_pipeline_id = payload.get('step_function_execution_id')
            sf_batch_id = payload.get('sf_batch_id')

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
                if not data_pipeline_name or not data_pipeline_id:
                    glue_arguments = glue_job_details.get('Arguments')
                    data_pipeline_name = glue_arguments.get('--step_function_info')
                    data_pipeline_id = glue_arguments.get('--step_function_execution_id')
                error_code = "GLUE FAILED"
            else:
                print(f"Extract for Lambda Details")
                error_job_type = "LAMBDA"
                # Extract required fields
                job_id = event.get('requestId')
                job_name = event.get('lambdaName')
                error_code = "LAMBDA FAILED"

            # Get the Error Details
            if 'error' in event:
                job_error = event['error']
                try:
                    job_error_details = json.loads(job_error.get('Cause'))
                
                    # Process the message (this is where you would add your custom processing logic)
                    print(f"job_error_details: {job_error_details}")
                    
                    if error_job_type == "GLUE":
                        # Extract required fields
                        job_id = job_error_details.get('Id')
                        #status = job_error_details.get('JobRunState', "FAILED")
                        error_info = job_error_details.get('ErrorMessage')
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
                else:
                    error_info = payload.get('errorInfo','Failed in job Execution')
                
                if 'errorMessage' in event_body:
                    error_message = event_body.get('errorMessage')
                else:
                    error_message = payload.get('errorMessage')
        
            
            # Capture error details in error log table
            #updated log table
            db_sp_params = sf_batch_id, error_job_type, job_name, job_id, json.dumps(payload), status, error_code, error_info, json.dumps(error_message), 'Lambda - Failure Flow', 'Lambda - Failure Flow'
            print(f"db_sp_params => {db_sp_params}")
            dbStoredProcedure = "txm_bitx_logs.update_step_function_failure_log_status"
            print(f"dbStoredProcedure => {dbStoredProcedure}")
            res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
            print(f"res => {res}")

            #Trigger Error notification
            severity="FATAL"
            error_info_subj = f"Failed in execution of Step Function - {data_pipeline_name}"
            response=log_and_email(env_profile,source,trading_partner,file_header_id,reprocess_flag,sf_batch_id,data_pipeline_name,data_pipeline_id,error_info_subj,json.dumps(error_message),severity,sns_topic_arn)
            print(f"Email sent successfully!message_id:{response['MessageId']}")
            
            # Return the extracted values
            result = {
                'file_header_id': file_header_id,
                'source' : source,
                'trading_partner': trading_partner,
                'reprocess_flag': reprocess_flag,
                'sf_batch_id': sf_batch_id,
                'job_type': error_job_type,
                'job_id': job_id,
                'job_name': job_name,
                'status': status,
                'error_info': error_info,
                'error_message': error_message
            }
            print(f"result => {result}")

            if reprocess_flag == 'Y' and trading_partner == 'jopari' and data_pipeline_name != 'ENRICHMENT':
                print(f"invoking enrichment step function")
                #updated log table - Step Function logs
                db_sp_params = sf_batch_id, None, None, None, None, None, None, None ,None, None, json.dumps(event), 'INVOKE ENRICHMENT', 'IN PROGRESS',  'Lambda - Invoke Enrichment'
                print(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
                print(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                print(f"res => {res}")

                payload = {
                    'source' : source,
                    'trading_partner': trading_partner,
                    'file_header_id': file_header_id,
                    'reprocess_flag': reprocess_flag,
                    'sf_batch_id': sf_batch_id
                }

                sf_client = boto3.client('stepfunctions')
                invoke_response = sf_client.start_execution(
                    stateMachineArn=step_function_arn,
                    input=json.dumps(payload)
                )
                print(f'invoke_response => {invoke_response}')
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
        response=log_and_email(env_profile,source,trading_partner,file_header_id,reprocess_flag,sf_batch_id,data_pipeline_name,data_pipeline_id,errorInfo,errorMessage,severity,sns_topic_arn)
        print(f"Email sent successfully!message_id:{response['MessageId']}")
        if sf_batch_id:
            #updated log table
            db_sp_params = sf_batch_id, 'LAMBDA', lambda_name, aws_request_id, json.dumps(payload), 'FAILED', 'FAILURE FLOW LAMBDA', errorInfo, errorMessage, 'Lambda - Failure Flow', 'Lambda - Failure Flow'
            print(f"db_sp_params => {db_sp_params}")
            dbStoredProcedure = "txm_bitx_logs.update_step_function_failure_log_status"
            print(f"dbStoredProcedure => {dbStoredProcedure}")
            res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
            print(f"res => {res}")
        raise e

    return {
        'statusCode': status_code,
        'body': result
    }