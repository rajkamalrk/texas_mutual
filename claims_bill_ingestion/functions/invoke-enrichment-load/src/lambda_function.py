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

def log_and_email(env_profile,source,trading_partner,file_header_id,reprocess_flag,sf_batch_id,error_info,error_message,severity,sns_topic_arn):
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
    process=f"Process : {source_tp_str} - Invoke Enrichment Load Step Function"
    errInfo=f"Error  : {error_info}"
    fileid=f"File Header ID (TxM Staging) : {file_header_id}"
    reprocessflag=f"Reprocess Flag  : {reprocess_flag}"
    batchid=f"Step Function Batch ID: {sf_batch_id}"
    time=f"Timestamp : {timestamp}"
    errLog=f"Fields & Content from Error log : {error_message}"
    message_body=f"{process}\n{errInfo}\n{fileid}\n{reprocessflag}\n{batchid}\n{time}\n{errLog}"
    print(message_body)
    subject=f"{env_profile} - {source_tp_str} - {error_info} -{severity}"
    
    response=sns_client.publish(
       TopicArn=sns_topic_arn,
       Subject=subject,
       Message=message_body
       )
    return response

def lambda_handler(event, context):
    try:
        print("Event => ", event)

        result = event
        status_code = 200
        payload = dict()
        source = ""
        trading_partner = ""
        file_header_id = ""
        reprocess_flag = ""
        sf_batch_id = ""
        env_profile = os.environ['env_profile']
        print(f'env_profile => {env_profile}')
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
        
        # Extract required fields
        source = event['parsedBody'].get('source')
        trading_partner = event['parsedBody'].get('trading_partner')
        file_header_id = event['parsedBody'].get('file_header_id')
        reprocess_flag = event['parsedBody'].get('reprocess_flag', 'N').upper()  # Default to N if not present
        sf_batch_id = event['parsedBody'].get('sf_batch_id')

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

        #updated log table
        db_sp_params = sf_batch_id, None, None, None, None, None ,None, None, None, invoke_response['executionArn'], json.dumps(payload), 'ENRICHMENT', 'IN PROGRESS',  'Lambda - Invoke Enrichment'
        print(f"db_sp_params => {db_sp_params}")
        dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
        print(f"dbStoredProcedure => {dbStoredProcedure}")
        res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
        print(f"res => {res}")
        
    except json.JSONDecodeError as e:
        print(f"Message is not in JSON format. {str(e)}")
        raise e
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        status_code = 500
        result["errorMessage"] = str(e)
    
    return {
        'statusCode': status_code,
        'requestId': aws_request_id,
        "lambdaName": lambda_name,
        'body': json.dumps(result)
    }