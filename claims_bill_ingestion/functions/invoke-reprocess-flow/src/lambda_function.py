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
    process=f"Process : {source_tp_str} - Invoke Reprocess Flow"
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

        source = ""
        trading_partner = ""
        file_header_id = None
        reprocess_flag = "Y"
        sf_batch_id = None
        is_skip_reprocess = False
        env_profile = os.environ['env_profile']
        print(f'env_profile => {env_profile}')
        sqs_queue_url = os.environ['sqs_queue_url']
        print(f'sqs_queue_url => {sqs_queue_url}')
        staging_step_function_arn = os.environ['staging_step_function_arn']
        print(f'staging_step_function_arn => {staging_step_function_arn}')
        enhanced_step_function_arn = os.environ['enhanced_step_function_arn']
        print(f'enhanced_step_function_arn => {enhanced_step_function_arn}')
        enrichment_step_function_arn = os.environ['enrichment_step_function_arn']
        print(f'enrichment_step_function_arn => {enrichment_step_function_arn}')
        sns_topic_arn = os.environ['sns_topic_arn']
        print(f'sns_topic_arn => {sns_topic_arn}')
        db_secret_key = os.environ['db_secret_key']
        print(f"db_secret_key => {db_secret_key}")
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
            # Get the message body
            message_body = record['body']
            
            # Process the message (this is where you would add your custom processing logic)
            print(f"Message Body: {message_body}")

            message = json.loads(message_body)
            print(f"Parsed Message: {message}")
            
            # Extract required fields
            source = message.get('source')
            trading_partner = message.get('trading_partner')

            db_sp_params = source, trading_partner, None, None, None, json.dumps(message), None, None, None ,None, None, None, 'INVOKE REPROCESS FLOW', reprocess_flag,  'Lambda - Invoke Reprocess Flow', 'Lambda - Invoke Reprocess Flow', 0
            print(f"db_sp_params => {db_sp_params}")
            dbStoredProcedure = "txm_bitx_logs.insert_step_function_logs"
            print(f"dbStoredProcedure => {dbStoredProcedure}")
            sf_batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 16)
            print(f"sf_batch_id => {sf_batch_id}")

            reprocess_query = f"""SELECT sf.sf_batch_id, status_type, status, staging_payload, enhanced_payload, enrichment_payload
                                FROM txm_bitx_logs.batch_step_function_master sf
                                WHERE sf.source = '{source}'
                                AND (sf.trading_partner = '{trading_partner}' OR sf.trading_partner IS NULL)
                                AND sf.reprocess_flag = 'N'
                                ORDER BY sf.update_timestamp DESC, IFNULL(sf.end_datetime, sf.start_datetime) DESC
                                LIMIT 1"""

            print(f'reprocess_query=>{reprocess_query}')
            # Execute the constructed query
            results = run_sql_query(reprocess_query, secrets)
            print(f"results => {results}")
            if results:
                is_skip_reprocess = False
                last_exec_result = results[0]
            else:
                is_skip_reprocess = True

            print(f'status_type=>{last_exec_result["status_type"]}')
            print(f'status=>{last_exec_result["status_type"]}')
            
            if not is_skip_reprocess and last_exec_result["status"] == "FAILED":
                print(f"Reprocess Flow STARTED for Source {source} and Trading Partner {trading_partner}...")
                if "STAGING" in last_exec_result["status_type"]:
                    print(f'Invoking reprocess flow for {last_exec_result["status_type"]} - {staging_step_function_arn}')
                    staging_payload = json.loads(last_exec_result["staging_payload"])
                    staging_payload["reprocess_flag"] = reprocess_flag
                    staging_payload["sf_batch_id"] = sf_batch_id
                    print(f'staging_payload => {staging_payload}')

                    sf_client = boto3.client('stepfunctions')
                    invoke_response = sf_client.start_execution(
                        stateMachineArn=staging_step_function_arn,
                        input=json.dumps(staging_payload)
                    )
                    print(f'invoke_response => {invoke_response}')

                    #updated log table
                    db_sp_params = sf_batch_id, None, None, None, None, invoke_response['executionArn'], json.dumps(staging_payload), None ,None, None, None, 'STAGING', None,  'Lambda - Invoke Reprocess Flow'
                    print(f"db_sp_params => {db_sp_params}")
                    dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
                    print(f"dbStoredProcedure => {dbStoredProcedure}")
                    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                    print(f"res => {res}")

                elif "ENHANCED" in last_exec_result["status_type"]:
                    print(f'Invoking reprocess flow for {last_exec_result["status_type"]} - {enhanced_step_function_arn}')
                    enhanced_payload = json.loads(last_exec_result["enhanced_payload"])
                    enhanced_payload["reprocess_flag"] = reprocess_flag
                    enhanced_payload["sf_batch_id"] = sf_batch_id
                    print(f'enhanced_payload => {enhanced_payload}')

                    sf_client = boto3.client('stepfunctions')
                    invoke_response = sf_client.start_execution(
                        stateMachineArn=enhanced_step_function_arn,
                        input=json.dumps(enhanced_payload)
                    )
                    print(f'invoke_response => {invoke_response}')
                
                    #updated log table
                    db_sp_params = sf_batch_id, None, None, None, None, None ,None, invoke_response['executionArn'], json.dumps(enhanced_payload), None, None, 'ENHANCED', 'IN PROGRESS',  'Lambda - Invoke Reprocess Flow'
                    print(f"db_sp_params => {db_sp_params}")
                    dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
                    print(f"dbStoredProcedure => {dbStoredProcedure}")
                    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                    print(f"res => {res}")
                
                elif "ENRICHMENT" in last_exec_result["status_type"]:
                    print(f'Invoking reprocess flow for {last_exec_result["status_type"]} - {enrichment_step_function_arn}')
                    enrichment_payload = json.loads(last_exec_result["enrichment_payload"])
                    enrichment_payload["reprocess_flag"] = reprocess_flag
                    enrichment_payload["sf_batch_id"] = sf_batch_id
                    print(f'enrichment_payload => {enrichment_payload}')

                    sf_client = boto3.client('stepfunctions')
                    invoke_response = sf_client.start_execution(
                        stateMachineArn=enrichment_step_function_arn,
                        input=json.dumps(enrichment_payload)
                    )
                    print(f'invoke_response => {invoke_response}')

                    #updated log table
                    db_sp_params = sf_batch_id, None, None, None, None, None ,None, None, None, invoke_response['executionArn'], json.dumps(enrichment_payload), 'ENRICHMENT', 'IN PROGRESS',  'Lambda - Invoke Reprocess Flow'
                    print(f"db_sp_params => {db_sp_params}")
                    dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
                    print(f"dbStoredProcedure => {dbStoredProcedure}")
                    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                    print(f"res => {res}")
                
                else:
                    raise_error=f'Unable to determine Step function load for Reprocess Flow - {last_exec_result["status_type"]} and {last_exec_result["status"]}'
                    print(raise_error)
                    raise Exception(raise_error)
            else:
                if trading_partner == 'jopari':
                    print(f'Invoking reprocess flow for processing CLAIM_MATCH_MANUAL for {last_exec_result["status_type"]} - {enrichment_step_function_arn}')
                    enrichment_payload = json.loads(last_exec_result["enrichment_payload"])
                    enrichment_payload["reprocess_flag"] = reprocess_flag
                    enrichment_payload["sf_batch_id"] = sf_batch_id
                    print(f'enrichment_payload => {enrichment_payload}')

                    sf_client = boto3.client('stepfunctions')
                    invoke_response = sf_client.start_execution(
                        stateMachineArn=enrichment_step_function_arn,
                        input=json.dumps(enrichment_payload)
                    )
                    print(f'invoke_response => {invoke_response}')

                    #updated log table
                    db_sp_params = sf_batch_id, None, None, None, None, None ,None, None, None, invoke_response['executionArn'], json.dumps(enrichment_payload), 'ENRICHMENT', 'IN PROGRESS',  'Lambda - Invoke Reprocess Flow'
                    print(f"db_sp_params => {db_sp_params}")
                    dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
                    print(f"dbStoredProcedure => {dbStoredProcedure}")
                    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                    print(f"res => {res}")
                else:
                    print(f"Skipping Reprocess Flow as no failures in last batch ran for Source {source} and Trading Partner {trading_partner}!!!")
                    #updated log table
                    db_sp_params = sf_batch_id, None, None, None, None, None ,None, None, None, None, None, 'REPROCESS FLOW', 'COMPLETED SKIP REPROCESS',  'Lambda - Invoke Reprocess Flow'
                    print(f"db_sp_params => {db_sp_params}")
                    dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
                    print(f"dbStoredProcedure => {dbStoredProcedure}")
                    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                    print(f"res => {res}")
            
            # Optionally, delete the message from the queue if processing is successful
            receipt_handle = record['receiptHandle']
            sqs_client = boto3.client('sqs')
            delete_response = sqs_client.delete_message(QueueUrl= sqs_queue_url,ReceiptHandle=receipt_handle)
            print(f'delete_response => {delete_response}')

            if len(event_records) > 1:
                # Delaying multiple batches together to avoid conflicts.
                print("Delaying multiple batches together to avoid conflicts...Sleep for 15 seconds")
                time.sleep(15)
        
    except json.JSONDecodeError as e:
        print(f"Message is not in JSON format. {str(e)}")
        raise e
    except Exception as e:
        print(f"Exception Block Error dump: {str(e)}")
        severity="FATAL"
        errorInfo="Unable to invoke Reprocess Flow"
        errorMessage=str(e)[:500]
        response=log_and_email(env_profile,source,trading_partner,file_header_id,reprocess_flag,sf_batch_id,errorInfo,errorMessage,severity,sns_topic_arn)
        print(f"Email sent successfully!message_id:{response['MessageId']}")
        if sf_batch_id:
            #updated log table
            db_sp_params = sf_batch_id, 'LAMBDA', lambda_name, aws_request_id, json.dumps(event), 'FAILED', 'INVOKE REPROCESS FLOW', errorInfo, errorMessage, 'Lambda - Invoke Reprocess Flow', 'Lambda - Invoke Reprocess Flow'
            print(f"db_sp_params => {db_sp_params}")
            dbStoredProcedure = "txm_bitx_logs.update_step_function_failure_log_status"
            print(f"dbStoredProcedure => {dbStoredProcedure}")
            res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
            print(f"res => {res}")
        raise
